"""Post news found from active Polymarket markets when it matches an open market."""

import asyncio
import html
import io
import json
import logging
import os
import re
import signal
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

# Defined before the configuration constants below, which validate their
# environment overrides and warn through this logger at import time.
logger = logging.getLogger("lash-news-bot")

NEWS_API_URL = "https://newsapi.org/v2/everything"
POLYMARKET_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
POLYMARKET_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
POLYMARKET_DATA_API_URL = "https://data-api.polymarket.com"
POLYMARKET_TRADES_URL = f"{POLYMARKET_DATA_API_URL}/v2/trades"
# Per-wallet activity feed on the same Data API the whale monitor already uses.
# Verified live: /activity?user=<address> returns TRADE/SPLIT/MERGE/REDEEM rows
# carrying usdcSize (transaction value), price, side, market title and tx hash.
POLYMARKET_WALLET_ACTIVITY_URL = f"{POLYMARKET_DATA_API_URL}/activity"
POLYMARKET_MARKET_PAGE = "https://polymarket.com/market"
# Polymarket's CLOB price-history endpoint, used by /chart.
POLYMARKET_PRICE_HISTORY_URL = "https://clob.polymarket.com/prices-history"
POLYGON_EXPLORER_TX = "https://polygonscan.com/tx"

# Railway (and most container hosts) wipe the filesystem on every deploy, so
# STATE_DIR should point at a mounted volume to persist settings across deploys.
STATE_DIR = Path(os.getenv("STATE_DIR") or ".")
SEEN_ARTICLES_FILE = STATE_DIR / "seen_articles.json"
GUILD_CHANNELS_FILE = STATE_DIR / "guild_channels.json"
GUILD_SEEN_ARTICLES_FILE = STATE_DIR / "seen_articles_by_guild.json"
GUILD_SETTINGS_FILE = STATE_DIR / "guild_settings.json"
MONITOR_STATE_FILE = STATE_DIR / "monitor_state.json"
WALLETS_FILE = STATE_DIR / "wallets.json"

MAX_MARKET_LOOKUPS_PER_POLL = 20
MAX_SAVED_ARTICLES = 500
NEWS_QUERY_MAX_LENGTH = 500
REQUEST_TIMEOUT_SECONDS = 20.0
REQUEST_MAX_ATTEMPTS = 3
RETRY_BACKOFF_BASE_SECONDS = 1.5
MAX_RETRY_DELAY_SECONDS = 20.0

# Monitoring bounds. Every alert type is throttled so a busy news or market day
# cannot turn into a wall of messages.
WATCHED_MARKET_LIMIT = 40
NEW_MARKET_LOOKBACK_LIMIT = 30
MAX_SEEN_MARKETS = 3000
MAX_SEEN_TRADES = 3000
MAX_ALERTS_PER_CYCLE = 10
MAX_KEYWORDS_PER_GUILD = 25
MAX_SOURCES_PER_GUILD = 25
MAX_KEYWORD_LENGTH = 50
DEFAULT_PRICE_MOVE_THRESHOLD = 10.0
MIN_PRICE_MOVE_THRESHOLD = 1.0
MAX_PRICE_MOVE_THRESHOLD = 100.0
DEFAULT_WHALE_MIN_USD = 10_000.0
MIN_WHALE_MIN_USD = 100.0
# A newly created market almost always reports liquidityNum == 0 (verified against
# the live Gamma API: 30/30 of the newest markets), so any non-zero default floor
# would silently suppress every new-market alert. Default to 0 -- alert on any new
# market -- and let administrators raise it through /alerts.
DEFAULT_NEW_MARKET_MIN_LIQUIDITY = 0.0
MIN_PRICE_ALERT_LIQUIDITY = 5_000.0

# ---------------------------------------------------------------------------
# Wallet watchlists
# ---------------------------------------------------------------------------
# Polymarket accounts are proxy wallets, so every tracked address is an EVM
# 0x-prefixed, 40-hex-character address. The address is the identity and the
# label is decorative. Watchlists live in their own state file so a large
# watchlist never bloats guild_settings.json.
WALLET_ADDRESS_PATTERN = re.compile(r"^0[xX][0-9a-fA-F]{40}$")
MAX_TRACKED_WALLETS_PER_GUILD = 25
MAX_WALLET_LABEL_LENGTH = 40
# Exactly one activity request per distinct address per cycle, hard-capped so a
# large watchlist can never turn one cycle into an unbounded burst of calls.
MAX_WALLET_ADDRESSES_PER_CYCLE = 50
WALLET_ACTIVITY_PAGE_SIZE = 50
MAX_WALLET_SEEN_EVENTS = 200
MAX_WALLET_TYPES_DISPLAYED = 6
DEFAULT_WALLET_MIN_USD = 0.0
MIN_WALLET_MIN_USD = 0.0
MAX_WALLET_MIN_USD = 10_000_000.0
# "trades" limits a watch to executed fills; "all" also covers splits, merges,
# redeems and reward payouts. Both are real rows in the Data API activity feed.
WALLET_ACTIVITY_FILTERS = ("all", "trades")
WALLET_ACTIVITY_FILTER_LABELS = {
    "all": "All activity",
    "trades": "Executed trades only",
}
# Activity types the Data API is known to report. They are used for display;
# a watch filtered to "all" accepts whatever type the API returns, so a new
# server-side type is delivered instead of silently dropped.
KNOWN_WALLET_ACTIVITY_TYPES = (
    "TRADE",
    "SPLIT",
    "MERGE",
    "REDEEM",
    "REWARD",
    "CONVERSION",
)
WALLET_BUY_ACTIVITY_TYPES = ("TRADE",)
ALERT_TYPES = ("news", "price", "new_market", "whale", "wallet", "x")
ALERT_LABELS = {
    "news": "News alerts",
    "price": "Price alerts",
    "new_market": "New market alerts",
    "whale": "Whale alerts",
    "wallet": "Wallet alerts",
    "x": "X (Twitter) alerts",
}
# The slash command that points one alert category at its own channel.
ALERT_CHANNEL_COMMANDS = {
    "news": "/set-news-alerts",
    "price": "/set-price-alerts",
    "new_market": "/set-new-markets",
    "whale": "/set-whale-alerts",
    "wallet": "/set-wallet-alerts",
    "x": "/set-x-alerts",
}


def parse_alert_channel_fallbacks(raw: str | None) -> dict[str, int]:
    """Parse ``ALERT_CHANNEL_FALLBACKS`` into ``{category: channel_id}``.

    Format: ``news=1234567890,whale=987654321`` (commas or semicolons). Unknown
    category names, blanks, and non-numeric ids are ignored so a typo can never
    send an alert to an unintended channel.
    """
    fallbacks: dict[str, int] = {}
    for chunk in re.split(r"[,;]", raw or ""):
        name, separator, value = chunk.partition("=")
        name = name.strip().lower()
        value = value.strip()
        if not separator or name not in ALERT_TYPES or not value:
            continue
        try:
            channel_id = int(value)
        except ValueError:
            logger.warning(
                "Ignoring ALERT_CHANNEL_FALLBACKS entry %r: %r is not a channel id.",
                chunk.strip(),
                value,
            )
            continue
        if channel_id > 0:
            fallbacks[name] = channel_id
    return fallbacks


# Optional server-side bootstrap for a category nobody has pointed at a channel
# yet. It only ever fills a gap in a server that already receives some alerts.
ALERT_CHANNEL_FALLBACKS = parse_alert_channel_fallbacks(os.getenv("ALERT_CHANNEL_FALLBACKS"))


def alert_fallback_channel_id(alert: str | None) -> int | None:
    """The configured bootstrap channel for one alert category, if any."""
    if not alert:
        return None
    return ALERT_CHANNEL_FALLBACKS.get(alert)


class _RetryableRequestError(RuntimeError):
    """A temporary upstream failure (429/5xx) that is safe to retry."""

    def __init__(self, status: int, retry_after: float | None = None) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


OWNER_ID = 1549940496607088670


# ---------------------------------------------------------------------------
# Market-aware news correlation configuration
# ---------------------------------------------------------------------------
# These thresholds are initial calibration settings, not a statistically
# validated probability. They are meant to be tuned against real alerts and
# rejected examples before any claim of calibration is made.
#
# The policy is tunable through the environment because "strict enough" is an
# editorial decision: a server that wants a quiet feed and a server that wants
# every plausible story are both legitimate. The defaults are the balanced
# setting -- a story with a clear, explainable link to a market posts; a story
# that only shares a subject needs a considerably higher score.
#
#   * Direct / Strong indirect: score >= NEWS_CORRELATION_ALERT_THRESHOLD
#   * Contextual:               score >= that threshold + NEWS_CORRELATION_CONTEXTUAL_MARGIN
#
# Matches at or above the diagnostics floor are kept for tuning and logging but
# are not posted unless they also clear the bar above.
def _env_int(name: str, default: int, low: int, high: int) -> int:
    """Read an integer override from the environment, clamped into range."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning(
            "%s must be a whole number; using the default %d.", name, default
        )
        return default
    return max(low, min(high, value))


MIN_CORRELATION_FOR_ALERT = _env_int("NEWS_CORRELATION_ALERT_THRESHOLD", 60, 1, 100)
MIN_CORRELATION_FOR_DIAGNOSTICS = _env_int(
    "NEWS_CORRELATION_DIAGNOSTICS_THRESHOLD", 45, 1, 100
)
# A contextual match has no concrete development behind it, so it must clear a
# noticeably higher bar than a direct one before it is allowed to post.
CONTEXTUAL_ALERT_MARGIN = _env_int("NEWS_CORRELATION_CONTEXTUAL_MARGIN", 15, 0, 100)
# Two structural guards keep the relaxed bar from turning into noise. Both are
# tunable because the right setting depends on how much a server wants to see:
#   NEWS_MIN_SUBSTANTIVE_TERMS: how many shared non-numeric, non-catalog-wide
#       terms count as a link when no named entity is shared. One shared ordinary
#       word ("strike", "post", "eagles") links two unrelated stories, so 2 is
#       the default.
#   NEWS_REQUIRE_HEADLINE_EVIDENCE: require that evidence to be in the headline.
#       NewsAPI's body text carries site furniture and related-article links, so
#       without this a printer launch matched a football market.
MIN_SUBSTANTIVE_TERMS = _env_int("NEWS_MIN_SUBSTANTIVE_TERMS", 2, 1, 10)


def _env_flag(name: str, default: bool) -> bool:
    """Read a boolean override from the environment."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().casefold() in {"1", "true", "yes", "on"}


REQUIRE_HEADLINE_EVIDENCE = _env_flag("NEWS_REQUIRE_HEADLINE_EVIDENCE", True)
# The diagnostics floor is only useful below the alert bar; keep it consistent
# even if the environment sets them the wrong way round.
MIN_CORRELATION_FOR_DIAGNOSTICS = min(
    MIN_CORRELATION_FOR_DIAGNOSTICS, MIN_CORRELATION_FOR_ALERT
)
MAX_RELATED_MARKETS = 3

# ---------------------------------------------------------------------------
# X/Twitter account tracking configuration
# ---------------------------------------------------------------------------
# The tracker polls the official X API v2 (pay-per-use) with an app-only
# bearer token. GET /2/users/:id/tweets with a per-account since_id returns
# only posts newer than the last one delivered, so a quiet account costs
# nothing between posts (X bills per post resource returned) and a restart
# never replays old posts. The X Activity API push stream is a possible later
# upgrade; polling keeps the failure modes simple and the spend visible.
X_API_BASE_URL = "https://api.x.com/2"
X_PROFILE_URL = "https://x.com"
X_TRACKED_FILE = STATE_DIR / "x_tracked.json"
# X handles are 1-15 characters of letters, digits and underscores. A display
# name is never proof of identity: the handle resolves to a numeric user id,
# and the id is what the API is queried with.
X_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,15}$")
MAX_TRACKED_X_ACCOUNTS_PER_GUILD = 25
MAX_X_ACCOUNTS_PER_CYCLE = 50
# The user-timeline endpoint's minimum page size is 5; only new posts are
# requested via since_id, so a larger page just absorbs a burst.
MAX_X_POSTS_PER_POLL = 5
MAX_X_SEEN_POSTS = 200
MAX_X_LABEL_LENGTH = 40
MAX_X_KEYWORDS_PER_ACCOUNT = 25
MAX_X_ALERTS_PER_CYCLE = 10
X_POLL_INTERVAL_SECONDS = _env_int("X_POLL_INTERVAL_SECONDS", 120, 60, 3600)
# "all" delivers every activity type the API reports. The narrower filters
# drop exactly what they name: "originals" keeps only original posts, and
# "no_replies" keeps everything except replies.
X_ACTIVITY_FILTERS = ("all", "originals", "no_replies")
X_ACTIVITY_FILTER_LABELS = {
    "all": "All activity (posts, replies, quotes, reposts)",
    "originals": "Original posts only",
    "no_replies": "Everything except replies",
}
# Human labels for the activity types identified from referenced_tweets.
X_ACTIVITY_TYPE_LABELS = {
    "post": "New post",
    "reply": "Reply",
    "quote": "Quote post",
    "repost": "Repost",
}

# Discord's hard embed limits. Exceeding any of them rejects the whole message
# with "Invalid Form Body", which would silently drop an alert, so every embed is
# clamped to these values immediately before it is sent.
EMBED_TITLE_LIMIT = 256
EMBED_DESCRIPTION_LIMIT = 4096
EMBED_FIELD_NAME_LIMIT = 256
EMBED_FIELD_VALUE_LIMIT = 1024
EMBED_FIELD_COUNT_LIMIT = 25
EMBED_FOOTER_LIMIT = 2048
EMBED_AUTHOR_LIMIT = 256
# Discord rejects empty field names and values; this placeholder keeps a
# trimmed field valid without adding visible clutter.
# The classic Sovereign palette: gold for news and market alerts, green for
# large buys, red for large sells.
SOVEREIGN_COLOR = discord.Color.from_rgb(198, 161, 91)
WHALE_BUY_COLOR = discord.Color.green()
WHALE_SELL_COLOR = discord.Color.red()

EMPTY_EMBED_TEXT = "\u200b"

# News source behavior.
# The bot keeps its current working provider as the default and primary fallback.
# A modular source adapter is introduced so another provider can be wired in later
# without changing the monitor loop; no new provider is activated here because no
# additional API key has been approved.
DEFAULT_NEWS_SOURCE_NAME = "NewsAPI"
MIN_SOURCE_PUBLICATION_AGE_HOURS = 72
# Treat an article without a usable publication timestamp as too stale to trust
# for market correlation unless it is clearly a breaking/live story from a
# reputable source.
UNKNOWN_PUBLICATION_STALE_HOURS = 6

# Dedup fingerprinting. We persist a content fingerprint in addition to the
# canonical URL so near-identical syndicated copies and minor headline variations
# do not replay as fresh alerts.
FINGERPRINT_CHUNK_SIZE = 200


class NewsSource:
    """A small description of an article's publisher and its known reliability.

    Reputation is a useful input to correlation but does not override it: a
    reputable source can still publish an irrelevant article, and a low-reputation
    source can occasionally report something relevant. This class only describes
    the source so the correlation step can weigh it; it does not decide relevance.
    """

    def __init__(
        self,
        *,
        name: str,
        domain: str | None = None,
        publisher_type: str = "news",
        is_primary_source: bool = False,
    ) -> None:
        self.name = name
        self.domain = domain
        self.publisher_type = publisher_type
        self.is_primary_source = is_primary_source

    @property
    def reliability_bonus(self) -> int:
        """A small trust nudge for the source, used only inside scoring.

        This is not a verdict on relevance and it does not let a reputable source
        make an unrelated article relevant.
        """
        if self.is_primary_source:
            return 8
        if self.name in RELIABLE_SOURCE_NAMES:
            return 5
        if self.publisher_type == "news" and self.name and self.name != "Unknown publication":
            return 2
        return 0


# A small, explicit registry of sources the bot treats as higher-reliability for
# market-related reporting. This list is descriptive, not prescriptive: it biases
# trust slightly upward for established wire services and primary sources, but it
# does not make their articles automatically relevant.
RELIABLE_SOURCE_NAMES = frozenset({
    # General and financial wires.
    "Reuters",
    "Associated Press",
    "AP",
    "BBC News",
    "Financial Times",
    "CNBC",
})


# Words that signal the article describes a concrete development or change that
# bears on an outcome (price movement, disruption, official action, decision).
# Generic reporting/meeting words are deliberately excluded so a story that only
# mentions the subject is not upgraded to a Strong indirect relationship.
DEVELOPMENT_SIGNAL_TERMS = frozenset({
    "rise", "rose", "rises", "rising",
    "fall", "fell", "falls", "falling",
    "climb", "climbed", "climbs",
    "jump", "jumped", "jumps",
    "surge", "surged", "surges",
    "plunge", "plunged", "plunges",
    "drop", "dropped", "drops",
    "decline", "declined", "declines",
    "gain", "gained", "gains",
    "rally", "rallied", "rallies",
    "slump", "slumped", "slumps",
    "outage", "outages", "disruption", "disruptions",
    "shortage", "shortages", "sanction", "sanctions",
    "ban", "banned", "bans", "hike", "hikes", "hiked",
    "cut", "cuts", "raise", "raised", "raises", "lower", "lowered", "lowers",
    "approve", "approved", "approves", "reject", "rejected", "rejects",
    "launch", "launched", "launches", "file", "filed", "files",
    "sue", "sued", "sues", "settle", "settled", "settlement", "settlements",
    "deal", "deals", "agreement", "agreements", "sign", "signed", "signs",
    "announce", "announced", "announces", "confirm", "confirmed", "confirms",
    "elect", "elected", "elects", "resign", "resigned", "resigns",
    "win", "won", "wins", "pass", "passed", "passes", "fail", "failed", "fails",
})


def source_from_article(article: dict[str, Any]) -> NewsSource:
    """Derive a NewsSource from a NewsAPI-style article dict."""
    source = article.get("source")
    if isinstance(source, dict):
        name = clean_article_text(source.get("name"))
        url = clean_article_text(source.get("url"))
        domain = ""
        if url and url.startswith("http"):
            domain = url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0].lower()
        return NewsSource(
            name=name,
            domain=domain or None,
            publisher_type="news",
            is_primary_source=False,
        )
    name = clean_article_text(source) if isinstance(source, str) else ""
    return NewsSource(name=name or "Unknown publication")


class MarketMatch:
    """One candidate news-to-market relationship that survived the deeper
    correlation evaluation.

    The score is a relevance estimate for *this* article to *this* market, not a
    prediction that the market will resolve YES and not a guarantee of price
    movement.
    """

    __slots__ = (
        "market",
        "score",
        "relationship",
        "explanation",
        "why_short",
    )

    def __init__(
        self,
        *,
        market: dict[str, Any],
        score: int,
        relationship: str,
        explanation: str,
        why_short: str,
    ) -> None:
        self.market = market
        self.score = score
        self.relationship = relationship
        self.explanation = explanation
        self.why_short = why_short


def classify_relationship(relationship: str) -> bool:
    """Return True when this relationship is a direct, defensible link.

    This describes the relationship itself. Whether the configured policy may
    post it is decided by :func:`is_alertable`, which is the only posting gate.
    """
    return relationship in {"Direct", "Strong indirect"}


def required_score(relationship: str) -> int | None:
    """The score a match of this relationship must reach to be posted.

    Returns ``None`` when the policy never posts that relationship.
      * ``Direct`` / ``Strong indirect`` need ``MIN_CORRELATION_FOR_ALERT``.
      * ``Contextual`` needs that threshold plus ``CONTEXTUAL_ALERT_MARGIN``,
        because a shared subject alone is much weaker evidence than a concrete
        development.
    """
    if classify_relationship(relationship):
        return MIN_CORRELATION_FOR_ALERT
    if relationship == "Contextual":
        return MIN_CORRELATION_FOR_ALERT + CONTEXTUAL_ALERT_MARGIN
    return None


def is_alertable(relationship: str, score: int) -> bool:
    """Whether the configured policy allows this match to be posted.

    This is the single posting gate for news alerts; ``compute_correlation``
    deliberately returns weaker matches too, so the gate -- not the scorer --
    decides what a server actually receives.
    """
    threshold = required_score(relationship)
    return threshold is not None and score >= threshold


def article_fingerprint(article: dict[str, Any]) -> str:
    """A stable, lightweight content fingerprint for dedup.

    Uses the headline plus the leading chunk of description/content so that
    minor headline variants and syndicated copies of the same story collapse
    onto the same key, while genuinely different stories stay distinct.

    The fingerprint is normalized enough to absorb minor headline variants and
    light editorial rewording, but it is not a semantic hash: a genuinely
    different story will still produce a different fingerprint.
    """
    parts = []
    for field in ("title", "description", "content"):
        text = clean_article_text(article.get(field))
        if text:
            parts.append(text)
    if not parts:
        return ""
    combined = " ".join(parts)
    chunk = combined[:FINGERPRINT_CHUNK_SIZE]
    normalized = chunk.casefold()
    # Collapse punctuation and short modifiers so minor headline variants and
    # syndicated copies map onto the same key.
    normalized = re.sub(r"[^A-Za-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\b(the|a|an|and|or|of|for|on|in|to|by|with|after|before|this|that|these|those|new|latest|reports|says|said|announced|updated|update|break|breaking)\b", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


MARKET_MATCH_STOP_WORDS = frozenset(
    {
        "a", "about", "after", "against", "all", "an", "and", "any", "are",
        "as", "at", "be", "because", "before", "between", "by", "can", "could",
        "did", "do", "does", "for", "from", "get", "has", "have", "how", "in",
        "into", "is", "it", "its", "latest", "may", "might", "more", "most",
        "new", "news", "no", "not", "of", "on", "or", "over", "report",
        "reports", "says", "said", "should", "than", "that", "the", "their",
        "them", "there", "these", "this", "those", "to", "today", "under",
        "update", "updates", "was", "were", "what", "when", "where", "which",
        "who", "why", "will", "with", "would", "yes", "year", "years",
        "above", "below", "candidate", "candidates", "down", "election",
        "elections", "first", "finish", "minister", "next", "nomination",
        "nominee", "nominees", "place", "presidency", "president",
        "presidential", "prime", "rank", "ranking", "second", "third",
        "up", "versus", "vs", "win", "winner", "wins", "won",
        "market", "markets", "polymarket", "announces",
        "announced", "confirms", "confirmed", "expected", "whether",
        # Function words and connectives that show up capitalized mid-headline.
        "if", "only", "otherwise", "however", "apart", "according", "still",
        "yet", "also", "now", "then", "both", "each", "every", "many", "some",
        "such", "just", "am", "pm", "et", "us", "uk", "eu", "un", "mr", "ms",
        "dr", "st", "other", "others", "through", "throughout", "until",
        "till", "across", "during", "since", "despite", "while", "without",
        "within", "onto", "upon", "per", "via", "among", "around", "besides",
        "beyond", "toward", "towards", "amid", "ago", "least", "most", "many",
        # Words that are only ever page furniture or a social-sharing prompt.
        "media", "social",
        # Generic nouns whose identity always lives in the other tokens: a market
        # about a US state or the United States is identified by the state,
        # country or person it names, never by the word "state" itself.
        "state", "states", "united", "global", "international", "world",
        # Article-page boilerplate that would otherwise become an "entity".
        "follow", "read", "share", "subscribe", "click", "photo", "image",
        "video", "watch", "advertisement", "copyright", "rights",
        "reserved", "editor", "opinion", "posted", "updated", "home",
    }
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


def write_json_atomically(path: Path, payload: Any) -> None:
    """Write JSON via a temp file plus rename so a crash cannot corrupt it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = path.with_name(path.name + ".tmp")
    temporary_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_file.replace(path)


def dedupe_strings(values: list[Any]) -> list[str]:
    """Trim, drop empties, and remove case-insensitive duplicates in order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def load_seen_articles() -> list[str]:
    """Load previously posted article URLs so restarts do not repost them."""
    try:
        data = json.loads(SEEN_ARTICLES_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read saved article history; starting with an empty history.")
        return []

    if not isinstance(data, list):
        logger.warning("Saved article history has an unexpected format; starting fresh.")
        return []

    urls: list[str] = []
    seen: set[str] = set()
    for url in data:
        if isinstance(url, str) and url not in seen:
            urls.append(url)
            seen.add(url)
    return urls[-MAX_SAVED_ARTICLES:]


def save_seen_articles(urls: list[str]) -> None:
    """Write a bounded history atomically."""
    write_json_atomically(SEEN_ARTICLES_FILE, urls[-MAX_SAVED_ARTICLES:])


def load_guild_channels() -> dict[str, int]:
    """Load persistent per-server alert channel IDs."""
    try:
        data = json.loads(GUILD_CHANNELS_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read saved server channel settings.")
        return {}

    if not isinstance(data, dict):
        logger.warning("Saved server channel settings have an unexpected format.")
        return {}

    channels: dict[str, int] = {}
    for guild_id, channel_id in data.items():
        if (
            isinstance(guild_id, str)
            and guild_id.isdigit()
            and isinstance(channel_id, int)
            and not isinstance(channel_id, bool)
            and channel_id > 0
        ):
            channels[guild_id] = channel_id
    return channels


def load_guild_seen_articles() -> dict[str, list[str]]:
    """Load article history independently for each Discord server."""
    try:
        data = json.loads(GUILD_SEEN_ARTICLES_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read per-server article history.")
        return {}

    if not isinstance(data, dict):
        logger.warning("Per-server article history has an unexpected format.")
        return {}

    histories: dict[str, list[str]] = {}
    for guild_id, urls in data.items():
        if not isinstance(guild_id, str) or not guild_id.isdigit():
            continue
        if not isinstance(urls, list):
            continue
        unique_urls = list(dict.fromkeys(
            url for url in urls if isinstance(url, str)
        ))
        histories[guild_id] = unique_urls[-MAX_SAVED_ARTICLES:]
    return histories


def save_guild_seen_articles(histories: dict[str, list[str]]) -> None:
    """Atomically persist bounded per-server article histories."""
    write_json_atomically(
        GUILD_SEEN_ARTICLES_FILE,
        {
            guild_id: urls[-MAX_SAVED_ARTICLES:]
            for guild_id, urls in histories.items()
        },
    )


def parse_guild_ids(value: str | None) -> list[int]:
    """Parse a comma/semicolon separated list of guild IDs for fast command sync."""
    if not value:
        return []

    guild_ids: list[int] = []
    for part in value.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            guild_ids.append(int(part))
        except ValueError as exc:
            raise ValueError(
                "DISCORD_DEV_GUILD_ID must be a comma-separated list of numeric "
                "Discord server IDs."
            ) from exc
    return guild_ids


def parse_channel_id(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError("DISCORD_CHANNEL_ID must be a numeric Discord channel ID.") from exc


def parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def clean_article_text(value: Any) -> str:
    """Normalize NewsAPI text and remove its truncation marker."""
    text = html.unescape(str(value or ""))
    text = re.sub(r"\[\+\s*\d+\s+chars?\]", "", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def summarize_article(article: dict[str, Any]) -> str:
    """Use available article excerpts to form a short, factual summary."""
    description = clean_article_text(article.get("description"))
    content = clean_article_text(article.get("content"))

    if description and content:
        if content.casefold().startswith(description.casefold()):
            content = content[len(description):].lstrip(" .;:—-")
        elif content.casefold() in description.casefold():
            content = ""

    source_text = " ".join(part for part in (description, content) if part)
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", source_text)
        if sentence.strip()
    ]
    if not sentences:
        return "Summary not provided by the source."

    # Prefer two to four distinct source sentences when the available excerpt
    # contains them; never invent facts to pad a shorter source excerpt.
    return " ".join(sentences[:4])[:1024]


def extract_article_topics(
    article: dict[str, Any],
) -> list[str]:
    """Return obvious capitalized entities from the article."""
    searchable_text = " ".join(
        clean_article_text(article.get(field))
        for field in ("title", "description", "content")
    )
    topics: list[str] = []
    seen: set[str] = set()

    def add_topic(topic: str) -> None:
        normalized = topic.strip()
        key = normalized.casefold()
        if normalized and key not in seen and len(normalized) <= 80:
            topics.append(normalized)
            seen.add(key)

    entity_pattern = re.compile(
        r"\b[A-Z][A-Za-z&'-]+(?:\s+[A-Z][A-Za-z&'-]+){1,2}\b|\b[A-Z]{2,}\b"
    )
    ignored_starts = {
        "a", "an", "after", "and", "breaking", "how", "in", "new",
        "the", "this", "what", "when", "where", "why",
    }
    for match in entity_pattern.finditer(searchable_text):
        entity = match.group().strip()
        first_word = entity.split()[0].casefold()
        if first_word not in ignored_starts:
            add_topic(entity)
        if len(topics) >= 6:
            break

    return topics[:6]


def meaningful_market_terms(text: str) -> set[str]:
    return {
        token.casefold()
        for token in re.findall(r"[A-Za-z0-9]+", text)
        if len(token) >= 2 and token.casefold() not in MARKET_MATCH_STOP_WORDS
    }


def make_market_news_query(
    market_lists: list[list[dict[str, Any]]],
    extra_terms: tuple[str, ...] = (),
) -> str:
    """Build a NewsAPI OR query from server keywords and active market questions."""
    candidate_terms: list[str] = []
    seen_terms: set[str] = set()

    # Server-defined keywords come first so a 500-character query never drops a
    # term an administrator explicitly asked for.
    for term in extra_terms:
        cleaned = clean_article_text(term).replace('"', "")
        if cleaned and cleaned.casefold() not in seen_terms:
            candidate_terms.append(cleaned)
            seen_terms.add(cleaned.casefold())

    max_markets = max((len(markets) for markets in market_lists), default=0)

    # Interleave volume rankings so the query is not filled by just one market
    # category when either ranking is dominated by similar contracts.
    for index in range(max_markets):
        for markets in market_lists:
            if index >= len(markets):
                continue
            question = clean_article_text(markets[index].get("question"))
            for term in re.findall(r"[^\W\d_][\w'-]*", question, flags=re.UNICODE):
                normalized = term.casefold()
                if (
                    (len(term) < 3 and not (len(term) == 2 and term.isupper()))
                    or not any(character.isalpha() for character in term)
                    or normalized in MARKET_MATCH_STOP_WORDS
                    or normalized in seen_terms
                ):
                    continue
                candidate_terms.append(term.replace('"', ""))
                seen_terms.add(normalized)

    clauses: list[str] = []
    for term in candidate_terms:
        clause = f'"{term}"'
        proposed = " OR ".join((*clauses, clause))
        if len(proposed) <= NEWS_QUERY_MAX_LENGTH:
            clauses.append(clause)

    if not clauses:
        raise RuntimeError("Active Polymarket markets did not provide searchable terms.")
    return " OR ".join(clauses)


# Calendar vocabulary is never evidence on its own: a market that resolves "by
# October 31" and an article published in October share a month, not a subject.
# Measured live: without this, "School holiday tomorrow (October 16, 2026)"
# matched a tweet-count market at 100%.
CALENDAR_TERMS = frozenset({
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december",
    "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct",
    "nov", "dec",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
    "sunday", "mon", "tue", "tues", "wed", "thu", "thur", "thurs", "fri",
    "sat", "sun", "weekend", "weekday", "monthly", "annual", "quarterly",
})


def is_weak_evidence_term(token: str) -> bool:
    """True for tokens that are numbers or calendar words.

    Both are shared by unrelated stories constantly, so they can add to a score
    once something substantive established the link, but they can never be the
    link.
    """
    return is_numeric_term(token) or token in CALENDAR_TERMS


def is_numeric_term(token: str) -> bool:
    """True for a bare number or year.

    Numbers are weak evidence on their own: a market that resolves on ``$90``
    and an article that mentions ``9%`` share a token, not a subject. Numbers
    still add to a score once some substantive term has established the link.
    """
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", token or ""))


def compute_common_catalog_terms(
    markets: list[dict[str, Any]], minimum_share: float = 0.15
) -> frozenset[str]:
    """Vocabulary that appears across too much of the catalog to identify a market.

    A token like ``october``, ``state`` or ``2026`` is in dozens of unrelated
    market questions, so sharing it with an article says nothing. Terms above
    ``minimum_share`` of the catalog are treated as non-evidence; a term that
    only a few markets use (``greenland``, ``brent``) stays decisive.
    """
    if not markets:
        return frozenset()
    counts: Counter[str] = Counter()
    for market in markets:
        if not isinstance(market, dict):
            continue
        terms = meaningful_market_terms(market.get("question") or "")
        terms |= {
            normalize_entity(entity) for entity in (market.get("entities") or set())
        }
        counts.update(term for term in terms if term)
    threshold = max(2, int(len(markets) * minimum_share))
    return frozenset(
        term for term, count in counts.items() if count >= threshold
    )


def normalize_entity(token: str) -> str:
    """Fold a candidate entity to a comparable key.

    Strips a trailing possessive (``ukraine's`` -> ``ukraine``) so an article that
    says "Ukraine's minister" still matches a market that names "Ukraine".
    """
    token = token.casefold().strip()
    token = re.sub(r"'s$", "", token)
    token = token.strip("'")
    return token


def extract_named_entities(text: str) -> set[str]:
    """Recover likely proper nouns from a cleaned text.

    This is intentionally lightweight: it is used to test whether an article is
    actually *about* the same person, company, asset, or institution a market
    names, not to build a general NLP pipeline.
    """
    entities: set[str] = set()
    for match in re.finditer(
        r"\b(?:[A-Z][A-Za-z0-9'-]{1,}|[A-Z]{2,})\b",
        text,
    ):
        normalized = normalize_entity(match.group())
        if normalized and normalized not in MARKET_MATCH_STOP_WORDS:
            entities.add(normalized)
    return entities


def extract_article_entities(article: dict[str, Any]) -> set[str]:
    """Entities mentioned across the article headline and available body text."""
    return extract_named_entities(
        " ".join(
            clean_article_text(article.get(field))
            for field in ("title", "description", "content")
        )
    )


def extract_market_question_entities(market: dict[str, Any]) -> set[str]:
    """Entities named in the market question and its parent event title."""
    parts = []
    for value in (market.get("question"), market.get("event_title")):
        text = clean_article_text(value)
        if text:
            parts.append(text)
    return extract_named_entities(" ".join(parts))


def parse_market_question(market: dict[str, Any]) -> dict[str, Any]:
    """Extract the structured bits of a market the correlation engine cares about.

    Returns a small, explicit record rather than raw Gamma dicts, so the scorer
    can read the market's actual question, the assets or outcomes it names, the
    deadline it cares about, and whatever resolution info is available -- without
    assuming any particular schema.
    """
    question = clean_article_text(market.get("question"))
    slug = market.get("slug")
    if not isinstance(question, str) or not question.strip():
        return {}
    if not isinstance(slug, str) or not slug.strip():
        slug = None

    outcomes = parse_outcomes(market)
    prices = parse_outcome_prices(market)

    resolution = clean_article_text(market.get("resolution", market.get("resolutionText")))
    description = clean_article_text(market.get("description"))
    event_title = clean_article_text(market.get("event_title")) or clean_article_text(
        market.get("event", {}).get("title") if isinstance(market.get("event"), dict) else None
    )

    end_date = parse_published_at(market.get("endDate"))

    # Pull out the named entities so we can tell whether an article is about the
    # specific thing the market resolves on. The description is deliberately
    # excluded: Gamma descriptions are long resolution paragraphs full of
    # capitalized ordinary words ("Note", "If", "However", "Otherwise"), and
    # treating those as identities made unrelated stories look correlated. The
    # question and the event title carry the market's actual subject, and the
    # description still feeds the resolution/focus checks below.
    named_entities: set[str] = set()
    if event_title:
        named_entities |= extract_named_entities(event_title)
    named_entities |= extract_named_entities(question)
    # Outcome and price labels are not meaningful entities by themselves.
    outcome_labels = {outcome.strip().casefold() for outcome in outcomes if outcome.strip()}
    named_entities -= outcome_labels
    named_entities -= {"yes", "no", "either", "neither", "buy", "sell", "long", "short"}

    return {
        "question": question,
        "slug": slug,
        "url": (
            f"https://polymarket.com/market/{quote(slug, safe='-_')}"
            if slug
            else "https://polymarket.com"
        ),
        "outcomes": outcomes,
        "prices": prices,
        "resolution": resolution,
        "description": description,
        "event_title": event_title,
        "end_date": end_date,
        "entities": {entity for entity in named_entities if entity.strip()},
    }


def article_event_description(article: dict[str, Any]) -> str:
    """A single sentence-ish summary of the event the article actually reports.

    This is used by the scorer and the explanation builder so the match text stays
    grounded in what the article says rather than in headlines alone.
    """
    return summarize_article(article)


def score_source_reliability(source: NewsSource) -> int:
    """Return a small reliability contribution for the source.

    This is a tie-breaker and trust nudge, not a verdict on relevance.
    """
    score = 0
    if source.is_primary_source:
        score += 8
    elif source.name in RELIABLE_SOURCE_NAMES:
        score += 5
    elif source.publisher_type == "news" and source.name and source.name != "Unknown publication":
        score += 2
    return score


def compute_correlation(
    article: dict[str, Any],
    market: dict[str, Any],
    *,
    common_terms: frozenset[str] = frozenset(),
) -> MarketMatch | None:
    """Evaluate the real relationship between one article and one market.

    Returns a MarketMatch only when the relationship is strong enough to explain
    why this specific story matters to this specific market. Returns None when the
    connection is weak, unsupported, stale, or ambiguous.

    ``common_terms`` carries the vocabulary that is too widespread across the
    catalog to identify any one market (see
    :func:`compute_common_catalog_terms`). Passing it makes the check stricter;
    the default keeps this function usable on its own.
    """
    source = source_from_article(article)
    parsed_market = parse_market_question(market)
    if not parsed_market:
        return None

    headline = clean_article_text(article.get("title"))
    summary = article_event_description(article)
    published_at = parse_published_at(
        article.get("publishedAt")
        if isinstance(article.get("publishedAt"), str)
        else None
    )

    article_entities = extract_article_entities(article)
    market_entities = parsed_market["entities"]

    # ---- cheap pre-filter: does this article even mention the same nouns? ----
    entity_overlap = article_entities & market_entities
    question_terms = meaningful_market_terms(parsed_market["question"])
    article_terms = meaningful_market_terms(
        " ".join(part for part in (headline, summary) if part)
    )
    question_term_overlap = article_terms & question_terms

    # Drop outcome-label noise ("yes"/"no") from overlap because they are not
    # meaningful entities by themselves.
    meaningful_entities = entity_overlap - {"yes", "no"}
    meaningful_overlap = bool(
        (meaningful_entities - question_term_overlap)
        or (meaningful_entities & question_terms)
        or question_term_overlap
    )
    if not meaningful_overlap:
        return None

    # A link that rests only on numbers, dates, or catalog-wide vocabulary is a
    # keyword coincidence, not a relationship. At least one term has to actually
    # identify this particular market.
    substantive_entities = {
        entity
        for entity in meaningful_entities
        if not is_weak_evidence_term(entity) and entity not in common_terms
    }
    substantive_terms = {
        term
        for term in question_term_overlap
        if not is_weak_evidence_term(term) and term not in common_terms
    }
    # A named entity that few markets mention is strong evidence on its own
    # ("iran", "brent", "greenland"). Ordinary vocabulary is not: a single shared
    # word -- "strike", "post", "state" -- links two completely unrelated stories,
    # so it takes at least two of them to count as a link.
    if not substantive_entities and len(substantive_terms) < MIN_SUBSTANTIVE_TERMS:
        return None

    # The evidence has to be in what the article's headline reports. NewsAPI's
    # description/content fields carry site furniture and "related article" text,
    # so a body match alone is not a statement about the story: without this, a
    # 3D-printer launch once matched a football market because the page mentioned
    # Frankfurt somewhere below the fold.
    if REQUIRE_HEADLINE_EVIDENCE:
        headline_entities = extract_named_entities(headline)
        headline_terms = meaningful_market_terms(headline)
        if not (
            (substantive_entities & headline_entities)
            or (substantive_terms & headline_terms)
        ):
            return None

    # ---- relationship reasoning ------------------------------------
    relationship = _classify_relationship(
        article=article,
        headline=headline,
        summary=summary,
        source=source,
        parsed_market=parsed_market,
        entity_overlap=entity_overlap,
        question_term_overlap=question_term_overlap,
        published_at=published_at,
    )
    if relationship == "Unrelated":
        return None

    # ---- score ----------------------------------------------------------
    score = _score_relationship(
        article=article,
        headline=headline,
        summary=summary,
        source=source,
        parsed_market=parsed_market,
        relationship=relationship,
        entity_overlap=entity_overlap,
        question_term_overlap=question_term_overlap,
        published_at=published_at,
    )

    # Retain anything above the diagnostics floor so the rejection is visible
    # and tunable; the posting decision itself is made by the policy gate in
    # select_polymarket_market().
    if score < MIN_CORRELATION_FOR_DIAGNOSTICS:
        return None

    explanation_text, why_short = build_match_explanation(
        article=article,
        headline=headline,
        summary=summary,
        source=source,
        parsed_market=parsed_market,
        relationship=relationship,
        score=score,
        entity_overlap=entity_overlap,
        question_term_overlap=question_term_overlap,
        published_at=published_at,
    )
    return MarketMatch(
        market=market,
        score=score,
        relationship=relationship,
        explanation=explanation_text,
        why_short=why_short,
    )


def market_focus_terms(parsed_market: dict[str, Any]) -> set[str]:
    """The market question's content terms excluding the market's own entities.

    These are the predicate/object words (e.g. ``sign``, ``deal``, ``rise``,
    ``90``) that describe what the market actually resolves on. An article that
    reproduces them is reporting the market's own event; an article that only
    shares the subject entity is not.
    """
    question_terms = meaningful_market_terms(parsed_market.get("question", ""))
    entities = {normalize_entity(entity) for entity in (parsed_market.get("entities") or set())}
    return {term for term in question_terms if term not in entities}


def _article_terms(headline: str, summary: str) -> set[str]:
    return meaningful_market_terms(" ".join(part for part in (headline, summary) if part))


def _development_signal(terms: set[str]) -> bool:
    """True when the article describes a concrete change bearing on an outcome."""
    return bool(terms & DEVELOPMENT_SIGNAL_TERMS)


def _classify_relationship(
    *,
    article: dict[str, Any],
    headline: str,
    summary: str,
    source: NewsSource,
    parsed_market: dict[str, Any],
    entity_overlap: set[str],
    question_term_overlap: set[str],
    published_at: datetime | None,
) -> str:
    """Classify the relationship into Direct / Strong indirect / Contextual / Unrelated.

    The classification is what the alert policy is gated on: an unrelated story
    must never pass merely because it shares keywords with the market, and a
    story that only mentions the same subject is not upgraded to a strong link.
    """
    article_terms = _article_terms(headline, summary)
    meaningful_entities = {
        normalize_entity(entity) for entity in entity_overlap
    } - {"yes", "no"}
    focus_overlap = article_terms & market_focus_terms(parsed_market)

    # Direct: the article reproduces the market's own subject *and* its
    # predicate/object terms, i.e. it reports on the event or decision itself.
    if meaningful_entities and focus_overlap:
        return "Direct"

    # Strong indirect: a concrete development with a real, explainable link to
    # the market (a shared entity or shared question terms).
    has_link = bool(meaningful_entities or question_term_overlap)
    if has_link and _development_signal(article_terms):
        return "Strong indirect"

    # Contextual: mentions the same subject or terms but establishes no
    # meaningful connection to the outcome.
    if has_link:
        return "Contextual"

    return "Unrelated"


def _score_relationship(
    *,
    article: dict[str, Any],
    headline: str,
    summary: str,
    source: NewsSource,
    parsed_market: dict[str, Any],
    relationship: str,
    entity_overlap: set[str],
    question_term_overlap: set[str],
    published_at: datetime | None,
) -> int:
    score = 0

    # Base by classification.
    if relationship == "Direct":
        score += 67
    elif relationship == "Strong indirect":
        score += 58
    else:
        score += 15

    # Entity correspondence strength.
    meaningful_entities = {
        normalize_entity(entity) for entity in entity_overlap
    } - {"yes", "no"}
    if meaningful_entities:
        score += min(24, len(meaningful_entities) * 8)
        # Penalize a single common-noun entity match unless the question also
        # shares a specific term.
        if len(meaningful_entities) == 1 and not question_term_overlap:
            score -= 12

    # Question-term correspondence.
    if question_term_overlap:
        score += min(20, len(question_term_overlap) * 5)

    # Source reliability nudge.
    score += source.reliability_bonus

    # Resolution-criteria compatibility: if the article describes something that
    # would plausibly affect the market's resolution condition, add a small
    # specificity bonus.
    if parsed_market["resolution"]:
        score += _resolution_compatibility_bonus(
            combined=" ".join(part for part in (headline, summary) if part),
            resolution=parsed_market["resolution"],
        )

    # Time-frame compatibility.
    if parsed_market["end_date"] and published_at:
        score += _timeframe_compatibility_bonus(
            published_at=published_at,
            end_date=parsed_market["end_date"],
            headline=headline,
        )
    elif published_at is None:
        # Unknown publication time is a mild penalty for market-linked news.
        score -= 5

    # Staleness penalty.
    if published_at and _is_stale_article(published_at):
        score -= 8

    # Clarity/specificity penalty.
    if _is_ambiguous_or_generative(headline, summary, parsed_market):
        score -= 10

    return int(max(0, min(100, score)))


def _resolution_compatibility_bonus(combined: str, resolution: str) -> int:
    """Small bonus when article content is plausibly compatible with the resolution text.

    Deliberately small: resolution text is often sparse or noisy, so we never treat
    keyword overlap with it as decisive.
    """
    if not resolution:
        return 0
    resolution_terms = meaningful_market_terms(resolution)
    if not resolution_terms:
        return 0
    combined_terms = meaningful_market_terms(combined)
    overlap = combined_terms & resolution_terms
    if overlap:
        return min(6, len(overlap) * 2)
    return 0


def _timeframe_compatibility_bonus(
    *, published_at: datetime, end_date: datetime, headline: str
) -> int:
    """Compatibility with the market's relevant time period."""
    delta_hours = (end_date - published_at).total_seconds() / 3600
    if delta_hours < 0:
        # Article predates the market window: compatibility is low and often
        # indicates a stale match.
        return -8
    if delta_hours <= 48:
        return 8
    if delta_hours <= 720:
        return 4
    if _headline_announces_future_event(headline):
        return 6
    return 0


def _headline_announces_future_event(headline: str) -> bool:
    """True when the headline points at a scheduled, not-yet-happened event.

    Only forward-looking markers count; ambiguous temporal words like "after" or
    "by" are deliberately excluded so a past-tense story is not treated as a
    future event.
    """
    return bool(
        re.search(
            r"\b(upcoming|scheduled|planned|will|set to|expected to|not before|deadline|meeting on|vote on|decision on|session on)\b",
            headline,
            flags=re.IGNORECASE,
        )
    )


def _is_stale_article(published_at: datetime) -> bool:
    age_hours = (datetime.now(timezone.utc) - published_at).total_seconds() / 3600
    return age_hours > MIN_SOURCE_PUBLICATION_AGE_HOURS


def _is_ambiguous_or_generative(headline: str, summary: str, parsed_market: dict[str, Any]) -> bool:
    """Flag articles whose link to the market is too vague to defend."""
    combined = " ".join(part for part in (headline, summary) if part)
    vague_markers = frozenset({
        "analysts say",
        "analysts expect",
        "could",
        "may",
        "might",
        "possible",
        "speculation",
        "speculative",
    })
    if not combined:
        return False
    lower = combined.casefold()
    vague_count = sum(1 for marker in vague_markers if marker in lower)
    if vague_count >= 3:
        return True

    # If the article clearly names entities but none of them match the market, the
    # link is too vague to defend. When the article names no recoverable entities
    # (for example an all-lowercase headline) we cannot judge, so we do not add a
    # penalty on that basis alone.
    if not parsed_market["entities"]:
        return False
    article_entities = extract_named_entities(combined)
    if not article_entities:
        return False

    # If the article reports a concrete development (a word like "hike", "deal",
    # "approve", "raise") it is describing the kind of event the market resolves on,
    # so a named-entity name mismatch alone should not mark it as ambiguous.
    article_terms = meaningful_market_terms(combined)
    if article_terms & DEVELOPMENT_SIGNAL_TERMS:
        return False

    # The same exemption also applies when the article shares a development-signal
    # term with the market question itself.
    question_term_overlap = article_terms & meaningful_market_terms(parsed_market["question"])
    if question_term_overlap & DEVELOPMENT_SIGNAL_TERMS:
        return False

    if len(article_entities & parsed_market["entities"]) == 0:
        return True
    return False

def _relationship_explanation_components(
    *,
    headline: str,
    summary: str,
    source: NewsSource,
    parsed_market: dict[str, Any],
    relationship: str,
    entity_overlap: set[str],
    question_term_overlap: set[str],
    published_at: datetime | None,
) -> dict[str, str]:
    parts: dict[str, str] = {}

    if entity_overlap:
        parts["entities"] = _format_entity_list(entity_overlap)
    if question_term_overlap:
        parts["question_terms"] = _format_term_list(question_term_overlap)
    parts["relationship"] = relationship
    parts["source"] = source.name
    if parsed_market.get("outcomes"):
        parts["outcomes"] = ", ".join(parsed_market["outcomes"][:6])
    if published_at:
        parts["published"] = discord.utils.format_dt(published_at, style="R")
    return parts


def _format_entity_list(entities: set[str]) -> str:
    ordered = sorted(entities)
    if not ordered:
        return ""
    return "\n".join(f"• {entity}" for entity in ordered[:10])


def _format_term_list(terms: set[str]) -> str:
    ordered = sorted(terms)
    if not ordered:
        return ""
    return "\n".join(f"• {term}" for term in ordered[:10])


def build_match_explanation(
    *,
    article: dict[str, Any],
    headline: str,
    summary: str,
    source: NewsSource,
    parsed_market: dict[str, Any],
    relationship: str,
    score: int,
    entity_overlap: set[str],
    question_term_overlap: set[str],
    published_at: datetime | None,
) -> tuple[str, str]:
    """Return (explanation, why_short) grounded in the article and the market."""
    market_question = parsed_market["question"]
    components = _relationship_explanation_components(
        headline=headline,
        summary=summary,
        source=source,
        parsed_market=parsed_market,
        relationship=relationship,
        entity_overlap=entity_overlap,
        question_term_overlap=question_term_overlap,
        published_at=published_at,
    )

    if relationship == "Direct":
        explanation = (
            f"This article reports on a development directly relevant to this market. "
            f"The market asks whether **{market_question}**; the article describes "
            f"the same subject matter and the same decision or event the market "
            f"resolves on, so the connection is not inferred from shared keywords alone."
        )
    elif relationship == "Strong indirect":
        explanation = (
            f"This article does not resolve the market directly, but it provides "
            f"credible information with a clear, explainable relationship to the "
            f"market question: **{market_question}**. "
        )
        if entity_overlap:
            explanation += (
                f"It involves {components['entities'].lower()} "
                if components["entities"]
                else ""
            )
    else:
        explanation = (
            f"This article has only a weak or background connection to the market "
            f"question, so it is not treated as a strong match."
        )

    explanation += f"\n\n**Source:** {components['source']}"
    if components.get("published"):
        explanation += f"\n**Published:** {components['published']}"
    if components.get("entities") or components.get("question_terms"):
        explanation += "\n\n**What overlaps:**\n"
        if components.get("entities"):
            explanation += components["entities"] + "\n"
        if components.get("question_terms"):
            explanation += components["question_terms"]

    why_short = (
        f"{relationship} match"
        + (f" — {market_question}" if market_question else "")
        + f" (correlation {score}%)"
    )

    return explanation, why_short


# ---------------------------------------------------------------------------
# News source adapters
# ---------------------------------------------------------------------------


class NewsSourceAdapter:
    """An abstract interface for one news ingestion provider.

    The monitor loop talks to providers only through this interface, so a new
    provider can be plugged in without touching ``run_news_cycle``.
    """

    name: str

    async def fetch_articles(
        self, query: str
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def article_default_source_name(self, article: dict[str, Any]) -> str:
        return self.name


class NewsAPIAdapter(NewsSourceAdapter):
    """The current working provider, retained as the default and fallback.

    It keeps the existing NewsAPI ``everything`` search behavior unchanged so the
    bot continues to work with the key it already has. The correlation layer is
    applied on top of whatever this adapter returns, so relevance filtering is
    independent of the provider.
    """

    name = DEFAULT_NEWS_SOURCE_NAME

    def __init__(
        self,
        session,
        api_key: str,
        *,
        api_keys: list[str] | None = None,
        language: str | None = None,
        request_json=None,
    ) -> None:
        self._session = session
        self.api_key = api_key
        # The configured key list, in the order the keys are tried.
        self.api_keys = list(api_keys or [])
        self.language = language or os.getenv("NEWS_LANGUAGE", "en")
        self._request_json = request_json

    def candidate_keys(self) -> list[str]:
        """Every key to try: the primary first, blanks and repeats removed."""
        keys: list[str] = []
        for key in [self.api_key, *self.api_keys]:
            if isinstance(key, str) and key and key not in keys:
                keys.append(key)
        return keys

    async def _request_page(self, params: dict[str, Any], key: str) -> Any:
        headers = {"X-Api-Key": key}
        if self._request_json is not None:
            return await self._request_json(
                NEWS_API_URL, params=params, headers=headers
            )
        return await self._session.get_json(
            NEWS_API_URL, params=params, headers=headers
        )

    async def fetch_articles(self, query: str) -> list[dict[str, Any]]:
        """Fetch articles, falling through to the spare keys when throttled.

        A single NewsAPI key is limited to 100 requests a day, which one
        monitoring loop can exhaust, so a rejected key must not fail the whole
        poll while another key is configured. The final key's error is raised.
        """
        keys = self.candidate_keys()
        if not keys:
            raise RuntimeError("NewsAPI key is not configured.")

        for index, key in enumerate(keys):
            try:
                return await self._fetch_with_key(query, key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if index == len(keys) - 1:
                    raise
                logger.warning(
                    "NewsAPI key %d of %d was rejected (%s); trying the next "
                    "key.",
                    index + 1,
                    len(keys),
                    exc,
                )
        raise RuntimeError("NewsAPI request failed.")

    async def _fetch_with_key(self, query: str, key: str) -> list[dict[str, Any]]:
        params = {
            "q": query,
            "language": self.language,
            "sortBy": "publishedAt",
            "pageSize": 20,
        }
        payload = await self._request_page(params, key)
        if not isinstance(payload, dict):
            raise RuntimeError("NewsAPI returned an unexpected response body.")
        if payload.get("status") != "ok":
            message = payload.get("message", "NewsAPI returned an error.")
            raise RuntimeError(str(message))
        articles = payload.get("articles", [])
        if not isinstance(articles, list):
            raise RuntimeError("NewsAPI returned an unexpected articles value.")
        return articles


def build_news_provider(
    session,
    *,
    api_key: str,
    provider_name: str = DEFAULT_NEWS_SOURCE_NAME,
    api_keys: list[str] | None = None,
    language: str | None = None,
) -> NewsSourceAdapter:
    """Build the active news provider from configuration.

    Today this returns NewsAPI when the configured provider is NewsAPI or when no
    provider is specified. The shape is intentionally narrow: adding another
    provider requires wiring an adapter and a key, not editing the monitor loop.
    """
    name = provider_name.strip()
    if name and name.lower() != DEFAULT_NEWS_SOURCE_NAME.lower():
        raise RuntimeError(
            f"News provider '{name}' is not configured. Only '{DEFAULT_NEWS_SOURCE_NAME}' "
            "is supported without additional setup."
        )
    return NewsAPIAdapter(
        session, api_key, api_keys=api_keys, language=language
    )


def select_polymarket_market(
    matches: list[MarketMatch],
) -> list[MarketMatch]:
    """Rank evaluated matches and return the best few for the alert.

    This is deliberately separate from the search step: it does not call Polymarket
    again. It just keeps the strongest qualifying matches, up to
    ``MAX_RELATED_MARKETS``, so the embed never becomes a long list of weak
    connections.
    """
    if not matches:
        return []

    ranked = sorted(
        matches,
        key=lambda match: (match.score, match.relationship != "Direct"),
        reverse=True,
    )
    # The policy gate: only matches the configured thresholds allow may post.
    # Everything else stays available for diagnostics and tuning, but it must
    # never ride along on an alert, so nothing is substituted in its place.
    return [
        match for match in ranked if is_alertable(match.relationship, match.score)
    ][:MAX_RELATED_MARKETS]


def _market_price_label(market: dict[str, Any]) -> str:
    """A compact price label for one market, e.g. ``$0.50``, or "" if unknown."""
    prices = parse_outcome_prices(market)
    if prices:
        return f"${prices[0]:.2f}"
    last = as_float(market.get("lastTradePrice"))
    if last is not None:
        return f"${last:.2f}"
    return ""


def _related_markets_field(matches: list[MarketMatch]) -> str:
    """Format the Related Markets embed field: one compact line per market.

    Each line follows the reference Market Intel design --

        ``\u2022 [Polymarket] <market question> | $0.50 (correlation: 75%)``

    i.e. platform, question, the market's current price when one is available,
    and the correlation score. No political alignment is included.
    """
    if not matches:
        return "No qualifying market correlation this cycle."

    lines: list[str] = []
    for match in matches:
        market = match.market
        question = clean_article_text(market.get("question")) or "Market question unavailable"
        url = market.get("url")
        if not (isinstance(url, str) and url.startswith("https://")):
            slug = market.get("slug")
            url = (
                f"{POLYMARKET_MARKET_PAGE}/{quote(str(slug).strip(), safe='-_')}"
                if isinstance(slug, str) and slug.strip()
                else ""
            )
        market_link = f"[Polymarket]({url})" if url else "Polymarket"
        details = " ".join(
            part
            for part in (_market_price_label(market), f"(correlation: {match.score}%)")
            if part
        )
        entry = [f"• [{market_link}] {question} | {details}"]

        # Keep whole market entries: an entry that would overflow the field is
        # dropped rather than cut in half, so every market shown stays readable.
        if lines and len("\n".join(lines + entry)) > EMBED_FIELD_VALUE_LIMIT:
            logger.debug("Related Markets field is full; later markets omitted.")
            break
        lines.extend(entry)

    field_text = "\n".join(lines)
    if len(field_text) > EMBED_FIELD_VALUE_LIMIT:
        logger.debug(
            "A single Related Markets entry exceeded %d characters and was clamped.",
            EMBED_FIELD_VALUE_LIMIT,
        )
    return fit_embed_text(field_text, EMBED_FIELD_VALUE_LIMIT)


def fit_embed_text(value: Any, limit: int) -> str:
    """Clamp one embed string to a Discord hard limit, leaving it intact when it fits."""
    text = "" if value is None else str(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def sanitize_embed(embed: discord.Embed) -> discord.Embed:
    """Clamp a built embed to Discord's hard limits so a long alert still posts.

    Discord rejects the entire message with "Invalid Form Body" when a single
    embed string is over its limit, which would silently drop the alert. Clamping
    at the single send boundary trims the text instead of losing the message.
    """
    embed.title = fit_embed_text(embed.title, EMBED_TITLE_LIMIT) or None
    embed.description = fit_embed_text(embed.description, EMBED_DESCRIPTION_LIMIT) or None
    clamped_fields: list[tuple[str, str, bool]] = []
    for field in embed.fields:
        if len(clamped_fields) >= EMBED_FIELD_COUNT_LIMIT:
            break
        clamped_fields.append(
            (
                fit_embed_text(field.name, EMBED_FIELD_NAME_LIMIT),
                fit_embed_text(field.value, EMBED_FIELD_VALUE_LIMIT),
                field.inline,
            )
        )
    embed.clear_fields()
    for name, value, inline in clamped_fields:
        embed.add_field(name=name or EMPTY_EMBED_TEXT, value=value or EMPTY_EMBED_TEXT, inline=inline)

    footer = embed.footer
    if footer is not None and footer.text:
        embed.set_footer(
            text=fit_embed_text(footer.text, EMBED_FOOTER_LIMIT),
            icon_url=footer.icon_url,
        )

    author = embed.author
    if author is not None and author.name:
        embed.set_author(
            name=fit_embed_text(author.name, EMBED_AUTHOR_LIMIT),
            url=author.url,
            icon_url=author.icon_url,
        )

    return embed


# ---------------------------------------------------------------------------
# Per-server settings and cross-restart monitor state
# ---------------------------------------------------------------------------


def parse_outcome_prices(market: dict[str, Any]) -> list[float]:
    """Parse Gamma's JSON-encoded `outcomePrices` string into floats."""
    raw = market.get("outcomePrices")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return []
    if not isinstance(raw, list):
        return []

    prices: list[float] = []
    for value in raw:
        number = as_float(value)
        if number is None:
            return []
        prices.append(number)
    return prices


def parse_outcomes(market: dict[str, Any]) -> list[str]:
    """Parse Gamma's JSON-encoded `outcomes` string into labels."""
    raw = market.get("outcomes")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return []
    if not isinstance(raw, list):
        return []
    return [str(value) for value in raw]


def default_guild_settings() -> dict[str, Any]:
    """The full set of per-server defaults (always the same shape)."""
    return {
        "channel_id": None,
        # Per-category channel overrides; None means "use the default channel".
        "channels": {alert: None for alert in ALERT_TYPES},
        "include_keywords": [],
        "exclude_keywords": [],
        "sources": [],
        "alerts": {alert: True for alert in ALERT_TYPES},
        "price_move_threshold": DEFAULT_PRICE_MOVE_THRESHOLD,
        "whale_min_usd": DEFAULT_WHALE_MIN_USD,
        "new_market_min_liquidity": DEFAULT_NEW_MARKET_MIN_LIQUIDITY,
    }


def normalize_guild_settings(raw: Any) -> dict[str, Any]:
    """Coerce one server's stored settings into a known-good shape.

    Anything missing, mistyped, or out of range falls back to the default so a
    hand-edited or half-written settings file cannot crash the bot.
    """
    settings = default_guild_settings()
    if not isinstance(raw, dict):
        return settings

    channel_id = raw.get("channel_id")
    if isinstance(channel_id, int) and not isinstance(channel_id, bool) and channel_id > 0:
        settings["channel_id"] = channel_id

    channels = raw.get("channels")
    if isinstance(channels, dict):
        for alert in ALERT_TYPES:
            candidate = channels.get(alert)
            if (
                isinstance(candidate, int)
                and not isinstance(candidate, bool)
                and candidate > 0
            ):
                settings["channels"][alert] = candidate

    for key, limit in (
        ("include_keywords", MAX_KEYWORDS_PER_GUILD),
        ("exclude_keywords", MAX_KEYWORDS_PER_GUILD),
        ("sources", MAX_SOURCES_PER_GUILD),
    ):
        values = raw.get(key)
        if isinstance(values, list):
            settings[key] = dedupe_strings(values)[:limit]

    alerts = raw.get("alerts")
    if isinstance(alerts, dict):
        for alert in ALERT_TYPES:
            if isinstance(alerts.get(alert), bool):
                settings["alerts"][alert] = alerts[alert]

    for key, low, high in (
        ("price_move_threshold", MIN_PRICE_MOVE_THRESHOLD, MAX_PRICE_MOVE_THRESHOLD),
        ("whale_min_usd", MIN_WHALE_MIN_USD, 10_000_000.0),
        ("new_market_min_liquidity", 0.0, 10_000_000.0),
    ):
        number = as_float(raw.get(key))
        if number is not None:
            settings[key] = clamp(number, low, high)

    return settings


def effective_channel_id(
    settings: dict[str, Any], alert: str | None = None
) -> int | None:
    """The channel one alert category posts to, or None when unconfigured.

    A category's own channel wins; otherwise the server's default channel is
    used. ``alert=None`` asks for the default channel directly.
    """
    channels = settings.get("channels")
    if alert is not None and isinstance(channels, dict):
        override = channels.get(alert)
        if (
            isinstance(override, int)
            and not isinstance(override, bool)
            and override > 0
        ):
            return override
    default = settings.get("channel_id")
    if isinstance(default, int) and not isinstance(default, bool) and default > 0:
        return default
    return None


def parse_news_api_keys(raw: str | None, primary: str | None) -> list[str]:
    """Parse extra NewsAPI keys, tried in order when the primary is throttled.

    ``NEWS_API_KEYS`` is comma or semicolon separated. Blanks, repeats, and the
    primary key itself are dropped so no key is ever tried twice.
    """
    keys: list[str] = []
    for chunk in re.split(r"[,;]", raw or ""):
        key = chunk.strip()
        if not key or key == primary or key in keys:
            continue
        keys.append(key)
    return keys


def load_guild_settings() -> dict[str, dict[str, Any]]:
    """Load every server's settings, seeding from the legacy channel file."""
    settings: dict[str, dict[str, Any]] = {}
    try:
        data = json.loads(GUILD_SETTINGS_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        data = {}
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read saved server settings; starting fresh.")
        data = {}

    if isinstance(data, dict):
        for guild_id, raw in data.items():
            if isinstance(guild_id, str) and guild_id.isdigit():
                settings[guild_id] = normalize_guild_settings(raw)
    elif data:
        logger.warning("Saved server settings have an unexpected format.")

    # Backward compatibility: servers configured before settings existed only
    # have an alert channel in guild_channels.json.
    for guild_id, channel_id in load_guild_channels().items():
        settings.setdefault(guild_id, default_guild_settings())
        if settings[guild_id]["channel_id"] is None:
            settings[guild_id]["channel_id"] = channel_id

    return settings


def save_guild_settings(settings: dict[str, dict[str, Any]]) -> None:
    write_json_atomically(GUILD_SETTINGS_FILE, settings)


def load_monitor_state() -> dict[str, Any]:
    """Load cross-restart dedupe state for markets, trades, and prices."""
    state: dict[str, Any] = {
        "seen_market_ids": [],
        "seen_trades": [],
        "last_prices": {},
        # Seeding is tracked explicitly rather than inferred from an empty
        # seen-set: if the first fetch legitimately returns nothing, inferring it
        # would swallow the next real alert instead of just the initial backlog.
        "new_markets_seeded": False,
        "whale_seeded": False,
    }
    try:
        data = json.loads(MONITOR_STATE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return state
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read saved monitor state; starting fresh.")
        return state

    if not isinstance(data, dict):
        return state

    for key in ("new_markets_seeded", "whale_seeded"):
        if isinstance(data.get(key), bool):
            state[key] = data[key]

    ids = data.get("seen_market_ids")
    if isinstance(ids, list):
        state["seen_market_ids"] = [
            str(value) for value in ids if isinstance(value, (str, int))
        ][-MAX_SEEN_MARKETS:]

    trades = data.get("seen_trades")
    if isinstance(trades, list):
        state["seen_trades"] = [
            value for value in trades if isinstance(value, str)
        ][-MAX_SEEN_TRADES:]

    prices = data.get("last_prices")
    if isinstance(prices, dict):
        clean_prices: dict[str, dict[str, float]] = {}
        for market_id, value in prices.items():
            if not isinstance(value, dict):
                continue
            price = as_float(value.get("price"))
            if price is None:
                continue
            alerted = as_float(value.get("alerted"))
            clean_prices[str(market_id)] = {
                "price": price,
                "alerted": price if alerted is None else alerted,
            }
        state["last_prices"] = clean_prices

    return state


def save_monitor_state(state: dict[str, Any]) -> None:
    write_json_atomically(MONITOR_STATE_FILE, state)

# ---------------------------------------------------------------------------
# Wallet watchlist storage
# ---------------------------------------------------------------------------


def normalize_wallet_address(value: Any) -> str | None:
    """Return the canonical (lower-case) watch address, or None when invalid.

    Polymarket accounts are proxy wallets, so an address must be a plain EVM
    0x address. A profile name, a truncated address, or a market slug is
    rejected instead of being stored as something that can never match.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not WALLET_ADDRESS_PATTERN.match(candidate):
        return None
    return candidate.lower()


def short_wallet_address(address: str) -> str:
    """A readable address for embeds: 0x1234…abcd."""
    if len(address) > 12:
        return f"{address[:6]}\u2026{address[-4:]}"
    return address


def default_wallet_watch(address: str) -> dict[str, Any]:
    """The full shape of one tracked wallet (always the same keys)."""
    return {
        "address": address,
        "label": "",
        "min_usd": DEFAULT_WALLET_MIN_USD,
        "activity_filter": "trades",
        "paused": False,
        # A tracked wallet's fill would otherwise alert twice: once through the
        # global whale monitor and once through the wallet alert. Suppressing
        # the whale copy is the default; /wallet settings can turn it off.
        "suppress_whale": True,
        "last_seen_ts": 0.0,
        "seen": [],
    }


def normalize_wallet_watch(address: Any, raw: Any) -> dict[str, Any] | None:
    """Coerce one stored watch into a known-good shape, or drop it."""
    normalized_address = normalize_wallet_address(address)
    if normalized_address is None:
        return None
    watch = default_wallet_watch(normalized_address)
    if not isinstance(raw, dict):
        return watch

    label = clean_article_text(raw.get("label"))[:MAX_WALLET_LABEL_LENGTH].strip()
    if label:
        watch["label"] = label
    minimum = as_float(raw.get("min_usd"))
    if minimum is not None:
        watch["min_usd"] = clamp(minimum, MIN_WALLET_MIN_USD, MAX_WALLET_MIN_USD)
    activity_filter = raw.get("activity_filter")
    if isinstance(activity_filter, str) and activity_filter in WALLET_ACTIVITY_FILTERS:
        watch["activity_filter"] = activity_filter
    if isinstance(raw.get("paused"), bool):
        watch["paused"] = raw["paused"]
    if isinstance(raw.get("suppress_whale"), bool):
        watch["suppress_whale"] = raw["suppress_whale"]
    last_seen = as_float(raw.get("last_seen_ts"))
    if last_seen is not None and last_seen >= 0:
        watch["last_seen_ts"] = last_seen
    seen = raw.get("seen")
    if isinstance(seen, list):
        watch["seen"] = dedupe_strings(seen)[-MAX_WALLET_SEEN_EVENTS:]
    return watch


def normalize_wallet_state(raw: Any) -> dict[str, dict[str, Any]]:
    """Coerce the whole watchlist file; a corrupt entry never breaks startup."""
    state: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return state
    for guild_id, guild_raw in raw.items():
        if not isinstance(guild_id, str) or not guild_id.isdigit():
            continue
        watches_raw = (
            guild_raw.get("watches") if isinstance(guild_raw, dict) else None
        )
        if not isinstance(watches_raw, dict):
            continue
        watches: dict[str, dict[str, Any]] = {}
        for address, watch_raw in watches_raw.items():
            watch = normalize_wallet_watch(address, watch_raw)
            if watch is not None:
                watches[watch["address"]] = watch
        state[guild_id] = {"watches": watches}
    return state


def load_wallet_state() -> dict[str, dict[str, Any]]:
    try:
        data = json.loads(WALLETS_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read the wallet watchlist file; starting fresh.")
        return {}
    return normalize_wallet_state(data)


def save_wallet_state(state: dict[str, dict[str, Any]]) -> None:
    write_json_atomically(WALLETS_FILE, state)


# ---------------------------------------------------------------------------
# X/Twitter account watchlist storage
# ---------------------------------------------------------------------------


class XApiError(RuntimeError):
    """An X API failure, carrying the HTTP status and a user-safe message.

    ``transient`` distinguishes "try again next cycle" (rate limits, 5xx,
    network trouble, or the account not being resolvable right now) from a
    persistent problem the operator must fix (an invalid API token) or a
    permanent per-account condition (account not found).
    """

    def __init__(
        self, status: int | None, message: str, *, transient: bool = True
    ) -> None:
        super().__init__(message)
        self.status = status
        self.transient = transient


def normalize_x_username(value: Any) -> str | None:
    """Return the canonical lower-case handle, or None when it is not one.

    Accepts ``@name``, ``name`` and ``https://x.com/name`` forms. X handles
    are 1-15 characters of letters, digits and underscores; anything else is
    rejected instead of stored as something the API can never resolve.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    url_match = re.search(
        r"(?:https?://)?(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})/?\s*$",
        candidate,
        re.IGNORECASE,
    )
    if url_match:
        candidate = url_match.group(1)
    candidate = candidate[1:] if candidate.startswith("@") else candidate
    candidate = candidate.strip()
    if not X_USERNAME_PATTERN.fullmatch(candidate):
        return None
    return candidate.lower()


def default_x_account(username: str) -> dict[str, Any]:
    """The full shape of one tracked X account (always the same keys)."""
    return {
        "username": username,
        # The numeric user id is the account's stable identity; a handle can
        # be renamed but the id never changes. Empty until first resolved.
        "user_id": "",
        "display_name": "",
        "profile_image_url": "",
        "label": "",
        "paused": False,
        "activity_filter": "all",
        "include_keywords": [],
        "exclude_keywords": [],
        # Optional per-account channel override; None uses the server's
        # x-alert channel (or its default channel).
        "channel_id": None,
        # Snowflake of the newest post already handled. X returns only posts
        # newer than this, so restarts never replay history.
        "since_id": "",
        "seen": [],
        "last_success_ts": 0.0,
        "last_error": "",
    }


def normalize_x_account(username: Any, raw: Any) -> dict[str, Any] | None:
    """Coerce one stored account into a known-good shape, or drop it."""
    normalized = normalize_x_username(username)
    if normalized is None:
        return None
    account = default_x_account(normalized)
    if not isinstance(raw, dict):
        return account

    # Keep the stored handle's original casing for display.
    stored_handle = raw.get("username")
    if isinstance(stored_handle, str) and stored_handle.strip():
        account["username"] = stored_handle.strip().lstrip("@")[:15]
    user_id = raw.get("user_id")
    if isinstance(user_id, str) and user_id.isdigit():
        account["user_id"] = user_id
    for key in ("display_name", "profile_image_url"):
        value = clean_article_text(raw.get(key))[:200]
        if value:
            account[key] = value
    label = clean_article_text(raw.get("label"))[:MAX_X_LABEL_LENGTH].strip()
    if label:
        account["label"] = label
    if isinstance(raw.get("paused"), bool):
        account["paused"] = raw["paused"]
    activity_filter = raw.get("activity_filter")
    if isinstance(activity_filter, str) and activity_filter in X_ACTIVITY_FILTERS:
        account["activity_filter"] = activity_filter
    for key in ("include_keywords", "exclude_keywords"):
        values = raw.get(key)
        if isinstance(values, list):
            account[key] = dedupe_strings(values)[:MAX_X_KEYWORDS_PER_ACCOUNT]
    channel_id = raw.get("channel_id")
    if (
        isinstance(channel_id, int)
        and not isinstance(channel_id, bool)
        and channel_id > 0
    ):
        account["channel_id"] = channel_id
    since_id = raw.get("since_id")
    if isinstance(since_id, (str, int)) and str(since_id).isdigit():
        account["since_id"] = str(since_id)
    seen = raw.get("seen")
    if isinstance(seen, list):
        account["seen"] = [
            str(value)
            for value in seen
            if isinstance(value, (str, int)) and str(value).isdigit()
        ][-MAX_X_SEEN_POSTS:]
    success = as_float(raw.get("last_success_ts"))
    if success is not None and success >= 0:
        account["last_success_ts"] = success
    last_error = clean_article_text(raw.get("last_error"))[:200]
    if last_error:
        account["last_error"] = last_error
    return account


def normalize_x_state(raw: Any) -> dict[str, dict[str, Any]]:
    """Coerce the whole x-tracked file; a corrupt entry never breaks startup."""
    state: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return state
    for guild_id, guild_raw in raw.items():
        if not isinstance(guild_id, str) or not guild_id.isdigit():
            continue
        accounts_raw = (
            guild_raw.get("accounts") if isinstance(guild_raw, dict) else None
        )
        if not isinstance(accounts_raw, dict):
            continue
        accounts: dict[str, dict[str, Any]] = {}
        for username, raw_account in accounts_raw.items():
            account = normalize_x_account(username, raw_account)
            if account is not None:
                accounts[account["username"].casefold()] = account
        state[guild_id] = {"accounts": accounts}
    return state


def load_x_state() -> dict[str, dict[str, Any]]:
    try:
        data = json.loads(X_TRACKED_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read the X watchlist file; starting fresh.")
        return {}
    return normalize_x_state(data)


def save_x_state(state: dict[str, dict[str, Any]]) -> None:
    write_json_atomically(X_TRACKED_FILE, state)


def x_post_activity(post: dict[str, Any]) -> str:
    """Classify one post as post / reply / quote / repost from its references."""
    refs = post.get("referenced_tweets")
    kinds = (
        {ref.get("type") for ref in refs if isinstance(ref, dict)}
        if isinstance(refs, list)
        else set()
    )
    if "retweeted" in kinds:
        return "repost"
    if "quoted" in kinds:
        return "quote"
    if "replied_to" in kinds:
        return "reply"
    return "post"


def x_post_is_newer(post_id: Any, since_id: str) -> bool:
    """True when this snowflake is newer than the stored high-water mark.

    X post ids are snowflakes: numeric strings that grow over time, so the
    comparison is integer, not lexicographic ("9" sorts after "10" as text).
    """
    if not isinstance(post_id, str) or not post_id.isdigit():
        return False
    if not since_id:
        return True
    try:
        return int(post_id) > int(since_id)
    except ValueError:
        return True


def x_post_passes_filters(
    account: dict[str, Any], post: dict[str, Any], activity: str
) -> bool:
    """Apply one account's activity and keyword filters to one post.

    Filters run on the retrieved post text (the X API has no server-side
    keyword filtering for timelines). Excluded words always win; when include
    keywords exist, at least one must appear. Matching is case-insensitive
    substring matching, so "$btc" and "btc" both match "BTC".
    """
    activity_filter = account.get("activity_filter", "all")
    if activity_filter == "originals" and activity != "post":
        return False
    if activity_filter == "no_replies" and activity == "reply":
        return False

    text = clean_article_text(post.get("text")).casefold()
    excludes = [
        keyword.casefold() for keyword in account.get("exclude_keywords") or []
    ]
    if any(word in text for word in excludes):
        return False
    includes = [
        keyword.casefold() for keyword in account.get("include_keywords") or []
    ]
    if includes and not any(word in text for word in includes):
        return False
    return True


def x_cashtag_symbols(text: str) -> list[str]:
    """Distinct $TICKER symbols mentioned in a post, in first-seen order.

    A cashtag is a cheap, factual hint that a post concerns a token. It is
    used only to offer Polymarket search links; it never claims the post
    predicts or moves any market.
    """
    seen: list[str] = []
    for match in re.finditer(r"\$([A-Za-z]{2,10})\b", text or ""):
        symbol = match.group(1).upper()
        if symbol not in seen:
            seen.append(symbol)
    return seen[:6]


def parse_keyword_list(raw: str | None) -> list[str] | None:
    """Split a comma/semicolon-separated keyword string, or None when empty."""
    if not raw:
        return None
    values = dedupe_strings(re.split(r"[,;]", raw))
    return values or None


def apply_x_account_options(
    account: dict[str, Any],
    *,
    label: str | None = None,
    activity_filter: str | None = None,
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
    channel: Any = None,
) -> dict[str, Any]:
    """Apply the options a command provided; None means "leave unchanged"."""
    if label is not None:
        account["label"] = label.strip()[:MAX_X_LABEL_LENGTH]
    if activity_filter is not None and activity_filter in X_ACTIVITY_FILTERS:
        account["activity_filter"] = activity_filter
    if include_keywords is not None:
        account["include_keywords"] = include_keywords[:MAX_X_KEYWORDS_PER_ACCOUNT]
    if exclude_keywords is not None:
        account["exclude_keywords"] = exclude_keywords[:MAX_X_KEYWORDS_PER_ACCOUNT]
    if channel is not None:
        account["channel_id"] = getattr(channel, "id", None)
    return account


def describe_x_account(account: dict[str, Any]) -> str:
    """One readable line for a tracked account."""
    handle = f"@{account['username']}"
    label = clean_article_text(account.get("label"))
    name = (
        f" (`{label}`)"
        if label and label.casefold() != account["username"].casefold()
        else ""
    )
    state = " [paused]" if account.get("paused") else ""
    return f"**{handle}**{name}{state}"


def wallet_activity_type(event: dict[str, Any]) -> str:
    return clean_article_text(
        event.get("type") or event.get("activityType")
    ).upper() or "OTHER"


def wallet_event_value(event: dict[str, Any]) -> float | None:
    """The transaction value in USDC.

    ``usdcSize`` is the settled cash amount the Data API reports for a wallet
    activity row. It is the trade's value, not the wallet's position size.
    """
    for key in ("usdcSize", "usdc_size", "value"):
        number = as_float(event.get(key))
        if number is not None:
            return number
    size = as_float(event.get("size"))
    price = as_float(event.get("price"))
    if size is None or price is None:
        return None
    return size * price


def wallet_activity_key(event: dict[str, Any]) -> str | None:
    """A stable identity for one activity row so it is never alerted twice."""
    transaction = event.get("transactionHash") or event.get("transaction_hash")
    if not transaction:
        return None
    parts = (
        transaction,
        event.get("type") or event.get("activityType"),
        event.get("asset") or event.get("token_id"),
        event.get("side"),
        event.get("size"),
        event.get("price"),
        event.get("timestamp"),
    )
    return "|".join(str(part) for part in parts)


def wallet_event_market_url(event: dict[str, Any]) -> str:
    slug = event.get("slug") or event.get("eventSlug")
    if isinstance(slug, str) and slug.strip():
        return f"{POLYMARKET_MARKET_PAGE}/{quote(slug.strip(), safe='-_')}"
    return "https://polymarket.com"


def describe_wallet_watch(watch: dict[str, Any]) -> str:
    """A one-line human summary of a watch, used by /wallet list and info."""
    label = watch.get("label") or short_wallet_address(watch["address"])
    state = "paused" if watch.get("paused") else "active"
    threshold = watch.get("min_usd") or 0.0
    activity = WALLET_ACTIVITY_FILTER_LABELS.get(
        watch.get("activity_filter", "all"), "All activity"
    )
    return (
        f"**{label}** \u2014 `{short_wallet_address(watch['address'])}` \u00b7 {state} "
        f"\u00b7 {activity} \u00b7 min ${threshold:,.0f}"
    )


# ---------------------------------------------------------------------------
# /chart: Polymarket price history and PNG rendering
# ---------------------------------------------------------------------------


def parse_price_history(payload: Any) -> list[tuple[int, float]]:
    """Parse a CLOB prices-history payload into sorted (timestamp, price) points.

    The series is trusted as a whole: one malformed point makes the chart
    unfaithful, so this returns an empty list rather than a partial line.
    """
    if not isinstance(payload, dict):
        return []
    history = payload.get("history")
    if not isinstance(history, list):
        return []
    points: list[tuple[int, float]] = []
    for entry in history:
        if not isinstance(entry, dict):
            return []
        timestamp = entry.get("t")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            return []
        price = as_float(entry.get("p"))
        if price is None:
            return []
        points.append((int(timestamp), price))
    points.sort(key=lambda point: point[0])
    return points


def first_clob_token_id(market: dict[str, Any]) -> str | None:
    """The first CLOB token id of a market (Gamma returns a JSON-encoded list)."""
    raw = market.get("clobTokenIds")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return None
    if not isinstance(raw, list):
        return None
    for token in raw:
        if isinstance(token, str) and token.strip():
            return token.strip()
        if isinstance(token, int) and not isinstance(token, bool):
            return str(token)
    return None


def chart_interval(days: int) -> str:
    """Map a lookback in days onto the CLOB's supported interval values."""
    if days <= 1:
        return "1d"
    if days <= 7:
        return "1w"
    if days <= 30:
        return "1m"
    return "max"


# CLOB "fidelity" is the resolution in minutes, and the API enforces a
# minimum per interval (1w rejects anything under 5, 1m rejects anything
# under 10). At its minimum a 1m window returns thousands of points, so the
# first value is the readable resolution and the second is the finest the
# API accepts, used as a fallback for markets with very little history.
CHART_FIDELITY_MINUTES = {
    "1d": (5, 1),
    "1w": (60, 5),
    "1m": (180, 10),
    "max": (1440, 60),
}


def render_price_chart(
    *,
    question: str,
    slug: str,
    outcome_label: str,
    points: list[tuple[int, float]],
) -> bytes:
    """Render one market's price history as a PNG in the Sovereign palette.

    matplotlib is imported here rather than at module scope so a broken install
    disables /chart instead of stopping the whole bot from starting.
    """
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    times = [datetime.fromtimestamp(point[0], tz=timezone.utc) for point in points]
    odds = [point[1] * 100.0 for point in points]

    figure, axes = plt.subplots(figsize=(8.0, 4.0), dpi=140)
    figure.patch.set_facecolor("#12100E")
    axes.set_facecolor("#12100E")
    axes.plot(times, odds, color="#C6A15B", linewidth=2.0)
    axes.fill_between(times, odds, min(odds), color="#C6A15B", alpha=0.12)
    axes.set_title(
        fit_embed_text(f"{question} | {outcome_label}", 110),
        color="#F3E4C2",
        fontsize=11,
    )
    axes.set_ylabel("Implied probability (%)", color="#F3E4C2", fontsize=9)
    axes.tick_params(colors="#C6A15B", labelsize=8)
    for spine in axes.spines.values():
        spine.set_color("#3A332A")
    axes.grid(color="#3A332A", linestyle=":", linewidth=0.6, alpha=0.6)
    axes.set_xlabel(slug, color="#8A7F6B", fontsize=8)
    figure.autofmt_xdate(rotation=20)
    figure.tight_layout()
    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", facecolor=figure.get_facecolor())
    plt.close(figure)
    return buffer.getvalue()


def apply_watch_options(
    watch: dict[str, Any],
    *,
    label: str | None = None,
    min_usd: float | None = None,
    activity_filter: str | None = None,
    suppress_whale: bool | None = None,
) -> dict[str, Any]:
    """Merge user-supplied /wallet options onto a watch, clamped and coerced."""
    if label is not None:
        watch["label"] = clean_article_text(label)[:MAX_WALLET_LABEL_LENGTH].strip()
    if min_usd is not None:
        watch["min_usd"] = clamp(min_usd, MIN_WALLET_MIN_USD, MAX_WALLET_MIN_USD)
    if activity_filter is not None and activity_filter in WALLET_ACTIVITY_FILTERS:
        watch["activity_filter"] = activity_filter
    if suppress_whale is not None:
        watch["suppress_whale"] = bool(suppress_whale)
    return watch


class WalletCommands(app_commands.Group):
    """The /wallet watchlist command group.

    Every subcommand is a method on this group rather than on the bot, because a
    discord.py group owns its children: the group copies each child command and
    binds it to the group instance, so ``self`` is the group and ``self.bot`` is
    the bot. Commands declared on the bot itself are top-level, so they cannot be
    nested under a group name.
    """

    def __init__(self, bot: "LashNewsBot") -> None:
        self.bot = bot
        super().__init__(
            name="wallet",
            description="Track Polymarket wallets and alert on their activity.",
            guild_only=True,
        )

    @app_commands.command(
        name="add",
        description="Track a Polymarket wallet and alert on its activity here.",
    )
    @app_commands.describe(
        address="The wallet address to watch (0x followed by 40 hex characters).",
        label="A nickname shown in alerts (optional).",
        min_usd="Only alert on activity worth at least this much (USDC).",
        activity="Which activity rows should generate an alert.",
    )
    @app_commands.choices(
        activity=[
            app_commands.Choice(name=WALLET_ACTIVITY_FILTER_LABELS[key], value=key)
            for key in WALLET_ACTIVITY_FILTERS
        ]
    )
    async def add(
        self,
        interaction: discord.Interaction,
        address: str,
        label: str | None = None,
        min_usd: float | None = None,
        activity: app_commands.Choice[str] | None = None,
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        normalized = normalize_wallet_address(address)
        if normalized is None:
            await interaction.response.send_message(
                "That is not a wallet address. Send the Polymarket proxy wallet "
                "as `0x` followed by 40 hex characters (for example "
                "`0x5fdd66ffd0325d4e8d1d86fbec50caa31770a747`).",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        if bot.get_watch(guild.id, normalized) is not None:
            await interaction.followup.send(
                f"`{short_wallet_address(normalized)}` is already tracked in this "
                "server. Use `/wallet info` to see it, or `/wallet settings` to "
                "change it.",
                ephemeral=True,
            )
            return
        if bot.watch_limit_reached(guild.id):
            await interaction.followup.send(
                f"This server already tracks the maximum of "
                f"{MAX_TRACKED_WALLETS_PER_GUILD} wallets. Remove one with "
                "`/wallet remove` first.",
                ephemeral=True,
            )
            return

        watch = apply_watch_options(
            default_wallet_watch(normalized),
            label=label,
            min_usd=min_usd,
            activity_filter=activity.value if activity is not None else None,
        )
        if not bot.set_watch(guild.id, watch):
            await interaction.followup.send(
                "I couldn't save this server's watchlist.", ephemeral=True
            )
            return

        note = ""
        try:
            events = await bot.fetch_wallet_activity(normalized)
        except Exception as exc:
            logger.warning(
                "Could not confirm wallet %s with Polymarket: %s",
                short_wallet_address(normalized),
                exc,
            )
            note = "\nPolymarket could not be reached to confirm it just now."
        else:
            if not events:
                note = (
                    "\nPolymarket reports no activity for that address, so it may "
                    "not be a Polymarket account yet. It is still tracked."
                )
        await interaction.followup.send(
            f"Tracking {describe_wallet_watch(watch)}.\n"
            "The first check only records a baseline, so existing history is not "
            "posted; new activity alerts from the next cycle." + note,
            ephemeral=True,
        )

    @app_commands.command(
        name="remove", description="Stop tracking a wallet in this server."
    )
    @app_commands.describe(address="The wallet address to stop tracking.")
    async def remove(self, interaction: discord.Interaction, address: str) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        normalized = normalize_wallet_address(address)
        if normalized is None:
            await interaction.response.send_message(
                "That is not a wallet address (`0x` plus 40 hex characters).",
                ephemeral=True,
            )
            return
        if not bot.drop_watch(guild.id, normalized):
            await interaction.response.send_message(
                f"`{short_wallet_address(normalized)}` is not tracked in this "
                "server. Use `/wallet list` to see what is.",
                ephemeral=True,
            )
            return
        bot.save_wallets()
        bot.prune_empty_guild_wallets(guild.id)
        await interaction.response.send_message(
            f"Stopped tracking `{short_wallet_address(normalized)}`.",
            ephemeral=True,
        )

    @app_commands.command(
        name="list", description="List the wallets this server tracks."
    )
    async def list_wallets(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/wallet list` inside a Discord server.", ephemeral=True
            )
            return
        watches = bot.list_watches(guild.id)
        if not watches:
            await interaction.response.send_message(
                "This server is not tracking any wallets yet. An administrator "
                "can add one with `/wallet add`.",
                ephemeral=True,
            )
            return
        last_check = (
            discord.utils.format_dt(bot.last_wallet_check_at, style="R")
            if bot.last_wallet_check_at is not None
            else "not yet"
        )
        lines = [
            f"**Tracked wallets** ({len(watches)}/{MAX_TRACKED_WALLETS_PER_GUILD})"
        ]
        lines.extend(
            f"\u2022 {describe_wallet_watch(watch)}" for watch in watches[:20]
        )
        if len(watches) > 20:
            lines.append(f"... and {len(watches) - 20} more")
        lines.append(f"**Last wallet check:** {last_check}")
        if bot.last_wallet_error:
            lines.append(f"**Latest issue:** {bot.last_wallet_error[:200]}")
        await interaction.response.send_message(
            "\n".join(lines)[:1900], ephemeral=True
        )

    @app_commands.command(
        name="info", description="Show one tracked wallet's settings and status."
    )
    @app_commands.describe(address="The tracked wallet address.")
    async def info(self, interaction: discord.Interaction, address: str) -> None:
        bot = self.bot
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/wallet info` inside a Discord server.", ephemeral=True
            )
            return
        normalized = normalize_wallet_address(address)
        if normalized is None:
            await interaction.response.send_message(
                "That is not a wallet address (`0x` plus 40 hex characters).",
                ephemeral=True,
            )
            return
        watch = bot.get_watch(guild.id, normalized)
        if watch is None:
            await interaction.response.send_message(
                f"`{short_wallet_address(normalized)}` is not tracked in this "
                "server. Use `/wallet list` to see the watchlist.",
                ephemeral=True,
            )
            return
        last_check = (
            discord.utils.format_dt(bot.last_wallet_check_at, style="R")
            if bot.last_wallet_check_at is not None
            else "not yet"
        )
        lines = [
            f"**Wallet:** {watch.get('label') or 'no label'}",
            f"**Address:** `{normalized}`",
            f"**Status:** {'paused' if watch.get('paused') else 'active'}",
            "**Activity filter:** "
            + WALLET_ACTIVITY_FILTER_LABELS.get(
                watch.get("activity_filter", "all"), "All activity"
            ),
            f"**Minimum value:** ${(watch.get('min_usd') or 0.0):,.2f}",
            "**Global whale alert for this wallet:** "
            + ("suppressed" if watch.get("suppress_whale", True) else "also sent"),
            "**Dedupe history:** "
            f"{len(watch.get('seen') or [])} event(s) remembered",
            f"**Last successful wallet check:** {last_check}",
        ]
        await interaction.response.send_message(
            "\n".join(lines)[:1900], ephemeral=True
        )

    @app_commands.command(
        name="settings",
        description="Change one tracked wallet's alert settings.",
    )
    @app_commands.describe(
        address="The tracked wallet address.",
        label="A new nickname shown in alerts.",
        min_usd="Minimum activity value in USDC before alerting.",
        activity="Which activity rows should generate an alert.",
        whale_alerts="Also send the global large-trade alert for this wallet.",
    )
    @app_commands.choices(
        activity=[
            app_commands.Choice(name=WALLET_ACTIVITY_FILTER_LABELS[key], value=key)
            for key in WALLET_ACTIVITY_FILTERS
        ]
    )
    async def settings(
        self,
        interaction: discord.Interaction,
        address: str,
        label: str | None = None,
        min_usd: float | None = None,
        activity: app_commands.Choice[str] | None = None,
        whale_alerts: bool | None = None,
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        normalized = normalize_wallet_address(address)
        if normalized is None:
            await interaction.response.send_message(
                "That is not a wallet address (`0x` plus 40 hex characters).",
                ephemeral=True,
            )
            return
        watch = bot.get_watch(guild.id, normalized)
        if watch is None:
            await interaction.response.send_message(
                f"`{short_wallet_address(normalized)}` is not tracked in this "
                "server. Add it with `/wallet add` first.",
                ephemeral=True,
            )
            return
        if label is None and min_usd is None and activity is None and whale_alerts is None:
            await interaction.response.send_message(
                f"Current settings: {describe_wallet_watch(watch)}\n"
                "Pass at least one option to change "
                "(`label`, `min_usd`, `activity`, or `whale_alerts`).",
                ephemeral=True,
            )
            return

        apply_watch_options(
            watch,
            label=label,
            min_usd=min_usd,
            activity_filter=activity.value if activity is not None else None,
            suppress_whale=None if whale_alerts is None else (not whale_alerts),
        )
        if not bot.set_watch(guild.id, watch):
            await interaction.response.send_message(
                "I couldn't save this server's watchlist.", ephemeral=True
            )
            return
        await interaction.response.send_message(
            f"Updated {describe_wallet_watch(watch)}.",
            ephemeral=True,
        )

    @app_commands.command(
        name="pause", description="Pause alerts for a tracked wallet (keeps it tracked)."
    )
    @app_commands.describe(address="The tracked wallet address.")
    async def pause(self, interaction: discord.Interaction, address: str) -> None:
        await self._set_paused(interaction, address, True)

    @app_commands.command(
        name="resume", description="Resume alerts for a paused tracked wallet."
    )
    @app_commands.describe(address="The tracked wallet address.")
    async def resume(self, interaction: discord.Interaction, address: str) -> None:
        await self._set_paused(interaction, address, False)

    async def _set_paused(
        self, interaction: discord.Interaction, address: str, paused: bool
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        normalized = normalize_wallet_address(address)
        if normalized is None:
            await interaction.response.send_message(
                "That is not a wallet address (`0x` plus 40 hex characters).",
                ephemeral=True,
            )
            return
        watch = bot.get_watch(guild.id, normalized)
        if watch is None:
            await interaction.response.send_message(
                f"`{short_wallet_address(normalized)}` is not tracked in this "
                "server. Add it with `/wallet add` first.",
                ephemeral=True,
            )
            return
        if watch.get("paused") is paused:
            await interaction.response.send_message(
                f"`{short_wallet_address(normalized)}` is already "
                f"{'paused' if paused else 'active'}.",
                ephemeral=True,
            )
            return
        watch["paused"] = paused
        if not paused:
            # Resuming must not replay everything that happened while paused, so
            # the next check only records a fresh baseline.
            watch["last_seen_ts"] = 0.0
        if not bot.set_watch(guild.id, watch):
            await interaction.response.send_message(
                "I couldn't save this server's watchlist.", ephemeral=True
            )
            return
        await interaction.response.send_message(
            f"Wallet alerts for `{short_wallet_address(normalized)}` are now "
            f"{'paused' if paused else 'active'}.",
            ephemeral=True,
        )


class XCommands(app_commands.Group):
    """The /xtrack command group for X/Twitter account monitoring.

    Same shape as /wallet: a discord.py group owns its children, so every
    subcommand lives on the group and reaches the bot through ``self.bot``.
    """

    def __init__(self, bot: "LashNewsBot") -> None:
        self.bot = bot
        super().__init__(
            name="xtrack",
            description="Track X (Twitter) accounts and alert on their posts.",
            guild_only=True,
        )

    @staticmethod
    def _clear_sentinel(value: str | None) -> bool:
        """True when a settings string means "remove the current list"."""
        return isinstance(value, str) and value.strip().casefold() in {
            "clear",
            "none",
            "reset",
        }

    @app_commands.command(
        name="add",
        description="Track an X account and alert on its new posts here.",
    )
    @app_commands.describe(
        username="The X handle to track (@name, name, or an x.com profile link).",
        label="A nickname shown in alerts (optional).",
        channel="Post this account's alerts to a specific channel (optional).",
        activity="Which activity should generate an alert.",
        keywords="Comma-separated; only posts containing one of them alert (optional).",
        exclude="Comma-separated; posts containing one of them are skipped (optional).",
    )
    @app_commands.choices(
        activity=[
            app_commands.Choice(name=X_ACTIVITY_FILTER_LABELS[key], value=key)
            for key in X_ACTIVITY_FILTERS
        ]
    )
    async def add(
        self,
        interaction: discord.Interaction,
        username: str,
        label: str | None = None,
        channel: discord.TextChannel | None = None,
        activity: app_commands.Choice[str] | None = None,
        keywords: str | None = None,
        exclude: str | None = None,
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        normalized = normalize_x_username(username)
        if normalized is None:
            await interaction.response.send_message(
                "That is not an X handle. Send the 1-15 character handle "
                "(letters, digits, underscores), for example "
                "`@VitalikButerin`.",
                ephemeral=True,
            )
            return
        if not bot.x_api_bearer:
            await interaction.response.send_message(
                "X tracking is not configured on this bot: the hosting "
                "environment is missing the X_API_BEARER_TOKEN variable, so "
                "handles cannot be resolved to accounts. Nothing was added.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        if bot.get_x_account(guild.id, normalized) is not None:
            await interaction.followup.send(
                f"`@{normalized}` is already tracked in this server. Use "
                "`/xtrack info` to see it or `/xtrack settings` to change it.",
                ephemeral=True,
            )
            return
        if bot.x_limit_reached(guild.id):
            await interaction.followup.send(
                f"This server already tracks the maximum of "
                f"{MAX_TRACKED_X_ACCOUNTS_PER_GUILD} X accounts. Remove one "
                "with `/xtrack remove` first.",
                ephemeral=True,
            )
            return

        account = default_x_account(normalized)
        apply_x_account_options(
            account,
            label=label,
            activity_filter=activity.value if activity is not None else None,
            include_keywords=parse_keyword_list(keywords),
            exclude_keywords=parse_keyword_list(exclude),
            channel=channel,
        )

        note = ""
        try:
            user = await bot.fetch_x_user(normalized)
        except XApiError as exc:
            if not exc.transient:
                await interaction.followup.send(str(exc), ephemeral=True)
                return
            # A transient failure (rate limit, X outage) still tracks the
            # account; the monitoring cycle resolves the handle on its next
            # pass instead of forcing the administrator to try again.
            note = (
                "\nX could not be reached to confirm the account just now; "
                "it is tracked and will be resolved on the next cycle."
            )
        else:
            account["user_id"] = str(user.get("id") or "")
            account["display_name"] = clean_article_text(user.get("name"))
            account["profile_image_url"] = clean_article_text(
                user.get("profile_image_url")
            )
            if user.get("protected"):
                note = (
                    "\nThat account is protected, so its posts are not "
                    "readable with public API access and no alerts will arrive."
                )

        if not bot.set_x_account(guild.id, account):
            await interaction.followup.send(
                "I couldn't save this server's X watchlist.", ephemeral=True
            )
            return

        destination = (
            f"<#{channel.id}>"
            if channel is not None
            else "this server's X alerts channel"
        )
        await interaction.followup.send(
            f"Tracking {describe_x_account(account)}.\n"
            f"Alerts: {destination}.\n"
            "The first check only records a baseline, so existing posts are "
            "not posted; new-post alerts start from the next cycle." + note,
            ephemeral=True,
        )

    @app_commands.command(
        name="remove", description="Stop tracking an X account in this server."
    )
    @app_commands.describe(username="The X handle to stop tracking.")
    async def remove(self, interaction: discord.Interaction, username: str) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        normalized = normalize_x_username(username)
        if normalized is None:
            await interaction.response.send_message(
                "That is not an X handle (1-15 letters, digits, underscores).",
                ephemeral=True,
            )
            return
        if not bot.drop_x_account(guild.id, normalized):
            await interaction.response.send_message(
                f"`@{normalized}` is not tracked in this server. Use "
                "`/xtrack list` to see what is.",
                ephemeral=True,
            )
            return
        bot.prune_empty_guild_x_accounts(guild.id)
        await interaction.response.send_message(
            f"Stopped tracking `@{normalized}`.", ephemeral=True
        )

    @app_commands.command(
        name="list", description="List the X accounts this server tracks."
    )
    async def list_accounts(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/xtrack list` inside a Discord server.", ephemeral=True
            )
            return
        accounts = bot.list_x_accounts(guild.id)
        if not accounts:
            await interaction.response.send_message(
                "This server is not tracking any X accounts yet. An "
                "administrator can add one with `/xtrack add`.",
                ephemeral=True,
            )
            return
        last_check = (
            discord.utils.format_dt(bot.last_x_check_at, style="R")
            if bot.last_x_check_at is not None
            else "not yet"
        )
        lines = [
            f"**Tracked X accounts** "
            f"({len(accounts)}/{MAX_TRACKED_X_ACCOUNTS_PER_GUILD})"
        ]
        lines.extend(
            f"\u2022 {describe_x_account(account)}" for account in accounts[:20]
        )
        if len(accounts) > 20:
            lines.append(f"... and {len(accounts) - 20} more")
        lines.append(f"**Last X check:** {last_check}")
        if bot.last_x_error:
            lines.append(f"**Latest issue:** {bot.last_x_error[:200]}")
        await interaction.response.send_message(
            "\n".join(lines)[:1900], ephemeral=True
        )

    @app_commands.command(
        name="info", description="Show one tracked X account's settings and status."
    )
    @app_commands.describe(username="The tracked X handle.")
    async def info(self, interaction: discord.Interaction, username: str) -> None:
        bot = self.bot
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/xtrack info` inside a Discord server.", ephemeral=True
            )
            return
        account = bot.get_x_account(guild.id, username)
        if account is None:
            await interaction.response.send_message(
                f"`{username}` is not tracked in this server. Use "
                "`/xtrack list` to see what is.",
                ephemeral=True,
            )
            return
        last_success = (
            discord.utils.format_dt(
                datetime.fromtimestamp(
                    account["last_success_ts"], tz=timezone.utc
                ),
                style="R",
            )
            if account.get("last_success_ts")
            else "never"
        )
        includes = ", ".join(account.get("include_keywords") or []) or "(any post)"
        excludes = ", ".join(account.get("exclude_keywords") or []) or "(none)"
        channel = (
            f"<#{account['channel_id']}>"
            if account.get("channel_id")
            else "server default"
        )
        lines = [
            f"**@{account['username']}**"
            + (f" — {account['label']}" if account.get("label") else ""),
            f"**Display name:** {account.get('display_name') or '(unknown)'}",
            f"**X user id:** {account.get('user_id') or '(not resolved yet)'}",
            f"**Status:** {'paused' if account.get('paused') else 'active'}",
            f"**Activity filter:** "
            f"{X_ACTIVITY_FILTER_LABELS.get(account['activity_filter'], 'all')}",
            f"**Keywords:** only {includes}; excluding {excludes}",
            f"**Alert channel:** {channel}",
            f"**Newest post handled:** {account.get('since_id') or '(none yet)'}",
            f"**Last successful check:** {last_success}",
        ]
        if account.get("last_error"):
            lines.append(f"**Latest issue:** {account['last_error'][:200]}")
        await interaction.response.send_message(
            "\n".join(lines)[:1900], ephemeral=True
        )

    @app_commands.command(
        name="pause", description="Pause alerts for one tracked X account."
    )
    @app_commands.describe(username="The tracked X handle to pause.")
    async def pause(self, interaction: discord.Interaction, username: str) -> None:
        await self._set_paused(interaction, username, True)

    @app_commands.command(
        name="resume", description="Resume alerts for one paused X account."
    )
    @app_commands.describe(username="The tracked X handle to resume.")
    async def resume(self, interaction: discord.Interaction, username: str) -> None:
        await self._set_paused(interaction, username, False)

    async def _set_paused(
        self, interaction: discord.Interaction, username: str, paused: bool
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        account = bot.get_x_account(guild.id, username)
        if account is None:
            await interaction.response.send_message(
                f"`{username}` is not tracked in this server. Use "
                "`/xtrack list` to see what is.",
                ephemeral=True,
            )
            return
        account["paused"] = paused
        bot.save_x_accounts()
        verb = "Paused" if paused else "Resumed"
        await interaction.response.send_message(
            f"{verb} alerts for {describe_x_account(account)}.", ephemeral=True
        )

    @app_commands.command(
        name="rename", description="Change the nickname shown for a tracked account."
    )
    @app_commands.describe(
        username="The tracked X handle.",
        label="The new nickname; pass 'clear' to remove it.",
    )
    async def rename(
        self, interaction: discord.Interaction, username: str, label: str
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        account = bot.get_x_account(guild.id, username)
        if account is None:
            await interaction.response.send_message(
                f"`{username}` is not tracked in this server. Use "
                "`/xtrack list` to see what is.",
                ephemeral=True,
            )
            return
        new_label = "" if self._clear_sentinel(label) else label.strip()
        account["label"] = new_label[:MAX_X_LABEL_LENGTH]
        bot.save_x_accounts()
        await interaction.response.send_message(
            f"Updated {describe_x_account(account)}.", ephemeral=True
        )

    @app_commands.command(
        name="settings", description="Change what one tracked account alerts on."
    )
    @app_commands.describe(
        username="The tracked X handle.",
        channel="New alert channel for this account (optional).",
        activity="Which activity should generate an alert.",
        keywords="Comma-separated include keywords; 'clear' removes them.",
        exclude="Comma-separated exclude keywords; 'clear' removes them.",
    )
    @app_commands.choices(
        activity=[
            app_commands.Choice(name=X_ACTIVITY_FILTER_LABELS[key], value=key)
            for key in X_ACTIVITY_FILTERS
        ]
    )
    async def settings(
        self,
        interaction: discord.Interaction,
        username: str,
        channel: discord.TextChannel | None = None,
        activity: app_commands.Choice[str] | None = None,
        keywords: str | None = None,
        exclude: str | None = None,
    ) -> None:
        bot = self.bot
        guild = interaction.guild
        if not await bot.require_admin(interaction) or guild is None:
            return
        account = bot.get_x_account(guild.id, username)
        if account is None:
            await interaction.response.send_message(
                f"`{username}` is not tracked in this server. Use "
                "`/xtrack list` to see what is.",
                ephemeral=True,
            )
            return

        include = exclude_list = None
        if keywords is not None:
            include = [] if self._clear_sentinel(keywords) else (
                parse_keyword_list(keywords) or []
            )
        if exclude is not None:
            exclude_list = [] if self._clear_sentinel(exclude) else (
                parse_keyword_list(exclude) or []
            )
        apply_x_account_options(
            account,
            activity_filter=activity.value if activity is not None else None,
            include_keywords=include,
            exclude_keywords=exclude_list,
            channel=channel,
        )
        bot.save_x_accounts()

        destination = (
            f"<#{account['channel_id']}>"
            if account.get("channel_id")
            else "the server's X alerts channel"
        )
        await interaction.response.send_message(
            f"Updated {describe_x_account(account)}.\n"
            f"Activity: {X_ACTIVITY_FILTER_LABELS.get(account['activity_filter'], 'all')}. "
            f"Alerts go to {destination}.",
            ephemeral=True,
        )

    @app_commands.command(
        name="test", description="Send a test X alert to the account's channel."
    )
    @app_commands.describe(username="The tracked X handle to test delivery for.")
    async def test(self, interaction: discord.Interaction, username: str) -> None:
        bot = self.bot
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/xtrack test` inside a Discord server.", ephemeral=True
            )
            return
        account = bot.get_x_account(guild.id, username)
        if account is None:
            await interaction.response.send_message(
                f"`{username}` is not tracked in this server. Use "
                "`/xtrack list` to see what is.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        destinations = await bot.get_alert_channels("x")
        channel = destinations.get(str(guild.id))
        override_id = account.get("channel_id")
        if isinstance(override_id, int) and override_id > 0:
            try:
                resolved = await bot.resolve_channel(override_id)
                if resolved.guild.id == guild.id:
                    channel = resolved
            except Exception:
                logger.warning(
                    "Could not resolve the X test channel %s for @%s.",
                    override_id,
                    account["username"],
                )
        if channel is None:
            await interaction.followup.send(
                "No channel is configured for X alerts in this server. Run "
                "`/set-x-alerts` (or `/setup`), or give this account its own "
                "channel with `/xtrack settings`.",
                ephemeral=True,
            )
            return
        embed = discord.Embed(
            title=f"Test \u00b7 @{account['username']}",
            url=f"{X_PROFILE_URL}/{account['username']}",
            description=(
                "This is a test notification from the X tracker.\n"
                f"New posts from this account are polled every "
                f"{bot.x_poll_interval} seconds and posted here automatically."
            ),
            color=SOVEREIGN_COLOR,
        )
        if account.get("display_name"):
            embed.set_author(name=account["display_name"])
        if await bot.send_embed(channel, embed, str(guild.id)):
            await interaction.followup.send(
                f"Test delivered to <#{channel.id}>.", ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"Could not deliver to <#{channel.id}>; check the bot's "
                "permissions (send messages and embed links).",
                ephemeral=True,
            )

    @app_commands.command(
        name="status", description="Show X tracking health and this server's watchlist."
    )
    async def status(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        guild = interaction.guild
        token = (
            "configured"
            if bot.x_api_bearer
            else "NOT configured (X_API_BEARER_TOKEN is missing)"
        )
        last_check = (
            discord.utils.format_dt(bot.last_x_check_at, style="R")
            if bot.last_x_check_at is not None
            else "not yet"
        )
        lines = [
            "**X tracker status**",
            f"**API access:** {token}",
            f"**Polling:** every {bot.x_poll_interval} seconds "
            "(polling, so delivery is never instantaneous)",
            f"**Last check:** {last_check}",
        ]
        if bot.x_monitor_started_at is not None:
            lines.append(
                "**Monitoring started:** "
                f"{discord.utils.format_dt(bot.x_monitor_started_at, style='R')}"
            )
        if bot.last_x_error:
            lines.append(f"**Latest issue:** {bot.last_x_error[:200]}")
        lines.append(
            f"**This process:** {bot.x_posts_seen} post(s) seen, "
            f"{bot.x_alerts_sent} alert(s) delivered."
        )
        if guild is not None:
            accounts = bot.list_x_accounts(guild.id)
            paused = sum(1 for account in accounts if account.get("paused"))
            lines.append(
                f"**This server:** {len(accounts)} tracked "
                f"({paused} paused) of "
                f"{MAX_TRACKED_X_ACCOUNTS_PER_GUILD} allowed."
            )
        await interaction.response.send_message(
            "\n".join(lines)[:1900], ephemeral=True
        )


class LashNewsBot(commands.Bot):
    def __init__(
        self,
        *,
        news_api_key: str,
        channel_id: int | None,
        poll_interval: int,
        dev_guild_ids: list[int] | None = None,
    ) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.news_api_key = news_api_key
        # The primary key first, then any spares from NEWS_API_KEYS. The full
        # ordered list is what the provider adapter rotates through.
        self.news_api_keys = [
            key
            for key in [
                news_api_key,
                *parse_news_api_keys(os.getenv("NEWS_API_KEYS"), news_api_key),
            ]
            if key
        ]
        self.channel_id = channel_id
        self.poll_interval = poll_interval
        self.dev_guild_ids = dev_guild_ids or []
        self.seen_articles = load_seen_articles()
        self.seen_article_urls = set(self.seen_articles)
        self.legacy_seen_article_urls = set(self.seen_article_urls)
        self.guild_settings = load_guild_settings()
        self.monitor_state = load_monitor_state()
        self.seen_market_ids = set(self.monitor_state["seen_market_ids"])
        self.seen_trades = set(self.monitor_state["seen_trades"])
        self.wallet_state = load_wallet_state()
        self.last_wallet_check_at: datetime | None = None
        self.last_wallet_error: str | None = None
        self.last_wallet_activity_count = 0
        self.wallet_alerts_sent = 0
        # X/Twitter tracking: the bearer token comes from the hosting
        # environment; an empty token disables the tracker with a clear
        # status message instead of failing at request time.
        self.x_api_bearer = os.getenv("X_API_BEARER_TOKEN", "").strip()
        self.x_state = load_x_state()
        self.x_poll_interval = X_POLL_INTERVAL_SECONDS
        self.x_task: asyncio.Task[None] | None = None
        self.x_poll_wakeup = asyncio.Event()
        self.last_x_check_at: datetime | None = None
        self.last_x_error: str | None = None
        self.x_monitor_started_at: datetime | None = None
        self.x_posts_seen = 0
        self.x_alerts_sent = 0
        self._x_monitor_running = False
        self.guild_seen_articles = load_guild_seen_articles()
        self.guild_seen_article_urls = {
            guild_id: set(urls)
            for guild_id, urls in self.guild_seen_articles.items()
        }
        self.news_session: aiohttp.ClientSession | None = None
        self.news_task: asyncio.Task[None] | None = None
        self.poll_wakeup = asyncio.Event()
        self.last_poll_at: datetime | None = None
        self.last_poll_error: str | None = None
        self.articles_sent = 0
        self.last_market_count = 0
        self.last_search_terms: list[str] = []
        self.last_sources: list[str] = []
        self.last_sync_at: datetime | None = None
        self.last_sync_error: str | None = None
        self.monitor_started_at: datetime | None = None
        self.consecutive_failures = 0
        self._monitor_running = False
        self._shutdown_requested = False
        self._command_registration_checked = False

    def register_commands(self, *commands_to_add: app_commands.Command) -> None:
        """Attach command objects to the tree with this bot as their binding.

        Every command is written as a method on this class, so its callback still
        expects ``self`` as its first argument. Adding a bare ``Command`` object to
        the tree leaves ``binding`` as ``None``, and discord.py then calls the
        callback *without* ``self`` -- every interaction dies with
        ``CommandSignatureMismatch`` and Discord reports that the application did
        not respond.

        discord.py performs this same binding step automatically for
        commands declared inside a Cog (``Cog._inject``); a plain Bot subclass gets
        no such step, so it is done here explicitly.

        The binding is always re-assigned rather than set only when missing: the
        decorator stores one ``Command`` object per class attribute, so those
        objects are shared by every instance of this class and a second instance
        would otherwise inherit the first instance's binding.
        """
        for command in commands_to_add:
            command.binding = self
            self.tree.add_command(command)

    async def setup_hook(self) -> None:
        self.news_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        )
        # Commands are attached to the tree once, before the gateway connects.
        # They are synced explicitly below instead of on every on_ready() event,
        # because on_ready() fires again after every reconnect and repeatedly
        # overwriting global commands can reset Discord's propagation window.
        self.register_commands(
            self.setup_slash,
            self.status_slash,
            self.test_slash,
            self.sources_slash,
            self.keywords_slash,
            self.alerts_slash,
            self.help_slash,
            self.servers_slash,
            self.server_info_slash,
            self.sync_slash,
            # Per-category channel configuration.
            self.set_news_alerts_slash,
            self.set_price_alerts_slash,
            self.set_new_markets_slash,
            self.set_whale_alerts_slash,
            self.set_wallet_alerts_slash,
            self.set_x_alerts_slash,
            # Diagnostics and charting.
            self.config_check_slash,
            self.chart_slash,
        )
        # /wallet is a command group, so it is added as a group rather than as a
        # bound method: the group binds its own subcommands to itself.
        self.wallet_group = WalletCommands(self)
        self.tree.add_command(self.wallet_group)
        # /xtrack watches X/Twitter accounts; same group pattern as /wallet.
        self.xtrack_group = XCommands(self)
        self.tree.add_command(self.xtrack_group)
        # Route unhandled application-command errors away from the library's
        # silent default so failures are logged and the user gets a reply.
        self.tree.on_error = self.on_app_command_error
        await self.sync_application_commands()
        self.install_signal_handlers()
        # Never allow two monitors to run at once. If one is somehow already
        # running, stop it before starting a fresh one so only a single loop
        # ever posts alerts.
        if self.news_task is not None and not self.news_task.done():
            logger.warning(
                "A monitoring loop was already running; cancelling it before "
                "starting a new one."
            )
            self.news_task.cancel()
        self.news_task = asyncio.create_task(
            self.poll_news_forever(), name="lash-news-monitor"
        )
        self.x_task = asyncio.create_task(
            self.poll_x_forever(), name="lash-x-monitor"
        )

    def install_signal_handlers(self) -> None:
        """Close cleanly on SIGTERM so Railway shutdown/restart is graceful.

        discord.py only turns SIGINT into a clean shutdown. Hosting platforms
        send SIGTERM, whose default action kills the process without running
        the monitor's cleanup, so it is handled explicitly here.
        """
        try:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown_signal)
        except (NotImplementedError, RuntimeError, ValueError):
            # Windows or a non-main thread: keep the platform default.
            logger.debug("SIGTERM handling is not available on this platform.")

    def _handle_shutdown_signal(self) -> None:
        if self._shutdown_requested:
            return
        self._shutdown_requested = True
        logger.info("Received SIGTERM; shutting down gracefully.")
        asyncio.get_running_loop().create_task(self.close())

    async def sync_guild_commands(self, guild_id: int) -> list[app_commands.AppCommand]:
        """Register the commands for one server so they appear immediately.

        Commands are added to the tree globally (``tree.add_command``), and
        discord.py only sends the commands registered *for that guild* when
        syncing one. Syncing a guild without copying the global commands into it
        first therefore pushes an empty list and reports "Synced 0 commands",
        leaving the server to wait for Discord's global propagation window. The
        copy is refreshed on every call so a server always receives the current
        command set, including a command added after the last sync.
        """
        guild = discord.Object(id=guild_id)
        self.tree.copy_global_to(guild=guild)
        return await self.tree.sync(guild=guild)

    async def sync_application_commands(self) -> None:
        """Register slash commands once per process without spamming Discord.

        Guild-scoped registration appears instantly and is used for development
        and testing. Global registration makes the commands available in every
        server the bot is installed in, but Discord can take up to an hour to
        propagate a global update.
        """
        local_commands = self.tree.get_commands()
        logger.info("Registering %d slash commands.", len(local_commands))

        for guild_id in self.dev_guild_ids:
            try:
                synced = await self.sync_guild_commands(guild_id)
            except Exception:
                logger.exception(
                    "Failed to sync slash commands to development server %s.",
                    guild_id,
                )
                continue
            logger.info(
                "Synced %d slash commands to development server %s "
                "(available immediately).",
                len(synced),
                guild_id,
            )

        try:
            synced = await self.tree.sync()
        except Exception as exc:
            self.last_sync_error = str(exc)[:300] or "Unknown sync error."
            logger.exception(
                "Failed to sync global slash commands. Commands will not appear "
                "until this succeeds. Check that the bot is invited with both the "
                "'bot' and 'applications.commands' scopes and that the token is "
                "valid for this application."
            )
            return

        self.last_sync_at = datetime.now(timezone.utc)
        self.last_sync_error = None
        logger.info(
            "Synced %d global slash commands. Discord may take up to an hour to "
            "show them in every server; use /sync to push them to one server "
            "instantly.%s",
            len(synced),
            ""
            if not self.dev_guild_ids
            else " Development servers were also synced instantly.",
        )

    async def on_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        """Log every slash-command failure and always tell the user something."""
        command_name = getattr(interaction.command, "name", "unknown")
        original = getattr(error, "original", error)
        logger.error(
            "Slash command /%s failed for user %s in server %s: %s: %s",
            command_name,
            interaction.user.id,
            interaction.guild_id,
            type(original).__name__,
            original,
            exc_info=original,
        )

        if isinstance(error, app_commands.MissingPermissions):
            message = "You do not have permission to use this command."
        elif isinstance(error, app_commands.CheckFailure):
            message = "You cannot use this command here."
        elif isinstance(error, app_commands.CommandOnCooldown):
            message = (
                f"That command is on cooldown. Try again in "
                f"{error.retry_after:.0f} seconds."
            )
        else:
            message = (
                "Something went wrong while running that command. The error has "
                "been logged."
            )

        try:
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
        except discord.HTTPException:
            logger.exception(
                "Could not deliver an error message for /%s.", command_name
            )

    async def close(self) -> None:
        for task in (self.news_task, self.x_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                # A monitor that already died (or that never reached the gateway)
                # must not turn a shutdown into a crash.
                logger.exception(
                    "A monitoring task ended with an error during shutdown."
                )
        if self.news_session is not None:
            await self.news_session.close()
        await super().close()

    async def on_ready(self) -> None:
        logger.info(
            "Connected to Discord as %s (in %d server(s)).",
            self.user,
            len(self.guilds),
        )
        # Discord only reports the guild list once the gateway is up, so the
        # duplicate-registration cleanup runs here, and once per process: a
        # reconnect must not rewrite the command list again.
        if not self._command_registration_checked:
            self._command_registration_checked = True
            await self.ensure_single_command_registration()

    async def ensure_single_command_registration(self) -> None:
        """Make every command show up exactly once in Discord.

        Two different things make the same command appear twice in a server:

        * the application allows user installation, so a global command is
          published once for the server and once for the user; and
        * registrations left behind by an earlier per-server sync, which
          Discord keeps until the server list is overwritten.

        Discord has no "remove just the duplicate" call, so the global list is
        rewritten as guild-install only, and per-server lists left over from
        other versions are cleared -- except the development servers, whose
        fresh copy was just written by the sync above. Both steps need a
        logged-in client, so this is a no-op while the command tree is being
        exercised offline.
        """
        if self.application_id is None or self.http.token is None:
            return
        await self.normalize_command_install_scopes()
        await self.clear_stale_guild_commands()

    async def normalize_command_install_scopes(self) -> None:
        """Publish the global commands for server installation only.

        A command that is published for both install types is listed twice in
        the picker even though it is the same command, which is what made the
        bot look like it had duplicates. Rewriting the global registration with
        ``integration_types=[0]`` leaves exactly one entry per command.
        """
        application_id = self.application_id
        if application_id is None:
            return
        payloads: list[dict[str, Any]] = []
        for command in self.tree.get_commands():
            payload = command.to_dict()
            payload["integration_types"] = [0]
            payloads.append(payload)
        try:
            await self.http.bulk_upsert_global_commands(application_id, payloads)
        except Exception:
            logger.exception(
                "Could not pin the global commands to server installation only."
            )
            return
        logger.info(
            "Pinned %d commands to server installation only, so each shows "
            "once.",
            len(payloads),
        )

    async def clear_stale_guild_commands(self) -> list[int]:
        """Delete per-server registrations that duplicate the global ones.

        Development servers are skipped: their registration is written fresh by
        the sync above and is what makes commands appear there instantly.
        """
        application_id = self.application_id
        if application_id is None:
            return []
        dev_guild_ids = set(self.dev_guild_ids)
        cleared: list[int] = []
        for guild in list(self.guilds):
            if guild.id in dev_guild_ids:
                continue
            try:
                existing = await self.http.get_guild_commands(
                    application_id, guild.id
                )
            except Exception:
                logger.warning(
                    "Could not read the command list of server %s.", guild.id
                )
                continue
            if not existing:
                continue
            try:
                await self.http.bulk_upsert_guild_commands(
                    application_id, guild.id, []
                )
            except Exception:
                logger.exception(
                    "Could not clear the duplicate commands of server %s.",
                    guild.id,
                )
                continue
            cleared.append(guild.id)
            logger.info(
                "Cleared %d duplicate per-server command registration(s) in "
                "server %s; the global commands now serve it.",
                len(existing),
                guild.id,
            )
        return cleared

    async def resolve_channel(
        self,
        channel_id: int,
    ) -> discord.TextChannel | discord.Thread:
        channel = self.get_channel(channel_id)
        if channel is None:
            channel = await self.fetch_channel(channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            raise TypeError(f"Discord channel {channel_id} is not a text channel or thread.")
        return channel

    def ensure_guild_history(self, guild_id: str) -> None:
        if guild_id in self.guild_seen_article_urls:
            return
        urls = self.seen_articles.copy()
        self.guild_seen_articles[guild_id] = urls
        self.guild_seen_article_urls[guild_id] = set(urls)
        save_guild_seen_articles(self.guild_seen_articles)

    def get_settings(self, guild_id: int | str) -> dict[str, Any]:
        """Return one server's settings without creating an entry for it."""
        return self.guild_settings.get(str(guild_id)) or default_guild_settings()

    def ensure_settings(self, guild_id: int | str) -> dict[str, Any]:
        """Return one server's settings, creating defaults in memory if absent."""
        key = str(guild_id)
        settings = self.guild_settings.get(key)
        if settings is None:
            settings = default_guild_settings()
            self.guild_settings[key] = settings
        return settings

    def save_settings(self) -> bool:
        try:
            save_guild_settings(self.guild_settings)
            return True
        except OSError:
            logger.exception("Could not save server settings.")
            return False

    def persist_monitor_state(self) -> None:
        self.monitor_state["seen_market_ids"] = self.monitor_state["seen_market_ids"][-MAX_SEEN_MARKETS:]
        self.monitor_state["seen_trades"] = self.monitor_state["seen_trades"][-MAX_SEEN_TRADES:]
        try:
            save_monitor_state(self.monitor_state)
        except OSError:
            logger.exception("Could not persist monitor state.")

    def remember_market_ids(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            if market_id not in self.seen_market_ids:
                self.seen_market_ids.add(market_id)
                self.monitor_state["seen_market_ids"].append(market_id)
        overflow = len(self.monitor_state["seen_market_ids"]) - MAX_SEEN_MARKETS
        for _ in range(max(0, overflow)):
            self.seen_market_ids.discard(
                self.monitor_state["seen_market_ids"].pop(0)
            )
        if market_ids:
            self.persist_monitor_state()

    def remember_trade_keys(self, trade_keys: list[str]) -> None:
        for key in trade_keys:
            if key not in self.seen_trades:
                self.seen_trades.add(key)
                self.monitor_state["seen_trades"].append(key)
        overflow = len(self.monitor_state["seen_trades"]) - MAX_SEEN_TRADES
        for _ in range(max(0, overflow)):
            self.seen_trades.discard(self.monitor_state["seen_trades"].pop(0))
        if trade_keys:
            self.persist_monitor_state()

    # ------------------------------------------------------------------
    # Wallet watchlists
    # ------------------------------------------------------------------

    def wallet_guild_state(self, guild_id: int | str) -> dict[str, Any]:
        """One server's watchlist entry, created in memory when absent."""
        key = str(guild_id)
        state = self.wallet_state.get(key)
        if state is None or not isinstance(state.get("watches"), dict):
            state = {"watches": {}}
            self.wallet_state[key] = state
        return state

    def get_watch(self, guild_id: int | str, address: str) -> dict[str, Any] | None:
        normalized = normalize_wallet_address(address)
        if normalized is None:
            return None
        return self.wallet_guild_state(guild_id)["watches"].get(normalized)

    def list_watches(self, guild_id: int | str) -> list[dict[str, Any]]:
        watches = self.wallet_guild_state(guild_id)["watches"]
        return sorted(watches.values(), key=lambda watch: watch["address"])

    def tracked_wallet_addresses(self) -> set[str]:
        """Every distinct address any server tracks, paused watches included."""
        return {
            address
            for state in self.wallet_state.values()
            for address in state.get("watches", {})
        }

    def guild_tracks_wallet(self, guild_id: int | str, address: Any) -> dict[str, Any] | None:
        return self.get_watch(guild_id, address) if isinstance(address, str) else None

    def save_wallets(self) -> bool:
        try:
            save_wallet_state(self.wallet_state)
            return True
        except OSError:
            logger.exception("Could not save the wallet watchlist.")
            return False

    def watch_limit_reached(self, guild_id: int | str) -> bool:
        return len(self.list_watches(guild_id)) >= MAX_TRACKED_WALLETS_PER_GUILD

    def set_watch(self, guild_id: int | str, watch: dict[str, Any]) -> bool:
        self.wallet_guild_state(guild_id)["watches"][watch["address"]] = watch
        return self.save_wallets()

    def drop_watch(self, guild_id: int | str, address: str) -> bool:
        normalized = normalize_wallet_address(address)
        if normalized is None:
            return False
        return self.wallet_guild_state(guild_id)["watches"].pop(normalized, None) is not None

    def prune_empty_guild_wallets(self, guild_id: int | str) -> None:
        key = str(guild_id)
        state = self.wallet_state.get(key)
        if state is not None and not state.get("watches"):
            self.wallet_state.pop(key, None)

    async def send_embed(
        self,
        channel: discord.TextChannel | discord.Thread,
        embed: discord.Embed,
        guild_id: str,
    ) -> bool:
        """Post one embed, logging instead of raising on a Discord failure."""
        try:
            await channel.send(
                embed=sanitize_embed(embed),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return True
        except asyncio.CancelledError:
            raise
        except discord.HTTPException:
            logger.exception(
                "Could not post an alert to channel %s in server %s.",
                channel.id,
                guild_id,
            )
            return False

    async def get_alert_channels(
        self,
        alert: str | None = None,
    ) -> dict[str, discord.TextChannel | discord.Thread]:
        """Resolve which channel each server wants for one alert category.

        ``alert`` selects a category, so a server can send whales to one
        channel while news still uses its default. ``alert=None`` resolves the
        default channel. A channel that cannot be resolved is reported; when
        nothing at all resolves the failure is raised so the caller can decide.
        """
        destinations: dict[str, discord.TextChannel | discord.Thread] = {}
        legacy_channel: discord.TextChannel | discord.Thread | None = None
        legacy_guild_id: str | None = None
        resolution_failures = 0

        if self.channel_id is not None:
            try:
                legacy_channel = await self.resolve_channel(self.channel_id)
                legacy_guild_id = str(legacy_channel.guild.id)
                self.ensure_guild_history(legacy_guild_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                resolution_failures += 1
                logger.exception("Could not resolve the legacy alert channel.")

        for guild_id, settings in list(self.guild_settings.items()):
            channel_id = effective_channel_id(settings, alert)
            if channel_id is None:
                continue
            try:
                channel = await self.resolve_channel(channel_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                resolution_failures += 1
                logger.exception(
                    "Could not resolve the %s alert channel configured for "
                    "server %s.",
                    alert or "default",
                    guild_id,
                )
                continue
            if str(channel.guild.id) != guild_id:
                logger.error(
                    "Configured %s alert channel %s does not belong to "
                    "server %s.",
                    alert or "default",
                    channel_id,
                    guild_id,
                )
                continue
            self.ensure_guild_history(guild_id)
            destinations[guild_id] = channel

        if (
            legacy_channel is not None
            and legacy_guild_id is not None
            and legacy_guild_id not in self.guild_settings
        ):
            destinations[legacy_guild_id] = legacy_channel

        # Bootstrap fallback for one category. It exists so a category cannot sit
        # dead forever because nobody ran its /set-...-alerts command -- including
        # after a deploy that started with an empty state directory. It only ever
        # fills a gap: an explicit choice always wins, a category switched off in
        # that server is respected, and the channel must belong to the server it
        # is used for, so it can never point one server's alerts at another's
        # channel.
        fallback_channel_id = alert_fallback_channel_id(alert)
        if fallback_channel_id is not None:
            for guild in list(getattr(self, "guilds", []) or []):
                guild_id = str(guild.id)
                if guild_id in destinations:
                    continue
                settings = self.get_settings(guild_id)
                if not settings.get("alerts", {}).get(alert, True):
                    continue
                if effective_channel_id(settings, alert) is not None:
                    continue
                try:
                    channel = await self.resolve_channel(fallback_channel_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Could not resolve the %s fallback channel %s.",
                        alert,
                        fallback_channel_id,
                    )
                    break
                if str(channel.guild.id) != guild_id:
                    continue
                logger.info(
                    "No %s channel is configured for server %s; using the "
                    "configured fallback channel %s. Run %s to choose another.",
                    alert,
                    guild_id,
                    fallback_channel_id,
                    ALERT_CHANNEL_COMMANDS[alert],
                )
                self.ensure_guild_history(guild_id)
                destinations[guild_id] = channel
                break

        if not destinations and resolution_failures:
            raise RuntimeError("Could not resolve any configured Discord alert channel.")

        return destinations

    async def get_alert_destinations(
        self,
    ) -> dict[str, dict[str, discord.TextChannel | discord.Thread]]:
        """Resolve every alert category's destinations, isolating failures.

        One deleted channel or a single failing lookup must never stop the other
        alert categories from running, so each category resolves independently
        and reports an empty mapping instead of raising.
        """
        destinations: dict[
            str, dict[str, discord.TextChannel | discord.Thread]
        ] = {}
        for alert in ALERT_TYPES:
            try:
                destinations[alert] = await self.get_alert_channels(alert)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not resolve the %s alert channels this cycle.", alert
                )
                destinations[alert] = {}
        return destinations

    async def get_configured_channel(
        self,
        guild: discord.Guild,
    ) -> discord.TextChannel | discord.Thread | None:
        guild_id = str(guild.id)
        settings = self.guild_settings.get(guild_id)
        channel_id = settings.get("channel_id") if settings else None
        if channel_id is None:
            channel_id = self.channel_id
        if channel_id is None:
            return None
        channel = await self.resolve_channel(channel_id)
        if channel.guild.id != guild.id:
            return None
        self.ensure_guild_history(guild_id)
        return channel

    async def require_admin(self, interaction: discord.Interaction) -> bool:
        """Reply and return False unless the caller is a server administrator.

        Must run before deferring, because the reply uses the initial response.
        """
        permissions = getattr(interaction.user, "guild_permissions", None)
        if (
            interaction.guild is None
            or permissions is None
            or not permissions.administrator
        ):
            await interaction.response.send_message(
                "Only server administrators can change these settings.",
                ephemeral=True,
            )
            return False
        return True

    @app_commands.command(
        name="setup",
        description="Choose this server's alert channel and which alerts are enabled.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="The channel where alerts should be posted.",
        news="News articles that match an active Polymarket market.",
        price="Alerts when a watched market's odds move sharply.",
        new_market="Alerts when new Polymarket markets are created.",
        whale="Alerts for large executed trades.",
        wallet="Alerts when a tracked wallet moves.",
    )
    async def setup_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        news: bool | None = None,
        price: bool | None = None,
        new_market: bool | None = None,
        whale: bool | None = None,
        wallet: bool | None = None,
    ) -> None:
        guild = interaction.guild
        if not await self.require_admin(interaction) or guild is None:
            return
        if channel.guild.id != guild.id:
            await interaction.response.send_message(
                "Choose a text channel from this server.",
                ephemeral=True,
            )
            return

        bot_member = guild.me
        if bot_member is not None:
            channel_permissions = channel.permissions_for(bot_member)
            if (
                not channel_permissions.send_messages
                or not channel_permissions.embed_links
            ):
                await interaction.response.send_message(
                    "I need permission to send messages and embeds in that channel.",
                    ephemeral=True,
                )
                return

        settings = self.ensure_settings(guild.id)
        settings["channel_id"] = channel.id
        for alert, value in (
            ("news", news),
            ("price", price),
            ("new_market", new_market),
            ("whale", whale),
            ("wallet", wallet),
        ):
            if value is not None:
                settings["alerts"][alert] = value
        if not self.save_settings():
            await interaction.response.send_message(
                "I couldn't save this server's settings.",
                ephemeral=True,
            )
            return

        try:
            self.ensure_guild_history(str(guild.id))
        except OSError:
            logger.exception(
                "Could not initialize article history for server %s.", guild.id
            )
        self.poll_wakeup.set()
        enabled = ", ".join(
            ALERT_LABELS[alert]
            for alert in ALERT_TYPES
            if settings["alerts"][alert]
        ) or "none"
        await interaction.response.send_message(
            f"Alerts for this server will be sent to {channel.mention}.\n"
            f"**Enabled alerts:** {enabled}",
            ephemeral=True,
        )

    @app_commands.command(
        name="alerts",
        description="Set this server's alert thresholds.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        price_move_threshold="Percentage points (1-100) a market must move before alerting.",
        whale_min_usd="Minimum value in USD for an executed trade to be alerted.",
        new_market_min_liquidity="Minimum liquidity in USD for a new market to be alerted.",
    )
    async def alerts_slash(
        self,
        interaction: discord.Interaction,
        price_move_threshold: float | None = None,
        whale_min_usd: float | None = None,
        new_market_min_liquidity: float | None = None,
    ) -> None:
        if not await self.require_admin(interaction):
            return
        if (
            price_move_threshold is None
            and whale_min_usd is None
            and new_market_min_liquidity is None
        ):
            await interaction.response.send_message(
                "Provide at least one threshold to change, or use /status to see "
                "the current values.",
                ephemeral=True,
            )
            return

        settings = self.ensure_settings(interaction.guild_id)
        message_lines: list[str] = []
        if price_move_threshold is not None:
            settings["price_move_threshold"] = clamp(
                price_move_threshold,
                MIN_PRICE_MOVE_THRESHOLD,
                MAX_PRICE_MOVE_THRESHOLD,
            )
            message_lines.append(
                f"**Price-move threshold:** {settings['price_move_threshold']:.0f} points"
            )
        if whale_min_usd is not None:
            settings["whale_min_usd"] = clamp(
                whale_min_usd, MIN_WHALE_MIN_USD, 10_000_000.0
            )
            message_lines.append(
                f"**Large-trade threshold:** ${settings['whale_min_usd']:,.0f}"
            )
        if new_market_min_liquidity is not None:
            settings["new_market_min_liquidity"] = clamp(
                new_market_min_liquidity, 0.0, 10_000_000.0
            )
            message_lines.append(
                f"**New-market liquidity floor:** "
                f"${settings['new_market_min_liquidity']:,.0f}"
            )

        if not self.save_settings():
            await interaction.response.send_message(
                "I couldn't save this server's settings.", ephemeral=True
            )
            return
        self.poll_wakeup.set()
        await interaction.response.send_message(
            "Updated this server's thresholds:\n" + "\n".join(message_lines),
            ephemeral=True,
        )

    @app_commands.command(
        name="status",
        description="Show monitoring health and this server's alert settings.",
    )
    @app_commands.guild_only()
    async def status_slash(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/status` inside a Discord server.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        try:
            channel = await self.get_configured_channel(guild)
        except Exception:
            logger.exception(
                "Could not resolve the configured channel for server %s.", guild.id
            )
            channel = None

        settings = self.get_settings(guild.id)
        monitor_running = self.news_task is not None and not self.news_task.done()
        last_check = (
            discord.utils.format_dt(self.last_poll_at, style="R")
            if self.last_poll_at is not None
            else "not yet"
        )
        synced_at = (
            discord.utils.format_dt(self.last_sync_at, style="R")
            if self.last_sync_at is not None
            else "not yet"
        )
        enabled = ", ".join(
            ALERT_LABELS[alert]
            for alert in ALERT_TYPES
            if settings["alerts"][alert]
        ) or "none"
        wallet_check = (
            discord.utils.format_dt(self.last_wallet_check_at, style="R")
            if self.last_wallet_check_at is not None
            else "not yet"
        )
        tracked_wallets = len(self.tracked_wallet_addresses())
        configured_channels = len(
            {
                channel_id
                for channel_id in (
                    [effective_channel_id(settings, alert) for alert in ALERT_TYPES]
                    + [effective_channel_id(settings)]
                )
                if channel_id is not None
            }
        )
        # A connected Discord client says nothing about whether the monitors are
        # alive, so every subsystem reports its own state.
        lines = [
            f"**Discord connection:** {'connected' if self.is_ready() else 'disconnected'}",
            f"**Commands synced:** {synced_at}",
            f"**Monitor loop:** {'running' if monitor_running else 'stopped'}",
            "**News polling:** "
            + (
                "configured (NewsAPI)"
                if self.news_api_keys
                else "disabled \u2014 NEWS_API_KEY is not set"
            ),
            "**Market-data polling:** "
            + ("running" if monitor_running else "stopped"),
            "**Wallet tracking:** "
            + (
                f"{tracked_wallets} wallet(s) tracked, last check {wallet_check}"
                if tracked_wallets
                else "no wallets tracked \u2014 use `/wallet add`"
            ),
            "**Alert delivery:** "
            + (
                f"{configured_channels} channel(s) configured"
                if configured_channels
                else "no channel configured \u2014 run `/setup`"
            ),
            f"**Last successful check:** {last_check}",
            f"**Alert channel:** {channel.mention if channel is not None else 'not configured'}",
            f"**Enabled alerts:** {enabled}",
            f"**Price-move threshold:** {settings['price_move_threshold']:.0f} points",
            f"**Large-trade threshold:** ${settings['whale_min_usd']:,.0f}",
            f"**New-market liquidity floor:** ${settings['new_market_min_liquidity']:,.0f}",
            f"**Include keywords:** {len(settings['include_keywords'])} "
            f"| **Exclude keywords:** {len(settings['exclude_keywords'])}",
            f"**Allowed sources:** {len(settings['sources']) or 'all'}",
            f"**Articles posted this run:** {self.articles_sent}",
        ]
        if self.consecutive_failures:
            lines.append(f"**Consecutive failures:** {self.consecutive_failures}")
        if self.last_poll_error:
            lines.append(f"**Latest issue:** {self.last_poll_error[:300]}")
        if self.last_sync_error:
            lines.append(f"**Command sync issue:** {self.last_sync_error[:200]}")
        if self.last_wallet_error:
            lines.append(f"**Wallet tracking issue:** {self.last_wallet_error[:200]}")
        await interaction.followup.send("\n".join(lines)[:1900], ephemeral=True)

    @app_commands.command(
        name="test",
        description="Send a test alert to this server's channel for one category.",
    )
    @app_commands.guild_only()
    @app_commands.describe(
        category="Which alert category to test (defaults to the main channel)."
    )
    @app_commands.choices(
        category=[
            app_commands.Choice(name=ALERT_LABELS[alert], value=alert)
            for alert in ALERT_TYPES
        ]
    )
    async def test_slash(
        self,
        interaction: discord.Interaction,
        category: app_commands.Choice[str] | None = None,
    ) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/test` inside a Discord server.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)

        alert = category.value if category is not None else None
        settings = self.get_settings(guild.id)
        channel_id = effective_channel_id(settings, alert)
        if channel_id is None:
            if alert is None:
                await interaction.followup.send(
                    "No alert channel is configured. Ask an administrator "
                    "to run `/setup`.",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(
                    f"No {ALERT_LABELS[alert].lower()} channel is "
                    "configured yet. Ask an administrator to run "
                    f"`{ALERT_CHANNEL_COMMANDS[alert]}`.",
                    ephemeral=True,
                )
            return

        try:
            channel = await self.resolve_channel(channel_id)
        except Exception:
            logger.exception(
                "Could not resolve the configured channel for server %s.",
                guild.id,
            )
            channel = None
        if channel is None or channel.guild.id != guild.id:
            await interaction.followup.send(
                "The configured alert channel is missing or belongs to "
                "another server. Ask an administrator to configure it again.",
                ephemeral=True,
            )
            return
        self.ensure_guild_history(str(guild.id))

        now = datetime.now(timezone.utc)
        label = ALERT_LABELS[alert] if alert is not None else "Alerts"
        embed = discord.Embed(
            title="Market Intel | Sovereign Predicts",
            description=(
                f"**{label} test**\n"
                f"This confirms that {label.lower()} can be delivered to "
                "this channel."
            ),
            color=SOVEREIGN_COLOR,
            timestamp=now,
        )
        embed.add_field(
            name="Summary",
            value="Test message only; this is not a real news article.",
            inline=False,
        )
        embed.add_field(name="Source", value="Sovereign Predicts", inline=True)
        embed.add_field(
            name="Published",
            value=discord.utils.format_dt(now, style="f"),
            inline=True,
        )
        embed.add_field(
            name="Topics",
            value="Market-driven news",
            inline=False,
        )
        if alert == "wallet":
            embed.add_field(
                name="Wallet tracking",
                value=(
                    f"This server tracks {len(self.list_watches(guild.id))} "
                    "wallet(s). Use `/wallet list` to review them."
                ),
                inline=False,
            )
        embed.set_footer(text="Market Intel • Sovereign Predicts")
        try:
            await channel.send(
                embed=embed,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except Exception:
            logger.exception(
                "Could not send a test alert to channel %s in server %s.",
                channel.id,
                guild.id,
            )
            await interaction.followup.send(
                "Discord couldn't send the test alert. Check the bot's channel permissions.",
                ephemeral=True,
            )
            return
        await interaction.followup.send(
            f"Test alert sent to {channel.mention}.",
            ephemeral=True,
        )

    async def apply_alert_channel(
        self,
        interaction: discord.Interaction,
        alert: str,
        channel: discord.TextChannel | None,
    ) -> None:
        """Point one alert category at its own channel, or clear that override.

        Only the named category is touched, so changing whale alerts never
        moves news, price, or new-market alerts. An omitted channel clears the
        override and the category falls back to this server's default channel.
        """
        guild = interaction.guild
        if not await self.require_admin(interaction) or guild is None:
            return

        label = ALERT_LABELS[alert]
        settings = self.ensure_settings(guild.id)

        if channel is None:
            settings["channels"][alert] = None
            if not self.save_settings():
                await interaction.response.send_message(
                    "I couldn't save this server's settings.", ephemeral=True
                )
                return
            default_id = effective_channel_id(settings)
            fallback = (
                f"<#{default_id}>" if default_id is not None else "`/setup`"
            )
            await interaction.response.send_message(
                f"**{label}** will now use this server's default channel "
                f"({fallback}). Every other alert category is unchanged.",
                ephemeral=True,
            )
            return

        if channel.guild.id != guild.id:
            await interaction.response.send_message(
                "Choose a text channel from this server.", ephemeral=True
            )
            return

        bot_member = guild.me
        if bot_member is not None:
            permissions = channel.permissions_for(bot_member)
            if not permissions.send_messages or not permissions.embed_links:
                await interaction.response.send_message(
                    "I need permission to send messages and embed links in that "
                    "channel.",
                    ephemeral=True,
                )
                return

        settings["channels"][alert] = channel.id
        if not self.save_settings():
            await interaction.response.send_message(
                "I couldn't save this server's settings.", ephemeral=True
            )
            return
        self.poll_wakeup.set()
        await interaction.response.send_message(
            f"**{label}** will now post to {channel.mention}. Every other alert "
            "category is unchanged.",
            ephemeral=True,
        )

    @app_commands.command(
        name="set-news-alerts",
        description="Send news alerts to their own channel in this server.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="Channel for news alerts; leave empty to use the default channel."
    )
    async def set_news_alerts_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        await self.apply_alert_channel(interaction, "news", channel)

    @app_commands.command(
        name="set-price-alerts",
        description="Send price-move alerts to their own channel in this server.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="Channel for price alerts; leave empty to use the default channel."
    )
    async def set_price_alerts_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        await self.apply_alert_channel(interaction, "price", channel)

    @app_commands.command(
        name="set-new-markets",
        description="Send new-market alerts to their own channel in this server.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="Channel for new markets; leave empty to use the default channel."
    )
    async def set_new_markets_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        await self.apply_alert_channel(interaction, "new_market", channel)

    @app_commands.command(
        name="set-whale-alerts",
        description="Send large-trade alerts to their own channel in this server.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="Channel for whale alerts; leave empty to use the default channel."
    )
    async def set_whale_alerts_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        await self.apply_alert_channel(interaction, "whale", channel)

    @app_commands.command(
        name="set-wallet-alerts",
        description="Send tracked-wallet alerts to their own channel in this server.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="Channel for wallet alerts; empty uses this server's default channel."
    )
    async def set_wallet_alerts_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        await self.apply_alert_channel(interaction, "wallet", channel)

    @app_commands.command(
        name="set-x-alerts",
        description="Send X (Twitter) tracker alerts to their own channel.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.describe(
        channel="Channel for X alerts; empty uses this server's default channel."
    )
    async def set_x_alerts_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        await self.apply_alert_channel(interaction, "x", channel)

    @app_commands.command(
        name="config-check",
        description="Check this server's alert channels and report anything broken.",
    )
    @app_commands.guild_only()
    async def config_check_slash(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/config-check` inside a Discord server.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)

        settings = self.get_settings(guild.id)
        default_id = effective_channel_id(settings)
        resolved: dict[int, tuple[bool, str]] = {}
        problems: list[str] = []

        async def describe(channel_id: int) -> tuple[bool, str]:
            """Resolve one configured channel and say whether it is usable."""
            cached = resolved.get(channel_id)
            if cached is not None:
                return cached
            try:
                channel = await self.resolve_channel(channel_id)
            except discord.NotFound:
                result = (False, f"<#{channel_id}> was deleted — configure it again")
            except Exception:
                result = (False, f"<#{channel_id}> could not be resolved")
            else:
                permissions = (
                    channel.permissions_for(guild.me)
                    if guild.me is not None
                    else None
                )
                if channel.guild.id != guild.id:
                    result = (
                        False,
                        f"<#{channel_id}> belongs to a different server",
                    )
                elif permissions is not None and (
                    not permissions.send_messages or not permissions.embed_links
                ):
                    result = (
                        False,
                        f"<#{channel_id}> is missing the permission to send "
                        "messages and embed links",
                    )
                else:
                    result = (True, f"<#{channel_id}>")
            resolved[channel_id] = result
            return result

        lines = ["**Alert channel check**"]
        if default_id is None:
            lines.append(
                "**Default channel:** not configured — run `/setup` or one of the "
                "`/set-` commands."
            )
            problems.append("No default alert channel is configured; run `/setup`.")
        else:
            ok, text = await describe(default_id)
            lines.append(f"**Default channel:** {'✅' if ok else '⚠'} {text}")
            if not ok:
                problems.append(text)

        channels = settings.get("channels")
        for alert in ALERT_TYPES:
            override = channels.get(alert) if isinstance(channels, dict) else None
            channel_id = effective_channel_id(settings, alert)
            if channel_id is None:
                lines.append(
                    f"**{ALERT_LABELS[alert]}** — ⚠ not configured · use "
                    f"`{ALERT_CHANNEL_COMMANDS[alert]}`"
                )
                continue
            ok, text = await describe(channel_id)
            if not ok:
                problems.append(text)
            source = (
                "this category has its own channel"
                if override
                else "using the default channel"
            )
            disabled = (
                ""
                if settings["alerts"].get(alert, True)
                else " · (disabled)"
            )
            lines.append(
                f"**{ALERT_LABELS[alert]}** — {'✅' if ok else '⚠'} {text} "
                f"({source}){disabled} · "
                f"`{ALERT_CHANNEL_COMMANDS[alert]}`"
            )

        if problems:
            lines.append(f"⚠ {len(problems)} problem(s) found")
        else:
            lines.append("✅ Every configured channel is ready.")
        await interaction.followup.send("\n".join(lines)[:1900], ephemeral=True)

    async def fetch_price_history(
        self, token_id: str, interval: str
    ) -> list[tuple[int, float]]:
        """Fetch one market's price history at a readable resolution.

        Each interval has an enforced minimum resolution, and a market that
        is only a few hours old has too few points at the coarser one, so the
        finer allowed resolution is tried before giving up.
        """
        candidates = CHART_FIDELITY_MINUTES.get(interval, (60,))
        points: list[tuple[int, float]] = []
        for fidelity in candidates:
            payload = await self.get_json(
                POLYMARKET_PRICE_HISTORY_URL,
                params={
                    "market": token_id,
                    "interval": interval,
                    "fidelity": fidelity,
                },
                headers={
                    "Accept": "application/json",
                    "User-Agent": "LashNewsBot/1.0",
                },
                timeout=20,
            )
            points = parse_price_history(payload)
            if len(points) >= 2:
                break
        return points

    async def find_chart_market(self, query: str) -> dict[str, Any] | None:
        """Resolve a market slug or a free-text query to one open market.

        A query without spaces is treated as a slug: the exact lookup runs
        first, and if Gamma does not know it the search results are accepted
        only when a market slug matches exactly. That way a typo reports "not
        found" instead of charting whatever the search returned first.
        """
        headers = {"Accept": "application/json", "User-Agent": "LashNewsBot/1.0"}
        slug_like = " " not in query

        if slug_like:
            payload = await self.get_json(
                POLYMARKET_MARKETS_URL,
                params={
                    "slug": query,
                    "closed": "false",
                    "active": "true",
                    "limit": 5,
                },
                headers=headers,
                timeout=15,
            )
            if isinstance(payload, list):
                for market in payload:
                    if isinstance(market, dict) and market.get("closed") is not True:
                        return market

        payload = await self.get_json(
            POLYMARKET_SEARCH_URL,
            params={
                "q": query,
                "events_status": "active",
                "limit_per_type": 10,
                "keep_closed_markets": 0,
                "search_tags": "false",
                "search_profiles": "false",
            },
            headers=headers,
            timeout=15,
        )
        if not isinstance(payload, dict) or not isinstance(
            payload.get("events"), list
        ):
            raise RuntimeError("Polymarket returned an unexpected search response.")

        found: list[dict[str, Any]] = []
        for event in payload["events"]:
            if not isinstance(event, dict) or event.get("closed") is True:
                continue
            for market in event.get("markets") or []:
                if isinstance(market, dict) and market.get("closed") is not True:
                    found.append(market)
        if not found:
            return None
        if slug_like:
            for market in found:
                if market.get("slug") == query:
                    return market
            return None
        return found[0]

    @app_commands.command(
        name="chart",
        description="Chart a Polymarket market's price history as a PNG.",
    )
    @app_commands.guild_only()
    @app_commands.describe(
        market="A market slug (for example brent-above-90) or a search phrase.",
        days="How many days of history to chart (default 7).",
    )
    async def chart_slash(
        self,
        interaction: discord.Interaction,
        market: str,
        days: int = 7,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Run `/chart` inside a Discord server.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)

        query = clean_article_text(market)
        if not query:
            await interaction.followup.send(
                "Give me a market slug or a search phrase.", ephemeral=True
            )
            return
        try:
            lookback = max(1, min(int(days), 365))
        except (TypeError, ValueError):
            lookback = 7

        try:
            found = await self.find_chart_market(query)
        except Exception:
            logger.exception("Could not look up a market for %r.", query)
            await interaction.followup.send(
                "Polymarket didn't answer the market lookup just now; try again in "
                "a minute.",
                ephemeral=True,
            )
            return
        if found is None:
            await interaction.followup.send(
                f"I couldn't find a Polymarket market matching `{query}`. Try a "
                "market slug or a shorter phrase.",
                ephemeral=True,
            )
            return

        token_id = first_clob_token_id(found)
        if token_id is None:
            await interaction.followup.send(
                "That market has no price history available yet, so there is "
                "nothing to chart.",
                ephemeral=True,
            )
            return

        interval = chart_interval(lookback)
        try:
            points = await self.fetch_price_history(token_id, interval)
        except Exception:
            logger.exception("Price history lookup failed for market %s.", token_id)
            await interaction.followup.send(
                "Polymarket didn't return price history for that market. Try again "
                "in a minute.",
                ephemeral=True,
            )
            return

        if len(points) < 2:
            await interaction.followup.send(
                "There isn't enough price history for that market to draw a chart "
                "yet.",
                ephemeral=True,
            )
            return

        question = clean_article_text(found.get("question")) or query
        slug = clean_article_text(found.get("slug")) or query
        outcomes = parse_outcomes(found)
        outcome_label = outcomes[0] if outcomes else "Yes"
        try:
            png = render_price_chart(
                question=question,
                slug=slug,
                outcome_label=outcome_label,
                points=points,
            )
        except Exception:
            logger.exception("Could not render a price chart for %s.", slug)
            await interaction.followup.send(
                "Chart rendering isn't available right now; the bot logs have the "
                "details.",
                ephemeral=True,
            )
            return

        latest = points[-1][1]
        first_price = points[0][1]
        embed = discord.Embed(
            title="Price history | Sovereign Predicts",
            description=(
                f"**{question[:350]}**\n\n"
                f"[Open this market]({self.market_page_url(slug)}) · "
                f"last {lookback} day(s) · {outcome_label} {latest:.1%} "
                f"(from {first_price:.1%})"
            ),
            color=SOVEREIGN_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_image(url="attachment://chart.png")
        embed.set_footer(text="Market Intel • Sovereign Predicts")
        await interaction.followup.send(
            embed=embed,
            file=discord.File(io.BytesIO(png), filename="chart.png"),
            ephemeral=True,
        )

    @app_commands.command(
        name="sources",
        description="View or configure which news publishers this server accepts.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.choices(action=[
        app_commands.Choice(name="Show", value="show"),
        app_commands.Choice(name="Add publisher", value="add"),
        app_commands.Choice(name="Remove publisher", value="remove"),
        app_commands.Choice(name="Clear (accept all)", value="clear"),
    ])
    @app_commands.describe(
        value="Comma-separated publisher names or domains (required for add/remove)."
    )
    async def sources_slash(
        self,
        interaction: discord.Interaction,
        action: app_commands.Choice[str],
        value: str | None = None,
    ) -> None:
        if action.value == "show":
            settings = self.get_settings(interaction.guild_id)
            allow_list = ", ".join(settings["sources"]) or "all publishers"
            seen = (
                "\n".join(f"• {source}" for source in self.last_sources)
                or "No results seen yet; they appear after the next news check."
            )
            await interaction.response.send_message(
                (
                    "**Older NewsAPI accounts can filter by publisher; this bot "
                    "filters after fetching so every server can differ.**\n"
                    f"**This server accepts:** {allow_list}\n\n"
                    f"**Publishers in the latest results:**\n{seen}"
                )[:1900],
                ephemeral=True,
            )
            return

        if not await self.require_admin(interaction):
            return
        settings = self.ensure_settings(interaction.guild_id)

        if action.value == "clear":
            settings["sources"] = []
            self.save_settings()
            self.poll_wakeup.set()
            await interaction.response.send_message(
                "Cleared the publisher filter; this server now accepts every source.",
                ephemeral=True,
            )
            return

        if not value or not value.strip():
            await interaction.response.send_message(
                "Provide one or more publisher names or domains, separated by commas.",
                ephemeral=True,
            )
            return

        parts = dedupe_strings(
            [part.strip()[:MAX_KEYWORD_LENGTH] for part in re.split(r"[,\n]", value)]
        )
        if action.value == "add":
            for part in parts:
                if len(settings["sources"]) >= MAX_SOURCES_PER_GUILD:
                    break
                settings["sources"].append(part)
            settings["sources"] = dedupe_strings(settings["sources"])
        else:
            remove = {part.casefold() for part in parts}
            settings["sources"] = [
                source
                for source in settings["sources"]
                if source.casefold() not in remove
            ]
        self.save_settings()
        self.poll_wakeup.set()
        allow_list = ", ".join(settings["sources"]) or "all publishers"
        await interaction.response.send_message(
            f"**This server now accepts:** {allow_list}", ephemeral=True
        )

    def keywords_summary(self, settings: dict[str, Any]) -> str:
        includes = ", ".join(settings["include_keywords"]) or "none"
        excludes = ", ".join(settings["exclude_keywords"]) or "none"
        market_terms = ", ".join(self.last_search_terms) or "none loaded yet"
        return (
            "**Include keywords** — added to the market search. When set, an "
            "article must contain at least one to be posted:\n"
            f"{includes}\n\n"
            "**Exclude keywords** — an article containing any of these is dropped:\n"
            f"{excludes}\n\n"
            f"**Terms currently derived from active markets:**\n{market_terms}"
        )[:1900]

    @app_commands.command(
        name="keywords",
        description="View or manage this server's include and exclude keywords.",
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    @app_commands.choices(action=[
        app_commands.Choice(name="List", value="list"),
        app_commands.Choice(name="Include: add", value="include_add"),
        app_commands.Choice(name="Include: remove", value="include_remove"),
        app_commands.Choice(name="Exclude: add", value="exclude_add"),
        app_commands.Choice(name="Exclude: remove", value="exclude_remove"),
        app_commands.Choice(name="Clear all", value="clear"),
    ])
    @app_commands.describe(words="Comma-separated keywords (required for add/remove).")
    async def keywords_slash(
        self,
        interaction: discord.Interaction,
        action: app_commands.Choice[str],
        words: str | None = None,
    ) -> None:
        if action.value == "list":
            await interaction.response.send_message(
                self.keywords_summary(self.get_settings(interaction.guild_id)),
                ephemeral=True,
            )
            return

        if not await self.require_admin(interaction):
            return
        settings = self.ensure_settings(interaction.guild_id)

        if action.value == "clear":
            settings["include_keywords"] = []
            settings["exclude_keywords"] = []
            self.save_settings()
            self.poll_wakeup.set()
            await interaction.response.send_message(
                "Cleared this server's include and exclude keywords.", ephemeral=True
            )
            return

        if not words or not words.strip():
            await interaction.response.send_message(
                "Provide one or more keywords, separated by commas.", ephemeral=True
            )
            return

        target = (
            "include_keywords"
            if action.value.startswith("include")
            else "exclude_keywords"
        )
        parts = dedupe_strings(
            [part.strip()[:MAX_KEYWORD_LENGTH] for part in re.split(r"[,\n]", words)]
        )
        current = settings[target]
        if action.value.endswith("add"):
            existing = {value.casefold() for value in current}
            for part in parts:
                if len(current) >= MAX_KEYWORDS_PER_GUILD:
                    break
                if part.casefold() not in existing:
                    current.append(part)
                    existing.add(part.casefold())
        else:
            remove = {part.casefold() for part in parts}
            settings[target] = [
                keyword for keyword in current if keyword.casefold() not in remove
            ]
        self.save_settings()
        self.poll_wakeup.set()
        await interaction.response.send_message(
            self.keywords_summary(settings), ephemeral=True
        )

    @app_commands.command(
        name="help",
        description="Show the bot's available slash and prefix commands.",
    )
    async def help_slash(self, interaction: discord.Interaction) -> None:
        message = (
            "**Sovereign Predicts \u2014 commands**\n"
            "**Alerts**\n"
            "\u2022 `/setup` \u2014 administrators pick this server's alert channel "
            "and toggle each alert type.\n"
            "\u2022 `/alerts` \u2014 the price-move, large-trade, and new-market "
            "thresholds.\n"
            "\u2022 `/set-news-alerts`, `/set-price-alerts`, `/set-new-markets`, "
            "`/set-whale-alerts`, `/set-wallet-alerts`, `/set-x-alerts` \u2014 "
            "one category per channel.\n"
            "**Wallet watchlists**\n"
            "\u2022 `/wallet add|remove|list|info|settings|pause|resume` \u2014 track "
            "Polymarket wallets and choose what alerts.\n"
            "**X (Twitter) tracker**\n"
            "\u2022 `/xtrack add|remove|list|info|settings|pause|resume|rename|"
            "test|status` \u2014 watch X accounts and alert on their new posts.\n"
            "**Monitoring**\n"
            "\u2022 `/chart` \u2014 render a market's price history as a PNG.\n"
            "\u2022 `/status` \u2014 per-subsystem health and this server's settings.\n"
            "\u2022 `/test` \u2014 send a test alert to the configured channel.\n"
            "\u2022 `/config-check` \u2014 report broken channels or permissions.\n"
            "**Filters**\n"
            "\u2022 `/sources` \u2014 which publishers this server accepts.\n"
            "\u2022 `/keywords` \u2014 include and exclude keywords.\n"
            "**Bot owner**\n"
            "\u2022 `/sync`, `/servers`, `/server-info`.\n"
            "\u2022 `!status` and `!test` \u2014 legacy prefix commands."
        )
        await interaction.response.send_message(message, ephemeral=True)

    @app_commands.command(
        name="servers",
        description="List all servers this bot is currently installed in (owner only).",
    )
    async def servers_slash(self, interaction: discord.Interaction) -> None:
        if not is_owner(interaction.user.id):
            await interaction.response.send_message(
                "Only the bot owner can use this command.",
                ephemeral=True,
            )
            return

        if not self.guilds:
            await interaction.response.send_message(
                "This bot is not currently in any Discord servers.",
                ephemeral=True,
            )
            return

        guilds = sorted(self.guilds, key=lambda guild: guild.name.lower())
        lines = [f"{guild.name} ({guild.id})" for guild in guilds]
        content = "**Installed servers**\n" + "\n".join(lines[:20])
        if len(lines) > 20:
            content += f"\n... and {len(lines) - 20} more"
        await interaction.response.send_message(content[:1800], ephemeral=True)

    @app_commands.command(
        name="sync",
        description="Re-register this bot's slash commands (owner only).",
    )
    @app_commands.guild_only()
    async def sync_slash(self, interaction: discord.Interaction) -> None:
        if not is_owner(interaction.user.id):
            await interaction.response.send_message(
                "Only the bot owner can use this command.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        lines: list[str] = []

        if interaction.guild_id is not None:
            try:
                synced = await self.sync_guild_commands(interaction.guild_id)
                lines.append(
                    f"Synced {len(synced)} commands to this server (instant)."
                )
            except Exception as exc:
                logger.exception("Manual development-guild sync failed.")
                lines.append(f"Server sync failed: {exc}")

        try:
            synced = await self.tree.sync()
        except Exception as exc:
            logger.exception("Manual global sync failed.")
            self.last_sync_error = str(exc)[:300] or "Unknown sync error."
            lines.append(f"Global sync failed: {exc}")
        else:
            self.last_sync_at = datetime.now(timezone.utc)
            self.last_sync_error = None
            lines.append(
                f"Synced {len(synced)} global commands "
                "(Discord may take up to an hour to propagate)."
            )

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="server-info",
        description="Show details about this server (owner only).",
    )
    @app_commands.guild_only()
    async def server_info_slash(self, interaction: discord.Interaction) -> None:
        if not is_owner(interaction.user.id):
            await interaction.response.send_message(
                "Only the bot owner can use this command.",
                ephemeral=True,
            )
            return

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run this command inside a Discord server.",
                ephemeral=True,
            )
            return

        text_channels = sum(1 for channel in guild.channels if isinstance(channel, discord.TextChannel))
        message = (
            f"**Server:** {guild.name}\n"
            f"**ID:** {guild.id}\n"
            f"**Members:** {guild.member_count}\n"
            f"**Text channels:** {text_channels}\n"
            f"**Owner:** {guild.owner_id if guild.owner_id else 'unknown'}"
        )
        await interaction.response.send_message(message, ephemeral=True)

    async def poll_news_forever(self) -> None:
        if self._monitor_running:
            logger.warning(
                "Monitoring loop is already running; not starting a second one."
            )
            return
        self._monitor_running = True
        try:
            await self.wait_until_ready()
            self.monitor_started_at = datetime.now(timezone.utc)
            logger.info(
                "News monitoring started; polling every %d seconds.",
                self.poll_interval,
            )
            await self._monitor_loop()
        except asyncio.CancelledError:
            logger.info("News monitoring stopped.")
            raise
        finally:
            self._monitor_running = False

    async def _monitor_loop(self) -> None:
        retry_attempt = 0
        while not self.is_closed():
            try:
                await self.run_monitor_cycle()
                retry_attempt = 0
                self.consecutive_failures = 0
                delay = self.poll_interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retry_attempt += 1
                self.consecutive_failures = retry_attempt
                self.last_poll_error = str(exc)[:300] or "Unknown news polling error."
                backoff = 30 * (2 ** min(retry_attempt - 1, 6))
                delay = min(self.poll_interval, backoff)
                logger.exception(
                    "News check failed; retrying in %d seconds (failure %d).",
                    delay,
                    retry_attempt,
                )
            try:
                await asyncio.wait_for(self.poll_wakeup.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            else:
                self.poll_wakeup.clear()

    @staticmethod
    def parse_retry_after(response: aiohttp.ClientResponse) -> float | None:
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return max(0.0, min(float(raw), MAX_RETRY_DELAY_SECONDS))
        except ValueError:
            return None

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
        attempts: int = REQUEST_MAX_ATTEMPTS,
    ) -> Any:
        """GET JSON with bounded retries, honoring Retry-After on throttling.

        Transient failures (timeouts, connection errors, 429 and 5xx responses)
        are retried with exponential backoff. Client errors (4xx other than 429)
        fail immediately, because retrying them cannot help.
        """
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")

        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                async with self.news_session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    if response.status == 429 or response.status >= 500:
                        raise _RetryableRequestError(
                            response.status,
                            self.parse_retry_after(response),
                        )
                    if response.status >= 400:
                        body = (await response.text())[:200]
                        raise RuntimeError(
                            f"{url} returned HTTP {response.status}: {body}"
                        )
                    return await response.json(content_type=None)
            except asyncio.CancelledError:
                raise
            except (_RetryableRequestError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                delay = getattr(exc, "retry_after", None) or min(
                    RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
                    MAX_RETRY_DELAY_SECONDS,
                )
                logger.warning(
                    "Request to %s failed (attempt %d/%d): %s. Retrying in %.1fs.",
                    url,
                    attempt,
                    attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

        raise RuntimeError(
            f"Request to {url} failed after {attempts} attempts: {last_error}"
        ) from last_error

    async def fetch_active_markets(self, order_by: str) -> list[dict[str, Any]]:
        """Fetch one ranked page of active, open markets from Gamma."""
        params = {
            "active": "true",
            "closed": "false",
            "limit": 100,
            "order": order_by,
            "ascending": "false",
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "LashNewsBot/1.0",
        }
        payload = await self.get_json(
            POLYMARKET_MARKETS_URL, params=params, headers=headers, timeout=15
        )

        if not isinstance(payload, list):
            raise RuntimeError("Polymarket returned an unexpected markets response.")
        return [
            market
            for market in payload
            if isinstance(market, dict)
            and market.get("active") is True
            and market.get("closed") is not True
            and market.get("archived") is not True
        ]

    async def run_monitor_cycle(self) -> None:
        """Run every alert type once, isolating one failure from the rest.

        Each category resolves its own destinations first, so a server that
        only configured whale alerts never triggers a news or price fetch.
        """
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")

        destinations_by_alert = await self.get_alert_destinations()
        if not any(destinations_by_alert.values()):
            logger.warning(
                "Monitoring is running but no alert channel is configured; "
                "use /setup or /set-news-alerts in a server."
            )
            self.last_poll_at = datetime.now(timezone.utc)
            self.last_poll_error = None
            return

        subsystems = (
            ("news", "news", self.run_news_cycle),
            ("new markets", "new_market", self.run_new_market_cycle),
            ("price moves", "price", self.run_price_cycle),
            ("large trades", "whale", self.run_whale_cycle),
            ("wallet activity", "wallet", self.run_wallet_cycle),
        )
        failures: list[str] = []
        attempted = 0
        skipped: list[str] = []
        for name, alert, runner in subsystems:
            destinations = destinations_by_alert.get(alert) or {}
            if not destinations:
                # This used to be a debug line, which is why a category that was
                # never pointed at a channel looked exactly like a dead task.
                skipped.append(f"{name} (run {ALERT_CHANNEL_COMMANDS[alert]})")
                continue
            attempted += 1
            try:
                await runner(destinations)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures.append(f"{name}: {exc}")
                logger.exception("The %s monitor failed.", name)

        self.skipped_alerts = skipped
        if skipped:
            logger.warning(
                "%d alert category/ies are not being polled because no channel "
                "is configured for them: %s. Everything else is unaffected.",
                len(skipped),
                "; ".join(skipped),
            )

        self.last_poll_at = datetime.now(timezone.utc)
        self.last_poll_error = "; ".join(failures)[:300] if failures else None
        if attempted and len(failures) == attempted:
            # Every subsystem that had a destination failed, so treat this as an
            # outage and let the outer loop back off instead of hammering the
            # APIs.
            raise RuntimeError(self.last_poll_error)

    def guild_accepts_article(self, guild_id: str, article: dict[str, Any]) -> bool:
        """Apply one server's include/exclude keyword and source filters."""
        settings = self.get_settings(guild_id)
        if not settings["alerts"].get("news", True):
            return False

        haystack = " ".join(
            clean_article_text(article.get(field)).casefold()
            for field in ("title", "description", "content")
        )
        for keyword in settings["exclude_keywords"]:
            if keyword.casefold() in haystack:
                return False

        include_keywords = settings["include_keywords"]
        if include_keywords and not any(
            keyword.casefold() in haystack for keyword in include_keywords
        ):
            return False

        sources = settings["sources"]
        if sources:
            source_name = ""
            source = article.get("source")
            if isinstance(source, dict):
                source_name = clean_article_text(source.get("name")).casefold()
            url = str(article.get("url") or "").casefold()
            if not any(
                configured.casefold() in source_name
                or configured.casefold() in url
                for configured in sources
            ):
                return False
        return True

    async def run_news_cycle(
        self,
        destinations: dict[str, discord.TextChannel | discord.Thread],
    ) -> None:
        if not getattr(self, "news_api_keys", None):
            logger.warning(
                "Skipping the news monitor: NEWS_API_KEY is not set, so no news "
                "alert can be delivered. Set NEWS_API_KEY (or NEWS_API_KEYS) to "
                "re-enable news polling."
            )
            return
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")

        market_lists = await asyncio.gather(
            self.fetch_active_markets("volume24hr"),
            self.fetch_active_markets("volumeNum"),
        )
        self.last_market_count = len(
            {
                str(market.get("slug") or market.get("id") or market.get("question"))
                for markets in market_lists
                for market in markets
            }
        )
        include_keywords = dedupe_strings([
            keyword
            for guild_id in destinations
            for keyword in self.get_settings(guild_id)["include_keywords"]
        ])
        news_query = make_market_news_query(market_lists, tuple(include_keywords))
        logger.info(
            "Searching news from %d active market listings with a %d-character query.",
            self.last_market_count,
            len(news_query),
        )
        self.last_search_terms = re.findall(r'"([^"]+)"', news_query)

        # ---- build the active market catalog once for this poll --------------
        active_markets = self._active_market_catalog(market_lists)
        logger.debug(
            "Active market catalog: %d markets loaded for correlation.",
            len(active_markets),
        )

        # ---- fetch news through the configured provider ----------------------
        provider = self._news_provider()
        raw_articles = await self._fetch_provider_articles(provider, news_query)
        self.last_sources = list(dict.fromkeys(
            clean_article_text(article.get("source", {}).get("name"))
            for article in raw_articles
            if isinstance(article, dict)
            and isinstance(article.get("source"), dict)
            and clean_article_text(article.get("source", {}).get("name"))
        ))[:25]

        # ---- dedupe, filter by server settings, then correlate --------------
        # Every article that does not become an alert is counted under the exact
        # rule that dropped it, so a cycle that found news but posted nothing
        # names the filter responsible instead of looking idle.
        correlated_articles: list[tuple[dict[str, Any], list[MarketMatch], list[str]]] = []
        lookup_failures = 0
        fingerprinted: set[str] = set()
        page_urls: set[str] = set()
        skipped = {
            "malformed": 0,
            "duplicate_url": 0,
            "server_filter": 0,
            "duplicate_content": 0,
            "no_market_match": 0,
        }
        for article in raw_articles:
            if not isinstance(article, dict):
                skipped["malformed"] += 1
                continue
            url = article.get("url")
            if not isinstance(url, str) or not url.startswith(("https://", "http://")):
                skipped["malformed"] += 1
                continue
            if url in page_urls:
                skipped["duplicate_url"] += 1
                continue
            page_urls.add(url)
            eligible_guilds = [
                guild_id
                for guild_id in destinations
                if url not in self.guild_seen_article_urls.get(guild_id, set())
                and self.guild_accepts_article(guild_id, article)
            ]
            if not eligible_guilds:
                skipped["server_filter"] += 1
                continue

            # Persist a content fingerprint so near-identical copies and minor
            # headline variants do not replay as new alerts.
            fp = article_fingerprint(article)
            if fp and fp in fingerprinted:
                skipped["duplicate_content"] += 1
                continue
            if fp:
                fingerprinted.add(fp)

            try:
                match_list = self._correlate_article(article, active_markets)
            except asyncio.CancelledError:
                raise
            except Exception:
                lookup_failures += 1
                logger.exception(
                    "Could not correlate a news article with active markets: %s",
                    clean_article_text(article.get("title"))[:160],
                )
                continue
            if not match_list:
                skipped["no_market_match"] += 1
                continue
            correlated_articles.append((article, match_list, eligible_guilds))

        # ---- post in chronological order -----------------------------------
        posted_count = 0
        for article, match_list, eligible_guilds in reversed(correlated_articles):
            url = article.get("url")
            if not isinstance(url, str):
                continue
            embed = self.make_news_embed(article, match_list)
            for guild_id in eligible_guilds:
                channel = destinations.get(guild_id)
                if channel is None:
                    continue
                if not await self.send_embed(channel, embed, guild_id):
                    continue
                self.record_article_seen(guild_id, url)
                self.articles_sent += 1
                posted_count += 1

        logger.info(
            "News check complete: %d article(s) returned, %d correlated with an "
            "active market, %d posted across servers, %d correlation failure(s), "
            "rejected: %s.",
            len(raw_articles),
            len(correlated_articles),
            posted_count,
            lookup_failures,
            ", ".join(f"{name}={count}" for name, count in skipped.items()),
        )
        if not raw_articles:
            logger.warning(
                "NewsAPI returned no articles for this cycle. The generated query "
                "was %d characters; check the key's plan and daily request limit, "
                "and the query terms (last terms: %s).",
                len(news_query),
                ", ".join(self.last_search_terms[:8]) or "none",
            )
        elif not correlated_articles:
            logger.warning(
                "NewsAPI returned %d article(s) but none cleared the alert bar "
                "(correlation >= %d for a direct or strong indirect match, >= %d "
                "for a contextual one), so nothing was posted.",
                len(raw_articles),
                MIN_CORRELATION_FOR_ALERT,
                MIN_CORRELATION_FOR_ALERT + CONTEXTUAL_ALERT_MARGIN,
            )

    @staticmethod
    def market_page_url(slug: Any) -> str:
        if isinstance(slug, str) and slug.strip():
            return f"{POLYMARKET_MARKET_PAGE}/{quote(slug.strip(), safe='-_')}"
        return "https://polymarket.com"

    def make_market_embed(self, market: dict[str, Any]) -> discord.Embed:
        question = clean_article_text(market.get("question")) or "New Polymarket market"
        liquidity = as_float(market.get("liquidityNum")) or 0.0
        volume = as_float(market.get("volumeNum")) or 0.0
        end_date = parse_published_at(market.get("endDate"))

        embed = discord.Embed(
            title="New Market | Sovereign Predicts",
            description=(
                f"**{question[:350]}**\n\n"
                f"[Open this market]({self.market_page_url(market.get('slug'))})"
            ),
            color=SOVEREIGN_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Liquidity", value=f"${liquidity:,.0f}", inline=True)
        embed.add_field(name="Total volume", value=f"${volume:,.0f}", inline=True)
        if end_date is not None:
            embed.add_field(
                name="Closes",
                value=discord.utils.format_dt(end_date, style="f"),
                inline=True,
            )
        image = market.get("icon") or market.get("image")
        if isinstance(image, str) and image.startswith("https://"):
            embed.set_thumbnail(url=image)
        embed.set_footer(text="Market Intel • Sovereign Predicts")
        return embed

    def make_price_embed(
        self,
        market: dict[str, Any],
        prices: list[float],
        outcomes: list[str],
        move_points: float,
    ) -> discord.Embed:
        question = clean_article_text(market.get("question")) or "Polymarket market"
        current = prices[0] if prices else 0.0
        label = outcomes[0] if outcomes else "Yes"
        rising = move_points > 0

        odds_lines = [
            f"• {outcomes[index] if index < len(outcomes) else f'Outcome {index + 1}'}: {price:.1%}"
            for index, price in enumerate(prices[:6])
        ]
        embed = discord.Embed(
            title=("📈 Market move | Sovereign Predicts" if rising else "📉 Market move | Sovereign Predicts"),
            description=(
                f"**{question[:350]}**\n\n"
                f"[Open this market]({self.market_page_url(market.get('slug'))})"
            ),
            color=discord.Color.green() if rising else discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(
            name="Current odds",
            value="\n".join(odds_lines) or f"{current:.1%}",
            inline=False,
        )
        embed.add_field(
            name=f"{label} moved",
            value=f"{move_points:+.1f} points",
            inline=True,
        )
        embed.add_field(name=f"{label} now", value=f"{current:.1%}", inline=True)
        volume_24h = as_float(market.get("volume24hr"))
        if volume_24h is not None:
            embed.add_field(
                name="24h volume", value=f"${volume_24h:,.0f}", inline=True
            )
        embed.set_footer(text="Market Intel • Sovereign Predicts")
        return embed

    def make_whale_embed(
        self, trade: dict[str, Any], notional: float
    ) -> discord.Embed:
        title = clean_article_text(trade.get("title")) or "Polymarket market"
        outcome = clean_article_text(trade.get("outcome")) or "unknown outcome"
        side = str(trade.get("side") or "?").upper()
        size = as_float(trade.get("size")) or 0.0
        price = as_float(trade.get("price")) or 0.0
        wallet = str(
            trade.get("proxy_wallet") or trade.get("proxyWallet") or ""
        )
        transaction = str(
            trade.get("transaction_hash") or trade.get("transactionHash") or ""
        )
        timestamp = as_float(trade.get("timestamp"))
        occurred_at = (
            datetime.fromtimestamp(timestamp, tz=timezone.utc)
            if timestamp
            else datetime.now(timezone.utc)
        )

        embed = discord.Embed(
            title="🐋 Large trade | Sovereign Predicts",
            description=(
                f"**{side} {outcome}** on {title[:250]}\n\n"
                f"[Open this market]({self.market_page_url(trade.get('slug'))})"
            ),
            color=WHALE_SELL_COLOR if side == "SELL" else WHALE_BUY_COLOR,
            timestamp=occurred_at,
        )
        embed.add_field(name="Value", value=f"${notional:,.0f}", inline=True)
        embed.add_field(name="Size", value=f"{size:,.2f} shares", inline=True)
        embed.add_field(name="Price", value=f"{price:.3f}", inline=True)
        if len(wallet) > 12:
            embed.add_field(
                name="Wallet", value=f"`{wallet[:6]}…{wallet[-4:]}`", inline=True
            )
        if transaction:
            embed.add_field(
                name="Transaction",
                value=f"[View on Polygonscan]({POLYGON_EXPLORER_TX}/{transaction})",
                inline=True,
            )
        embed.add_field(
            name="Fill type",
            value="Executed taker trade (matched fill) — not an open order or a position.",
            inline=False,
        )
        embed.set_footer(text="Market Intel • Sovereign Predicts")
        return embed

    @staticmethod
    def trade_key(trade: dict[str, Any]) -> str | None:
        """A stable per-fill identity so one fill is never alerted twice."""
        transaction = trade.get("transaction_hash") or trade.get("transactionHash")
        if not transaction:
            return None
        parts = (
            transaction,
            trade.get("proxy_wallet") or trade.get("proxyWallet"),
            trade.get("token_id") or trade.get("asset"),
            trade.get("side"),
            trade.get("size"),
            trade.get("price"),
            trade.get("timestamp"),
        )
        return "|".join(str(part) for part in parts)

    @staticmethod
    def trade_notional(trade: dict[str, Any]) -> float | None:
        size = as_float(trade.get("size"))
        price = as_float(trade.get("price"))
        if size is None or price is None:
            return None
        return size * price


    def guild_accepts_wallet_event(
        self, watch: dict[str, Any], event: dict[str, Any]
    ) -> bool:
        """Apply one watch's activity-type and value filters."""
        activity_filter = watch.get("activity_filter", "all")
        if activity_filter == "trades" and wallet_activity_type(event) != "TRADE":
            return False
        threshold = as_float(watch.get("min_usd")) or 0.0
        if threshold <= 0:
            return True
        value = wallet_event_value(event)
        return value is not None and value >= threshold

    async def fetch_wallet_activity(self, address: str) -> list[dict[str, Any]]:
        """One descending page of a wallet's most recent activity rows."""
        payload = await self.get_json(
            POLYMARKET_WALLET_ACTIVITY_URL,
            params={
                "user": address,
                "limit": WALLET_ACTIVITY_PAGE_SIZE,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            },
            headers={"Accept": "application/json", "User-Agent": "LashNewsBot/1.0"},
            timeout=15,
        )
        if isinstance(payload, dict):
            payload = payload.get("data")
        if not isinstance(payload, list):
            raise RuntimeError(
                "Polymarket returned an unexpected wallet activity response."
            )
        return [event for event in payload if isinstance(event, dict)]

    async def run_wallet_cycle(
        self,
        destinations: dict[str, discord.TextChannel | discord.Thread],
    ) -> None:
        """Alert on new activity for every wallet a server watches.

        Each distinct address is fetched once per cycle however many servers
        track it, and the address count is hard-capped so a large watchlist can
        never turn one cycle into an unbounded burst of API calls. The first
        poll of a newly added wallet only records a baseline, so adding a busy
        wallet does not dump its history into the channel.
        """
        watches_by_guild: dict[str, list[dict[str, Any]]] = {}
        for guild_id in destinations:
            if not self.get_settings(guild_id)["alerts"].get("wallet", True):
                continue
            watches = self.list_watches(guild_id)
            if watches:
                watches_by_guild[guild_id] = watches
        if not watches_by_guild:
            self.last_wallet_check_at = datetime.now(timezone.utc)
            self.last_wallet_error = None
            return

        addresses = sorted(
            {
                watch["address"]
                for watches in watches_by_guild.values()
                for watch in watches
            }
        )
        if len(addresses) > MAX_WALLET_ADDRESSES_PER_CYCLE:
            logger.warning(
                "The watchlist has %d distinct wallets; only the first %d are "
                "checked this cycle.",
                len(addresses),
                MAX_WALLET_ADDRESSES_PER_CYCLE,
            )
            addresses = addresses[:MAX_WALLET_ADDRESSES_PER_CYCLE]

        events_by_address: dict[str, list[dict[str, Any]]] = {}
        failures = 0
        for address in addresses:
            try:
                events_by_address[address] = await self.fetch_wallet_activity(address)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                events_by_address[address] = []
                logger.warning(
                    "Wallet activity lookup failed for %s: %s",
                    short_wallet_address(address),
                    exc,
                )

        self.last_wallet_activity_count = sum(
            len(events) for events in events_by_address.values()
        )
        alerted = 0
        posted_by_guild: dict[str, int] = {}
        for guild_id, watches in watches_by_guild.items():
            channel = destinations.get(guild_id)
            if channel is None:
                continue
            for watch in watches:
                if watch.get("paused"):
                    continue
                events = events_by_address.get(watch["address"]) or []
                if not events:
                    continue
                timestamps = [
                    value
                    for value in (as_float(event.get("timestamp")) for event in events)
                    if value is not None
                ]
                newest = max(timestamps) if timestamps else 0.0
                since = as_float(watch.get("last_seen_ts")) or 0.0
                if since <= 0:
                    watch["last_seen_ts"] = newest
                    continue
                seen = list(dict.fromkeys(watch.get("seen") or []))
                seen_set = set(seen)
                for event in sorted(
                    events,
                    key=lambda item: as_float(item.get("timestamp")) or 0.0,
                ):
                    timestamp = as_float(event.get("timestamp")) or 0.0
                    if timestamp <= since:
                        continue
                    key = wallet_activity_key(event)
                    if key is not None:
                        if key in seen_set:
                            continue
                        seen_set.add(key)
                        seen.append(key)
                    if not self.guild_accepts_wallet_event(watch, event):
                        continue
                    if posted_by_guild.get(guild_id, 0) >= MAX_ALERTS_PER_CYCLE:
                        continue
                    embed = self.make_wallet_embed(event, watch)
                    if await self.send_embed(channel, embed, guild_id):
                        posted_by_guild[guild_id] = posted_by_guild.get(guild_id, 0) + 1
                        alerted += 1
                        self.wallet_alerts_sent += 1
                watch["seen"] = seen[-MAX_WALLET_SEEN_EVENTS:]
                if newest > since:
                    watch["last_seen_ts"] = newest

        self.save_wallets()
        self.last_wallet_check_at = datetime.now(timezone.utc)
        self.last_wallet_error = (
            f"{failures} of {len(addresses)} wallet lookups failed" if failures else None
        )
        logger.info(
            "Wallet check complete: %d wallet(s) polled, %d activity row(s) seen, "
            "%d alert(s) posted, %d lookup failure(s).",
            len(addresses),
            self.last_wallet_activity_count,
            alerted,
            failures,
        )

    # ------------------------------------------------------------------
    # X account watchlists
    # ------------------------------------------------------------------

    def x_guild_state(self, guild_id: int | str) -> dict[str, Any]:
        """One server's X watchlist entry, created in memory when absent."""
        key = str(guild_id)
        state = self.x_state.get(key)
        if state is None or not isinstance(state.get("accounts"), dict):
            state = {"accounts": {}}
            self.x_state[key] = state
        return state

    def get_x_account(
        self, guild_id: int | str, username: str
    ) -> dict[str, Any] | None:
        normalized = normalize_x_username(username)
        if normalized is None:
            return None
        return self.x_guild_state(guild_id)["accounts"].get(normalized)

    def list_x_accounts(self, guild_id: int | str) -> list[dict[str, Any]]:
        accounts = self.x_guild_state(guild_id)["accounts"]
        return sorted(
            accounts.values(), key=lambda account: account["username"].casefold()
        )

    def set_x_account(self, guild_id: int | str, account: dict[str, Any]) -> bool:
        self.x_guild_state(guild_id)["accounts"][
            account["username"].casefold()
        ] = account
        return self.save_x_accounts()

    def drop_x_account(self, guild_id: int | str, username: str) -> bool:
        normalized = normalize_x_username(username)
        if normalized is None:
            return False
        return (
            self.x_guild_state(guild_id)["accounts"].pop(normalized, None)
            is not None
        )

    def x_limit_reached(self, guild_id: int | str) -> bool:
        return (
            len(self.list_x_accounts(guild_id)) >= MAX_TRACKED_X_ACCOUNTS_PER_GUILD
        )

    def prune_empty_guild_x_accounts(self, guild_id: int | str) -> None:
        key = str(guild_id)
        state = self.x_state.get(key)
        if state is not None and not state.get("accounts"):
            self.x_state.pop(key, None)

    def save_x_accounts(self) -> bool:
        try:
            save_x_state(self.x_state)
            return True
        except OSError:
            logger.exception("Could not save the X watchlist.")
            return False

    def tracked_x_accounts_by_user_id(
        self,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """{user_id: {guild_id: account}} for every resolved tracked account."""
        by_user: dict[str, dict[str, dict[str, Any]]] = {}
        for guild_id, state in self.x_state.items():
            for account in state.get("accounts", {}).values():
                user_id = account.get("user_id")
                if not user_id:
                    continue
                by_user.setdefault(user_id, {})[guild_id] = account
        return by_user

    # ------------------------------------------------------------------
    # X API access (official v2, app-only bearer token)
    # ------------------------------------------------------------------

    def x_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.x_api_bearer}",
            "Accept": "application/json",
            "User-Agent": "LashNewsBot/1.0",
        }

    @staticmethod
    def x_http_status(exc: Exception) -> int | None:
        """The HTTP status embedded in a get_json failure message, if any."""
        match = re.search(r"\bHTTP (\d{3})\b", str(exc))
        return int(match.group(1)) if match else None

    def x_api_error_from(self, exc: Exception, subject: str) -> XApiError:
        """Explain an X API failure in operator terms."""
        status = self.x_http_status(exc)
        if status == 404:
            return XApiError(
                404,
                f"@{subject} was not found on X. Check the handle; suspended "
                "or deleted accounts cannot be tracked.",
                transient=False,
            )
        if status == 401:
            return XApiError(
                401,
                "X rejected the configured API token (HTTP 401). Set a valid "
                "X_API_BEARER_TOKEN in the hosting environment.",
                transient=False,
            )
        if status == 403:
            return XApiError(
                403,
                f"@{subject} is protected or not readable with the configured "
                "X API access.",
                transient=False,
            )
        if status == 429:
            return XApiError(
                429,
                "X rate-limited the request; it will be retried next cycle.",
                transient=True,
            )
        if "not found error" in str(exc).casefold():
            # X sometimes reports a missing user as HTTP 400 with a
            # "Not Found Error" body; retrying it forever would never help.
            return XApiError(
                status,
                f"@{subject} was not found on X. Check the handle; suspended "
                "or deleted accounts cannot be tracked.",
                transient=False,
            )
        if status is not None and status >= 500:
            return XApiError(
                status,
                "X is having a temporary problem; it will be retried next cycle.",
                transient=True,
            )
        return XApiError(
            status, str(exc)[:200] or "Unknown X API error.", transient=True
        )

    async def fetch_x_user(self, username: str) -> dict[str, Any]:
        """Resolve a handle to its X user object, or raise XApiError."""
        try:
            payload = await self.get_json(
                f"{X_API_BASE_URL}/users/by/username/{username}",
                params={"user.fields": "profile_image_url,protected,verified"},
                headers=self.x_headers(),
                timeout=15,
            )
        except RuntimeError as exc:
            raise self.x_api_error_from(exc, username) from exc
        user = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(user, dict) or not str(user.get("id", "")).isdigit():
            raise XApiError(
                None, "X returned an unexpected user response.", transient=True
            )
        return user

    async def fetch_x_posts(
        self, user_id: str, since_id: str = ""
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """One page of a user's recent posts plus the expanded includes.

        Returns ``(posts, includes)`` where includes carries the media and
        referenced-post objects needed to render rich alerts. Only posts newer
        than ``since_id`` are requested, so a quiet account returns nothing
        and costs nothing on X's pay-per-use billing.
        """
        params: dict[str, str] = {
            "max_results": str(MAX_X_POSTS_PER_POLL),
            "tweet.fields": (
                "created_at,public_metrics,attachments,referenced_tweets"
            ),
            "expansions": (
                "attachments.media_keys,referenced_tweets.id,"
                "referenced_tweets.id.author_id"
            ),
            "media.fields": "url,preview_image_url,type",
            "user.fields": "profile_image_url,name,username",
        }
        if since_id:
            params["since_id"] = since_id
        try:
            payload = await self.get_json(
                f"{X_API_BASE_URL}/users/{user_id}/tweets",
                params=params,
                headers=self.x_headers(),
                timeout=15,
            )
        except RuntimeError as exc:
            raise self.x_api_error_from(exc, f"id:{user_id}") from exc
        if not isinstance(payload, dict):
            raise XApiError(
                None, "X returned an unexpected timeline response.", transient=True
            )
        posts = payload.get("data")
        if posts is None:
            # An empty timeline answer: no "data" key at all.
            includes = payload.get("includes")
            return [], includes if isinstance(includes, dict) else {}
        if not isinstance(posts, list):
            raise XApiError(
                None, "X returned an unexpected timeline response.", transient=True
            )
        includes = payload.get("includes")
        return (
            [post for post in posts if isinstance(post, dict)],
            includes if isinstance(includes, dict) else {},
        )

    async def resolve_x_account(self, account: dict[str, Any]) -> bool:
        """Ensure one account has a resolved user id; False when it cannot."""
        if account.get("user_id"):
            return True
        try:
            user = await self.fetch_x_user(account["username"])
        except XApiError as exc:
            account["last_error"] = str(exc)[:200]
            if not exc.transient:
                logger.warning(
                    "X account @%s cannot be resolved: %s",
                    account["username"],
                    exc,
                )
            return False
        account["user_id"] = str(user.get("id") or "")
        account["display_name"] = clean_article_text(user.get("name"))
        account["profile_image_url"] = clean_article_text(
            user.get("profile_image_url")
        )
        account["last_error"] = ""
        return bool(account["user_id"])

    # ------------------------------------------------------------------
    # The X monitoring cycle
    # ------------------------------------------------------------------

    async def poll_x_forever(self) -> None:
        """The X monitoring task: poll until cancelled or the bot closes."""
        if self._x_monitor_running:
            logger.warning(
                "The X monitoring loop is already running; not starting a "
                "second one."
            )
            return
        self._x_monitor_running = True
        try:
            await self.wait_until_ready()
            self.x_monitor_started_at = datetime.now(timezone.utc)
            if not self.x_api_bearer:
                logger.warning(
                    "X tracking is disabled: X_API_BEARER_TOKEN is not set in "
                    "the hosting environment. Set it and restart to enable "
                    "X account alerts."
                )
                self.last_x_error = (
                    "X_API_BEARER_TOKEN is not set; X tracking is disabled."
                )
                return
            logger.info(
                "X tracking started; polling every %d seconds.",
                self.x_poll_interval,
            )
            await self._x_monitor_loop()
        except asyncio.CancelledError:
            logger.info("X monitoring stopped.")
            raise
        finally:
            self._x_monitor_running = False

    async def _x_monitor_loop(self) -> None:
        retry_attempt = 0
        while not self.is_closed():
            try:
                await self.run_x_cycle()
                retry_attempt = 0
                delay = self.x_poll_interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retry_attempt += 1
                self.last_x_error = str(exc)[:300] or "Unknown X polling error."
                backoff = 30 * (2 ** min(retry_attempt - 1, 6))
                delay = min(self.x_poll_interval, backoff)
                logger.exception(
                    "X check failed; retrying in %d seconds (failure %d).",
                    delay,
                    retry_attempt,
                )
            try:
                await asyncio.wait_for(self.x_poll_wakeup.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            else:
                self.x_poll_wakeup.clear()

    async def run_x_cycle(self) -> None:
        """Poll every tracked X account once and deliver the new posts.

        Each distinct user id is fetched once per cycle however many servers
        track it, using the newest since_id across those servers, so one
        account never costs more than one timeline request per cycle. Every
        server still applies its own filters, dedupe history, and channel.
        """
        if not self.x_api_bearer:
            self.last_x_check_at = datetime.now(timezone.utc)
            self.last_x_error = (
                "X_API_BEARER_TOKEN is not set; X tracking is disabled. "
                "Set it in the hosting environment and restart."
            )
            return

        try:
            destinations = await self.get_alert_channels("x")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Could not resolve the X alert channels this cycle.")
            destinations = {}

        accounts_by_guild: dict[str, list[dict[str, Any]]] = {}
        for guild_id, channel in destinations.items():
            if channel is None:
                continue
            if not self.get_settings(guild_id)["alerts"].get("x", True):
                continue
            active = [
                account
                for account in self.list_x_accounts(guild_id)
                if not account.get("paused")
            ]
            if active:
                accounts_by_guild[guild_id] = active

        # An account with its own channel override is polled even when its
        # server has no default X alerts channel configured.
        for guild_id in list(self.x_state.keys()):
            if guild_id in accounts_by_guild:
                continue
            if not self.get_settings(guild_id)["alerts"].get("x", True):
                continue
            override_accounts = [
                account
                for account in self.list_x_accounts(guild_id)
                if not account.get("paused") and account.get("channel_id")
            ]
            if override_accounts:
                accounts_by_guild[guild_id] = override_accounts

        if not accounts_by_guild:
            self.last_x_check_at = datetime.now(timezone.utc)
            self.last_x_error = None
            return

        # Resolve any handle that has not been mapped to a user id yet.
        for guild_accounts in accounts_by_guild.values():
            for account in guild_accounts:
                if not account.get("user_id"):
                    await self.resolve_x_account(account)

        poll_targets: dict[str, dict[str, dict[str, Any]]] = {}
        for user_id, guild_map in self.tracked_x_accounts_by_user_id().items():
            eligible = {
                guild_id: account
                for guild_id, account in guild_map.items()
                if guild_id in accounts_by_guild
            }
            if eligible:
                poll_targets[user_id] = eligible

        if not poll_targets:
            self.last_x_check_at = datetime.now(timezone.utc)
            self.last_x_error = None
            return

        if len(poll_targets) > MAX_X_ACCOUNTS_PER_CYCLE:
            logger.warning(
                "The X watchlist has %d distinct accounts; only the first %d "
                "are polled this cycle.",
                len(poll_targets),
                MAX_X_ACCOUNTS_PER_CYCLE,
            )
            poll_targets = dict(
                sorted(poll_targets.items())[:MAX_X_ACCOUNTS_PER_CYCLE]
            )

        failures = 0
        posts_seen = 0
        alerted = 0
        posted_by_guild: dict[str, int] = {}
        for user_id, guild_map in poll_targets.items():
            best_since = ""
            for account in guild_map.values():
                since = account.get("since_id") or ""
                if since and (not best_since or int(since) > int(best_since)):
                    best_since = since
            try:
                posts, includes = await self.fetch_x_posts(user_id, best_since)
            except XApiError as exc:
                failures += 1
                for account in guild_map.values():
                    account["last_error"] = str(exc)[:200]
                logger.warning(
                    "X timeline lookup failed for user %s: %s", user_id, exc
                )
                continue

            posts_seen += len(posts)
            newest_id = ""
            for post in posts:
                post_id = str(post.get("id") or "")
                if post_id.isdigit() and (
                    not newest_id or int(post_id) > int(newest_id)
                ):
                    newest_id = post_id
            # Newest first, so a burst arrives in reading order.
            ordered = sorted(
                posts,
                key=lambda post: int(str(post.get("id") or "0")),
                reverse=True,
            )

            for guild_id, account in guild_map.items():
                since = account.get("since_id") or ""
                if not since:
                    # First poll of this account for this server: record a
                    # baseline of the newest post so its existing history is
                    # never dumped into the channel.
                    if newest_id:
                        account["since_id"] = newest_id
                    if newest_id:
                        account["last_success_ts"] = (
                            datetime.now(timezone.utc).timestamp()
                        )
                    continue
                seen = list(dict.fromkeys(str(v) for v in account.get("seen") or []))
                seen_set = set(seen)
                delivered = 0
                # If anything could not be delivered, the high-water mark must
                # not advance past it or that post would be skipped forever.
                undelivered = False
                for post in ordered:
                    post_id = str(post.get("id") or "")
                    if not post_id or not x_post_is_newer(post_id, since):
                        continue
                    if post_id in seen_set:
                        continue
                    activity = x_post_activity(post)
                    if not x_post_passes_filters(account, post, activity):
                        # Remember it anyway, so changing filters later does
                        # not replay old posts.
                        seen_set.add(post_id)
                        seen.append(post_id)
                        continue
                    if delivered >= MAX_X_ALERTS_PER_CYCLE or (
                        posted_by_guild.get(guild_id, 0)
                        >= MAX_X_ALERTS_PER_CYCLE
                    ):
                        undelivered = True
                        break
                    channel = destinations.get(guild_id)
                    override_id = account.get("channel_id")
                    if isinstance(override_id, int) and override_id > 0:
                        try:
                            resolved = await self.resolve_channel(override_id)
                        except Exception:
                            logger.warning(
                                "Could not resolve the X alert channel %s for "
                                "@%s; using the server's X channel.",
                                override_id,
                                account["username"],
                            )
                        else:
                            if str(resolved.guild.id) == guild_id:
                                channel = resolved
                    if channel is None:
                        undelivered = True
                        break
                    embed = self.make_x_embed(post, account, activity, includes)
                    if await self.send_embed(channel, embed, guild_id):
                        delivered += 1
                        alerted += 1
                        self.x_alerts_sent += 1
                        posted_by_guild[guild_id] = (
                            posted_by_guild.get(guild_id, 0) + 1
                        )
                        seen_set.add(post_id)
                        seen.append(post_id)
                    else:
                        undelivered = True
                        break
                account["seen"] = seen[-MAX_X_SEEN_POSTS:]
                if not undelivered and newest_id and (
                    not since or int(newest_id) > int(since)
                ):
                    account["since_id"] = newest_id
                if delivered or newest_id:
                    account["last_success_ts"] = (
                        datetime.now(timezone.utc).timestamp()
                    )
                if not failures:
                    account["last_error"] = ""

        self.save_x_accounts()
        self.x_posts_seen = posts_seen
        self.last_x_check_at = datetime.now(timezone.utc)
        self.last_x_error = (
            f"{failures} of {len(poll_targets)} X lookups failed"
            if failures
            else None
        )
        logger.info(
            "X check complete: %d account(s) polled, %d post(s) seen, "
            "%d alert(s) posted, %d lookup failure(s).",
            len(poll_targets),
            posts_seen,
            alerted,
            failures,
        )

    def make_x_embed(
        self,
        post: dict[str, Any],
        account: dict[str, Any],
        activity: str,
        includes: dict[str, Any],
    ) -> discord.Embed:
        """Build the alert embed for one detected post."""
        username = account["username"]
        display = account.get("display_name") or username
        label = clean_article_text(account.get("label"))
        post_id = str(post.get("id") or "")
        url = f"{X_PROFILE_URL}/{username}/status/{post_id}"

        label_text = X_ACTIVITY_TYPE_LABELS.get(activity, "New post")
        embed = discord.Embed(
            title=f"{label_text} \u00b7 @{username}",
            url=url,
            description=clean_article_text(post.get("text"))[:4000] or None,
            color=SOVEREIGN_COLOR,
        )
        embed.set_author(
            name=f"{display} (@{username})" + (f" \u2014 {label}" if label else ""),
            url=f"{X_PROFILE_URL}/{username}",
            icon_url=account.get("profile_image_url") or None,
        )

        created = parse_published_at(post.get("created_at"))
        if created is not None:
            embed.timestamp = created

        # Media: the first photo is attached as the embed image; anything
        # else (videos, extra photos) is linked so nothing is silently lost.
        media_urls: list[str] = []
        media_by_key = {
            item.get("media_key"): item
            for item in includes.get("media", [])
            if isinstance(item, dict)
        }
        attachments = post.get("attachments")
        media_keys = (
            attachments.get("media_keys") if isinstance(attachments, dict) else None
        ) or []
        for key in media_keys:
            item = media_by_key.get(key)
            if not isinstance(item, dict):
                continue
            media_url = item.get("url") or item.get("preview_image_url")
            if not media_url:
                continue
            if item.get("type") in ("photo", "animated_gif") and not (
                embed.image and embed.image.url
            ):
                embed.set_image(url=str(media_url))
            else:
                media_urls.append(str(media_url))
        if media_urls:
            embed.add_field(
                name="Media",
                value="\n".join(f"[Attachment]({link})" for link in media_urls[:4]),
                inline=False,
            )

        # The referenced post for quotes/reposts, or the parent for replies.
        refs = post.get("referenced_tweets")
        ref_ids = {
            str(ref.get("id"))
            for ref in refs
            if isinstance(ref, dict) and ref.get("id")
        } if isinstance(refs, list) else set()
        users_by_id = {
            str(user.get("id")): user
            for user in includes.get("users", [])
            if isinstance(user, dict)
        }
        for ref_post in includes.get("tweets", []):
            if (
                not isinstance(ref_post, dict)
                or str(ref_post.get("id")) not in ref_ids
            ):
                continue
            ref_author = users_by_id.get(str(ref_post.get("author_id")))
            ref_handle = (
                ref_author.get("username")
                if isinstance(ref_author, dict)
                else None
            )
            ref_name = f"@{ref_handle}" if ref_handle else "an earlier post"
            ref_text = clean_article_text(ref_post.get("text"))[:700]
            if activity == "quote":
                field_name = "Quoted post"
            elif activity == "repost":
                field_name = "Reposted post"
            else:
                field_name = "Replying to"
            embed.add_field(
                name=field_name,
                value=f"{ref_name}: {ref_text}" if ref_text else ref_name,
                inline=False,
            )
            break

        metrics = post.get("public_metrics")
        if isinstance(metrics, dict):
            parts = []
            for key, noun in (
                ("like_count", "likes"),
                ("retweet_count", "reposts"),
                ("reply_count", "replies"),
                ("quote_count", "quotes"),
            ):
                count = metrics.get(key)
                if isinstance(count, int) and count:
                    parts.append(f"{count:,} {noun}")
            if parts:
                embed.set_footer(text=" \u00b7 ".join(parts))

        # Optional Polymarket convenience: offer market-search links for the
        # tokens this post names. This is a search shortcut, not a claim that
        # the post affects any market's outcome or price.
        post_text = clean_article_text(post.get("text"))
        symbols = x_cashtag_symbols(post_text)
        if symbols:
            links = " ".join(
                f"[${symbol}](https://polymarket.com/search?_q="
                f"{quote(symbol)})"
                for symbol in symbols
            )
            embed.add_field(
                name="Polymarket", value=f"Search markets: {links}", inline=False
            )
        elif "polymarket" in post_text.casefold():
            embed.add_field(
                name="Polymarket",
                value="[Browse markets](https://polymarket.com/markets)",
                inline=False,
            )
        return embed

    def whale_alert_is_duplicate(self, guild_id: str, wallet: Any) -> bool:
        """True when this server already gets a wallet alert for the same fill.

        A tracked wallet's large fill would otherwise post twice: once through
        the global whale monitor and once through the wallet monitor. The
        per-watch ``suppress_whale`` flag (on by default) turns the whale copy
        off; setting it to false keeps both.
        """
        if not isinstance(wallet, str):
            return False
        watch = self.get_watch(guild_id, wallet)
        if watch is None or watch.get("paused"):
            return False
        if not self.get_settings(guild_id)["alerts"].get("wallet", True):
            return False
        return bool(watch.get("suppress_whale", True))

    def make_wallet_embed(
        self, event: dict[str, Any], watch: dict[str, Any]
    ) -> discord.Embed:
        """Build the tracked-wallet alert embed for one activity row."""
        address = watch["address"]
        label = watch.get("label") or short_wallet_address(address)
        activity_type = wallet_activity_type(event)
        title = clean_article_text(event.get("title")) or "Polymarket market"
        outcome = clean_article_text(event.get("outcome")) or "unknown outcome"
        side = clean_article_text(event.get("side")).upper()
        value = wallet_event_value(event)
        size = as_float(event.get("size"))
        price = as_float(event.get("price"))
        transaction = event.get("transactionHash") or event.get("transaction_hash")
        timestamp = as_float(event.get("timestamp"))
        occurred_at = (
            datetime.fromtimestamp(timestamp, tz=timezone.utc)
            if timestamp
            else datetime.now(timezone.utc)
        )

        if activity_type == "TRADE" and side == "SELL":
            color = WHALE_SELL_COLOR
        elif activity_type == "TRADE":
            color = WHALE_BUY_COLOR
        else:
            color = SOVEREIGN_COLOR

        headline = (
            f"{side} {outcome}".strip()
            if activity_type == "TRADE"
            else activity_type.title()
        )
        embed = discord.Embed(
            title=f"\U0001F441 Tracked wallet | {label}",
            description=(
                f"**{headline}** on {title[:250]}\n\n"
                f"[Open this market]({wallet_event_market_url(event)})"
            ),
            color=color,
            timestamp=occurred_at,
        )
        if value is not None:
            embed.add_field(
                name="Transaction value", value=f"${value:,.2f}", inline=True
            )
        if size is not None:
            embed.add_field(name="Size", value=f"{size:,.2f} shares", inline=True)
        if price is not None:
            embed.add_field(name="Price", value=f"{price:.3f}", inline=True)
        embed.add_field(name="Activity", value=activity_type.title(), inline=True)
        embed.add_field(
            name="Wallet",
            value=f"`{short_wallet_address(address)}`",
            inline=True,
        )
        if transaction:
            embed.add_field(
                name="Transaction",
                value=f"[View on Polygonscan]({POLYGON_EXPLORER_TX}/{transaction})",
                inline=True,
            )
        embed.add_field(
            name="Scope",
            value=(
                "Value of this single activity row. It is not the wallet's total "
                "position, ownership, or intent."
            ),
            inline=False,
        )
        embed.set_footer(text="Market Intel \u2022 Sovereign Predicts")
        return embed

    async def run_new_market_cycle(
        self,
        destinations: dict[str, discord.TextChannel | discord.Thread],
    ) -> None:
        interested = [
            (guild_id, channel)
            for guild_id, channel in destinations.items()
            if self.get_settings(guild_id)["alerts"].get("new_market", True)
        ]
        if not interested:
            return

        payload = await self.get_json(
            POLYMARKET_MARKETS_URL,
            params={
                "closed": "false",
                "limit": NEW_MARKET_LOOKBACK_LIMIT,
                "order": "createdAt",
                "ascending": "false",
            },
            headers={"Accept": "application/json", "User-Agent": "LashNewsBot/1.0"},
            timeout=15,
        )
        if not isinstance(payload, list):
            raise RuntimeError("Polymarket returned an unexpected markets response.")

        # On the very first successful run the whole lookback page looks "new";
        # record it silently so deploying does not dump a wall of old markets.
        seeding = not self.monitor_state.get("new_markets_seeded")
        new_ids: list[str] = []
        posted = 0
        for market in reversed(payload):
            if not isinstance(market, dict):
                continue
            market_id = str(market.get("id") or "")
            if not market_id or market_id in self.seen_market_ids:
                continue
            new_ids.append(market_id)
            if seeding:
                continue

            slug = market.get("slug")
            question = clean_article_text(market.get("question"))
            if (
                market.get("active") is not True
                or market.get("closed") is True
                or market.get("archived") is True
                or not isinstance(slug, str)
                or not slug.strip()
                or not question
            ):
                continue

            liquidity = as_float(market.get("liquidityNum")) or 0.0
            eligible = [
                (guild_id, channel)
                for guild_id, channel in interested
                if liquidity
                >= self.get_settings(guild_id)["new_market_min_liquidity"]
            ]
            if not eligible or posted >= MAX_ALERTS_PER_CYCLE:
                continue

            embed = self.make_market_embed(market)
            for guild_id, channel in eligible:
                await self.send_embed(channel, embed, guild_id)
            posted += 1

        self.remember_market_ids(new_ids)
        self.monitor_state["new_markets_seeded"] = True
        self.persist_monitor_state()
        logger.info(
            "New-market check complete: %d new markets seen, %d posted.",
            len(new_ids),
            posted,
        )

    async def run_price_cycle(
        self,
        destinations: dict[str, discord.TextChannel | discord.Thread],
    ) -> None:
        tracked: list[tuple[str, discord.TextChannel | discord.Thread, float]] = []
        for guild_id, channel in destinations.items():
            settings = self.get_settings(guild_id)
            if not settings["alerts"].get("price", True):
                continue
            tracked.append((guild_id, channel, float(settings["price_move_threshold"])))
        if not tracked:
            return

        markets = await self.fetch_active_markets("volume24hr")
        watched = [m for m in markets if isinstance(m, dict)][:WATCHED_MARKET_LIMIT]
        watched_ids = {str(m.get("id") or "") for m in watched}

        posted = 0
        for market in watched:
            market_id = str(market.get("id") or "")
            prices = parse_outcome_prices(market)
            if not market_id or not prices:
                continue
            yes_price = prices[0]
            entry = self.monitor_state["last_prices"].get(market_id)
            if entry is None:
                # First sighting: record a baseline instead of alerting.
                self.monitor_state["last_prices"][market_id] = {
                    "price": yes_price,
                    "alerted": yes_price,
                }
                continue
            entry["price"] = yes_price

            liquidity = as_float(market.get("liquidityNum")) or 0.0
            if liquidity < MIN_PRICE_ALERT_LIQUIDITY:
                entry["alerted"] = yes_price
                continue

            baseline = entry["alerted"]
            move_points = (yes_price - baseline) * 100
            outcomes = parse_outcomes(market)
            alerted = False
            for guild_id, channel, threshold in tracked:
                if abs(move_points) < threshold:
                    continue
                if posted >= MAX_ALERTS_PER_CYCLE:
                    break
                embed = self.make_price_embed(
                    market, prices, outcomes, move_points
                )
                if await self.send_embed(channel, embed, guild_id):
                    posted += 1
                    alerted = True
            if alerted:
                entry["alerted"] = yes_price

        # Drop markets that fell out of the watched set so state stays bounded.
        for stale_id in list(self.monitor_state["last_prices"]):
            if stale_id not in watched_ids:
                del self.monitor_state["last_prices"][stale_id]
        self.persist_monitor_state()
        logger.info(
            "Price-move check complete: %d markets watched, %d alerts posted.",
            len(watched),
            posted,
        )

    async def run_whale_cycle(
        self,
        destinations: dict[str, discord.TextChannel | discord.Thread],
    ) -> None:
        tracked: list[tuple[str, discord.TextChannel | discord.Thread, float]] = []
        for guild_id, channel in destinations.items():
            settings = self.get_settings(guild_id)
            if not settings["alerts"].get("whale", True):
                continue
            tracked.append((guild_id, channel, float(settings["whale_min_usd"])))
        if not tracked:
            return

        # The API filters by value server-side, so request only the smallest
        # threshold any server cares about and filter the rest locally.
        lowest_threshold = min(threshold for _, _, threshold in tracked)
        payload = await self.get_json(
            POLYMARKET_TRADES_URL,
            params={
                "limit": 100,
                "taker_only": "true",
                "filter_type": "CASH",
                "filter_amount": f"{lowest_threshold:.0f}",
            },
            headers={"Accept": "application/json", "User-Agent": "LashNewsBot/1.0"},
            timeout=15,
        )
        trades = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(trades, list):
            raise RuntimeError("Polymarket returned an unexpected trades response.")

        seeding = not self.monitor_state.get("whale_seeded")
        new_keys: list[str] = []
        posted = 0
        for trade in reversed(trades):
            if not isinstance(trade, dict):
                continue
            key = self.trade_key(trade)
            if key is None or key in self.seen_trades:
                continue
            new_keys.append(key)
            if seeding:
                continue

            notional = self.trade_notional(trade)
            if notional is None:
                continue
            wallet = trade.get("proxy_wallet") or trade.get("proxyWallet")
            eligible = [
                (guild_id, channel)
                for guild_id, channel, threshold in tracked
                if notional >= threshold
                # A wallet this server already tracks gets its own, more
                # detailed wallet alert, so the whale copy is suppressed.
                and not self.whale_alert_is_duplicate(guild_id, wallet)
            ]
            if not eligible or posted >= MAX_ALERTS_PER_CYCLE:
                continue

            embed = self.make_whale_embed(trade, notional)
            for guild_id, channel in eligible:
                await self.send_embed(channel, embed, guild_id)
            posted += 1

        self.remember_trade_keys(new_keys)
        self.monitor_state["whale_seeded"] = True
        self.persist_monitor_state()
        logger.info(
            "Large-trade check complete: %d new fills seen, %d posted.",
            len(new_keys),
            posted,
        )

    def record_article_seen(self, guild_id: str, url: str) -> None:
        guild_urls = self.guild_seen_articles.setdefault(guild_id, [])
        guild_url_set = self.guild_seen_article_urls.setdefault(guild_id, set())
        if url not in guild_url_set:
            guild_urls.append(url)
            guild_url_set.add(url)
            if len(guild_urls) > MAX_SAVED_ARTICLES:
                oldest_url = guild_urls.pop(0)
                guild_url_set.discard(oldest_url)
            try:
                save_guild_seen_articles(self.guild_seen_articles)
            except OSError:
                logger.exception(
                    "Could not persist article history for server %s.",
                    guild_id,
                )

        if url not in self.seen_article_urls:
            self.seen_articles.append(url)
            self.seen_article_urls.add(url)
            if len(self.seen_articles) > MAX_SAVED_ARTICLES:
                oldest_url = self.seen_articles.pop(0)
                self.seen_article_urls.discard(oldest_url)
            try:
                save_seen_articles(self.seen_articles)
            except OSError:
                logger.exception("Could not persist the legacy article history.")

    async def find_polymarket_market(
        self,
        article: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Look up an active Polymarket market for one NewsAPI headline.

        This remains the compatibility/search entry point. The correlation pipeline
        now evaluates relevance itself; this method is still used by diagnostics and
        by code paths that only need one candidate market.
        """
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")

        query = clean_article_text(article.get("title"))[:200]
        if not query:
            query = clean_article_text(article.get("description"))[:200]
        if not query:
            return None

        params = {
            "q": query,
            "events_status": "active",
            "limit_per_type": 10,
            "keep_closed_markets": 0,
            "search_tags": "false",
            "search_profiles": "false",
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "LashNewsBot/1.0",
        }
        payload = await self.get_json(
            POLYMARKET_SEARCH_URL, params=params, headers=headers, timeout=10
        )

        if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            raise RuntimeError("Polymarket returned an unexpected search response.")

        # The correlation engine now decides relevance, so this compatibility
        # entry point returns the single best-scoring candidate market it can
        # defend, or None when nothing qualifies.
        best_market: dict[str, Any] | None = None
        best_score = -1
        for event in payload["events"]:
            if not isinstance(event, dict):
                continue
            for market in event.get("markets") or []:
                if not isinstance(market, dict):
                    continue
                match = compute_correlation(article, market)
                if match is not None and match.score > best_score:
                    best_market = market
                    best_score = match.score
        return best_market

    # ------------------------------------------------------------------
    # News correlation helpers
    # ------------------------------------------------------------------

    def _news_provider(self) -> NewsSourceAdapter:
        """The active news provider for this bot.

        NewsAPI is the default and only configured provider for now. The provider
        is built on demand so the session can be injected and swapped in tests.
        """
        if not hasattr(self, "_cached_news_provider"):
            keys = list(getattr(self, "news_api_keys", None) or [])
            if not keys and self.news_api_key:
                keys = [self.news_api_key]
            self._cached_news_provider = build_news_provider(
                self,
                api_key=keys[0] if keys else "",
                api_keys=keys,
                provider_name=os.getenv("NEWS_SOURCE", DEFAULT_NEWS_SOURCE_NAME),
            )
        return self._cached_news_provider

    async def _fetch_provider_articles(
        self, provider: NewsSourceAdapter, query: str
    ) -> list[dict[str, Any]]:
        """Fetch articles from the configured provider, honoring the same retry
        semantics the rest of the bot already uses."""
        try:
            articles = await provider.fetch_articles(query)
        except RuntimeError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("News provider returned an unhandled error.")
            raise RuntimeError("News provider failed.") from exc
        return [
            article
            for article in articles
            if isinstance(article, dict)
        ]

    def _active_market_catalog(
        self, market_lists: list[list[dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """Build a deduplicated catalog of active, open markets for correlation.

        This is the efficient candidate pool the correlation step evaluates against.
        It is intentionally smaller than the raw fetch so the deeper scorer is not
        run against every article-market combination.
        """
        catalog: dict[str, dict[str, Any]] = {}
        for markets in market_lists:
            for market in markets:
                if not isinstance(market, dict):
                    continue
                if market.get("active") is not True:
                    continue
                if market.get("closed") is True:
                    continue
                if market.get("archived") is True:
                    continue
                parsed = parse_market_question(market)
                if not parsed:
                    continue
                key = str(parsed["slug"]).strip().casefold()
                if key in catalog:
                    # Merge in any richer detail from a duplicate listing.
                    existing = catalog[key]
                    if not existing.get("description") and parsed.get("description"):
                        existing["description"] = parsed["description"]
                    if not existing.get("resolution") and parsed.get("resolution"):
                        existing["resolution"] = parsed["resolution"]
                    if not existing.get("entities") and parsed.get("entities"):
                        existing["entities"] = parsed["entities"]
                    continue
                catalog[key] = {
                    **parsed,
                    "source": market,
                }
        markets = list(catalog.values())
        # Computed from the catalog itself, so "october" or "2026" cannot link an
        # unrelated article to a market just because both mention it.
        self.catalog_common_terms = compute_common_catalog_terms(markets)
        logger.debug(
            "Correlation vocabulary: %d catalog-wide term(s) excluded from "
            "evidence.",
            len(self.catalog_common_terms),
        )
        return markets

    def _correlate_article(
        self, article: dict[str, Any], catalog: list[dict[str, Any]]
    ) -> list[MarketMatch]:
        """Evaluate one article against the active market catalog.

        Returns the qualifying matches in descending correlation order.
        """
        if not catalog:
            return []

        matches: list[MarketMatch] = []
        source = source_from_article(article)
        headline = clean_article_text(article.get("title"))
        summary = article_event_description(article)
        published_at = parse_published_at(
            article.get("publishedAt")
            if isinstance(article.get("publishedAt"), str)
            else None
        )

        # Pre-filter candidates by cheap entity/term overlap so we do not run the
        # deeper scorer on every article-market combination.
        article_entities = extract_article_entities(article)
        article_terms = meaningful_market_terms(
            " ".join(part for part in (headline, summary) if part)
        )
        candidates: list[dict[str, Any]] = []
        for market in catalog:
            question = market.get("question") or ""
            entities = market.get("entities") or set()
            question_terms = meaningful_market_terms(question)
            entity_overlap = article_entities & entities
            term_overlap = article_terms & question_terms
            meaningful = bool(
                (entity_overlap - question_terms)
                or (entity_overlap & question_terms)
                or term_overlap
            )
            if not meaningful:
                continue
            candidates.append(
                {
                    "market": market["source"],
                    "parsed": market,
                    "entity_overlap": entity_overlap,
                    "question_term_overlap": term_overlap,
                }
            )

        if not candidates:
            return []

        common_terms = getattr(self, "catalog_common_terms", frozenset())
        for candidate in candidates:
            try:
                match = compute_correlation(
                    article=article,
                    market=candidate["market"],
                    common_terms=common_terms,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug(
                    "Correlation failed for one market; skipping it.",
                    exc_info=True,
                )
                continue
            if match is None:
                continue
            matches.append(match)

        selected = select_polymarket_market(matches)
        if matches and not selected:
            # Retained by the diagnostics floor but below the posting bar. This
            # is the line that tells an operator why a plausible-looking story
            # did not alert and how far off it was.
            best = max(matches, key=lambda match: match.score)
            logger.info(
                "No market cleared the alert bar for %r; the closest was %r at "
                "%d%% (%s, needs %s%%).",
                headline[:120],
                clean_article_text(best.market.get("question"))[:120],
                best.score,
                best.relationship,
                required_score(best.relationship),
            )
        return selected

    # ------------------------------------------------------------------
    # Embed builders
    # ------------------------------------------------------------------

    def make_embed(self, article: dict[str, Any]) -> discord.Embed:
        """Compatibility entry point for existing callers.

        Existing command and monitor code references this method directly, so it
        stays on the public surface and delegates to the redesigned builder when
        correlation data is available.
        """
        match_list = article.get("_correlation_matches")
        if isinstance(match_list, list) and match_list:
            return self.make_news_embed(article, match_list)
        return self.make_news_embed(article, [])

    def make_news_embed(
        self, article: dict[str, Any], matches: list[MarketMatch]
    ) -> discord.Embed:
        """Build the redesigned Market Intel embed.

        Title: Market Intel | Sovereign Predicts
        Sections: News, Link, Summary, Related Markets, Footer.
        Political alignment is intentionally absent.
        """
        headline = clean_article_text(article.get("title")) or "Untitled article"
        url = str(article.get("url") or "")
        source = article.get("source")
        source_name = (
            clean_article_text(source.get("name"))
            if isinstance(source, dict)
            else ""
        ) or "Unknown publication"
        published_at = parse_published_at(
            article.get("publishedAt")
            if isinstance(article.get("publishedAt"), str)
            else None
        )

        # Reference layout: a News line naming the publisher and the headline,
        # then a Read Article link, then Summary and Related Markets below.
        # Political alignment is deliberately not part of this design.
        embed = discord.Embed(
            title="Market Intel | Sovereign Predicts",
            url=url if url.startswith(("https://", "http://")) else None,
            description=(
                f"**News:** [{source_name}] {headline}\n"
                f"**Link:** [Read Article]({url})"
            ),
            color=SOVEREIGN_COLOR,
            timestamp=published_at or datetime.now(timezone.utc),
        )

        summary = summarize_article(article)
        embed.add_field(
            name="Summary",
            value=summary,
            inline=False,
        )

        related = _related_markets_field(matches)
        if related:
            embed.add_field(
                name="Related Markets",
                value=related,
                inline=False,
            )
        else:
            embed.add_field(
                name="Related Markets",
                value="No actively correlated market this cycle.",
                inline=False,
            )

        # "Generated by Sovereign Predicts | 31/07/2026, 1:09 AM", as in the
        # reference design. The publisher is already named on the News line, so
        # the footer carries the attribution and the publication stamp.
        # Plain text, not Discord's <t:...> markup: markup is rendered in an
        # embed description or field but not in a footer, where it would appear
        # literally.
        embed.set_footer(
            text="Generated by Sovereign Predicts | "
            + (
                published_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                if published_at
                else "publication time unknown"
            ),
        )
        return embed

    @commands.command(name="test")
    async def test_command(self, ctx: commands.Context[commands.Bot]) -> None:
        await ctx.reply(
            "Sovereign Predicts is online and ready to monitor news.",
            mention_author=False,
        )

    @commands.command(name="status")
    async def status_command(self, ctx: commands.Context[commands.Bot]) -> None:
        channel: discord.TextChannel | discord.Thread | None = None
        if ctx.guild is not None:
            try:
                channel = await self.get_configured_channel(ctx.guild)
            except Exception:
                logger.exception(
                    "Could not resolve the configured channel for server %s.",
                    ctx.guild.id,
                )
        destination = channel.mention if channel is not None else "not configured"
        last_check = (
            discord.utils.format_dt(self.last_poll_at, style="R")
            if self.last_poll_at is not None
            else "not yet"
        )
        monitor_running = self.news_task is not None and not self.news_task.done()
        lines = [
            f"**Discord connection:** {'connected' if self.is_ready() else 'disconnected'}",
            f"**News monitoring:** {'running' if monitor_running else 'stopped'}",
            f"**Destination:** {destination}",
            f"**News provider:** {'NewsAPI' if self.news_api_keys else 'not set'}",
            "**Discovery:** active Polymarket market questions",
            f"**Markets searched:** {self.last_market_count}",
            "**Posting filter:** article must match an active market",
            f"**Last successful check:** {last_check}",
            f"**Articles posted this run:** {self.articles_sent}",
        ]
        if self.last_poll_error:
            lines.append(f"**Latest issue:** {self.last_poll_error}")
        await ctx.reply("\n".join(lines), mention_author=False)


def create_bot() -> LashNewsBot:
    token = os.getenv("DISCORD_BOT_TOKEN")
    news_api_key = os.getenv("NEWS_API_KEY") or ""
    if not token:
        raise ValueError("DISCORD_BOT_TOKEN is required.")
    if not news_api_key:
        # News alerts need a NewsAPI key, but price, new-market, and whale
        # alerts do not. A missing key degrades one category instead of
        # refusing to boot, which on a host like Railway is a crash loop.
        logger.warning(
            "NEWS_API_KEY is not set, so market-matched news is disabled. "
            "Price, new-market, and whale alerts are unaffected; set "
            "NEWS_API_KEY (or NEWS_API_KEYS) to enable news."
        )

    try:
        poll_interval = int(os.getenv("NEWS_POLL_INTERVAL_SECONDS", "300"))
    except ValueError as exc:
        raise ValueError("NEWS_POLL_INTERVAL_SECONDS must be a whole number.") from exc
    if poll_interval < 60:
        raise ValueError("NEWS_POLL_INTERVAL_SECONDS must be at least 60.")

    return LashNewsBot(
        news_api_key=news_api_key,
        channel_id=parse_channel_id(os.getenv("DISCORD_CHANNEL_ID")),
        poll_interval=poll_interval,
        dev_guild_ids=parse_guild_ids(
            os.getenv("DISCORD_DEV_GUILD_ID") or os.getenv("DISCORD_GUILD_IDS")
        ),
    )


def main() -> None:
    try:
        bot = create_bot()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    token = os.environ["DISCORD_BOT_TOKEN"]
    try:
        bot.run(token, log_handler=None)
    except discord.PrivilegedIntentsRequired as exc:
        raise SystemExit(
            "Enable Message Content Intent in Discord Developer Portal > Bot, "
            "then restart Sovereign Predicts."
        ) from exc


if __name__ == "__main__":
    main()
