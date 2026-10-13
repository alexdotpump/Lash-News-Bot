"""Post news found from active Polymarket markets when it matches an open market."""

import asyncio
import html
import json
import logging
import os
import re
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

NEWS_API_URL = "https://newsapi.org/v2/everything"
POLYMARKET_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
POLYMARKET_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
POLYMARKET_DATA_API_URL = "https://data-api.polymarket.com"
POLYMARKET_TRADES_URL = f"{POLYMARKET_DATA_API_URL}/v2/trades"
POLYMARKET_MARKET_PAGE = "https://polymarket.com/market"
POLYGON_EXPLORER_TX = "https://polygonscan.com/tx"

# Railway (and most container hosts) wipe the filesystem on every deploy, so
# STATE_DIR should point at a mounted volume to persist settings across deploys.
STATE_DIR = Path(os.getenv("STATE_DIR") or ".")
SEEN_ARTICLES_FILE = STATE_DIR / "seen_articles.json"
GUILD_CHANNELS_FILE = STATE_DIR / "guild_channels.json"
GUILD_SEEN_ARTICLES_FILE = STATE_DIR / "seen_articles_by_guild.json"
GUILD_SETTINGS_FILE = STATE_DIR / "guild_settings.json"
MONITOR_STATE_FILE = STATE_DIR / "monitor_state.json"

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
ALERT_TYPES = ("news", "price", "new_market", "whale")
ALERT_LABELS = {
    "news": "News matching a market",
    "price": "Market price moves",
    "new_market": "New markets",
    "whale": "Large trades",
}


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
# A match is postable by default only when both conditions hold:
#   * correlation >= MIN_CORRELATION_FOR_ALERT
#   * relationship is Direct or Strong indirect
#
# Matches in the 70-84 band are retained for diagnostics but are not sent
# automatically under the default policy.
MIN_CORRELATION_FOR_ALERT = 85
MIN_CORRELATION_FOR_DIAGNOSTICS = 70
MAX_RELATED_MARKETS = 3

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
    """Return True when this relationship is eligible for a public alert."""
    return relationship in {"Direct", "Strong indirect"}


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
    }
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("lash-news-bot")


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

    # Try to pull out the named entities so we can tell whether an article
    # is about the specific thing the market resolves on.
    named_entities: set[str] = set()
    if description:
        named_entities |= extract_named_entities(description)
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
) -> MarketMatch | None:
    """Evaluate the real relationship between one article and one market.

    Returns a MarketMatch only when the relationship is strong enough to explain
    why this specific story matters to this specific market. Returns None when the
    connection is weak, unsupported, stale, or ambiguous.
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

    if relationship == "Contextual" and score < MIN_CORRELATION_FOR_ALERT:
        return None

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
        score += 60
    elif relationship == "Strong indirect":
        score += 50
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
        score -= 15

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

    def __init__(self, session, api_key: str) -> None:
        self._session = session
        self._api_key = api_key

    async def fetch_articles(self, query: str) -> list[dict[str, Any]]:
        if not self._api_key:
            raise RuntimeError("NewsAPI key is not configured.")
        params = {
            "q": query,
            "language": os.getenv("NEWS_LANGUAGE", "en"),
            "sortBy": "publishedAt",
            "pageSize": 20,
        }
        headers = {"X-Api-Key": self._api_key}
        payload = await self._session.get_json(
            NEWS_API_URL, params=params, headers=headers
        )
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
    return NewsAPIAdapter(session, api_key)


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
    top = ranked[:MAX_RELATED_MARKETS]
    cutoffs: list[MarketMatch] = []
    for match in top:
        if match.score >= MIN_CORRELATION_FOR_DIAGNOSTICS:
            cutoffs.append(match)
    if not cutoffs:
        cutoffs = top[:1]
    return cutoffs


def _related_markets_field(matches: list[MarketMatch]) -> str:
    """Format the Related Markets embed field from qualified matches.

    Each related market shows the exact market question, the verified Polymarket
    platform label, the correlation percentage, a clickable link, and a short
    reason when space permits. No political alignment is included.
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
        score = match.score
        relationship = match.relationship

        line = f"• [{market_link}] {question} — Correlation: {score}%"
        lines.append(line)
        if match.why_short:
            lines.append(f"    * {match.why_short}")
        if match.explanation and len(match.explanation) <= 300 and relationship in {"Direct", "Strong indirect"}:
            lines.append(f"    * Why it matches: {match.explanation}")
    return "\n".join(lines)


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
        )
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
        if self.news_task is not None:
            self.news_task.cancel()
            try:
                await self.news_task
            except asyncio.CancelledError:
                pass
        if self.news_session is not None:
            await self.news_session.close()
        await super().close()

    async def on_ready(self) -> None:
        logger.info(
            "Connected to Discord as %s (in %d server(s)).",
            self.user,
            len(self.guilds),
        )

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

    async def send_embed(
        self,
        channel: discord.TextChannel | discord.Thread,
        embed: discord.Embed,
        guild_id: str,
    ) -> bool:
        """Post one embed, logging instead of raising on a Discord failure."""
        try:
            await channel.send(
                embed=embed,
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
    ) -> dict[str, discord.TextChannel | discord.Thread]:
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
            channel_id = settings.get("channel_id")
            if channel_id is None:
                continue
            try:
                channel = await self.resolve_channel(channel_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                resolution_failures += 1
                logger.exception(
                    "Could not resolve the alert channel configured for server %s.",
                    guild_id,
                )
                continue
            if str(channel.guild.id) != guild_id:
                logger.error(
                    "Configured channel %s does not belong to server %s.",
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
        if not destinations and resolution_failures:
            raise RuntimeError("Could not resolve any configured Discord alert channel.")
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
    )
    async def setup_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        news: bool | None = None,
        price: bool | None = None,
        new_market: bool | None = None,
        whale: bool | None = None,
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
        lines = [
            f"**Discord connection:** {'connected' if self.is_ready() else 'disconnected'}",
            f"**Commands synced:** {synced_at}",
            f"**Monitoring:** {'running' if monitor_running else 'stopped'}",
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
        await interaction.followup.send("\n".join(lines)[:1900], ephemeral=True)

    @app_commands.command(
        name="test",
        description="Send a test news alert to this server's configured channel.",
    )
    @app_commands.guild_only()
    async def test_slash(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Run `/test` inside a Discord server.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        try:
            channel = await self.get_configured_channel(guild)
        except Exception:
            logger.exception("Could not resolve the configured channel for server %s.", guild.id)
            channel = None
        if channel is None:
            await interaction.followup.send(
                "No alert channel is configured. Ask an administrator to run `/setup`.",
                ephemeral=True,
            )
            return

        now = datetime.now(timezone.utc)
        embed = discord.Embed(
            title="Market Intel | Sovereign Predicts",
            description=(
                "**Test alert**\n"
                "This confirms that news alerts can be delivered to this channel."
            ),
            color=discord.Color.from_rgb(198, 161, 91),
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
            "**Sovereign Predicts — commands**\n"
            "• `/setup channel:<#channel>` — administrators pick this server's "
            "alert channel and toggle each alert type.\n"
            "• `/alerts` — set the price-move, large-trade, and new-market "
            "thresholds for this server.\n"
            "• `/status` — monitoring health, last successful check, and this "
            "server's settings.\n"
            "• `/test` — send a test alert to the configured channel.\n"
            "• `/sources` — show or configure which publishers this server "
            "accepts.\n"
            "• `/keywords` — view or manage include and exclude keywords.\n"
            "• `/help` — show this command list.\n"
            "• `/sync`, `/servers`, `/server-info` — owner only.\n"
            "• `!status` and `!test` — legacy prefix commands."
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
        """Run every alert type once, isolating one failure from the rest."""
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")

        destinations = await self.get_alert_channels()
        if not destinations:
            logger.warning(
                "Monitoring is running but no alert channel is configured; "
                "use /setup in a server."
            )
            self.last_poll_at = datetime.now(timezone.utc)
            self.last_poll_error = None
            return

        subsystems = (
            ("news", self.run_news_cycle),
            ("new markets", self.run_new_market_cycle),
            ("price moves", self.run_price_cycle),
            ("large trades", self.run_whale_cycle),
        )
        failures: list[str] = []
        for name, runner in subsystems:
            try:
                await runner(destinations)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures.append(f"{name}: {exc}")
                logger.exception("The %s monitor failed.", name)

        self.last_poll_at = datetime.now(timezone.utc)
        self.last_poll_error = "; ".join(failures)[:300] if failures else None
        if len(failures) == len(subsystems):
            # Every subsystem failed, so treat this as an outage and let the
            # outer loop back off instead of hammering the APIs.
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
        correlated_articles: list[tuple[dict[str, Any], list[MarketMatch], list[str]]] = []
        lookup_failures = 0
        fingerprinted: set[str] = set()
        page_urls: set[str] = set()
        for article in raw_articles:
            if not isinstance(article, dict):
                continue
            url = article.get("url")
            if not isinstance(url, str) or not url.startswith(("https://", "http://")):
                continue
            if url in page_urls:
                continue
            page_urls.add(url)
            eligible_guilds = [
                guild_id
                for guild_id in destinations
                if url not in self.guild_seen_article_urls.get(guild_id, set())
                and self.guild_accepts_article(guild_id, article)
            ]
            if not eligible_guilds:
                continue

            # Persist a content fingerprint so near-identical copies and minor
            # headline variants do not replay as new alerts.
            fp = article_fingerprint(article)
            if fp and fp in fingerprinted:
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
            "News check complete: %d results, %d new articles checked, "
            "%d correlated with active markets, %d posted across servers, "
            "%d correlation failures.",
            len(raw_articles),
            len(raw_articles),
            len(correlated_articles),
            posted_count,
            lookup_failures,
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
            color=discord.Color.from_rgb(198, 161, 91),
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
            color=discord.Color.from_rgb(198, 161, 91),
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
            eligible = [
                (guild_id, channel)
                for guild_id, channel, threshold in tracked
                if notional >= threshold
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
            self._cached_news_provider = build_news_provider(
                self,
                api_key=self.news_api_key,
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
        return list(catalog.values())

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

        for candidate in candidates:
            try:
                match = compute_correlation(
                    article=article,
                    market=candidate["market"],
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

        return select_polymarket_market(matches)

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

        embed = discord.Embed(
            title="Market Intel | Sovereign Predicts",
            description=(
                f"**News**\n[{source_name} — {url}]({url})\n\n"
                f"**Link**\n[Read Article]({url})"
            ),
            color=discord.Color.from_rgb(59, 130, 246),
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

        embed.set_footer(
            text=f"Sovereign Predicts | {source_name} | "
            f"{discord.utils.format_dt(published_at, style='R') if published_at else 'publication time unknown'}",
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
    news_api_key = os.getenv("NEWS_API_KEY")
    if not token:
        raise ValueError("DISCORD_BOT_TOKEN is required.")
    if not news_api_key:
        raise ValueError("NEWS_API_KEY is required.")

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
