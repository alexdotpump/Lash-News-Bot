"""Post news found from active Polymarket markets when it matches an open market."""

import asyncio
import html
import json
import logging
import os
import re
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
SEEN_ARTICLES_FILE = Path("seen_articles.json")
GUILD_CHANNELS_FILE = Path("guild_channels.json")
GUILD_SEEN_ARTICLES_FILE = Path("seen_articles_by_guild.json")
MAX_MARKET_LOOKUPS_PER_POLL = 20
MAX_SAVED_ARTICLES = 500
NEWS_QUERY_MAX_LENGTH = 500
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
    recent_urls = urls[-MAX_SAVED_ARTICLES:]
    temporary_file = SEEN_ARTICLES_FILE.with_suffix(".tmp")
    temporary_file.write_text(
        json.dumps(recent_urls, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_file.replace(SEEN_ARTICLES_FILE)


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


def save_guild_channels(channels: dict[str, int]) -> None:
    """Atomically persist per-server alert channels."""
    temporary_file = GUILD_CHANNELS_FILE.with_suffix(".tmp")
    temporary_file.write_text(
        json.dumps(channels, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_file.replace(GUILD_CHANNELS_FILE)


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
    bounded = {
        guild_id: urls[-MAX_SAVED_ARTICLES:]
        for guild_id, urls in histories.items()
    }
    temporary_file = GUILD_SEEN_ARTICLES_FILE.with_suffix(".tmp")
    temporary_file.write_text(
        json.dumps(bounded, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_file.replace(GUILD_SEEN_ARTICLES_FILE)


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
) -> str:
    """Build a NewsAPI OR query from highly traded active market questions."""
    candidate_terms: list[str] = []
    seen_terms: set[str] = set()
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


def market_match_score(
    article: dict[str, Any],
    event: dict[str, Any],
    market: dict[str, Any],
) -> int:
    """Score specific headline overlap with a market and its parent event."""
    headline = clean_article_text(article.get("title"))
    market_text = " ".join(
        clean_article_text(value)
        for value in (event.get("title"), market.get("question"))
        if isinstance(value, str)
    )
    headline_terms = meaningful_market_terms(headline)
    shared_terms = headline_terms & meaningful_market_terms(market_text)
    if not shared_terms:
        return 0

    named_terms = {
        match.group().casefold()
        for match in re.finditer(
            r"\b(?:[A-Z][A-Za-z0-9'-]{1,}|[A-Z]{2,})\b",
            headline,
        )
        if match.group().casefold() not in MARKET_MATCH_STOP_WORDS
    }
    if len(shared_terms) == 1 and not (shared_terms & named_terms):
        return 0

    return len(shared_terms) * 10 + len(shared_terms & named_terms) * 2


def select_polymarket_market(
    article: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    """Select the strongest active, open Polymarket market matching a headline."""
    events = payload.get("events")
    if not isinstance(events, list):
        return None

    candidates: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        if event.get("active") is not True or event.get("closed") is True:
            continue
        if event.get("archived") is True:
            continue

        markets = event.get("markets")
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            if market.get("active") is not True or market.get("closed") is True:
                continue
            if market.get("archived") is True:
                continue

            slug = market.get("slug")
            question = clean_article_text(market.get("question"))
            if not isinstance(slug, str) or not slug.strip() or not question:
                continue

            score = market_match_score(article, event, market)
            if score == 0:
                continue
            candidates.append(
                {
                    "question": question,
                    "url": f"https://polymarket.com/market/{quote(slug, safe='-_')}",
                    "score": score,
                }
            )

    return max(candidates, key=lambda candidate: candidate["score"], default=None)


class LashNewsBot(commands.Bot):
    def __init__(
        self,
        *,
        news_api_key: str,
        channel_id: int | None,
        poll_interval: int,
    ) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.news_api_key = news_api_key
        self.channel_id = channel_id
        self.poll_interval = poll_interval
        self.seen_articles = load_seen_articles()
        self.seen_article_urls = set(self.seen_articles)
        self.legacy_seen_article_urls = set(self.seen_article_urls)
        self.guild_channels = load_guild_channels()
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
        self._commands_synced = False

    async def setup_hook(self) -> None:
        self.news_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        )
        self.tree.add_command(self.setup_slash)
        self.tree.add_command(self.status_slash)
        self.tree.add_command(self.test_slash)
        self.tree.add_command(self.sources_slash)
        self.tree.add_command(self.keywords_slash)
        self.tree.add_command(self.help_slash)
        self.news_task = asyncio.create_task(self.poll_news_forever())

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
        logger.info("Connected to Discord as %s.", self.user)
        if self._commands_synced:
            return
        try:
            synced = await self.tree.sync()
        except Exception:
            logger.exception(
                "Could not sync slash commands; they will be retried after reconnect."
            )
            return
        self._commands_synced = True
        logger.info("Synced %d Discord slash commands.", len(synced))

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

        for guild_id, channel_id in list(self.guild_channels.items()):
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
            and legacy_guild_id not in self.guild_channels
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
        channel_id = self.guild_channels.get(guild_id)
        if channel_id is None:
            channel_id = self.channel_id
        if channel_id is None:
            return None
        channel = await self.resolve_channel(channel_id)
        if channel.guild.id != guild.id:
            return None
        self.ensure_guild_history(guild_id)
        return channel

    @app_commands.command(
        name="setup",
        description="Choose this server's news-alert channel.",
    )
    @app_commands.default_permissions(administrator=True)
    async def setup_slash(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
    ) -> None:
        guild = interaction.guild
        permissions = getattr(interaction.user, "guild_permissions", None)
        if guild is None:
            await interaction.response.send_message(
                "Run `/setup` inside a Discord server.",
                ephemeral=True,
            )
            return
        if permissions is None or not permissions.administrator:
            await interaction.response.send_message(
                "Only server administrators can change the alert channel.",
                ephemeral=True,
            )
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
            if not channel_permissions.send_messages or not channel_permissions.embed_links:
                await interaction.response.send_message(
                    "I need permission to send messages and embeds in that channel.",
                    ephemeral=True,
                )
                return

        updated_channels = dict(self.guild_channels)
        updated_channels[str(guild.id)] = channel.id
        try:
            save_guild_channels(updated_channels)
        except OSError:
            logger.exception("Could not save alert channel for server %s.", guild.id)
            await interaction.response.send_message(
                "I couldn't save this server's alert-channel setting.",
                ephemeral=True,
            )
            return
        self.guild_channels = updated_channels
        try:
            self.ensure_guild_history(str(guild.id))
        except OSError:
            logger.exception(
                "Could not initialize article history for server %s.",
                guild.id,
            )
        self.poll_wakeup.set()
        await interaction.response.send_message(
            f"News alerts for this server will be sent to {channel.mention}.",
            ephemeral=True,
        )

    @app_commands.command(
        name="status",
        description="Show bot connection, monitoring, and this server's alert channel.",
    )
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
            logger.exception("Could not resolve the configured channel for server %s.", guild.id)
            channel = None

        monitor_running = self.news_task is not None and not self.news_task.done()
        last_check = (
            discord.utils.format_dt(self.last_poll_at, style="R")
            if self.last_poll_at is not None
            else "not yet"
        )
        lines = [
            f"**Discord connection:** {'connected' if self.is_ready() else 'disconnected'}",
            f"**News monitoring:** {'running' if monitor_running else 'stopped'}",
            f"**Alert channel:** {channel.mention if channel is not None else 'not configured'}",
            f"**Last successful news check:** {last_check}",
            f"**Articles posted this run:** {self.articles_sent}",
        ]
        if self.last_poll_error:
            lines.append(f"**Latest issue:** {self.last_poll_error[:300]}")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="test",
        description="Send a test news alert to this server's configured channel.",
    )
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
            title="Market Intel | Lash",
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
        embed.add_field(name="Source", value="Lash News Bot", inline=True)
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
        embed.set_footer(text="Market Intel • Lash News Bot")
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
        description="Show sources seen in the latest NewsAPI results.",
    )
    async def sources_slash(self, interaction: discord.Interaction) -> None:
        if not self.last_sources:
            message = (
                "No news-source results are available yet. Sources will appear after "
                "the next successful news check."
            )
        else:
            message = (
                "**NewsAPI search:** available publishers; no fixed source filter.\n"
                "**Sources in the latest results:**\n"
                + "\n".join(f"• {source}" for source in self.last_sources)
            )
        await interaction.response.send_message(message[:1900], ephemeral=True)

    @app_commands.command(
        name="keywords",
        description="Show the current terms derived from active Polymarket markets.",
    )
    async def keywords_slash(self, interaction: discord.Interaction) -> None:
        if not self.last_search_terms:
            message = (
                "No market-derived search terms are loaded yet. They are generated "
                "from active Polymarket markets during a news check."
            )
        else:
            message = (
                "**Current market-derived search terms** "
                "(no separate keyword filter):\n"
                + ", ".join(self.last_search_terms)
            )
        await interaction.response.send_message(message[:1900], ephemeral=True)

    @app_commands.command(
        name="help",
        description="Show the bot's available slash and prefix commands.",
    )
    async def help_slash(self, interaction: discord.Interaction) -> None:
        message = (
            "**News bot commands**\n"
            "• `/setup #channel` — administrators choose this server's alert channel.\n"
            "• `/status` — show connection, monitoring, channel, and last successful check.\n"
            "• `/test` — send a test alert to the configured channel.\n"
            "• `/sources` — show sources seen in the latest NewsAPI results.\n"
            "• `/keywords` — show terms derived from active markets.\n"
            "• `/help` — show this command list.\n"
            "• `!status` and `!test` — existing prefix commands."
        )
        await interaction.response.send_message(message, ephemeral=True)

    async def poll_news_forever(self) -> None:
        await self.wait_until_ready()
        retry_attempt = 0
        while not self.is_closed():
            try:
                await self.check_for_articles()
                retry_attempt = 0
                delay = self.poll_interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retry_attempt += 1
                self.last_poll_error = str(exc)[:300] or "Unknown news polling error."
                delay = min(
                    self.poll_interval,
                    30 * (2 ** min(retry_attempt - 1, 6)),
                )
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

    async def fetch_active_markets(self, order_by: str) -> list[dict[str, Any]]:
        """Fetch one ranked page of active, open markets from Gamma."""
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")

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
        async with self.news_session.get(
            POLYMARKET_MARKETS_URL,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:
            if response.status >= 400:
                response.raise_for_status()
            payload = await response.json()

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

    async def check_for_articles(self) -> None:
        if self.news_session is None:
            raise RuntimeError("The HTTP session is not available.")
        destinations = await self.get_alert_channels()
        if not destinations:
            logger.warning(
                "News monitoring is running but no alert channel is configured; "
                "use /setup in a server."
            )
            return

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
        news_query = make_market_news_query(market_lists)
        logger.info(
            "Searching news from %d active market listings with a %d-character query.",
            self.last_market_count,
            len(news_query),
        )
        self.last_search_terms = re.findall(r'"([^"]+)"', news_query)

        params = {
            "q": news_query,
            "language": os.getenv("NEWS_LANGUAGE", "en"),
            "sortBy": "publishedAt",
            "pageSize": 20,
        }
        headers = {"X-Api-Key": self.news_api_key}

        async with self.news_session.get(
            NEWS_API_URL,
            params=params,
            headers=headers,
        ) as response:
            if response.status >= 400:
                response.raise_for_status()
            payload: dict[str, Any] = await response.json()

        if payload.get("status") != "ok":
            message = payload.get("message", "NewsAPI returned an error.")
            raise RuntimeError(str(message))

        articles = payload.get("articles", [])
        if not isinstance(articles, list):
            raise RuntimeError("NewsAPI returned an unexpected articles value.")

        self.last_sources = list(dict.fromkeys(
            clean_article_text(article.get("source", {}).get("name"))
            for article in articles
            if isinstance(article, dict)
            and isinstance(article.get("source"), dict)
            and clean_article_text(article.get("source", {}).get("name"))
        ))[:25]

        candidate_articles: list[tuple[dict[str, Any], list[str]]] = []
        page_urls: set[str] = set()
        for article in articles:
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
            ]
            if eligible_guilds:
                candidate_articles.append((article, eligible_guilds))

        market_matched_articles: list[tuple[dict[str, Any], list[str]]] = []
        lookup_failures = 0
        for article, eligible_guilds in candidate_articles[:MAX_MARKET_LOOKUPS_PER_POLL]:
            try:
                market = await self.find_polymarket_market(article)
            except asyncio.CancelledError:
                raise
            except Exception:
                lookup_failures += 1
                logger.exception(
                    "Could not check a NewsAPI article against Polymarket: %s",
                    clean_article_text(article.get("title"))[:160],
                )
                continue
            if market is None:
                continue
            article["_polymarket_market"] = market
            market_matched_articles.append((article, eligible_guilds))

        # NewsAPI returns newest first. Reverse the small batch so each channel
        # reads in chronological order.
        posted_count = 0
        for article, eligible_guilds in reversed(market_matched_articles):
            url = article.get("url")
            if not isinstance(url, str):
                continue
            embed = self.make_embed(article)
            for guild_id in eligible_guilds:
                channel = destinations.get(guild_id)
                if channel is None:
                    continue
                try:
                    await channel.send(
                        embed=embed,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                except asyncio.CancelledError:
                    raise
                except discord.HTTPException:
                    logger.exception(
                        "Could not post article to channel %s in server %s.",
                        channel.id,
                        guild_id,
                    )
                    continue
                self.record_article_seen(guild_id, url)
                self.articles_sent += 1
                posted_count += 1

        self.last_poll_at = datetime.now(timezone.utc)
        self.last_poll_error = None
        logger.info(
            "News check complete: %d results, %d new articles checked, "
            "%d matched active markets, %d posted across servers, "
            "%d market-lookup failures.",
            len(articles),
            min(len(candidate_articles), MAX_MARKET_LOOKUPS_PER_POLL),
            len(market_matched_articles),
            posted_count,
            lookup_failures,
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
        """Look up an active Polymarket market for one NewsAPI headline."""
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
        async with self.news_session.get(
            POLYMARKET_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status >= 400:
                response.raise_for_status()
            payload = await response.json()

        if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            raise RuntimeError("Polymarket returned an unexpected search response.")
        return select_polymarket_market(article, payload)

    def make_embed(self, article: dict[str, Any]) -> discord.Embed:
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
            title="Market Intel | Lash",
            description=(
                f"**News**\n[{source_name[:100]}] {headline[:350]}\n\n"
                f"**Link**\n[Read Article]({url})"
            ),
            color=discord.Color.from_rgb(198, 161, 91),
            timestamp=published_at or datetime.now(timezone.utc),
        )

        embed.add_field(
            name="Summary",
            value=summarize_article(article),
            inline=False,
        )

        topics = extract_article_topics(article)
        embed.add_field(
            name="Topics",
            value="\n".join(f"• {topic}" for topic in topics) or "No topics identified.",
            inline=False,
        )
        embed.add_field(
            name="Source",
            value=source_name[:1024],
            inline=True,
        )
        embed.add_field(
            name="Published",
            value=(
                discord.utils.format_dt(published_at, style="f")
                if published_at is not None
                else "Not provided by source"
            ),
            inline=True,
        )
        market = article.get("_polymarket_market")
        if isinstance(market, dict):
            market_question = clean_article_text(market.get("question"))
            market_url = market.get("url")
            if market_question and isinstance(market_url, str):
                embed.add_field(
                    name="Polymarket Market",
                    value=f"{market_question[:300]}\n[Open this market]({market_url})",
                    inline=False,
                )
        embed.set_footer(text="Market Intel • Lash News Bot")
        return embed

    @commands.command(name="test")
    async def test_command(self, ctx: commands.Context[commands.Bot]) -> None:
        await ctx.reply(
            "Lash News Bot is online and ready to monitor news.",
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
            "then restart Lash News Bot."
        ) from exc


if __name__ == "__main__":
    main()