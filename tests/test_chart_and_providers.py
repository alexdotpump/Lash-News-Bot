"""Tests for /chart rendering, price-history parsing, and the news provider.

These cover the surface the other suites leave open: /chart end to end (market
lookup, price history, PNG rendering, embed), the NewsAPI adapter's request
shape, and the extra-key rotation that makes a second API key useful.
"""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import main  # noqa: E402

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def market(
    *,
    slug: str = "brent-above-90",
    question: str = "Will Brent crude rise above $90 before Dec 31?",
    tokens: str | list | None = '["93165051950362910", "80762690013233003"]',
) -> dict:
    payload = {
        "id": "12345",
        "question": question,
        "slug": slug,
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '["0.62", "0.38"]',
        "active": True,
        "closed": False,
        "archived": False,
    }
    if tokens is not None:
        payload["clobTokenIds"] = tokens
    return payload


def history(count: int = 48) -> dict:
    start = 1791909838 - count * 3600
    return {
        "history": [
            {"t": start + index * 3600, "p": round(0.4 + index * 0.004, 4)}
            for index in range(count)
        ]
    }


class FakeResponse:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self._done = False

    def is_done(self) -> bool:
        return self._done

    async def send_message(self, content=None, **kwargs):
        self._done = True
        self.sent.append({"content": content, **kwargs})

    async def defer(self, *, ephemeral=False, thinking=False):
        self._done = True


class FakeFollowup:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, content=None, **kwargs):
        self.sent.append({"content": content, **kwargs})


class FakeInteraction:
    def __init__(self, guild_id: int = 7) -> None:
        self.response = FakeResponse()
        self.followup = FakeFollowup()
        self.command = SimpleNamespace(name="chart")
        self.user = SimpleNamespace(
            id=1, guild_permissions=SimpleNamespace(administrator=True)
        )
        self.guild = SimpleNamespace(id=guild_id, name="Test Server", me=object())
        self.guild_id = guild_id


class ChartTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _bot(self, handler):
        """A bot whose HTTP layer is the supplied ``handler(url, params)``."""
        bot = main.create_bot()
        self.calls: list[tuple[str, dict]] = []

        async def get_json(url, params=None, headers=None, timeout=None):
            self.calls.append((url, params or {}))
            return handler(url)

        bot.get_json = get_json

        async def no_sync(guild=None):
            return []

        async def idle_monitor():
            await asyncio.Event().wait()

        bot.tree.sync = no_sync
        bot.poll_news_forever = idle_monitor
        self.addAsyncCleanup(bot.close)
        await bot.setup_hook()
        return bot

    async def _invoke(self, bot, name, params):
        interaction = FakeInteraction()
        await bot.tree.get_command(name)._do_call(interaction, params)
        return interaction

    async def test_chart_posts_a_png_for_a_slug(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return [market()]
            if url == main.POLYMARKET_PRICE_HISTORY_URL:
                return history()
            raise AssertionError(f"unexpected URL {url}")

        bot = await self._bot(handler)
        interaction = await self._invoke(bot, "chart", {"market": "brent-above-90"})

        self.assertEqual(
            [url for url, _ in self.calls],
            [main.POLYMARKET_MARKETS_URL, main.POLYMARKET_PRICE_HISTORY_URL],
        )
        self.assertEqual(len(interaction.followup.sent), 1)
        message = interaction.followup.sent[0]
        embed = message["embed"]
        self.assertEqual(embed.color, main.SOVEREIGN_COLOR)
        self.assertEqual(embed.image.url, "attachment://chart.png")
        self.assertEqual(embed.title, "Price history | Sovereign Predicts")
        self.assertLessEqual(len(embed.title), main.EMBED_TITLE_LIMIT)
        self.assertLessEqual(len(embed.description), main.EMBED_DESCRIPTION_LIMIT)
        self.assertEqual(message["file"].filename, "chart.png")
        # The history request must name the first outcome's CLOB token.
        self.assertEqual(
            self.calls[1][1]["market"], "93165051950362910"
        )
        self.assertEqual(self.calls[1][1]["interval"], "1w")

    async def test_chart_respects_the_days_option(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return [market()]
            return history()

        bot = await self._bot(handler)
        await self._invoke(bot, "chart", {"market": "brent-above-90", "days": 30})
        self.assertEqual(self.calls[1][1]["interval"], "1m")

    async def test_bare_token_falls_back_to_search_when_the_slug_is_unknown(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return []
            if url == main.POLYMARKET_SEARCH_URL:
                return {"events": [{"closed": False, "markets": [market()]}]}
            if url == main.POLYMARKET_PRICE_HISTORY_URL:
                return history()
            raise AssertionError(f"unexpected URL {url}")

        bot = await self._bot(handler)
        interaction = await self._invoke(bot, "chart", {"market": "brent-above-90"})
        self.assertEqual(
            [url for url, _ in self.calls],
            [
                main.POLYMARKET_MARKETS_URL,
                main.POLYMARKET_SEARCH_URL,
                main.POLYMARKET_PRICE_HISTORY_URL,
            ],
        )
        self.assertIn("embed", interaction.followup.sent[0])

    async def test_a_phrase_query_goes_straight_to_search(self):
        def handler(url):
            if url == main.POLYMARKET_SEARCH_URL:
                return {"events": [{"closed": False, "markets": [market()]}]}
            if url == main.POLYMARKET_PRICE_HISTORY_URL:
                return history()
            raise AssertionError(f"unexpected URL {url}")

        bot = await self._bot(handler)
        await self._invoke(bot, "chart", {"market": "Brent crude above 90"})
        self.assertEqual(
            [url for url, _ in self.calls][0], main.POLYMARKET_SEARCH_URL
        )

    async def test_unknown_market_reports_instead_of_crashing(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return []
            return {"events": []}

        bot = await self._bot(handler)
        interaction = await self._invoke(bot, "chart", {"market": "nope"})
        self.assertEqual(len(interaction.followup.sent), 1)
        self.assertIn("couldn't find", interaction.followup.sent[0]["content"])
        self.assertTrue(interaction.followup.sent[0]["ephemeral"])

    async def test_market_without_a_token_is_reported(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return [market(tokens=None)]
            raise AssertionError(f"unexpected URL {url}")

        bot = await self._bot(handler)
        interaction = await self._invoke(bot, "chart", {"market": "brent-above-90"})
        self.assertIn("no price history", interaction.followup.sent[0]["content"])

    async def test_too_little_history_is_reported(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return [market()]
            return {"history": [{"t": 1791909838, "p": 0.5}]}

        bot = await self._bot(handler)
        interaction = await self._invoke(bot, "chart", {"market": "brent-above-90"})
        self.assertIn("enough price history", interaction.followup.sent[0]["content"])

    async def test_price_history_failure_is_reported(self):
        def handler(url):
            if url == main.POLYMARKET_MARKETS_URL:
                return [market()]
            raise RuntimeError("clob is down")

        bot = await self._bot(handler)
        interaction = await self._invoke(bot, "chart", {"market": "brent-above-90"})
        self.assertIn(
            "didn't return price history", interaction.followup.sent[0]["content"]
        )


class PriceHistoryTests(unittest.TestCase):
    def test_parses_and_sorts_points(self):
        points = main.parse_price_history(
            {"history": [{"t": 30, "p": 0.3}, {"t": 10, "p": 0.1}]}
        )
        self.assertEqual(points, [(10, 0.1), (30, 0.3)])

    def test_rejects_malformed_payloads(self):
        self.assertEqual(main.parse_price_history(None), [])
        self.assertEqual(main.parse_price_history({"history": "nope"}), [])
        self.assertEqual(
            main.parse_price_history({"history": [{"t": "x", "p": 1}, "junk"]}), []
        )

    def test_first_clob_token_id_handles_both_encodings(self):
        self.assertEqual(
            main.first_clob_token_id({"clobTokenIds": '["abc", "def"]'}), "abc"
        )
        self.assertEqual(
            main.first_clob_token_id({"clobTokenIds": ["abc"]}), "abc"
        )
        self.assertIsNone(main.first_clob_token_id({}))
        self.assertIsNone(main.first_clob_token_id({"clobTokenIds": "nope"}))
        self.assertIsNone(main.first_clob_token_id({"clobTokenIds": []}))

    def test_chart_interval_maps_lookbacks(self):
        self.assertEqual(main.chart_interval(1), "1d")
        self.assertEqual(main.chart_interval(7), "1w")
        self.assertEqual(main.chart_interval(30), "1m")
        self.assertEqual(main.chart_interval(90), "max")

    def test_render_price_chart_produces_a_png(self):
        data = main.render_price_chart(
            question="Will Brent crude rise above $90 before Dec 31?",
            slug="brent-above-90",
            outcome_label="Yes",
            points=main.parse_price_history(history()),
        )
        self.assertEqual(data[:8], PNG_MAGIC)
        self.assertGreater(len(data), 5000)


class NewsProviderTests(unittest.IsolatedAsyncioTestCase):
    def test_build_news_provider_rejects_an_unwired_provider(self):
        with self.assertRaises(RuntimeError) as ctx:
            main.build_news_provider(None, api_key="k", provider_name="NewsWire")
        self.assertIn("not configured", str(ctx.exception).lower())

    def test_build_news_provider_defaults_to_newsapi(self):
        adapter = main.build_news_provider(None, api_key="k")
        self.assertEqual(adapter.name, "NewsAPI")
        self.assertEqual(adapter.api_key, "k")

    def test_parse_news_api_keys_dedupes_and_skips_the_primary(self):
        self.assertEqual(main.parse_news_api_keys("a, b ; a", "a"), ["b"])
        self.assertEqual(main.parse_news_api_keys("", "a"), [])
        self.assertEqual(main.parse_news_api_keys(" b , c ", None), ["b", "c"])

    async def test_fetch_articles_sends_the_documented_request(self):
        seen: list[tuple] = []

        async def request_json(url, params=None, headers=None):
            seen.append((url, params, headers))
            return {"status": "ok", "articles": [{"title": "hi"}]}

        adapter = main.NewsAPIAdapter(
            None, api_key="secret", language="en", request_json=request_json
        )
        articles = await adapter.fetch_articles("brent OR crude")
        self.assertEqual(articles, [{"title": "hi"}])
        url, params, headers = seen[0]
        self.assertEqual(url, main.NEWS_API_URL)
        self.assertEqual(params["q"], "brent OR crude")
        self.assertEqual(params["sortBy"], "publishedAt")
        self.assertEqual(headers, {"X-Api-Key": "secret"})

    async def test_missing_key_raises_before_any_request(self):
        adapter = main.NewsAPIAdapter(None, api_key="")
        with self.assertRaises(RuntimeError):
            await adapter.fetch_articles("test")

    async def test_error_status_is_surfaced(self):
        async def request_json(url, params=None, headers=None):
            return {"status": "error", "message": "rate limited"}

        adapter = main.NewsAPIAdapter(None, api_key="k", request_json=request_json)
        with self.assertRaises(RuntimeError) as ctx:
            await adapter.fetch_articles("test")
        self.assertIn("rate limited", str(ctx.exception))

    async def test_a_rejected_key_is_never_written_to_the_logs(self):
        """A credential must not leak into the logs when a request fails."""

        async def request_json(url, params=None, headers=None):
            if headers["X-Api-Key"] == "super-secret-primary":
                raise RuntimeError("HTTP 429")
            return {"status": "ok", "articles": []}

        adapter = main.NewsAPIAdapter(
            None,
            api_key="super-secret-primary",
            api_keys=["backup"],
            request_json=request_json,
        )
        with self.assertLogs("lash-news-bot", level="WARNING") as logs:
            await adapter.fetch_articles("q")
        self.assertTrue(logs.output, "a rejected key must be reported")
        for line in logs.output:
            self.assertNotIn("super-secret-primary", line)

    def test_a_missing_news_key_is_an_actionable_startup_warning(self):
        previous = os.environ.pop("NEWS_API_KEY", None)
        try:
            with self.assertLogs("lash-news-bot", level="WARNING") as logs:
                bot = main.create_bot()
            message = "\n".join(logs.output)
            self.assertIn("NEWS_API_KEY", message)
            self.assertIn("news", message.lower())
            self.assertEqual(bot.news_api_keys, [])
        finally:
            if previous is not None:
                os.environ["NEWS_API_KEY"] = previous

    async def test_a_throttled_key_falls_through_to_the_next(self):
        attempts: list[str] = []

        async def request_json(url, params=None, headers=None):
            attempts.append(headers["X-Api-Key"])
            if headers["X-Api-Key"] == "first":
                raise RuntimeError("HTTP 429")
            return {"status": "ok", "articles": []}

        adapter = main.NewsAPIAdapter(
            None, api_key="first", api_keys=["second"], request_json=request_json
        )
        self.assertEqual(await adapter.fetch_articles("q"), [])
        self.assertEqual(attempts, ["first", "second"])

    async def test_the_final_key_error_is_raised(self):
        async def request_json(url, params=None, headers=None):
            raise RuntimeError(f"HTTP 429 for {headers['X-Api-Key']}")

        adapter = main.NewsAPIAdapter(
            None, api_key="first", api_keys=["second"], request_json=request_json
        )
        with self.assertRaises(RuntimeError) as ctx:
            await adapter.fetch_articles("q")
        self.assertIn("second", str(ctx.exception))

    async def test_the_bot_wires_its_configured_provider(self):
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                bot = main.create_bot()
                bot.news_api_keys = ["primary", "backup"]
                seen: list[dict] = []

                async def request_json(url, params=None, headers=None):
                    seen.append(headers)
                    return {"status": "ok", "articles": []}

                bot.get_json = request_json
                adapter = bot._news_provider()
                self.assertEqual(adapter.api_keys, ["primary", "backup"])
                await adapter.fetch_articles("q")
                self.assertEqual(seen[0]["X-Api-Key"], "primary")
                await bot.close()
            finally:
                os.chdir(cwd)


class ChartDependencyTests(unittest.TestCase):
    """Regression test for the production /chart failure.

    /chart reported "Chart rendering isn't available right now" in production
    while passing locally, because main.py imports matplotlib lazily -- so a
    broken install disables /chart instead of stopping the bot -- and the local
    virtualenv already had it. The Docker image installs only what
    requirements.txt declares, and matplotlib was missing from it. This pins the
    declared dependency the image needs.
    """

    def test_matplotlib_is_a_declared_requirement(self):
        requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")
        declared = {
            line.split("=")[0].split(">")[0].strip().lower()
            for line in requirements.splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        self.assertIn(
            "matplotlib",
            declared,
            "matplotlib must be declared or the deployed image cannot render "
            "/chart",
        )

    def test_a_missing_renderer_raises_instead_of_returning_a_blank_image(self):
        import sys

        with mock.patch.dict(sys.modules, {"matplotlib": None}):
            with self.assertRaises(ImportError):
                main.render_price_chart(
                    question="q",
                    slug="s",
                    outcome_label="Yes",
                    points=[(10, 0.5), (20, 0.6)],
                )


if __name__ == "__main__":
    unittest.main()
