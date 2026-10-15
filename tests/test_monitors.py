"""Tests for the Polymarket monitors, using payload shapes captured from the live APIs.

Covers: first-run suppression, dedupe across poll cycles, per-server thresholds and
alert toggles, and that an alert is only sent when the data supports it.
"""

import asyncio
import os
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import main  # noqa: E402


def market(
    market_id: str,
    *,
    question: str = "Will it happen?",
    slug: str = "will-it-happen",
    liquidity: float = 50_000.0,
    prices: str = '["0.50", "0.50"]',
    active: bool = True,
) -> dict:
    """A trimmed Gamma /markets entry shaped like the live response."""
    return {
        "id": market_id,
        "question": question,
        "slug": slug,
        "active": active,
        "closed": False,
        "archived": False,
        "liquidityNum": liquidity,
        "volumeNum": 10_000.0,
        "liquidity": "50000",
        "outcomes": '["Yes", "No"]',
        "outcomePrices": prices,
        "endDate": "2026-12-31T00:00:00Z",
        "icon": "https://example.test/icon.png",
    }


def trade(
    tx: str,
    *,
    size: float = 20_000.0,
    price: float = 0.80,
    timestamp: int = 1791909838,
) -> dict:
    """A trimmed Data API /v2/trades entry using the live snake_case field names."""
    return {
        "proxy_wallet": "0x5fdd66ffd0325d4e8d1d86fbec50caa31770a747",
        "transaction_hash": tx,
        "token_id": "93165051950362910307954899561590757548504145872776331937416526730412530406088",
        "side": "BUY",
        "size": size,
        "price": price,
        "timestamp": timestamp,
        "title": "Racing Club de Lens vs. Sporting CP: O/U 1.5",
        "outcome": "Over",
        "slug": "ucl-rcl-spo-2026-10-13-total-1pt5",
    }


class FakeChannel:
    def __init__(self, channel_id: int = 555) -> None:
        self.id = channel_id
        self.mention = f"<#{channel_id}>"


class MonitorTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _bot(self, **settings):
        bot = main.create_bot()
        bot.tree.sync = lambda guild=None: asyncio.sleep(0, result=[])

        async def no_sync(guild=None):
            return []

        bot.tree.sync = no_sync
        self.posts: list[tuple[str, main.discord.Embed]] = []

        async def record_send(channel, embed, guild_id):
            self.posts.append((guild_id, embed))
            return True

        bot.send_embed = record_send
        for guild_id, overrides in {"1": settings}.items():
            bot.ensure_settings(guild_id).update(overrides)
        self.addAsyncCleanup(bot.close)
        return bot

    def _stub_markets(self, bot, payloads):
        """Return each payload in turn from bot.get_json."""
        queue = list(payloads)

        async def stub(url, params=None, headers=None, timeout=None):
            if not queue:
                raise AssertionError("get_json called more times than expected")
            return queue.pop(0)

        bot.get_json = stub


class NewMarketTests(MonitorTestCase):
    async def test_first_run_records_silently_then_alerts_on_genuinely_new_markets(self):
        bot = await self._bot()
        self._stub_markets(
            bot,
            [
                [market("1"), market("2")],          # first cycle: baseline only
                [market("3"), market("2"), market("1")],  # cycle 2: only "3" is new
            ],
        )
        destinations = {"1": FakeChannel()}

        await bot.run_new_market_cycle(destinations)
        self.assertEqual(self.posts, [], "first run must not flood old markets")

        await bot.run_new_market_cycle(destinations)
        self.assertEqual(len(self.posts), 1)
        guild_id, embed = self.posts[0]
        self.assertEqual(guild_id, "1")
        self.assertIn("Will it happen?", embed.description)
        self.assertIn("/market/will-it-happen", embed.description)

    async def test_same_market_is_never_alerted_twice(self):
        bot = await self._bot()
        repeated = [market("1"), market("2")]
        self._stub_markets(bot, [repeated, list(repeated), list(repeated)])
        destinations = {"1": FakeChannel()}

        await bot.run_new_market_cycle(destinations)   # baseline
        await bot.run_new_market_cycle(destinations)
        await bot.run_new_market_cycle(destinations)
        self.assertEqual(self.posts, [])

    async def test_liquidity_floor_and_alert_toggle_are_honoured(self):
        bot = await self._bot(new_market_min_liquidity=100_000.0)
        self._stub_markets(bot, [[], [market("9", liquidity=50_000.0)]])
        destinations = {"1": FakeChannel()}
        await bot.run_new_market_cycle(destinations)
        await bot.run_new_market_cycle(destinations)
        self.assertEqual(self.posts, [], "below-floor market must not alert")

        bot.ensure_settings("1")["alerts"]["new_market"] = False
        self._stub_markets(bot, [[], [market("10", liquidity=200_000.0)]])
        await bot.run_new_market_cycle(destinations)
        await bot.run_new_market_cycle(destinations)
        self.assertEqual(self.posts, [], "disabled alert type must not post")

    async def test_inactive_and_closed_markets_are_ignored(self):
        bot = await self._bot()
        self._stub_markets(
            bot,
            [[], [market("20", active=False), market("21", question="")]],
        )
        await bot.run_new_market_cycle({"1": FakeChannel()})
        await bot.run_new_market_cycle({"1": FakeChannel()})
        self.assertEqual(self.posts, [])


class PriceMoveTests(MonitorTestCase):
    async def test_first_sighting_baselines_then_a_large_move_alerts(self):
        bot = await self._bot(price_move_threshold=10.0)
        self._stub_markets(
            bot,
            [
                [market("1", prices='["0.50", "0.50"]')],
                [market("1", prices='["0.65", "0.35"]')],
            ],
        )
        destinations = {"1": FakeChannel()}

        await bot.run_price_cycle(destinations)
        self.assertEqual(self.posts, [], "first sighting must only record a baseline")

        await bot.run_price_cycle(destinations)
        self.assertEqual(len(self.posts), 1)
        _, embed = self.posts[0]
        self.assertIn("+15.0 points", embed.fields[1].value)
        self.assertEqual(embed.color, main.discord.Color.green())

    async def test_a_small_move_does_not_alert(self):
        bot = await self._bot(price_move_threshold=10.0)
        self._stub_markets(
            bot,
            [
                [market("1", prices='["0.50", "0.50"]')],
                [market("1", prices='["0.53", "0.47"]')],
            ],
        )
        await bot.run_price_cycle({"1": FakeChannel()})
        await bot.run_price_cycle({"1": FakeChannel()})
        self.assertEqual(self.posts, [])

    async def test_low_liquidity_markets_are_skipped(self):
        bot = await self._bot(price_move_threshold=10.0)
        low = market("1", prices='["0.50", "0.50"]', liquidity=10.0)
        low_moved = market("1", prices='["0.90", "0.10"]', liquidity=10.0)
        self._stub_markets(bot, [[low], [low_moved]])
        await bot.run_price_cycle({"1": FakeChannel()})
        await bot.run_price_cycle({"1": FakeChannel()})
        self.assertEqual(self.posts, [])

    async def test_a_move_does_not_alert_twice_for_the_same_level(self):
        bot = await self._bot(price_move_threshold=10.0)
        first = [market("1", prices='["0.50", "0.50"]')]
        moved = [market("1", prices='["0.70", "0.30"]')]
        self._stub_markets(bot, [first, moved, moved])
        destinations = {"1": FakeChannel()}
        await bot.run_price_cycle(destinations)
        await bot.run_price_cycle(destinations)
        await bot.run_price_cycle(destinations)
        self.assertEqual(len(self.posts), 1)


class WhaleTests(MonitorTestCase):
    async def test_first_run_is_silent_then_new_fills_alert(self):
        bot = await self._bot(whale_min_usd=10_000.0)
        self._stub_markets(
            bot,
            [
                {"data": [trade("0xaa")]},
                {"data": [trade("0xbb"), trade("0xaa")]},
            ],
        )
        destinations = {"1": FakeChannel()}

        await bot.run_whale_cycle(destinations)
        self.assertEqual(self.posts, [])

        await bot.run_whale_cycle(destinations)
        self.assertEqual(len(self.posts), 1)
        _, embed = self.posts[0]
        self.assertIn("Large trade", embed.title)
        self.assertIn("Executed taker trade", embed.fields[-1].value)
        self.assertIn("0xbb", embed.fields[-2].value)

    async def test_trades_below_the_threshold_do_not_alert(self):
        bot = await self._bot(whale_min_usd=100_000.0)
        small = trade("0xcc", size=100.0, price=0.50)  # $50
        self._stub_markets(bot, [{"data": []}, {"data": [small]}])
        await bot.run_whale_cycle({"1": FakeChannel()})
        await bot.run_whale_cycle({"1": FakeChannel()})
        self.assertEqual(self.posts, [])

    async def test_a_fill_is_never_alerted_twice(self):
        bot = await self._bot(whale_min_usd=1_000.0)
        fill = {"data": [trade("0xdd")]}
        self._stub_markets(bot, [{"data": []}, dict(fill), dict(fill)])
        destinations = {"1": FakeChannel()}
        await bot.run_whale_cycle(destinations)
        await bot.run_whale_cycle(destinations)
        await bot.run_whale_cycle(destinations)
        self.assertEqual(len(self.posts), 1)

    async def test_unexpected_payload_raises_so_the_cycle_can_isolate_it(self):
        bot = await self._bot()
        self._stub_markets(bot, [{"unexpected": True}])
        with self.assertRaises(RuntimeError):
            await bot.run_whale_cycle({"1": FakeChannel()})

    async def test_servers_with_different_thresholds_get_different_alerts(self):
        bot = await self._bot(whale_min_usd=100_000.0)
        bot.ensure_settings("2").update(
            {"channel_id": 556, "whale_min_usd": 10_000.0}
        )
        self._stub_markets(bot, [{"data": []}, {"data": [trade("0xee", size=20_000.0, price=0.80)]}])
        await bot.run_whale_cycle({"1": FakeChannel(555), "2": FakeChannel(556)})
        await bot.run_whale_cycle({"1": FakeChannel(555), "2": FakeChannel(556)})
        self.assertEqual([guild for guild, _ in self.posts], ["2"])


class MonitorCycleIsolationTests(MonitorTestCase):
    async def test_one_failing_subsystem_does_not_stop_the_others(self):
        bot = await self._bot(channel_id=555)
        calls: list[str] = []

        async def boom(destinations):
            calls.append("boom")
            raise RuntimeError("upstream exploded")

        async def ok(destinations):
            calls.append("ok")

        bot.run_news_cycle = boom
        bot.run_new_market_cycle = ok
        bot.run_price_cycle = ok
        bot.run_whale_cycle = ok

        async def destinations_by_category():
            return {category: {"1": FakeChannel()} for category in main.ALERT_TYPES}

        bot.get_alert_destinations = destinations_by_category

        class FakeSession:
            async def close(self):
                return None

        bot.news_session = FakeSession()

        await bot.run_monitor_cycle()
        self.assertEqual(calls, ["boom", "ok", "ok", "ok"])
        self.assertIn("upstream exploded", bot.last_poll_error)


if __name__ == "__main__":
    unittest.main()
