"""Tests for wallet watchlists, wallet activity monitoring, and persistence.

Every test here runs offline: the Polymarket activity feed is replaced by a
deterministic in-memory fake, so these are simulation tests and not live
provider checks. The live endpoint shape they model was verified against
``https://data-api.polymarket.com/activity?user=<address>``.
"""

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import discord  # noqa: E402
from discord import app_commands  # noqa: E402

import main  # noqa: E402

ADDRESS = "0x5fdd66ffd0325d4e8d1d86fbec50caa31770a747"
OTHER_ADDRESS = "0x0000000000000000000000000000000000000001"
GUILD = 111
OTHER_GUILD = 222


def activity(
    *,
    timestamp: float,
    transaction: str = "0xabc",
    value: float = 100.0,
    kind: str = "TRADE",
    side: str = "BUY",
    size: float = 200.0,
    price: float = 0.5,
    address: str = ADDRESS,
) -> dict:
    """One activity row shaped exactly like the live Data API response."""
    return {
        "proxyWallet": address,
        "timestamp": timestamp,
        "conditionId": "0xcondition",
        "type": kind,
        "size": size,
        "usdcSize": value,
        "transactionHash": transaction,
        "price": price,
        "asset": "110708529615315935570159860749875594852726156979453433884273897043825256543816",
        "side": side,
        "outcomeIndex": 0,
        "title": "Will it rain tomorrow?",
        "slug": "will-it-rain-tomorrow",
        "eventSlug": "will-it-rain-tomorrow",
        "outcome": "Yes",
        "name": "someone",
        "pseudonym": "Quiet-Cipher",
    }


class FakeResponse:
    def __init__(self) -> None:
        self.done = False
        self.sent: list[dict] = []

    def is_done(self) -> bool:
        return self.done

    async def send_message(self, content=None, **kwargs):
        self.done = True
        self.sent.append({"content": content, **kwargs})

    async def defer(self, *, ephemeral=False, thinking=False):
        self.done = True
        self.sent.append({"deferred": True, "ephemeral": ephemeral})


class FakeFollowup:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, content=None, **kwargs):
        self.sent.append({"content": content, **kwargs})


class FakeInteraction:
    """Just enough of an interaction for the /wallet callbacks."""

    def __init__(self, *, administrator: bool = True, guild_id: int = GUILD) -> None:
        self.response = FakeResponse()
        self.followup = FakeFollowup()
        self.command = SimpleNamespace(name="wallet")
        self.user = SimpleNamespace(
            id=7,
            guild_permissions=SimpleNamespace(administrator=administrator),
        )
        self.guild = SimpleNamespace(id=guild_id) if guild_id is not None else None
        self.guild_id = guild_id

    @property
    def replies(self) -> list[dict]:
        return self.response.sent + self.followup.sent

    @property
    def text(self) -> str:
        return "\n".join(
            str(reply.get("content")) for reply in self.replies if reply.get("content")
        )


class FakeChannel:
    def __init__(self, guild_id: int = GUILD) -> None:
        self.id = 900 + guild_id
        self.guild = SimpleNamespace(id=guild_id)
        self.embeds: list[discord.Embed] = []

    async def send(self, *, embed=None, allowed_mentions=None, **kwargs):
        self.embeds.append(embed)


class WalletTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.feed: dict[str, list[dict]] = {}

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _bot(self) -> main.LashNewsBot:
        bot = main.create_bot()

        async def no_sync(guild=None):
            return bot.tree.get_commands()

        async def idle_monitor():
            await asyncio.Event().wait()

        bot.tree.sync = no_sync
        bot.poll_news_forever = idle_monitor
        bot._connection.application_id = 999

        async def fetch(address: str) -> list[dict]:
            return list(self.feed.get(address, []))

        bot.fetch_wallet_activity = fetch
        self.addAsyncCleanup(bot.close)
        await bot.setup_hook()
        return bot

    def _group(self, bot: main.LashNewsBot) -> app_commands.Group:
        group = bot.tree.get_command("wallet")
        self.assertIsInstance(group, app_commands.Group)
        return group

    async def _call(self, bot, name, interaction, **params):
        command = self._group(bot).get_command(name)
        self.assertIsNotNone(command, f"/wallet {name} is not registered")
        await command._do_call(interaction, params)
        return interaction


class AddressValidationTests(unittest.TestCase):
    def test_valid_addresses_are_normalised_to_lower_case(self):
        self.assertEqual(main.normalize_wallet_address(ADDRESS.upper()), ADDRESS)
        self.assertEqual(main.normalize_wallet_address(f"  {ADDRESS}  "), ADDRESS)

    def test_invalid_addresses_are_rejected(self):
        for value in (
            "",
            "not-an-address",
            "0x1234",
            "5fdd66ffd0325d4e8d1d86fbec50caa31770a747",
            "0x5fdd66ffd0325d4e8d1d86fbec50caa31770a74g",
            None,
            12345,
        ):
            with self.subTest(value=value):
                self.assertIsNone(main.normalize_wallet_address(value))

    def test_short_address_helper_keeps_both_ends(self):
        self.assertEqual(main.short_wallet_address(ADDRESS), "0x5fdd\u2026a747")


class WatchlistCommandTests(WalletTestCase):
    async def test_add_list_and_remove_round_trip(self):
        bot = await self._bot()
        self.feed[ADDRESS] = [activity(timestamp=1000.0)]

        added = await self._call(bot, "add", FakeInteraction(), address=ADDRESS, label="Whale 1")
        self.assertIn("Tracking", added.text)
        self.assertIn("Whale 1", added.text)
        self.assertEqual([w["address"] for w in bot.list_watches(GUILD)], [ADDRESS])

        listed = await self._call(bot, "list", FakeInteraction())
        self.assertIn("Whale 1", listed.text)
        self.assertIn("Last wallet check", listed.text)

        removed = await self._call(bot, "remove", FakeInteraction(), address=ADDRESS)
        self.assertIn("Stopped tracking", removed.text)
        self.assertEqual(bot.list_watches(GUILD), [])

    async def test_an_invalid_address_is_reported_and_not_stored(self):
        bot = await self._bot()
        interaction = await self._call(
            bot, "add", FakeInteraction(), address="not-an-address"
        )
        self.assertIn("not a wallet address", interaction.text)
        self.assertEqual(bot.list_watches(GUILD), [])

    async def test_adding_the_same_wallet_twice_is_reported(self):
        bot = await self._bot()
        await self._call(bot, "add", FakeInteraction(), address=ADDRESS)
        again = await self._call(bot, "add", FakeInteraction(), address=ADDRESS.upper())
        self.assertIn("already tracked", again.text)
        self.assertEqual(len(bot.list_watches(GUILD)), 1)

    async def test_a_non_administrator_cannot_change_the_watchlist(self):
        bot = await self._bot()
        interaction = await self._call(
            bot, "add", FakeInteraction(administrator=False), address=ADDRESS
        )
        self.assertIn("administrator", interaction.text.lower())
        self.assertEqual(bot.list_watches(GUILD), [])

    async def test_the_watchlist_is_capped(self):
        bot = await self._bot()
        for index in range(main.MAX_TRACKED_WALLETS_PER_GUILD):
            address = f"0x{index:040x}"
            await self._call(bot, "add", FakeInteraction(), address=address)
        overflow = await self._call(
            bot, "add", FakeInteraction(), address=f"0x{'f' * 40}"
        )
        self.assertIn("maximum", overflow.text)
        self.assertEqual(
            len(bot.list_watches(GUILD)), main.MAX_TRACKED_WALLETS_PER_GUILD
        )

    async def test_settings_update_and_report_current_values(self):
        bot = await self._bot()
        await self._call(bot, "add", FakeInteraction(), address=ADDRESS)

        shown = await self._call(bot, "settings", FakeInteraction(), address=ADDRESS)
        self.assertIn("Current settings", shown.text)

        updated = await self._call(
            bot,
            "settings",
            FakeInteraction(),
            address=ADDRESS,
            label="Renamed",
            min_usd=2500.0,
            activity=app_commands.Choice(name="All activity", value="all"),
            whale_alerts=True,
        )
        self.assertIn("Updated", updated.text)
        watch = bot.get_watch(GUILD, ADDRESS)
        self.assertEqual(watch["label"], "Renamed")
        self.assertEqual(watch["min_usd"], 2500.0)
        self.assertEqual(watch["activity_filter"], "all")
        self.assertFalse(watch["suppress_whale"])

    async def test_pause_and_resume_change_only_the_paused_flag(self):
        bot = await self._bot()
        await self._call(bot, "add", FakeInteraction(), address=ADDRESS)
        bot.get_watch(GUILD, ADDRESS)["last_seen_ts"] = 500.0

        paused = await self._call(bot, "pause", FakeInteraction(), address=ADDRESS)
        self.assertIn("paused", paused.text)
        self.assertTrue(bot.get_watch(GUILD, ADDRESS)["paused"])

        again = await self._call(bot, "pause", FakeInteraction(), address=ADDRESS)
        self.assertIn("already paused", again.text)

        resumed = await self._call(bot, "resume", FakeInteraction(), address=ADDRESS)
        self.assertIn("active", resumed.text)
        watch = bot.get_watch(GUILD, ADDRESS)
        self.assertFalse(watch["paused"])
        # Resuming re-baselines so nothing from the paused window is replayed.
        self.assertEqual(watch["last_seen_ts"], 0.0)

    async def test_info_reports_status_for_a_tracked_wallet(self):
        bot = await self._bot()
        await self._call(bot, "add", FakeInteraction(), address=ADDRESS, label="Watch me")
        info = await self._call(bot, "info", FakeInteraction(), address=ADDRESS)
        self.assertIn("Watch me", info.text)
        self.assertIn(ADDRESS, info.text)

    async def test_info_reports_an_unknown_wallet(self):
        bot = await self._bot()
        info = await self._call(bot, "info", FakeInteraction(), address=ADDRESS)
        self.assertIn("not tracked", info.text)


class WatchlistPersistenceTests(WalletTestCase):
    async def test_a_watchlist_survives_a_restart(self):
        first = await self._bot()
        await self._call(
            first, "add", FakeInteraction(), address=ADDRESS, label="Survivor"
        )

        second = await self._bot()
        watch = second.get_watch(GUILD, ADDRESS)
        self.assertIsNotNone(watch)
        self.assertEqual(watch["label"], "Survivor")

    async def test_servers_keep_separate_watchlists(self):
        bot = await self._bot()
        await self._call(bot, "add", FakeInteraction(guild_id=GUILD), address=ADDRESS)

        self.assertIsNotNone(bot.get_watch(GUILD, ADDRESS))
        self.assertIsNone(bot.get_watch(OTHER_GUILD, ADDRESS))

        other = await self._call(
            bot, "list", FakeInteraction(guild_id=OTHER_GUILD)
        )
        self.assertIn("not tracking any wallets", other.text)

    async def test_a_corrupt_watchlist_file_does_not_break_startup(self):
        (Path(".") / "wallets.json").write_text("{not json", encoding="utf-8")
        self.assertEqual(main.load_wallet_state(), {})

    async def test_unknown_keys_and_addresses_are_dropped_on_load(self):
        state = main.normalize_wallet_state(
            {
                "111": {"watches": {"0x-deadbeef": {"label": "bad"}, ADDRESS: {"label": "ok"}}},
                "not-a-guild": {"watches": {ADDRESS: {}}},
                "222": "not-a-dict",
            }
        )
        self.assertEqual(set(state), {"111"})
        self.assertEqual(set(state["111"]["watches"]), {ADDRESS})


class WalletActivityCycleTests(WalletTestCase):
    async def _seeded_bot(self, *, min_usd=None, activity_filter=None, paused=False):
        bot = await self._bot()
        watch = main.default_wallet_watch(ADDRESS)
        if min_usd is not None:
            watch["min_usd"] = min_usd
        if activity_filter is not None:
            watch["activity_filter"] = activity_filter
        watch["paused"] = paused
        bot.set_watch(GUILD, watch)
        channel = FakeChannel(GUILD)
        self.feed[ADDRESS] = [activity(timestamp=1000.0, transaction="0xseed")]
        await bot.run_wallet_cycle({str(GUILD): channel})
        return bot, channel

    async def test_the_first_check_only_records_a_baseline(self):
        bot, channel = await self._seeded_bot()
        self.assertEqual(channel.embeds, [])
        self.assertEqual(bot.get_watch(GUILD, ADDRESS)["last_seen_ts"], 1000.0)

    async def test_new_activity_posts_one_wallet_alert(self):
        bot, channel = await self._seeded_bot()
        self.feed[ADDRESS].append(activity(timestamp=2000.0, transaction="0xfresh"))

        await bot.run_wallet_cycle({str(GUILD): channel})

        self.assertEqual(len(channel.embeds), 1)
        embed = channel.embeds[0]
        self.assertIn("Tracked wallet", embed.title)
        self.assertIn("Yes", embed.description)
        fields = {field.name: field.value for field in embed.fields}
        self.assertEqual(fields["Activity"], "Trade")
        self.assertIn("$", fields["Transaction value"])
        self.assertIn("polygonscan.com/tx/0xfresh", fields["Transaction"])
        self.assertIn("not the wallet's total", fields["Scope"])
        self.assertEqual(bot.get_watch(GUILD, ADDRESS)["last_seen_ts"], 2000.0)

    async def test_the_same_activity_is_never_posted_twice(self):
        bot, channel = await self._seeded_bot()
        self.feed[ADDRESS].append(activity(timestamp=2000.0, transaction="0xfresh"))

        await bot.run_wallet_cycle({str(GUILD): channel})
        await bot.run_wallet_cycle({str(GUILD): channel})

        self.assertEqual(len(channel.embeds), 1)

    async def test_each_address_is_fetched_once_per_cycle(self):
        bot = await self._bot()
        calls: list[str] = []

        async def counting_fetch(address: str) -> list[dict]:
            calls.append(address)
            return [activity(timestamp=1000.0)]

        bot.fetch_wallet_activity = counting_fetch
        bot.set_watch(GUILD, main.default_wallet_watch(ADDRESS))
        bot.set_watch(OTHER_GUILD, main.default_wallet_watch(ADDRESS))

        await bot.run_wallet_cycle(
            {str(GUILD): FakeChannel(GUILD), str(OTHER_GUILD): FakeChannel(OTHER_GUILD)}
        )
        self.assertEqual(calls, [ADDRESS])

    async def test_activity_is_delivered_to_every_server_tracking_the_wallet(self):
        bot = await self._bot()
        bot.set_watch(GUILD, main.default_wallet_watch(ADDRESS))
        bot.set_watch(OTHER_GUILD, main.default_wallet_watch(ADDRESS))
        self.feed[ADDRESS] = [activity(timestamp=1000.0)]
        channels = {str(GUILD): FakeChannel(GUILD), str(OTHER_GUILD): FakeChannel(OTHER_GUILD)}
        await bot.run_wallet_cycle(channels)

        self.feed[ADDRESS].append(activity(timestamp=2000.0, transaction="0xfresh"))
        await bot.run_wallet_cycle(channels)

        self.assertEqual(len(channels[str(GUILD)].embeds), 1)
        self.assertEqual(len(channels[str(OTHER_GUILD)].embeds), 1)

    async def test_a_trades_only_watch_ignores_other_activity_types(self):
        bot, channel = await self._seeded_bot(activity_filter="trades")
        self.feed[ADDRESS].append(
            activity(timestamp=2000.0, transaction="0xredeem", kind="REDEEM", value=900.0)
        )

        await bot.run_wallet_cycle({str(GUILD): channel})
        self.assertEqual(channel.embeds, [])

        self.feed[ADDRESS].append(activity(timestamp=3000.0, transaction="0xtrade"))
        await bot.run_wallet_cycle({str(GUILD): channel})
        self.assertEqual(len(channel.embeds), 1)

    async def test_an_all_activity_watch_receives_non_trade_rows(self):
        bot, channel = await self._seeded_bot(activity_filter="all")
        self.feed[ADDRESS].append(
            activity(timestamp=2000.0, transaction="0xredeem", kind="REDEEM", value=900.0)
        )

        await bot.run_wallet_cycle({str(GUILD): channel})
        self.assertEqual(len(channel.embeds), 1)
        self.assertIn("Redeem", {field.value for field in channel.embeds[0].fields})

    async def test_the_minimum_value_filters_small_activity(self):
        bot, channel = await self._seeded_bot(min_usd=5000.0)
        self.feed[ADDRESS].append(
            activity(timestamp=2000.0, transaction="0xsmall", value=100.0)
        )
        await bot.run_wallet_cycle({str(GUILD): channel})
        self.assertEqual(channel.embeds, [])

        self.feed[ADDRESS].append(
            activity(timestamp=3000.0, transaction="0xbig", value=9000.0)
        )
        await bot.run_wallet_cycle({str(GUILD): channel})
        self.assertEqual(len(channel.embeds), 1)

    async def test_a_paused_wallet_does_not_alert_but_stays_tracked(self):
        bot, channel = await self._seeded_bot(paused=True)
        self.feed[ADDRESS].append(activity(timestamp=2000.0, transaction="0xfresh"))

        await bot.run_wallet_cycle({str(GUILD): channel})

        self.assertEqual(channel.embeds, [])
        self.assertIsNotNone(bot.get_watch(GUILD, ADDRESS))

    async def test_a_server_that_disabled_wallet_alerts_is_skipped(self):
        bot, channel = await self._seeded_bot()
        bot.ensure_settings(GUILD)["alerts"]["wallet"] = False
        self.feed[ADDRESS].append(activity(timestamp=2000.0, transaction="0xfresh"))

        await bot.run_wallet_cycle({str(GUILD): channel})
        self.assertEqual(channel.embeds, [])

    async def test_a_provider_failure_is_reported_without_losing_the_cycle(self):
        bot, channel = await self._seeded_bot()

        async def boom(address: str):
            raise RuntimeError("Polymarket is down")

        bot.fetch_wallet_activity = boom
        await bot.run_wallet_cycle({str(GUILD): channel})

        self.assertIsNotNone(bot.last_wallet_error)
        self.assertIn("failed", bot.last_wallet_error)
        self.assertIsNotNone(bot.last_wallet_check_at)

    async def test_the_address_count_is_capped_per_cycle(self):
        bot = await self._bot()
        for index in range(main.MAX_WALLET_ADDRESSES_PER_CYCLE + 2):
            bot.set_watch(GUILD, main.default_wallet_watch(f"0x{index:040x}"))
        calls: list[str] = []

        async def counting_fetch(address: str) -> list[dict]:
            calls.append(address)
            return []

        bot.fetch_wallet_activity = counting_fetch
        await bot.run_wallet_cycle({str(GUILD): FakeChannel(GUILD)})
        self.assertEqual(len(calls), main.MAX_WALLET_ADDRESSES_PER_CYCLE)


class WhaleDuplicateSuppressionTests(WalletTestCase):
    def _trade(self, wallet: str = ADDRESS) -> dict:
        return {
            "proxy_wallet": wallet,
            "side": "BUY",
            "size": 100000.0,
            "price": 0.5,
            "timestamp": 2000,
            "transaction_hash": "0xwhale",
            "title": "Will it rain tomorrow?",
            "slug": "will-it-rain-tomorrow",
            "outcome": "Yes",
        }

    async def test_a_tracked_wallet_fill_does_not_also_post_a_whale_alert(self):
        bot = await self._bot()
        bot.set_watch(GUILD, main.default_wallet_watch(ADDRESS))
        bot.monitor_state["whale_seeded"] = True
        channel = FakeChannel(GUILD)

        async def get_json(url, **kwargs):
            return {"data": [self._trade()]}

        bot.get_json = get_json
        await bot.run_whale_cycle({str(GUILD): channel})
        self.assertEqual(channel.embeds, [])

    async def test_whale_alerts_still_post_when_suppression_is_disabled(self):
        bot = await self._bot()
        watch = main.default_wallet_watch(ADDRESS)
        watch["suppress_whale"] = False
        bot.set_watch(GUILD, watch)
        bot.monitor_state["whale_seeded"] = True
        channel = FakeChannel(GUILD)

        async def get_json(url, **kwargs):
            return {"data": [self._trade()]}

        bot.get_json = get_json
        await bot.run_whale_cycle({str(GUILD): channel})
        self.assertEqual(len(channel.embeds), 1)
        self.assertIn("Large trade", channel.embeds[0].title)

    async def test_an_untracked_wallet_fill_still_posts_a_whale_alert(self):
        bot = await self._bot()
        bot.set_watch(GUILD, main.default_wallet_watch(ADDRESS))
        bot.monitor_state["whale_seeded"] = True
        channel = FakeChannel(GUILD)

        async def get_json(url, **kwargs):
            return {"data": [self._trade(wallet=OTHER_ADDRESS)]}

        bot.get_json = get_json
        await bot.run_whale_cycle({str(GUILD): channel})
        self.assertEqual(len(channel.embeds), 1)


class WalletChannelTests(WalletTestCase):
    async def test_the_wallet_category_can_use_its_own_channel(self):
        settings = main.normalize_guild_settings(
            {
                "channel_id": 555,
                "channels": {"whale": 666, "wallet": 777},
                "alerts": {"wallet": True},
            }
        )
        self.assertEqual(main.effective_channel_id(settings, "wallet"), 777)
        self.assertEqual(main.effective_channel_id(settings, "news"), 555)

    async def test_wallet_is_a_first_class_alert_category(self):
        self.assertIn("wallet", main.ALERT_TYPES)
        settings = main.default_guild_settings()
        self.assertIn("wallet", settings["alerts"])
        self.assertIn("wallet", settings["channels"])
        self.assertEqual(main.ALERT_CHANNEL_COMMANDS["wallet"], "/set-wallet-alerts")


if __name__ == "__main__":
    unittest.main()
