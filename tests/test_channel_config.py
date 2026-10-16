"""Tests for per-category, per-server channel configuration.

Covers the multi-server requirements: each alert category has its own channel,
settings are keyed by guild and category, changing one category never touches
another, settings persist across restarts, permissions are enforced, invalid or
deleted channels are reported instead of crashing, and the embed palette matches
the requested SovereignBot colors (gold news, green/red whales).
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


class FakeResponse:
    def __init__(self) -> None:
        self.done = False
        self.sent: list[tuple[str, dict]] = []

    def is_done(self) -> bool:
        return self.done

    async def send_message(self, content=None, **kwargs):
        self.done = True
        self.sent.append(("send_message", {"content": content, **kwargs}))

    async def defer(self, *, ephemeral=False, thinking=False):
        self.done = True
        self.sent.append(("defer", {"ephemeral": ephemeral, "thinking": thinking}))


class FakeFollowup:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    async def send(self, content=None, **kwargs):
        self.sent.append(("followup", {"content": content, **kwargs}))


class FakeChannel:
    """A text-channel stand-in with configurable bot permissions."""

    def __init__(
        self,
        channel_id: int,
        guild_id: int,
        *,
        send: bool = True,
        embed: bool = True,
        view: bool = True,
    ) -> None:
        self.id = channel_id
        self.guild = SimpleNamespace(id=guild_id)
        self.mention = f"<#{channel_id}>"
        self._permissions = SimpleNamespace(
            send_messages=send,
            embed_links=embed,
            view_channel=view,
        )
        self.sent_embeds: list = []

    def permissions_for(self, _member):
        return self._permissions

    async def send(self, *, embed=None, allowed_mentions=None, file=None):
        self.sent_embeds.append(embed)


class FakeGuild:
    def __init__(self, guild_id: int, *, me: object | None = None) -> None:
        self.id = guild_id
        self.me = object() if me is None else me


class CommandInteraction:
    """Interaction rich enough for a command callback to run to completion."""

    def __init__(self, guild: FakeGuild | None, *, administrator: bool = True) -> None:
        self.response = FakeResponse()
        self.followup = FakeFollowup()
        self.user = SimpleNamespace(
            id=1,
            guild_permissions=SimpleNamespace(administrator=administrator),
        )
        self.guild = guild
        self.guild_id = guild.id if guild is not None else None
        self.command = SimpleNamespace(name="test")

    @property
    def response_text(self) -> str:
        # Prefer the followup: deferred commands reply through it.
        for source in (self.followup.sent, self.response.sent):
            for _, payload in reversed(source):
                content = payload.get("content")
                if content:
                    return content
        return ""


def not_found_error() -> discord.NotFound:
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"), "Unknown Channel"
    )


class ChannelConfigTestCase(unittest.IsolatedAsyncioTestCase):
    """Base case with isolated state files and a stubbed Discord sync."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _build_bot(self):
        bot = main.create_bot()

        async def no_sync(guild=None):
            return []

        # Tests never log in, so wait_until_ready() would raise inside the
        # monitor task; keep it idle instead of starting real monitoring.
        async def idle_monitor():
            await asyncio.Event().wait()

        bot.tree.sync = no_sync
        bot.poll_news_forever = idle_monitor
        self.addAsyncCleanup(bot.close)
        return bot

    def _install_channels(self, bot, channels: dict[int, FakeChannel]) -> None:
        """Resolve every known channel id; raise NotFound for unknown ones."""
        calls: list[int] = []

        async def resolve(channel_id: int):
            calls.append(channel_id)
            channel = channels.get(channel_id)
            if channel is None:
                raise not_found_error()
            return channel

        bot.resolve_channel = resolve
        self.resolve_calls = calls


# ---------------------------------------------------------------------------
# Settings schema, normalization, and persistence
# ---------------------------------------------------------------------------


class SettingsSchemaTests(ChannelConfigTestCase):
    def test_normalize_accepts_valid_category_channels(self):
        normalized = main.normalize_guild_settings(
            {
                "channels": {
                    "news": 111,
                    "whale": 222,
                    "price": 333,
                    "new_market": 444,
                }
            }
        )
        self.assertEqual(
            normalized["channels"],
            {"news": 111, "whale": 222, "price": 333, "new_market": 444, "wallet": None},
        )

    def test_normalize_rejects_garbage_category_channels(self):
        normalized = main.normalize_guild_settings(
            {
                "channels": {
                    "news": "not-an-int",
                    "whale": True,       # bools are not channel ids
                    "price": 0,          # zero is not a channel id
                    "new_market": -5,    # negatives are not channel ids
                    "bogus": 123,        # unknown categories are ignored
                }
            }
        )
        self.assertEqual(
            normalized["channels"], {alert: None for alert in main.ALERT_TYPES}
        )

    def test_effective_channel_prefers_override_then_default(self):
        settings = main.normalize_guild_settings(
            {"channel_id": 555, "channels": {"whale": 666}}
        )
        self.assertEqual(main.effective_channel_id(settings, "whale"), 666)
        self.assertEqual(main.effective_channel_id(settings, "news"), 555)
        self.assertEqual(main.effective_channel_id(settings), 555)
        bare = main.default_guild_settings()
        self.assertIsNone(main.effective_channel_id(bare, "whale"))
        self.assertIsNone(main.effective_channel_id(bare))

    async def test_category_channels_persist_across_a_restart(self):
        bot = await self._build_bot()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666
        settings["channels"]["price"] = 777
        self.assertTrue(bot.save_settings())

        reloaded = main.load_guild_settings()
        self.assertEqual(reloaded["111"]["channel_id"], 555)
        self.assertEqual(reloaded["111"]["channels"]["whale"], 666)
        self.assertEqual(reloaded["111"]["channels"]["price"], 777)
        self.assertIsNone(reloaded["111"]["channels"]["news"])

    async def test_two_servers_keep_independent_category_channels(self):
        bot = await self._build_bot()
        first = bot.ensure_settings("1")
        first["channels"]["whale"] = 1111
        second = bot.ensure_settings("2")
        second["channels"]["whale"] = 2222
        second["channel_id"] = 9
        self.assertTrue(bot.save_settings())

        reloaded = main.load_guild_settings()
        self.assertEqual(reloaded["1"]["channels"]["whale"], 1111)
        self.assertIsNone(reloaded["1"]["channel_id"])
        self.assertEqual(reloaded["2"]["channels"]["whale"], 2222)
        self.assertEqual(reloaded["2"]["channel_id"], 9)


# ---------------------------------------------------------------------------
# /set-... commands
# ---------------------------------------------------------------------------


SET_COMMANDS = {
    "set-news-alerts": "news",
    "set-price-alerts": "price",
    "set-new-markets": "new_market",
    "set-whale-alerts": "whale",
}


class SetterCommandTests(ChannelConfigTestCase):
    async def _set_channel(self, bot, command_name, interaction, channel=None):
        command = bot.tree.get_command(command_name)
        params = {} if channel is None else {"channel": channel}
        await command._do_call(interaction, params)

    async def test_every_set_command_stores_only_its_own_category(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        guild = FakeGuild(111)
        interaction = CommandInteraction(guild)
        channel = FakeChannel(1001, 111)

        await self._set_channel(bot, "set-whale-alerts", interaction, channel)

        settings = bot.guild_settings["111"]
        self.assertEqual(settings["channels"]["whale"], 1001)
        for category in main.ALERT_TYPES:
            if category != "whale":
                self.assertIsNone(
                    settings["channels"][category],
                    f"setting whale must not touch {category}",
                )
        self.assertIn("Whale alerts", interaction.response_text)
        self.assertIn("<#1001>", interaction.response_text)
        self.assertIn("unchanged", interaction.response_text)

    async def test_changing_one_category_never_overwrites_another(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        guild = FakeGuild(111)

        await self._set_channel(
            bot, "set-whale-alerts", CommandInteraction(guild), FakeChannel(1001, 111)
        )
        await self._set_channel(
            bot, "set-price-alerts", CommandInteraction(guild), FakeChannel(1002, 111)
        )
        await self._set_channel(
            bot, "set-news-alerts", CommandInteraction(guild), FakeChannel(1003, 111)
        )
        # Re-run one command with a different channel: others stay put.
        await self._set_channel(
            bot, "set-whale-alerts", CommandInteraction(guild), FakeChannel(1004, 111)
        )

        channels = bot.guild_settings["111"]["channels"]
        self.assertEqual(channels["whale"], 1004)
        self.assertEqual(channels["price"], 1002)
        self.assertEqual(channels["news"], 1003)
        self.assertIsNone(channels["new_market"])

    async def test_two_servers_configure_independently(self):
        bot = await self._build_bot()
        await bot.setup_hook()

        await self._set_channel(
            bot,
            "set-whale-alerts",
            CommandInteraction(FakeGuild(111)),
            FakeChannel(1111, 111),
        )
        await self._set_channel(
            bot,
            "set-whale-alerts",
            CommandInteraction(FakeGuild(222)),
            FakeChannel(2222, 222),
        )

        self.assertEqual(bot.guild_settings["111"]["channels"]["whale"], 1111)
        self.assertEqual(bot.guild_settings["222"]["channels"]["whale"], 2222)

        self._install_channels(
            bot,
            {
                1111: FakeChannel(1111, 111),
                2222: FakeChannel(2222, 222),
            },
        )
        destinations = await bot.get_alert_channels("whale")
        self.assertEqual(set(destinations), {"111", "222"})
        self.assertEqual(destinations["111"].id, 1111)
        self.assertEqual(destinations["222"].id, 2222)

    async def test_non_administrators_are_rejected(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        bot.ensure_settings("111")
        interaction = CommandInteraction(FakeGuild(111), administrator=False)

        await self._set_channel(
            bot, "set-whale-alerts", interaction, FakeChannel(1001, 111)
        )

        self.assertIn("administrators", interaction.response_text)
        self.assertIsNone(bot.guild_settings["111"]["channels"]["whale"])

    async def test_channel_from_another_server_is_rejected(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        bot.ensure_settings("111")
        interaction = CommandInteraction(FakeGuild(111))

        await self._set_channel(
            bot, "set-whale-alerts", interaction, FakeChannel(9999, 222)
        )

        self.assertIn("this server", interaction.response_text)
        self.assertIsNone(bot.guild_settings["111"]["channels"]["whale"])

    async def test_missing_bot_permissions_are_rejected(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        bot.ensure_settings("111")
        interaction = CommandInteraction(FakeGuild(111))
        no_send = FakeChannel(1001, 111, send=False)

        await self._set_channel(bot, "set-whale-alerts", interaction, no_send)
        self.assertIn("permission", interaction.response_text)
        self.assertIsNone(bot.guild_settings["111"]["channels"]["whale"])

        no_embed = FakeChannel(1002, 111, embed=False)
        await self._set_channel(bot, "set-price-alerts", interaction, no_embed)
        self.assertIn("permission", interaction.response_text)
        self.assertIsNone(bot.guild_settings["111"]["channels"]["price"])

    async def test_clearing_a_category_falls_back_to_the_default_channel(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 1001

        interaction = CommandInteraction(FakeGuild(111))
        await self._set_channel(bot, "set-whale-alerts", interaction, None)

        self.assertIsNone(bot.guild_settings["111"]["channels"]["whale"])
        self.assertEqual(bot.guild_settings["111"]["channel_id"], 555)
        self.assertIn("<#555>", interaction.response_text)

    async def test_settings_are_written_to_disk_for_the_right_guild(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        await self._set_channel(
            bot,
            "set-new-markets",
            CommandInteraction(FakeGuild(111)),
            FakeChannel(1001, 111),
        )
        self.assertTrue(main.GUILD_SETTINGS_FILE.exists())
        reloaded = main.load_guild_settings()
        self.assertEqual(reloaded["111"]["channels"]["new_market"], 1001)
        self.assertIsNone(reloaded["111"]["channels"]["news"])


# ---------------------------------------------------------------------------
# Channel routing per category
# ---------------------------------------------------------------------------


class ChannelRoutingTests(ChannelConfigTestCase):
    async def test_categories_resolve_to_their_own_channels(self):
        bot = await self._build_bot()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666
        settings["channels"]["price"] = 777
        self._install_channels(
            bot,
            {
                555: FakeChannel(555, 111),
                666: FakeChannel(666, 111),
                777: FakeChannel(777, 111),
            },
        )

        self.assertEqual((await bot.get_alert_channels("whale"))["111"].id, 666)
        self.assertEqual((await bot.get_alert_channels("price"))["111"].id, 777)
        self.assertEqual((await bot.get_alert_channels("news"))["111"].id, 555)
        self.assertEqual((await bot.get_alert_channels())["111"].id, 555)

    async def test_deleted_channel_is_reported_without_crashing_the_cycle(self):
        bot = await self._build_bot()
        bot.ensure_settings("111")["channels"]["whale"] = 404404
        self._install_channels(bot, {})

        # The raw resolver fails loudly when its only channel is gone ...
        with self.assertRaises(RuntimeError):
            await bot.get_alert_channels("whale")
        # ... while the monitor-level resolver isolates the failure so one
        # deleted channel cannot stop the other alert categories.
        destinations = await bot.get_alert_destinations()
        for category in main.ALERT_TYPES:
            self.assertEqual(destinations[category], {})

    async def test_channel_from_another_guild_is_skipped(self):
        bot = await self._build_bot()
        bot.ensure_settings("111")["channels"]["whale"] = 666
        self._install_channels(bot, {666: FakeChannel(666, 222)})

        destinations = await bot.get_alert_channels("whale")
        self.assertEqual(destinations, {})

    async def test_get_alert_destinations_isolates_a_failing_category(self):
        bot = await self._build_bot()
        bot.ensure_settings("111")["channels"]["whale"] = 666

        async def resolve(channel_id: int):
            raise RuntimeError("gateway unavailable")

        bot.resolve_channel = resolve

        destinations = await bot.get_alert_destinations()
        self.assertEqual(
            set(destinations), set(main.ALERT_TYPES)
        )
        for category in main.ALERT_TYPES:
            self.assertEqual(destinations[category], {})


class MonitorRoutingTests(ChannelConfigTestCase):
    async def test_monitor_cycle_routes_whale_alerts_to_whale_channels(self):
        bot = await self._build_bot()
        # Two servers with only whale channels configured: every other
        # category has no destination, so only the whale subsystem may run.
        bot.ensure_settings("111")["channels"]["whale"] = 666
        bot.ensure_settings("222")["channels"]["whale"] = 777

        channel_666 = FakeChannel(666, 111)
        channel_777 = FakeChannel(777, 222)
        self._install_channels(bot, {666: channel_666, 777: channel_777})

        # Only the trades endpoint is allowed: any other subsystem calling the
        # network here would mean it ran with the wrong destinations.
        requested_urls: list[str] = []

        async def get_json(url, params=None, headers=None, timeout=None):
            requested_urls.append(url)
            return {
                "data": [
                    {
                        "proxy_wallet": "0xabc",
                        "transaction_hash": "0xfeed",
                        "token_id": "123",
                        "side": "BUY",
                        "size": 50_000.0,
                        "price": 0.90,
                        "timestamp": 1791909838,
                        "title": "Test market",
                        "outcome": "Yes",
                        "slug": "test-market",
                    }
                ]
            }

        bot.get_json = get_json

        class FakeSession:
            async def close(self):
                return None

        bot.news_session = FakeSession()
        # Skip the first-run suppression so the fill can alert immediately.
        bot.monitor_state["whale_seeded"] = True

        posts: list[tuple[str, int]] = []

        async def record_send(channel, embed, guild_id):
            posts.append((guild_id, channel.id))
            return True

        bot.send_embed = record_send

        await bot.run_monitor_cycle()

        self.assertIsNone(bot.last_poll_error)
        self.assertEqual(
            requested_urls,
            [main.POLYMARKET_TRADES_URL],
            "only the whale subsystem may run when whale is the only category "
            "with a configured channel",
        )
        self.assertEqual(sorted(posts), [("111", 666), ("222", 777)])

    async def test_news_goes_to_default_while_whale_goes_to_override(self):
        bot = await self._build_bot()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666
        self._install_channels(bot, {555: FakeChannel(555, 111), 666: FakeChannel(666, 111)})

        destinations = await bot.get_alert_destinations()
        self.assertEqual(destinations["news"]["111"].id, 555)
        self.assertEqual(destinations["price"]["111"].id, 555)
        self.assertEqual(destinations["new_market"]["111"].id, 555)
        self.assertEqual(destinations["whale"]["111"].id, 666)


# ---------------------------------------------------------------------------
# Embed colors
# ---------------------------------------------------------------------------


def whale_trade(side: str) -> dict:
    return {
        "proxy_wallet": "0xabc",
        "transaction_hash": "0xfeed",
        "token_id": "123",
        "side": side,
        "size": 50_000.0,
        "price": 0.90,
        "timestamp": 1791909838,
        "title": "Test market",
        "outcome": "Yes",
        "slug": "test-market",
    }


class EmbedColorTests(ChannelConfigTestCase):
    async def test_whale_buy_alert_is_green(self):
        bot = await self._build_bot()
        embed = bot.make_whale_embed(whale_trade("BUY"), 45_000.0)
        self.assertEqual(embed.color, discord.Color.green())
        self.assertEqual(embed.color, main.WHALE_BUY_COLOR)

    async def test_whale_sell_alert_is_red(self):
        bot = await self._build_bot()
        embed = bot.make_whale_embed(whale_trade("SELL"), 45_000.0)
        self.assertEqual(embed.color, discord.Color.red())
        self.assertEqual(embed.color, main.WHALE_SELL_COLOR)

    async def test_new_market_alert_keeps_the_classic_gold(self):
        bot = await self._build_bot()
        embed = bot.make_market_embed(
            {
                "question": "Will it happen?",
                "slug": "will-it-happen",
                "liquidityNum": 1000,
                "volumeNum": 2000,
            }
        )
        self.assertEqual(embed.color, main.SOVEREIGN_COLOR)

    async def test_test_alert_embed_is_classic_gold(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        channel = FakeChannel(555, 111)
        self._install_channels(bot, {555: channel})
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("test")
        await command._do_call(interaction, {})

        self.assertEqual(len(channel.sent_embeds), 1)
        self.assertEqual(channel.sent_embeds[0].color, main.SOVEREIGN_COLOR)
        self.assertIn("Test alert sent", interaction.response_text)


# ---------------------------------------------------------------------------
# /config-check diagnostic
# ---------------------------------------------------------------------------


class ConfigCheckTests(ChannelConfigTestCase):
    async def test_reports_every_category_and_its_channel(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666
        settings["alerts"]["price"] = False
        self._install_channels(
            bot, {555: FakeChannel(555, 111), 666: FakeChannel(666, 111)}
        )
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("config-check")
        await command._do_call(interaction, {})

        text = interaction.followup.sent[-1][1]["content"]
        self.assertIn("<#555>", text)
        self.assertIn("<#666>", text)
        self.assertIn("Whale alerts", text)
        self.assertIn("own channel", text)
        self.assertIn("(disabled)", text)   # the disabled price toggle
        self.assertIn("✅", text)
        self.assertIn("/set-whale-alerts", text)

    async def test_flags_missing_permissions_deleted_and_foreign_channels(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666      # deleted (not resolvable)
        settings["channels"]["price"] = 777      # belongs to another guild
        settings["channels"]["news"] = 888       # bot cannot send
        self._install_channels(
            bot,
            {
                555: FakeChannel(555, 111),
                777: FakeChannel(777, 222),
                888: FakeChannel(888, 111, send=False),
            },
        )
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("config-check")
        await command._do_call(interaction, {})

        text = interaction.followup.sent[-1][1]["content"]
        self.assertIn("was deleted", text)
        self.assertIn("different server", text)
        self.assertIn("send messages", text)
        self.assertRegex(text, r"⚠ \d+ problem\(s\) found")

    async def test_works_with_no_configuration_at_all(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("config-check")
        await command._do_call(interaction, {})

        text = interaction.followup.sent[-1][1]["content"]
        self.assertIn("not configured", text)
        self.assertIn("/setup", text)

    async def test_requires_a_server(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        interaction = CommandInteraction(None)

        command = bot.tree.get_command("config-check")
        await command._do_call(interaction, {})

        self.assertIn("Discord server", interaction.response_text)


# ---------------------------------------------------------------------------
# /test targets a chosen category
# ---------------------------------------------------------------------------


class TestCommandCategoryTests(ChannelConfigTestCase):
    async def test_test_alert_goes_to_the_chosen_category_channel(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666
        default_channel = FakeChannel(555, 111)
        whale_channel = FakeChannel(666, 111)
        self._install_channels(bot, {555: default_channel, 666: whale_channel})
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("test")
        await command._do_call(
            interaction,
            {"category": app_commands.Choice(name="Whale alerts", value="whale")},
        )

        self.assertEqual(len(whale_channel.sent_embeds), 1)
        self.assertEqual(default_channel.sent_embeds, [])
        self.assertIn("<#666>", interaction.response_text)

    async def test_test_without_category_uses_the_default_channel(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        settings = bot.ensure_settings("111")
        settings["channel_id"] = 555
        settings["channels"]["whale"] = 666
        default_channel = FakeChannel(555, 111)
        whale_channel = FakeChannel(666, 111)
        self._install_channels(bot, {555: default_channel, 666: whale_channel})
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("test")
        await command._do_call(interaction, {})

        self.assertEqual(len(default_channel.sent_embeds), 1)
        self.assertEqual(whale_channel.sent_embeds, [])

    async def test_unset_category_reports_not_configured(self):
        bot = await self._build_bot()
        await bot.setup_hook()
        interaction = CommandInteraction(FakeGuild(111))

        command = bot.tree.get_command("test")
        await command._do_call(
            interaction,
            {"category": app_commands.Choice(name="Price alerts", value="price")},
        )

        self.assertIn("price alerts channel", interaction.response_text)


if __name__ == "__main__":
    unittest.main()
