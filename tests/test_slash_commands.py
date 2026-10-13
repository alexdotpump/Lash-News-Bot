"""Tests for slash-command registration, sync strategy, and error handling."""

import inspect
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

EXPECTED_COMMANDS = {
    "setup", "alerts", "status", "test", "sources", "keywords", "help",
    "servers", "server-info", "sync",
}
# Commands that must be restricted to servers (not usable in DMs).
GUILD_ONLY = {
    "setup", "alerts", "status", "test", "sources", "keywords",
    "sync", "server-info",
}
# Commands that only server administrators may run by default.
ADMIN_ONLY = {"setup", "alerts", "sources", "keywords"}


class FakeResponse:
    def __init__(self) -> None:
        self.done = False
        self.sent: list[tuple[str, dict]] = []

    def is_done(self) -> bool:
        return self.done

    async def send_message(self, content=None, **kwargs):
        self.done = True
        self.sent.append(("send_message", {"content": content, **kwargs}))


class FakeFollowup:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    async def send(self, content=None, **kwargs):
        self.sent.append(("followup", {"content": content, **kwargs}))


class FakeInteraction:
    def __init__(self, responded: bool = False) -> None:
        self.response = FakeResponse()
        self.response.done = responded
        self.followup = FakeFollowup()
        self.command = SimpleNamespace(name="test")
        self.user = SimpleNamespace(id=1)
        self.guild_id = 2


class BoundInteraction(FakeInteraction):
    """Interaction rich enough for a command callback to run to completion."""

    def __init__(self, command_name: str, *, administrator: bool = True) -> None:
        super().__init__()
        self.command = SimpleNamespace(name=command_name)
        self.user = SimpleNamespace(
            id=1,
            guild_permissions=SimpleNamespace(administrator=administrator),
        )
        self.guild = SimpleNamespace(id=2)
        self.guild_id = 2


class BotTestCase(unittest.IsolatedAsyncioTestCase):
    """Base case that isolates state files and stubs out Discord's sync call."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.guild_syncs: list[int] = []
        self.global_syncs: int = 0

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _build_bot(self, **env):
        previous = {k: os.environ.get(k) for k in env}
        for key, value in env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            bot = main.create_bot()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        bot._connection.application_id = 123456789

        async def default_sync(guild=None):
            if guild is None:
                self.global_syncs += 1
            else:
                self.guild_syncs.append(guild.id)
            return bot.tree.get_commands()

        bot.tree.sync = default_sync
        return bot


class CommandRegistrationTests(BotTestCase):
    async def test_all_expected_commands_registered_without_duplicates(self):
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            names = [c.name for c in bot.tree.get_commands()]
            self.assertEqual(len(names), len(set(names)), f"duplicate names: {names}")
            self.assertEqual(set(names), EXPECTED_COMMANDS)
        finally:
            await bot.close()

    async def test_payloads_are_valid_for_discord(self):
        """Discord rejects descriptions >100 chars and empty option descriptions."""
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            for command in bot.tree.get_commands():
                payload = command.to_dict()
                self.assertTrue(payload.get("description"))
                self.assertLessEqual(len(payload["description"]), 100)
                self.assertGreaterEqual(len(payload["name"]), 1)
                for option in payload.get("options", []):
                    desc = option.get("description", "")
                    self.assertTrue(desc, f"{command.name}.{option['name']} empty desc")
                    self.assertLessEqual(len(desc), 100)
        finally:
            await bot.close()

    async def test_guild_only_flags(self):
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            by_name = {c.name: c for c in bot.tree.get_commands()}
            for name in GUILD_ONLY:
                payload = by_name[name].to_dict()
                self.assertFalse(
                    payload.get("dm_permission", True),
                    f"/{name} should not be usable in DMs",
                )
        finally:
            await bot.close()

    async def test_only_admin_commands_are_default_admin_gated(self):
        admin_value = int(discord.Permissions(administrator=True).value)
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            for command in bot.tree.get_commands():
                payload = command.to_dict()
                perms = payload.get("default_member_permissions")
                if command.name in ADMIN_ONLY:
                    self.assertEqual(
                        int(perms), admin_value,
                        f"/{command.name} should default to administrator-only",
                    )
                else:
                    self.assertIsNone(
                        perms, f"/{command.name} unexpectedly admin-gated"
                    )
        finally:
            await bot.close()

    async def test_sync_called_once_globally_and_per_dev_guild(self):
        """on_ready must not sync; setup_hook syncs dev guilds + global exactly once."""
        bot = await self._build_bot(DISCORD_DEV_GUILD_ID="111, 222")
        try:
            self.assertEqual(bot.dev_guild_ids, [111, 222])
            await bot.setup_hook()
            self.assertEqual(self.guild_syncs, [111, 222])
            self.assertEqual(self.global_syncs, 1)
            self.assertIsNotNone(bot.last_sync_at)
            self.assertIsNone(bot.last_sync_error)

            # A reconnect fires on_ready again; it must NOT sync a second time.
            await bot.on_ready()
            self.assertEqual(self.global_syncs, 1)
            self.assertEqual(self.guild_syncs, [111, 222])
        finally:
            await bot.close()

    async def test_no_dev_guild_still_syncs_globally(self):
        bot = await self._build_bot(DISCORD_DEV_GUILD_ID=None)
        try:
            await bot.setup_hook()
            self.assertEqual(self.guild_syncs, [])
            self.assertEqual(self.global_syncs, 1)
        finally:
            await bot.close()

    async def test_sync_failure_is_captured_not_raised(self):
        bot = await self._build_bot()
        try:
            async def failing_sync(guild=None):
                raise discord.HTTPException(
                    SimpleNamespace(status=403, reason="Forbidden"), "nope"
                )

            bot.tree.sync = failing_sync
            await bot.setup_hook()  # must not raise
            self.assertIsNotNone(bot.last_sync_error)
            self.assertIsNone(bot.last_sync_at)
        finally:
            await bot.close()

    async def test_invalid_dev_guild_id_raises_clear_error(self):
        with self.assertRaises(ValueError) as ctx:
            main.parse_guild_ids("abc")
        self.assertIn("DISCORD_DEV_GUILD_ID", str(ctx.exception))


class CommandInvocationTests(BotTestCase):
    """Regression tests for the bug where every slash command answered nothing.

    Callbacks are methods on the Bot subclass. If the command objects are added to
    the tree without their ``binding`` set, discord.py calls the callback *without*
    ``self`` and every interaction dies with ``CommandSignatureMismatch`` -- so the
    command is visible in Discord but replies "the application did not respond".
    These tests drive the real invocation entry point and would fail without the
    binding step in ``register_commands``.
    """

    async def test_every_command_is_bound_to_the_bot(self):
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            commands = bot.tree.get_commands()
            self.assertEqual(len(commands), len(EXPECTED_COMMANDS))
            for command in commands:
                self.assertIs(
                    command.binding,
                    bot,
                    f"/{command.name} has no binding, so its callback would be "
                    f"called without self and never respond",
                )
        finally:
            await bot.close()

    async def test_parameterless_command_invokes_and_responds(self):
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            command = bot.tree.get_command("help")
            interaction = BoundInteraction("help")
            await command._do_call(interaction, {})
            self.assertEqual(len(interaction.response.sent), 1)
            self.assertIn("commands", interaction.response.sent[0][1]["content"])
        finally:
            await bot.close()

    async def test_command_with_required_option_invokes_and_responds(self):
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            command = bot.tree.get_command("keywords")
            # Discord always supplies required options, which the tree transforms
            # into a Choice before _do_call runs.
            choice = app_commands.Choice(name="List", value="list")
            interaction = BoundInteraction("keywords")
            await command._do_call(interaction, {"action": choice})
            self.assertEqual(len(interaction.response.sent), 1)
            self.assertTrue(interaction.response.sent[0][1].get("ephemeral"))
        finally:
            await bot.close()

    async def test_no_command_callback_is_left_expecting_self(self):
        """Any callback that takes ``self`` must have a binding, or it is dropped."""
        bot = await self._build_bot()
        try:
            await bot.setup_hook()
            for command in bot.tree.get_commands():
                parameters = list(inspect.signature(command._callback).parameters)
                if parameters and parameters[0] == "self":
                    self.assertIsNotNone(
                        command.binding,
                        f"/{command.name} takes self but has no binding, so "
                        f"discord.py would call it without self",
                    )
        finally:
            await bot.close()


class ErrorHandlerTests(BotTestCase):
    async def _make_bot(self):
        return await self._build_bot()

    async def test_responds_when_interaction_not_yet_answered(self):
        bot = await self._make_bot()
        try:
            interaction = FakeInteraction(responded=False)
            await bot.on_app_command_error(
                interaction, app_commands.AppCommandError("boom")
            )
            self.assertEqual(len(interaction.response.sent), 1)
            self.assertTrue(interaction.response.sent[0][1].get("ephemeral"))
            self.assertEqual(interaction.followup.sent, [])
        finally:
            await bot.close()

    async def test_uses_followup_when_already_answered(self):
        bot = await self._make_bot()
        try:
            interaction = FakeInteraction(responded=True)
            await bot.on_app_command_error(
                interaction, app_commands.AppCommandError("boom")
            )
            self.assertEqual(interaction.response.sent, [])
            self.assertEqual(len(interaction.followup.sent), 1)
            self.assertTrue(interaction.followup.sent[0][1].get("ephemeral"))
        finally:
            await bot.close()

    async def test_missing_permissions_message(self):
        bot = await self._make_bot()
        try:
            interaction = FakeInteraction()
            error = app_commands.MissingPermissions(["administrator"])
            await bot.on_app_command_error(interaction, error)
            content = interaction.response.sent[0][1]["content"]
            self.assertIn("permission", content.lower())
        finally:
            await bot.close()


if __name__ == "__main__":
    unittest.main()
