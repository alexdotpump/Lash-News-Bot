"""Tests for the single-registration guarantee behind the duplicate commands.

Discord lists the same command twice when it is published for both install
types, and it keeps per-server registrations from earlier syncs forever. These
tests pin the cleanup that leaves exactly one visible copy of every command.
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

import main  # noqa: E402


class FakeHTTP:
    """Records the application-command calls the bot makes on Discord."""

    def __init__(self, guild_commands: dict[int, list[dict]] | None = None) -> None:
        self.token = None
        self.guild_commands = guild_commands or {}
        self.global_upserts: list[list[dict]] = []
        self.guild_upserts: list[tuple[int, list[dict]]] = []
        self.guild_reads: list[int] = []

    async def close(self):
        return None

    async def bulk_upsert_global_commands(self, application_id, payload):
        self.global_upserts.append(payload)
        return payload

    async def get_guild_commands(self, application_id, guild_id):
        self.guild_reads.append(guild_id)
        return list(self.guild_commands.get(guild_id, []))

    async def bulk_upsert_guild_commands(self, application_id, guild_id, payload):
        self.guild_upserts.append((guild_id, payload))
        return payload


class CommandRegistrationTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _bot(self, *, logged_in: bool, dev_guild_ids=(), guilds=()) -> main.LashNewsBot:
        bot = main.create_bot()

        async def no_sync(guild=None):
            return bot.tree.get_commands()

        async def idle_monitor():
            await asyncio.Event().wait()

        bot.tree.sync = no_sync
        bot.poll_news_forever = idle_monitor
        bot._connection.application_id = 999
        bot.dev_guild_ids = list(dev_guild_ids)
        bot._connection._guilds = {
            guild_id: SimpleNamespace(id=guild_id) for guild_id in guilds
        }
        http = FakeHTTP()
        http.token = "logged-in" if logged_in else None
        bot.http = http
        self.addAsyncCleanup(bot.close)
        await bot.setup_hook()
        return bot

    async def test_nothing_is_rewritten_before_the_bot_is_logged_in(self):
        bot = await self._bot(logged_in=False, guilds=(111,))
        await bot.on_ready()
        self.assertEqual(bot.http.global_upserts, [])
        self.assertEqual(bot.http.guild_upserts, [])
        self.assertEqual(bot.http.guild_reads, [])

    async def test_every_command_is_published_for_server_installation_only(self):
        bot = await self._bot(logged_in=True)
        await bot.on_ready()

        self.assertEqual(len(bot.http.global_upserts), 1)
        payloads = bot.http.global_upserts[0]
        self.assertEqual(len(payloads), len(bot.tree.get_commands()))
        # A command published for both install types is listed twice in the
        # picker, which is exactly the duplicate that was reported.
        for payload in payloads:
            self.assertEqual(payload["integration_types"], [0])

    async def test_leftover_per_server_commands_are_cleared(self):
        bot = await self._bot(logged_in=True, guilds=(111,))
        bot.http.guild_commands = {111: [{"name": "test"}, {"name": "sync"}]}
        await bot.on_ready()

        self.assertEqual(bot.http.guild_reads, [111])
        self.assertEqual(bot.http.guild_upserts, [(111, [])])

    async def test_a_dev_guild_keeps_its_fresh_instant_registration(self):
        bot = await self._bot(logged_in=True, dev_guild_ids=[222], guilds=(111, 222))
        bot.http.guild_commands = {111: [{"name": "test"}], 222: [{"name": "test"}]}
        await bot.on_ready()

        self.assertEqual(bot.http.guild_reads, [111])
        self.assertEqual([guild_id for guild_id, _ in bot.http.guild_upserts], [111])

    async def test_a_server_without_leftovers_is_left_alone(self):
        bot = await self._bot(logged_in=True, guilds=(111,))
        await bot.on_ready()
        self.assertEqual(bot.http.guild_reads, [111])
        self.assertEqual(bot.http.guild_upserts, [])

    async def test_a_reconnect_does_not_rewrite_the_commands_again(self):
        bot = await self._bot(logged_in=True, guilds=(111,))
        await bot.on_ready()
        await bot.on_ready()
        self.assertEqual(len(bot.http.global_upserts), 1)

    async def test_a_failing_cleanup_does_not_stop_startup(self):
        bot = await self._bot(logged_in=True, guilds=(111,))

        async def boom(application_id, payload):
            raise RuntimeError("Discord is unreachable")

        bot.http.bulk_upsert_global_commands = boom
        await bot.on_ready()  # must not raise
        self.assertEqual(bot.http.guild_upserts, [])


if __name__ == "__main__":
    unittest.main()
