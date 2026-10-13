"""Tests for the hardened monitoring loop: retries, guards, and shutdown."""

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import aiohttp  # noqa: E402

import main  # noqa: E402


class FakeResponse:
    def __init__(self, status, payload=None, headers=None, text=""):
        self.status = status
        self._payload = payload
        self.headers = headers or {}
        self._text = text

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        return self._text


class FakeGetContext:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc_info):
        return False


class FakeSession:
    """A stand-in for aiohttp.ClientSession.

    Each queued item is either a FakeResponse or an exception instance; an
    exception entry simulates a dropped connection for that attempt.
    """

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.closed = False

    def get(self, *args, **kwargs):
        self.calls += 1
        if not self._responses:
            raise AssertionError("FakeSession ran out of queued responses")
        item = self._responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return FakeGetContext(item)

    async def close(self):
        self.closed = True


class MonitorTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.bot = main.create_bot()
        self.bot._connection.application_id = 1
        # Record sleep delays instead of waiting so tests stay fast.
        self.sleeps: list[float] = []

        async def fake_sleep(delay):
            self.sleeps.append(delay)

        self._sleep_patcher = mock.patch.object(asyncio, "sleep", fake_sleep)
        self._sleep_patcher.start()

    async def asyncTearDown(self) -> None:
        self._sleep_patcher.stop()
        os.chdir(self._cwd)
        self._tmp.cleanup()

    # ---- get_json -------------------------------------------------------

    async def test_get_json_retries_on_429_then_succeeds(self):
        session = FakeSession([
            FakeResponse(429, headers={"Retry-After": "2"}),
            FakeResponse(200, payload={"ok": True}),
        ])
        self.bot.news_session = session
        result = await self.bot.get_json("https://example.test/x")
        self.assertEqual(result, {"ok": True})
        self.assertEqual(session.calls, 2)
        self.assertEqual(self.sleeps, [2.0])  # honored Retry-After exactly

    async def test_get_json_retries_on_500_then_succeeds(self):
        session = FakeSession([
            FakeResponse(503),
            FakeResponse(200, payload=[1, 2]),
        ])
        self.bot.news_session = session
        result = await self.bot.get_json("https://example.test/retry")
        self.assertEqual(result, [1, 2])
        self.assertEqual(session.calls, 2)

    async def test_get_json_retries_dropped_connection(self):
        session = FakeSession([
            aiohttp.ClientConnectionError("dropped"),
            FakeResponse(200, payload={"ok": 1}),
        ])
        self.bot.news_session = session
        result = await self.bot.get_json("https://example.test/drop")
        self.assertEqual(result, {"ok": 1})
        self.assertEqual(session.calls, 2)

    async def test_get_json_fails_fast_on_client_error_status(self):
        session = FakeSession([FakeResponse(400, text="bad request")])
        self.bot.news_session = session
        with self.assertRaises(RuntimeError) as ctx:
            await self.bot.get_json("https://example.test/z")
        self.assertIn("400", str(ctx.exception))
        self.assertEqual(session.calls, 1)  # no retry
        self.assertEqual(self.sleeps, [])

    async def test_get_json_gives_up_after_max_attempts(self):
        session = FakeSession([
            FakeResponse(500), FakeResponse(500), FakeResponse(500),
        ])
        self.bot.news_session = session
        with self.assertRaises(RuntimeError) as ctx:
            await self.bot.get_json("https://example.test/q")
        self.assertIn("failed after 3 attempts", str(ctx.exception))
        self.assertEqual(session.calls, 3)
        self.assertEqual(len(self.sleeps), 2)  # slept between attempts only

    async def test_parse_retry_after(self):
        parse = main.LashNewsBot.parse_retry_after
        self.assertIsNone(parse(FakeResponse(429)))
        self.assertIsNone(parse(FakeResponse(429, headers={"Retry-After": "soon"})))
        self.assertEqual(parse(FakeResponse(429, headers={"Retry-After": "5"})), 5.0)
        # Capped so a hostile value cannot stall the loop for hours.
        self.assertEqual(
            parse(FakeResponse(429, headers={"Retry-After": "100000"})),
            main.MAX_RETRY_DELAY_SECONDS,
        )

    # ---- loop guards ----------------------------------------------------

    async def test_second_monitor_loop_is_refused(self):
        self.bot._monitor_running = True
        await self.bot.poll_news_forever()
        self.assertIsNone(self.bot.monitor_started_at)
        self.bot._monitor_running = False

    async def test_close_cancels_monitor_and_closes_session(self):
        async def never_ending():
            await asyncio.sleep(3600)

        self.bot.news_task = asyncio.create_task(never_ending())
        session = FakeSession([])
        self.bot.news_session = session
        await self.bot.close()
        self.assertTrue(self.bot.news_task.cancelled())
        self.assertTrue(session.closed)


if __name__ == "__main__":
    unittest.main()
