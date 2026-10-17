"""Tests for the X/Twitter tracker: watchlists, filtering, and delivery.

The official X API is mocked at the bot-method boundary (``fetch_x_user`` /
``fetch_x_posts``) so these tests verify the tracker's own behavior --
resolution, baseline, dedupe, filtering, per-server isolation, and delivery --
without network access. Live X API calls are not exercised here; end-to-end
delivery needs an authorized X_API_BEARER_TOKEN and is reported separately.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(TESTS_DIR))

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import main  # noqa: E402
from test_monitor import FakeResponse, FakeSession  # noqa: E402


def post(
    post_id: str,
    text: str = "Hello world",
    *,
    referenced=None,
    created_at: str = "2026-10-16T12:00:00Z",
    metrics: dict | None = None,
    media_keys: list | None = None,
) -> dict:
    """A trimmed v2 post object shaped like the live API response."""
    payload = {
        "id": post_id,
        "text": text,
        "author_id": "42",
        "created_at": created_at,
        "public_metrics": metrics
        or {
            "like_count": 3,
            "retweet_count": 1,
            "reply_count": 0,
            "quote_count": 0,
            "impression_count": 100,
        },
    }
    if referenced:
        payload["referenced_tweets"] = referenced
    if media_keys:
        payload["attachments"] = {"media_keys": media_keys}
    return payload


def user(user_id: str = "42", username: str = "testhandle", **extra) -> dict:
    payload = {
        "id": user_id,
        "name": "Test Handle",
        "username": username,
        "profile_image_url": "https://pbs.example/img.png",
        "protected": False,
    }
    payload.update(extra)
    return payload


class FakeChannel:
    """A channel stand-in that records the embeds posted to it."""

    def __init__(self, channel_id: int = 555, guild_id: int = 111) -> None:
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.guild = SimpleNamespace(id=guild_id)
        self.sent: list[dict] = []
        self.fail = False

    async def send(self, *, embed=None, allowed_mentions=None, content=None):
        if self.fail:
            raise main.discord.HTTPException(
                SimpleNamespace(status=403, reason="Forbidden"), "no perms"
            )
        self.sent.append({"embed": embed, "content": content})


class FakeInteraction:
    """Rich enough for the /xtrack callbacks to run to completion."""

    def __init__(self, guild_id: int = 111, administrator: bool = True) -> None:
        owner = self

        class _Response:
            def __init__(self) -> None:
                self.done = False
                self.sent: list[tuple] = []

            def is_done(self) -> bool:
                return self.done

            async def send_message(self, content=None, **kwargs):
                self.done = True
                self.sent.append((content, kwargs))

            async def defer(self, *, ephemeral=False, thinking=False):
                self.done = True

        class _Followup:
            def __init__(self) -> None:
                self.sent: list[tuple] = []

            async def send(self, content=None, **kwargs):
                self.sent.append((content, kwargs))

        self.response = _Response()
        self.followup = _Followup()
        self.user = SimpleNamespace(
            id=1,
            guild_permissions=SimpleNamespace(administrator=administrator),
        )
        self.guild = SimpleNamespace(id=guild_id)
        self.guild_id = guild_id
        self.command = SimpleNamespace(name="xtrack")

    def reply_text(self) -> str:
        """Whatever the command told the user, from response or followup."""
        if self.response.sent:
            return self.response.sent[0][0] or ""
        if self.followup.sent:
            return self.followup.sent[0][0] or ""
        return ""


class XTrackerTestCase(unittest.IsolatedAsyncioTestCase):
    """Base case: isolated state dir, bearer token set, stubbed network."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.bot = main.create_bot()
        self.bot._connection.application_id = 1
        # Tests configure the token directly; env wiring is covered separately.
        self.bot.x_api_bearer = "test-bearer-token"
        self.bot.news_session = FakeSession([])

    async def asyncTearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def track(self, guild_id: str, account: dict) -> None:
        self.bot.set_x_account(guild_id, account)

    def channel_for(self, guild_id: str, channel: FakeChannel | None) -> None:
        async def fake_channels(alert=None):
            assert alert == "x"
            return {guild_id: channel} if channel is not None else {}

        self.bot.get_alert_channels = fake_channels

    def serve_posts(self, mapping) -> list[str]:
        """Map user_id -> posts or (posts, includes); record fetch calls."""
        calls: list[str] = []

        async def fake_posts(user_id: str, since_id: str = ""):
            calls.append(user_id)
            payload = mapping.get(user_id, [])
            if isinstance(payload, tuple):
                posts, includes = payload
            else:
                posts, includes = payload, {}
            if since_id:
                posts = [
                    item for item in posts if main.x_post_is_newer(item["id"], since_id)
                ]
            return posts, includes

        self.bot.fetch_x_posts = fake_posts
        return calls


class UsernameNormalizationTests(unittest.TestCase):
    def test_accepts_plain_at_and_url_forms(self):
        cases = {
            "elonmusk": "elonmusk",
            "@elonmusk": "elonmusk",
            " https://x.com/elonmusk ": "elonmusk",
            "https://x.com/elonmusk/": "elonmusk",
            "http://www.twitter.com/aBc_123": "abc_123",
            "x.com/OneChar": "onechar",
            "@a": "a",
        }
        for raw, expected in cases.items():
            self.assertEqual(main.normalize_x_username(raw), expected, raw)

    def test_rejects_invalid_handles(self):
        for raw in (
            "",
            "   ",
            "@",
            "a b",
            "this_handle_is_way_too_long",
            "handle!",
            "1234567890123456",  # 16 characters
            None,
            123,
            "@@double",
            "https://x.com/search?q=abc",
        ):
            self.assertIsNone(main.normalize_x_username(raw), raw)


class PostClassificationTests(unittest.TestCase):
    def test_activity_types(self):
        self.assertEqual(main.x_post_activity({"id": "1"}), "post")
        self.assertEqual(
            main.x_post_activity(
                {"referenced_tweets": [{"type": "replied_to", "id": "1"}]}
            ),
            "reply",
        )
        self.assertEqual(
            main.x_post_activity(
                {"referenced_tweets": [{"type": "quoted", "id": "1"}]}
            ),
            "quote",
        )
        self.assertEqual(
            main.x_post_activity(
                {"referenced_tweets": [{"type": "retweeted", "id": "1"}]}
            ),
            "repost",
        )
        # A repost beats a quote when both appear.
        self.assertEqual(
            main.x_post_activity(
                {
                    "referenced_tweets": [
                        {"type": "quoted", "id": "1"},
                        {"type": "retweeted", "id": "2"},
                    ]
                }
            ),
            "repost",
        )

    def test_snowflake_comparison_is_numeric_not_lexicographic(self):
        self.assertTrue(main.x_post_is_newer("100", "99"))
        self.assertFalse(main.x_post_is_newer("9", "10"))
        self.assertTrue(main.x_post_is_newer("10", ""))
        self.assertFalse(main.x_post_is_newer("abc", "10"))
        self.assertFalse(main.x_post_is_newer(None, "10"))


class FilterTests(unittest.TestCase):
    def account(self, **kwargs):
        account = main.default_x_account("tester")
        account.update(kwargs)
        return account

    def test_activity_filters(self):
        reply = post("1", referenced=[{"type": "replied_to", "id": "0"}])
        activity = main.x_post_activity(reply)
        self.assertTrue(main.x_post_passes_filters(self.account(), reply, activity))
        self.assertFalse(
            main.x_post_passes_filters(
                self.account(activity_filter="originals"), reply, activity
            )
        )
        self.assertFalse(
            main.x_post_passes_filters(
                self.account(activity_filter="no_replies"), reply, activity
            )
        )
        quote = post("2", referenced=[{"type": "quoted", "id": "0"}])
        quote_activity = main.x_post_activity(quote)
        self.assertTrue(
            main.x_post_passes_filters(
                self.account(activity_filter="no_replies"), quote, quote_activity
            )
        )

    def test_include_keywords_are_case_insensitive_substrings(self):
        account = self.account(include_keywords=["btc", "$eth"])
        self.assertTrue(
            main.x_post_passes_filters(account, post("1", "BTC to the moon"), "post")
        )
        self.assertTrue(
            main.x_post_passes_filters(account, post("2", "watching $ETH"), "post")
        )
        self.assertFalse(
            main.x_post_passes_filters(account, post("3", "ethereum moves"), "post")
        )

    def test_exclude_wins_over_include(self):
        account = self.account(include_keywords=["btc"], exclude_keywords=["giveaway"])
        self.assertFalse(
            main.x_post_passes_filters(
                account, post("1", "BTC giveaway incoming"), "post"
            )
        )
        self.assertTrue(
            main.x_post_passes_filters(account, post("2", "BTC breakout"), "post")
        )


class StateNormalizationTests(unittest.TestCase):
    def test_corrupt_entries_are_dropped_not_fatal(self):
        state = main.normalize_x_state(
            {
                "111": {
                    "accounts": {
                        "good": {"user_id": "42", "since_id": "100"},
                        "Bad Handle!": {},
                        "nofilter": {"activity_filter": "bogus"},
                    }
                },
                "not-a-guild": {"accounts": {"x": {}}},
                "222": "garbage",
            }
        )
        self.assertEqual(set(state), {"111"})
        accounts = state["111"]["accounts"]
        self.assertEqual(set(accounts), {"good", "nofilter"})
        self.assertEqual(accounts["good"]["user_id"], "42")
        self.assertEqual(accounts["good"]["since_id"], "100")
        # A bogus filter falls back to the default rather than being stored.
        self.assertEqual(accounts["nofilter"]["activity_filter"], "all")

    def test_state_round_trips_through_the_file(self):
        bot = main.create_bot()
        account = main.default_x_account("roundtrip")
        account["user_id"] = "77"
        account["include_keywords"] = ["alpha"]
        bot.set_x_account("111", account)
        reloaded = main.load_x_state()
        restored = reloaded["111"]["accounts"]["roundtrip"]
        self.assertEqual(restored["user_id"], "77")
        self.assertEqual(restored["include_keywords"], ["alpha"])

    def test_seen_list_is_bounded_and_numeric(self):
        account = main.normalize_x_account(
            "tester",
            {"seen": [str(i) for i in range(500)] + ["not-a-number", None]},
        )
        self.assertLessEqual(len(account["seen"]), main.MAX_X_SEEN_POSTS)
        self.assertTrue(all(value.isdigit() for value in account["seen"]))


class ApiErrorMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bot = main.create_bot()

    def test_statuses_map_to_actionable_messages(self):
        cases = {
            404: ("not found", False),
            401: ("X_API_BEARER_TOKEN", False),
            403: ("protected", False),
            429: ("rate-limited", True),
            503: ("temporary", True),
        }
        for status, (needle, transient) in cases.items():
            exc = RuntimeError(f"https://api.x.com/x returned HTTP {status}: body")
            mapped = self.bot.x_api_error_from(exc, "somebody")
            self.assertEqual(mapped.status, status)
            self.assertIn(needle.casefold(), str(mapped).casefold())
            self.assertEqual(mapped.transient, transient)

    def test_resource_not_found_body_is_permanent(self):
        # X sometimes answers a missing user with HTTP 400 and a
        # "Not Found Error" body; that must not be retried forever.
        exc = RuntimeError(
            'https://api.x.com/2/users/by/username/ghost returned HTTP 400: '
            '{"title":"Not Found Error","type":".../resource-not-found"}'
        )
        mapped = self.bot.x_api_error_from(exc, "ghost")
        self.assertFalse(mapped.transient)
        self.assertIn("not found", str(mapped).lower())

    def test_unknown_failures_are_transient(self):
        mapped = self.bot.x_api_error_from(RuntimeError("connection reset"), "u")
        self.assertIsNone(mapped.status)
        self.assertTrue(mapped.transient)


class FetchTests(unittest.IsolatedAsyncioTestCase):
    """fetch_x_user / fetch_x_posts against the shared fake session."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.bot = main.create_bot()
        self.bot.x_api_bearer = "token"

    async def asyncTearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def use_session(self, *responses) -> FakeSession:
        session = FakeSession(list(responses))
        self.bot.news_session = session
        return session

    async def test_user_lookup_success(self):
        self.use_session(FakeResponse(200, payload={"data": user()}))
        found = await self.bot.fetch_x_user("testhandle")
        self.assertEqual(found["id"], "42")

    async def test_user_lookup_not_found_is_permanent(self):
        self.use_session(FakeResponse(404, text="{}"))
        with self.assertRaises(main.XApiError) as ctx:
            await self.bot.fetch_x_user("ghost")
        self.assertIn("not found", str(ctx.exception).lower())
        self.assertFalse(ctx.exception.transient)

    async def test_rejected_token_names_the_variable(self):
        self.use_session(FakeResponse(401, text="{}"))
        with self.assertRaises(main.XApiError) as ctx:
            await self.bot.fetch_x_user("anybody")
        self.assertIn("X_API_BEARER_TOKEN", str(ctx.exception))
        self.assertFalse(ctx.exception.transient)

    async def test_timeline_parses_posts_and_includes(self):
        self.use_session(
            FakeResponse(
                200,
                payload={
                    "data": [post("1"), post("2")],
                    "includes": {"users": [user()], "tweets": [], "media": []},
                },
            )
        )
        posts, includes = await self.bot.fetch_x_posts("42", since_id="0")
        self.assertEqual([item["id"] for item in posts], ["1", "2"])
        self.assertIn("users", includes)

    async def test_empty_timeline_returns_no_posts(self):
        self.use_session(FakeResponse(200, payload={"meta": {"result_count": 0}}))
        posts, includes = await self.bot.fetch_x_posts("42")
        self.assertEqual(posts, [])
        self.assertEqual(includes, {})

    async def test_rate_limit_after_retries_is_reported(self):
        self.use_session(
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(429, headers={"Retry-After": "0"}),
        )
        with self.assertRaises(main.XApiError) as ctx:
            await self.bot.fetch_x_posts("42")
        self.assertEqual(ctx.exception.status, 429)
        self.assertTrue(ctx.exception.transient)


class CycleTests(XTrackerTestCase):
    async def test_missing_token_reports_actionable_error_without_calls(self):
        self.bot.x_api_bearer = ""

        async def boom(alert=None):
            raise AssertionError("no channel work should happen without a token")

        self.bot.get_alert_channels = boom
        await self.bot.run_x_cycle()
        self.assertIn("X_API_BEARER_TOKEN", self.bot.last_x_error)

    async def test_first_poll_records_baseline_without_posting(self):
        account = main.default_x_account("quiet")
        account["user_id"] = "42"
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        self.serve_posts({"42": [post("100", "old news")]})

        await self.bot.run_x_cycle()
        self.assertEqual(channel.sent, [])
        self.assertEqual(self.bot.get_x_account("111", "quiet")["since_id"], "100")

    async def test_new_post_is_delivered_with_link_author_and_metrics(self):
        account = main.default_x_account("loud")
        account["user_id"] = "42"
        account["since_id"] = "100"
        account["display_name"] = "Test Handle"
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        self.serve_posts(
            {
                "42": (
                    [
                        post(
                            "200",
                            "BTC breaks $100k",
                            metrics={"like_count": 10, "retweet_count": 2},
                        )
                    ],
                    {},
                )
            }
        )

        await self.bot.run_x_cycle()
        self.assertEqual(len(channel.sent), 1)
        embed = channel.sent[0]["embed"]
        self.assertEqual(embed.title, "New post \u00b7 @loud")
        self.assertEqual(embed.description, "BTC breaks $100k")
        self.assertIn("/loud/status/200", embed.url)
        self.assertIn("Test Handle", embed.author.name)
        self.assertIn("10 likes", embed.footer.text)
        self.assertEqual(self.bot.get_x_account("111", "loud")["since_id"], "200")
        self.assertEqual(self.bot.x_alerts_sent, 1)

    async def test_same_post_is_not_delivered_after_a_restart(self):
        account = main.default_x_account("restarted")
        account["user_id"] = "42"
        account["since_id"] = "100"
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        fetches = self.serve_posts({"42": [post("200", "once only")]})

        await self.bot.run_x_cycle()
        self.assertEqual(len(channel.sent), 1)

        # A restart reloads the watchlist from disk into a fresh bot; the
        # same post must not be delivered again.
        restarted = main.create_bot()
        restarted.x_api_bearer = "token"
        restarted.news_session = FakeSession([])

        async def still_one(alert=None):
            return {"111": channel}

        restarted.get_alert_channels = still_one
        restarted.fetch_x_posts = self.bot.fetch_x_posts
        await restarted.run_x_cycle()
        self.assertEqual(len(channel.sent), 1)
        self.assertEqual(fetches, ["42", "42"])  # it polled, but delivered nothing

    async def test_older_posts_beyond_since_id_are_ignored(self):
        account = main.default_x_account("old")
        account["user_id"] = "42"
        account["since_id"] = "500"
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        # The provider filters by since_id; this simulates a misbehaving
        # response that also returns an old post.
        self.serve_posts({"42": [post("200", "stale"), post("600", "fresh")]})

        await self.bot.run_x_cycle()
        self.assertEqual(
            [sent["embed"].description for sent in channel.sent], ["fresh"]
        )

    async def test_paused_account_is_not_even_polled(self):
        account = main.default_x_account("napping")
        account["user_id"] = "42"
        account["since_id"] = "100"
        account["paused"] = True
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        calls = self.serve_posts({"42": [post("200")]})

        await self.bot.run_x_cycle()
        self.assertEqual(channel.sent, [])
        self.assertEqual(calls, [])

    async def test_resume_delivers_new_posts_again(self):
        account = main.default_x_account("back")
        account["user_id"] = "42"
        account["since_id"] = "100"
        account["paused"] = True
        self.track("111", account)
        account["paused"] = False
        channel = FakeChannel()
        self.channel_for("111", channel)
        self.serve_posts({"42": [post("200", "hello again")]})

        await self.bot.run_x_cycle()
        self.assertEqual(len(channel.sent), 1)

    async def test_removed_account_is_not_polled(self):
        account = main.default_x_account("gone")
        account["user_id"] = "42"
        self.track("111", account)
        self.bot.drop_x_account("111", "gone")
        self.bot.prune_empty_guild_x_accounts("111")
        channel = FakeChannel()
        self.channel_for("111", channel)
        calls = self.serve_posts({"42": [post("200")]})

        await self.bot.run_x_cycle()
        self.assertEqual(calls, [])
        self.assertEqual(channel.sent, [])

    async def test_keyword_filters_apply_per_account(self):
        account = main.default_x_account("picky")
        account["user_id"] = "42"
        account["since_id"] = "100"
        account["include_keywords"] = ["btc"]
        account["exclude_keywords"] = ["giveaway"]
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        self.serve_posts(
            {
                "42": [
                    post("200", "ETH merge"),
                    post("201", "BTC giveaway"),
                    post("202", "BTC breakout!"),
                ]
            }
        )

        await self.bot.run_x_cycle()
        self.assertEqual(
            [sent["embed"].description for sent in channel.sent], ["BTC breakout!"]
        )
        # Filtered posts are remembered so re-filtering never replays them.
        stored = self.bot.get_x_account("111", "picky")
        self.assertEqual(set(stored["seen"]), {"200", "201", "202"})

    async def test_guilds_are_isolated_and_share_one_fetch(self):
        account_a = main.default_x_account("shared")
        account_a["user_id"] = "42"
        account_a["since_id"] = "100"
        account_b = main.default_x_account("shared")
        account_b["user_id"] = "42"
        account_b["since_id"] = "100"
        account_b["include_keywords"] = ["polymarket"]
        self.track("111", account_a)
        self.track("222", account_b)
        channel_a = FakeChannel(555, guild_id=111)
        channel_b = FakeChannel(777, guild_id=222)

        async def two_guilds(alert=None):
            return {"111": channel_a, "222": channel_b}

        self.bot.get_alert_channels = two_guilds
        calls = self.serve_posts({"42": [post("200", "Polymarket listing soon")]})

        await self.bot.run_x_cycle()
        # One fetch for the shared account...
        self.assertEqual(calls, ["42"])
        # ...but each server applies its own filters and channel.
        self.assertEqual(len(channel_a.sent), 1)
        self.assertEqual(len(channel_b.sent), 1)
        self.assertEqual(self.bot.get_x_account("111", "shared")["since_id"], "200")
        self.assertEqual(self.bot.get_x_account("222", "shared")["since_id"], "200")

    async def test_guild_with_disabled_alerts_is_skipped(self):
        account = main.default_x_account("muted")
        account["user_id"] = "42"
        account["since_id"] = "100"
        self.bot.ensure_settings("111")["alerts"]["x"] = False
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        calls = self.serve_posts({"42": [post("200")]})

        await self.bot.run_x_cycle()
        self.assertEqual(calls, [])
        self.assertEqual(channel.sent, [])

    async def test_failed_delivery_does_not_advance_the_high_water_mark(self):
        account = main.default_x_account("flaky")
        account["user_id"] = "42"
        account["since_id"] = "100"
        self.track("111", account)
        channel = FakeChannel()
        channel.fail = True
        self.channel_for("111", channel)
        self.serve_posts({"42": [post("200", "important")]})

        await self.bot.run_x_cycle()
        # The post could not be delivered, so the next cycle must retry it.
        self.assertEqual(self.bot.get_x_account("111", "flaky")["since_id"], "100")

    async def test_transient_lookup_failure_marks_the_account(self):
        account = main.default_x_account("wobbly")
        account["user_id"] = "42"
        self.track("111", account)
        self.channel_for("111", FakeChannel())

        async def failing(user_id, since_id=""):
            raise main.XApiError(429, "rate-limited; retry next cycle")

        self.bot.fetch_x_posts = failing
        await self.bot.run_x_cycle()
        self.assertIn(
            "rate-limited", self.bot.get_x_account("111", "wobbly")["last_error"]
        )
        self.assertIn("1 of 1", self.bot.last_x_error)

    async def test_unresolved_handle_is_resolved_by_the_cycle(self):
        account = main.default_x_account("fresh")
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)

        async def lookup(username):
            return user("42", username)

        async def no_posts(user_id, since_id=""):
            return [], {}

        self.bot.fetch_x_user = lookup
        self.bot.fetch_x_posts = no_posts
        await self.bot.run_x_cycle()
        stored = self.bot.get_x_account("111", "fresh")
        self.assertEqual(stored["user_id"], "42")
        self.assertEqual(stored["display_name"], "Test Handle")

    async def test_per_account_channel_override_wins(self):
        account = main.default_x_account("routed")
        account["user_id"] = "42"
        account["since_id"] = "100"
        account["channel_id"] = 999
        self.track("111", account)
        default_channel = FakeChannel(555, guild_id=111)
        override_channel = FakeChannel(999, guild_id=111)
        self.channel_for("111", default_channel)

        async def resolve(channel_id):
            return override_channel

        self.bot.resolve_channel = resolve
        self.serve_posts({"42": [post("200", "routed post")]})

        await self.bot.run_x_cycle()
        self.assertEqual(default_channel.sent, [])
        self.assertEqual(len(override_channel.sent), 1)

    async def test_override_channel_works_without_a_guild_x_channel(self):
        account = main.default_x_account("solo")
        account["user_id"] = "42"
        account["since_id"] = "100"
        account["channel_id"] = 999
        self.track("111", account)

        async def no_guild_channels(alert=None):
            return {}

        override_channel = FakeChannel(999, guild_id=111)

        async def resolve(channel_id):
            return override_channel

        self.bot.get_alert_channels = no_guild_channels
        self.bot.resolve_channel = resolve
        self.serve_posts({"42": [post("200", "still delivered")]})

        await self.bot.run_x_cycle()
        self.assertEqual(len(override_channel.sent), 1)

    async def test_reply_quote_and_repost_labels(self):
        account = main.default_x_account("reference")
        account["user_id"] = "42"
        account["since_id"] = "100"
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        includes = {
            "tweets": [post("1", "original text")],
            "users": [user("42", "origauthor")],
            "media": [],
        }
        self.serve_posts(
            {
                "42": (
                    [
                        post(
                            "200",
                            "check this out",
                            referenced=[{"type": "quoted", "id": "1"}],
                        ),
                        post(
                            "201",
                            "agreed",
                            referenced=[{"type": "replied_to", "id": "1"}],
                        ),
                        post("202", "", referenced=[{"type": "retweeted", "id": "1"}]),
                    ],
                    includes,
                )
            }
        )

        await self.bot.run_x_cycle()
        titles = [sent["embed"].title for sent in channel.sent]
        self.assertEqual(
            titles,
            [
                "Repost \u00b7 @reference",
                "Reply \u00b7 @reference",
                "Quote post \u00b7 @reference",
            ],
        )
        quote_embed = channel.sent[2]["embed"]
        fields = {field.name: field.value for field in quote_embed.fields}
        self.assertIn("Quoted post", fields)
        self.assertIn("@origauthor", fields["Quoted post"])

    async def test_no_accounts_means_a_clean_cycle(self):
        self.channel_for("111", FakeChannel())
        await self.bot.run_x_cycle()
        self.assertIsNone(self.bot.last_x_error)
        self.assertIsNotNone(self.bot.last_x_check_at)

    async def test_burst_is_capped_per_guild(self):
        account = main.default_x_account("busy")
        account["user_id"] = "42"
        account["since_id"] = "100"
        self.track("111", account)
        channel = FakeChannel()
        self.channel_for("111", channel)
        burst = [
            post(str(200 + index), f"post {index}")
            for index in range(main.MAX_X_ALERTS_PER_CYCLE + 5)
        ]
        self.serve_posts({"42": burst})

        await self.bot.run_x_cycle()
        self.assertLessEqual(
            len(channel.sent), main.MAX_X_ALERTS_PER_CYCLE
        )


class CommandTests(XTrackerTestCase):
    """Drive the /xtrack subcommands through the real invocation path."""

    def setUp(self) -> None:
        super().setUp()
        # The bot attaches its group in setup_hook; build one directly so
        # these tests exercise the same bound commands without gateway sync.
        self.group = main.XCommands(self.bot)

    async def call(self, name: str, interaction, **params):
        command = self.group.get_command(name)
        await command._do_call(interaction, params)

    async def test_add_requires_a_token(self):
        self.bot.x_api_bearer = ""
        interaction = FakeInteraction()
        await self.call("add", interaction, username="somehandle")
        self.assertIn("X_API_BEARER_TOKEN", interaction.reply_text())

    async def test_add_rejects_invalid_handles(self):
        interaction = FakeInteraction()
        await self.call("add", interaction, username="not a handle!")
        self.assertIn("not an X handle", interaction.reply_text())

    async def test_add_resolves_and_stores(self):
        async def lookup(username):
            return user("42", username)

        self.bot.fetch_x_user = lookup
        interaction = FakeInteraction()
        await self.call(
            "add", interaction, username="@TestHandle", label="Alpha feed"
        )
        account = self.bot.get_x_account(111, "testhandle")
        self.assertIsNotNone(account)
        self.assertEqual(account["user_id"], "42")
        self.assertEqual(account["label"], "Alpha feed")
        self.assertIn("@testhandle", interaction.reply_text())
        self.assertIn("baseline", interaction.reply_text())

    async def test_add_still_tracks_on_a_transient_lookup_failure(self):
        async def lookup(username):
            raise main.XApiError(429, "rate-limited")

        self.bot.fetch_x_user = lookup
        interaction = FakeInteraction()
        await self.call("add", interaction, username="later")
        account = self.bot.get_x_account(111, "later")
        self.assertIsNotNone(account)
        self.assertEqual(account["user_id"], "")  # resolved on a later cycle
        self.assertIn("next cycle", interaction.reply_text())

    async def test_add_reports_a_missing_account(self):
        async def lookup(username):
            raise main.XApiError(
                404, "@ghost was not found on X.", transient=False
            )

        self.bot.fetch_x_user = lookup
        interaction = FakeInteraction()
        await self.call("add", interaction, username="ghost")
        self.assertIsNone(self.bot.get_x_account(111, "ghost"))
        self.assertIn("not found", interaction.reply_text())

    async def test_add_refuses_a_duplicate(self):
        self.track("111", main.default_x_account("dup"))
        interaction = FakeInteraction()
        await self.call("add", interaction, username="dup")
        self.assertIn("already tracked", interaction.reply_text())

    async def test_add_enforces_the_per_server_limit(self):
        for index in range(main.MAX_TRACKED_X_ACCOUNTS_PER_GUILD):
            self.track("111", main.default_x_account(f"acc{index}"))
        interaction = FakeInteraction()
        await self.call("add", interaction, username="one_more")
        self.assertIn("maximum", interaction.reply_text())

    async def test_pause_resume_remove_round_trip(self):
        self.track("111", main.default_x_account("toggle"))

        interaction = FakeInteraction()
        await self.call("pause", interaction, username="toggle")
        self.assertTrue(self.bot.get_x_account(111, "toggle")["paused"])

        interaction = FakeInteraction()
        await self.call("resume", interaction, username="toggle")
        self.assertFalse(self.bot.get_x_account(111, "toggle")["paused"])

        interaction = FakeInteraction()
        await self.call("remove", interaction, username="toggle")
        self.assertIsNone(self.bot.get_x_account(111, "toggle"))

    async def test_rename_changes_only_the_label(self):
        account = main.default_x_account("nick")
        account["user_id"] = "42"
        self.track("111", account)
        interaction = FakeInteraction()
        await self.call("rename", interaction, username="nick", label="Mr. X")
        stored = self.bot.get_x_account(111, "nick")
        self.assertEqual(stored["label"], "Mr. X")
        self.assertEqual(stored["user_id"], "42")

    async def test_settings_updates_and_clears_filters(self):
        self.track("111", main.default_x_account("config"))
        interaction = FakeInteraction()
        await self.call(
            "settings",
            interaction,
            username="config",
            keywords="btc, sol",
            exclude="giveaway",
        )
        stored = self.bot.get_x_account(111, "config")
        self.assertEqual(stored["include_keywords"], ["btc", "sol"])
        self.assertEqual(stored["exclude_keywords"], ["giveaway"])

        interaction = FakeInteraction()
        await self.call(
            "settings", interaction, username="config", keywords="clear"
        )
        self.assertEqual(
            self.bot.get_x_account(111, "config")["include_keywords"], []
        )

    async def test_list_reports_the_watchlist(self):
        self.track("111", main.default_x_account("visible"))
        interaction = FakeInteraction()
        await self.call("list", interaction)
        self.assertIn("@visible", interaction.reply_text())

    async def test_info_reports_account_details(self):
        account = main.default_x_account("detailed")
        account["user_id"] = "42"
        account["label"] = "The feed"
        self.track("111", account)
        interaction = FakeInteraction()
        await self.call("info", interaction, username="detailed")
        text = interaction.reply_text()
        self.assertIn("@detailed", text)
        self.assertIn("The feed", text)
        self.assertIn("42", text)

    async def test_status_reports_token_and_health(self):
        interaction = FakeInteraction()
        await self.call("status", interaction)
        text = interaction.reply_text()
        self.assertIn("configured", text)
        self.assertIn("polling", text.lower())

    async def test_status_names_the_missing_variable_without_a_token(self):
        self.bot.x_api_bearer = ""
        interaction = FakeInteraction()
        await self.call("status", interaction)
        self.assertIn("X_API_BEARER_TOKEN", interaction.reply_text())

    async def test_non_admins_are_rejected(self):
        interaction = FakeInteraction(administrator=False)
        await self.call("add", interaction, username="somehandle")
        self.assertIn("administrator", interaction.reply_text().lower())

    async def test_guilds_cannot_see_each_others_watchlists(self):
        self.track("111", main.default_x_account("mine"))
        self.assertEqual(self.bot.list_x_accounts("222"), [])
        interaction = FakeInteraction(guild_id=222)
        await self.call("info", interaction, username="mine")
        self.assertIn("not tracked", interaction.reply_text())


class EnvWiringTests(unittest.TestCase):
    def test_interval_env_override_is_clamped(self):
        self.assertEqual(main._env_int("X_POLL_INTERVAL_SECONDS", 120, 60, 3600), 120)
        os.environ["X_POLL_INTERVAL_SECONDS"] = "10"
        try:
            # Re-reading clamps a too-small value instead of failing.
            self.assertEqual(main._env_int("X_POLL_INTERVAL_SECONDS", 120, 60, 3600), 60)
        finally:
            os.environ.pop("X_POLL_INTERVAL_SECONDS", None)

    def test_x_category_is_a_first_class_alert_type(self):
        self.assertIn("x", main.ALERT_TYPES)
        self.assertEqual(main.ALERT_CHANNEL_COMMANDS["x"], "/set-x-alerts")
        settings = main.default_guild_settings()
        self.assertIn("x", settings["channels"])
        self.assertIn("x", settings["alerts"])
        self.assertTrue(settings["alerts"]["x"])

    def test_fallback_channel_parsing_supports_x(self):
        fallbacks = main.parse_alert_channel_fallbacks("x=123456,news=999")
        self.assertEqual(fallbacks["x"], 123456)
        self.assertEqual(fallbacks["news"], 999)


class CashtagTests(unittest.TestCase):
    def test_extracts_distinct_symbols_in_order_and_uppercases(self):
        text = "Watching $btc and $ETH closely. Did I mention $btc?"
        self.assertEqual(main.x_cashtag_symbols(text), ["BTC", "ETH"])

    def test_caps_the_number_of_symbols(self):
        text = " ".join(f"${letter}coin" for letter in "abcdefgh")
        self.assertEqual(len(main.x_cashtag_symbols(text)), 6)

    def test_ignores_non_letter_and_empty_cases(self):
        self.assertEqual(main.x_cashtag_symbols("price is $100 and $500"), [])
        self.assertEqual(main.x_cashtag_symbols(""), [])
        self.assertEqual(main.x_cashtag_symbols(None), [])


class PolymarketLinkageTests(XTrackerTestCase):
    """The optional Polymarket search field in X alert embeds."""

    def embed_for(self, text: str):
        account = main.default_x_account("cashtag")
        return self.bot.make_x_embed(post("1", text), account, "post", {})

    def field(self, embed, name: str):
        for field in embed.fields:
            if field.name == name:
                return field
        return None

    def test_cashtags_add_search_links(self):
        embed = self.embed_for("Long $BTC, short $SOL. Not financial advice.")
        field = self.field(embed, "Polymarket")
        self.assertIsNotNone(field)
        self.assertIn("$BTC", field.value)
        self.assertIn("polymarket.com/search?_q=BTC", field.value)
        self.assertIn("$SOL", field.value)
        self.assertIn("polymarket.com/search?_q=SOL", field.value)

    def test_polymarket_mention_without_cashtags_offers_a_browse_link(self):
        embed = self.embed_for("Polymarket odds look interesting tonight")
        field = self.field(embed, "Polymarket")
        self.assertIsNotNone(field)
        self.assertIn("polymarket.com/markets", field.value)

    def test_plain_post_gets_no_polymarket_field(self):
        embed = self.embed_for("Just a normal post about the weather")
        self.assertIsNone(self.field(embed, "Polymarket"))


class FallbackChannelTests(XTrackerTestCase):
    async def test_configured_fallback_channel_bootstraps_delivery(self):
        account = main.default_x_account("bootstrapped")
        account["user_id"] = "42"
        account["since_id"] = "100"
        self.track("111", account)
        # The real get_alert_channels consults ALERT_CHANNEL_FALLBACKS; run
        # the genuine implementation with a stubbed channel resolver.
        main.ALERT_CHANNEL_FALLBACKS["x"] = 9001
        self.addCleanup(main.ALERT_CHANNEL_FALLBACKS.pop, "x", None)
        fallback_channel = FakeChannel(9001, guild_id=111)
        self.bot._connection._guilds = {111: SimpleNamespace(id=111)}

        async def resolve(channel_id):
            return fallback_channel

        self.bot.resolve_channel = resolve
        self.serve_posts({"42": [post("200", "delivered via fallback")]})

        await self.bot.run_x_cycle()
        self.assertEqual(len(fallback_channel.sent), 1)


if __name__ == "__main__":
    unittest.main()
