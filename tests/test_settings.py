"""Tests for per-server settings and monitor-state persistence.

Covers the multi-server requirements: each server's settings survive a restart,
one server's configuration cannot overwrite another's, and corrupt or hand-edited
state files fall back to safe defaults instead of crashing the bot.
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import main  # noqa: E402


class SettingsStateTestCase(unittest.TestCase):
    """Every case runs in a temporary directory so state files are isolated."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()


class DefaultSettingsTests(SettingsStateTestCase):
    def test_defaults_have_the_full_expected_shape(self):
        settings = main.default_guild_settings()
        self.assertEqual(
            set(settings),
            {
                "channel_id",
                "channels",
                "include_keywords",
                "exclude_keywords",
                "sources",
                "alerts",
                "price_move_threshold",
                "whale_min_usd",
                "new_market_min_liquidity",
            },
        )
        self.assertEqual(set(settings["alerts"]), set(main.ALERT_TYPES))
        self.assertTrue(all(settings["alerts"].values()))
        # Every alert category starts with its own unset channel override.
        self.assertEqual(set(settings["channels"]), set(main.ALERT_TYPES))
        self.assertTrue(all(value is None for value in settings["channels"].values()))

    def test_normalize_rejects_garbage_and_clamps_numbers(self):
        normalized = main.normalize_guild_settings(
            {
                "channel_id": "not-an-int",
                "include_keywords": ["AI", "ai", "  ", "Crypto"],
                "exclude_keywords": "not-a-list",
                "sources": [f"s{i}" for i in range(main.MAX_SOURCES_PER_GUILD + 10)],
                "alerts": {"news": False, "price": "yes", "bogus": True},
                "price_move_threshold": 9999,
                "whale_min_usd": -5,
                "new_market_min_liquidity": "abc",
            }
        )
        self.assertIsNone(normalized["channel_id"])
        # Case-insensitive duplicates and blanks are dropped, order preserved.
        self.assertEqual(normalized["include_keywords"], ["AI", "Crypto"])
        self.assertEqual(normalized["exclude_keywords"], [])
        self.assertEqual(len(normalized["sources"]), main.MAX_SOURCES_PER_GUILD)
        self.assertFalse(normalized["alerts"]["news"])
        # A non-bool alert value keeps the default rather than being coerced.
        self.assertTrue(normalized["alerts"]["price"])
        self.assertEqual(
            normalized["price_move_threshold"], main.MAX_PRICE_MOVE_THRESHOLD
        )
        self.assertEqual(normalized["whale_min_usd"], main.MIN_WHALE_MIN_USD)
        self.assertEqual(
            normalized["new_market_min_liquidity"],
            main.default_guild_settings()["new_market_min_liquidity"],
        )

    def test_new_market_floor_defaults_to_zero(self):
        # Brand-new markets report liquidityNum == 0, so a non-zero default floor
        # would suppress every new-market alert (verified against the live API).
        self.assertEqual(main.DEFAULT_NEW_MARKET_MIN_LIQUIDITY, 0.0)
        self.assertEqual(
            main.default_guild_settings()["new_market_min_liquidity"], 0.0
        )

    def test_normalize_of_a_non_dict_is_all_defaults(self):
        self.assertEqual(
            main.normalize_guild_settings(["nope"]), main.default_guild_settings()
        )


class PersistenceTests(SettingsStateTestCase):
    def test_settings_survive_a_reload(self):
        settings = {
            "111": main.normalize_guild_settings(
                {"channel_id": 555, "include_keywords": ["election"]}
            )
        }
        main.save_guild_settings(settings)
        self.assertTrue(main.GUILD_SETTINGS_FILE.exists())

        reloaded = main.load_guild_settings()
        self.assertEqual(reloaded["111"]["channel_id"], 555)
        self.assertEqual(reloaded["111"]["include_keywords"], ["election"])

    def test_servers_do_not_overwrite_each_other(self):
        first = main.default_guild_settings()
        first["channel_id"] = 111
        first["include_keywords"] = ["ai"]
        second = main.default_guild_settings()
        second["channel_id"] = 222
        second["exclude_keywords"] = ["sports"]
        main.save_guild_settings({"1": first, "2": second})

        reloaded = main.load_guild_settings()
        self.assertEqual(reloaded["1"]["channel_id"], 111)
        self.assertEqual(reloaded["2"]["channel_id"], 222)
        self.assertEqual(reloaded["1"]["include_keywords"], ["ai"])
        self.assertEqual(reloaded["2"]["exclude_keywords"], ["sports"])
        self.assertEqual(reloaded["2"]["include_keywords"], [])

    def test_legacy_channel_file_seeds_settings(self):
        # Servers configured before settings existed only have guild_channels.json.
        main.write_json_atomically(main.GUILD_CHANNELS_FILE, {"9": 909})
        main.write_json_atomically(main.GUILD_SETTINGS_FILE, {})
        reloaded = main.load_guild_settings()
        self.assertEqual(reloaded["9"]["channel_id"], 909)

    def test_corrupt_settings_file_falls_back_to_empty(self):
        main.GUILD_SETTINGS_FILE.write_text("{not json", encoding="utf-8")
        self.assertEqual(main.load_guild_settings(), {})

    def test_non_numeric_guild_keys_are_ignored(self):
        main.write_json_atomically(
            main.GUILD_SETTINGS_FILE, {"not-a-guild": {"channel_id": 1}, "7": {}}
        )
        reloaded = main.load_guild_settings()
        self.assertNotIn("not-a-guild", reloaded)
        self.assertIn("7", reloaded)

    def test_monitor_state_round_trips_and_is_bounded(self):
        state = {
            "seen_market_ids": [str(i) for i in range(main.MAX_SEEN_MARKETS + 50)],
            "seen_trades": ["a|b|c"],
            "last_prices": {"m1": {"price": 0.4, "alerted": 0.3}},
        }
        main.save_monitor_state(state)

        reloaded = main.load_monitor_state()
        self.assertEqual(len(reloaded["seen_market_ids"]), main.MAX_SEEN_MARKETS)
        self.assertEqual(reloaded["seen_trades"], ["a|b|c"])
        self.assertEqual(reloaded["last_prices"]["m1"], {"price": 0.4, "alerted": 0.3})

    def test_corrupt_monitor_state_falls_back_to_empty(self):
        main.MONITOR_STATE_FILE.write_text("<<<", encoding="utf-8")
        state = main.load_monitor_state()
        self.assertEqual(state["seen_market_ids"], [])
        self.assertEqual(state["seen_trades"], [])
        self.assertEqual(state["last_prices"], {})

    def test_atomic_write_leaves_no_temp_file_behind(self):
        main.write_json_atomically(main.GUILD_SETTINGS_FILE, {"1": {}})
        leftovers = [p.name for p in Path(".").iterdir() if p.suffix == ".tmp"]
        self.assertEqual(leftovers, [])
        self.assertEqual(
            json.loads(main.GUILD_SETTINGS_FILE.read_text(encoding="utf-8")), {"1": {}}
        )


class MarketParsingTests(unittest.TestCase):
    def test_outcome_prices_parses_json_encoded_strings(self):
        self.assertEqual(
            main.parse_outcome_prices({"outcomePrices": '["0.62", "0.38"]'}),
            [0.62, 0.38],
        )

    def test_outcome_prices_accepts_a_real_list(self):
        self.assertEqual(main.parse_outcome_prices({"outcomePrices": [0.5, 0.5]}), [0.5, 0.5])

    def test_outcome_prices_rejects_bad_values(self):
        self.assertEqual(main.parse_outcome_prices({"outcomePrices": "nope"}), [])
        self.assertEqual(main.parse_outcome_prices({"outcomePrices": '["x"]'}), [])
        self.assertEqual(main.parse_outcome_prices({}), [])

    def test_outcomes_parses_json_encoded_strings(self):
        self.assertEqual(
            main.parse_outcomes({"outcomes": '["Yes", "No"]'}), ["Yes", "No"]
        )
        self.assertEqual(main.parse_outcomes({"outcomes": "nope"}), [])


if __name__ == "__main__":
    unittest.main()
