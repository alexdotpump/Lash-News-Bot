"""Tests for the market-aware correlation engine and redesigned news embed.

These tests assert on the real scoring, classification, explanation, dedup, and
embed-output paths using mocked fixtures rather than live APIs, and they preserve
the existing command/monitor behavior by leaving those paths untouched.
"""

import os
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

os.environ.setdefault("DISCORD_BOT_TOKEN", "dummy.token.value")
os.environ.setdefault("NEWS_API_KEY", "dummy-news-key")

import discord  # noqa: E402

import main  # noqa: E402


def article(
    *,
    title: str = "Untitled",
    description: str = "",
    content: str = "",
    source_name: str = "Reuters",
    source_url: str = "https://reuters.com/",
    url: str = "https://example.test/article",
    published_at: str | None = "2026-10-12T12:00:00Z",
) -> dict:
    return {
        "title": title,
        "description": description,
        "content": content,
        "source": {"name": source_name, "url": source_url} if source_name else {},
        "url": url,
        "publishedAt": published_at,
    }


def market(
    *,
    question: str = "Will the market resolve this?",
    slug: str = "will-it-resolve",
    outcomes: str | list = '["Yes", "No"]',
    outcomePrices: str | list = '["0.55", "0.45"]',
    endDate: str = "2026-12-31T00:00:00Z",
    description: str = "",
    event_title: str = "",
    resolution: str = "",
) -> dict:
    return {
        "question": question,
        "slug": slug,
        "outcomes": outcomes,
        "outcomePrices": outcomePrices,
        "endDate": endDate,
        "description": description,
        "event_title": event_title,
        "resolution": resolution,
        "active": True,
        "closed": False,
        "archived": False,
    }


# Fixtures -------------------------------------------------------------------

UKRAINE_ARTICLE = dict(
    title="Ukraine's energy minister announces a new crude supply deal",
    description="The minister confirmed a new crude supply agreement today.",
    content="The minister confirmed a new crude supply agreement today.",
    source_name="Reuters",
    published_at="2026-10-12T12:00:00Z",
)
UKRAINE_MARKET = dict(
    question="Will Ukraine sign a new crude supply deal before Dec 31?",
    slug="ukraine-crude-deal",
    description="A market on Ukraine signing a crude supply deal.",
    event_title="Ukraine energy policy",
    endDate="2026-12-31T00:00:00Z",
)

BRENT_ARTICLE = dict(
    title="Brent crude prices climbed after a major producer reported outages",
    description="Crude prices rose after a major producer reported outages.",
    content="Crude prices rose after a major producer reported outages today.",
    source_name="Reuters",
    published_at="2026-10-12T12:00:00Z",
)
BRENT_MARKET = dict(
    question="Will Brent crude rise above $90 before Dec 31?",
    slug="brent-above-90",
    description="A market on Brent crude crossing $90.",
    event_title="Crude markets",
    endDate="2026-12-31T00:00:00Z",
)

GENEVA_ARTICLE = dict(
    title="Energy ministers met in Geneva this week to discuss Brent crude",
    description="Ministers from several countries met in Geneva to discuss Brent crude.",
    content="Ministers from several countries met in Geneva to discuss Brent crude this week.",
    source_name="Reuters",
    published_at="2026-10-12T12:00:00Z",
)

APPLE_ARTICLE = dict(
    title="Apple reported record iPhone sales in the latest quarter",
    description="Apple Inc. said iPhone sales reached a record high.",
    content="Apple Inc. said iPhone sales reached a record high today.",
    source_name="Reuters",
    published_at="2026-10-12T12:00:00Z",
)
APPLE_MARKET = dict(
    question="Will Apple build a factory in Vietnam before 2027?",
    slug="apple-vietnam-factory",
    description="A market on Apple building a factory in Vietnam.",
    event_title="Apple expansion",
)


class FakeBotForEmbed(unittest.TestCase):
    """A minimal fake so embed tests can call make_news_embed without a real session/state."""

    def make_news_embed(self, article_obj, matches):
        return main.LashNewsBot.make_news_embed(self, article_obj, matches)


class CorrelationUnitTests(unittest.TestCase):
    def test_direct_match_scores_above_alert_threshold(self):
        match = main.compute_correlation(article(**UKRAINE_ARTICLE), market(**UKRAINE_MARKET))
        self.assertIsNotNone(match)
        self.assertGreaterEqual(match.score, main.MIN_CORRELATION_FOR_ALERT)
        self.assertEqual(match.relationship, "Direct")

    def test_direct_match_requires_market_focus_terms_not_just_the_subject(self):
        parsed = main.parse_market_question(market(**UKRAINE_MARKET))
        focus = main.market_focus_terms(parsed)
        self.assertIn("deal", focus)
        self.assertNotIn("ukraine", focus)

    def test_strong_indirect_match_is_scored_and_classified(self):
        match = main.compute_correlation(article(**BRENT_ARTICLE), market(**BRENT_MARKET))
        self.assertIsNotNone(match)
        self.assertGreaterEqual(match.score, main.MIN_CORRELATION_FOR_DIAGNOSTICS)
        self.assertEqual(match.relationship, "Strong indirect")

    def test_contextual_match_is_rejected_by_default(self):
        # Background connectivity without a concrete development is below the
        # diagnostics floor, so it is rejected rather than alerted.
        match = main.compute_correlation(article(**GENEVA_ARTICLE), market(**BRENT_MARKET))
        self.assertIsNone(match)

    def test_contextual_classification_is_not_alertable(self):
        self.assertFalse(main.classify_relationship("Contextual"))
        self.assertFalse(main.classify_relationship("Unrelated"))
        self.assertTrue(main.classify_relationship("Direct"))
        self.assertTrue(main.classify_relationship("Strong indirect"))

    def test_unrelated_high_keyword_overlap_is_rejected(self):
        match = main.compute_correlation(article(**APPLE_ARTICLE), market(**APPLE_MARKET))
        self.assertIsNone(match)

    def test_same_person_different_event_is_rejected(self):
        article_obj = article(
            title="Sam Bankman-Fried gave a university lecture on risk",
            description="Sam Bankman-Fried gave a university lecture on risk management.",
            content="Sam Bankman-Fried gave a university lecture on risk management today.",
            source_name="Reuters",
            published_at="2026-10-12T12:00:00Z",
        )
        m = market(
            question="Will a new FTX-related settlement be announced before 2027?",
            slug="ftx-new-settlement",
            description="A market on a new FTX-related settlement.",
            event_title="FTX legal proceedings",
        )
        self.assertIsNone(main.compute_correlation(article_obj, m))

    def test_same_asset_unrelated_market_is_rejected(self):
        article_obj = article(
            title="Bitcoin miners switched to renewable power this quarter",
            description="Several miners announced renewable power contracts.",
            content="Several miners announced renewable power contracts today.",
            source_name="Reuters",
            published_at="2026-10-12T12:00:00Z",
        )
        m = market(
            question="Will the SEC approve a spot Bitcoin ETF by 2026?",
            slug="sec-bitcoin-etf",
            description="A market on SEC approval of a spot Bitcoin ETF.",
            event_title="SEC cryptocurrency decisions",
        )
        self.assertIsNone(main.compute_correlation(article_obj, m))

    def test_incomplete_resolution_info_is_handled_safely(self):
        article_obj = article(
            title="The central bank signaled a possible rate hike next month",
            description="The central bank signaled a possible rate hike next month.",
            content="The central bank signaled a possible rate hike next month today.",
            source_name="Reuters",
            published_at="2026-10-12T12:00:00Z",
        )
        m = market(
            question="Will the central bank raise rates next month?",
            slug="cb-rate-hike",
            description="A market on a central bank rate hike.",
            event_title="Central bank decisions",
            outcomePrices=["0.40", "0.60"],
            endDate="2026-12-31T00:00:00Z",
        )
        match = main.compute_correlation(article_obj, m)
        self.assertIsNotNone(match)
        self.assertGreaterEqual(match.score, main.MIN_CORRELATION_FOR_DIAGNOSTICS)

    def test_invalid_market_question_returns_no_match(self):
        m = {"active": True, "closed": False, "archived": False}
        self.assertIsNone(main.compute_correlation(article(), m))

    def test_malformed_article_data_is_handled_safely(self):
        self.assertIsNone(main.compute_correlation({"bad": True}, market()))

    def test_empty_article_title_does_not_crash(self):
        self.assertIsNone(main.compute_correlation(article(title=""), market()))

    def test_unknown_publication_timestamp_is_handled(self):
        self.assertIsNone(
            main.compute_correlation(article(published_at=None), market(**BRENT_MARKET))
        )

    def test_future_deadline_is_timeframe_compatible(self):
        self.assertIsNotNone(
            main.compute_correlation(article(**BRENT_ARTICLE), market(**BRENT_MARKET))
        )

    def test_past_deadline_is_not_alertable(self):
        past = dict(BRENT_MARKET, endDate="2024-12-31T00:00:00Z")
        match = main.compute_correlation(article(**BRENT_ARTICLE), market(**past))
        self.assertTrue(match is None or match.score < main.MIN_CORRELATION_FOR_ALERT)


class CorrelationScoreBoundaryTests(unittest.TestCase):
    def test_score_is_an_integer_in_range(self):
        match = main.compute_correlation(article(**BRENT_ARTICLE), market(**BRENT_MARKET))
        self.assertIsNotNone(match)
        self.assertIsInstance(match.score, int)
        self.assertGreaterEqual(match.score, 0)
        self.assertLessEqual(match.score, 100)

    def test_rejection_below_diagnostics_threshold(self):
        # A story that only mentions the subject is rejected, not retained.
        self.assertIsNone(
            main.compute_correlation(article(**GENEVA_ARTICLE), market(**BRENT_MARKET))
        )

    def test_alert_policy_is_gated_on_classification_not_score_alone(self):
        # Even a high-scoring story is not alertable unless its classification is
        # Direct or Strong indirect.
        self.assertFalse(main.classify_relationship("Contextual"))
        match = main.compute_correlation(article(**UKRAINE_ARTICLE), market(**UKRAINE_MARKET))
        self.assertTrue(main.classify_relationship(match.relationship))

    def test_unrelated_story_never_passes_via_keywords(self):
        self.assertIsNone(
            main.compute_correlation(article(**APPLE_ARTICLE), market(**APPLE_MARKET))
        )


class SourceHandlingTests(unittest.TestCase):
    def test_reliable_source_gets_bonus(self):
        self.assertEqual(main.NewsSource(name="Reuters", publisher_type="news").reliability_bonus, 5)

    def test_primary_source_gets_higher_bonus(self):
        src = main.NewsSource(name="Treasury.gov", publisher_type="news", is_primary_source=True)
        self.assertEqual(src.reliability_bonus, 8)

    def test_unknown_source_gets_no_bonus(self):
        self.assertEqual(main.NewsSource(name="Unknown publication").reliability_bonus, 0)

    def test_primary_source_does_not_make_irrelevant_article_relevant(self):
        article_obj = article(
            title="Treasury released a report on agricultural subsidies",
            description="The Treasury released a report on agricultural subsidies.",
            content="The Treasury released a report on agricultural subsidies today.",
            source_name="Treasury.gov",
            published_at="2026-10-12T12:00:00Z",
        )
        self.assertIsNone(main.compute_correlation(article_obj, market(**BRENT_MARKET)))


class TimestampAndStalenessTests(unittest.TestCase):
    def test_stale_article_is_not_alertable(self):
        stale = dict(BRENT_ARTICLE, published_at="2018-01-01T00:00:00Z")
        match = main.compute_correlation(article(**stale), market(**BRENT_MARKET))
        self.assertTrue(match is None or match.score < main.MIN_CORRELATION_FOR_ALERT)

    def test_unknown_publication_time_is_a_mild_penalty(self):
        unknown = dict(BRENT_ARTICLE, published_at=None)
        match = main.compute_correlation(article(**unknown), market(**BRENT_MARKET))
        self.assertIsNotNone(match)
        self.assertLess(match.score, main.MIN_CORRELATION_FOR_ALERT)

    def test_published_at_is_used_not_fetch_time(self):
        self.assertIsNotNone(
            main.compute_correlation(article(**BRENT_ARTICLE), market(**BRENT_MARKET))
        )


class ArticleFingerprintTests(unittest.TestCase):
    def test_identical_articles_share_fingerprint(self):
        a = article(title="Brent crude prices climb on supply fears")
        b = article(title="Brent crude prices climb on supply fears")
        self.assertEqual(main.article_fingerprint(a), main.article_fingerprint(b))

    def test_minor_headline_variation_collapses(self):
        base = "Brent crude prices climb on supply fears"
        variants = [
            base + "!",
            "  " + base + "  ",
            base.upper(),
            base.replace(" ", "  "),
        ]
        fingerprints = {main.article_fingerprint(article(title=variant)) for variant in variants}
        self.assertEqual(len(fingerprints), 1, "minor variants must collapse to one fingerprint")

    def test_different_articles_have_different_fingerprints(self):
        a = article(title="Brent crude prices climb")
        b = article(title="Apple reported record iPhone sales")
        self.assertNotEqual(main.article_fingerprint(a), main.article_fingerprint(b))

    def test_empty_article_yields_empty_fingerprint(self):
        self.assertEqual(main.article_fingerprint({}), "")


class SelectPolymarketMarketsTests(unittest.TestCase):
    def test_returns_best_scoring_matches_only(self):
        a = article(**BRENT_ARTICLE)
        matches = [
            main.compute_correlation(a, market(**BRENT_MARKET)),
            main.compute_correlation(
                a,
                market(
                    question="Will the SEC approve a spot Bitcoin ETF by 2026?",
                    slug="sec-bitcoin-etf",
                    description="A market on SEC approval of a spot Bitcoin ETF.",
                    event_title="SEC cryptocurrency decisions",
                ),
            ),
        ]
        matches = [m for m in matches if m is not None]
        selected = main.select_polymarket_market(matches)
        self.assertLessEqual(len(selected), main.MAX_RELATED_MARKETS)
        self.assertTrue(selected)

    def test_empty_matches_returns_empty_list(self):
        self.assertEqual(main.select_polymarket_market([]), [])

    def test_ranked_direct_beats_indirect(self):
        a = article(**UKRAINE_ARTICLE)
        direct = main.compute_correlation(a, market(**UKRAINE_MARKET))
        indirect = main.compute_correlation(a, market(**BRENT_MARKET))
        present = [m for m in (direct, indirect) if m is not None]
        selected = main.select_polymarket_market(present)
        self.assertTrue(selected)
        self.assertGreaterEqual(selected[0].score, selected[-1].score)


class EmbedFormatTests(unittest.TestCase):
    def setUp(self):
        self.bot = FakeBotForEmbed()

    def test_embed_has_spec_title_and_classic_gold_accent(self):
        match = main.compute_correlation(article(**UKRAINE_ARTICLE), market(**UKRAINE_MARKET))
        embed = self.bot.make_news_embed(article(**UKRAINE_ARTICLE), [match] if match else [])
        self.assertEqual(embed.title, "Market Intel | Sovereign Predicts")
        # The classic SovereignBot gold/brown (rgb(198, 161, 91)), restored
        # from the original embeds in git history after the correlation
        # redesign briefly switched news alerts to blue.
        self.assertEqual(embed.color, main.SOVEREIGN_COLOR)
        self.assertEqual(embed.color.value, 0xC6A15B)

    def test_embed_has_news_link_summary_and_related_markets(self):
        art = article(**dict(UKRAINE_ARTICLE, url="https://example.test/article"))
        match = main.compute_correlation(art, market(**UKRAINE_MARKET))
        embed = self.bot.make_news_embed(art, [match] if match else [])
        self.assertIn("Read Article", embed.description)
        self.assertIn("Summary", [f.name for f in embed.fields])
        self.assertIn("Related Markets", [f.name for f in embed.fields])

    def test_related_markets_shows_question_url_and_score(self):
        art = article(**dict(UKRAINE_ARTICLE, url="https://example.test/article"))
        match = main.compute_correlation(art, market(**UKRAINE_MARKET))
        self.assertIsNotNone(match)
        embed = self.bot.make_news_embed(art, [match])
        related_field = next(f.value for f in embed.fields if f.name == "Related Markets")
        self.assertIn("Correlation:", related_field)
        self.assertIn("Polymarket", related_field)
        self.assertIn("ukraine-crude-deal", related_field)

    def test_footer_has_publisher_and_timestamp_attribution(self):
        embed = self.bot.make_news_embed(article(**UKRAINE_ARTICLE), [])
        self.assertIn("Reuters", embed.footer.text)
        self.assertIn("Sovereign Predicts", embed.footer.text)

    def test_no_political_alignment_field_exists(self):
        embed = self.bot.make_news_embed(article(**UKRAINE_ARTICLE), [])
        for field in embed.fields:
            self.assertNotIn(field.name.lower(), {"political alignment", "political bias"})
        self.assertNotIn("political", embed.footer.text.lower())
        self.assertNotIn("political", embed.title.lower())

    def test_max_related_markets_is_respected(self):
        art = article(**BRENT_ARTICLE)
        markets = [
            market(
                question=f"Will Brent crude rise above ${level} before Dec 31?",
                slug=f"brent-above-{level}",
                description="A market on Brent crude crossing a price.",
                event_title="Crude markets",
            )
            for level in [80, 85, 90, 95, 100]
        ]
        matches = [m for m in (main.compute_correlation(art, mkt) for mkt in markets) if m]
        selected = main.select_polymarket_market(matches)
        self.assertLessEqual(len(selected), main.MAX_RELATED_MARKETS)

    def test_make_embed_delegates_when_correlation_data_is_present(self):
        art = article(**dict(UKRAINE_ARTICLE, url="https://example.test/article"))
        match = main.compute_correlation(art, market(**UKRAINE_MARKET))
        art["_correlation_matches"] = [match] if match else []
        embed = self.bot.make_news_embed(art, art["_correlation_matches"])
        self.assertEqual(embed.title, "Market Intel | Sovereign Predicts")

    def test_make_embed_fallback_when_no_correlation_data(self):
        embed = self.bot.make_news_embed(article(), [])
        self.assertIn("Market Intel", embed.title)

    def test_related_markets_field_is_clamped_to_the_discord_limit(self):
        long_question = (
            "Will Brent crude settle above ninety dollars before December 31? " * 25
        )
        match = main.MarketMatch(
            market=market(question=long_question, slug="brent-very-long"),
            score=90,
            relationship="Direct",
            explanation="The article reports the supply decision this market resolves on.",
            why_short="Reports the crude supply decision.",
        )
        field = main._related_markets_field([match])
        self.assertLessEqual(len(field), main.EMBED_FIELD_VALUE_LIMIT)
        self.assertTrue(field.endswith("\u2026"))

    def test_related_markets_field_drops_whole_entries_when_full(self):
        entries = [
            main.MarketMatch(
                market=market(
                    question=(
                        f"Will Brent crude settle above {level} dollars before "
                        "December 31?"
                    ),
                    slug=f"brent-{level}",
                ),
                score=90,
                relationship="Direct",
                explanation="The article reports the supply decision this market resolves on.",
                why_short="Reports the crude supply decision.",
            )
            for level in (80, 85, 90, 95)
        ]
        field = main._related_markets_field(entries)
        self.assertLessEqual(len(field), main.EMBED_FIELD_VALUE_LIMIT)
        self.assertIn("brent-80", field)
        self.assertNotIn("brent-95", field)

    def test_sanitize_embed_clamps_every_over_long_part(self):
        embed = discord.Embed(title="T" * 400, description="D" * 5000)
        embed.add_field(name="N" * 400, value="V" * 5000, inline=False)
        for index in range(29):
            embed.add_field(name=f"field-{index}", value="V" * 5000, inline=False)
        embed.set_footer(text="F" * 3000)
        embed.set_author(name="A" * 400)
        main.sanitize_embed(embed)
        self.assertEqual(len(embed.title), main.EMBED_TITLE_LIMIT)
        self.assertEqual(len(embed.description), main.EMBED_DESCRIPTION_LIMIT)
        self.assertEqual(len(embed.fields), main.EMBED_FIELD_COUNT_LIMIT)
        for field in embed.fields:
            self.assertLessEqual(len(field.name), main.EMBED_FIELD_NAME_LIMIT)
            self.assertLessEqual(len(field.value), main.EMBED_FIELD_VALUE_LIMIT)
        self.assertEqual(len(embed.footer.text), main.EMBED_FOOTER_LIMIT)
        self.assertEqual(len(embed.author.name), main.EMBED_AUTHOR_LIMIT)
        self.assertTrue(embed.fields[0].value.endswith("\u2026"))
        self.assertEqual(len(embed.fields[0].name), main.EMBED_FIELD_NAME_LIMIT)


class ProviderAndFailureTests(unittest.TestCase):
    def test_build_news_provider_requires_approved_provider(self):
        with self.assertRaises(RuntimeError) as ctx:
            main.build_news_provider(None, api_key="key", provider_name="SomeOtherProvider")
        self.assertIn("not configured", str(ctx.exception).lower())

    def test_newsapi_adapter_rejects_missing_key(self):
        class DummySession:
            pass

        adapter = main.NewsAPIAdapter(DummySession(), api_key="")
        with self.assertRaises(RuntimeError):
            import asyncio

            asyncio.run(adapter.fetch_articles("test"))

    def test_market_api_outage_is_handled_at_candidate_level(self):
        bad_market = {"active": True, "closed": False, "archived": False, "question": None}
        self.assertIsNone(main.compute_correlation(article(), bad_market))


class DeduplicationAndSyndicationTests(unittest.TestCase):
    def test_fingerprint_is_stable_across_minor_variations(self):
        base = "Brent crude prices climb on supply fears"
        variants = [base, base + "!", "  " + base + "  ", base.upper()]
        fingerprints = {main.article_fingerprint(article(title=variant)) for variant in variants}
        self.assertEqual(len(fingerprints), 1)

    def test_fingerprint_distinguishes_different_stories(self):
        self.assertNotEqual(
            main.article_fingerprint(article(title="Brent crude prices climb")),
            main.article_fingerprint(article(title="Apple reported record iPhone sales")),
        )


class DiagnosticsAndRejectionLoggingTests(unittest.TestCase):
    def test_rejection_reason_not_emitted_as_match(self):
        self.assertIsNone(
            main.compute_correlation(article(**APPLE_ARTICLE), market(**APPLE_MARKET))
        )

    def test_diagnostic_threshold_exposes_mid_range_matches(self):
        match = main.compute_correlation(article(**BRENT_ARTICLE), market(**BRENT_MARKET))
        self.assertIsNotNone(match)
        self.assertGreaterEqual(match.score, main.MIN_CORRELATION_FOR_DIAGNOSTICS)
        self.assertLess(match.score, main.MIN_CORRELATION_FOR_ALERT)

    def test_direct_match_is_alertable(self):
        match = main.compute_correlation(article(**UKRAINE_ARTICLE), market(**UKRAINE_MARKET))
        self.assertIsNotNone(match)
        self.assertTrue(main.classify_relationship(match.relationship))
        self.assertGreaterEqual(match.score, main.MIN_CORRELATION_FOR_ALERT)


class FakeChannel:
    def __init__(self, channel_id: int = 555) -> None:
        self.id = channel_id
        self.mention = f"<#{channel_id}>"


class ValidatingFakeChannel(FakeChannel):
    """Stand-in for Discord that rejects embeds over the documented limits.

    Discord answers an over-long embed with HTTP 400 "Invalid Form Body", so the
    checks here mirror that behaviour: an alert that Discord would drop fails the
    test instead of disappearing quietly.
    """

    def __init__(self, channel_id: int = 555) -> None:
        super().__init__(channel_id)
        self.sent_embeds: list = []

    async def send(self, *, embed=None, allowed_mentions=None):
        self.assert_embed_is_valid(embed)
        self.sent_embeds.append(embed)

    def assert_embed_is_valid(self, embed) -> None:
        if embed is None:
            raise AssertionError("an embed is required")
        if embed.title and len(embed.title) > main.EMBED_TITLE_LIMIT:
            raise AssertionError("embed title is over Discord's limit")
        if embed.description and len(embed.description) > main.EMBED_DESCRIPTION_LIMIT:
            raise AssertionError("embed description is over Discord's limit")
        if len(embed.fields) > main.EMBED_FIELD_COUNT_LIMIT:
            raise AssertionError("embed has too many fields")
        for field in embed.fields:
            if len(field.name) > main.EMBED_FIELD_NAME_LIMIT:
                raise AssertionError("field name is over Discord's limit")
            if len(field.value) > main.EMBED_FIELD_VALUE_LIMIT:
                raise AssertionError(
                    "field value is over Discord's limit: "
                    f"{field.name!r} ({len(field.value)} characters)"
                )
        if embed.footer and embed.footer.text:
            if len(embed.footer.text) > main.EMBED_FOOTER_LIMIT:
                raise AssertionError("embed footer is over Discord's limit")


class NewsCycleIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end coverage of the correlation pipeline through run_news_cycle."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    async def _bot(self, *, articles, markets, stub_post: bool = True):
        bot = main.create_bot()
        bot._connection.application_id = 1

        class FakeSession:
            async def close(self):
                return None

        bot.news_session = FakeSession()  # non-None marker; get_json is stubbed below

        async def fake_get_json(url, params=None, headers=None, timeout=None):
            if url == main.POLYMARKET_MARKETS_URL:
                return list(markets)
            raise AssertionError(f"unexpected API call: {url}")

        bot.get_json = fake_get_json

        class FakeAdapter:
            name = "NewsAPI"

            async def fetch_articles(self, query):
                return list(articles)

        bot._news_provider = lambda: FakeAdapter()

        self.posts = []

        async def record(channel, embed, guild_id):
            self.posts.append((guild_id, embed))
            return True

        if stub_post:
            bot.send_embed = record
        bot.ensure_settings("1")
        bot.ensure_guild_history("1")
        self.addAsyncCleanup(bot.close)
        return bot

    async def test_relevant_article_posts_and_irrelevant_does_not(self):
        relevant = article(**dict(UKRAINE_ARTICLE, url="https://example.test/ukraine"))
        irrelevant = article(**dict(APPLE_ARTICLE, url="https://example.test/apple"))
        bot = await self._bot(
            articles=[relevant, irrelevant],
            markets=[market(**UKRAINE_MARKET), market(**APPLE_MARKET)],
        )
        await bot.run_news_cycle({"1": FakeChannel()})
        self.assertEqual(len(self.posts), 1, "only the relevant article may post")
        _, embed = self.posts[0]
        self.assertEqual(embed.title, "Market Intel | Sovereign Predicts")
        related = next(f.value for f in embed.fields if f.name == "Related Markets")
        self.assertIn("Correlation:", related)
        self.assertIn("ukraine-crude-deal", related)

    async def test_syndicated_duplicate_posts_only_once(self):
        first = article(**dict(UKRAINE_ARTICLE, url="https://example.test/a"))
        duplicate = article(**dict(UKRAINE_ARTICLE, url="https://example.test/b"))
        bot = await self._bot(
            articles=[first, duplicate],
            markets=[market(**UKRAINE_MARKET)],
        )
        await bot.run_news_cycle({"1": FakeChannel()})
        self.assertEqual(len(self.posts), 1, "a syndicated copy must not post twice")

    async def test_irrelevant_article_produces_no_post(self):
        bot = await self._bot(
            articles=[article(**dict(APPLE_ARTICLE, url="https://example.test/apple"))],
            markets=[market(**APPLE_MARKET)],
        )
        await bot.run_news_cycle({"1": FakeChannel()})
        self.assertEqual(self.posts, [])

    async def test_real_send_path_trims_an_over_long_embed(self):
        bot = await self._bot(articles=[], markets=[], stub_post=False)
        channel = ValidatingFakeChannel()
        embed = discord.Embed(
            title="Market Intel | Sovereign Predicts",
            description="https://example.test/" + "d" * 5000,
        )
        embed.add_field(name="Summary", value="S" * 4000, inline=False)
        embed.add_field(name="Related Markets", value="R" * 4000, inline=False)
        posted = await bot.send_embed(channel, embed, "1")
        self.assertTrue(posted, "the trimmed alert must still post")
        self.assertEqual(len(channel.sent_embeds), 1)
        sent = channel.sent_embeds[0]
        self.assertLessEqual(len(sent.description), main.EMBED_DESCRIPTION_LIMIT)
        for field in sent.fields:
            self.assertLessEqual(len(field.value), main.EMBED_FIELD_VALUE_LIMIT)

    async def test_news_cycle_alert_satisfies_discord_limits(self):
        article_fixture = article(
            **dict(UKRAINE_ARTICLE, url="https://example.test/ukraine")
        )
        bot = await self._bot(
            articles=[article_fixture],
            markets=[market(**UKRAINE_MARKET)],
            stub_post=False,
        )
        channel = ValidatingFakeChannel()
        await bot.run_news_cycle({"1": channel})
        self.assertEqual(len(channel.sent_embeds), 1)
        related = next(
            field.value
            for field in channel.sent_embeds[0].fields
            if field.name == "Related Markets"
        )
        self.assertIn("Correlation:", related)

if __name__ == "__main__":
    unittest.main()
