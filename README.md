# Lash News Bot

Lash News Bot watches Polymarket markets and posts relevant alerts to independently
configured Discord servers. It covers four alert types: market-moving news, sharp odds
moves, brand-new markets, and large executed trades.

## Run & Operate

- `python main.py` — run the bot.
- `python -m pip install -r requirements.txt` — install dependencies.
- `python -m unittest discover -s tests` — run the test suite.
- Required environment variables: `DISCORD_BOT_TOKEN` and `NEWS_API_KEY`.
- Optional: `NEWS_LANGUAGE`, `NEWS_POLL_INTERVAL_SECONDS` (default `300`, minimum `60`),
  `DISCORD_CHANNEL_ID` (legacy fallback channel), `DISCORD_DEV_GUILD_ID`/`DISCORD_GUILD_IDS`
  (comma-separated servers that receive instant command sync), and `STATE_DIR`.

### Persistence (important on Railway)

Railway and most container hosts **wipe the filesystem on every deploy**. Point `STATE_DIR`
at a mounted volume (for example `/data`) or every server's settings and dedupe history are
lost on each redeploy. Without a volume the bot still runs, but `/setup` must be repeated
after every deploy.

### Discord setup

- Invite the bot with **both** the `bot` **and** `applications.commands` OAuth scopes, or
  slash commands will never appear.
- Enable **Message Content Intent** in the Developer Portal only if you want the legacy
  `!test` / `!status` prefix commands. Slash commands do not need it.
- Grant View Channel, Send Messages, and Embed Links in each alert channel.

## Commands

| Command | Who | What it does |
|---|---|---|
| `/setup` | administrators | Pick the alert channel and toggle each alert type for this server. |
| `/alerts` | administrators | Set the price-move, large-trade, and new-market thresholds. |
| `/set-news-alerts`, `/set-price-alerts`, `/set-new-markets`, `/set-whale-alerts`, `/set-wallet-alerts` | administrators | Send one alert category to its own channel. |
| `/status` | members | Per-subsystem health (Discord, news polling, market-data polling, wallet tracking, alert delivery) and this server's settings. |
| `/test` | members | Send a test embed to the configured channel. |
| `/chart` | members | Render a market's price history as a PNG. |
| `/config-check` | members | Report broken alert channels or permissions. |
| `/wallet add` | administrators | Track a Polymarket wallet address here. |
| `/wallet remove` | administrators | Stop tracking a wallet. |
| `/wallet list` | members | List this server's tracked wallets. |
| `/wallet info` | members | Show one tracked wallet's settings and status. |
| `/wallet settings` | administrators | Change a wallet's label, minimum value, activity filter, or whale-alert suppression. |
| `/wallet pause`, `/wallet resume` | administrators | Stop or restart alerts for one wallet without deleting it. |
| `/sources` | administrators | Show or configure which publishers this server accepts. |
| `/keywords` | administrators | View or manage include/exclude keywords. |
| `/help` | everyone | Command list. |
| `/sync`, `/servers`, `/server-info` | bot owner | Force a re-sync, list servers, show connection details. |
| `!status`, `!test` | members | Legacy prefix equivalents. |

## Alert types

- **News** — a NewsAPI query built from the questions of active, high-volume Polymarket
  markets (plus each server's include keywords). Each candidate article is then evaluated
  against the active market catalog by a market-aware correlation engine (see below) before
  anything is posted, and each server applies its own include/exclude keyword and publisher
  filters.
- **Price moves** — watches the top markets by 24-hour volume and alerts when an outcome's
  price moves past this server's threshold. The first sighting only records a baseline, so
  deploying never triggers a burst of alerts.
- **New markets** — polls Gamma with `order=createdAt` and alerts on markets it has not seen.
  The very first successful check seeds silently so deploying does not dump old markets.
- **Large trades** — reads Polymarket's Data API trade feed (`/v2/trades` with
  `taker_only=true` and a server-side `filter_type=CASH` floor) so only **executed taker
  fills** alert, never open orders or positions. Each embed says so explicitly and links the
  transaction on Polygonscan.
- **Wallet activity** — watches the addresses a server tracks. See below.

### Wallet tracking

Each server keeps its own watchlist. Wallets are identified by their address; a label is
cosmetic. Watchlists persist in `wallets.json` inside `STATE_DIR`, so they survive a redeploy
when `STATE_DIR` is a mounted volume.

- **Data source** — Polymarket's Data API activity feed,
  `https://data-api.polymarket.com/activity?user=<address>`, the same public host the whale
  monitor already uses. Verified live: it returns `TRADE`, `SPLIT`, `MERGE`, `REDEEM`,
  `REWARD`, and `CONVERSION` rows carrying `usdcSize` (the settled transaction value), price,
  side, market title, slug, and the transaction hash. Addresses are Polymarket proxy wallets:
  a plain EVM `0x` address. A syntactically valid address that is not a Polymarket account is
  accepted but reports no activity, and `/wallet add` says so instead of pretending.
- **What is tracked** — new activity rows for a tracked wallet, with the market, outcome,
  side, size, transaction value, timestamp, and links to the market and the transaction.
- **Filters** — an activity filter (`trades` for executed fills only, `all` for every row),
  a per-wallet minimum transaction value, and pause/resume.
- **Dedupe and baselines** — the first check after a wallet is added only records a baseline,
  so adding a busy wallet does not dump its history into the channel. Each wallet remembers
  its newest processed timestamp plus a bounded set of activity keys, and both are persisted,
  so a restart cannot replay an alert.
- **Whale overlap** — a tracked wallet's large fill would otherwise alert twice (once from the
  whale monitor, once from the wallet monitor). By default the whale copy is suppressed for
  tracked wallets; `/wallet settings whale_alerts:true` turns it back on.
- **Value vs position** — the reported value is the value of that single activity row. It is
  not the wallet's total position, ownership, or intent, and every wallet embed says so.
- **Bounds** — at most `MAX_TRACKED_WALLETS_PER_GUILD` (25) wallets per server and
  `MAX_WALLET_ADDRESSES_PER_CYCLE` (50) distinct addresses polled per cycle, one request per
  address per cycle no matter how many servers track it.

### News correlation engine

News alerts are not keyword matches. Every candidate article is scored against the active
market catalog and only genuinely related stories are posted.

- **Candidate filtering** — cheap entity and question-term overlap narrows the catalog before
  the deeper scorer runs, so the expensive path is never applied to every article/market pair.
- **Relationship classification** — each evaluated pair is classified as `Direct` (the article
  reports the event or decision the market resolves on), `Strong indirect` (credible
  information with a clear, explainable relationship to the outcome), `Contextual`
  (background only), or `Unrelated`.
- **Correlation score (0-100)** — an estimate of *this article's relevance to this specific
  market question*. It is **not** a prediction that the market resolves YES and **not** a
  probability of any outcome. Scores combine entity correspondence, question focus terms,
  source reliability, resolution-criteria compatibility, time-frame compatibility, and
  penalties for superficial keyword overlap, stale articles, and unsupported claims.
- **Alert policy** — the bar is one explicit, tunable gate (`is_alertable`). A `Direct` or
  `Strong indirect` match posts at `NEWS_CORRELATION_ALERT_THRESHOLD` (default **60**); a
  `Contextual` match needs that plus `NEWS_CORRELATION_CONTEXTUAL_MARGIN` (default **75**),
  because a shared subject is much weaker evidence than a concrete development. Matches at or
  above `NEWS_CORRELATION_DIAGNOSTICS_THRESHOLD` (default **45**) are kept for logging and
  tuning and are reported as `near miss` lines; everything below is rejected. Nothing below
  the bar is ever substituted onto an alert.
- **Structural guards** — a lower score bar only helps if the score means something, so a
  match must also clear three checks:
  - the shared evidence cannot be only numbers, only calendar words ("October", "Tuesday"),
    or vocabulary that appears across the market catalog ("2026", "december") — those are
    excluded from evidence because a market that resolves "by October 31" and an article
    published in October share a month, not a subject;
  - a named entity that few markets use is evidence by itself; shared ordinary words are not,
    so at least `NEWS_MIN_SUBSTANTIVE_TERMS` (default 2) of them are required, because one
    shared word links two unrelated stories;
  - the evidence must be in the article **headline** (`NEWS_REQUIRE_HEADLINE_EVIDENCE`,
    default true), since NewsAPI body text carries site furniture and related-article links.
- **Market entities** — taken from the market question and event title. Gamma's long
  resolution descriptions are deliberately excluded: they are full of capitalized ordinary
  words ("Note", "If", "However") that used to make unrelated stories look correlated.
- **Self-explaining matches** — each accepted match carries a short, article-grounded reason
  describing why this story matters to that market. The engine never invents facts,
  resolution rules, or causal links the evidence does not support.
- **Dedup** — canonical URL plus a normalized content fingerprint collapses repeated polling,
  minor headline variants, and syndicated copies so one story does not alert twice.
- **Source adapter** — providers are plugged in behind a small adapter interface. `NewsAPI` is
  the default and remains the fallback; no additional provider is enabled without a key.

##### Measured precision (read this before lowering the bar)

Verified against the live NewsAPI and Gamma APIs (94 articles from five pages of the
market-driven query, 191 top-volume markets):

| Configuration | Alerts produced | Notes |
| --- | --- | --- |
| Previous policy (85/70), guards on | 5 | 2 were off-topic |
| Current policy (60/45), guards on | 7 | 2 more matches, one off-topic |
| Current policy, headline guard off | 28 | mostly off-topic |

Two conclusions from that run, both worth knowing before tuning: the relaxed bar roughly
doubles the number of matches on the same input while the guards keep obvious junk out; and
the score itself is a bag-of-words estimate whose remaining ceiling is the matcher, not the
threshold. If alerts look too noisy, raise `NEWS_CORRELATION_ALERT_THRESHOLD`; if the feed is
too quiet, set `NEWS_REQUIRE_HEADLINE_EVIDENCE=false` and re-check
`NEWS_MIN_SUBSTANTIVE_TERMS`. The `near miss` log lines name the closest market and the score
it needed, which is the fastest way to see how far off a rejected story was.

All of these thresholds and guards are environment settings, not per-server settings and not
secrets.

##### Alert channel fallbacks

`ALERT_CHANNEL_FALLBACKS` (for example
`news=1556620827154321419,whale=1556058128674586644`) names a channel for a category that has
no channel configured. It exists because a category nobody pointed anywhere used to be skipped
silently, which looked exactly like a dead task. The fallback only fills a gap: an explicit
`/set-...-alerts` choice always wins, a category switched off in that server is left off, and
the channel must belong to the server it is used for, so it can never cross between servers.

### Rate and noise limits

Every alert type is throttled: at most `MAX_ALERTS_PER_CYCLE` (10) alerts per cycle across
all servers, the watched market set is capped at 40, and the dedupe history is bounded at
3000 entries per type. Polling defaults to every 5 minutes.

## Slash commands troubleshooting

If commands appear but answering them fails, or they never appear at all:

1. Check the startup logs. Registration is logged, and any sync failure is logged with the
   reason (usually a missing `applications.commands` scope or an invalid token).
2. Global commands can take up to an hour to propagate. Set `DISCORD_DEV_GUILD_ID` for an
   instant, guild-scoped sync while testing, or run the owner-only `/sync`.
3. Commands are registered once in `setup_hook`, guarded by `register_commands`, which binds
   each command object to the bot. Without that binding, `discord.py` calls a callback
   without `self` and every interaction dies with `CommandSignatureMismatch` — the command is
   visible but answers "the application did not respond". `tests/test_slash_commands.py`
   covers this.
4. Unhandled command errors are logged and the user always gets a reply, so `/status` and the
   logs should show the real exception instead of failing silently.

## Stack

- Python 3.11+
- `discord.py` for the Discord connection and application commands
- `aiohttp` for asynchronous NewsAPI, Polymarket Gamma, and Polymarket Data API requests
- `matplotlib` for `/chart`. It is imported lazily so a broken install disables `/chart`
  instead of stopping the bot, which also means it must stay declared in `requirements.txt`
  or the deployed image has no renderer — that was the production `/chart` failure.
  `tests/test_chart_and_providers.py` pins the dependency.

## Files

- `main.py` — monitoring loops, alert embeds, per-server settings, and commands.
- `requirements.txt` — Python dependencies.
- `tests/` — unit tests for command registration/invocation, HTTP retries, per-server
  settings persistence, the monitors, and the news correlation engine
  (`tests/test_correlation.py`).
- `guild_settings.json` — per-server settings (created at runtime, in `STATE_DIR`).
- `monitor_state.json` — cross-restart dedupe and price baselines (in `STATE_DIR`).
- `wallets.json` — per-server wallet watchlists, per-wallet settings, and wallet dedupe state
  (in `STATE_DIR`).
- `seen_articles_by_guild.json` — duplicate history kept independently for each server.
- `guild_channels.json` — legacy alert-channel file, still read for backward compatibility.

## Data sources

Polymarket market data comes from Gamma (`gamma-api.polymarket.com`) and executed trades from
the Data API (`data-api.polymarket.com`); both are public and need no API key. NewsAPI
supplies the headlines. There is no official Polymarket news endpoint.
