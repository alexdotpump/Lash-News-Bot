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
| `/status` | members | Monitoring health, last successful check, and this server's settings. |
| `/test` | members | Send a test embed to the configured channel. |
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
- **Alert policy** — a match is posted only when its score is at least
  `MIN_CORRELATION_FOR_ALERT` (default 85) **and** its classification is `Direct` or
  `Strong indirect`. Matches scoring at or above `MIN_CORRELATION_FOR_DIAGNOSTICS`
  (default 70) are retained for diagnostics; everything below is rejected. An unrelated story
  is never promoted merely because it shares keywords.
- **Self-explaining matches** — each accepted match carries a short, article-grounded reason
  describing why this story matters to that market. The engine never invents facts,
  resolution rules, or causal links the evidence does not support.
- **Dedup** — canonical URL plus a normalized content fingerprint collapses repeated polling,
  minor headline variants, and syndicated copies so one story does not alert twice.
- **Source adapter** — providers are plugged in behind a small adapter interface. `NewsAPI` is
  the default and remains the fallback; no additional provider is enabled without a key.

These thresholds (`MIN_CORRELATION_FOR_ALERT`, `MIN_CORRELATION_FOR_DIAGNOSTICS`,
`MAX_RELATED_MARKETS`) are code-level calibration constants, not per-server settings and not
secrets.

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

## Files

- `main.py` — monitoring loops, alert embeds, per-server settings, and commands.
- `requirements.txt` — Python dependencies.
- `tests/` — unit tests for command registration/invocation, HTTP retries, per-server
  settings persistence, the monitors, and the news correlation engine
  (`tests/test_correlation.py`).
- `guild_settings.json` — per-server settings (created at runtime, in `STATE_DIR`).
- `monitor_state.json` — cross-restart dedupe and price baselines (in `STATE_DIR`).
- `seen_articles_by_guild.json` — duplicate history kept independently for each server.
- `guild_channels.json` — legacy alert-channel file, still read for backward compatibility.

## Data sources

Polymarket market data comes from Gamma (`gamma-api.polymarket.com`) and executed trades from
the Data API (`data-api.polymarket.com`); both are public and need no API key. NewsAPI
supplies the headlines. There is no official Polymarket news endpoint.
