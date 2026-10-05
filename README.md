# Lash News Bot

Lash News Bot searches news using active Polymarket markets and posts matching articles to independently configured Discord servers.

## Run & Operate

- `python main.py` — run the bot.
- `python -m pip install -r requirements.txt` — install dependencies.
- Required environment variables: `DISCORD_BOT_TOKEN` and `NEWS_API_KEY`.
- Optional environment variables: `DISCORD_CHANNEL_ID` (legacy fallback alert channel), `NEWS_LANGUAGE` (defaults to `en`), and `NEWS_POLL_INTERVAL_SECONDS` (defaults to `300`, minimum `60`).
- Enable Message Content Intent in the Discord Developer Portal for the existing `!test` and `!status` prefix commands. Slash commands use Discord interactions.
- The bot needs permission to view the server, send messages, and embed links in each configured alert channel.

## Stack

- Python 3.11+
- `discord.py` for Discord connection and commands
- `aiohttp` for asynchronous NewsAPI and Polymarket requests

## Files

- `main.py` — market-driven news discovery, per-server delivery, Discord embeds, and commands.
- `requirements.txt` — Python dependencies.
- `guild_channels.json` — persistent alert-channel selection for each Discord server.
- `seen_articles_by_guild.json` — duplicate history kept independently for each server.

## Behavior

- Each poll searches active, open Polymarket markets ranked by 24-hour and lifetime volume, then uses their questions to create a NewsAPI query. NewsAPI limits each query to 500 characters and returns up to 20 articles per poll; every returned article is checked against Polymarket, and only articles matching an active, open market are posted.
- Includes the matched market's Polymarket link at the bottom of each alert.
- Runs polling as an asynchronous background task while the bot is connected, with exponential retry delays after failures; errors are logged without stopping the Discord connection.
- `/setup` lets a server administrator choose that server's alert channel. Each server's setting is saved separately; the optional `DISCORD_CHANNEL_ID` remains as a legacy fallback for its server.
- `/status`, `/test`, `/sources`, `/keywords`, and `/help` provide server status, a test embed, recent NewsAPI sources, the current market-derived search terms, and command help.
- Existing `!status` and `!test` prefix commands remain available.
- Remembers recently posted URLs globally for compatibility and separately per server for multi-server duplicate prevention.