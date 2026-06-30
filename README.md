# Study Bot for Discord

A comprehensive Discord bot developed in Python, designed to facilitate community focus, monitor study duration, and gamify the learning experience. Key functionalities include advanced time tracking, a progression-based leveling system, and a real-time web dashboard.

## Core Features

* **Automated Time Tracking:** Automatically logs study duration when users enable their camera or screen sharing within designated focus channels.
* **Gamification Mechanics:** * Experience (XP) and leveling architecture with automated Discord role synchronization.
    * Daily objective tracking, consecutive activity streaks, and unlockable achievement badges.
    * Programmatically generated user profile cards accessible via the `/card` command.
* **Analytics and Reporting:** Daily performance leaderboards, automated weekly progress reports delivered via Direct Message (DM), and granular personal statistics.
* **Real-time Web Dashboard:** Embedded Flask web server providing live updates on active study sessions and user activity heatmaps.
* **Artificial Intelligence Integration:** Capability to query study-related topics utilizing the OpenRouter AI API.
* **Shared Virtual Coin Wallet:** Study rewards, task rewards, daily rewards, and game winnings all use the same virtual coin wallet. New wallets start with 100,000 coins, `/daily` grants a random 1,000-5,000 coins every 24 hours, and every casino result is stored in SQLite history tables.

## Runtime Architecture

The bot starts from `bot.py`, uses `services/` for persistence, and loads a fixed set of Discord cogs from `plugins/`. Admin-only commands are protected by Discord administrator permission or the configured bot admin role.

Available admin commands include `/admin setup`, `/admin setup_status`, `/admin game_channels add|remove|list|clear`, `/admin db_status`, `/admin backup`, `/admin reset_all_data`, and `/admin coins add|remove|set`. Runtime plugin management, `/config`, and `/acl` commands are not registered.

Destructive admin reset is available as `/admin reset_all_data` and requires the exact confirmation string `RESET <guild_id>`. It backs up the SQLite database first and resets only the current guild's study/economy/user data, leaving server config and class-role setup intact.

## Casino Commands

All casino features use virtual coins only and have no real-money value.
Admins must set at least one game channel with `/admin game_channels add <channel> [game]` before casino commands will run. Use `game:all` for every game, or assign a channel to `blackjack`, `taixiu`, `slot`, `dice`, `hilo`, or `casino`.

* `!wallet [@member]`, `!balance [@member]`, or `/balance [member]` - view the shared virtual coin wallet.
* `!daily` or `daily` - claim a random 1,000-5,000 coins once every 24 hours.
* `/tasks ideas`, `/tasks preset`, `/tasks add`, and `/tasks done` - complete study tasks for extra coins.
* `/blackjack bet:<amount>`, `!blackjack <amount>`, or `blackjack <amount>` - play Blackjack/Xì Dách with Hit, Stand, Double, and Surrender buttons/reactions.
* `/taixiu`, `!taixiu`, or `taixiu` - open one public Tài Xỉu board. The finished board keeps its result; run the command again to create a new board.
* `!slot <amount>` or `slot <amount>` - play Slot Machine with three weighted emoji reels and Spin/Spin Again buttons.
* `!dice <amount>` or `dice <amount>` - play Dice Duel against the bot with two dice each; ties refund the bet.
* `!hilo <amount>` or `hilo <amount>` - play Hi-Lo from 1-100 with Higher/Lower, Cash Out, and Continue buttons.
* `!casino bet <amount>` or `casino bet <amount>` - set your default Tài Xỉu bet amount.
* `!casino leaderboard` or `casino leaderboard` - show top 10 users by current balance.

Casino limits: minimum bet is 1,000 coins, maximum bet is 1,000,000 coins, and balances are never allowed to go negative.

## Deployment Guide (VPS / Docker)

This repository is fully containerized using Docker, ensuring a streamlined and consistent deployment process across Virtual Private Server (VPS) environments for continuous (24/7) operation.

### Prerequisites

* [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed on the host machine.
* A valid Discord Bot Token, obtainable via the [Discord Developer Portal](https://discord.com/developers/applications).

### Installation Procedure

**1. Clone the Repository**
```bash
git clone [https://github.com/TruongDat05/study-bot-discord.git](https://github.com/TruongDat05/study-bot-discord.git)
cd study-bot-discord
