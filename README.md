# Study Bot for Discord

A comprehensive Discord bot developed in Python, designed to facilitate community focus, monitor study duration, and gamify the learning experience. Key functionalities include advanced time tracking, integrated Pomodoro timers, a progression-based leveling system, and a real-time web dashboard.

## Core Features

* **Automated Time Tracking:** Automatically logs study duration when users enable their camera or screen sharing within designated focus channels.
* **Pomodoro System (Individual & Collaborative):** Integrated Pomodoro timers supporting customizable work and break intervals, synchronized group study sessions, and automated experience point (XP) distribution.
* **Gamification Mechanics:** * Experience (XP) and leveling architecture with automated Discord role synchronization.
    * Daily objective tracking, consecutive activity streaks, and unlockable achievement badges.
    * Programmatically generated user profile cards accessible via the `/card` command.
* **Analytics and Reporting:** Daily performance leaderboards, automated weekly progress reports delivered via Direct Message (DM), and granular personal statistics.
* **Real-time Web Dashboard:** Embedded Flask web server providing live updates on active study sessions and user activity heatmaps.
* **Artificial Intelligence Integration:** Capability to query study-related topics utilizing the OpenRouter AI API.

## Runtime Architecture

The bot now has three small core systems under `core/`:

* `core/plugin_manager.py` loads, unloads, reloads, lists, and safely syncs Discord extension plugins from `plugins/`. A failed plugin is logged and recorded without crashing the bot.
* `core/config_manager.py` stores per-guild key-value config in the database and bridges the older `guild_configs` table so existing setup data still works.
* `core/acl.py` stores allow/deny ACL rules by user, role, channel, category, or guild default. Sensitive commands call `acl_check(...)` through the shared bot context.

Plugins live under `plugins/`. Pomodoro, weekly reports, AI chat, and moderation are loaded as cogs now. Economy, loans, temporary rooms, notifications, leaderboard, and study voice tracking have plugin boundary files and remain compatible while their larger legacy implementations are migrated out of `bot.py` incrementally.

New admin command groups:

* `/bot plugins`, `/bot load`, `/bot unload`, `/bot reload`, `/bot reload_all`, `/bot status`
* `/config get`, `/config set`, `/config list`, `/config delete`, `/config export`, `/config import`
* `/acl list`, `/acl allow_user`, `/acl deny_user`, `/acl allow_role`, `/acl deny_role`, `/acl allow_channel`, `/acl deny_channel`, `/acl allow_guild`, `/acl deny_guild`, `/acl remove`, `/acl test`

Destructive admin reset is available as `/admin reset_all_data` and requires the exact confirmation string `RESET <guild_id>`. It backs up the SQLite database first and resets only the current guild's study/economy/user data, leaving config, ACL rules, plugin settings, and role setup intact.

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
