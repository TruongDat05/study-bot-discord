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

The bot starts from `bot.py`, uses `services/` for persistence, and loads a fixed set of Discord cogs from `plugins/`. Admin-only commands are protected by Discord administrator permission or the configured bot admin role.

Available admin commands include `/admin setup`, `/admin setup_status`, `/admin db_status`, `/admin backup`, `/admin reset_all_data`, and `/admin coins add|remove|set`. Runtime plugin management, `/config`, and `/acl` commands are not registered.

Destructive admin reset is available as `/admin reset_all_data` and requires the exact confirmation string `RESET <guild_id>`. It backs up the SQLite database first and resets only the current guild's study/economy/user data, leaving server config and class-role setup intact.

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
