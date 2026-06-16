from __future__ import annotations

import asyncio
import importlib
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)


@dataclass
class PluginFailure:
    plugin: str
    error: str
    when: str


class PluginManager:
    """Load, unload, reload, and list Discord extension plugins.

    Plugins are regular ``discord.py`` extensions under ``plugins/``. A failed
    extension is captured in ``failed`` and never stops the bot process, which
    lets maintainers repair one feature without taking the whole bot offline.
    """

    def __init__(
        self,
        bot: commands.Bot,
        *,
        plugin_package: str = 'plugins',
        plugin_dir: str | Path = 'plugins',
        config_manager=None,
        default_plugins: list[str] | None = None,
    ):
        self.bot = bot
        self.plugin_package = plugin_package
        self.plugin_dir = Path(plugin_dir)
        self.config_manager = config_manager
        self.default_plugins = default_plugins or [
            'ai_chat',
            'study_voice',
            'pomodoro',
            'weekly_report',
            'moderation',
            'notify',
            'economy',
            'loans',
            'rooms',
            'tasklist',
            'schedule',
            'reminders',
            'leaderboard',
        ]
        self.failed: dict[str, PluginFailure] = {}
        self.started_at = datetime.now()
        self._sync_lock = asyncio.Lock()

    def normalize(self, plugin: str) -> str:
        plugin = str(plugin or '').strip().removesuffix('.py').replace('/', '.')
        if plugin.startswith(f'{self.plugin_package}.'):
            return plugin
        return f'{self.plugin_package}.{plugin}'

    def short_name(self, extension: str) -> str:
        return extension.removeprefix(f'{self.plugin_package}.')

    def discover(self) -> list[str]:
        if not self.plugin_dir.exists():
            return []
        return sorted(
            path.stem for path in self.plugin_dir.glob('*.py')
            if path.name != '__init__.py' and not path.name.startswith('_')
        )

    def loaded(self) -> list[str]:
        return sorted(self.short_name(name) for name in self.bot.extensions if name.startswith(f'{self.plugin_package}.'))

    def unloaded(self) -> list[str]:
        loaded = set(self.loaded())
        return [name for name in self.discover() if name not in loaded]

    def status(self) -> dict[str, Any]:
        return {
            'loaded': self.loaded(),
            'unloaded': self.unloaded(),
            'failed': {self.short_name(name): failure.error for name, failure in self.failed.items()},
            'started_at': self.started_at.isoformat(timespec='seconds'),
        }

    def autoload_names(self) -> list[str]:
        configured: list[str] = []
        if self.config_manager:
            for guild in self.bot.guilds:
                try:
                    value = self.config_manager.get(guild.id, 'autoload_plugins')
                    if isinstance(value, str):
                        configured.extend(item.strip() for item in value.split(',') if item.strip())
                    elif isinstance(value, list):
                        configured.extend(str(item).strip() for item in value if str(item).strip())
                except Exception:
                    log.warning('[Plugin] Could not read autoload_plugins for guild %s', guild.id, exc_info=True)
        names = configured or list(self.default_plugins)
        discovered = set(self.discover())
        return [name for name in dict.fromkeys(names) if name in discovered]

    def _restore_guild_commands(self, guild: discord.Guild, commands: list) -> None:
        self.bot.tree.clear_commands(guild=guild)
        for command in commands:
            self.bot.tree.add_command(command, guild=guild, override=True)

    def _prepare_guild_commands_for_sync(self, guild: discord.Guild, *, reason: str) -> bool:
        previous_commands = list(self.bot.tree.get_commands(guild=guild))
        try:
            self.bot.tree.clear_commands(guild=guild)
            self.bot.tree.copy_global_to(guild=guild)
        except Exception:
            log.error('[Plugin] Could not prepare commands for %s after %s', guild.name, reason, exc_info=True)
            try:
                self._restore_guild_commands(guild, previous_commands)
            except Exception:
                log.error('[Plugin] Could not restore previous commands for %s', guild.name, exc_info=True)
            return False

        if not self.bot.tree.get_commands(guild=guild):
            log.error('[Plugin] Refusing to sync zero commands to %s after %s', guild.name, reason)
            try:
                self._restore_guild_commands(guild, previous_commands)
            except Exception:
                log.error('[Plugin] Could not restore previous commands for %s', guild.name, exc_info=True)
            return False

        return True

    async def sync_commands(self, *, reason: str = 'plugin change') -> dict[int, int]:
        """Synchronize slash commands with a lock to avoid overlapping syncs."""
        results: dict[int, int] = {}
        async with self._sync_lock:
            for guild in self.bot.guilds:
                if not self._prepare_guild_commands_for_sync(guild, reason=reason):
                    continue
                try:
                    synced = await self.bot.tree.sync(guild=guild)
                    results[guild.id] = len(synced)
                    log.info('[Plugin] Synced %s commands to %s after %s', len(synced), guild.name, reason)
                except Exception:
                    log.error('[Plugin] Slash command sync failed for %s after %s', guild.name, reason, exc_info=True)
        return results

    async def load(self, plugin: str, *, sync: bool = True) -> tuple[bool, str]:
        extension = self.normalize(plugin)
        if extension in self.bot.extensions:
            return True, f'{self.short_name(extension)} already loaded.'
        try:
            importlib.invalidate_caches()
            await self.bot.load_extension(extension)
            self.failed.pop(extension, None)
            log.info('[Plugin] Loaded %s', extension)
            if sync:
                await self.sync_commands(reason=f'load {self.short_name(extension)}')
            return True, f'Loaded {self.short_name(extension)}.'
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
            self.failed[extension] = PluginFailure(extension, error, datetime.now().isoformat(timespec='seconds'))
            log.error('[Plugin] Failed to load %s: %s', extension, error, exc_info=True)
            return False, error

    async def unload(self, plugin: str, *, sync: bool = True) -> tuple[bool, str]:
        extension = self.normalize(plugin)
        if extension not in self.bot.extensions:
            return False, f'{self.short_name(extension)} is not loaded.'
        try:
            await self.bot.unload_extension(extension)
            log.info('[Plugin] Unloaded %s', extension)
            if sync:
                await self.sync_commands(reason=f'unload {self.short_name(extension)}')
            return True, f'Unloaded {self.short_name(extension)}.'
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
            self.failed[extension] = PluginFailure(extension, error, datetime.now().isoformat(timespec='seconds'))
            log.error('[Plugin] Failed to unload %s: %s', extension, error, exc_info=True)
            return False, error

    async def reload(self, plugin: str, *, sync: bool = True) -> tuple[bool, str]:
        extension = self.normalize(plugin)
        try:
            importlib.invalidate_caches()
            if extension in self.bot.extensions:
                await self.bot.reload_extension(extension)
            else:
                await self.bot.load_extension(extension)
            self.failed.pop(extension, None)
            log.info('[Plugin] Reloaded %s', extension)
            if sync:
                await self.sync_commands(reason=f'reload {self.short_name(extension)}')
            return True, f'Reloaded {self.short_name(extension)}.'
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
            self.failed[extension] = PluginFailure(extension, error, datetime.now().isoformat(timespec='seconds'))
            log.error('[Plugin] Failed to reload %s: %s', extension, error, exc_info=True)
            return False, error

    async def load_autoloaded(self, *, sync: bool = False) -> list[tuple[str, bool, str]]:
        results = []
        for name in self.autoload_names():
            ok, message = await self.load(name, sync=False)
            results.append((name, ok, message))
        if sync:
            await self.sync_commands(reason='autoload')
        return results

    async def reload_all(self) -> list[tuple[str, bool, str]]:
        results = []
        names = self.loaded() or self.autoload_names()
        for name in names:
            ok, message = await self.reload(name, sync=False)
            results.append((name, ok, message))
        await self.sync_commands(reason='reload_all')
        return results


class BotControlCog(commands.Cog, name='BotControlCore'):
    control = app_commands.Group(name='bot', description='Quản lý runtime bot')

    def __init__(
        self,
        manager: PluginManager,
        *,
        require_admin: Callable[[discord.Interaction, str], Any],
        repository=None,
    ):
        self.manager = manager
        self.require_admin = require_admin
        self.repository = repository

    async def _guard(self, interaction: discord.Interaction, action: str) -> bool:
        return await self.require_admin(interaction, action)

    @control.command(name='plugins', description='Liệt kê plugin loaded/unloaded/failed')
    async def plugins(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'bot.plugins'):
            return
        status = self.manager.status()
        failed = status['failed']
        lines = [
            '**Plugins**',
            f'Loaded: `{", ".join(status["loaded"]) or "none"}`',
            f'Unloaded: `{", ".join(status["unloaded"]) or "none"}`',
        ]
        if failed:
            lines.append('Failed:')
            lines.extend(f'`{name}`: `{error}`' for name, error in failed.items())
        else:
            lines.append('Failed: `none`')
        await interaction.response.send_message('\n'.join(lines)[:1900], ephemeral=True)

    @control.command(name='load', description='Load một plugin')
    async def load(self, interaction: discord.Interaction, plugin: str):
        if not await self._guard(interaction, 'bot.load'):
            return
        await interaction.response.defer(ephemeral=True)
        ok, message = await self.manager.load(plugin)
        await interaction.followup.send(('✅ ' if ok else '❌ ') + message, ephemeral=True)

    @control.command(name='unload', description='Unload một plugin')
    async def unload(self, interaction: discord.Interaction, plugin: str):
        if not await self._guard(interaction, 'bot.unload'):
            return
        await interaction.response.defer(ephemeral=True)
        ok, message = await self.manager.unload(plugin)
        await interaction.followup.send(('✅ ' if ok else '❌ ') + message, ephemeral=True)

    @control.command(name='reload', description='Reload một plugin')
    async def reload(self, interaction: discord.Interaction, plugin: str):
        if not await self._guard(interaction, 'bot.reload'):
            return
        await interaction.response.defer(ephemeral=True)
        ok, message = await self.manager.reload(plugin)
        await interaction.followup.send(('✅ ' if ok else '❌ ') + message, ephemeral=True)

    @control.command(name='reload_all', description='Reload tất cả plugin đang loaded')
    async def reload_all(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'bot.reload_all'):
            return
        await interaction.response.defer(ephemeral=True)
        results = await self.manager.reload_all()
        lines = ['**Reload all plugins**']
        lines.extend(f'{"✅" if ok else "❌"} `{name}`: {message}' for name, ok, message in results)
        await interaction.followup.send('\n'.join(lines)[:1900], ephemeral=True)

    @control.command(name='status', description='Xem trạng thái bot/runtime')
    async def status(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'bot.status'):
            return
        plugin_status = self.manager.status()
        lines = [
            '**Bot status**',
            f'Plugins loaded: `{len(plugin_status["loaded"])}` · failed: `{len(plugin_status["failed"])}`',
            f'Started at: `{plugin_status["started_at"]}`',
        ]
        if self.repository:
            try:
                db_status = self.repository.db_status()
                lines.append(f'Database: `{db_status.get("backend")}` rows `{sum(db_status.get("counts", {}).values())}`')
            except Exception as e:
                lines.append(f'Database status error: `{e}`')
        await interaction.response.send_message('\n'.join(lines), ephemeral=True)
