from __future__ import annotations

import json
import logging
import re
from io import BytesIO
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable

import discord
from discord import app_commands
from discord.ext import commands

from services.database import DatabaseService

log = logging.getLogger(__name__)

KEY_RE = re.compile(r'^[A-Za-z0-9_.-]{1,100}$')
MENTION_ID_RE = re.compile(r'\d{15,25}')

INT_KEYS = {
    'create_room_channel_id',
    'temp_room_category_id',
    'report_channel_id',
    'welcome_channel_id',
    'admin_role_id',
    'coins_per_minute',
    'room_rent_coin_per_minute',
    'schedule_completion_bonus_coins',
}
LIST_INT_KEYS = {'ai_enabled_channels', 'focus_channel_ids'}
LIST_STRING_KEYS = {'autoload_plugins'}
STRING_KEYS = {'notify_channel_mode', 'command_prefix', 'timezone', 'reminder_delivery'}

IMPORTANT_CONFIG_DEFAULTS = {
    'create_room_channel_id': None,
    'temp_room_category_id': None,
    'report_channel_id': None,
    'welcome_channel_id': None,
    'admin_role_id': None,
    'coins_per_minute': 10,
    'ai_enabled_channels': [],
    'notify_channel_mode': 'voice',
    'timezone': 'UTC',
    'reminder_delivery': 'dm',
    'room_rent_coin_per_minute': 2,
    'schedule_completion_bonus_coins': 10,
    'autoload_plugins': [
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
    ],
    'command_prefix': '!',
}


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


class ConfigManager:
    """Database-backed per-guild key-value configuration.

    Older setup commands still write the historical wide ``guild_configs`` row.
    This manager reads those values as fallback and writes new overrides to the
    key-value table. That gives the bot a gradual migration path without losing
    existing server setup data.
    """

    def __init__(
        self,
        database: DatabaseService,
        *,
        legacy_repository=None,
        defaults: dict[str, Any] | None = None,
    ):
        self.database = database
        self.legacy_repository = legacy_repository
        self.defaults = deepcopy(IMPORTANT_CONFIG_DEFAULTS)
        if defaults:
            self.defaults.update(defaults)

    def initialize(self) -> None:
        self.database.initialize()

    def validate_key(self, key: str) -> str:
        key = str(key or '').strip()
        if not KEY_RE.match(key):
            raise ValueError('Config key must be 1-100 chars: letters, numbers, dot, underscore, or dash.')
        lowered = key.lower()
        if any(token in lowered for token in ('token', 'secret', 'password', 'api_key', 'apikey')):
            raise ValueError('Secret-like keys are intentionally blocked from Discord config.')
        return key

    def _legacy_config(self, guild_id: int) -> dict:
        if not self.legacy_repository:
            return {}
        try:
            return self.legacy_repository.get_guild_config(int(guild_id))
        except Exception:
            log.warning('[Config] Could not read legacy guild config for %s', guild_id, exc_info=True)
            return {}

    def _legacy_set(self, guild_id: int, key: str, value: Any) -> None:
        if not self.legacy_repository:
            return
        legacy_keys = {
            'create_room_channel_id',
            'temp_room_category_id',
            'report_channel_id',
            'admin_role_id',
            'coins_per_minute',
            'focus_channel_ids',
        }
        if key in legacy_keys:
            self.legacy_repository.set_guild_config(int(guild_id), key, value)

    def _decode(self, raw: str | None, value_type: str) -> Any:
        if value_type == 'int':
            return int(raw) if raw not in (None, '') else None
        if value_type == 'float':
            return float(raw) if raw not in (None, '') else None
        if value_type == 'bool':
            return str(raw).lower() in ('1', 'true', 'yes', 'on')
        if value_type == 'json':
            if raw in (None, ''):
                return None
            return json.loads(raw)
        if value_type == 'null':
            return None
        return '' if raw is None else str(raw)

    def _encode(self, key: str, value: Any, value_type: str | None = None) -> tuple[str | None, str]:
        if value_type:
            value_type = value_type.lower().strip()
        if value is None:
            return None, 'null'
        if value_type is None:
            value = self.parse_value(key, value)
            if isinstance(value, bool):
                value_type = 'bool'
            elif isinstance(value, int) and not isinstance(value, bool):
                value_type = 'int'
            elif isinstance(value, float):
                value_type = 'float'
            elif isinstance(value, (dict, list)):
                value_type = 'json'
            else:
                value_type = 'string'
        if value_type == 'json':
            return _json_dumps(value), 'json'
        if value_type == 'bool':
            return '1' if bool(value) else '0', 'bool'
        if value_type == 'int':
            return str(int(value)), 'int'
        if value_type == 'float':
            return str(float(value)), 'float'
        return str(value), 'string'

    def parse_value(self, key: str, raw: Any) -> Any:
        key = self.validate_key(key)
        if not isinstance(raw, str):
            return raw
        value = raw.strip()
        if key in INT_KEYS:
            match = MENTION_ID_RE.search(value)
            return int(match.group(0) if match else value)
        if key in LIST_INT_KEYS:
            if not value:
                return []
            if value.startswith('['):
                parsed = json.loads(value)
                return [int(item) for item in parsed]
            return [int(match) for match in MENTION_ID_RE.findall(value)]
        if key in LIST_STRING_KEYS:
            if not value:
                return []
            if value.startswith('['):
                parsed = json.loads(value)
                return [str(item).strip() for item in parsed if str(item).strip()]
            return [item.strip() for item in value.split(',') if item.strip()]
        if value.lower() in ('true', 'false', 'yes', 'no', 'on', 'off'):
            return value.lower() in ('true', 'yes', 'on')
        if value.startswith(('{', '[')):
            return json.loads(value)
        return value

    def get(self, guild_id: int, key: str, default: Any = None) -> Any:
        key = self.validate_key(key)
        self.initialize()
        with self.database.read_connection() as conn:
            row = conn.execute(
                'SELECT value, type FROM guild_config_values WHERE guild_id = ? AND key = ?',
                (int(guild_id), key),
            ).fetchone()
        if row:
            return self._decode(row['value'], row['type'])

        legacy = self._legacy_config(guild_id)
        if key == 'ai_enabled_channels' and legacy.get('focus_channel_ids'):
            return list(legacy.get('focus_channel_ids') or [])
        if key in legacy:
            return legacy[key]
        if key in self.defaults:
            return deepcopy(self.defaults[key])
        return default

    def set(self, guild_id: int, key: str, value: Any, *, updated_by: int | None = None, value_type: str | None = None) -> Any:
        key = self.validate_key(key)
        parsed = self.parse_value(key, value)
        encoded, resolved_type = self._encode(key, parsed, value_type=value_type)
        self.initialize()
        with self.database.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO guild_config_values
                    (guild_id, key, value, type, updated_by, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(guild_id), key, encoded, resolved_type, updated_by, _now()),
            )
        try:
            self._legacy_set(guild_id, key, parsed)
        except Exception:
            log.warning('[Config] Legacy config mirror failed for %s/%s', guild_id, key, exc_info=True)
        return parsed

    def delete(self, guild_id: int, key: str) -> bool:
        key = self.validate_key(key)
        self.initialize()
        with self.database.transaction() as conn:
            cur = conn.execute(
                'DELETE FROM guild_config_values WHERE guild_id = ? AND key = ?',
                (int(guild_id), key),
            )
        if key in INT_KEYS and key != 'coins_per_minute':
            with contextlib_suppress_log('[Config] Legacy clear failed'):
                self._legacy_set(guild_id, key, None)
        elif key == 'focus_channel_ids':
            with contextlib_suppress_log('[Config] Legacy focus clear failed'):
                self._legacy_set(guild_id, key, [])
        elif key == 'coins_per_minute':
            with contextlib_suppress_log('[Config] Legacy coin default failed'):
                self._legacy_set(guild_id, key, self.defaults.get('coins_per_minute', 10))
        return cur.rowcount > 0

    def list(self, guild_id: int) -> dict[str, dict[str, Any]]:
        self.initialize()
        result: dict[str, dict[str, Any]] = {}
        for key, value in self.defaults.items():
            result[key] = {'value': deepcopy(value), 'type': 'default', 'source': 'default', 'updated_by': None, 'updated_at': None}

        legacy = self._legacy_config(guild_id)
        for key, value in legacy.items():
            result[key] = {'value': value, 'type': 'legacy', 'source': 'guild_configs', 'updated_by': None, 'updated_at': None}
        if legacy.get('focus_channel_ids') and 'ai_enabled_channels' not in result:
            result['ai_enabled_channels'] = {
                'value': list(legacy.get('focus_channel_ids') or []),
                'type': 'legacy',
                'source': 'focus_channel_ids',
                'updated_by': None,
                'updated_at': None,
            }

        with self.database.read_connection() as conn:
            rows = conn.execute(
                'SELECT key, value, type, updated_by, updated_at FROM guild_config_values WHERE guild_id = ? ORDER BY key',
                (int(guild_id),),
            ).fetchall()
        for row in rows:
            result[row['key']] = {
                'value': self._decode(row['value'], row['type']),
                'type': row['type'],
                'source': 'guild_config_values',
                'updated_by': row['updated_by'],
                'updated_at': row['updated_at'],
            }
        return dict(sorted(result.items()))

    def export(self, guild_id: int) -> dict[str, Any]:
        return {key: row['value'] for key, row in self.list(guild_id).items()}

    def import_values(self, guild_id: int, values: dict[str, Any], *, updated_by: int | None = None) -> list[str]:
        if not isinstance(values, dict):
            raise ValueError('Config import must be a JSON object.')
        updated: list[str] = []
        for key, value in values.items():
            self.set(guild_id, key, value, updated_by=updated_by)
            updated.append(self.validate_key(key))
        return updated


class contextlib_suppress_log:
    def __init__(self, message: str):
        self.message = message

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None:
            log.warning('%s: %s', self.message, exc)
        return True


def _format_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        text = _json_dumps(value)
    else:
        text = str(value)
    return text if len(text) <= 900 else text[:900] + '...'


class ConfigCog(commands.Cog, name='ConfigCore'):
    """Discord commands for managing per-guild runtime settings."""

    config = app_commands.Group(name='config', description='Quản lý cấu hình server')

    def __init__(
        self,
        manager: ConfigManager,
        *,
        require_admin: Callable[[discord.Interaction, str], Any],
    ):
        self.manager = manager
        self.require_admin = require_admin

    async def _guard(self, interaction: discord.Interaction, action: str) -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        return await self.require_admin(interaction, action)

    @config.command(name='get', description='Xem một giá trị cấu hình')
    @app_commands.describe(key='Tên key cấu hình')
    async def config_get(self, interaction: discord.Interaction, key: str):
        if not await self._guard(interaction, 'config.get'):
            return
        value = self.manager.get(interaction.guild_id, key)
        await interaction.response.send_message(f'`{key}` = `{_format_value(value)}`', ephemeral=True)

    @config.command(name='set', description='Lưu một giá trị cấu hình')
    @app_commands.describe(key='Tên key cấu hình', value='Giá trị mới: số, mention/id, CSV, hoặc JSON')
    async def config_set(self, interaction: discord.Interaction, key: str, value: str):
        if not await self._guard(interaction, 'config.set'):
            return
        try:
            parsed = self.manager.set(interaction.guild_id, key, value, updated_by=interaction.user.id)
        except Exception as e:
            await interaction.response.send_message(f'Không lưu được config: `{e}`', ephemeral=True)
            return
        await interaction.response.send_message(f'Đã lưu `{key}` = `{_format_value(parsed)}`', ephemeral=True)

    @config.command(name='list', description='Liệt kê cấu hình server')
    async def config_list(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'config.list'):
            return
        rows = self.manager.list(interaction.guild_id)
        lines = ['**Config server**']
        for key, row in rows.items():
            lines.append(f'`{key}` = `{_format_value(row["value"])}` ({row["source"]})')
        text = '\n'.join(lines)
        await interaction.response.send_message(text[:1900], ephemeral=True)

    @config.command(name='delete', description='Xóa một key cấu hình')
    @app_commands.describe(key='Tên key cần xóa')
    async def config_delete(self, interaction: discord.Interaction, key: str):
        if not await self._guard(interaction, 'config.delete'):
            return
        removed = self.manager.delete(interaction.guild_id, key)
        status = 'Đã xóa override' if removed else 'Không có override trong key-value config'
        await interaction.response.send_message(f'{status}: `{key}`', ephemeral=True)

    @config.command(name='export', description='Xuất cấu hình server dạng JSON')
    async def config_export(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'config.export'):
            return
        payload = _json_dumps(self.manager.export(interaction.guild_id))
        if len(payload) <= 1900:
            await interaction.response.send_message(f'```json\n{payload}\n```', ephemeral=True)
            return
        file = discord.File(
            BytesIO(payload.encode('utf-8')),
            filename=f'guild_{interaction.guild_id}_config.json',
        )
        await interaction.response.send_message('Config export:', file=file, ephemeral=True)

    @config.command(name='import', description='Import cấu hình từ JSON object an toàn')
    @app_commands.describe(payload='JSON object, ví dụ {"coins_per_minute":10}')
    async def config_import(self, interaction: discord.Interaction, payload: str):
        if not await self._guard(interaction, 'config.import'):
            return
        try:
            raw = json.loads(payload)
            updated = self.manager.import_values(interaction.guild_id, raw, updated_by=interaction.user.id)
        except Exception as e:
            await interaction.response.send_message(f'Import thất bại: `{e}`', ephemeral=True)
            return
        await interaction.response.send_message(f'Đã import `{len(updated)}` key: `{", ".join(updated[:20])}`', ephemeral=True)
