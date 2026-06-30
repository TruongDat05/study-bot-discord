from __future__ import annotations

import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import contextlib
import logging
import os
import signal
import sys                    
import json
import random
import re
import textwrap
import calendar
import sqlite3
import threading
import io
import math
import httpx
import contextvars
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path      
from datetime import datetime, timedelta
from types import SimpleNamespace
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, send_file
from services.database import DatabaseService
from services.repositories import BotRepository
from plugins.games.catalog import GAME_CATALOG, GAME_LABELS, GAME_ORDER

try:
    from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

load_dotenv()

def _env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)

def _env_optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default

TOKEN              = os.getenv('DISCORD_TOKEN')
DATABASE_URL       = os.getenv('DATABASE_URL', 'sqlite:///data/bot.db')
LEGACY_CREATE_ROOM_CHANNEL_ID = _env_optional_int('CREATE_ROOM_CHANNEL_ID')
LEGACY_TEMP_ROOM_CATEGORY_ID  = _env_optional_int('TEMP_ROOM_CATEGORY_ID')
DEFAULT_GUILD_ID   = _env_optional_int('DEFAULT_GUILD_ID')
GEMINI_API_KEY     = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
GROQ_API_KEY       = os.getenv('GROQ_API_KEY')
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
OPENROUTER_REFERER = (
    os.getenv('OPENROUTER_HTTP_REFERER')
    or os.getenv('OPENROUTER_SITE_URL')
    or 'https://localhost'
)
OPENROUTER_TITLE   = os.getenv('OPENROUTER_APP_NAME', 'Study Discord Bot')
HUGGINGFACE_API_KEY = (
    os.getenv('HUGGINGFACE_API_KEY')
    or os.getenv('HUGGING_FACE_API_KEY')
    or os.getenv('HF_TOKEN')
)

GEMINI_FLASH_MODEL      = os.getenv('GEMINI_FLASH_MODEL', 'gemini-3.5-flash')
GEMINI_FLASH_LITE_MODEL = os.getenv('GEMINI_FLASH_LITE_MODEL', 'gemini-3.1-flash-lite')
GROQ_MODELS             = os.getenv('GROQ_MODELS', 'llama-3.3-70b-versatile,llama-3.1-8b-instant')
AI_PROVIDER_ORDER       = os.getenv('AI_PROVIDER_ORDER', 'groq,gemini,openrouter,huggingface')
OPENROUTER_MODEL        = os.getenv('OPENROUTER_MODEL', 'meta-llama/llama-3.3-70b-instruct:free')
HUGGINGFACE_MODEL       = os.getenv('HUGGINGFACE_MODEL', 'mistralai/Mistral-7B-Instruct-v0.3')
AI_HTTP_TIMEOUT         = max(1.0, _env_float('AI_HTTP_TIMEOUT', 60.0))
AI_ONE_MESSAGE_LIMIT    = max(1, min(2000, _env_int('AI_ONE_MESSAGE_LIMIT', 1900) or 1900))
AI_MAX_OUTPUT_TOKENS    = max(1, _env_int('AI_MAX_OUTPUT_TOKENS', 1800) or 1800)
AI_TEMPERATURE          = max(0.0, min(2.0, _env_float('AI_TEMPERATURE', 0.5)))

WARN_BEFORE_KICK    = 10
WAIT_SECONDS        = 60
TEMP_ROOM_DELETE_DELAY_SECONDS = 10
ROLE_SYNC_BATCH_SIZE  = 5
ROLE_SYNC_BATCH_DELAY = 1
REPORT_HOUR         = 23
REPORT_MINUTE       = 0
DAILY_BOARD_HOUR    = 0
DAILY_BOARD_MINUTE  = 0


def _ensure_writable_base_dir() -> Path:
    """Determine the best writable directory for storing application data.
    
    Tries DATA_DIR_ENV first if set, then falls back to local project directory.
    Ensures the directory exists and is writable.
    """
    DATA_DIR_ENV = os.getenv('DATA_DIR')
    
    if DATA_DIR_ENV:
        try:
            data_dir = Path(DATA_DIR_ENV)
            # Try to create the directory if it doesn't exist
            data_dir.mkdir(parents=True, exist_ok=True)
            # Verify writeability by attempting a test write
            test_file = data_dir / '.write_test'
            test_file.touch()
            test_file.unlink()
            return data_dir
        except (OSError, PermissionError, IOError):
            # Fall back to local directory if DATA_DIR is not writable
            pass
    
    # Fall back to script directory (always writable locally)
    return Path(__file__).parent.resolve()

BASE_DIR = _ensure_writable_base_dir()
log_file_path = BASE_DIR / 'bot.log'
DATA_FILE           = BASE_DIR / 'study_data.json'
RUNTIME_STATE_FILE  = BASE_DIR / 'runtime_state.json'
ROLE_CONFIG_FILE    = BASE_DIR / 'role_config.json'
BACKUP_DIR          = BASE_DIR / 'backups'

DASHBOARD_PORT      = _env_int('DASHBOARD_PORT', 5000)
ABSENT_DAYS_WARN    = 2
CHECKPOINT_MINUTES  = 5
LIVE_UPDATE_MINUTES = 5
RUNTIME_RESTORE_MAX_AGE_SECONDS = max(60, _env_int('RUNTIME_RESTORE_MAX_AGE_SECONDS', 1800))

COINS_PER_MINUTE = max(0, _env_int('COINS_PER_MINUTE', 10))
BOT_LOAN_INTEREST_PERCENT = max(0.0, _env_float('BOT_LOAN_INTEREST_PERCENT', 10.0))
BOT_LOAN_DAYS = max(1, _env_int('BOT_LOAN_DAYS', 7))
MAX_BOT_LOAN_AMOUNT = max(1, _env_int('MAX_BOT_LOAN_AMOUNT', 5000))
MAX_ACTIVE_LOANS = max(1, _env_int('MAX_ACTIVE_LOANS', 3))
MAX_PENDING_LOAN_OFFERS = max(1, _env_int('MAX_PENDING_LOAN_OFFERS', 10))
TRANSACTION_HISTORY_LIMIT = max(1, _env_int('TRANSACTION_HISTORY_LIMIT', 100))
LOAN_HISTORY_LIMIT = max(1, _env_int('LOAN_HISTORY_LIMIT', 50))

CLASS_THRESHOLDS = [0, 100, 500, 1500, 5000, 15000, 50000, 100000, 250000, 500000, 1000000]
CLASS_NAMES      = [
    'Newbie', 'Worker', 'Student', 'Trader', 'Rich', 'Elite',
    'Millionaire', 'Tycoon', 'Noble', 'King', 'Legend',
]

# Backward-compatible aliases for existing role maps and persisted `level` fields.
LEVEL_THRESHOLDS = CLASS_THRESHOLDS
LEVEL_NAMES = CLASS_NAMES

CLASS_ROLE_NAMES: dict[int, str] = {
    1: 'Class 1 Newbie',
    2: 'Class 2 Student',
    3: 'Class 3 Worker',
    4: 'Class 4 Trader',
    5: 'Class 5 Rich',
    6: 'Class 6 Elite',
    7: 'Class 7 Millionaire',
    8: 'Class 8 Tycoon',
    9: 'Class 9 Noble',
    10: 'Class 10 Legend',
}

database = DatabaseService(DATABASE_URL)
repository = BotRepository(database, default_coins_per_minute=COINS_PER_MINUTE)
_database_initialized = False
_guild_context: contextvars.ContextVar[int | None] = contextvars.ContextVar('guild_id', default=None)

CORE_CONFIG_KEYS = {
    'create_room_channel_id',
    'temp_room_category_id',
    'report_channel_id',
    'admin_role_id',
    'coins_per_minute',
    'focus_channel_ids',
}

CONFIG_DEFAULTS = {
    'ai_enabled_channels': [],
    'autoload_plugins': [],
    'command_prefix': '!',
    'game_channel_ids': [],
    'game_channel_map': {},
    'notify_channel_mode': 'dm',
    'reminder_delivery': 'dm',
    'room_rent_coin_per_minute': 2,
    'schedule_completion_bonus_coins': 10,
    'timezone': os.getenv('TZ') or 'UTC',
    'welcome_channel_id': None,
}

STARTUP_EXTENSIONS = [
    'plugins.casino',
    'plugins.ai_chat',
    'plugins.study_voice',
    'plugins.weekly_report',
    'plugins.moderation',
    'plugins.notify',
    'plugins.economy',
    'plugins.loans',
    'plugins.rooms',
    'plugins.tasklist',
    'plugins.schedule',
    'plugins.leaderboard',
]


class GuildConfigManager:
    """Tiny compatibility layer over repository config and guild_config_values."""

    def __init__(self, repository: BotRepository):
        self.repository = repository
        self.defaults = CONFIG_DEFAULTS

    def initialize(self):
        self.repository.initialize()

    @staticmethod
    def _decode_value(raw: str | None, value_type: str, default=None):
        if raw is None:
            return default
        try:
            if value_type == 'json':
                return json.loads(raw)
            if value_type == 'int':
                return int(raw)
            if value_type == 'float':
                return float(raw)
            if value_type == 'bool':
                return str(raw).lower() in {'1', 'true', 'yes', 'on'}
        except (TypeError, ValueError, json.JSONDecodeError):
            return default
        return raw

    @staticmethod
    def _encode_value(value) -> tuple[str | None, str]:
        if value is None:
            return None, 'none'
        if isinstance(value, bool):
            return '1' if value else '0', 'bool'
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value), 'int'
        if isinstance(value, float):
            return str(value), 'float'
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, separators=(',', ':')), 'json'
        return str(value), 'string'

    def get(self, guild_id: int, key: str, default=None):
        guild_id = int(guild_id)
        fallback = self.defaults.get(key, default)
        self.repository.initialize()
        if key in CORE_CONFIG_KEYS:
            return self.repository.get_guild_config(guild_id).get(key, fallback)
        with database.read_connection() as conn:
            row = conn.execute(
                'SELECT value, type FROM guild_config_values WHERE guild_id = ? AND key = ?',
                (guild_id, key),
            ).fetchone()
        if not row:
            return fallback
        return self._decode_value(row['value'], row['type'], fallback)

    def set(self, guild_id: int, key: str, value, *, updated_by: int | None = None):
        guild_id = int(guild_id)
        self.repository.initialize()
        if key in CORE_CONFIG_KEYS:
            return self.repository.set_guild_config(guild_id, key, value)
        raw, value_type = self._encode_value(value)
        now = datetime.now().isoformat(timespec='seconds')
        with database.transaction() as conn:
            conn.execute(
                """
                INSERT INTO guild_config_values (guild_id, key, value, type, updated_by, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, key) DO UPDATE SET
                    value = excluded.value,
                    type = excluded.type,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                """,
                (guild_id, key, raw, value_type, updated_by, now),
            )
        return value

# ─── QUEST CONFIG ────────────────────────────────────────────────────────────

QUEST_POOL = [
    {'id': 'study_30',     'desc': 'Học đủ 30 phút hôm nay',          'target': 30,  'type': 'minutes',    'coins': 50,  'emoji': '⏱️'},
    {'id': 'study_60',     'desc': 'Học đủ 1 tiếng hôm nay',          'target': 60,  'type': 'minutes',    'coins': 100, 'emoji': '🕐'},
    {'id': 'study_120',    'desc': 'Học đủ 2 tiếng hôm nay',          'target': 120, 'type': 'minutes',    'coins': 200, 'emoji': '🔥'},
    {'id': 'study_180',    'desc': 'Học đủ 3 tiếng hôm nay',          'target': 180, 'type': 'minutes',    'coins': 350, 'emoji': '💎'},
    {'id': 'streak_3',     'desc': 'Duy trì streak 3 ngày liên tiếp',  'target': 3,   'type': 'streak',     'coins': 80,  'emoji': '📅'},
    {'id': 'streak_7',     'desc': 'Duy trì streak 7 ngày liên tiếp',  'target': 7,   'type': 'streak',     'coins': 200, 'emoji': '🗓️'},
    {'id': 'early_bird',   'desc': 'Vào phòng học trước 8h sáng',      'target': 8,   'type': 'hour_before','coins': 75,  'emoji': '🌅'},
    {'id': 'night_owl',    'desc': 'Học sau 22h tối',                  'target': 22,  'type': 'hour_after', 'coins': 75,  'emoji': '🦉'},
    {'id': 'first_in',     'desc': 'Người đầu tiên vào phòng hôm nay', 'target': 1,   'type': 'first_in',   'coins': 60,  'emoji': '🥇'},
    {'id': 'two_sessions', 'desc': 'Học ít nhất 2 phiên trong ngày',   'target': 2,   'type': 'sessions',   'coins': 80,  'emoji': '🔄'},
]

QUEST_DAILY_COUNT = 3

# ─── BADGE CONFIG ─────────────────────────────────────────────────────────────

BADGES: dict[str, dict] = {
    'streak_3':    {'name': 'Khởi đầu 🌱',      'desc': 'Streak 3 ngày',       'condition': ('streak', 3)},
    'streak_7':    {'name': 'Tuần lễ 📅',        'desc': 'Streak 7 ngày',       'condition': ('streak', 7)},
    'streak_14':   {'name': 'Hai tuần 💪',        'desc': 'Streak 14 ngày',      'condition': ('streak', 14)},
    'streak_30':   {'name': 'Một tháng 🔥',       'desc': 'Streak 30 ngày',      'condition': ('streak', 30)},
    'streak_100':  {'name': 'Huyền thoại 👑',     'desc': 'Streak 100 ngày',     'condition': ('streak', 100)},
    'total_1h':    {'name': 'Bước đầu ⏱️',       'desc': 'Tổng cộng 1 giờ',    'condition': ('total_hours', 1)},
    'total_10h':   {'name': 'Chăm chỉ 📚',        'desc': 'Tổng cộng 10 giờ',   'condition': ('total_hours', 10)},
    'total_50h':   {'name': 'Kiên trì ✨',         'desc': 'Tổng cộng 50 giờ',   'condition': ('total_hours', 50)},
    'total_100h':  {'name': 'Trăm giờ 💯',        'desc': 'Tổng cộng 100 giờ',  'condition': ('total_hours', 100)},
    'total_500h':  {'name': 'Bậc thầy ⚡',        'desc': 'Tổng cộng 500 giờ',  'condition': ('total_hours', 500)},
    'marathon_4h': {'name': 'Marathon 🏃',        'desc': 'Học 4 tiếng 1 ngày', 'condition': ('daily_hours', 4)},
    'marathon_8h': {'name': 'Siêu marathon 🚀',   'desc': 'Học 8 tiếng 1 ngày', 'condition': ('daily_hours', 8)},
    'level_5':     {'name': 'Elite Class ⭐',      'desc': 'Đạt class Elite',     'condition': ('class', 5)},
    'level_10':    {'name': 'Legend Class 👑',     'desc': 'Đạt class Legend',    'condition': ('class', 10)},
    'early_bird':  {'name': 'Cú sáng ☀️',        'desc': 'Học trước 8h sáng',   'condition': ('special', 'early_bird')},
    'night_owl':   {'name': 'Cú đêm 🦉',          'desc': 'Học sau 0h đêm',      'condition': ('special', 'night_owl')},
    'quest_10':    {'name': 'Người thực hiện 📋', 'desc': 'Hoàn thành 10 quest', 'condition': ('quests_done', 10)},
    'quest_50':    {'name': 'Siêu nhiệm vụ 🎯',   'desc': 'Hoàn thành 50 quest', 'condition': ('quests_done', 50)},
    'xp_1000':     {'name': '1.000 coins 💰',     'desc': 'Kiếm 1.000 coins',    'condition': ('total_earned', 1000)},
    'xp_10000':    {'name': '10.000 coins 💎',    'desc': 'Kiếm 10.000 coins',   'condition': ('total_earned', 10000)},
}

NOTIFY_GREEN = 0x2ECC71
NOTIFY_GOLD = 0xF1C40F
NOTIFY_RED = 0xE74C3C
NOTIFY_BLUE = 0x5865F2
NOTIFY_PURPLE = 0x9B59B6

STUDY_MILESTONE_SECONDS = [
    3600,
    7200,
    18000,
    36000,
    72000,
    180000,
    360000,
    720000,
    1800000,
]
COIN_EARNING_MILESTONES = [1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]

if not TOKEN:
    raise ValueError('Không tìm thấy DISCORD_TOKEN trong file .env!')

backup_dir_warning: str | None = None

# ✅ Ensure directories exist and are writable
try:
    BACKUP_DIR.mkdir(exist_ok=True, parents=True)
except (OSError, PermissionError, IOError) as e:
    # If BACKUP_DIR creation fails, ensure at least BASE_DIR exists
    try:
        BASE_DIR.mkdir(exist_ok=True, parents=True)
    except Exception:
        pass
    backup_dir_warning = (
        f'Warning: Could not create BACKUP_DIR at {BACKUP_DIR}: {e}. '
        f'Using {BASE_DIR} as fallback.'
    )

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),  # ✅ Use variable
        logging.StreamHandler(sys.stdout),                      # ✅ Explicit stdout
    ],
)
log = logging.getLogger(__name__)
if backup_dir_warning:
    log.warning(backup_dir_warning)

#  Suppress noisy libraries
logging.getLogger('discord').setLevel(logging.WARNING)
logging.getLogger('discord.http').setLevel(logging.WARNING)

# ─── BOT SETUP ───────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.voice_states    = True
intents.members         = True
intents.message_content = True

def _command_prefix_resolver(client: commands.Bot, message: discord.Message):
    if getattr(message, 'guild', None):
        try:
            return str(get_guild_config(message.guild.id).get('command_prefix') or '!')
        except Exception:
            log.warning('[Config] Failed to resolve command_prefix for %s', message.guild.id, exc_info=True)
    return '!'

bot = commands.Bot(command_prefix=_command_prefix_resolver, intents=intents, help_command=None)

config_manager = GuildConfigManager(repository)

def _capture_guild_context(guild_id: int | None):
    if guild_id:
        _guild_context.set(int(guild_id))


async def _capture_app_command_guild_context(interaction: discord.Interaction) -> bool:
    _capture_guild_context(interaction.guild_id)
    return True

bot.tree.interaction_check = _capture_app_command_guild_context


@bot.before_invoke
async def _capture_prefix_command_guild_context(ctx):
    guild = getattr(ctx, 'guild', None)
    _capture_guild_context(guild.id if guild else None)


# ─── STATE ───────────────────────────────────────────────────────────────────

pending_checks:       dict[int, asyncio.Task] = {}
join_times:           dict[int, datetime]     = {}
last_checkpoint:      dict[int, datetime]     = {}
milestone_sent:       dict[int, set]          = {}
runtime_member_guild_ids: dict[int, int]      = {}
live_message_ids:     dict[int, int]          = {}
daily_first_join:     dict[str, int]          = {}
session_counts:       dict[int, int]          = {}
daily_board_sent:     set                     = set()
report_sent_today:    set                     = set()
remind_tasks:         dict[int, tuple]        = {}
media_active_members: set                     = set()
cam_thanks_sent:     set[int]                 = set()
temp_rooms:           dict[str, dict]         = {}
temporary_room_delete_tasks: dict[int, asyncio.Task] = {}
_role_sync_locks:     dict[int, asyncio.Lock] = {}
_dashboard_started:   bool                    = False
_room_panel_view_registered: bool             = False
_startup_extensions_ready: bool               = False
_core_cogs_ready: bool                        = False

_data_lock = threading.RLock()
_runtime_lock = threading.Lock()
_last_data_save_success: bool = True

# ─── MEDIA HELPERS ───────────────────────────────────────────────────────────

def is_media_active(vs: discord.VoiceState) -> bool:
    return bool(vs.self_video or vs.self_stream)

def media_status_icon(vs: discord.VoiceState) -> str:
    if vs.self_video and vs.self_stream: return '📷📺'
    if vs.self_video:  return '📷'
    if vs.self_stream: return '📺'
    return '⏸️'

# ─── DATA HELPERS ────────────────────────────────────────────────────────────

def initialize_database():
    global _database_initialized
    if _database_initialized:
        return
    repository.initialize()
    _database_initialized = True


def _current_guild_id(default: int | None = None) -> int:
    guild_id = _guild_context.get()
    if guild_id:
        return int(guild_id)
    if default:
        return int(default)
    if DEFAULT_GUILD_ID:
        return int(DEFAULT_GUILD_ID)
    if bot.guilds and len(bot.guilds) == 1:
        return int(bot.guilds[0].id)
    return 0


@contextmanager
def guild_data_context(guild_id: int | None):
    token = _guild_context.set(int(guild_id) if guild_id else None)
    try:
        yield
    finally:
        _guild_context.reset(token)


def get_guild_config(guild_id: int) -> dict:
    initialize_database()
    config = repository.get_guild_config(int(guild_id))
    for key in (
        'create_room_channel_id',
        'temp_room_category_id',
        'report_channel_id',
        'welcome_channel_id',
        'admin_role_id',
        'coins_per_minute',
        'focus_channel_ids',
        'ai_enabled_channels',
        'notify_channel_mode',
        'autoload_plugins',
        'command_prefix',
    ):
        try:
            value = config_manager.get(int(guild_id), key)
        except Exception:
            log.warning('[Config] Failed to resolve key %s for guild %s', key, guild_id, exc_info=True)
            continue
        if value is not None:
            config[key] = value
    return config


def set_guild_config(guild_id: int, key: str, value):
    initialize_database()
    return repository.set_guild_config(int(guild_id), key, value)


def save_guild_config(guild_id: int, config: dict) -> dict:
    initialize_database()
    return repository.save_guild_config(int(guild_id), config)


def require_guild_config(interaction_or_guild) -> tuple[dict | None, str | None]:
    guild = getattr(interaction_or_guild, 'guild', interaction_or_guild)
    if guild is None:
        return None, 'Lệnh này chỉ dùng được trong server.'
    config = get_guild_config(guild.id)
    missing = [
        label for key, label in (
            ('create_room_channel_id', 'create-room voice channel'),
            ('temp_room_category_id', 'temporary room category'),
            ('report_channel_id', 'report channel'),
        )
        if not config.get(key)
    ]
    if missing:
        return config, f'Chưa setup: {", ".join(missing)}. Dùng `/admin setup` trước.'
    return config, None


def _configured_guild_ids() -> set[int]:
    initialize_database()
    ids = {int(cfg['guild_id']) for cfg in repository.list_guild_configs()}
    ids.update(guild.id for guild in bot.guilds)
    return ids


def coins_per_minute_for(guild_id: int | None = None) -> int:
    gid = _guild_data_id(guild_id)
    if not gid:
        return COINS_PER_MINUTE
    return max(0, _as_int(get_guild_config(gid).get('coins_per_minute'), COINS_PER_MINUTE))


def _guild_data_id(guild_id: int | None = None) -> int:
    return _current_guild_id(guild_id)


# Synchronous data boundary shared by Discord tasks and the Flask dashboard.
# Keep work inside _data_lock limited to local repository transactions only;
# do not add network calls or long CPU work here, or async handlers can stall.
def load_data(guild_id: int | None = None) -> dict:
    initialize_database()
    gid = _guild_data_id(guild_id)
    if not gid:
        return {}
    with _data_lock:
        data = repository.load_guild_data(gid)
        return _normalize_all_users(data)


def save_data(data: dict, guild_id: int | None = None):
    global _last_data_save_success
    initialize_database()
    gid = _guild_data_id(guild_id)
    if not gid:
        log.error('[Data] Cannot save without guild context.')
        _last_data_save_success = False
        return
    with _data_lock:
        _last_data_save_success = False
        _normalize_all_users(data)
        repository.save_guild_data(gid, data)
        _last_data_save_success = True


def update_data(mutator, guild_id: int | None = None):
    """
    Thread-safe DB-backed update that persists changes atomically
    and returns a deepcopy of the saved state.
    """
    global _last_data_save_success
    initialize_database()
    gid = _guild_data_id(guild_id)
    if not gid:
        log.error('[Data] Cannot update without guild context.')
        return mutator({}), {}
    with _data_lock:
        _last_data_save_success = False
        result, data = repository.update_guild_data(gid, mutator, _normalize_all_users)
        _last_data_save_success = True
        return result, data


def load_all_guild_data() -> dict[int, dict]:
    return {guild_id: load_data(guild_id) for guild_id in _configured_guild_ids()}

def _serialize_dt(dt: datetime) -> str:
    return dt.isoformat()

def _parse_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

def _temp_room_key(guild_id: int, channel_id: int) -> str:
    return f'{int(guild_id)}:{int(channel_id)}'

def _temp_room_key_for_channel_id(channel_id: int | None, guild_id: int | None = None) -> str | None:
    if not channel_id:
        return None
    if guild_id:
        key = _temp_room_key(guild_id, channel_id)
        if key in temp_rooms:
            return key
    for key, meta in list(temp_rooms.items()):
        if _as_int(meta.get('room_id')) == int(channel_id):
            if guild_id and _as_int(meta.get('guild_id')) != int(guild_id):
                continue
            return key
    return None

def _temp_room_meta(channel_id: int | None, guild_id: int | None = None) -> dict:
    key = _temp_room_key_for_channel_id(channel_id, guild_id)
    return temp_rooms.get(key, {}) if key else {}

def _serialize_temp_rooms_snapshot(guild_id: int | None = None) -> dict:
    snapshot: dict[str, dict] = {}
    for key, meta in list(temp_rooms.items()):
        room_id = _as_int(meta.get('room_id'))
        room_guild_id = _as_int(meta.get('guild_id'))
        if guild_id and room_guild_id != int(guild_id):
            continue
        if not room_id or not room_guild_id:
            continue
        created_at = meta.get('created_at')
        if isinstance(created_at, datetime):
            created_at_str = _serialize_dt(created_at)
        else:
            created_at_str = str(created_at or '')
        snapshot[key] = {
            'room_id': room_id,
            'owner_id': _as_int(meta.get('owner_id')),
            'guild_id': room_guild_id,
            'created_at': created_at_str,
            'mode': _room_mode(meta.get('mode')),
        }
    return snapshot

def _restore_temp_rooms_from_snapshot(raw: dict, guild_id: int | None = None):
    restored = 0

    rooms = raw.get('temp_rooms', {}) if isinstance(raw, dict) else {}
    if not isinstance(rooms, dict):
        return

    for room_key, meta in rooms.items():
        if not isinstance(meta, dict):
            continue
        try:
            room_id = int(str(room_key).split(':')[-1])
        except (TypeError, ValueError):
            continue

        channel = bot.get_channel(room_id)
        if not isinstance(channel, discord.VoiceChannel):
            continue

        created_at = _parse_dt(str(meta.get('created_at', ''))) or datetime.now()
        room_guild_id = _as_int(meta.get('guild_id'), guild_id or channel.guild.id)
        if guild_id and room_guild_id != int(guild_id):
            continue
        temp_rooms[_temp_room_key(room_guild_id, room_id)] = {
            'room_id': room_id,
            'owner_id': _as_int(meta.get('owner_id')),
            'guild_id': room_guild_id,
            'created_at': created_at,
            'mode': _room_mode(meta.get('mode')),
        }
        restored += 1

    if restored:
        log.info(f'[TempRoom] Restored {restored} tracked temporary rooms.')

def _runtime_guild_id_for_member(member_id: int) -> int | None:
    member_id = int(member_id)
    guild_id = runtime_member_guild_ids.get(member_id)
    if guild_id:
        return int(guild_id)

    for guild in bot.guilds:
        member = guild.get_member(member_id)
        if member and member.voice and member.voice.channel:
            runtime_member_guild_ids[member_id] = guild.id
            return guild.id

    for guild in bot.guilds:
        if guild.get_member(member_id):
            runtime_member_guild_ids[member_id] = guild.id
            return guild.id
    return None

def _runtime_member_ids_for_guild(guild_id: int) -> set[int]:
    ids = set(join_times.keys())
    ids.update(last_checkpoint.keys())
    ids.update(milestone_sent.keys())
    ids.update(media_active_members)
    ids.update(session_counts.keys())
    ids.update(mid for mid in daily_first_join.values() if isinstance(mid, int))
    return {
        int(mid)
        for mid in ids
        if _runtime_guild_id_for_member(int(mid)) == int(guild_id)
    }

def _runtime_guild_ids_for_save() -> set[int]:
    guild_ids: set[int] = set()
    try:
        guild_ids.update(_configured_guild_ids())
    except Exception as e:
        log.error(f'[Runtime] Could not read configured guild ids: {e}', exc_info=True)
    guild_ids.update(int(gid) for gid in runtime_member_guild_ids.values() if gid)
    for meta in temp_rooms.values():
        guild_id = _as_int(meta.get('guild_id'))
        if guild_id:
            guild_ids.add(guild_id)
    for member_id in set(join_times) | set(last_checkpoint) | set(milestone_sent) | set(media_active_members):
        guild_id = _runtime_guild_id_for_member(member_id)
        if guild_id:
            guild_ids.add(guild_id)
    return {int(gid) for gid in guild_ids if gid}

def _runtime_snapshot_for_guild(guild_id: int, now: datetime) -> dict:
    member_ids = _runtime_member_ids_for_guild(guild_id)
    return {
        'guild_id': int(guild_id),
        'saved_at': now.strftime('%Y-%m-%d'),
        'saved_at_ts': _serialize_dt(now),
        'join_times': {
            str(mid): _serialize_dt(ts)
            for mid, ts in join_times.items()
            if mid in member_ids
        },
        'last_checkpoint': {
            str(mid): _serialize_dt(ts)
            for mid, ts in last_checkpoint.items()
            if mid in member_ids
        },
        'milestone_sent': {
            str(mid): sorted(list(ms))
            for mid, ms in milestone_sent.items()
            if mid in member_ids
        },
        'daily_first_join': {
            d: int(mid)
            for d, mid in daily_first_join.items()
            if isinstance(mid, int) and mid in member_ids
        },
        'session_counts': {
            str(mid): int(cnt)
            for mid, cnt in session_counts.items()
            if mid in member_ids
        },
        'media_active_members': sorted(mid for mid in media_active_members if mid in member_ids),
        'temp_rooms': _serialize_temp_rooms_snapshot(guild_id),
    }

def save_runtime_state() -> bool:
    now = datetime.now()
    with _runtime_lock:
        try:
            initialize_database()
            guild_ids = _runtime_guild_ids_for_save()
            if not guild_ids:
                log.debug('[Runtime] No guilds available for runtime snapshot.')
                return True
            for guild_id in sorted(guild_ids):
                repository.save_runtime_state(guild_id, _runtime_snapshot_for_guild(guild_id, now))
            return True
        except sqlite3.OperationalError as e:
            log.error(f'[Runtime] SQLite operational error while saving runtime state: {e}', exc_info=True)
        except sqlite3.DatabaseError as e:
            log.error(f'[Runtime] SQLite database error while saving runtime state: {e}', exc_info=True)
        except IOError as e:
            log.error(f'Lỗi lưu runtime state: {e}', exc_info=True)
        except Exception as e:
            log.error(f'Lỗi không xác định khi lưu runtime state: {e}', exc_info=True)
    return False

def load_runtime_states() -> dict[int, dict]:
    with _runtime_lock:
        try:
            initialize_database()
            states = repository.load_runtime_states()
            real_states = {gid: state for gid, state in states.items() if gid}
            if real_states:
                return real_states
            if 0 in states:
                return {0: states[0]}
        except Exception as e:
            log.error(f'Lỗi đọc runtime state từ DB: {e}', exc_info=True)

        try:
            if RUNTIME_STATE_FILE.exists():
                with open(RUNTIME_STATE_FILE, 'r', encoding='utf-8') as f:
                    return {0: json.load(f)}
        except (json.JSONDecodeError, IOError) as e:
            log.error(f'Lỗi đọc runtime state: {e}')
    return {}

def load_runtime_state() -> dict:
    states = load_runtime_states()
    if 0 in states:
        return states[0]
    return next(iter(states.values()), {})

def restore_runtime_state():
    states = load_runtime_states()
    if not states:
        return

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')

    restored_join: dict[int, datetime] = {}
    restored_checkpoint: dict[int, datetime] = {}
    restored_milestones: dict[int, set] = {}
    restored_media: set[int] = set()
    restored_sessions: dict[int, int] = {}
    restored_first_join: dict[str, int] = {}

    temp_rooms.clear()
    runtime_member_guild_ids.clear()

    for source_guild_id, raw in states.items():
        if not isinstance(raw, dict):
            continue
        guild_id = None if source_guild_id == 0 else int(source_guild_id)
        saved_at = raw.get('saved_at')
        same_day = saved_at == today
        saved_at_ts = _parse_dt(str(raw.get('saved_at_ts', '')))
        session_restore_allowed = same_day
        if saved_at_ts is not None:
            age_seconds = (now - saved_at_ts).total_seconds()
            session_restore_allowed = 0 <= age_seconds <= RUNTIME_RESTORE_MAX_AGE_SECONDS
        elif not same_day:
            session_restore_allowed = False

        _restore_temp_rooms_from_snapshot(raw, guild_id)

        if not session_restore_allowed:
            log.info(f'[Runtime] Stored voice sessions are stale for guild_id={source_guild_id}; current voice members will start fresh.')

        local_join: dict[int, datetime] = {}
        local_checkpoint: dict[int, datetime] = {}

        if not session_restore_allowed:
            if same_day:
                for mid_str, cnt in raw.get('session_counts', {}).items():
                    try:
                        mid = int(mid_str)
                        cnt_i = int(cnt)
                    except (ValueError, TypeError):
                        continue
                    if cnt_i > 0:
                        restored_sessions[mid] = cnt_i
                        if guild_id:
                            runtime_member_guild_ids[mid] = guild_id
                for d, mid in raw.get('daily_first_join', {}).items():
                    if isinstance(d, str) and isinstance(mid, int):
                        restored_first_join[d] = mid
                        if guild_id:
                            runtime_member_guild_ids[mid] = guild_id
            continue

        for mid_str, ts_str in raw.get('join_times', {}).items():
            try:
                mid = int(mid_str)
            except (ValueError, TypeError):
                continue
            ts = _parse_dt(ts_str)
            if ts is None:
                continue
            if ts > now:
                ts = now
            local_join[mid] = ts
            restored_join[mid] = ts
            if guild_id:
                runtime_member_guild_ids[mid] = guild_id

        for mid_str, ts_str in raw.get('last_checkpoint', {}).items():
            try:
                mid = int(mid_str)
            except (ValueError, TypeError):
                continue
            if mid not in local_join:
                continue
            ts = _parse_dt(ts_str)
            if ts is None:
                continue
            if ts < local_join[mid]:
                ts = local_join[mid]
            if ts > now:
                ts = now
            local_checkpoint[mid] = ts
            restored_checkpoint[mid] = ts

        for mid in local_join:
            restored_checkpoint.setdefault(mid, local_join[mid])

        for mid_str, vals in raw.get('milestone_sent', {}).items():
            try:
                mid = int(mid_str)
            except (ValueError, TypeError):
                continue
            if mid not in restored_join:
                continue
            if not isinstance(vals, list):
                continue
            clean = {int(v) for v in vals if isinstance(v, int)}
            restored_milestones[mid] = clean

        for mid in raw.get('media_active_members', []):
            if isinstance(mid, int) and mid in restored_join:
                restored_media.add(mid)

        if same_day:
            for mid_str, cnt in raw.get('session_counts', {}).items():
                try:
                    mid = int(mid_str)
                    cnt_i = int(cnt)
                except (ValueError, TypeError):
                    continue
                if cnt_i > 0:
                    restored_sessions[mid] = cnt_i
                    if guild_id:
                        runtime_member_guild_ids[mid] = guild_id
            for d, mid in raw.get('daily_first_join', {}).items():
                if isinstance(d, str) and isinstance(mid, int):
                    restored_first_join[d] = mid
                    if guild_id:
                        runtime_member_guild_ids[mid] = guild_id

    join_times.clear()
    join_times.update(restored_join)
    last_checkpoint.clear()
    last_checkpoint.update(restored_checkpoint)
    milestone_sent.clear()
    milestone_sent.update(restored_milestones)
    media_active_members.clear()
    media_active_members.update(restored_media)
    session_counts.clear()
    session_counts.update(restored_sessions)
    daily_first_join.clear()
    daily_first_join.update(restored_first_join)

    log.info(
        f'[Runtime] Restored {len(join_times)} phiên across {len(states)} snapshot(s), '
        f'{len(media_active_members)} đang active media.'
    )

def backup_data():
    BACKUP_DIR.mkdir(exist_ok=True, parents=True)
    try:
        dest = repository.backup_db(BACKUP_DIR)
        files = sorted(
            BACKUP_DIR.glob('bot_db_*.sqlite3'),
            key=lambda p: p.stat().st_mtime
        )
        for old in files[:-30]:
            old.unlink()
        log.info(f'[Backup] Saved → {dest}')
    except Exception as e:
        log.error(f'[Backup] Error: {e}')

def _default_user(name: str) -> dict:
    return {
        'name': name,
        'daily': {},
        'daily_earnings': {},
        'total': 0,
        'balance': 100000,
        'total_earned': 0,
        'debt': 0,
        'net_worth': 0,
        'class': 0,
        'xp': 0,
        'level': 0,
        'streak': 0,
        'longest_streak': 0,
        'last_study_date': '',
        'goal': None,
        'goal_seconds': 0,
        'last_absent_warn': '',
        'xp_acc_secs': 0,
        'coins_acc_secs': 0,
        'transactions': [],
        'active_loans': [],
        'loan_offers': [],
        'loan_history': [],
        'credit_score': 600,
        'notifications_enabled': True,
        'notified_classes': [],
        'notified_study_milestones': [],
        'notified_coin_milestones': [],
        'notified_loan_overdue': [],
        'badges': [],
        'badge_dates': {},
        'quests_done_total': 0,
        'daily_quests': {},
        'special_flags': [],
        'remind_hour': None,
    }

def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def get_money_class(amount: int) -> int:
    amount = max(0, _as_int(amount))
    for i in range(len(CLASS_THRESHOLDS) - 1, -1, -1):
        if amount >= CLASS_THRESHOLDS[i]:
            return i
    return 0

def coins_to_next_class(amount: int) -> tuple[int, int]:
    class_idx = get_money_class(amount)
    if class_idx >= len(CLASS_THRESHOLDS) - 1:
        return class_idx, 0
    return class_idx, max(0, CLASS_THRESHOLDS[class_idx + 1] - amount)

def get_level(amount: int) -> int:
    """Compatibility shim: persisted `level` now means money class."""
    return get_money_class(amount)

def xp_to_next_level(amount: int) -> tuple[int, int]:
    """Compatibility shim for older callers; returns class progress in coins."""
    return coins_to_next_class(amount)

def format_coins(amount: int | float) -> str:
    try:
        amount_i = int(round(float(amount)))
    except (TypeError, ValueError):
        amount_i = 0
    return f'{amount_i:,} coins'

def _active_loans(info: dict) -> list[dict]:
    loans = []
    for loan in info.get('active_loans', []):
        if not isinstance(loan, dict):
            continue
        remaining = _as_int(loan.get('remaining', loan.get('total_due', 0)))
        if remaining > 0 and loan.get('status', 'active') == 'active':
            loan['remaining'] = remaining
            loans.append(loan)
    return loans

def _active_debt(info: dict) -> int:
    return sum(_as_int(loan.get('remaining', 0)) for loan in _active_loans(info))

def _sync_money_class(info: dict) -> int:
    class_idx = get_money_class(info.get('total_earned', 0))
    info['class'] = class_idx
    # Keep old `level` consumers working, but derive it from money now.
    info['level'] = class_idx
    debt = _active_debt(info)
    info['debt'] = debt
    info['net_worth'] = _as_int(info.get('balance', 0)) - debt
    return class_idx

def _normalize_user_record(uid: str, info: dict) -> dict:
    name = info.get('name') if isinstance(info, dict) else None
    if not isinstance(info, dict):
        info = {}
    defaults = _default_user(name or f'User {uid}')
    for key, value in defaults.items():
        if key not in info:
            info[key] = deepcopy(value)

    for key in ('daily', 'daily_earnings', 'badge_dates', 'daily_quests'):
        if not isinstance(info.get(key), dict):
            info[key] = {}
    for key in (
        'badges', 'special_flags', 'transactions', 'active_loans', 'loan_offers',
        'loan_history', 'notified_classes', 'notified_study_milestones',
        'notified_coin_milestones', 'notified_loan_overdue',
    ):
        if not isinstance(info.get(key), list):
            info[key] = []
    for key in (
        'total', 'balance', 'total_earned', 'xp', 'streak', 'longest_streak',
        'goal_seconds', 'last_absent_warn', 'xp_acc_secs', 'coins_acc_secs',
        'quests_done_total', 'credit_score',
    ):
        if key == 'last_absent_warn':
            continue
        info[key] = _as_int(info.get(key, defaults.get(key, 0)))
    info['notifications_enabled'] = bool(info.get('notifications_enabled', True))

    info['transactions'] = info.get('transactions', [])[-TRANSACTION_HISTORY_LIMIT:]
    info['loan_history'] = info.get('loan_history', [])[-LOAN_HISTORY_LIMIT:]
    info['active_loans'] = _active_loans(info)
    info['loan_offers'] = [
        offer for offer in info.get('loan_offers', [])
        if isinstance(offer, dict) and offer.get('status', 'pending') == 'pending'
    ][-MAX_PENDING_LOAN_OFFERS:]
    _sync_money_class(info)
    return info

def _normalize_all_users(data: dict) -> dict:
    if not isinstance(data, dict):
        return {}
    for uid, info in list(data.items()):
        if not isinstance(info, dict):
            data[uid] = _default_user(f'User {uid}')
        data[uid] = _normalize_user_record(str(uid), data[uid])
    return data

def notifications_enabled_for(user_id: int) -> bool:
    data = load_data()
    uid = str(user_id)
    if uid not in data:
        return True
    return bool(data[uid].get('notifications_enabled', True))

def set_notifications_enabled(user_id: int, enabled: bool, name: str | None = None):
    uid = str(user_id)

    def mutator(data: dict):
        account = _ensure_account(data, uid, name or f'User {uid}')
        account['notifications_enabled'] = bool(enabled)

    update_data(mutator)

def _claim_user_notification(user_id: int, field: str, key: str, name: str | None = None) -> bool:
    uid = str(user_id)

    def mutator(data: dict):
        account = _ensure_account(data, uid, name or f'User {uid}')
        claimed = account.setdefault(field, [])
        if key in claimed:
            return False
        claimed.append(key)
        return True

    claimed, _ = update_data(mutator)
    return bool(claimed)

def _claim_user_notifications(user_id: int, field: str, keys: list[str], name: str | None = None) -> list[str]:
    uid = str(user_id)

    def mutator(data: dict):
        account = _ensure_account(data, uid, name or f'User {uid}')
        claimed = account.setdefault(field, [])
        newly_claimed = []
        for key in keys:
            if key in claimed:
                continue
            claimed.append(key)
            newly_claimed.append(key)
        return newly_claimed

    newly_claimed, _ = update_data(mutator)
    return newly_claimed or []

_NOTICE_EMOJI_RE = re.compile(r'[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE0F]')

def _compact_notice_description(message: str) -> str:
    text = str(message or '').strip()
    text = _NOTICE_EMOJI_RE.sub('', text)
    text = re.sub(r'[─━]{3,}', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip() or 'Thông báo từ BetterMe.'

def build_compact_notice_embed(
    title: str,
    description: str,
    color: int,
    footer_prefix: str = 'One percent better every day',
) -> discord.Embed:
    now = datetime.now()
    embed = discord.Embed(
        title=str(title or 'Thông báo').strip(),
        description=_compact_notice_description(description),
        color=color,
    )
    embed.set_footer(text=f'{footer_prefix} • Today at {now:%H:%M}')
    return embed

async def send_voice_notice(
    channel: discord.abc.Messageable,
    member: discord.Member,
    title: str,
    description: str,
    color: int,
    footer_prefix: str = 'One percent better every day',
):
    if not notifications_enabled_for(member.id):
        return
    if channel is None:
        log.info(f'Cannot send notice to voice channel chat for {member} ({member.id}): no channel')
        return

    embed = build_compact_notice_embed(
        title=title,
        description=description,
        color=color,
        footer_prefix=footer_prefix,
    )

    try:
        await channel.send(
            embed=embed,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.Forbidden:
        log.info(f'Cannot send notice to voice channel chat: {getattr(channel, "id", None)}')
    except Exception as e:
        log.warning(f'Failed to send voice notice: {e}')

async def send_private_notify_embed(
    member: discord.Member,
    title: str,
    description: str,
    color: int = NOTIFY_BLUE,
    footer: str = 'One percent better every day',
    respect_user_setting: bool = True,
):
    if respect_user_setting and not notifications_enabled_for(member.id):
        return

    embed = build_compact_notice_embed(
        title=title,
        description=description,
        color=color,
        footer_prefix=footer,
    )

    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.info(f'Cannot DM notification to {member} ({member.id}). User may have DMs closed.')
    except Exception as e:
        log.warning(f'Failed to send DM notification to {member.id}: {e}')

def _notice_title_color_from_text(message: str) -> tuple[str, int]:
    text = str(message or '').lower()
    if 'bị kick' in text or 'không bật cam' in text:
        return 'Đã rời phòng', NOTIFY_RED
    if (
        'cảnh báo' in text
        or 'vừa tắt cam' in text
        or 'thiếu quyền' in text
        or 'thất bại' in text
        or 'có lỗi' in text
        or 'không tìm thấy' in text
    ):
        return 'Cảnh báo', NOTIFY_RED
    if 'nhắc học' in text or 'nhắc' in text or 'chưa học' in text:
        return 'Nhắc nhở', NOTIFY_GOLD
    if 'chào mừng' in text or 'phòng học tạm đã được tạo' in text:
        return 'Chào mừng', NOTIFY_BLUE
    return 'Thông báo', NOTIFY_BLUE

def reset_cam_notification(member_id: int):
    cam_thanks_sent.discard(member_id)

def _focus_notice_channel(channel):
    if channel and is_focus_channel(getattr(channel, 'id', None)) and hasattr(channel, 'send'):
        return channel
    return None

def _current_voice_notice_channel(member: discord.Member):
    voice_channel = getattr(getattr(member, 'voice', None), 'channel', None)
    return _focus_notice_channel(voice_channel)

def _resolve_voice_notice_channel(
    member: discord.Member,
    channel=None,
    *,
    allow_current: bool = True,
):
    config = get_guild_config(member.guild.id)
    mode = str(config.get('notify_channel_mode') or 'voice').lower().strip()
    if mode in {'off', 'disabled', 'none'}:
        return None
    report_channel = None
    report_channel_id = config.get('report_channel_id')
    if report_channel_id:
        report_channel = bot.get_channel(int(report_channel_id))
    if mode in {'report', 'log', 'logs'}:
        return report_channel if report_channel and hasattr(report_channel, 'send') else None
    voice_channel = _focus_notice_channel(channel) or (
        _current_voice_notice_channel(member) if allow_current else None
    )
    if mode in {'both', 'voice_or_report'}:
        return voice_channel or (report_channel if report_channel and hasattr(report_channel, 'send') else None)
    return voice_channel

async def notify_cam_started(member: discord.Member, channel=None):
    if not notifications_enabled_for(member.id):
        return
    if member.id in cam_thanks_sent:
        return
    notice_channel = _resolve_voice_notice_channel(member, channel)
    if not notice_channel:
        return
    cam_thanks_sent.add(member.id)
    await send_voice_notice(
        channel=notice_channel,
        member=member,
        title='Cảm ơn',
        description=f'{member.display_name}, cảm ơn bạn đã bật Cam hoặc Stream.',
        color=NOTIFY_GREEN,
    )

async def notify_class_up(member: discord.Member, channel, new_class: str):
    if not notifications_enabled_for(member.id):
        return
    notice_channel = _resolve_voice_notice_channel(member, channel)
    if not notice_channel:
        return
    key = str(new_class)
    if not _claim_user_notification(member.id, 'notified_classes', key, member.display_name):
        return
    await send_voice_notice(
        channel=notice_channel,
        member=member,
        title='Chúc mừng',
        description=f'{member.mention} đã đạt được hạng **{new_class}**.',
        color=NOTIFY_GOLD,
    )

def _study_milestone_key(seconds: int) -> str:
    return f'{int(seconds) // 3600}h'

def _format_study_milestone_duration(seconds: int) -> str:
    hours = max(1, int(seconds) // 3600)
    return f'{hours} giờ'

async def notify_study_milestones(member: discord.Member, channel=None):
    if not notifications_enabled_for(member.id):
        return
    notice_channel = _resolve_voice_notice_channel(member, channel)
    if not notice_channel:
        return
    uid = str(member.id)
    data = load_data()
    info = data.get(uid)
    if not isinstance(info, dict):
        return
    total_seconds = _as_int(info.get('total', 0))
    reached = [
        _study_milestone_key(seconds) for seconds in STUDY_MILESTONE_SECONDS
        if total_seconds >= seconds
    ]
    newly_claimed = _claim_user_notifications(
        member.id,
        'notified_study_milestones',
        reached,
        member.display_name,
    )
    if not newly_claimed:
        return
    highest_key = newly_claimed[-1]
    milestone_seconds = next(
        (seconds for seconds in STUDY_MILESTONE_SECONDS if _study_milestone_key(seconds) == highest_key),
        0,
    )
    label = _format_study_milestone_duration(milestone_seconds) if milestone_seconds else highest_key
    await send_voice_notice(
        channel=notice_channel,
        member=member,
        title='Chúc mừng',
        description=(
            f'{member.mention} đã học tổng cộng **{label}** và đạt một cột mốc mới.\n'
            'Hãy tiếp tục duy trì thói quen học tập này nhé!'
        ),
        color=NOTIFY_GOLD,
    )

async def notify_coin_milestones(member: discord.Member, channel=None):
    if not notifications_enabled_for(member.id):
        return
    notice_channel = _resolve_voice_notice_channel(member, channel)
    if not notice_channel:
        return
    uid = str(member.id)
    data = load_data()
    info = data.get(uid)
    if not isinstance(info, dict):
        return
    total_earned = _as_int(info.get('total_earned', 0))
    reached = [str(amount) for amount in COIN_EARNING_MILESTONES if total_earned >= amount]
    newly_claimed = _claim_user_notifications(
        member.id,
        'notified_coin_milestones',
        reached,
        member.display_name,
    )
    if not newly_claimed:
        return
    amount = _as_int(newly_claimed[-1])
    await send_voice_notice(
        channel=notice_channel,
        member=member,
        title='Cột mốc kinh tế',
        description=f'Bạn đã kiếm tổng cộng **{format_coins(amount)}**.',
        color=NOTIFY_PURPLE,
        footer_prefix='Economy System',
    )

async def notify_loan_event(
    member: discord.Member,
    channel,
    title: str,
    amount: int,
    detail: str,
):
    description = (
        f'Số tiền: `{format_coins(amount)}`\n'
        f'{detail}'
    )
    notice_channel = _resolve_voice_notice_channel(member, channel, allow_current=False)
    if notice_channel:
        await send_voice_notice(
            channel=notice_channel,
            member=member,
            title=title,
            description=description,
            color=NOTIFY_PURPLE,
            footer_prefix='Economy System',
        )
        return
    await send_private_notify_embed(
        member=member,
        title=title,
        description=description,
        color=NOTIFY_PURPLE,
        footer='Economy System',
    )

async def notify_overdue_loans(member: discord.Member, channel=None):
    if not notifications_enabled_for(member.id):
        return
    notice_channel = _resolve_voice_notice_channel(member, channel)
    if not notice_channel:
        return
    uid = str(member.id)
    data = load_data()
    info = data.get(uid)
    if not isinstance(info, dict):
        return

    overdue_loans = [
        loan for loan in _active_loans(info)
        if _is_overdue(loan) and loan.get('id')
    ]
    if not overdue_loans:
        return

    overdue_ids = [str(loan.get('id')) for loan in overdue_loans]
    newly_claimed = set(_claim_user_notifications(
        member.id,
        'notified_loan_overdue',
        overdue_ids,
        member.display_name,
    ))
    if not newly_claimed:
        return

    new_loans = [loan for loan in overdue_loans if str(loan.get('id')) in newly_claimed]
    total_overdue = sum(_as_int(loan.get('remaining', 0)) for loan in new_loans)
    loan_lines = [
        f'Loan `{loan.get("id", "?")}` quá hạn từ `{loan.get("due_date", "n/a")}`.'
        for loan in new_loans[:5]
    ]
    if len(new_loans) > 5:
        loan_lines.append(f'Còn {len(new_loans) - 5} khoản quá hạn khác.')

    await send_voice_notice(
        channel=notice_channel,
        member=member,
        title='Khoản vay quá hạn',
        description=(
            f'Tổng quá hạn: `{format_coins(total_overdue)}`\n'
            + '\n'.join(loan_lines)
        ),
        color=NOTIFY_RED,
        footer_prefix='Economy System',
    )

async def send_private_session_summary(
    member: discord.Member,
    session_time: str,
    today_time: str,
    earned_today: int,
    balance: int,
    debt: int,
    class_name: str,
    total_earned: int,
    streak: int,
):
    if not notifications_enabled_for(member.id):
        return

    description = (
        f'Phiên này: `{session_time}`\n'
        f'Hôm nay: `{today_time}`\n'
        f'Earned hôm nay: `{format_coins(earned_today)}`\n'
        f'Balance: `{format_coins(balance)}` · Debt: `{format_coins(debt)}`\n'
        f'Class: `{class_name}` · Total earned: `{format_coins(total_earned)}`\n'
        f'Streak: `{streak} ngày`'
    )
    embed = build_compact_notice_embed(
        title='Phiên học kết thúc',
        description=description,
        color=NOTIFY_GREEN,
    )

    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.info(f'Cannot DM session summary to {member} ({member.id})')
    except Exception as e:
        log.warning(f'Failed to DM session summary to {member.id}: {e}')

async def notify_session_finished(
    member: discord.Member,
    session_time: str,
    today_time: str,
    earned_today: int,
    balance: int,
    debt: int,
    current_class: str,
    total_earned: int,
    streak: int,
):
    await send_private_session_summary(
        member=member,
        session_time=session_time,
        today_time=today_time,
        earned_today=earned_today,
        balance=balance,
        debt=debt,
        class_name=current_class,
        total_earned=total_earned,
        streak=streak,
    )

async def _handle_progress_notifications(member: discord.Member, result: dict | None = None, channel=None):
    result = result or {}
    notice_channel = _resolve_voice_notice_channel(member, channel)
    if not notice_channel:
        return
    if result.get('level_up'):
        await notify_class_up(member, notice_channel, class_label(result['new_level']))
    await notify_study_milestones(member, notice_channel)
    await notify_coin_milestones(member, notice_channel)
    await notify_overdue_loans(member, notice_channel)

def _new_id(prefix: str) -> str:
    stamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f'{prefix}_{stamp}_{random.randint(1000, 9999)}'

def _append_transaction(
    info: dict,
    tx_type: str,
    amount: int | float,
    description: str,
    counterparty: str | None = None,
    meta: dict | None = None,
    balance_before: int | float | None = None,
) -> dict:
    amount_int    = _as_int(round(amount))
    balance_after = _as_int(info.get('balance', 0))
    if balance_before is None and tx_type != 'interest':
        balance_before = balance_after - amount_int
    tx = {
        'id': _new_id('tx'),
        'ts': datetime.now().isoformat(timespec='seconds'),
        'type': tx_type,
        'amount': amount_int,
        'balance': balance_after,
        'balance_after': balance_after,
        'description': description,
    }
    if balance_before is not None:
        tx['balance_before'] = _as_int(balance_before)
    if counterparty:
        tx['counterparty'] = counterparty
    if meta:
        tx['meta'] = meta
    info.setdefault('transactions', []).append(tx)
    info['transactions'] = info['transactions'][-TRANSACTION_HISTORY_LIMIT:]
    return tx

def _append_loan_history(info: dict, action: str, description: str, loan_id: str, amount: int = 0):
    event = {
        'id': _new_id('loan'),
        'ts': datetime.now().isoformat(timespec='seconds'),
        'action': action,
        'loan_id': loan_id,
        'amount': _as_int(amount),
        'description': description,
    }
    info.setdefault('loan_history', []).append(event)
    info['loan_history'] = info['loan_history'][-LOAN_HISTORY_LIMIT:]
    return event

def _record_coin_earning(info: dict, amount: int, day_key: str, source: str, description: str):
    amount = _as_int(amount)
    if amount <= 0:
        return
    balance_before = _as_int(info.get('balance', 0))
    info['balance'] = balance_before + amount
    info['total_earned'] = _as_int(info.get('total_earned', 0)) + amount
    info.setdefault('daily_earnings', {})[day_key] = (
        _as_int(info.get('daily_earnings', {}).get(day_key, 0)) + amount
    )
    _sync_money_class(info)
    _append_transaction(
        info,
        'earning',
        amount,
        description,
        meta={'source': source, 'day': day_key},
        balance_before=balance_before,
    )

def _is_overdue(loan: dict) -> bool:
    due = loan.get('due_date')
    if not due:
        return False
    try:
        return datetime.strptime(due, '%Y-%m-%d').date() < datetime.now().date()
    except ValueError:
        return False

def _credit_score(info: dict) -> int:
    base = max(300, min(850, _as_int(info.get('credit_score', 600), 600)))
    active = len(_active_loans(info))
    overdue = sum(1 for loan in _active_loans(info) if _is_overdue(loan))
    return max(300, min(850, base - active * 5 - overdue * 40))

def _loan_interest(amount: int, interest_percent: float) -> int:
    return max(0, math.ceil(amount * interest_percent / 100))

def _pending_loan_offer_count(info: dict) -> int:
    return sum(1 for offer in info.get('loan_offers', []) if offer.get('status', 'pending') == 'pending')

def _ensure_account(data: dict, uid: str, name: str) -> dict:
    if uid not in data:
        data[uid] = _default_user(name)
    data[uid]['name'] = name or data[uid].get('name', f'User {uid}')
    return _normalize_user_record(uid, data[uid])

def _find_pending_offer(data: dict, loan_id: str) -> tuple[str, dict, dict] | None:
    loan_id = loan_id.strip()
    for lender_uid, lender_info in data.items():
        if not isinstance(lender_info, dict):
            continue
        for offer in lender_info.get('loan_offers', []):
            if offer.get('id') == loan_id and offer.get('status', 'pending') == 'pending':
                return str(lender_uid), lender_info, offer
    return None

def _loan_line(loan: dict) -> str:
    overdue = ' ⚠️ overdue' if _is_overdue(loan) else ''
    lender = loan.get('lender_name', 'Unknown')
    return (
        f'`{loan.get("id", "?")}` from **{lender}** · remaining `{format_coins(loan.get("remaining", 0))}` '
        f'· interest `{loan.get("interest_percent", 0)}%` · due `{loan.get("due_date", "n/a")}`{overdue}'
    )

def _offer_line(offer: dict, incoming: bool) -> str:
    other_name = offer.get('lender_name' if incoming else 'borrower_name', 'Unknown')
    direction = 'from' if incoming else 'to'
    return (
        f'`{offer.get("id", "?")}` {direction} **{other_name}** · '
        f'amount `{format_coins(offer.get("amount", 0))}` · '
        f'interest `{offer.get("interest_percent", 0)}%` · '
        f'due after `{offer.get("days", "?")}` days'
    )

def _pending_incoming_offers(data: dict, borrower_uid: str) -> list[dict]:
    offers: list[dict] = []
    for lender_info in data.values():
        if not isinstance(lender_info, dict):
            continue
        for offer in lender_info.get('loan_offers', []):
            if (
                isinstance(offer, dict)
                and offer.get('status', 'pending') == 'pending'
                and str(offer.get('borrower_id')) == borrower_uid
            ):
                offers.append(offer)
    return offers

def _parse_positive_int(raw: str, field_name: str = 'Amount') -> tuple[int | None, str | None]:
    text = str(raw or '').replace(',', '').replace('_', '').strip()
    if not text:
        return None, f'{field_name} không được để trống.'
    try:
        value = int(text)
    except ValueError:
        return None, f'{field_name} không hợp lệ.'
    if value <= 0:
        return None, f'{field_name} phải lớn hơn 0.'
    return value, None

def _parse_percent(raw: str) -> tuple[float | None, str | None]:
    text = str(raw or '').replace('%', '').strip()
    try:
        value = float(text)
    except ValueError:
        return None, 'Lãi suất không hợp lệ.'
    if value < 0 or value > 100:
        return None, 'Lãi suất phải nằm trong khoảng 0-100%.'
    return value, None

def _parse_discord_user_id(raw: str) -> int:
    match = re.search(r'\d{15,25}', str(raw or ''))
    if not match:
        raise ValueError('Invalid user ID')
    return int(match.group(0))

async def _resolve_guild_member_from_input(
    interaction: discord.Interaction,
    raw: str,
) -> tuple[discord.Member | None, str | None]:
    if not interaction.guild:
        return None, 'Chức năng này chỉ dùng được trong server.'
    try:
        user_id = _parse_discord_user_id(raw)
    except ValueError:
        return None, 'Không nhận ra người vay. Hãy nhập mention hoặc user ID hợp lệ.'

    member = interaction.guild.get_member(user_id)
    if member:
        return member, None
    try:
        member = await interaction.guild.fetch_member(user_id)
        return member, None
    except discord.NotFound:
        return None, 'Không tìm thấy user này trong server.'
    except discord.HTTPException:
        return None, 'Không thể kiểm tra user lúc này. Thử lại sau nhé.'

def _borrow_from_bot(user_id: int, user_name: str, amount: int) -> dict:
    amount = _as_int(amount)
    if amount <= 0:
        return {'ok': False, 'error': 'Số coins muốn vay phải lớn hơn 0.'}

    uid = str(user_id)

    def mutator(data: dict):
        borrower = _ensure_account(data, uid, user_name)
        if amount > MAX_BOT_LOAN_AMOUNT:
            return {'ok': False, 'error': f'Tối đa mỗi khoản vay bot là {format_coins(MAX_BOT_LOAN_AMOUNT)}.'}
        if len(_active_loans(borrower)) >= MAX_ACTIVE_LOANS:
            return {'ok': False, 'error': f'Bạn đã có quá nhiều khoản vay active ({MAX_ACTIVE_LOANS}).'}
        if any(loan.get('lender_id') == 'bot' for loan in _active_loans(borrower)):
            return {'ok': False, 'error': 'Bạn đã có khoản vay bot đang active. Trả xong rồi vay tiếp nhé.'}

        interest = _loan_interest(amount, BOT_LOAN_INTEREST_PERCENT)
        total_due = amount + interest
        loan_id = _new_id('loan')
        due_date = (datetime.now() + timedelta(days=BOT_LOAN_DAYS)).strftime('%Y-%m-%d')
        loan = {
            'id': loan_id,
            'lender_id': 'bot',
            'lender_name': 'Study Bot',
            'borrower_id': uid,
            'borrower_name': user_name,
            'principal': amount,
            'interest_percent': BOT_LOAN_INTEREST_PERCENT,
            'interest': interest,
            'total_due': total_due,
            'remaining': total_due,
            'borrowed_at': datetime.now().isoformat(timespec='seconds'),
            'due_date': due_date,
            'status': 'active',
        }
        borrower['balance'] += amount
        borrower.setdefault('active_loans', []).append(loan)
        _sync_money_class(borrower)
        _append_transaction(borrower, 'borrowing', amount, 'Borrowed virtual coins from Study Bot', counterparty='bot', meta={'loan_id': loan_id})
        if interest:
            _append_transaction(borrower, 'interest', interest, f'Interest added to debt ({BOT_LOAN_INTEREST_PERCENT:g}%)', counterparty='bot', meta={'loan_id': loan_id})
        _append_loan_history(borrower, 'borrow', f'Borrowed {format_coins(amount)} from Study Bot', loan_id, amount)
        return {'ok': True, 'loan': loan, 'balance': borrower['balance']}

    result, _ = update_data(mutator)
    return result

def _repay_active_loans(user_id: int, user_name: str, amount: int) -> dict:
    amount = _as_int(amount)
    if amount <= 0:
        return {'ok': False, 'error': 'Số coins trả nợ phải lớn hơn 0.'}

    uid = str(user_id)

    def mutator(data: dict):
        borrower = _ensure_account(data, uid, user_name)
        debt = _active_debt(borrower)
        if debt <= 0:
            return {'ok': False, 'error': 'Bạn không có khoản nợ active.'}
        if amount > debt:
            return {'ok': False, 'error': f'Không thể trả quá số nợ hiện tại ({format_coins(debt)}).'}
        if borrower.get('balance', 0) < amount:
            return {'ok': False, 'error': f'Balance không đủ. Bạn có {format_coins(borrower.get("balance", 0))}.'}

        borrower['balance'] -= amount
        remaining_payment = amount
        paid_loans = []
        active = sorted(_active_loans(borrower), key=lambda loan: loan.get('due_date', '9999-99-99'))
        for loan in active:
            if remaining_payment <= 0:
                break
            pay_now = min(remaining_payment, _as_int(loan.get('remaining', 0)))
            loan['remaining'] -= pay_now
            remaining_payment -= pay_now
            lender_id = loan.get('lender_id')
            if lender_id and lender_id != 'bot':
                lender = _ensure_account(data, str(lender_id), loan.get('lender_name', f'User {lender_id}'))
                lender['balance'] += pay_now
                _sync_money_class(lender)
                _append_transaction(
                    lender,
                    'repayment',
                    pay_now,
                    f'Repayment from {user_name}',
                    counterparty=uid,
                    meta={'loan_id': loan.get('id')},
                )
                _append_loan_history(lender, 'repayment_received', f'Received {format_coins(pay_now)} from {user_name}', loan.get('id', '?'), pay_now)
            _append_loan_history(borrower, 'repay', f'Repaid {format_coins(pay_now)} to {loan.get("lender_name", "Unknown")}', loan.get('id', '?'), pay_now)
            if loan['remaining'] <= 0:
                loan['status'] = 'paid'
                borrower['credit_score'] = min(850, _as_int(borrower.get('credit_score', 600), 600) + 15)
                paid_loans.append(str(loan.get('id', '?')))

        borrower['active_loans'] = [loan for loan in active if loan.get('remaining', 0) > 0]
        _sync_money_class(borrower)
        _append_transaction(borrower, 'repayment', -amount, 'Loan repayment', meta={'paid_loans': paid_loans})
        return {'ok': True, 'balance': borrower['balance'], 'debt': _active_debt(borrower), 'paid_loans': paid_loans}

    result, _ = update_data(mutator)
    return result

def _create_user_loan_offer(
    lender_id: int,
    lender_name: str,
    borrower_id: int,
    borrower_name: str,
    amount: int,
    interest_percent: float,
    days: int,
) -> dict:
    amount = _as_int(amount)
    days = _as_int(days)
    interest_percent = _as_float(interest_percent)
    if amount <= 0:
        return {'ok': False, 'error': 'Số coins cho vay phải lớn hơn 0.'}
    if days <= 0 or days > 365:
        return {'ok': False, 'error': 'Thời hạn phải từ 1 đến 365 ngày.'}
    if interest_percent < 0 or interest_percent > 100:
        return {'ok': False, 'error': 'Lãi suất phải nằm trong khoảng 0-100%.'}
    if lender_id == borrower_id:
        return {'ok': False, 'error': 'Không thể tạo khoản vay với chính mình.'}

    lender_uid = str(lender_id)
    borrower_uid = str(borrower_id)

    def mutator(data: dict):
        lender = _ensure_account(data, lender_uid, lender_name)
        borrower = _ensure_account(data, borrower_uid, borrower_name)
        if lender.get('balance', 0) < amount:
            return {'ok': False, 'error': f'Balance lender không đủ ({format_coins(lender.get("balance", 0))}).'}
        if _pending_loan_offer_count(lender) >= MAX_PENDING_LOAN_OFFERS:
            return {'ok': False, 'error': 'Bạn đang có quá nhiều offer pending.'}
        if any(o.get('borrower_id') == borrower_uid for o in lender.get('loan_offers', []) if o.get('status', 'pending') == 'pending'):
            return {'ok': False, 'error': 'Bạn đã có offer pending cho người này.'}

        loan_id = _new_id('loan')
        interest = _loan_interest(amount, interest_percent)
        offer = {
            'id': loan_id,
            'status': 'pending',
            'lender_id': lender_uid,
            'lender_name': lender_name,
            'borrower_id': borrower_uid,
            'borrower_name': borrower_name,
            'amount': amount,
            'interest_percent': interest_percent,
            'interest': interest,
            'days': days,
            'created_at': datetime.now().isoformat(timespec='seconds'),
        }
        lender.setdefault('loan_offers', []).append(offer)
        _append_loan_history(lender, 'offer', f'Offered {format_coins(amount)} to {borrower_name}', loan_id, amount)
        _append_loan_history(borrower, 'offer_received', f'Loan offer from {lender_name}: {format_coins(amount)}', loan_id, amount)
        return {'ok': True, 'offer': offer}

    result, _ = update_data(mutator)
    return result

def _tx_line(tx: dict) -> str:
    sign = '+' if _as_int(tx.get('amount', 0)) > 0 else ''
    return (
        f'`{tx.get("ts", "")}` **{tx.get("type", "tx")}** '
        f'`{sign}{format_coins(tx.get("amount", 0))}` · {tx.get("description", "")} '
        f'· balance `{format_coins(tx.get("balance", 0))}`'
    )

def _update_streak(data: dict, uid: str, today: str) -> tuple[int, bool]:
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    last_date = data[uid].get('last_study_date', '')
    streak    = data[uid].get('streak', 0)
    if last_date == today:
        return streak, False
    elif last_date == yesterday:
        streak += 1
    else:
        streak = 1
    data[uid]['streak']         = streak
    data[uid]['longest_streak'] = max(streak, data[uid].get('longest_streak', 0))
    data[uid]['last_study_date'] = today
    return streak, True

def format_time(seconds: int) -> str:
    if seconds <= 0: return '0s'
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:  return f'{h}h {m}m'
    if m > 0:  return f'{m}m {s}s'
    return f'{s}s'

def class_label(class_idx: int) -> str:
    class_idx = max(0, min(_as_int(class_idx), len(CLASS_NAMES) - 1))
    return f'Class {class_idx} {CLASS_NAMES[class_idx]}'

def class_up_message(class_idx: int) -> str:
    return f'🎉 **CLASS UP! Bạn đã đạt {class_label(class_idx)}** 🎊'

def _default_progress_result() -> dict:
    return {
        'coins_earned': 0,
        'class_up': False,
        'new_class': None,
        'balance': 0,
        'total_earned': 0,
        'debt': 0,
        'xp_gained': 0,
        'level_up': False,
        'new_level': None,
        'streak': 0,
        'total_xp': 0,
        'goal': None,
        'goal_seconds': 0,
        'today_seconds': 0,
    }

def _get_live_enriched_data(guild: discord.Guild | None = None) -> dict:
    data = load_data(guild.id if guild else None)
    for mid in list(join_times.keys()):
        if guild:
            member = guild.get_member(mid)
            if not member:
                continue
        else:
            member = None
        uid = str(mid)
        if uid in data:
            continue
        name = f'User {mid}'
        if member:
            name = member.display_name
        else:
            for known_guild in bot.guilds:
                m = known_guild.get_member(mid)
                if m:
                    name = m.display_name
                    break
        data[uid] = _default_user(name)
    return data

def get_report_channel_for(member: discord.Member):
    config = get_guild_config(member.guild.id)
    channel_id = config.get('report_channel_id')
    ch = bot.get_channel(channel_id) if channel_id else None
    if ch and getattr(ch, 'guild', None) and ch.guild.id == member.guild.id:
        return ch
    return None

def _split_seconds_by_day(start_time: datetime, end_time: datetime) -> list[tuple[str, int]]:
    if end_time <= start_time:
        return []
    parts: list[tuple[str, int]] = []
    cursor = start_time
    while cursor < end_time:
        next_day = (cursor + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        segment_end = min(end_time, next_day)
        segment_seconds = int((segment_end - cursor).total_seconds())
        if segment_seconds > 0:
            parts.append((cursor.strftime('%Y-%m-%d'), segment_seconds))
        cursor = segment_end
    return parts

def _resolve_study_window(
    member_id: int,
    start_time: datetime,
    end_time: datetime,
) -> tuple[datetime, datetime] | None:
    if end_time <= start_time:
        return None

    session_start = join_times.get(member_id)
    checkpoint = last_checkpoint.get(member_id, session_start or start_time)
    effective_start = max(start_time, checkpoint)
    effective_end = end_time

    if session_start is not None:
        effective_start = max(effective_start, session_start)
    if effective_end <= effective_start:
        return None
    return effective_start, effective_end

def _get_pending_study_window(
    member_id: int,
    now: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    if member_id not in join_times or member_id not in media_active_members:
        return None
    now = now or datetime.now()
    checkpoint = last_checkpoint.get(member_id, join_times[member_id])
    return _resolve_study_window(member_id, checkpoint, now)

def _sync_checkpoint_after_persist(member_id: int, end_time: datetime):
    if not _last_data_save_success or member_id not in join_times:
        return
    current_checkpoint = last_checkpoint.get(member_id, join_times[member_id])
    if end_time > current_checkpoint:
        last_checkpoint[member_id] = end_time
        save_runtime_state()

def add_study_time(
    member_id: int,
    member_name: str,
    seconds: int,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> dict:
    if seconds <= 0:
        return {}
    end_time = end_time or datetime.now()
    start_time = start_time or (end_time - timedelta(seconds=seconds))

    resolved_window = _resolve_study_window(member_id, start_time, end_time)
    if not resolved_window:
        return {}
    start_time, end_time = resolved_window
    seconds = int((end_time - start_time).total_seconds())
    if seconds <= 0:
        return {}
    day_parts = _split_seconds_by_day(start_time, end_time)
    if not day_parts:
        return {}
    uid   = str(member_id)
    coins_per_minute = coins_per_minute_for()

    def mutator(data: dict):
        if uid not in data:
            data[uid] = _default_user(member_name)
        _normalize_user_record(uid, data[uid])
        data[uid]['name'] = member_name
        old_class = get_money_class(data[uid].get('total_earned', 0))
        total_coins_earned = 0
        streak = data[uid].get('streak', 0)
        for day_key, day_seconds in day_parts:
            data[uid]['daily'][day_key] = data[uid]['daily'].get(day_key, 0) + day_seconds
            data[uid]['total'] = data[uid].get('total', 0) + day_seconds
            coin_acc = data[uid].get('coins_acc_secs', 0) + day_seconds
            coins_earned = (coin_acc // 60) * coins_per_minute
            data[uid]['coins_acc_secs'] = coin_acc % 60
            streak, is_new_day = _update_streak(data, uid, day_key)
            if is_new_day and streak > 1:
                coins_earned += streak * 5
            total_coins_earned += coins_earned
            _record_coin_earning(
                data[uid],
                coins_earned,
                day_key,
                'study_time',
                f'Study reward for {format_time(day_seconds)}',
            )
        today = end_time.strftime('%Y-%m-%d')
        new_class = _sync_money_class(data[uid])
        return {
            'coins_earned': total_coins_earned,
            'class_up': new_class > old_class,
            'new_class': new_class,
            'balance': data[uid].get('balance', 0),
            'total_earned': data[uid].get('total_earned', 0),
            'debt': data[uid].get('debt', 0),
            'xp_gained': total_coins_earned,
            'level_up': new_class > old_class,
            'new_level': new_class,
            'streak': streak,
            'total_xp': data[uid].get('total_earned', 0),
            'goal': data[uid].get('goal'),
            'goal_seconds': data[uid].get('goal_seconds', 0),
            'today_seconds': data[uid]['daily'].get(today, 0),
        }

    result, _ = update_data(mutator)
    if not _last_data_save_success:
        log.critical(
            f'Không thể xác minh dữ liệu sau khi cộng thời gian cho {member_name} ({member_id}).'
        )
        return {}
    guild_id = _guild_data_id()
    if guild_id:
        channel_id = None
        used_camera = False
        used_stream = False
        guild = bot.get_guild(int(guild_id))
        member = guild.get_member(member_id) if guild else None
        if member and member.voice and member.voice.channel:
            channel_id = member.voice.channel.id
            used_camera = bool(member.voice.self_video)
            used_stream = bool(member.voice.self_stream)
        try:
            repository.record_study_session_chunk(
                guild_id=int(guild_id),
                user_id=int(member_id),
                channel_id=channel_id,
                started_at=start_time.astimezone().isoformat(timespec='seconds'),
                ended_at=end_time.astimezone().isoformat(timespec='seconds'),
                duration_seconds=seconds,
                active_seconds=seconds,
                earned_coins=_as_int(result.get('coins_earned', 0)),
                used_camera=used_camera,
                used_stream=used_stream,
                ended_reason='checkpoint',
            )
        except Exception as e:
            log.warning('[StudySession] Could not persist study session chunk: %s', e, exc_info=True)
    _sync_checkpoint_after_persist(member_id, end_time)
    return result

def add_coins_direct(uid: str, coin_amount: int):
    def mutator(data: dict):
        if uid not in data:
            return
        _normalize_user_record(uid, data[uid])
        today = datetime.now().strftime('%Y-%m-%d')
        _record_coin_earning(
            data[uid],
            coin_amount,
            today,
            'bonus',
            'Bonus reward',
        )

    update_data(mutator)

def add_xp_direct(uid: str, xp_amount: int):
    """Compatibility hook for modules that still call the old reward callback."""
    add_coins_direct(uid, xp_amount)

# ─── QUEST SYSTEM ────────────────────────────────────────────────────────────

def generate_daily_quests(uid: str, today: str, member_name: str = '') -> list[dict]:
    def mutator(data: dict):
        if uid not in data:
            data[uid] = _default_user(member_name or f'User {uid}')
        existing = data[uid].get('daily_quests', {}).get(today)
        if existing:
            return existing
        streak = data[uid].get('streak', 0)
        pool = [q for q in QUEST_POOL if not (q['type'] == 'streak' and streak >= q['target'])]
        chosen = random.sample(pool, min(QUEST_DAILY_COUNT, len(pool)))
        quests = [
            {
                'id': q['id'],
                'progress': 0,
                'done': False,
                'notified': False,
                'notification_pending': False,
            }
            for q in chosen
        ]
        data[uid].setdefault('daily_quests', {})[today] = quests
        return quests

    quests, _ = update_data(mutator)
    return quests

def get_quest_info(quest_id: str) -> dict | None:
    return next((q for q in QUEST_POOL if q['id'] == quest_id), None)

def update_quest_progress(uid: str, today: str, override_today_secs: int = None, member_name: str = '') -> list[str]:
    def mutator(data: dict):
        if uid not in data:
            data[uid] = _default_user(member_name or f'User {uid}')
        quests = data[uid].get('daily_quests', {}).get(today, [])
        today_secs = override_today_secs if override_today_secs is not None else data[uid]['daily'].get(today, 0)
        streak = data[uid].get('streak', 0)
        now_hour = datetime.now().hour
        try:
            sessions = session_counts.get(int(uid), 0)
        except (ValueError, TypeError):
            sessions = 0
        just_done = []
        for q in quests:
            if q.get('done'):
                continue
            info = get_quest_info(q['id'])
            if not info:
                continue
            t, target = info['type'], info['target']
            if t == 'minutes':
                q['progress'] = min(target, today_secs // 60)
            elif t == 'streak':
                q['progress'] = min(target, streak)
            elif t == 'hour_before':
                if now_hour < target:
                    q['progress'] = target
            elif t == 'hour_after':
                if now_hour >= target:
                    q['progress'] = target
            elif t == 'first_in':
                first_id = daily_first_join.get(today)
                if first_id and str(first_id) == uid:
                    q['progress'] = target
            elif t == 'sessions':
                q['progress'] = min(target, sessions)

            if q['progress'] >= target and not q.get('done'):
                q['done'] = True
                q['notified'] = False
                q['notification_pending'] = True
                just_done.append(q['id'])
                data[uid]['quests_done_total'] = data[uid].get('quests_done_total', 0) + 1
                coins_bonus = info.get('coins', info.get('xp', 0))
                _record_coin_earning(
                    data[uid],
                    coins_bonus,
                    today,
                    'quest',
                    f'Quest reward: {info["desc"]}',
                )
                log.info(f'Quest done [{q["id"]}] → +{coins_bonus} coins cho {data[uid]["name"]}')
        data[uid].setdefault('daily_quests', {})[today] = quests
        return just_done

    just_done, _ = update_data(mutator)
    return just_done

def claim_completed_quest_notifications(uid: str, today: str) -> list[dict]:
    def mutator(data: dict):
        if uid not in data:
            return []
        quests = data[uid].get('daily_quests', {}).get(today, [])
        claimed: list[dict] = []
        for q in quests:
            pending_notification = q.get('notification_pending')
            if pending_notification is None:
                pending_notification = q.get('done') and not q.get('notified', False)
            if q.get('done') and pending_notification:
                info = get_quest_info(q['id'])
                if info:
                    claimed.append(info)
                q['notification_pending'] = False
                q['notified'] = True
        if claimed:
            data[uid].setdefault('daily_quests', {})[today] = quests
        return claimed

    claimed, _ = update_data(mutator)
    return claimed

# ─── BADGE SYSTEM ────────────────────────────────────────────────────────────

def check_and_award_badges(uid: str, member: discord.Member = None) -> list[str]:
    def mutator(data: dict):
        if uid not in data:
            return []
        info = data[uid]
        earned = set(info.get('badges', []))
        total_hours = info.get('total', 0) / 3600
        total_earned = info.get('total_earned', 0)
        class_idx = get_money_class(total_earned)
        streak = info.get('streak', 0)
        quests_done = info.get('quests_done_total', 0)
        special_flags = info.get('special_flags', [])
        today = datetime.now().strftime('%Y-%m-%d')
        today_secs = info['daily'].get(today, 0)
        new_badges = []
        for bid, bdef in BADGES.items():
            if bid in earned:
                continue
            ctype, cval = bdef['condition']
            awarded = False
            if ctype == 'streak' and streak >= cval:
                awarded = True
            elif ctype == 'total_hours' and total_hours >= cval:
                awarded = True
            elif ctype == 'daily_hours' and today_secs >= cval * 3600:
                awarded = True
            elif ctype in ('level', 'class') and class_idx >= cval:
                awarded = True
            elif ctype in ('xp', 'total_earned') and total_earned >= cval:
                awarded = True
            elif ctype == 'quests_done' and quests_done >= cval:
                awarded = True
            elif ctype == 'special' and cval in special_flags:
                awarded = True
            if awarded:
                new_badges.append(bid)
                earned.add(bid)
        if new_badges:
            data[uid]['badges'] = list(earned)
            badge_dates = data[uid].setdefault('badge_dates', {})
            for bid in new_badges:
                badge_dates.setdefault(bid, today)
        return new_badges

    new_badges, _ = update_data(mutator)
    return new_badges

def award_special_flag(uid: str, flag: str):
    def mutator(data: dict):
        if uid not in data:
            data[uid] = _default_user(f'User {uid}')
        flags = data[uid].get('special_flags', [])
        if flag not in flags:
            flags.append(flag)
            data[uid]['special_flags'] = flags

    update_data(mutator)

def format_badges(badge_ids: list) -> str:
    if not badge_ids:
        return '_Chưa có huy hiệu nào_'
    parts = [BADGES[b]['name'] for b in badge_ids if b in BADGES]
    return '  '.join(parts) if parts else '_Chưa có huy hiệu nào_'

# ─── PROFILE CARD ─────────────────────────────────────────────────────────────

PROFILE_CARD_W = 900
PROFILE_CARD_H = 560


def _try_font(paths: list[str], size: int):
    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()


def _card_font(size: int, bold: bool = False):
    return _try_font([
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf' if bold else '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf' if bold else '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
        '/System/Library/Fonts/Helvetica.ttc',
        'C:/Windows/Fonts/arialbd.ttf' if bold else 'C:/Windows/Fonts/arial.ttf',
    ], size)


def _draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    draw.rounded_rectangle(list(xy), radius=radius, fill=fill, outline=outline, width=width)


def _lerp_channel(start: int, end: int, t: float) -> int:
    return int(start + (end - start) * max(0.0, min(1.0, t)))


def _lerp_rgba(start: tuple[int, int, int, int], end: tuple[int, int, int, int], t: float):
    return tuple(_lerp_channel(start[i], end[i], t) for i in range(4))


def _discord_dark_background(width: int, height: int):
    img = Image.new('RGBA', (width, height), (14, 15, 20, 255))
    draw = ImageDraw.Draw(img)
    top = (17, 18, 24, 255)
    bottom = (34, 36, 46, 255)
    for y in range(height):
        t = y / max(1, height - 1)
        draw.line((0, y, width, y), fill=_lerp_rgba(top, bottom, t))

    for x in range(0, width, 72):
        draw.line((x, 0, x, height), fill=(255, 255, 255, 5), width=1)
    for y in range(0, height, 72):
        draw.line((0, y, width, y), fill=(255, 255, 255, 4), width=1)

    accent = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    accent_draw = ImageDraw.Draw(accent)
    accent_draw.polygon(
        [(0, 0), (width, 0), (width, int(height * 0.22)), (0, int(height * 0.40))],
        fill=(88, 101, 242, 34),
    )
    accent_draw.rectangle((0, 0, width, 5), fill=(88, 101, 242, 180))
    img.alpha_composite(accent)
    return img


def _draw_soft_shadow(layer, xy, radius: int, alpha: int = 95, blur: int = 18, offset: tuple[int, int] = (0, 8)):
    x1, y1, x2, y2 = [int(v) for v in xy]
    dx, dy = offset
    shadow = Image.new('RGBA', layer.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.rounded_rectangle(
        (x1 + dx, y1 + dy, x2 + dx, y2 + dy),
        radius=radius,
        fill=(0, 0, 0, alpha),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(blur))
    layer.alpha_composite(shadow)


def _draw_discord_panel(
    layer,
    draw,
    xy,
    radius: int = 22,
    fill=(43, 45, 49, 238),
    outline=(78, 80, 88, 210),
    width: int = 1,
    shadow: bool = True,
):
    if shadow:
        _draw_soft_shadow(layer, xy, radius)
    draw.rounded_rectangle(list(xy), radius=radius, fill=fill, outline=outline, width=width)


def _profile_card_text_width(draw, text: str, font) -> int:
    bbox = draw.textbbox((0, 0), str(text), font=font)
    return bbox[2] - bbox[0]


def _draw_fitted_text(draw, xy, text: str, font, fill, max_width: int, anchor: str | None = None):
    fitted = _fit_profile_card_text(draw, text, font, max_width)
    draw.text(xy, fitted, font=font, fill=fill, anchor=anchor)
    return fitted


def _fit_profile_card_text(draw, text: str, font, max_width: int) -> str:
    text = re.sub(r'\s+', ' ', str(text or '')).strip()
    if not text or _profile_card_text_width(draw, text, font) <= max_width:
        return text

    suffix = '...'
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        candidate = text[:mid].rstrip() + suffix
        if _profile_card_text_width(draw, candidate, font) <= max_width:
            lo = mid
        else:
            hi = mid - 1
    return text[:lo].rstrip() + suffix


def _wrap_profile_card_text(draw, text: str, font, max_width: int, max_lines: int) -> list[str]:
    text = re.sub(r'\s+', ' ', str(text or '')).strip()
    if not text:
        return []

    lines: list[str] = []
    for paragraph in textwrap.wrap(text, width=90) or [text]:
        current = ''
        for word in paragraph.split():
            candidate = f'{current} {word}'.strip()
            if _profile_card_text_width(draw, candidate, font) <= max_width:
                current = candidate
                continue
            if current:
                lines.append(current)
                current = word
            else:
                lines.append(_fit_profile_card_text(draw, word, font, max_width))
                current = ''
            if len(lines) >= max_lines:
                break
        if current and len(lines) < max_lines:
            lines.append(current)
        if len(lines) >= max_lines:
            break

    if len(lines) == max_lines and _profile_card_text_width(draw, lines[-1], font) > max_width:
        lines[-1] = _fit_profile_card_text(draw, lines[-1], font, max_width)
    return lines[:max_lines]


def _profile_card_compact_number(value: int | float) -> str:
    number = _as_int(value)
    sign = '-' if number < 0 else ''
    number = abs(number)
    for suffix, threshold in (('B', 1_000_000_000), ('M', 1_000_000), ('K', 1_000)):
        if number >= threshold:
            compact = number / threshold
            text = f'{compact:.1f}'.rstrip('0').rstrip('.')
            return f'{sign}{text}{suffix}'
    return f'{sign}{number:,}'


def _profile_card_rank(uid: str, data: dict) -> int | None:
    ranked = sorted(
        (
            (user_id, _as_int(info.get('total_earned', 0)))
            for user_id, info in data.items()
            if isinstance(info, dict)
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    for index, (user_id, _) in enumerate(ranked, start=1):
        if user_id == uid:
            return index
    return None


def _profile_card_resize_cover(img, size: tuple[int, int]):
    target_w, target_h = size
    src_w, src_h = img.size
    if src_w <= 0 or src_h <= 0:
        return Image.new('RGB', size, (15, 23, 42))

    scale = max(target_w / src_w, target_h / src_h)
    try:
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS
    resized = img.resize((max(1, int(src_w * scale)), max(1, int(src_h * scale))), resample)
    left = max(0, (resized.width - target_w) // 2)
    top = max(0, (resized.height - target_h) // 2)
    return resized.crop((left, top, left + target_w, top + target_h))


def _load_remote_profile_image(avatar_url: str | None, timeout: float, max_bytes: int = 3_000_000):
    if not avatar_url:
        return None
    url = str(avatar_url).strip()
    if not url.lower().startswith(('https://', 'http://')):
        return None

    response = httpx.get(
        url,
        timeout=timeout,
        follow_redirects=True,
        headers={'User-Agent': 'discord-study-bot/1.0'},
    )
    response.raise_for_status()

    content_type = response.headers.get('content-type', '').lower()
    if content_type and not content_type.startswith('image/'):
        raise ValueError(f'avatar response is not an image: {content_type}')

    content_length = _as_int(response.headers.get('content-length'), 0)
    if content_length > max_bytes:
        raise ValueError('avatar image is too large')

    content = response.content
    if len(content) > max_bytes:
        raise ValueError('avatar image is too large')

    with Image.open(io.BytesIO(content)) as img:
        if img.width > 4096 or img.height > 4096:
            raise ValueError('avatar dimensions are too large')
        img = ImageOps.exif_transpose(img)
        return img.convert('RGB').copy()


def _draw_gradient_progress_bar(
    layer,
    draw,
    xy,
    pct: float,
    *,
    background=(30, 31, 36, 255),
    outline=(78, 80, 88, 210),
    start=(88, 101, 242, 255),
    end=(35, 165, 90, 255),
):
    x1, y1, x2, y2 = [int(v) for v in xy]
    width = max(1, x2 - x1)
    height = max(1, y2 - y1)
    radius = max(1, height // 2)
    pct = max(0.0, min(1.0, float(pct)))
    draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=background, outline=outline, width=1)

    fill_w = int(width * pct)
    if fill_w <= 0:
        return

    gradient = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    gradient_draw = ImageDraw.Draw(gradient)
    for x in range(fill_w):
        t = x / max(1, fill_w - 1)
        gradient_draw.line((x, 0, x, height), fill=_lerp_rgba(start, end, t))

    mask = Image.new('L', (width, height), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, fill_w, height), radius=radius, fill=255)
    clip = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    clip.paste(gradient, (0, 0), mask)
    layer.alpha_composite(clip, (x1, y1))


def _profile_card_gradient_background(width: int, height: int):
    img = Image.new('RGB', (width, height), (10, 15, 28))
    draw = ImageDraw.Draw(img)
    for y in range(height):
        t = y / max(1, height - 1)
        r = int(11 + 28 * t)
        g = int(18 + 28 * t)
        b = int(35 + 55 * t)
        draw.line((0, y, width, y), fill=(r, g, b))

    for _ in range(130):
        x = random.randint(0, width - 1)
        y = random.randint(0, int(height * 0.65))
        alpha = random.randint(28, 70)
        color = (80 + alpha, 100 + alpha, 150 + alpha)
        draw.point((x, y), fill=color)
    return img


def _profile_card_background(width: int, height: int):
    candidates = [
        os.getenv('PROFILE_CARD_BACKGROUND', '').strip(),
        str(BASE_DIR / 'profile_card_bg.png'),
        str(BASE_DIR / 'profile_card_bg.jpg'),
    ]
    for raw_path in candidates:
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        if not path.exists():
            continue
        try:
            return _profile_card_resize_cover(Image.open(path).convert('RGB'), (width, height))
        except Exception as e:
            log.warning(f'Failed to load profile card background {path}: {e}')
    return _profile_card_gradient_background(width, height)


def _profile_card_avatar(
    avatar_url: str | None,
    display_name: str,
    size: int,
    timeout: float = 8.0,
    corner_radius: int | None = None,
):
    avatar = None
    if avatar_url:
        try:
            loaded = _load_remote_profile_image(avatar_url, timeout)
            if loaded is not None:
                avatar = _profile_card_resize_cover(loaded, (size, size))
        except Exception as e:
            log.warning(f'Failed to download profile card avatar: {e}')

    if avatar is None:
        avatar = Image.new('RGB', (size, size), (47, 49, 54))
        draw = ImageDraw.Draw(avatar)
        for y in range(size):
            t = y / max(1, size - 1)
            draw.line(
                (0, y, size, y),
                fill=(
                    _lerp_channel(88, 35, t),
                    _lerp_channel(101, 165, t),
                    _lerp_channel(242, 90, t),
                ),
            )
        initial = (display_name or '?').strip()[:1].upper() or '?'
        font = _card_font(max(20, int(size * 0.46)), True)
        draw.text((size // 2, size // 2 - 2), initial, font=font, fill=(255, 255, 255), anchor='mm')

    mask = Image.new('L', (size, size), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, size, size), radius=corner_radius or size // 2, fill=255)
    out = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    out.paste(avatar.convert('RGBA'), (0, 0), mask)
    return out


def _draw_profile_card_stat(draw, box, label: str, value: str, color, label_font, value_font):
    x1, y1, x2, y2 = box
    _draw_rounded_rect(draw, box, 18, fill=(43, 45, 49, 236), outline=(78, 80, 88, 210), width=1)
    draw.rounded_rectangle((x1 + 13, y1 + 16, x1 + 18, y2 - 16), radius=3, fill=color)
    draw.text((x1 + 30, y1 + 13), label.upper(), font=label_font, fill=(181, 186, 193, 245))
    fitted = _fit_profile_card_text(draw, value, value_font, max(1, x2 - x1 - 44))
    draw.text((x1 + 30, y1 + 35), fitted, font=value_font, fill=(242, 243, 245, 255))


def generate_profile_card(
    member_id: int,
    guild_id: int | None = None,
    display_name: str | None = None,
    avatar_url: str | None = None,
) -> bytes | None:
    if not PIL_AVAILABLE:
        return None
    uid  = str(member_id)
    data = load_data(guild_id)
    if uid not in data and guild_id is None:
        for candidate_guild_id in _configured_guild_ids():
            data = load_data(candidate_guild_id)
            if uid in data:
                break
    if uid not in data:
        return None
    info    = data[uid]
    name    = display_name or info.get('name', 'Unknown')
    total_earned = _as_int(info.get('total_earned', 0))
    balance = _as_int(info.get('balance', 0))
    debt = _active_debt(info)
    class_idx = min(get_money_class(total_earned), len(CLASS_NAMES) - 1)
    class_name = class_label(class_idx)
    streak  = _as_int(info.get('streak', 0))
    total   = _as_int(info.get('total', 0))
    today   = datetime.now().strftime('%Y-%m-%d')
    today_secs = _as_int(info.get('daily', {}).get(today, 0))
    today_secs += _get_unsaved_study_seconds(member_id)
    today_earned = _as_int(info.get('daily_earnings', {}).get(today, 0))
    rank = _profile_card_rank(uid, data)

    if class_idx >= len(CLASS_THRESHOLDS) - 1:
        coin_start = CLASS_THRESHOLDS[class_idx]
        coin_end = coin_start
        coin_pct = 1.0
    else:
        coin_start = CLASS_THRESHOLDS[class_idx]
        coin_end = CLASS_THRESHOLDS[class_idx + 1]
        coin_pct = max(0.0, min(1.0, (total_earned - coin_start) / max(1, coin_end - coin_start)))

    W, H = PROFILE_CARD_W, PROFILE_CARD_H
    base = _discord_dark_background(W, H)
    layer = Image.new('RGBA', (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)

    TEXT = (242, 243, 245, 255)
    MUTED = (181, 186, 193, 255)
    SOFT = (148, 155, 164, 255)
    PANEL = (43, 45, 49, 244)
    PANEL_DARK = (30, 31, 36, 246)
    BORDER = (78, 80, 88, 220)
    BLURPLE = (88, 101, 242, 255)
    GREEN = (35, 165, 90, 255)
    YELLOW = (254, 231, 92, 255)
    RED = (242, 63, 66, 255)
    PINK = (235, 69, 158, 255)
    CYAN = (88, 191, 255, 255)

    f_name = _card_font(40, True)
    f_sub = _card_font(20)
    f_level = _card_font(62, True)
    f_level_label = _card_font(13, True)
    f_label = _card_font(12, True)
    f_stat = _card_font(22, True)
    f_about_title = _card_font(16, True)
    f_about = _card_font(17)
    f_small = _card_font(16)
    f_pill = _card_font(15, True)

    _draw_discord_panel(layer, draw, (24, 22, W - 24, H - 22), 30, fill=PANEL_DARK, outline=BORDER, width=1)
    draw.rounded_rectangle((24, 22, W - 24, 34), radius=6, fill=(88, 101, 242, 210))
    _draw_discord_panel(layer, draw, (48, 50, W - 48, 226), 24, fill=PANEL, outline=BORDER, shadow=False)
    _draw_discord_panel(layer, draw, (636, 72, 824, 202), 22, fill=PANEL_DARK, outline=BORDER, shadow=False)

    avatar_size = 128
    avatar_x, avatar_y = 78, 74
    draw.ellipse(
        (avatar_x - 7, avatar_y - 7, avatar_x + avatar_size + 7, avatar_y + avatar_size + 7),
        fill=(88, 101, 242, 255),
        outline=(130, 140, 255, 190),
        width=2,
    )
    layer.alpha_composite(_profile_card_avatar(avatar_url, name, avatar_size), (avatar_x, avatar_y))
    draw.ellipse(
        (avatar_x + avatar_size - 31, avatar_y + avatar_size - 31, avatar_x + avatar_size + 3, avatar_y + avatar_size + 3),
        fill=PANEL,
    )
    draw.ellipse(
        (avatar_x + avatar_size - 25, avatar_y + avatar_size - 25, avatar_x + avatar_size - 3, avatar_y + avatar_size - 3),
        fill=GREEN,
    )

    _draw_fitted_text(draw, (232, 72), name, f_name, TEXT, 370)
    _draw_fitted_text(draw, (233, 124), class_name, f_sub, MUTED, 372)

    rank_text = f'#{rank:,}' if rank else 'Unranked'
    pill_y = 158
    rank_pill = f'Rank {rank_text}'
    rank_w = min(164, max(96, _profile_card_text_width(draw, rank_pill, f_pill) + 28))
    draw.rounded_rectangle((232, pill_y, 232 + rank_w, pill_y + 30), radius=15, fill=(88, 101, 242, 52), outline=(88, 101, 242, 150), width=1)
    _draw_fitted_text(draw, (246, pill_y + 15), rank_pill, f_pill, (214, 219, 255, 255), rank_w - 28, anchor='lm')
    earned_pill = f'{_profile_card_compact_number(total_earned)} earned'
    earned_x = 232 + rank_w + 10
    earned_w = min(206, max(118, _profile_card_text_width(draw, earned_pill, f_pill) + 28))
    draw.rounded_rectangle((earned_x, pill_y, earned_x + earned_w, pill_y + 30), radius=15, fill=(35, 165, 90, 45), outline=(35, 165, 90, 130), width=1)
    _draw_fitted_text(draw, (earned_x + 14, pill_y + 15), earned_pill, f_pill, (196, 255, 214, 255), earned_w - 28, anchor='lm')

    progress_label = 'Max class reached' if coin_end == coin_start else f'{total_earned - coin_start:,} / {coin_end - coin_start:,} coins'
    draw.text((232, 198), 'CLASS PROGRESS', font=f_label, fill=SOFT)
    draw.text((612, 198), f'{int(coin_pct * 100)}%', font=f_label, fill=SOFT, anchor='ra')
    _draw_gradient_progress_bar(
        layer,
        draw,
        (232, 214, 612, 226),
        coin_pct,
        background=(30, 31, 36, 255),
        outline=(78, 80, 88, 190),
        start=BLURPLE,
        end=GREEN,
    )
    _draw_fitted_text(draw, (232, 236), progress_label, f_small, SOFT, 380)

    draw.text((730, 94), 'CLASS', font=f_level_label, fill=SOFT, anchor='mm')
    draw.text((730, 140), f'{class_idx:02d}', font=f_level, fill=TEXT, anchor='mm')
    _draw_fitted_text(draw, (730, 183), CLASS_NAMES[class_idx], f_small, MUTED, 148, anchor='mm')

    net_worth = balance - debt
    stats = [
        ('Balance', _profile_card_compact_number(balance), YELLOW),
        ('Total earned', _profile_card_compact_number(total_earned), GREEN),
        ('Net worth', _profile_card_compact_number(net_worth), BLURPLE if net_worth >= 0 else RED),
        ('Debt', _profile_card_compact_number(debt), RED if debt else SOFT),
        ('Streak', f'{streak} days', PINK),
        ('Today study', format_time(today_secs), CYAN),
        ('All time study', format_time(total), MUTED),
        ('Earned today', _profile_card_compact_number(today_earned), GREEN),
    ]
    stat_w, stat_h = 190, 76
    start_x, start_y = 48, 252
    gap_x, gap_y = 14, 14
    for index, (label, value, color) in enumerate(stats):
        col = index % 4
        row = index // 4
        x1 = start_x + col * (stat_w + gap_x)
        y1 = start_y + row * (stat_h + gap_y)
        _draw_profile_card_stat(draw, (x1, y1, x1 + stat_w, y1 + stat_h), label, value, color, f_label, f_stat)

    about = (
        info.get('about_me')
        or info.get('about')
        or info.get('bio')
        or 'One percent better every day.'
    )
    _draw_discord_panel(layer, draw, (48, 434, W - 48, 514), 20, fill=PANEL, outline=BORDER, shadow=False)
    about_lines = _wrap_profile_card_text(draw, about, f_about, 654, 2)
    about_x, about_y = 70, 456
    draw.text((about_x, about_y), 'ABOUT', font=f_about_title, fill=SOFT)
    for i, line in enumerate(about_lines):
        draw.text((about_x + 104, about_y - 1 + i * 23), line, font=f_about, fill=MUTED)

    final = Image.alpha_composite(base, layer)
    mask = Image.new('L', (W, H), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, W, H), radius=34, fill=255)
    out = Image.new('RGBA', (W, H), (0, 0, 0, 0))
    out.paste(final, (0, 0), mask)

    buf = io.BytesIO()
    out.save(buf, format='PNG', optimize=True)
    buf.seek(0)
    return buf.getvalue()


STUDY_LEADERBOARD_PER_PAGE = 10
STUDY_LEADERBOARD_W = 900
STUDY_LEADERBOARD_H = 980
STUDY_CHART_W = 900
STUDY_CHART_H = 560


def _study_leaderboard_time(seconds: int) -> str:
    seconds = max(0, _as_int(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours:
        return f'{hours}h {minutes:02d}m'
    return f'{minutes}m'


def _member_avatar_url(member: discord.Member | discord.User | None, size: int = 128) -> str | None:
    avatar = getattr(member, 'display_avatar', None)
    if not avatar:
        return None
    try:
        return str(avatar.with_size(size).url)
    except Exception:
        return str(getattr(avatar, 'url', '') or '') or None


def _build_study_leaderboard_entries(guild: discord.Guild | None) -> tuple[list[dict], str]:
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.now()
    data = _get_live_enriched_data(guild)
    entries: list[dict] = []

    for uid, info in data.items():
        if not isinstance(info, dict):
            continue
        seconds = _as_int(info.get('daily', {}).get(today, 0))
        member = None
        try:
            member_id = int(uid)
        except (TypeError, ValueError):
            member_id = None
        if member_id is not None:
            if guild:
                member = guild.get_member(member_id)
            seconds += _get_unsaved_study_seconds(member_id, now)
        if seconds <= 0:
            continue
        entries.append({
            'user_id': member_id,
            'display_name': member.display_name if member else str(info.get('name') or f'User {uid}'),
            'avatar_url': _member_avatar_url(member, 128),
            'study_seconds': seconds,
        })

    entries.sort(key=lambda entry: entry['study_seconds'], reverse=True)
    for rank, entry in enumerate(entries, start=1):
        entry['rank'] = rank
    return entries, today


def render_study_leaderboard_image(
    entries: list[dict],
    page: int,
    total_pages: int,
    today: str,
    total_entries: int | None = None,
    top_seconds: int | None = None,
) -> bytes:
    W, H = STUDY_LEADERBOARD_W, STUDY_LEADERBOARD_H
    base = _discord_dark_background(W, H)
    layer = Image.new('RGBA', (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)

    total_entries = len(entries) if total_entries is None else max(0, int(total_entries))
    top_seconds = max(top_seconds or 0, *(_as_int(entry.get('study_seconds', 0)) for entry in entries), 0)
    page_seconds = sum(_as_int(entry.get('study_seconds', 0)) for entry in entries)

    f_title = _card_font(40, True)
    f_sub = _card_font(18)
    f_rank = _card_font(22, True)
    f_name = _card_font(23, True)
    f_meta = _card_font(13, True)
    f_time = _card_font(22, True)
    f_empty = _card_font(24, True)
    f_footer = _card_font(16)
    f_summary_label = _card_font(12, True)
    f_summary_value = _card_font(22, True)

    TEXT = (242, 243, 245, 255)
    MUTED = (181, 186, 193, 255)
    SOFT = (148, 155, 164, 255)
    PANEL = (43, 45, 49, 244)
    PANEL_DARK = (30, 31, 36, 246)
    BORDER = (78, 80, 88, 220)
    BLURPLE = (88, 101, 242, 255)
    GREEN = (35, 165, 90, 255)
    GOLD = (254, 231, 92, 255)
    SILVER = (181, 186, 193, 255)
    BRONZE = (205, 127, 50, 255)
    CYAN = (88, 191, 255, 255)

    _draw_discord_panel(layer, draw, (24, 22, W - 24, H - 22), 30, fill=PANEL_DARK, outline=BORDER, width=1)
    draw.rounded_rectangle((24, 22, W - 24, 34), radius=6, fill=(88, 101, 242, 210))
    _draw_discord_panel(layer, draw, (48, 50, W - 48, 146), 24, fill=PANEL, outline=BORDER, shadow=False)
    draw.text((72, 76), 'Study Leaderboard', font=f_title, fill=TEXT)
    draw.text((74, 119), f'Today • {today}', font=f_sub, fill=MUTED)
    draw.text((W - 74, 102), f'Page {page}/{total_pages}', anchor='rm', font=f_time, fill=GOLD)

    summary_y = 166
    summary_h = 58
    summary_gap = 14
    summary_w = (W - 96 - summary_gap * 2) // 3
    summary_stats = [
        ('MEMBERS', f'{total_entries:,}', BLURPLE),
        ('TOP TIME', _study_leaderboard_time(top_seconds), GREEN),
        ('THIS PAGE', _study_leaderboard_time(page_seconds), CYAN),
    ]
    for index, (label, value, color) in enumerate(summary_stats):
        x1 = 48 + index * (summary_w + summary_gap)
        x2 = x1 + summary_w
        _draw_rounded_rect(draw, (x1, summary_y, x2, summary_y + summary_h), 18, fill=PANEL, outline=BORDER, width=1)
        draw.rounded_rectangle((x1 + 14, summary_y + 14, x1 + 19, summary_y + summary_h - 14), radius=3, fill=color)
        draw.text((x1 + 31, summary_y + 12), label, font=f_summary_label, fill=SOFT)
        _draw_fitted_text(draw, (x1 + 31, summary_y + 33), value, f_summary_value, TEXT, summary_w - 48)

    if not entries:
        _draw_discord_panel(layer, draw, (74, 270, W - 74, 382), 24, fill=PANEL, outline=BORDER, shadow=False)
        draw.text((W // 2, 326), 'No study time recorded today', anchor='mm', font=f_empty, fill=MUTED)
    else:
        start_y = 248
        row_h = 58
        gap = 10
        row_x1, row_x2 = 52, W - 52
        for index, entry in enumerate(entries):
            y = start_y + index * (row_h + gap)
            rank_num = _as_int(entry.get('rank', index + 1), index + 1)
            rank_color = {1: GOLD, 2: SILVER, 3: BRONZE}.get(rank_num, CYAN)
            row_fill = (47, 49, 54, 244) if rank_num > 3 else (55, 52, 42, 246)
            _draw_rounded_rect(draw, (row_x1, y, row_x2, y + row_h), 18, fill=row_fill, outline=BORDER, width=1)
            draw.rounded_rectangle((row_x1, y, row_x1 + 7, y + row_h), radius=4, fill=rank_color)

            rank_text = f'#{rank_num:,}'
            _draw_fitted_text(draw, (90, y + row_h // 2), rank_text, f_rank, rank_color, 66, anchor='mm')

            avatar = _profile_card_avatar(entry.get('avatar_url'), entry.get('display_name', ''), 44, timeout=2.5)
            layer.alpha_composite(avatar, (126, y + 7))

            seconds = _as_int(entry.get('study_seconds', 0))
            time_text = _study_leaderboard_time(entry.get('study_seconds', 0))
            time_w = min(182, max(112, _profile_card_text_width(draw, time_text, f_time) + 34))
            time_x2 = row_x2 - 24
            time_x1 = time_x2 - time_w
            name_x = 190
            name_max = max(120, time_x1 - name_x - 22)
            _draw_fitted_text(draw, (name_x, y + 15), entry.get('display_name', 'Unknown'), f_name, TEXT, name_max)
            draw.text((name_x, y + 40), 'TODAY STUDY', font=f_meta, fill=SOFT)
            progress_x1 = name_x + 104
            progress_x2 = max(progress_x1 + 40, time_x1 - 24)
            _draw_gradient_progress_bar(
                layer,
                draw,
                (progress_x1, y + 45, progress_x2, y + 51),
                seconds / max(1, top_seconds),
                background=(30, 31, 36, 255),
                outline=(30, 31, 36, 255),
                start=BLURPLE,
                end=GREEN,
            )
            draw.rounded_rectangle((time_x1, y + 14, time_x2, y + 44), radius=15, fill=(35, 165, 90, 44), outline=(35, 165, 90, 150), width=1)
            _draw_fitted_text(draw, ((time_x1 + time_x2) // 2, y + 29), time_text, f_time, TEXT, time_w - 22, anchor='mm')

    draw.text((W // 2, H - 44), 'Live sessions are included in today\'s totals', anchor='mm', font=f_footer, fill=SOFT)
    final = Image.alpha_composite(base, layer)
    mask = Image.new('L', (W, H), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, W, H), radius=34, fill=255)
    out = Image.new('RGBA', (W, H), (0, 0, 0, 0))
    out.paste(final, (0, 0), mask)
    buf = io.BytesIO()
    out.save(buf, format='PNG', optimize=True)
    buf.seek(0)
    return buf.getvalue()


class StudyLeaderboardView(discord.ui.View):
    def __init__(self, author_id: int, entries: list[dict], today: str):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.entries = entries
        self.today = today
        self.page = 1
        self.per_page = STUDY_LEADERBOARD_PER_PAGE
        self.total_pages = max(1, math.ceil(len(entries) / self.per_page))
        self._sync_buttons()

    def _page_entries(self) -> list[dict]:
        start = (self.page - 1) * self.per_page
        return self.entries[start:start + self.per_page]

    def _image_file(self) -> discord.File:
        image = render_study_leaderboard_image(
            self._page_entries(),
            self.page,
            self.total_pages,
            self.today,
            len(self.entries),
            self.entries[0].get('study_seconds', 0) if self.entries else 0,
        )
        return discord.File(io.BytesIO(image), filename=f'study_leaderboard_{self.page}.png')

    def _sync_buttons(self):
        if len(self.children) < 5:
            return
        self.children[0].disabled = self.page <= 1
        self.children[1].disabled = self.page <= 1
        self.children[2].label = f'{self.page}/{self.total_pages}'
        self.children[2].disabled = True
        self.children[3].disabled = self.page >= self.total_pages
        self.children[4].disabled = self.page >= self.total_pages

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        is_admin = bool(getattr(getattr(interaction.user, 'guild_permissions', None), 'administrator', False))
        if interaction.user.id == self.author_id or is_admin:
            return True
        await interaction.response.send_message('Bạn không thể điều khiển bảng xếp hạng này.', ephemeral=True)
        return False

    async def _edit_page(self, interaction: discord.Interaction):
        self._sync_buttons()
        await interaction.response.edit_message(attachments=[self._image_file()], view=self)

    @discord.ui.button(label='First', style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = 1
        await self._edit_page(interaction)

    @discord.ui.button(label='Prev', style=discord.ButtonStyle.primary)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(1, self.page - 1)
        await self._edit_page(interaction)

    @discord.ui.button(label='1/1', style=discord.ButtonStyle.secondary, disabled=True)
    async def page_indicator(self, interaction: discord.Interaction, button: discord.ui.Button):
        return

    @discord.ui.button(label='Next', style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.total_pages, self.page + 1)
        await self._edit_page(interaction)

    @discord.ui.button(label='Last', style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = self.total_pages
        await self._edit_page(interaction)


def _previous_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _monthly_study_seconds(data: dict, year: int, month: int, guild: discord.Guild | None = None) -> list[int]:
    days = calendar.monthrange(year, month)[1]
    totals = [0] * days
    for uid, info in data.items():
        if not isinstance(info, dict):
            continue
        daily = info.get('daily', {})
        if not isinstance(daily, dict):
            continue
        for day in range(1, days + 1):
            totals[day - 1] += _as_int(daily.get(f'{year:04d}-{month:02d}-{day:02d}', 0))

    now = datetime.now()
    if year == now.year and month == now.month:
        today_index = now.day - 1
        tracked_member_ids: set[int] = set()
        for uid in data.keys():
            try:
                tracked_member_ids.add(int(uid))
            except (TypeError, ValueError):
                continue
        for member_id in list(join_times.keys()):
            if member_id not in tracked_member_ids:
                continue
            if guild and not guild.get_member(member_id):
                continue
            totals[today_index] += _get_unsaved_study_seconds(member_id, now)
    return totals


def render_monthly_study_chart_image(
    current: list[int],
    previous: list[int],
    now: datetime,
    subject: str,
) -> bytes:
    W, H = STUDY_CHART_W, STUDY_CHART_H
    base = _profile_card_background(W, H).convert('RGBA')
    base.alpha_composite(Image.new('RGBA', (W, H), (0, 0, 0, 116)))
    layer = Image.new('RGBA', (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)

    f_title = _card_font(34, True)
    f_sub = _card_font(18)
    f_axis = _card_font(13)
    f_summary = _card_font(20, True)
    f_legend = _card_font(16, True)

    TEXT = (248, 250, 252, 255)
    MUTED = (203, 213, 225, 230)
    SOFT = (148, 163, 184, 230)
    GOLD = (250, 204, 21, 235)
    BLUE = (56, 189, 248, 210)

    _draw_rounded_rect(draw, (24, 22, W - 24, H - 22), 30, fill=(6, 18, 30, 168), outline=(255, 255, 255, 36), width=2)
    draw.text((52, 54), 'MONTHLY STUDY CHART', font=f_title, fill=TEXT)
    draw.text((54, 95), f'{subject} • This month vs last month', font=f_sub, fill=MUTED)

    chart_x, chart_y, chart_w, chart_h = 72, 142, 760, 284
    _draw_rounded_rect(draw, (chart_x - 18, chart_y - 20, chart_x + chart_w + 18, chart_y + chart_h + 44), 22, fill=(13, 45, 64, 132), outline=(255, 255, 255, 22), width=1)

    max_days = max(len(current), len(previous), 1)
    max_hours = max(max(current or [0]), max(previous or [0]), 3600) / 3600
    max_hours = max(1.0, max_hours * 1.12)

    for tick in range(5):
        ratio = tick / 4
        y = chart_y + chart_h - int(chart_h * ratio)
        draw.line((chart_x, y, chart_x + chart_w, y), fill=(255, 255, 255, 24), width=1)
        label = f'{max_hours * ratio:.0f}h'
        draw.text((chart_x - 12, y), label, anchor='rm', font=f_axis, fill=SOFT)

    group_w = chart_w / max_days
    bar_w = max(4, int(group_w * 0.28))
    for day in range(1, max_days + 1):
        cur = current[day - 1] if day <= len(current) else 0
        prev = previous[day - 1] if day <= len(previous) else 0
        center = chart_x + (day - 0.5) * group_w
        for value, color, offset in ((prev, BLUE, -bar_w * 0.6), (cur, GOLD, bar_w * 0.6)):
            bar_h = int(chart_h * ((value / 3600) / max_hours))
            x1 = int(center + offset - bar_w / 2)
            x2 = x1 + bar_w
            y1 = chart_y + chart_h - bar_h
            y2 = chart_y + chart_h
            if bar_h > 0:
                _draw_rounded_rect(draw, (x1, y1, x2, y2), min(5, bar_w // 2), fill=color)
        if day == 1 or day == max_days or day % 5 == 0:
            draw.text((int(center), chart_y + chart_h + 12), str(day), anchor='mt', font=f_axis, fill=SOFT)

    legend_y = 454
    draw.rounded_rectangle((56, legend_y, 72, legend_y + 16), radius=5, fill=GOLD)
    draw.text((82, legend_y - 1), 'This month', font=f_legend, fill=MUTED)
    draw.rounded_rectangle((202, legend_y, 218, legend_y + 16), radius=5, fill=BLUE)
    draw.text((228, legend_y - 1), 'Last month', font=f_legend, fill=MUTED)

    current_total = sum(current)
    previous_total = sum(previous)
    summary = f'This month: {format_time(current_total)}   •   Last month: {format_time(previous_total)}'
    draw.text((W // 2, 506), summary, anchor='mm', font=f_summary, fill=TEXT)

    final = Image.alpha_composite(base, layer)
    buf = io.BytesIO()
    final.save(buf, format='PNG', optimize=True)
    buf.seek(0)
    return buf.getvalue()

# ─── ROLE MANAGEMENT ─────────────────────────────────────────────────────────

def _iter_chunks(items: list, size: int):
    """Yield successive chunks of `size` from `items`."""
    if size <= 0:
        raise ValueError('Chunk size must be positive.')
    for idx in range(0, len(items), size):
        yield items[idx:idx + size]


async def _fetch_member_from_guild(guild: discord.Guild, member_id: int) -> discord.Member | None:
    """Fetch a member from the Discord API when the cache misses."""
    try:
        return await guild.fetch_member(member_id)
    except discord.NotFound:
        return None
    except discord.Forbidden:
        log.warning(f'[MemberFetch] Missing access to fetch {member_id} in {guild.name}')
    except discord.HTTPException as e:
        log.warning(f'[MemberFetch] Failed to fetch {member_id} in {guild.name}: {e}')
    return None


def _persisted_class_role_ids(guild_id: int) -> dict[int, int]:
    initialize_database()
    return repository.get_class_roles(int(guild_id))


def _save_guild_class_role_ids(guild_id: int, role_ids: dict[int, int]):
    initialize_database()
    repository.save_class_roles(
        int(guild_id),
        {
            level: (role_id, CLASS_ROLE_NAMES.get(level, ''))
            for level, role_id in sorted(role_ids.items())
            if level in CLASS_ROLE_NAMES and role_id
        },
    )


def _find_class_role_by_level(guild: discord.Guild, level: int) -> discord.Role | None:
    role_name = CLASS_ROLE_NAMES.get(level)
    if not role_name:
        return None

    role = discord.utils.get(guild.roles, name=role_name)
    if role:
        return role

    persisted_id = _persisted_class_role_ids(guild.id).get(level)
    if persisted_id:
        role = guild.get_role(persisted_id)
        if role:
            return role

    return None


def _known_class_role_ids(guild: discord.Guild, role_ids: dict[int, int] | None = None) -> set[int]:
    ids = {int(role_id) for role_id in (role_ids or {}).values() if role_id}
    ids.update(_persisted_class_role_ids(guild.id).values())
    for level in CLASS_ROLE_NAMES:
        role = discord.utils.get(guild.roles, name=CLASS_ROLE_NAMES[level])
        if role:
            ids.add(role.id)
    return ids


def _unmanageable_class_roles(guild: discord.Guild, role_ids: dict[int, int]) -> list[discord.Role]:
    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member:
        return []
    roles = []
    for role_id in role_ids.values():
        role = guild.get_role(role_id)
        if role and role >= bot_member.top_role:
            roles.append(role)
    return roles


async def ensure_class_roles(guild: discord.Guild) -> dict[int, int]:
    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member:
        log.error(f'[RoleSetup] Could not resolve bot member in {guild.name}')
        return {}

    if not bot_member.guild_permissions.manage_roles:
        log.error(f'[RoleSetup] Bot thiếu quyền Manage Roles trong {guild.name}')
        return {}

    role_ids: dict[int, int] = {}
    for level, role_name in CLASS_ROLE_NAMES.items():
        role = _find_class_role_by_level(guild, level)

        if role is None:
            try:
                role = await guild.create_role(
                    name=role_name,
                    reason='Auto-create class role for study economy bot',
                )
                log.info(f'[RoleSetup] Created role {role_name} in {guild.name}')
            except discord.Forbidden:
                log.error(f'[RoleSetup] Không đủ quyền tạo role {role_name} trong {guild.name}')
                continue
            except discord.HTTPException as e:
                log.error(f'[RoleSetup] Lỗi tạo role {role_name} trong {guild.name}: {e}')
                continue
            except Exception as e:
                log.error(f'[RoleSetup] Lỗi tạo role {role_name} trong {guild.name}: {e}', exc_info=True)
                continue

        if role >= bot_member.top_role:
            log.warning(
                f'[RoleSetup] Role {role.name} cao hơn hoặc bằng role bot '
                f'({bot_member.top_role.name}) trong {guild.name}; bot không thể gán role này.'
            )

        role_ids[level] = role.id

    if role_ids:
        _save_guild_class_role_ids(guild.id, role_ids)
    return role_ids


def _get_level_role_name(role_id: int | None, guild: discord.Guild | None = None) -> str | None:
    """Resolve a role ID to its display name, falling back across known guilds."""
    if not role_id:
        return None

    if guild:
        role = guild.get_role(role_id)
        if role:
            return role.name

    for known_guild in bot.guilds:
        if guild and known_guild.id == guild.id:
            continue
        role = known_guild.get_role(role_id)
        if role:
            return role.name

    return f'ID:{role_id}'


async def _ensure_role_synced(
    member: discord.Member,
    current_level: int,
    role_ids: dict[int, int] | None = None,
):
    """Assign the correct money-class role to `member` and remove stale ones.

    Uses a per-member lock to serialize concurrent syncs. Validates bot
    permissions and role hierarchy before touching anything. Retries transient
    HTTP errors with backoff. Assigns the expected role first, verifies it,
    then removes stale roles — so a mid-way failure never leaves the member
    without any class role.
    """
    lock = _role_sync_locks.setdefault(member.id, asyncio.Lock())
    async with lock:
        current_level = max(0, min(int(current_level), len(LEVEL_THRESHOLDS) - 1))
        guild = member.guild
        member_name = member.display_name
        role_ids = role_ids if role_ids is not None else await ensure_class_roles(guild)
        if not role_ids:
            return

        expected_role_id = role_ids.get(current_level)
        expected_role = guild.get_role(expected_role_id) if expected_role_id else None

        if current_level in CLASS_ROLE_NAMES and (not expected_role_id or not expected_role):
            role_ids = await ensure_class_roles(guild)
            expected_role_id = role_ids.get(current_level)
            expected_role = guild.get_role(expected_role_id) if expected_role_id else None

        if current_level in CLASS_ROLE_NAMES and not expected_role:
            log.error(f'[RoleSync] No class role available for class {current_level} in {guild.name}')
            return

        if expected_role_id and not expected_role:
            log.error(f'[RoleSync] Role ID {expected_role_id} for class {current_level} not found in {guild.name}')
            return

        bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
        if not bot_member:
            log.error(f'[RoleSync] Could not resolve bot member in {guild.name}')
            return

        if not bot_member.guild_permissions.manage_roles:
            log.error(f'[RoleSync] Bot missing Manage Roles permission in {guild.name}')
            return

        bot_top = bot_member.top_role
        if expected_role and expected_role >= bot_top:
            log.error(
                f'[RoleSync] Cannot assign {expected_role.name} in {guild.name} '
                f'because bot top role ({bot_top.name}) is not higher'
            )
            return

        async def _reload_member_state() -> discord.Member | None:
            cached_member = guild.get_member(member.id)
            try:
                return await guild.fetch_member(member.id)
            except discord.NotFound:
                log.warning(f'[RoleSync] Member {member_name} is no longer in {guild.name}')
                return None
            except discord.Forbidden:
                if cached_member:
                    log.warning(f'[RoleSync] Using cached roles for {member_name}; fetch_member forbidden in {guild.name}')
                    return cached_member
                log.error(f'[RoleSync] Cannot refresh member state for {member_name} in {guild.name}')
                return None
            except discord.HTTPException as e:
                if cached_member:
                    log.warning(f'[RoleSync] Fresh role fetch failed for {member_name}: {e}. Using cache.')
                    return cached_member
                log.error(f'[RoleSync] Fresh role fetch failed for {member_name}: {e}')
                return None

        async def _run_role_op(action: str, runner):
            for attempt in range(1, 4):
                try:
                    await runner()
                    return
                except discord.Forbidden:
                    raise
                except discord.HTTPException as e:
                    if attempt == 3:
                        raise
                    log.warning(
                        f'[RoleSync] {action} attempt {attempt}/3 failed for {member_name}: {e}. Retrying...'
                    )
                    await asyncio.sleep(0.75 * attempt)

        refreshed_member = await _reload_member_state()
        if refreshed_member is None:
            return
        member = refreshed_member
        current_roles = tuple(member.roles)
        has_expected = bool(expected_role and expected_role in current_roles)

        # 1) Assign the expected role first (so the member is never roleless mid-sync).
        if expected_role and not has_expected:
            try:
                await _run_role_op(
                    f'Assigning {expected_role.name}',
                    lambda: member.add_roles(expected_role, reason=f'Role sync -> class {current_level}'),
                )
                log.info(f'[RoleSync] Assigned {expected_role.name} to {member_name}')
            except discord.Forbidden:
                log.error(f'[RoleSync] Forbidden assigning {expected_role.name} to {member_name}')
                return
            except Exception as e:
                log.error(f'[RoleSync] Error assigning role to {member_name}: {e}')
                return

            refreshed_member = await _reload_member_state()
            if refreshed_member is None:
                return
            member = refreshed_member
            current_roles = tuple(member.roles)
            if expected_role not in current_roles:
                log.warning(f'[RoleSync] Expected role {expected_role.name} still missing for {member_name}; skipping removals.')
                return

        # 2) Remove stale class roles (but only ones the bot can actually manage).
        roles_to_remove = []
        class_role_ids = _known_class_role_ids(guild, role_ids)
        for role in current_roles:
            if role.id in class_role_ids and role != expected_role:
                roles_to_remove.append(role)

        if not roles_to_remove:
            return

        manageable = [role for role in roles_to_remove if role < bot_top]
        unmanageable = [role for role in roles_to_remove if role >= bot_top]
        if unmanageable:
            log.warning(
                f'[RoleSync] Cannot remove {[role.name for role in unmanageable]} '
                f'from {member_name} due to hierarchy'
            )
        if manageable:
            try:
                await _run_role_op(
                    'Removing stale class roles',
                    lambda: member.remove_roles(*manageable, reason=f'Role sync -> class {current_level}'),
                )
                log.info(f'[RoleSync] Removed {[role.name for role in manageable]} from {member_name}')
            except discord.Forbidden:
                log.error(f'[RoleSync] Forbidden removing roles from {member_name}')
            except Exception as e:
                log.error(f'[RoleSync] Error removing roles from {member_name}: {e}')


async def _sync_guild_class_members(
    guild: discord.Guild,
    role_ids: dict[int, int] | None = None,
) -> tuple[int, int]:
    role_ids = role_ids if role_ids is not None else await ensure_class_roles(guild)
    if not role_ids:
        return 0, 0

    with guild_data_context(guild.id):
        data = load_data()
    updated = 0
    skipped = 0
    members_to_sync: list[tuple[discord.Member, int]] = []
    for uid, info in data.items():
        try:
            member_id = int(uid)
        except (ValueError, TypeError):
            continue
        member = guild.get_member(member_id) or await _fetch_member_from_guild(guild, member_id)
        if not member:
            skipped += 1
            continue
        members_to_sync.append((member, info.get('class', info.get('level', 0))))
        updated += 1

    batches = list(_iter_chunks(members_to_sync, ROLE_SYNC_BATCH_SIZE))
    for idx, batch in enumerate(batches):
        await asyncio.gather(
            *[_ensure_role_synced(member, level, role_ids=role_ids) for member, level in batch],
            return_exceptions=True,
        )
        if idx < len(batches) - 1:
            await asyncio.sleep(ROLE_SYNC_BATCH_DELAY)

    return updated, skipped

# ─── SESSION MANAGEMENT ──────────────────────────────────────────────────────

def record_join(member: discord.Member):
    _capture_guild_context(member.guild.id)
    runtime_member_guild_ids[member.id] = member.guild.id
    now   = datetime.now()
    today = now.strftime('%Y-%m-%d')
    join_times[member.id]      = now
    last_checkpoint[member.id] = now
    milestone_sent[member.id]  = set()
    reset_cam_notification(member.id)
    session_counts[member.id]  = session_counts.get(member.id, 0) + 1
    if today not in daily_first_join:
        daily_first_join[today] = member.id
    uid = str(member.id)
    if now.hour < 6:
        award_special_flag(uid, 'night_owl')
    if now.hour < 8:
        award_special_flag(uid, 'early_bird')
    if member.voice and is_media_active(member.voice):
        media_active_members.add(member.id)
    else:
        media_active_members.discard(member.id)
    save_runtime_state()
    log.info(f'{member.display_name} bắt đầu học lúc {now.strftime("%H:%M:%S")}')

def _rebuild_daily_session_state(now: datetime):
    today = now.strftime('%Y-%m-%d')
    current_focus_members: list[tuple[datetime, int]] = []
    for mid, joined_at in join_times.items():
        current_focus_members.append((joined_at, mid))
        session_counts[mid] = max(1, session_counts.get(mid, 0))
    current_focus_members.sort(key=lambda item: item[0])
    if current_focus_members:
        daily_first_join[today] = current_focus_members[0][1]

def _get_unsaved_study_seconds(member_id: int, now: datetime | None = None) -> int:
    window = _get_pending_study_window(member_id, now or datetime.now())
    if not window:
        return 0
    start_time, end_time = window
    return max(0, int((end_time - start_time).total_seconds()))

async def _do_checkpoint(member: discord.Member, now: datetime | None = None) -> tuple[int, dict]:
    _capture_guild_context(member.guild.id)
    if member.id not in join_times:
        return 0, _default_progress_result()
    now = now or datetime.now()
    window = _get_pending_study_window(member.id, now)
    result = _default_progress_result()
    if not window:
        return 0, result

    start_time, end_time = window
    elapsed = max(0, int((end_time - start_time).total_seconds()))
    if elapsed > 0:
        saved_result = add_study_time(
            member.id,
            member.display_name,
            elapsed,
            start_time=start_time,
            end_time=end_time,
        )
        if saved_result:
            result.update(saved_result)
            log.info(f'[Checkpoint] {member.display_name}: +{format_time(elapsed)}')
    return elapsed, result

async def _check_milestones(member: discord.Member):
    if member.id not in join_times:
        return
    await notify_study_milestones(member)

async def _check_quests_and_badges(member: discord.Member, channel=None):
    _capture_guild_context(member.guild.id)
    uid   = str(member.id)
    today = datetime.now().strftime('%Y-%m-%d')
    generate_daily_quests(uid, today)
    data       = load_data()
    saved_secs = data.get(uid, {}).get('daily', {}).get(today, 0)
    real_secs = saved_secs + _get_unsaved_study_seconds(member.id)
        
    update_quest_progress(uid, today, override_today_secs=real_secs)
    data_fresh = load_data()
    new_class  = data_fresh.get(uid, {}).get('class', data_fresh.get(uid, {}).get('level', 0))
    claim_completed_quest_notifications(uid, today)

    await _ensure_role_synced(member, new_class)
    await notify_coin_milestones(member, channel)
        
    check_and_award_badges(uid, member)

async def record_leave_and_notify(member: discord.Member) -> int:
    _capture_guild_context(member.guild.id)
    if member.id not in join_times: return 0

    now        = datetime.now()
    _, result = await _do_checkpoint(member, now)
    if result.get('level_up'):
        await _ensure_role_synced(member, result['new_level'])

    total_duration = int((now - join_times.pop(member.id)).total_seconds())
    last_checkpoint.pop(member.id, None)
    milestone_sent.pop(member.id, None)
    runtime_member_guild_ids.pop(member.id, None)
    media_active_members.discard(member.id)
    reset_cam_notification(member.id)
    save_runtime_state()

    uid   = str(member.id)
    today = now.strftime('%Y-%m-%d')
    generate_daily_quests(uid, today)
    data_now   = load_data()
    final_secs = data_now.get(uid, {}).get('daily', {}).get(today, 0)

    update_quest_progress(uid, today, override_today_secs=final_secs)
    claim_completed_quest_notifications(uid, today)

    check_and_award_badges(uid, member)
    
    data_final = load_data()
    if uid in data_final:
        info       = data_final[uid]
        class_idx  = min(info.get('class', info.get('level', 0)), len(CLASS_NAMES) - 1)
        balance    = info.get('balance', 0)
        total_earned = info.get('total_earned', 0)
        debt       = _active_debt(info)
        streak     = info.get('streak', 0)
        today_secs = info['daily'].get(today, 0)
        today_earned = info.get('daily_earnings', {}).get(today, 0)

        await _ensure_role_synced(member, class_idx)

        if total_duration > 30:
            await notify_session_finished(
                member=member,
                session_time=format_time(total_duration),
                today_time=format_time(today_secs),
                earned_today=today_earned,
                balance=balance,
                debt=debt,
                current_class=class_label(class_idx),
                total_earned=total_earned,
                streak=streak,
            )
            
    return total_duration

# ─── LIVE MESSAGE ─────────────────────────────────────────────────────────────

def _guild_focus_channel_ids(guild: discord.Guild) -> set[int]:
    config = get_guild_config(guild.id)
    ids = {_as_int(ch) for ch in config.get('focus_channel_ids', []) if _as_int(ch)}
    for meta in list(temp_rooms.values()):
        if meta.get('guild_id') == guild.id and _room_mode(meta.get('mode')) == 'study':
            room_id = _as_int(meta.get('room_id'))
            if room_id:
                ids.add(room_id)
    try:
        for room in repository.list_active_private_rooms(guild.id):
            if _room_mode(room.get('mode')) != 'study':
                continue
            room_id = _as_int(room.get('channel_id'))
            if room_id:
                ids.add(room_id)
    except Exception as e:
        log.warning('[FocusChannels] Could not load private rooms for guild %s: %s', guild.id, e)
    return ids

def _channel_belongs_to_guild_focus(channel, guild: discord.Guild) -> bool:
    if not channel:
        return False
    if channel.guild.id != guild.id:
        return False
    return channel.id in _guild_focus_channel_ids(guild)

async def _update_live_message_for_channel(channel):
    if channel and getattr(channel, 'guild', None):
        await update_live_message(channel.guild)

async def update_live_message(guild: discord.Guild):
    config = get_guild_config(guild.id)
    report_channel_id = config.get('report_channel_id')
    channel = bot.get_channel(report_channel_id) if report_channel_id else None
    if not channel: return
    now       = datetime.now()
    voice_ids = _guild_focus_channel_ids(guild)
    today     = now.strftime('%Y-%m-%d')
    with guild_data_context(guild.id):
        data = _get_live_enriched_data(guild)
    active    = []
    for mid, start_time in list(join_times.items()):
        m = guild.get_member(mid)
        if not m or not m.voice or not m.voice.channel: continue
        if m.voice.channel.id not in voice_ids: continue
        uid   = str(mid)
        saved = data.get(uid, {}).get('daily', {}).get(today, 0)
        today_total = saved + _get_unsaved_study_seconds(mid, now)
        active.append({
            'm':       m,
            'session': int((now - start_time).total_seconds()),
            'today':   today_total,
            'icon':    media_status_icon(m.voice),
        })
    active.sort(key=lambda x: x['today'], reverse=True)
    lines = [
        f'🔴 **ĐANG HỌC** · `{now.strftime("%H:%M:%S")}`',
        '━━━━━━━━━━━━━━━━━━━━━━━━',
    ]
    if not active:
        lines.append('😴 _Chưa có ai trong phòng học..._')
    else:
        for i, a in enumerate(active, 1):
            rank = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
            lines.append(
                f'{rank} {a["icon"]} **{a["m"].display_name}**'
                f' | Phiên: `{format_time(a["session"])}` | Hôm nay: `{format_time(a["today"])}`'
            )
    total_today = sum(a['today'] for a in active)
    lines += [
        '━━━━━━━━━━━━━━━━━━━━━━━━',
        f'👥 `{len(active)} người` · Tổng: `{format_time(total_today)}`',
        f'_⟳ Cập nhật mỗi {LIVE_UPDATE_MINUTES} phút_',
    ]
    content = '\n'.join(lines)
    try:
        msg_id = live_message_ids.get(channel.id)
        if msg_id:
            try:
                old = await channel.fetch_message(msg_id)
                await old.edit(content=content)
                return
            except discord.NotFound:
                live_message_ids.pop(channel.id, None)
        new = await channel.send(content, silent=True)
        live_message_ids[channel.id] = new.id
    except Exception as e:
        log.error(f'Live message error: {e}')

async def update_all_live_messages():
    for guild in bot.guilds:
        await update_live_message(guild)

# ─── DAILY BOARD ─────────────────────────────────────────────────────────────

async def _send_daily_board(target_date: str | None = None, guild: discord.Guild | None = None):
    if target_date is None:
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        report_date = target_date

    target_guilds = [guild] if guild else list(bot.guilds)
    for target_guild in target_guilds:
        config = get_guild_config(target_guild.id)
        report_channel_id = config.get('report_channel_id')
        ch = bot.get_channel(report_channel_id) if report_channel_id else None
        if not ch:
            continue
        with guild_data_context(target_guild.id):
            data = load_data()
        sorted_data = sorted(
            data.items(),
            key=lambda x: x[1].get('daily', {}).get(report_date, 0),
            reverse=True,
        )
        has_data = any(info.get('daily', {}).get(report_date, 0) > 0 for _, info in sorted_data)
        day_fmt  = datetime.strptime(report_date, '%Y-%m-%d').strftime('%d/%m/%Y')

        lines = [
            f'📊 **BÁO CÁO NGÀY {day_fmt}**',
            f'━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
        ]

        if not has_data:
            lines.append('😴 Hôm nay chưa có ai vào học!')
        else:
            rank = 0
            for uid, info in sorted_data:
                t = info.get('daily', {}).get(report_date, 0)
                if t <= 0:
                    continue
                rank  += 1
                medal  = ['🥇', '🥈', '🥉'][rank-1] if rank <= 3 else f'`{rank}.`'
                class_idx = info.get('class', info.get('level', 0))
                streak = info.get('streak', 0)
                earned = info.get('daily_earnings', {}).get(report_date, 0)
                total  = info.get('total', 0)
                lines.append(
                    f'{medal} **{info["name"]}**'
                    f' · `{class_label(class_idx)}` · 🔥`{streak}d`'
                    f'\n       ⏱️ Hôm nay: `{format_time(t)}`'
                    f'  |  📚 Tổng: `{format_time(total)}`'
                    f'  |  💰 `{format_coins(earned)}`'
                )

        total_today  = sum(info.get('daily', {}).get(report_date, 0) for _, info in sorted_data)
        active_count = sum(1 for _, info in sorted_data if info.get('daily', {}).get(report_date, 0) > 0)
        lines += [
            f'━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
            f'👥 **{active_count} người** học hôm nay · ⏱️ Tổng: **{format_time(total_today)}**',
            f'_Bảng tổng kết tự động mỗi ngày lúc 00:00 🕛_',
        ]

        chunks      = []
        current     = []
        current_len = 0
        for line in lines:
            line_len = len(line) + 1
            if current_len + line_len > 1900 and current:
                chunks.append('\n'.join(current))
                current     = [line]
                current_len = line_len
            else:
                current.append(line)
                current_len += line_len
        if current:
            chunks.append('\n'.join(current))

        try:
            for chunk in chunks:
                await ch.send(chunk)
            log.info(f'[DailyBoard] Gửi ngày {report_date} → #{ch.name}')
        except Exception as e:
            log.error(f'[DailyBoard] Lỗi: {e}')

# ─── REMIND SYSTEM ───────────────────────────────────────────────────────────

async def _remind_loop(member: discord.Member, hour: int):
    log.info('[Remind] Study reminder task skipped because reminders are disabled for member_id=%s.', member.id)
    return

# ─── HELPERS ─────────────────────────────────────────────────────────────────

async def safe_send_dm(member: discord.Member, message: str, *, respect_user_setting: bool = True):
    title, color = _notice_title_color_from_text(message)
    await send_private_notify_embed(
        member=member,
        title=title,
        description=_compact_notice_description(message),
        color=color,
        respect_user_setting=respect_user_setting,
    )

async def _cancel_reminder_task(member_id: int):
    old = remind_tasks.pop(member_id, None)
    if not old:
        return

    old_task = old[1]
    if old_task and not old_task.done():
        old_task.cancel()
        try:
            await old_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f'[Remind] Lỗi khi huỷ task cho {member_id}: {e}')

def bot_can_move(member): return member.guild.me.guild_permissions.move_members

def cancel_task(mid: int):
    t = pending_checks.pop(mid, None)
    if t and not t.done(): t.cancel()

def start_check(member: discord.Member, reason: str):
    cancel_task(member.id)
    _capture_guild_context(member.guild.id)
    pending_checks[member.id] = asyncio.create_task(check_media(member))
    log.info(f'{member.display_name} {reason} → {WAIT_SECONDS}s countdown.')

# ─── TEMPORARY ROOM HELPERS ──────────────────────────────────────────────────

def _is_create_room_channel(channel) -> bool:
    if not channel or not getattr(channel, 'guild', None):
        return False
    config = get_guild_config(channel.guild.id)
    return bool(config.get('create_room_channel_id') == channel.id)

def _is_temporary_room_id(channel_id: int | None) -> bool:
    return bool(_temp_room_key_for_channel_id(channel_id))

def _temporary_room_mode(channel_id: int | None) -> str | None:
    if not channel_id:
        return None
    meta = _temp_room_meta(channel_id)
    if not meta:
        return None
    return _room_mode(meta.get('mode'))

def _is_temporary_study_room_id(channel_id: int | None) -> bool:
    return _temporary_room_mode(channel_id) == 'study'

def is_focus_channel(channel_id: int | None) -> bool:
    if not channel_id:
        return False
    if _is_temporary_room_id(channel_id):
        return _is_temporary_study_room_id(channel_id)
    channel = bot.get_channel(channel_id)
    if channel and getattr(channel, 'guild', None):
        return channel_id in _guild_focus_channel_ids(channel.guild)
    for config in repository.list_guild_configs():
        if channel_id in set(config.get('focus_channel_ids') or []):
            return True
    return False

def _room_mode(mode: str | None) -> str:
    return 'entertainment' if str(mode or '').lower() == 'entertainment' else 'study'

def _room_mode_label(mode: str | None) -> str:
    return 'Phòng giải trí' if _room_mode(mode) == 'entertainment' else 'Phòng học'

def _temp_room_name(member: discord.Member, mode: str = 'study') -> str:
    clean_name = ' '.join(member.display_name.split()).strip() or f'User {member.id}'
    prefix = '🎮' if _room_mode(mode) == 'entertainment' else '📚'
    return f'{prefix} Phòng của {clean_name[:80]}'

def _register_temporary_room(channel: discord.VoiceChannel, owner: discord.Member, mode: str = 'study'):
    temp_rooms[_temp_room_key(channel.guild.id, channel.id)] = {
        'room_id': channel.id,
        'owner_id': owner.id,
        'guild_id': channel.guild.id,
        'created_at': datetime.now(),
        'mode': _room_mode(mode),
    }
    save_runtime_state()

def _remove_temporary_room_tracking(channel_id: int):
    key = _temp_room_key_for_channel_id(channel_id)
    if key:
        temp_rooms.pop(key, None)

    task = temporary_room_delete_tasks.pop(channel_id, None)
    if task and not task.done():
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        if task is not current_task:
            task.cancel()
    save_runtime_state()

async def _delete_temporary_room(channel, reason: str, finalize_members: bool = True) -> bool:
    if not channel or not _is_temporary_room_id(channel.id):
        return False

    try:
        if finalize_members:
            await _finalize_temporary_room_members(channel, reason)
        await channel.delete(reason=reason)
        log.info(f'[TempRoom] Deleted {channel.name} ({channel.id})')
        _remove_temporary_room_tracking(channel.id)
        return True
    except discord.Forbidden:
        log.error(f'[TempRoom] Missing permission to delete room {channel.id}')
    except discord.HTTPException as e:
        log.error(f'[TempRoom] Failed to delete room {channel.id}: {e}')
    except Exception as e:
        log.error(f'[TempRoom] Unexpected delete error for {channel.id}: {e}', exc_info=True)
    return False

async def _delete_empty_temporary_room_after_delay(channel):
    room_id = channel.id
    try:
        await asyncio.sleep(TEMP_ROOM_DELETE_DELAY_SECONDS)
        if not _is_temporary_room_id(room_id):
            return

        current_channel = bot.get_channel(room_id)
        if not current_channel:
            _remove_temporary_room_tracking(room_id)
            return

        await _checkpoint_temporary_room_members(current_channel)
        non_bot_members = [m for m in getattr(current_channel, 'members', []) if not m.bot]
        if non_bot_members:
            log.info(f'[TempRoom] Skip delete {room_id}; room has members again.')
            return

        await _delete_temporary_room(current_channel, 'Temporary study room is empty', finalize_members=False)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error(f'[TempRoom] Cleanup task error for {room_id}: {e}', exc_info=True)
    finally:
        if temporary_room_delete_tasks.get(room_id) is asyncio.current_task():
            temporary_room_delete_tasks.pop(room_id, None)

def _schedule_temporary_room_cleanup(channel):
    if not channel or not _is_temporary_room_id(channel.id):
        return

    old_task = temporary_room_delete_tasks.get(channel.id)
    if old_task and not old_task.done():
        old_task.cancel()

    temporary_room_delete_tasks[channel.id] = asyncio.create_task(
        _delete_empty_temporary_room_after_delay(channel)
    )
    log.info(f'[TempRoom] Scheduled cleanup for {channel.name} ({channel.id})')

async def _checkpoint_temporary_room_members(channel):
    for member in list(getattr(channel, 'members', [])):
        if member.bot or member.id not in join_times:
            continue
        try:
            _, result = await _do_checkpoint(member)
            if result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
            await _handle_progress_notifications(member, result, channel)
        except Exception as e:
            log.error(f'[TempRoom] Failed to checkpoint {member.display_name} before cleanup: {e}', exc_info=True)

async def _finalize_temporary_room_members(channel, reason: str):
    if not is_focus_channel(getattr(channel, 'id', None)):
        return
    for member in list(getattr(channel, 'members', [])):
        if member.bot:
            continue
        try:
            await _handle_focus_leave(member, channel, reason=reason)
        except Exception as e:
            log.error(f'[TempRoom] Failed to finalize {member.display_name} before deleting {channel.id}: {e}', exc_info=True)

class TemporaryRoomModeView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=60)
        self.owner_id = int(owner_id)
        self.selected_mode: str | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message('Chỉ người tạo phòng mới chọn mode phòng.', ephemeral=True)
        return False

    async def _choose(self, interaction: discord.Interaction, mode: str):
        self.selected_mode = _room_mode(mode)
        for item in self.children:
            item.disabled = True
        embed = discord.Embed(
            title='Đã chọn mode phòng',
            description=f'Mode: **{_room_mode_label(mode)}**. Bot đang tạo phòng cho bạn.',
            color=0x57F287 if self.selected_mode == 'study' else 0x5865F2,
        )
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    @discord.ui.button(label='Phòng học', style=discord.ButtonStyle.success, custom_id='temp_room_mode_study')
    async def study_mode(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, 'study')

    @discord.ui.button(label='Phòng giải trí', style=discord.ButtonStyle.primary, custom_id='temp_room_mode_entertainment')
    async def entertainment_mode(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, 'entertainment')

async def _ask_temporary_room_mode(member: discord.Member, source_channel) -> str:
    view = TemporaryRoomModeView(member.id)
    embed = discord.Embed(
        title='Chọn mode phòng',
        description=(
            '**Phòng học**\n'
            f'• Cần bật Cam hoặc Stream trong {WAIT_SECONDS}s.\n'
            '• Chỉ thời gian bật Cam/Stream mới được tính học và nhận coins.\n\n'
            '**Phòng giải trí**\n'
            '• Không bắt buộc bật Cam hoặc Stream.\n'
            '• Dùng để chơi game hoặc trò chuyện, không tính thời gian học.\n\n'
            'Nếu hết thời gian chọn, bot sẽ mặc định tạo **Phòng học**.'
        ),
        color=0x5865F2,
    )
    message = None
    try:
        if hasattr(source_channel, 'send'):
            message = await source_channel.send(
                content=member.mention,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
    except discord.HTTPException:
        message = None

    if message is None:
        try:
            message = await member.send(embed=embed, view=view)
        except discord.HTTPException:
            return 'study'

    await view.wait()
    mode = _room_mode(view.selected_mode)
    if view.selected_mode is not None:
        try:
            await message.delete()
        except discord.HTTPException:
            pass
        return mode
    if view.selected_mode is None:
        for item in view.children:
            item.disabled = True
        timeout_embed = discord.Embed(
            title='Hết thời gian chọn mode',
            description='Bot mặc định tạo **Phòng học**.',
            color=0xFEE75C,
        )
        try:
            await message.edit(embed=timeout_embed, view=view)
        except discord.HTTPException:
            pass
    return mode

async def _send_temporary_room_welcome(channel, owner: discord.Member):
    mode = _temporary_room_mode(getattr(channel, 'id', None)) or 'study'
    if mode == 'entertainment':
        title = 'Chào mừng đến với phòng giải trí'
        description = (
            f'Chủ phòng: **{owner.display_name}**\n\n'
            '**Mode phòng**\n'
            '• Đây là **Phòng giải trí**.\n'
            '• Không bắt buộc bật Cam hoặc Stream.\n'
            '• Phòng dùng để chơi game hoặc trò chuyện, không tính thời gian học và không cộng coins học.\n'
            '• Phòng sẽ tự xóa sau khi không còn thành viên thật nào ở lại.\n\n'
            '**Chúc bạn chơi vui :3**'
        )
    else:
        title = 'Chào mừng đến với phòng học tạm'
        description = (
            f'Chủ phòng: **{owner.display_name}**\n\n'
            '**Mode phòng**\n'
            '• Đây là **Phòng học**.\n'
            f'• Bật **Cam 📷 hoặc Stream 📺** trong **{WAIT_SECONDS}s** để bắt đầu tính giờ.\n'
            '• Bot chỉ cộng thời gian học và coins khi bạn đang bật Cam hoặc Stream.\n'
            '• Phòng sẽ tự xóa sau khi không còn thành viên thật nào ở lại.\n\n'
            '**Chúc bạn học vui :3**'
        )
    embed = discord.Embed(
        title=title,
        description=description,
        color=0xFEE75C,
    )
    try:
        await channel.send(content=owner.mention, embed=embed, view=RoomPanelView(), silent=True)
    except AttributeError:
        await safe_send_dm(owner, 'Phòng học tạm đã được tạo. Hãy bật Cam hoặc Stream để bắt đầu tính giờ nhé!')
    except discord.Forbidden:
        log.warning(f'[TempRoom] Missing permission to send welcome in {channel.id}')
    except discord.HTTPException as e:
        log.warning(f'[TempRoom] Failed to send welcome in {channel.id}: {e}')
    except Exception as e:
        log.error(f'[TempRoom] Unexpected welcome error in {channel.id}: {e}', exc_info=True)

async def _handle_create_room_join(member: discord.Member, source_channel) -> bool:
    guild = source_channel.guild
    config, config_error = require_guild_config(guild)
    if config_error:
        log.error(f'[TempRoom] {guild.name} is not configured: {config_error}')
        await safe_send_dm(
            member,
            f'Bot chưa được setup trong server này. Báo admin chạy `/admin setup` nhé.',
            respect_user_setting=False,
        )
        return False

    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member:
        log.error(f'[TempRoom] Cannot resolve bot member in {guild.name}')
        return False

    category = None
    category_id = config.get('temp_room_category_id') if config else None
    if category_id:
        category = guild.get_channel(category_id)
        if not isinstance(category, discord.CategoryChannel):
            log.error(f'[TempRoom] Category {category_id} not found in {guild.name}')
            await safe_send_dm(
                member,
                'Bot không tìm thấy category chứa phòng tạm. Báo admin kiểm tra `/admin setup_status` nhé.',
                respect_user_setting=False,
            )
            return False

        category_perms = category.permissions_for(bot_member)
        if not category_perms.manage_channels:
            log.error(f'[TempRoom] Missing Manage Channels permission in category {category.id}')
            await safe_send_dm(
                member,
                'Bot thiếu quyền Manage Channels trong category phòng tạm.',
                respect_user_setting=False,
            )
            return False
    elif not bot_member.guild_permissions.manage_channels:
        log.error(f'[TempRoom] Missing Manage Channels permission in {guild.name}')
        await safe_send_dm(
            member,
            'Bot thiếu quyền Manage Channels để tạo phòng học tạm.',
            respect_user_setting=False,
        )
        return False

    if not bot_member.guild_permissions.move_members:
        log.error(f'[TempRoom] Missing Move Members permission in {guild.name}')
        await safe_send_dm(
            member,
            'Bot thiếu quyền Move Members để chuyển bạn vào phòng học tạm.',
            respect_user_setting=False,
        )
        return False

    room_mode = await _ask_temporary_room_mode(member, source_channel)
    if not (member.voice and member.voice.channel and member.voice.channel.id == source_channel.id):
        return False

    try:
        temp_channel = await guild.create_voice_channel(
            _temp_room_name(member, room_mode),
            category=category,
            reason=f'Temporary {_room_mode(room_mode)} room for {member} ({member.id})',
        )
    except discord.Forbidden:
        log.error(f'[TempRoom] Missing permission to create room in {guild.name}')
        await safe_send_dm(member, 'Bot thiếu quyền tạo phòng học tạm.', respect_user_setting=False)
        return False
    except discord.HTTPException as e:
        log.error(f'[TempRoom] Failed to create room for {member.display_name}: {e}')
        await safe_send_dm(member, 'Tạo phòng học tạm thất bại. Thử lại sau nhé.', respect_user_setting=False)
        return False
    except Exception as e:
        log.error(f'[TempRoom] Unexpected create error for {member.display_name}: {e}', exc_info=True)
        await safe_send_dm(member, 'Có lỗi khi tạo phòng học tạm. Thử lại sau nhé.', respect_user_setting=False)
        return False

    _register_temporary_room(temp_channel, member, room_mode)

    try:
        if not (member.voice and member.voice.channel and member.voice.channel.id == source_channel.id):
            await _delete_temporary_room(temp_channel, 'Owner left join-to-create channel before move')
            return False
        await member.move_to(temp_channel, reason='Move into temporary study room')
    except discord.Forbidden:
        log.error(f'[TempRoom] Missing permission to move {member.display_name}')
        await safe_send_dm(
            member,
            'Bot tạo được phòng nhưng thiếu quyền chuyển bạn vào phòng.',
            respect_user_setting=False,
        )
        await _delete_temporary_room(temp_channel, 'Failed to move owner into temporary study room')
        return False
    except discord.HTTPException as e:
        log.error(f'[TempRoom] Failed to move {member.display_name}: {e}')
        await safe_send_dm(
            member,
            'Bot tạo được phòng nhưng không chuyển bạn vào được. Thử lại sau nhé.',
            respect_user_setting=False,
        )
        await _delete_temporary_room(temp_channel, 'Failed to move owner into temporary study room')
        return False
    except Exception as e:
        log.error(f'[TempRoom] Unexpected move error for {member.display_name}: {e}', exc_info=True)
        await safe_send_dm(member, 'Có lỗi khi chuyển bạn vào phòng học tạm.', respect_user_setting=False)
        await _delete_temporary_room(temp_channel, 'Unexpected move error')
        return False

    await _send_temporary_room_welcome(temp_channel, member)
    log.info(f'[TempRoom] Created {temp_channel.name} ({temp_channel.id}) for {member.display_name}')
    return True

async def _handle_focus_leave(member: discord.Member, channel, reason: str = 'rời phòng học') -> int:
    _, checkpoint_result = await _do_checkpoint(member)
    media_active_members.discard(member.id)

    if checkpoint_result.get('level_up'):
        await _ensure_role_synced(member, checkpoint_result['new_level'])
    duration = await record_leave_and_notify(member)
    cancel_task(member.id)
    log.info(f'{member.display_name} rời phòng sau {format_time(duration)}')

    await _update_live_message_for_channel(channel)
    return duration

def _get_cached_member(member_id: int) -> discord.Member | None:
    for guild in bot.guilds:
        member = guild.get_member(member_id)
        if member:
            return member
    return None

async def _flush_active_sessions(reason: str = 'shutdown'):
    if not join_times:
        save_runtime_state()
        return

    log.info(f'[Runtime] Flushing {len(join_times)} active voice sessions before {reason}.')
    for mid in list(join_times.keys()):
        member = _get_cached_member(mid)
        if not member or not member.voice or not member.voice.channel:
            continue
        if not is_focus_channel(member.voice.channel.id):
            continue
        if mid not in media_active_members and not is_media_active(member.voice):
            continue
        try:
            _capture_guild_context(member.guild.id)
            _, result = await _do_checkpoint(member)
            if result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
        except Exception as e:
            log.error(f'[Runtime] Failed to flush {member.display_name} before {reason}: {e}', exc_info=True)
    save_runtime_state()

# ─── MEDIA CHECK ─────────────────────────────────────────────────────────────

async def check_media(member: discord.Member):
    try:
        _capture_guild_context(member.guild.id)
        await asyncio.sleep(WAIT_SECONDS - WARN_BEFORE_KICK)
        if not (member.voice and member.voice.channel and
                is_focus_channel(member.voice.channel.id)): return
        if is_media_active(member.voice): return
        notice_channel = member.voice.channel
        await send_voice_notice(
            channel=notice_channel,
            member=member,
            title='Cảnh báo',
            description=(
                'Bạn chưa bật Cam hoặc Stream.\n'
                f'Bạn sẽ bị kick sau {WARN_BEFORE_KICK} giây nếu không bật.'
            ),
            color=NOTIFY_RED,
        )
        await asyncio.sleep(WARN_BEFORE_KICK)
        if not (member.voice and member.voice.channel and
                is_focus_channel(member.voice.channel.id)): return
        if not is_media_active(member.voice):
            if not bot_can_move(member): return
            _, checkpoint_result = await _do_checkpoint(member)
            media_active_members.discard(member.id)
            if checkpoint_result.get('level_up'):
                await _ensure_role_synced(member, checkpoint_result['new_level'])
            notice_channel = member.voice.channel
            await send_voice_notice(
                channel=notice_channel,
                member=member,
                title='Đã rời phòng',
                description=(
                    'Bạn đã bị kick vì chưa bật Cam hoặc Stream.\n'
                    'Hãy bật Cam hoặc Stream khi vào lại.'
                ),
                color=NOTIFY_RED,
            )
            await record_leave_and_notify(member)
            await member.move_to(None)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f'check_media error: {e}')
    finally:
        pending_checks.pop(member.id, None)

# ─── SCHEDULED TASKS ─────────────────────────────────────────────────────────

@tasks.loop(minutes=1)
async def scheduled_tasks():
    now = datetime.now()

    if now.hour == REPORT_HOUR and now.minute == REPORT_MINUTE:
        report_key = now.strftime('%Y-%m-%d')
        if report_key not in report_sent_today:
            report_sent_today.add(report_key)
            await _send_report()

    if now.hour == DAILY_BOARD_HOUR and now.minute == DAILY_BOARD_MINUTE:
        today_key = now.strftime('%Y-%m-%d')
        if today_key not in daily_board_sent:
            daily_board_sent.add(today_key)
            await _send_daily_board()

    if now.hour == 9 and now.minute == 0:
        await _check_overdue_loan_notifications()

    if now.hour == 4 and now.minute == 0:
        backup_data()

    if now.hour == 0 and now.minute == 0:
        session_counts.clear()
        daily_first_join.clear()
        _rebuild_daily_session_state(now)
        today_key     = now.strftime('%Y-%m-%d')
        yesterday_key = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        report_sent_today.intersection_update({today_key, yesterday_key})
        daily_board_sent.intersection_update({today_key, yesterday_key})
        save_runtime_state()
        log.info('Reset session counts & daily_first_join.')

@tasks.loop(minutes=CHECKPOINT_MINUTES)
async def checkpoint_task():
    now = datetime.now()
    log.info(f'[{now.strftime("%H:%M")}] Checkpoint...')

    # Prune stale role sync locks for members no longer in voice.
    active_ids = set(join_times.keys())
    stale_lock_ids = [mid for mid in _role_sync_locks if mid not in active_ids]
    for mid in stale_lock_ids:
        lock = _role_sync_locks[mid]
        if not lock.locked():
            del _role_sync_locks[mid]
    if stale_lock_ids:
        log.info(f'[Cleanup] Pruned {len(stale_lock_ids)} stale role sync locks.')

    for mid in list(join_times.keys()):
        member = None
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m and m.voice and m.voice.channel and is_focus_channel(m.voice.channel.id):
                member = m; break
        if not member:
            join_times.pop(mid, None)
            last_checkpoint.pop(mid, None)
            milestone_sent.pop(mid, None)
            runtime_member_guild_ids.pop(mid, None)
            media_active_members.discard(mid)
            reset_cam_notification(mid)
            save_runtime_state()
            continue

        was_active = mid in media_active_members
        now_active = bool(member.voice and is_media_active(member.voice))
        _capture_guild_context(member.guild.id)
        runtime_member_guild_ids[mid] = member.guild.id
        
        if was_active:
            elapsed, result = await _do_checkpoint(member)
            if result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
            await _handle_progress_notifications(member, result, member.voice.channel)
            await _check_quests_and_badges(member, member.voice.channel)
        
        if now_active and not was_active:
            media_active_members.add(mid)
            last_checkpoint[mid] = datetime.now()
            save_runtime_state()
            await notify_cam_started(member, member.voice.channel)
        elif not now_active and was_active:
            media_active_members.discard(mid)
            save_runtime_state()
            
    await update_all_live_messages()
    _update_live_cache()
    save_runtime_state()

@scheduled_tasks.before_loop
async def before_scheduled_tasks():
    await bot.wait_until_ready()

@checkpoint_task.before_loop
async def before_checkpoint_task():
    await bot.wait_until_ready()

# ─── REPORTS ─────────────────────────────────────────────────────────────────

async def _send_report(guild: discord.Guild | None = None):
    today = datetime.now().strftime('%Y-%m-%d')
    target_guilds = [guild] if guild else list(bot.guilds)
    for target_guild in target_guilds:
        config = get_guild_config(target_guild.id)
        report_channel_id = config.get('report_channel_id')
        ch = bot.get_channel(report_channel_id) if report_channel_id else None
        if not ch:
            continue
        with guild_data_context(target_guild.id):
            data = load_data()
        sorted_data = sorted(data.items(), key=lambda x: x[1].get('daily', {}).get(today, 0), reverse=True)
        lines       = [f'📊 **Báo cáo ngày {today}**\n']
        has_data    = False
        rank        = 0
        for uid, info in sorted_data:
            t = info.get('daily', {}).get(today, 0)
            if t > 0:
                has_data  = True
                rank     += 1
                medal     = ['🥇', '🥈', '🥉'][rank-1] if rank <= 3 else f'`{rank}.`'
                class_idx = info.get('class', info.get('level', 0))
                lines.append(
                    f'{medal} **{info["name"]}** `{class_label(class_idx)}` '
                    f'🔥{info.get("streak",0)} — `{format_time(t)}`'
                )
        if not has_data: lines.append('😴 Hôm nay chưa có ai học!')
        chunks      = []
        current     = []
        current_len = 0
        for line in lines:
            line_len = len(line) + 1
            if current_len + line_len > 1900 and current:
                chunks.append('\n'.join(current))
                current     = [line]
                current_len = line_len
            else:
                current.append(line)
                current_len += line_len
        if current:
            chunks.append('\n'.join(current))
        for chunk in chunks:
            await ch.send(chunk)

async def _check_absences():
    log.info('[Remind] Absence study reminders are disabled; skipping check.')
    return

async def _check_overdue_loan_notifications():
    for guild in bot.guilds:
        with guild_data_context(guild.id):
            data = load_data()
            for uid, info in data.items():
                if not isinstance(info, dict):
                    continue
                if not any(_is_overdue(loan) for loan in _active_loans(info)):
                    continue
                try:
                    member_id = int(uid)
                except (TypeError, ValueError):
                    continue
                member = guild.get_member(member_id)
                if member:
                    await notify_overdue_loans(member)

async def _sync_member_progress(member: discord.Member, previous_level: int | None = None):
    _capture_guild_context(member.guild.id)
    notice_channel = _current_voice_notice_channel(member)
    if (
        member.id in join_times
        and member.id in media_active_members
        and _last_data_save_success
    ):
        last_checkpoint[member.id] = datetime.now()
        save_runtime_state()
    await _check_quests_and_badges(member, notice_channel)
    if previous_level is None:
        return
    info = load_data().get(str(member.id), {})
    current_level = info.get('class', info.get('level', 0))
    await _handle_progress_notifications(
        member,
        {'level_up': current_level > previous_level, 'new_level': current_level},
        notice_channel,
    )

# ─── AI ──────────────────────────────────────────────────────────────────────

DEFAULT_AI_SYSTEM_PROMPT = (
    'You are a smart, direct general-purpose AI assistant inside a Discord server. '
    'Reply in the same language as the user. '
    'Answer any normal topic honestly and helpfully. '
    'For simple questions, answer in 1-3 sentences. '
    'For moderate questions, answer with enough detail using short paragraphs or bullets. '
    'For complex questions, explain clearly with structure, examples, formulas, or code blocks when useful. '
    'Never pad answers. Never be shallow. '
    'Prefer complete answers over over-compressed summaries; the bot will fit long answers into one Discord message after generation. '
    'Use Discord markdown when helpful. '
    'For Vietnamese conversations, adapt address pronouns to the user vibe: mình/bạn, tôi/bạn, tôi/ông, t/m, or tao/mày. '
    'Mirror casual intimate pronouns only when the user has clearly used them first and the exchange is friendly or joking. '
    'Never start using mày/tao on your own. '
    'If the conversation is tense, sensitive, argumentative, or contains personal attacks, switch to neutral polite pronouns such as mình/bạn or tôi/bạn. '
    'Some rude words may be playful among friends, so read context, but do not escalate hostility. '
    'If unsure, say so briefly.'
)
AI_SYSTEM_PROMPT = os.getenv('AI_SYSTEM_PROMPT') or DEFAULT_AI_SYSTEM_PROMPT
AI_RETRYABLE_STATUS_CODES = {404, 429, 500, 502, 503, 504}
AI_RETRYABLE_ERROR_PATTERNS = (
    'model not found',
    'not_found',
    'does not exist',
    'invalid model',
    'invalid_model',
    'quota',
    'rate limit',
    'rate_limit',
    'resource_exhausted',
    'too many requests',
    'temporarily unavailable',
    'overloaded',
)
AI_AUTH_STATUS_CODES = {401, 403}
AI_AUTH_ERROR_PATTERNS = (
    'api key not valid',
    'invalid api key',
    'invalid_api_key',
    'api_key_invalid',
    'incorrect api key',
    'invalid authentication',
    'unauthenticated',
)
DEFAULT_AI_PROVIDER_ORDER = ('groq', 'gemini', 'openrouter', 'huggingface')
AI_PROVIDER_ALIASES = {
    'groq': 'groq',
    'gemini': 'gemini',
    'google': 'gemini',
    'openrouter': 'openrouter',
    'open_router': 'openrouter',
    'huggingface': 'huggingface',
    'hugging_face': 'huggingface',
    'hf': 'huggingface',
}


class AIProviderError(Exception):
    def __init__(
        self,
        provider: str,
        message: str,
        retryable: bool = False,
        auth_failed: bool = False,
    ):
        super().__init__(message)
        self.provider = provider
        self.retryable = retryable
        self.auth_failed = auth_failed


def _split_env_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(',') if item.strip()]


def _configured_ai_provider_order() -> list[str]:
    order: list[str] = []
    for item in _split_env_list(AI_PROVIDER_ORDER):
        normalized = item.strip().lower().replace('-', '_').replace(' ', '_')
        provider = AI_PROVIDER_ALIASES.get(normalized)
        if not provider:
            log.warning(f'Ignoring unknown AI provider in AI_PROVIDER_ORDER: {item}')
            continue
        if provider not in order:
            order.append(provider)

    for provider in DEFAULT_AI_PROVIDER_ORDER:
        if provider not in order:
            order.append(provider)
    return order


AI_EMPTY_RESPONSE = '❌ AI không trả về nội dung.'
AI_SAFE_TRIM_FALLBACK = 'Câu trả lời quá dài nên bot chưa thể thu gọn an toàn trong một tin nhắn Discord.'
AI_TRUNCATION_WORDS = r'rút\s*gọn|rut\s*gon|shortened|truncated|trimmed|condensed|summari[sz]ed'
AI_STANDALONE_TRUNCATION_MARKER_RE = re.compile(
    rf'^[\s_*`~>-]*(?:\.{{3}}|…)?\s*(?:[\(\[]\s*)?(?:{AI_TRUNCATION_WORDS})(?:\s*[\)\]])?[\s_*`~.>-]*$',
    re.IGNORECASE,
)
AI_TRAILING_TRUNCATION_MARKER_RE = re.compile(
    rf'(?:\s*[_*`~>-]*(?:\.{{3}}|…)?\s*[\(\[]\s*(?:{AI_TRUNCATION_WORDS})\s*[\)\]][\s_*`~.>-]*)+$',
    re.IGNORECASE,
)
AI_SENTENCE_BOUNDARY_RE = re.compile(r'[.!?。！？…]+(?:["\'”’)\]]+)?(?=\s|$)')
AI_PARAGRAPH_BOUNDARY_RE = re.compile(r'\n\s*\n+')
AI_LIST_LINE_BOUNDARY_RE = re.compile(r'\n(?=\s*(?:[-*+•]|\d+[.)])\s+)')


def _strip_ai_truncation_markers(text: str) -> str:
    cleaned = (text or '').strip()
    if not cleaned:
        return ''

    cleaned = AI_TRAILING_TRUNCATION_MARKER_RE.sub('', cleaned).strip()
    lines = [
        line.rstrip()
        for line in cleaned.splitlines()
        if not AI_STANDALONE_TRUNCATION_MARKER_RE.match(line.strip())
    ]
    cleaned = '\n'.join(lines).strip()
    return AI_TRAILING_TRUNCATION_MARKER_RE.sub('', cleaned).strip()


def _fallback_ai_trim_message(limit: int) -> str:
    if len(AI_SAFE_TRIM_FALLBACK) <= limit:
        return AI_SAFE_TRIM_FALLBACK
    short_fallback = 'Câu trả lời quá dài.'
    if len(short_fallback) <= limit:
        return short_fallback
    tiny_fallback = '❌'
    if len(tiny_fallback) <= limit:
        return tiny_fallback
    return ''


def smart_cut_at_sentence(text: str, limit: int = AI_ONE_MESSAGE_LIMIT) -> str:
    if limit <= 0:
        return ''

    text = _strip_ai_truncation_markers(text)
    if len(text) <= limit:
        return text or AI_EMPTY_RESPONSE

    cut = text[:limit]
    boundary_positions: list[int] = []
    boundary_positions.extend(match.start() for match in AI_PARAGRAPH_BOUNDARY_RE.finditer(cut))
    boundary_positions.extend(match.end() for match in AI_SENTENCE_BOUNDARY_RE.finditer(cut))
    boundary_positions.extend(match.start() for match in AI_LIST_LINE_BOUNDARY_RE.finditer(cut))

    for boundary in sorted(set(boundary_positions), reverse=True):
        candidate = _strip_ai_truncation_markers(cut[:boundary])
        if candidate and len(candidate) <= limit:
            return candidate

    return _fallback_ai_trim_message(limit)


def _openai_compatible_token_limit(provider: dict) -> dict:
    token_param = provider.get('max_token_param', 'max_tokens')
    return {token_param: AI_MAX_OUTPUT_TOKENS}


def _is_ai_auth_error(status_code: int, body: str) -> bool:
    normalized = (body or '').lower()
    if status_code == 401:
        return True
    if status_code == 403:
        return not any(pattern in normalized for pattern in AI_RETRYABLE_ERROR_PATTERNS)
    return status_code == 400 and any(pattern in normalized for pattern in AI_AUTH_ERROR_PATTERNS)


def _is_ai_retryable_error(status_code: int, body: str) -> bool:
    if status_code in AI_RETRYABLE_STATUS_CODES:
        return True
    normalized = (body or '').lower()
    return status_code in {400, 403} and any(
        pattern in normalized for pattern in AI_RETRYABLE_ERROR_PATTERNS
    )


def _configured_ai_providers() -> list[dict]:
    provider_groups: dict[str, list[dict]] = {
        'groq': [],
        'gemini': [],
        'openrouter': [],
        'huggingface': [],
    }
    if GROQ_API_KEY:
        for model in _split_env_list(GROQ_MODELS):
            provider_groups['groq'].append({
                'kind': 'openai_compatible',
                'name': f'Groq {model}',
                'model': model,
                'api_key': GROQ_API_KEY,
                'auth_group': 'groq',
                'base_url': 'https://api.groq.com/openai/v1',
                'max_token_param': 'max_completion_tokens',
            })
    if GEMINI_API_KEY:
        provider_groups['gemini'].extend([
            {'kind': 'gemini', 'name': 'Gemini 3.5 Flash', 'model': GEMINI_FLASH_MODEL, 'api_key': GEMINI_API_KEY, 'auth_group': 'gemini'},
            {'kind': 'gemini', 'name': 'Gemini 3.1 Flash-Lite', 'model': GEMINI_FLASH_LITE_MODEL, 'api_key': GEMINI_API_KEY, 'auth_group': 'gemini'},
        ])
    if OPENROUTER_API_KEY:
        provider_groups['openrouter'].append({
            'kind': 'openai_compatible',
            'name': f'OpenRouter {OPENROUTER_MODEL}',
            'model': OPENROUTER_MODEL,
            'api_key': OPENROUTER_API_KEY,
            'auth_group': 'openrouter',
            'base_url': 'https://openrouter.ai/api/v1',
            'max_token_param': 'max_tokens',
            'headers': {
                'HTTP-Referer': OPENROUTER_REFERER,
                'X-Title': OPENROUTER_TITLE,
            },
        })
    if HUGGINGFACE_API_KEY:
        provider_groups['huggingface'].append({
            'kind': 'openai_compatible',
            'name': f'Hugging Face {HUGGINGFACE_MODEL}',
            'model': HUGGINGFACE_MODEL,
            'api_key': HUGGINGFACE_API_KEY,
            'auth_group': 'huggingface',
            'base_url': 'https://router.huggingface.co/v1',
            'max_token_param': 'max_tokens',
        })

    providers: list[dict] = []
    for provider_name in _configured_ai_provider_order():
        providers.extend(provider_groups.get(provider_name, []))
    return providers


async def _post_ai_json(client: httpx.AsyncClient, provider: str, url: str, headers: dict, payload: dict):
    try:
        response = await client.post(url, headers=headers, json=payload)
    except (httpx.TimeoutException, httpx.RequestError) as e:
        raise AIProviderError(provider, f'{type(e).__name__}: {e}', retryable=True) from e

    response_text = response.text[:300]
    if _is_ai_retryable_error(response.status_code, response.text):
        raise AIProviderError(provider, f'HTTP {response.status_code}: {response_text}', retryable=True)
    if response.status_code >= 400:
        raise AIProviderError(
            provider,
            f'HTTP {response.status_code}: {response_text}',
            auth_failed=_is_ai_auth_error(response.status_code, response.text),
        )

    try:
        return response.json()
    except ValueError as e:
        raise AIProviderError(provider, 'Phản hồi AI không phải JSON hợp lệ.', retryable=True) from e


def _extract_gemini_text(provider: str, payload: dict) -> str:
    parts = (
        payload.get('candidates', [{}])[0]
        .get('content', {})
        .get('parts', [])
    )
    text = '\n'.join(part.get('text', '') for part in parts if isinstance(part, dict)).strip()
    if text:
        return text
    block_reason = payload.get('promptFeedback', {}).get('blockReason')
    if block_reason:
        raise AIProviderError(provider, f'Gemini chặn phản hồi: {block_reason}')
    raise AIProviderError(provider, 'Gemini không trả về nội dung văn bản.', retryable=True)


def _extract_openai_compatible_text(provider: str, payload: dict) -> str:
    choices = payload.get('choices') or []
    if not choices:
        raise AIProviderError(provider, 'Provider không trả về lựa chọn phản hồi.', retryable=True)

    message = choices[0].get('message') or {}
    content = message.get('content', choices[0].get('text', ''))
    if isinstance(content, str):
        text = content.strip()
    elif isinstance(content, list):
        text = '\n'.join(
            part.get('text', '') if isinstance(part, dict) else str(part)
            for part in content
        ).strip()
    else:
        text = str(content).strip() if content else ''

    if not text:
        raise AIProviderError(provider, 'Provider trả về phản hồi rỗng.', retryable=True)
    return text


def _extract_huggingface_text_generation_text(provider: str, payload) -> str:
    if isinstance(payload, list) and payload:
        payload = payload[0]
    if isinstance(payload, dict):
        text = (
            payload.get('generated_text')
            or payload.get('summary_text')
            or payload.get('text')
            or ''
        )
    else:
        text = str(payload or '')

    text = text.strip()
    if not text:
        raise AIProviderError(provider, 'Hugging Face trả về phản hồi rỗng.', retryable=True)
    return text


async def _call_gemini_provider(client: httpx.AsyncClient, provider: dict, question: str) -> str:
    model = provider['model']
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent'
    payload = {
        'system_instruction': {'parts': [{'text': AI_SYSTEM_PROMPT}]},
        'contents': [
            {
                'role': 'user',
                'parts': [{'text': question}],
            },
        ],
        'generationConfig': {
            'maxOutputTokens': AI_MAX_OUTPUT_TOKENS,
            'temperature': AI_TEMPERATURE,
        },
    }
    headers = {
        'x-goog-api-key': provider['api_key'],
        'Content-Type': 'application/json',
    }
    response = await _post_ai_json(client, provider['name'], url, headers, payload)
    return _extract_gemini_text(provider['name'], response)


async def _call_openai_compatible_provider(client: httpx.AsyncClient, provider: dict, question: str) -> str:
    headers = {
        'Authorization': f"Bearer {provider['api_key']}",
        'Content-Type': 'application/json',
    }
    headers.update(provider.get('headers', {}))
    payload = {
        'model': provider['model'],
        'messages': [
            {'role': 'system', 'content': AI_SYSTEM_PROMPT},
            {'role': 'user', 'content': question},
        ],
        'temperature': AI_TEMPERATURE,
    }
    payload.update(_openai_compatible_token_limit(provider))
    url = f"{provider['base_url'].rstrip('/')}/chat/completions"
    response = await _post_ai_json(client, provider['name'], url, headers, payload)
    return _extract_openai_compatible_text(provider['name'], response)


async def _call_huggingface_text_generation_provider(client: httpx.AsyncClient, provider: dict, question: str) -> str:
    headers = {
        'Authorization': f"Bearer {provider['api_key']}",
        'Content-Type': 'application/json',
    }
    prompt = f'{AI_SYSTEM_PROMPT}\n\nCâu hỏi: {question}\n\nTrả lời:'
    payload = {
        'inputs': prompt,
        'parameters': {
            'max_new_tokens': AI_MAX_OUTPUT_TOKENS,
            'temperature': AI_TEMPERATURE,
            'return_full_text': False,
        },
    }
    url = provider.get('url') or f"https://api-inference.huggingface.co/models/{provider['model']}"
    response = await _post_ai_json(client, provider['name'], url, headers, payload)
    return _extract_huggingface_text_generation_text(provider['name'], response)


async def _call_ai_provider(client: httpx.AsyncClient, provider: dict, question: str) -> str:
    if provider['kind'] == 'gemini':
        return await _call_gemini_provider(client, provider, question)
    if provider['kind'] == 'huggingface_text_generation':
        return await _call_huggingface_text_generation_provider(client, provider, question)
    return await _call_openai_compatible_provider(client, provider, question)


async def _ask_ai_raw(question: str) -> str:
    providers = _configured_ai_providers()
    if not providers:
        return '❌ Thiếu API key AI trong .env. Cần GEMINI_API_KEY, GROQ_API_KEY, OPENROUTER_API_KEY hoặc HF_TOKEN.'

    errors: list[AIProviderError] = []
    failed_auth_groups: set[str] = set()
    async with httpx.AsyncClient(timeout=AI_HTTP_TIMEOUT) as client:
        for provider in providers:
            auth_group = provider.get('auth_group', provider['name'])
            if auth_group in failed_auth_groups:
                log.warning(f'Skipping AI provider {provider["name"]}; auth already failed for {auth_group}.')
                continue
            try:
                answer = await _call_ai_provider(client, provider, question)
                return answer.strip() or AI_EMPTY_RESPONSE
            except AIProviderError as e:
                errors.append(e)
                if e.auth_failed:
                    failed_auth_groups.add(auth_group)
                level = logging.WARNING if e.retryable else logging.ERROR
                log.log(level, f'AI provider failed ({e.provider}, retryable={e.retryable}, auth_failed={e.auth_failed}): {e}')
            except Exception as e:
                wrapped = AIProviderError(provider['name'], str(e))
                errors.append(wrapped)
                log.exception(f'Unexpected AI error from {provider["name"]}')

    retryable_count = sum(1 for e in errors if e.retryable)
    log.error(f'All AI providers failed ({retryable_count}/{len(errors)} retryable failures).')
    return '❌ Lỗi AI. Tất cả provider hiện chưa phản hồi được, thử lại sau nhé!'


async def _compact_ai_answer(question: str, answer: str, limit: int = AI_ONE_MESSAGE_LIMIT) -> str:
    compact_prompt = (
        f'Viết lại câu trả lời sau để vừa tối đa {limit} ký tự trong một tin nhắn Discord. '
        'Giữ câu trả lời hoàn chỉnh: kết luận, điều kiện, các bước chính, ví dụ hoặc cảnh báo quan trọng nếu có. '
        'Chỉ cô đọng phần diễn đạt dư thừa; không biến câu trả lời phức tạp thành bản tóm tắt quá ngắn. '
        'Nếu phải lược bớt, ưu tiên bỏ ví dụ phụ, câu chuyển ý và chi tiết lặp. '
        'Không thêm mở bài. Không nói rằng câu trả lời đã được rút gọn, shortened, truncated hoặc trimmed. '
        'Không lặp lại câu hỏi.\n\n'
        f'Câu hỏi: {question}\n\n'
        f'Câu trả lời cần viết lại:\n{answer}'
    )
    compacted = await _ask_ai_raw(compact_prompt)
    return _strip_ai_truncation_markers(compacted)


async def _ask_ai(question: str) -> str:
    answer = _strip_ai_truncation_markers(await _ask_ai_raw(question))
    if len(answer) <= AI_ONE_MESSAGE_LIMIT:
        return answer or AI_EMPTY_RESPONSE

    compacted = await _compact_ai_answer(question, answer, AI_ONE_MESSAGE_LIMIT)
    if not compacted or compacted.startswith('❌'):
        return smart_cut_at_sentence(answer, AI_ONE_MESSAGE_LIMIT)
    if len(compacted) <= AI_ONE_MESSAGE_LIMIT:
        return compacted
    return smart_cut_at_sentence(compacted, AI_ONE_MESSAGE_LIMIT)

# ─── SLASH COMMANDS ──────────────────────────────────────────────────────────

def _build_rank_message(target: discord.Member, data: dict) -> str:
    uid = str(target.id)
    if uid not in data:
        return f'❌ **{target.display_name}** chưa có dữ liệu!'
    info    = data[uid]
    balance = info.get('balance', 0)
    total_earned = info.get('total_earned', 0)
    debt = _active_debt(info)
    net_worth = balance - debt
    class_idx = min(get_money_class(total_earned), len(CLASS_NAMES) - 1)
    streak  = info.get('streak', 0)
    longest = info.get('longest_streak', 0)
    total   = info.get('total', 0)
    today   = datetime.now().strftime('%Y-%m-%d')
    saved   = info.get('daily', {}).get(today, 0)
    saved += _get_unsaved_study_seconds(target.id)
    earned_today = info.get('daily_earnings', {}).get(today, 0)
    if class_idx >= len(CLASS_THRESHOLDS) - 1:
        coins_needed = 0; pct = 100; bar_f = 20
    else:
        coin_start = CLASS_THRESHOLDS[class_idx]
        coin_end = CLASS_THRESHOLDS[class_idx + 1]
        coin_cur = max(0, total_earned - coin_start)
        coins_needed = max(0, coin_end - total_earned)
        span = max(1, coin_end - coin_start)
        pct = int((coin_cur / span) * 100)
        bar_f = int((coin_cur / span) * 20)
    coin_bar = '█' * bar_f + '░' * (20 - bar_f)
    class_role = _find_class_role_by_level(target.guild, class_idx) if target.guild else None
    role_name = class_role.name if class_role else None
    role_str  = f'🏷️ Role: **{role_name}**\n' if role_name else ''
    recent    = sorted(info.get('daily', {}).items(), reverse=True)[:5]
    recent_str = ' · '.join([f'`{d[5:]}`{format_time(s)}' for d, s in recent])
    badges    = info.get('badges', [])
    badge_str = format_badges(badges[:6]) if badges else '_Chưa có_'
    return (
        f'╔══════════════════════════════╗\n'
        f'   🎓 **{target.display_name}**\n'
        f'╚══════════════════════════════╝\n'
        f'🏛️ **{class_label(class_idx)}**\n{role_str}'
        f'──────────────────────────────\n'
        f'💰 Total earned: `{format_coins(total_earned)}` | `{coin_bar}` **{pct}%**\n'
        f'_{f"còn **{format_coins(coins_needed)}** để lên {class_label(class_idx+1)}" if coins_needed > 0 else "✨ Max class!"}_\n'
        f'──────────────────────────────\n'
        f'💵 Balance: `{format_coins(balance)}` | Debt: `{format_coins(debt)}` | Net worth: `{format_coins(net_worth)}`\n'
        f'🪙 Earned hôm nay: `{format_coins(earned_today)}`\n'
        f'🔥 Streak: `{streak} ngày` _(kỷ lục: {longest})_\n'
        f'🕐 Hôm nay: `{format_time(saved)}`\n'
        f'📚 Tổng: `{format_time(total)}`\n'
        f'🏅 Huy hiệu: {badge_str}\n'
        f'──────────────────────────────\n'
        f'📅 Gần nhất: {recent_str}'
    )

# ── /rank ──────────────────────────────────────────────────────────────────

@bot.tree.command(name='rank', description='Xem ví, class và thống kê của bạn')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_rank(interaction: discord.Interaction, member: discord.Member = None):
    is_self = member is None
    await interaction.response.defer(ephemeral=is_self)
    target = member or interaction.user
    await interaction.followup.send(
        _build_rank_message(target, _get_live_enriched_data(interaction.guild)),
        ephemeral=is_self
    )


@bot.tree.command(name='profile', description='Xem profile học tập của bạn')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_profile(interaction: discord.Interaction, member: discord.Member = None):
    is_self = member is None
    await interaction.response.defer(ephemeral=is_self)
    target = member or interaction.user
    await interaction.followup.send(
        _build_rank_message(target, _get_live_enriched_data(interaction.guild)),
        ephemeral=is_self,
    )

# ── /quest ─────────────────────────────────────────────────────────────────

@bot.tree.command(name='quest', description='Xem nhiệm vụ hàng ngày')
async def slash_quest(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    uid   = str(interaction.user.id)
    today = datetime.now().strftime('%Y-%m-%d')

    raw = load_data()
    if uid not in raw:
        def ensure_user(data: dict):
            data.setdefault(uid, _default_user(interaction.user.display_name))

        update_data(ensure_user)

    data = _get_live_enriched_data(interaction.guild)

    mid         = interaction.user.id
    saved_secs  = data[uid]['daily'].get(today, 0)
    real_today_secs = saved_secs + _get_unsaved_study_seconds(mid)

    generate_daily_quests(uid, today)
    update_quest_progress(uid, today, override_today_secs=real_today_secs)

    data   = load_data()
    quests = data[uid].get('daily_quests', {}).get(today, [])

    lines      = [f'📋 **Nhiệm vụ hôm nay** _{today}_\n']
    total_done = 0
    for q in quests:
        info = get_quest_info(q['id'])
        if not info: continue
        pct    = min(100, int(q['progress'] / max(1, info['target']) * 100))
        bar    = '█' * (pct // 10) + '░' * (10 - pct // 10)
        status = '✅' if q.get('done') else '🔲'
        reward = info.get('coins', info.get('xp', 0))
        coin_str = f'+{format_coins(reward)} ✓' if q.get('done') else format_coins(reward)
        lines.append(
            f'{status} {info["emoji"]} **{info["desc"]}**\n'
            f'   `{bar}` {q["progress"]}/{info["target"]}  _{coin_str}_'
        )
        if q.get('done'): total_done += 1
    total_done_ever = data[uid].get('quests_done_total', 0)
    lines.append(f'\n✨ Hôm nay: `{total_done}/{len(quests)}` · Tổng đã làm: `{total_done_ever}` quest')
    lines.append('_Quest tự reset lúc 0h mỗi đêm_')
    await interaction.followup.send('\n'.join(lines), ephemeral=True)

# ── /badges ────────────────────────────────────────────────────────────────

@bot.tree.command(name='badges', description='Xem tất cả huy hiệu')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_badges(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer(ephemeral=True)
    target = member or interaction.user
    uid    = str(target.id)
    if target.id == interaction.user.id:
        raw = load_data()
        if uid not in raw:
            def ensure_user(data: dict):
                data.setdefault(uid, _default_user(interaction.user.display_name))

            update_data(ensure_user)
    data   = _get_live_enriched_data(interaction.guild)
    if uid not in data:
        await interaction.followup.send(
            f'❌ **{target.display_name}** chưa có dữ liệu!', ephemeral=True
        ); return
    earned = set(data[uid].get('badges', []))
    lines  = [f'🏅 **Huy hiệu của {target.display_name}** ({len(earned)}/{len(BADGES)})\n']
    categories = {
        '🔥 Streak':      ['streak_3', 'streak_7', 'streak_14', 'streak_30', 'streak_100'],
        '📚 Thời gian':   ['total_1h', 'total_10h', 'total_50h', 'total_100h', 'total_500h'],
        '🏃 Kỷ lục ngày': ['marathon_4h', 'marathon_8h'],
        '🏛️ Class':       ['level_5', 'level_10'],
        '⏰ Đặc biệt':    ['early_bird', 'night_owl'],
        '📋 Quest':       ['quest_10', 'quest_50'],
        '💰 Coins':       ['xp_1000', 'xp_10000'],
    }
    for cat_name, badge_ids in categories.items():
        cat_parts = []
        for bid in badge_ids:
            bdef = BADGES.get(bid, {})
            icon = '✅' if bid in earned else '🔒'
            cat_parts.append(f'{icon} {bdef.get("name", "?")}')
        lines.append(f'\n**{cat_name}**')
        lines.append('  '.join(cat_parts))
    await interaction.followup.send('\n'.join(lines), ephemeral=True)


@bot.tree.command(name='achievements', description='Xem achievements/huy hiệu học tập')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_achievements(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer(ephemeral=True)
    target = member or interaction.user
    uid = str(target.id)
    if target.id == interaction.user.id:
        raw = load_data()
        if uid not in raw:
            def ensure_user(data: dict):
                data.setdefault(uid, _default_user(interaction.user.display_name))

            update_data(ensure_user)
    data = _get_live_enriched_data(interaction.guild)
    if uid not in data:
        await interaction.followup.send(f'**{target.display_name}** chưa có achievements.', ephemeral=True)
        return
    earned = set(data[uid].get('badges', []))
    unlocked = [BADGES[key] for key in earned if key in BADGES]
    locked_count = max(0, len(BADGES) - len(earned))
    lines = [f'🏅 **Achievements của {target.display_name}** ({len(earned)}/{len(BADGES)})']
    if unlocked:
        for badge in unlocked[:20]:
            lines.append(f'✅ **{badge.get("name", "?")}** · {badge.get("desc", "")}')
    else:
        lines.append('📭 Chưa unlock achievement nào.')
    if locked_count:
        lines.append(f'\n🔒 Còn `{locked_count}` achievement chưa unlock.')
    await interaction.followup.send('\n'.join(lines)[:1900], ephemeral=True)

# ── /card ──────────────────────────────────────────────────────────────────

@bot.tree.command(name='card', description='Tạo ảnh profile card để chia sẻ')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_card(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    await interaction.response.defer(thinking=True)
    if not PIL_AVAILABLE:
        await interaction.followup.send(
            '❌ Tính năng `/card` cần **Pillow**: `pip install Pillow`', ephemeral=True
        ); return
    avatar_url = _member_avatar_url(target, 256)
    card_bytes = await asyncio.to_thread(
        generate_profile_card,
        target.id,
        interaction.guild_id,
        target.display_name,
        avatar_url,
    )
    if not card_bytes:
        await interaction.followup.send(
            f'❌ **{target.display_name}** chưa có dữ liệu học tập!', ephemeral=True
        ); return
    file = discord.File(io.BytesIO(card_bytes), filename=f'card_{target.display_name}.png')
    await interaction.followup.send(f'📸 **Profile card của {target.display_name}**', file=file)

# ── /stats ─────────────────────────────────────────────────────────────────

@bot.tree.command(name='stats', description='Xem thống kê học tập chi tiết')
@app_commands.describe(member='Thành viên (để trống = bản thân)', period='Khoảng thống kê')
@app_commands.choices(period=[
    app_commands.Choice(name='today', value='today'),
    app_commands.Choice(name='week', value='week'),
    app_commands.Choice(name='month', value='month'),
])
async def slash_stats(
    interaction: discord.Interaction,
    member: discord.Member = None,
    period: str = 'today',
):
    await interaction.response.defer(ephemeral=True)
    target = member or interaction.user
    uid    = str(target.id)
    if target.id == interaction.user.id:
        raw = load_data()
        if uid not in raw:
            def ensure_user(data: dict):
                data.setdefault(uid, _default_user(interaction.user.display_name))

            update_data(ensure_user)
    data   = _get_live_enriched_data(interaction.guild)
    if uid not in data:
        await interaction.followup.send(
            f'❌ **{target.display_name}** chưa có dữ liệu!', ephemeral=True
        ); return
    info        = data[uid]
    now_dt      = datetime.now()
    today       = now_dt.strftime('%Y-%m-%d')
    today_saved = info.get('daily', {}).get(today, 0)
    today_saved += _get_unsaved_study_seconds(target.id)
    balance    = info.get('balance', 0)
    total_earned = info.get('total_earned', 0)
    debt       = _active_debt(info)
    class_idx  = min(get_money_class(total_earned), len(CLASS_NAMES) - 1)
    today_earned = info.get('daily_earnings', {}).get(today, 0)
    streak     = info.get('streak', 0)
    _, coins_need = coins_to_next_class(total_earned)
    recent     = sorted(info.get('daily', {}).items(), reverse=True)[:7]
    recent_str = '\n'.join([f'  `{d}`: {format_time(s)}' for d, s in recent])
    badges     = info.get('badges', [])
    goal       = info.get('goal')
    goal_secs  = info.get('goal_seconds', 0)
    daily       = info.get('daily', {})
    earnings    = info.get('daily_earnings', {})
    if period == 'week':
        period_days = [(now_dt - timedelta(days=offset)).strftime('%Y-%m-%d') for offset in range(7)]
        period_label = '7 ngày'
        period_secs = sum(_as_int(daily.get(day, 0)) for day in period_days) + _get_unsaved_study_seconds(target.id)
        period_earned = sum(_as_int(earnings.get(day, 0)) for day in period_days)
    elif period == 'month':
        month_prefix = now_dt.strftime('%Y-%m-')
        period_label = 'tháng này'
        period_secs = sum(_as_int(value) for day, value in daily.items() if str(day).startswith(month_prefix))
        period_secs += _get_unsaved_study_seconds(target.id)
        period_earned = sum(_as_int(value) for day, value in earnings.items() if str(day).startswith(month_prefix))
    else:
        period = 'today'
        period_label = 'hôm nay'
        period_secs = today_saved
        period_earned = today_earned
    msg = (
        f'📊 **Thống kê của {target.display_name}**\n'
        f'🏛️ `{class_label(class_idx)}` | Total earned `{format_coins(total_earned)}` _(còn {format_coins(coins_need)})_\n'
        f'💵 Balance: `{format_coins(balance)}` | Debt: `{format_coins(debt)}` | Net worth: `{format_coins(balance - debt)}`\n'
        f'📌 Khoảng `{period_label}`: `{format_time(period_secs)}` · earned `{format_coins(period_earned)}`\n'
        f'🪙 Earned hôm nay: `{format_coins(today_earned)}`\n'
        f'🔥 Streak: `{streak} ngày` _(kỷ lục: {info.get("longest_streak",0)})_\n'
        f'🕐 Hôm nay: `{format_time(today_saved)}`\n'
        f'📚 Tổng: `{format_time(info.get("total",0))}`\n'
        f'🏅 Huy hiệu: `{len(badges)}/{len(BADGES)}`\n'
    )
    if goal and goal_secs > 0:
        pct = min(100, int((today_saved / goal_secs) * 100))
        msg += f'🎯 **{goal}**: `{pct}%`\n'
    msg += f'📅 7 ngày:\n{recent_str}\n'
    msg += f'\n_Dùng `/card` để tạo ảnh profile!_'
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name='streak', description='Xem streak học tập')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_streak(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    is_self = member is None
    await interaction.response.defer(ephemeral=is_self)
    data = _get_live_enriched_data(interaction.guild)
    info = data.get(str(target.id))
    if not info:
        await interaction.followup.send(f'**{target.display_name}** chưa có dữ liệu streak.', ephemeral=is_self)
        return
    await interaction.followup.send(
        (
            f'🔥 **Streak của {target.display_name}**\n'
            f'Hiện tại: `{_as_int(info.get("streak", 0))}` ngày\n'
            f'Kỷ lục: `{_as_int(info.get("longest_streak", 0))}` ngày\n'
            f'Lần học gần nhất: `{info.get("last_study_date") or "chưa có"}`'
        ),
        ephemeral=is_self,
    )

# ── /leaderboard ───────────────────────────────────────────────────────────

@bot.tree.command(name='leaderboard', description='Bảng xếp hạng học tập')
@app_commands.describe(metric='Loại leaderboard')
@app_commands.choices(metric=[
    app_commands.Choice(name='study_time', value='study_time'),
    app_commands.Choice(name='coins', value='coins'),
    app_commands.Choice(name='streak', value='streak'),
    app_commands.Choice(name='tasks', value='tasks'),
])
async def slash_leaderboard(interaction: discord.Interaction, metric: str = 'study_time'):
    await interaction.response.defer()
    if metric != 'study_time':
        data = _get_live_enriched_data(interaction.guild)
        lines = [f'🏆 **Leaderboard — {metric}**\n']
        if metric == 'coins':
            rows = sorted(data.values(), key=lambda info: _as_int(info.get('balance', 0)), reverse=True)
            rows = [row for row in rows if _as_int(row.get('balance', 0)) > 0][:10]
            for index, info in enumerate(rows, 1):
                lines.append(f'`{index}.` **{info.get("name", "Unknown")}** · balance `{format_coins(info.get("balance", 0))}`')
        elif metric == 'streak':
            rows = sorted(data.values(), key=lambda info: _as_int(info.get('streak', 0)), reverse=True)
            rows = [row for row in rows if _as_int(row.get('streak', 0)) > 0][:10]
            for index, info in enumerate(rows, 1):
                lines.append(f'`{index}.` **{info.get("name", "Unknown")}** · `{_as_int(info.get("streak", 0))}` ngày')
        elif metric == 'tasks':
            rows = repository.completed_task_leaderboard(interaction.guild_id, limit=10) if interaction.guild_id else []
            for index, row in enumerate(rows, 1):
                lines.append(f'`{index}.` **{row.get("display_name", "Unknown")}** · `{_as_int(row.get("completed_tasks", 0))}` tasks')
        if len(lines) == 1:
            lines.append('📭 Chưa có dữ liệu.')
        await interaction.followup.send('\n'.join(lines))
        return
    if not PIL_AVAILABLE:
        data = _get_live_enriched_data(interaction.guild)
        top10 = sorted(data.values(), key=lambda info: _as_int(info.get('total', 0)), reverse=True)[:10]
        lines = ['🏆 **Leaderboard — study_time**\n']
        lines.extend(
            f'`{index}.` **{info.get("name", "Unknown")}** · `{format_time(info.get("total", 0))}`'
            for index, info in enumerate(top10, 1)
            if _as_int(info.get('total', 0)) > 0
        )
        if len(lines) == 1:
            lines.append('📭 Chưa có dữ liệu.')
        await interaction.followup.send('\n'.join(lines))
        return
    entries, today = _build_study_leaderboard_entries(interaction.guild)
    view = StudyLeaderboardView(interaction.user.id, entries, today)
    await interaction.followup.send(file=view._image_file(), view=view)


@bot.tree.command(name='study_chart', description='Biểu đồ học tháng này so với tháng trước')
@app_commands.describe(member='Thành viên (để trống = cả server)')
async def slash_study_chart(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer()
    if not PIL_AVAILABLE:
        await interaction.followup.send(
            '❌ Tính năng biểu đồ ảnh cần **Pillow**: `pip install Pillow`',
            ephemeral=True,
        )
        return

    data = _get_live_enriched_data(interaction.guild)
    subject = interaction.guild.name if interaction.guild else 'Server'
    if member:
        uid = str(member.id)
        data = {uid: data.get(uid, _default_user(member.display_name))}
        subject = member.display_name

    now = datetime.now()
    prev_year, prev_month = _previous_month(now.year, now.month)
    current = _monthly_study_seconds(data, now.year, now.month, interaction.guild)
    previous = _monthly_study_seconds(data, prev_year, prev_month, interaction.guild)
    image = render_monthly_study_chart_image(current, previous, now, subject)
    file = discord.File(io.BytesIO(image), filename='monthly_study_chart.png')
    await interaction.followup.send(file=file)

# ── /top_alltime ───────────────────────────────────────────────────────────

@bot.tree.command(name='top_alltime', description='Bảng xếp hạng tổng thời gian học (all-time)')
async def slash_top_alltime(interaction: discord.Interaction):
    await interaction.response.defer()
    data  = load_data()
    top10 = sorted(
        [(u, i) for u, i in data.items() if i.get('total', 0) > 0],
        key=lambda x: x[1].get('total', 0), reverse=True
    )[:10]
    lines = ['🏆 **All-Time Top 10 — Tổng thời gian học**\n']
    if not top10:
        lines.append('📭 Chưa có dữ liệu.')
    else:
        for i, (uid, info) in enumerate(top10, 1):
            medal = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
            class_idx = info.get('class', info.get('level', 0))
            debt = _active_debt(info)
            lines.append(
                f'{medal} **{info["name"]}** `{class_label(class_idx)}` '
                f'🔥{info.get("streak",0)} · 📚 `{format_time(info.get("total",0))}`'
                f' · 💰 Earned `{format_coins(info.get("total_earned",0))}`'
                f' · Balance `{format_coins(info.get("balance",0))}` · Debt `{format_coins(debt)}`'
            )
    await interaction.followup.send('\n'.join(lines))

# ── Wallet & Economy ───────────────────────────────────────────────────────

def _wallet_message_for_member(target: discord.Member, data: dict) -> str:
    uid = str(target.id)
    if uid not in data:
        return f'❌ **{target.display_name}** chưa có ví.'
    info = data[uid]
    debt = _active_debt(info)
    class_idx = info.get('class', info.get('level', 0))
    return (
        f'💼 **Ví của {target.display_name}**\n'
        f'💵 Balance: `{format_coins(info.get("balance", 0))}`\n'
        f'💰 Total earned: `{format_coins(info.get("total_earned", 0))}`\n'
        f'🏛️ Class: `{class_label(class_idx)}`\n'
        f'💳 Debt: `{format_coins(debt)}` · Net worth: `{format_coins(info.get("balance", 0) - debt)}`\n'
        f'⭐ Credit score: `{_credit_score(info)}`'
    )


def _game_setup_channel_ids(guild_id: int | None) -> list[int]:
    if not guild_id:
        return []
    ids: list[int] = []
    for channel_id in _guild_game_channel_ids(int(guild_id)):
        if channel_id and channel_id not in ids:
            ids.append(channel_id)
    for channel_id in _guild_game_channel_map(int(guild_id)).keys():
        if channel_id and channel_id not in ids:
            ids.append(channel_id)
    return ids


def _game_setup_channel_error(guild: discord.Guild | None, guild_id: int | None) -> str:
    channel_ids = _game_setup_channel_ids(guild_id)
    if not channel_ids:
        return '❌ Admin chưa set kênh game. Dùng `/admin game_channels add <channel>` trước.'
    allowed_text = _format_config_channels(guild, channel_ids) if guild else ', '.join(f'`{cid}`' for cid in channel_ids)
    return f'❌ Lệnh tiền/game chỉ dùng được trong kênh đã set game.\nKênh game hiện tại: {allowed_text}'


async def _require_game_setup_channel_interaction(interaction: discord.Interaction) -> bool:
    if interaction.guild_id is None or interaction.channel_id is None:
        await interaction.response.send_message('❌ Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return False
    if _is_configured_game_channel(interaction.guild_id, interaction.channel_id):
        return True
    message = _game_setup_channel_error(interaction.guild, interaction.guild_id)
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)
    return False


async def _require_game_setup_channel_message(message: discord.Message) -> bool:
    if message.guild is None:
        await message.channel.send('❌ Lệnh này chỉ dùng được trong server.')
        return False
    if _is_configured_game_channel(message.guild.id, message.channel.id):
        return True
    await message.channel.send(_game_setup_channel_error(message.guild, message.guild.id))
    return False


async def _send_wallet_text_command(message: discord.Message, raw_target: str | None = None) -> None:
    if message.guild is None:
        await message.channel.send('❌ Lệnh này chỉ dùng được trong server.')
        return
    if not await _require_game_setup_channel_message(message):
        return
    _capture_guild_context(message.guild.id)
    target = message.author
    if message.mentions:
        target = message.mentions[0]
    elif raw_target:
        target = None
        raw_id = str(raw_target).strip().removeprefix('<@').removeprefix('!').removesuffix('>')
        if raw_id.isdigit():
            target = message.guild.get_member(int(raw_id))
            if target is None:
                with contextlib.suppress(discord.HTTPException):
                    target = await message.guild.fetch_member(int(raw_id))
        if target is None:
            await message.channel.send('❌ Không tìm thấy thành viên đó.')
            return

    uid = str(target.id)
    if target.id == message.author.id:
        def ensure_self(data: dict):
            _ensure_account(data, uid, target.display_name)

        _, data = update_data(ensure_self)
    else:
        data = load_data()
    await message.channel.send(_wallet_message_for_member(target, data))


@bot.tree.command(name='balance', description='Xem ví coins của bạn hoặc thành viên khác')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_balance(interaction: discord.Interaction, member: discord.Member = None):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    target = member or interaction.user
    is_self = target.id == interaction.user.id
    await interaction.response.defer(ephemeral=is_self)
    uid = str(target.id)

    if is_self:
        def ensure_self(data: dict):
            _ensure_account(data, uid, target.display_name)

        _, data = update_data(ensure_self)
    else:
        data = load_data()

    await interaction.followup.send(_wallet_message_for_member(target, data), ephemeral=is_self)


@bot.tree.command(name='pay', description='Chuyển coins ảo cho thành viên khác')
@app_commands.describe(member='Người nhận', amount='Số coins muốn chuyển')
async def slash_pay(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 1_000_000_000],
):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    if member.bot:
        await interaction.followup.send('❌ Không thể chuyển coins cho bot.', ephemeral=True)
        return
    if member.id == interaction.user.id:
        await interaction.followup.send('❌ Không thể tự chuyển cho chính mình.', ephemeral=True)
        return

    sender_uid = str(interaction.user.id)
    receiver_uid = str(member.id)

    def mutator(data: dict):
        sender = _ensure_account(data, sender_uid, interaction.user.display_name)
        receiver = _ensure_account(data, receiver_uid, member.display_name)
        if sender.get('balance', 0) < amount:
            return {'ok': False, 'error': f'Bạn chỉ có {format_coins(sender.get("balance", 0))}.'}

        sender['balance'] -= amount
        receiver['balance'] += amount
        _sync_money_class(sender)
        _sync_money_class(receiver)
        _append_transaction(sender, 'payment', -amount, f'Paid {member.display_name}', counterparty=receiver_uid)
        _append_transaction(receiver, 'payment', amount, f'Received from {interaction.user.display_name}', counterparty=sender_uid)
        return {
            'ok': True,
            'sender_balance': sender['balance'],
            'receiver_balance': receiver['balance'],
        }

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể chuyển coins.")}', ephemeral=True)
        return

    await interaction.followup.send(
        f'✅ Đã chuyển **{format_coins(amount)}** cho **{member.display_name}**.\n'
        f'💵 Balance của bạn: `{format_coins(result["sender_balance"])}`',
        ephemeral=True,
    )


@bot.tree.command(name='transactions', description='Xem lịch sử giao dịch gần đây')
@app_commands.describe(limit='Số dòng muốn xem (1-25)')
async def slash_transactions(
    interaction: discord.Interaction,
    limit: app_commands.Range[int, 1, 25] = 10,
):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)

    def ensure_self(data: dict):
        _ensure_account(data, uid, interaction.user.display_name)

    _, data = update_data(ensure_self)
    txs = list(reversed(data[uid].get('transactions', [])))[0:limit]
    if not txs:
        await interaction.followup.send('📭 Chưa có giao dịch nào.', ephemeral=True)
        return
    lines = [f'🧾 **{len(txs)} giao dịch gần đây**\n']
    lines.extend(_tx_line(tx) for tx in txs)
    await interaction.followup.send('\n'.join(lines), ephemeral=True)


economy_group = app_commands.Group(name='economy', description='Lệnh kinh tế coins ảo')

@economy_group.command(name='leaderboard', description='Top tài sản theo total earned')
async def economy_leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()
    data = load_data()
    top10 = sorted(
        [(uid, info) for uid, info in data.items() if info.get('total_earned', 0) > 0],
        key=lambda item: item[1].get('total_earned', 0),
        reverse=True,
    )[:10]
    lines = ['💰 **Economy Leaderboard — Total Earned**\n']
    if not top10:
        lines.append('📭 Chưa có ai kiếm coins.')
    else:
        for i, (_, info) in enumerate(top10, 1):
            medal = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
            debt = _active_debt(info)
            class_idx = info.get('class', info.get('level', 0))
            lines.append(
                f'{medal} **{info["name"]}** `{class_label(class_idx)}`\n'
                f'       💰 Earned `{format_coins(info.get("total_earned", 0))}`'
                f' · 💵 Balance `{format_coins(info.get("balance", 0))}`'
                f' · 💳 Debt `{format_coins(debt)}`'
            )
    await interaction.followup.send('\n'.join(lines))


async def economy_adjust(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: int,
    reason: str = 'Admin adjustment',
):
    if not await _require_admin(interaction, 'economy.adjust'):
        return
    await interaction.response.defer(ephemeral=True)
    if amount == 0:
        await interaction.followup.send('❌ Amount phải khác 0.', ephemeral=True)
        return

    uid = str(member.id)

    def mutator(data: dict):
        account = _ensure_account(data, uid, member.display_name)
        new_balance = account.get('balance', 0) + amount
        if new_balance < 0:
            return {'ok': False, 'error': f'Balance không thể âm. Hiện có {format_coins(account.get("balance", 0))}.'}
        account['balance'] = new_balance
        _sync_money_class(account)
        _append_transaction(
            account,
            'admin_adjustment',
            amount,
            reason[:120],
            counterparty=str(interaction.user.id),
        )
        return {'ok': True, 'balance': account['balance']}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể adjust.")}', ephemeral=True)
        return
    await interaction.followup.send(
        f'✅ Đã adjust **{member.display_name}** `{format_coins(amount)}`.\n'
        f'Balance mới: `{format_coins(result["balance"])}`',
        ephemeral=True,
    )


loan_group = app_commands.Group(name='loan', description='Vay và cho vay coins ảo')

@loan_group.command(name='borrow', description='Vay coins từ bot với lãi và hạn trả')
@app_commands.describe(amount='Số coins muốn vay')
async def loan_borrow(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 1_000_000_000],
):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)

    def mutator(data: dict):
        borrower = _ensure_account(data, uid, interaction.user.display_name)
        if amount > MAX_BOT_LOAN_AMOUNT:
            return {'ok': False, 'error': f'Tối đa mỗi khoản vay bot là {format_coins(MAX_BOT_LOAN_AMOUNT)}.'}
        if len(_active_loans(borrower)) >= MAX_ACTIVE_LOANS:
            return {'ok': False, 'error': f'Bạn đã có quá nhiều khoản vay active ({MAX_ACTIVE_LOANS}).'}
        if any(loan.get('lender_id') == 'bot' for loan in _active_loans(borrower)):
            return {'ok': False, 'error': 'Bạn đã có khoản vay bot đang active. Trả xong rồi vay tiếp nhé.'}

        interest = _loan_interest(amount, BOT_LOAN_INTEREST_PERCENT)
        total_due = amount + interest
        loan_id = _new_id('loan')
        due_date = (datetime.now() + timedelta(days=BOT_LOAN_DAYS)).strftime('%Y-%m-%d')
        loan = {
            'id': loan_id,
            'lender_id': 'bot',
            'lender_name': 'Study Bot',
            'borrower_id': uid,
            'borrower_name': interaction.user.display_name,
            'principal': amount,
            'interest_percent': BOT_LOAN_INTEREST_PERCENT,
            'interest': interest,
            'total_due': total_due,
            'remaining': total_due,
            'borrowed_at': datetime.now().isoformat(timespec='seconds'),
            'due_date': due_date,
            'status': 'active',
        }
        borrower['balance'] += amount
        borrower.setdefault('active_loans', []).append(loan)
        _sync_money_class(borrower)
        _append_transaction(borrower, 'borrowing', amount, 'Borrowed from Study Bot', counterparty='bot', meta={'loan_id': loan_id})
        if interest:
            _append_transaction(borrower, 'interest', interest, f'Interest added to debt ({BOT_LOAN_INTEREST_PERCENT:g}%)', counterparty='bot', meta={'loan_id': loan_id})
        _append_loan_history(borrower, 'borrow', f'Borrowed {format_coins(amount)} from Study Bot', loan_id, amount)
        return {'ok': True, 'loan': loan, 'balance': borrower['balance']}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể vay coins.")}', ephemeral=True)
        return

    loan = result['loan']
    await interaction.followup.send(
        f'✅ Đã vay **{format_coins(amount)}** từ bot.\n'
        f'💳 Nợ phải trả: `{format_coins(loan["remaining"])}` '
        f'(lãi `{BOT_LOAN_INTEREST_PERCENT:g}%`) · Hạn: `{loan["due_date"]}`\n'
        f'🆔 Loan ID: `{loan["id"]}` · Balance: `{format_coins(result["balance"])}`',
        ephemeral=True,
    )
    if isinstance(interaction.user, discord.Member):
        await notify_loan_event(
            interaction.user,
            interaction.channel,
            'Khoản vay đã tạo',
            amount,
            f'Nợ phải trả: `{format_coins(loan["remaining"])}`. Hạn: `{loan["due_date"]}`.',
        )


@loan_group.command(name='repay', description='Trả nợ active')
@app_commands.describe(amount='Số coins muốn trả')
async def loan_repay(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 1_000_000_000],
):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)

    def mutator(data: dict):
        borrower = _ensure_account(data, uid, interaction.user.display_name)
        debt = _active_debt(borrower)
        if debt <= 0:
            return {'ok': False, 'error': 'Bạn không có khoản nợ active.'}
        if amount > debt:
            return {'ok': False, 'error': f'Không thể trả quá số nợ hiện tại ({format_coins(debt)}).'}
        if borrower.get('balance', 0) < amount:
            return {'ok': False, 'error': f'Balance không đủ. Bạn có {format_coins(borrower.get("balance", 0))}.'}

        borrower['balance'] -= amount
        remaining_payment = amount
        paid_loans = []
        active = sorted(_active_loans(borrower), key=lambda loan: loan.get('due_date', '9999-99-99'))
        for loan in active:
            if remaining_payment <= 0:
                break
            pay_now = min(remaining_payment, _as_int(loan.get('remaining', 0)))
            loan['remaining'] -= pay_now
            remaining_payment -= pay_now
            lender_id = loan.get('lender_id')
            if lender_id and lender_id != 'bot':
                lender = _ensure_account(data, str(lender_id), loan.get('lender_name', f'User {lender_id}'))
                lender['balance'] += pay_now
                _sync_money_class(lender)
                _append_transaction(
                    lender,
                    'repayment',
                    pay_now,
                    f'Repayment from {interaction.user.display_name}',
                    counterparty=uid,
                    meta={'loan_id': loan.get('id')},
                )
                _append_loan_history(lender, 'repayment_received', f'Received {format_coins(pay_now)} from {interaction.user.display_name}', loan.get('id', '?'), pay_now)
            _append_loan_history(borrower, 'repay', f'Repaid {format_coins(pay_now)} to {loan.get("lender_name", "Unknown")}', loan.get('id', '?'), pay_now)
            if loan['remaining'] <= 0:
                loan['status'] = 'paid'
                borrower['credit_score'] = min(850, _as_int(borrower.get('credit_score', 600), 600) + 15)
                paid_loans.append(str(loan.get('id', '?')))

        borrower['active_loans'] = [loan for loan in active if loan.get('remaining', 0) > 0]
        _sync_money_class(borrower)
        _append_transaction(borrower, 'repayment', -amount, 'Loan repayment', meta={'paid_loans': paid_loans})
        return {'ok': True, 'balance': borrower['balance'], 'debt': _active_debt(borrower), 'paid_loans': paid_loans}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể trả nợ.")}', ephemeral=True)
        return

    paid_note = f'\n✅ Đã tất toán: `{", ".join(result["paid_loans"])}`' if result.get('paid_loans') else ''
    await interaction.followup.send(
        f'✅ Đã trả **{format_coins(amount)}**.\n'
        f'💵 Balance: `{format_coins(result["balance"])}` · Debt còn lại: `{format_coins(result["debt"])}`'
        f'{paid_note}',
        ephemeral=True,
    )
    if isinstance(interaction.user, discord.Member):
        await notify_loan_event(
            interaction.user,
            interaction.channel,
            'Đã trả nợ',
            amount,
            f'Dư nợ còn lại: `{format_coins(result["debt"])}`.',
        )


@loan_group.command(name='status', description='Xem nợ, lãi, hạn trả và credit score')
async def loan_status(interaction: discord.Interaction):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)

    def ensure_self(data: dict):
        _ensure_account(data, uid, interaction.user.display_name)

    _, data = update_data(ensure_self)
    info = data[uid]
    loans = _active_loans(info)
    incoming = _pending_incoming_offers(data, uid)
    outgoing = [
        offer for offer in info.get('loan_offers', [])
        if isinstance(offer, dict) and offer.get('status', 'pending') == 'pending'
    ]
    lines = [
        f'💳 **Loan status — {interaction.user.display_name}**',
        f'Debt: `{format_coins(_active_debt(info))}` · Balance: `{format_coins(info.get("balance", 0))}` · Credit score: `{_credit_score(info)}`',
    ]
    if not loans:
        lines.append('\n✅ Không có khoản vay active.')
    else:
        lines.append('\n**Khoản vay active:**')
        lines.extend(_loan_line(loan) for loan in loans)
    if incoming:
        lines.append('\n**Offer bạn có thể nhận:**')
        lines.extend(_offer_line(offer, incoming=True) for offer in incoming[:5])
        if len(incoming) > 5:
            lines.append(f'_...còn {len(incoming) - 5} offer khác_')
    if outgoing:
        lines.append('\n**Offer bạn đã gửi:**')
        lines.extend(_offer_line(offer, incoming=False) for offer in outgoing[:5])
        if len(outgoing) > 5:
            lines.append(f'_...còn {len(outgoing) - 5} offer khác_')
    await interaction.followup.send('\n'.join(lines), ephemeral=True)


@loan_group.command(name='offer', description='Tạo lời mời cho vay user-to-user')
@app_commands.describe(member='Người vay', amount='Số coins cho vay', interest_percent='Lãi suất %', days='Số ngày đến hạn')
async def loan_offer(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 1_000_000_000],
    interest_percent: app_commands.Range[float, 0.0, 100.0],
    days: app_commands.Range[int, 1, 365],
):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    if member.bot:
        await interaction.followup.send('❌ Không thể cho bot vay.', ephemeral=True)
        return
    if member.id == interaction.user.id:
        await interaction.followup.send('❌ Không thể tạo khoản vay với chính mình.', ephemeral=True)
        return

    lender_uid = str(interaction.user.id)
    borrower_uid = str(member.id)

    def mutator(data: dict):
        lender = _ensure_account(data, lender_uid, interaction.user.display_name)
        borrower = _ensure_account(data, borrower_uid, member.display_name)
        if lender.get('balance', 0) < amount:
            return {'ok': False, 'error': f'Balance lender không đủ ({format_coins(lender.get("balance", 0))}).'}
        if _pending_loan_offer_count(lender) >= MAX_PENDING_LOAN_OFFERS:
            return {'ok': False, 'error': 'Bạn đang có quá nhiều offer pending.'}
        if any(o.get('borrower_id') == borrower_uid for o in lender.get('loan_offers', []) if o.get('status', 'pending') == 'pending'):
            return {'ok': False, 'error': 'Bạn đã có offer pending cho người này.'}

        loan_id = _new_id('loan')
        interest = _loan_interest(amount, interest_percent)
        offer = {
            'id': loan_id,
            'status': 'pending',
            'lender_id': lender_uid,
            'lender_name': interaction.user.display_name,
            'borrower_id': borrower_uid,
            'borrower_name': member.display_name,
            'amount': amount,
            'interest_percent': interest_percent,
            'interest': interest,
            'days': days,
            'created_at': datetime.now().isoformat(timespec='seconds'),
        }
        lender.setdefault('loan_offers', []).append(offer)
        _append_loan_history(lender, 'offer', f'Offered {format_coins(amount)} to {member.display_name}', loan_id, amount)
        _append_loan_history(borrower, 'offer_received', f'Loan offer from {interaction.user.display_name}: {format_coins(amount)}', loan_id, amount)
        return {'ok': True, 'offer': offer}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể tạo offer.")}', ephemeral=True)
        return

    offer = result['offer']
    await interaction.followup.send(
        f'✅ Đã tạo loan offer cho **{member.display_name}**.\n'
        f'🆔 Loan ID: `{offer["id"]}` · Amount `{format_coins(amount)}` · Lãi `{interest_percent:g}%` '
        f'· Due sau `{days}` ngày\n'
        f'Người vay dùng `/loan accept {offer["id"]}` để nhận.',
        ephemeral=True,
    )
    await notify_loan_event(
        member,
        interaction.channel,
        'Bạn nhận được lời mời vay',
        amount,
        f'Người cho vay: **{interaction.user.display_name}**. Lãi `{interest_percent:g}%`, thời hạn `{days}` ngày.',
    )


@loan_group.command(name='accept', description='Chấp nhận loan offer')
@app_commands.describe(loan_id='ID của loan offer')
async def loan_accept(interaction: discord.Interaction, loan_id: str):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    borrower_uid = str(interaction.user.id)

    def mutator(data: dict):
        found = _find_pending_offer(data, loan_id)
        if not found:
            return {'ok': False, 'error': 'Không tìm thấy loan offer pending.'}
        lender_uid, lender, offer = found
        if offer.get('borrower_id') != borrower_uid:
            return {'ok': False, 'error': 'Offer này không dành cho bạn.'}
        if lender_uid == borrower_uid:
            return {'ok': False, 'error': 'Khoản vay không hợp lệ: lender và borrower trùng nhau.'}

        borrower = _ensure_account(data, borrower_uid, interaction.user.display_name)
        lender = _ensure_account(data, lender_uid, offer.get('lender_name', f'User {lender_uid}'))
        amount = _as_int(offer.get('amount', 0))
        if amount <= 0:
            return {'ok': False, 'error': 'Offer có amount không hợp lệ.'}
        if lender.get('balance', 0) < amount:
            return {'ok': False, 'error': 'Lender hiện không đủ balance để giải ngân.'}
        if len(_active_loans(borrower)) >= MAX_ACTIVE_LOANS:
            return {'ok': False, 'error': f'Bạn đã có quá nhiều khoản vay active ({MAX_ACTIVE_LOANS}).'}

        interest_percent = _as_float(offer.get('interest_percent', 0))
        interest = _as_int(offer.get('interest', _loan_interest(amount, interest_percent)))
        total_due = amount + interest
        due_date = (datetime.now() + timedelta(days=_as_int(offer.get('days', 1), 1))).strftime('%Y-%m-%d')
        loan = {
            'id': offer['id'],
            'lender_id': lender_uid,
            'lender_name': lender.get('name', offer.get('lender_name', 'Unknown')),
            'borrower_id': borrower_uid,
            'borrower_name': interaction.user.display_name,
            'principal': amount,
            'interest_percent': interest_percent,
            'interest': interest,
            'total_due': total_due,
            'remaining': total_due,
            'borrowed_at': datetime.now().isoformat(timespec='seconds'),
            'due_date': due_date,
            'status': 'active',
        }
        offer['status'] = 'accepted'
        lender['balance'] -= amount
        borrower['balance'] += amount
        borrower.setdefault('active_loans', []).append(loan)
        _sync_money_class(lender)
        _sync_money_class(borrower)
        _append_transaction(lender, 'lending', -amount, f'Lent to {interaction.user.display_name}', counterparty=borrower_uid, meta={'loan_id': loan['id']})
        _append_transaction(borrower, 'borrowing', amount, f'Borrowed from {lender.get("name", "Unknown")}', counterparty=lender_uid, meta={'loan_id': loan['id']})
        if interest:
            _append_transaction(borrower, 'interest', interest, f'Interest added to debt ({interest_percent:g}%)', counterparty=lender_uid, meta={'loan_id': loan['id']})
        _append_loan_history(lender, 'lent', f'Lent {format_coins(amount)} to {interaction.user.display_name}', loan['id'], amount)
        _append_loan_history(borrower, 'borrow', f'Borrowed {format_coins(amount)} from {lender.get("name", "Unknown")}', loan['id'], amount)
        return {'ok': True, 'loan': loan, 'balance': borrower['balance']}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể accept loan.")}', ephemeral=True)
        return

    loan = result['loan']
    await interaction.followup.send(
        f'✅ Đã nhận **{format_coins(loan["principal"])}** từ **{loan["lender_name"]}**.\n'
        f'💳 Debt: `{format_coins(loan["remaining"])}` · Lãi `{loan["interest_percent"]:g}%` · Due `{loan["due_date"]}`',
        ephemeral=True,
    )


@loan_group.command(name='cancel', description='Hủy loan offer pending của bạn')
@app_commands.describe(loan_id='ID của loan offer')
async def loan_cancel(interaction: discord.Interaction, loan_id: str):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    lender_uid = str(interaction.user.id)

    def mutator(data: dict):
        found = _find_pending_offer(data, loan_id)
        if not found:
            return {'ok': False, 'error': 'Không tìm thấy loan offer pending.'}
        owner_uid, lender, offer = found
        if owner_uid != lender_uid:
            return {'ok': False, 'error': 'Bạn chỉ có thể hủy offer của chính mình.'}
        borrower_uid = offer.get('borrower_id')
        borrower = data.get(str(borrower_uid))
        offer['status'] = 'cancelled'
        _append_loan_history(lender, 'cancel', f'Cancelled offer to {offer.get("borrower_name", "Unknown")}', loan_id, _as_int(offer.get('amount', 0)))
        if isinstance(borrower, dict):
            _append_loan_history(borrower, 'offer_cancelled', f'Offer from {interaction.user.display_name} was cancelled', loan_id, _as_int(offer.get('amount', 0)))
        return {'ok': True}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'❌ {result.get("error", "Không thể hủy offer.")}', ephemeral=True)
        return
    await interaction.followup.send(f'✅ Đã hủy loan offer `{loan_id}`.', ephemeral=True)


@loan_group.command(name='history', description='Xem lịch sử loan gần đây')
async def loan_history(interaction: discord.Interaction):
    if not await _require_game_setup_channel_interaction(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)

    def ensure_self(data: dict):
        _ensure_account(data, uid, interaction.user.display_name)

    _, data = update_data(ensure_self)
    history = list(reversed(data[uid].get('loan_history', [])))[:10]
    if not history:
        await interaction.followup.send('📭 Chưa có lịch sử loan.', ephemeral=True)
        return
    lines = ['📜 **Loan history gần đây**\n']
    for event in history:
        sign = '+' if _as_int(event.get('amount', 0)) > 0 else ''
        lines.append(
            f'`{event.get("ts", "")}` **{event.get("action", "")}** '
            f'`{event.get("loan_id", "?")}` `{sign}{format_coins(event.get("amount", 0))}` · {event.get("description", "")}'
        )
    await interaction.followup.send('\n'.join(lines), ephemeral=True)

notify_group = app_commands.Group(name='notify', description='Cài đặt thông báo tự động')

@notify_group.command(name='on', description='Bật thông báo tự động từ bot')
async def notify_on(interaction: discord.Interaction):
    display_name = getattr(interaction.user, 'display_name', getattr(interaction.user, 'name', 'Unknown'))
    set_notifications_enabled(interaction.user.id, True, display_name)
    await interaction.response.send_message('Đã bật thông báo tự động.', ephemeral=True)

@notify_group.command(name='off', description='Tắt thông báo tự động từ bot')
async def notify_off(interaction: discord.Interaction):
    display_name = getattr(interaction.user, 'display_name', getattr(interaction.user, 'name', 'Unknown'))
    set_notifications_enabled(interaction.user.id, False, display_name)
    await interaction.response.send_message(
        'Đã tắt thông báo tự động. Bot sẽ bỏ qua thông báo chủ động cho bạn.',
        ephemeral=True,
    )

@notify_group.command(name='status', description='Xem trạng thái thông báo tự động')
async def notify_status(interaction: discord.Interaction):
    enabled = notifications_enabled_for(interaction.user.id)
    await interaction.response.send_message(
        f"Thông báo tự động hiện đang: **{'Bật' if enabled else 'Tắt'}**",
        ephemeral=True,
    )

economy_group.remove_command('adjust')
bot.tree.add_command(economy_group)
bot.tree.add_command(loan_group)
bot.tree.add_command(notify_group)

# ── /studying ──────────────────────────────────────────────────────────────

@bot.tree.command(name='studying', description='Xem ai đang học ngay lúc này')
async def slash_studying(interaction: discord.Interaction):
    await interaction.response.defer()
    now   = datetime.now()
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('❌ Lệnh này chỉ dùng được trong server!', ephemeral=True)
        return
    data  = _get_live_enriched_data(guild)
    today = now.strftime('%Y-%m-%d')
    lines = ['🟢 **Đang học ngay lúc này**\n']
    count = 0
    for mid, st in sorted(join_times.items()):
        m = guild.get_member(mid)
        if not m or not m.voice: continue
        secs  = int((now - st).total_seconds())
        uid   = str(mid)
        saved = data.get(uid, {}).get('daily', {}).get(today, 0)
        total = saved + _get_unsaved_study_seconds(mid, now)
        icon  = media_status_icon(m.voice)
        count += 1
        lines.append(f'{icon} **{m.display_name}** | Phiên: `{format_time(secs)}` | Hôm nay: `{format_time(total)}`')
    if count == 0: lines.append('😴 Không có ai đang học...')
    else:          lines.append(f'\n👥 `{count} người`')
    await interaction.followup.send('\n'.join(lines))

# ── /setgoal ───────────────────────────────────────────────────────────────

@bot.tree.command(name='setgoal', description='Đặt mục tiêu học tập hàng ngày')
@app_commands.describe(goal='Mô tả mục tiêu', hours='Số giờ (0-23)', minutes='Số phút (0-59)')
async def slash_setgoal(
    interaction: discord.Interaction,
    goal: str,
    hours:   app_commands.Range[int, 0, 23] = 0,
    minutes: app_commands.Range[int, 0, 59] = 0,
):
    await interaction.response.defer(ephemeral=True)
    total = hours * 3600 + minutes * 60
    if total <= 0:
        await interaction.followup.send('❌ Ít nhất 1 phút!', ephemeral=True); return
    uid  = str(interaction.user.id)

    def save_goal(data: dict):
        if uid not in data:
            data[uid] = _default_user(interaction.user.display_name)
        data[uid]['goal'] = goal
        data[uid]['goal_seconds'] = total

    update_data(save_goal)
    await interaction.followup.send(
        f'✅ Mục tiêu: **"{goal}"** — `{format_time(total)}`/ngày 💪', ephemeral=True
    )

# ── /remind disabled ───────────────────────────────────────────────────────

@bot.tree.command(name='remind', description='Tính năng nhắc học tự động đã tắt')
@app_commands.describe(hour='Tham số cũ, không còn tạo nhắc học')
async def slash_remind(interaction: discord.Interaction, hour: app_commands.Range[int, -1, 23]):
    await interaction.response.defer(ephemeral=True)
    uid  = str(interaction.user.id)

    old = remind_tasks.pop(interaction.user.id, None)
    if old:
        task = old[1]
        if task and not task.done(): task.cancel()

    def disable_remind(data: dict):
        if uid in data:
            data[uid]['remind_hour'] = None

    update_data(disable_remind)

    await interaction.followup.send(
        'Tính năng nhắc học tự động đã được tắt. Bot sẽ không gửi DM hoặc thông báo nhắc học.',
        ephemeral=True
    )

# ── /room_panel ────────────────────────────────────────────────────────────

def _resolve_room_control_channel(interaction: discord.Interaction) -> tuple[discord.VoiceChannel | None, str | None]:
    _capture_guild_context(interaction.guild_id)
    if not interaction.guild:
        return None, '❌ Chức năng này chỉ dùng được trong server.'

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not member or not member.voice or not isinstance(member.voice.channel, discord.VoiceChannel):
        return None, '❌ Bạn cần ở trong phòng học tạm để dùng điều khiển phòng.'

    channel = member.voice.channel
    if not _is_temporary_room_id(channel.id):
        return None, '❌ Điều khiển này chỉ áp dụng cho phòng tạm do bot tạo.'

    meta = _temp_room_meta(channel.id, interaction.guild.id)
    is_owner = meta.get('owner_id') == member.id
    is_admin = member.guild_permissions.manage_channels
    if not (is_owner or is_admin):
        return None, '❌ Chỉ chủ phòng hoặc admin có quyền điều khiển phòng này.'

    bot_member = interaction.guild.me or (interaction.guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member or not channel.permissions_for(bot_member).manage_channels:
        return None, '❌ Bot thiếu quyền **Manage Channels** trong phòng này.'

    return channel, None

async def _send_room_control_error(interaction: discord.Interaction, message: str):
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


class RoomRenameModal(discord.ui.Modal, title='Đổi tên phòng'):
    room_name = discord.ui.TextInput(
        label='Tên phòng mới',
        min_length=1,
        max_length=90,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        _capture_guild_context(interaction.guild_id)
        channel, error = _resolve_room_control_channel(interaction)
        if error or channel is None:
            await _send_room_control_error(interaction, error or '❌ Không thể đổi tên phòng.')
            return
        new_name = ' '.join(str(self.room_name.value).split()).strip()
        if not new_name:
            await interaction.response.send_message('❌ Tên phòng không hợp lệ.', ephemeral=True)
            return

        try:
            await channel.edit(
                name=new_name[:90],
                reason=f'Temporary room renamed by {interaction.user} ({interaction.user.id})',
            )
            await interaction.response.send_message(f'✅ Đã đổi tên phòng thành **{new_name[:90]}**.', ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message('❌ Bot thiếu quyền đổi tên phòng.', ephemeral=True)
        except discord.HTTPException as e:
            log.warning(f'[TempRoom] Rename failed for {channel.id}: {e}')
            await interaction.response.send_message('❌ Đổi tên thất bại. Thử lại sau nhé.', ephemeral=True)


def _interaction_display_name(interaction: discord.Interaction) -> str:
    return getattr(interaction.user, 'display_name', getattr(interaction.user, 'name', 'Unknown'))


async def _panel_show_balance(interaction: discord.Interaction):
    _capture_guild_context(interaction.guild_id)
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)
    display_name = _interaction_display_name(interaction)

    def ensure_self(data: dict):
        _ensure_account(data, uid, display_name)

    _, data = update_data(ensure_self)
    info = data[uid]
    debt = _active_debt(info)
    class_idx = info.get('class', info.get('level', 0))
    embed = discord.Embed(
        title='💼 Ví coins ảo của bạn',
        description=(
            f'**Balance:** `{format_coins(info.get("balance", 0))}`\n'
            f'**Total earned:** `{format_coins(info.get("total_earned", 0))}`\n'
            f'**Class/Level:** `{class_label(class_idx)}`\n'
            f'**Debt:** `{format_coins(debt)}`\n'
            f'**Credit score:** `{_credit_score(info)}`\n\n'
            '_Tất cả chỉ là coins ảo trong server, không phải tiền thật._'
        ),
        color=0xF1C40F,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


async def _panel_loan_status(interaction: discord.Interaction):
    _capture_guild_context(interaction.guild_id)
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)
    display_name = _interaction_display_name(interaction)

    def ensure_self(data: dict):
        _ensure_account(data, uid, display_name)

    _, data = update_data(ensure_self)
    info = data[uid]
    loans = _active_loans(info)
    incoming = _pending_incoming_offers(data, uid)
    outgoing = [
        offer for offer in info.get('loan_offers', [])
        if isinstance(offer, dict) and offer.get('status', 'pending') == 'pending'
    ]

    lines = [
        f'💳 **Nợ/Vay — {display_name}**',
        f'Debt active: `{format_coins(_active_debt(info))}` · '
        f'Balance: `{format_coins(info.get("balance", 0))}` · '
        f'Credit score: `{_credit_score(info)}`',
    ]

    if loans:
        lines.append('\n**Khoản vay active:**')
        lines.extend(_loan_line(loan) for loan in loans[:5])
        if len(loans) > 5:
            lines.append(f'_...còn {len(loans) - 5} khoản vay khác_')
    else:
        lines.append('\n✅ Không có khoản vay active.')

    if incoming:
        lines.append('\n**Offer bạn có thể nhận:**')
        lines.extend(_offer_line(offer, incoming=True) for offer in incoming[:5])
        if len(incoming) > 5:
            lines.append(f'_...còn {len(incoming) - 5} offer khác_')
        lines.append('_Dùng `/loan accept <loan_id>` để nhận offer._')

    if outgoing:
        lines.append('\n**Offer bạn đã gửi:**')
        lines.extend(_offer_line(offer, incoming=False) for offer in outgoing[:5])
        if len(outgoing) > 5:
            lines.append(f'_...còn {len(outgoing) - 5} offer khác_')

    await interaction.followup.send('\n'.join(lines), ephemeral=True)


class BorrowModal(discord.ui.Modal, title='🏦 Vay coins ảo'):
    amount = discord.ui.TextInput(
        label='Số coins muốn vay',
        placeholder='Ví dụ: 1000',
        required=True,
        max_length=12,
    )

    async def on_submit(self, interaction: discord.Interaction):
        _capture_guild_context(interaction.guild_id)
        amount, error = _parse_positive_int(str(self.amount.value), 'Số coins')
        if error:
            await interaction.response.send_message(f'❌ {error}', ephemeral=True)
            return

        result = _borrow_from_bot(interaction.user.id, _interaction_display_name(interaction), amount)
        if not result.get('ok'):
            await interaction.response.send_message(f'❌ {result.get("error", "Không thể vay coins.")}', ephemeral=True)
            return

        loan = result['loan']
        await interaction.response.send_message(
            f'✅ Đã vay **{format_coins(amount)}** coins ảo từ bot.\n'
            f'💳 Nợ phải trả: `{format_coins(loan["remaining"])}` '
            f'(lãi `{BOT_LOAN_INTEREST_PERCENT:g}%`) · Hạn: `{loan["due_date"]}`\n'
            f'🆔 Loan ID: `{loan["id"]}` · Balance: `{format_coins(result["balance"])}`',
            ephemeral=True,
        )
        if isinstance(interaction.user, discord.Member):
            await notify_loan_event(
                interaction.user,
                interaction.channel,
                'Khoản vay đã tạo',
                amount,
                f'Nợ phải trả: `{format_coins(loan["remaining"])}`. Hạn: `{loan["due_date"]}`.',
            )


class RepayModal(discord.ui.Modal, title='💳 Trả nợ'):
    amount = discord.ui.TextInput(
        label='Số coins muốn trả',
        placeholder='Ví dụ: 500',
        required=True,
        max_length=12,
    )

    async def on_submit(self, interaction: discord.Interaction):
        _capture_guild_context(interaction.guild_id)
        amount, error = _parse_positive_int(str(self.amount.value), 'Số coins')
        if error:
            await interaction.response.send_message(f'❌ {error}', ephemeral=True)
            return

        result = _repay_active_loans(interaction.user.id, _interaction_display_name(interaction), amount)
        if not result.get('ok'):
            await interaction.response.send_message(f'❌ {result.get("error", "Không thể trả nợ.")}', ephemeral=True)
            return

        paid_note = f'\n✅ Đã tất toán: `{", ".join(result["paid_loans"])}`' if result.get('paid_loans') else ''
        await interaction.response.send_message(
            f'✅ Đã trả **{format_coins(amount)}** coins ảo.\n'
            f'💵 Balance: `{format_coins(result["balance"])}` · Debt còn lại: `{format_coins(result["debt"])}`'
            f'{paid_note}',
            ephemeral=True,
        )
        if isinstance(interaction.user, discord.Member):
            await notify_loan_event(
                interaction.user,
                interaction.channel,
                'Đã trả nợ',
                amount,
                f'Dư nợ còn lại: `{format_coins(result["debt"])}`.',
            )


class LendModal(discord.ui.Modal, title='🤝 Cho vay coins ảo'):
    borrower = discord.ui.TextInput(
        label='Người vay',
        placeholder='Mention hoặc user ID',
        required=True,
        max_length=40,
    )
    amount = discord.ui.TextInput(
        label='Số coins',
        placeholder='Ví dụ: 1000',
        required=True,
        max_length=12,
    )
    interest = discord.ui.TextInput(
        label='Lãi suất %',
        placeholder='Ví dụ: 10',
        required=True,
        max_length=6,
    )
    days = discord.ui.TextInput(
        label='Thời hạn ngày',
        placeholder='Ví dụ: 7',
        required=True,
        max_length=3,
    )

    async def on_submit(self, interaction: discord.Interaction):
        _capture_guild_context(interaction.guild_id)
        amount, error = _parse_positive_int(str(self.amount.value), 'Số coins')
        if error:
            await interaction.response.send_message(f'❌ {error}', ephemeral=True)
            return
        interest_percent, error = _parse_percent(str(self.interest.value))
        if error:
            await interaction.response.send_message(f'❌ {error}', ephemeral=True)
            return
        days, error = _parse_positive_int(str(self.days.value), 'Thời hạn')
        if error:
            await interaction.response.send_message(f'❌ {error}', ephemeral=True)
            return
        if days > 365:
            await interaction.response.send_message('❌ Thời hạn phải từ 1 đến 365 ngày.', ephemeral=True)
            return

        member, error = await _resolve_guild_member_from_input(interaction, str(self.borrower.value))
        if error or member is None:
            await interaction.response.send_message(f'❌ {error or "Không tìm thấy người vay."}', ephemeral=True)
            return
        if member.bot:
            await interaction.response.send_message('❌ Không thể cho bot vay.', ephemeral=True)
            return
        if member.id == interaction.user.id:
            await interaction.response.send_message('❌ Không thể tạo khoản vay với chính mình.', ephemeral=True)
            return

        result = _create_user_loan_offer(
            lender_id=interaction.user.id,
            lender_name=_interaction_display_name(interaction),
            borrower_id=member.id,
            borrower_name=member.display_name,
            amount=amount,
            interest_percent=interest_percent,
            days=days,
        )
        if not result.get('ok'):
            await interaction.response.send_message(f'❌ {result.get("error", "Không thể tạo offer.")}', ephemeral=True)
            return

        offer = result['offer']
        await interaction.response.send_message(
            f'✅ Đã tạo loan offer bằng coins ảo cho **{member.display_name}**.\n'
            f'🆔 Loan ID: `{offer["id"]}` · Amount `{format_coins(amount)}` · '
            f'Lãi `{interest_percent:g}%` · Due sau `{days}` ngày\n'
            f'Người vay dùng `/loan accept {offer["id"]}` để nhận.',
            ephemeral=True,
        )
        await notify_loan_event(
            member,
            interaction.channel,
            'Bạn nhận được lời mời vay',
            amount,
            f'Người cho vay: **{_interaction_display_name(interaction)}**. Lãi `{interest_percent:g}%`, thời hạn `{days}` ngày.',
        )


class RoomControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label='Khóa', emoji='🔒', style=discord.ButtonStyle.secondary, row=0, custom_id='room_lock')
    async def lock_room(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel, error = _resolve_room_control_channel(interaction)
        if error or channel is None:
            await _send_room_control_error(interaction, error or '❌ Không thể khóa phòng.')
            return
        await interaction.response.defer(ephemeral=True)
        overwrite = channel.overwrites_for(interaction.guild.default_role)
        overwrite.connect = False
        try:
            await channel.set_permissions(
                interaction.guild.default_role,
                overwrite=overwrite,
                reason=f'Temporary room locked by {interaction.user} ({interaction.user.id})',
            )
            await interaction.followup.send(f'🔒 Đã khóa **{channel.name}**.', ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send('❌ Bot thiếu quyền khóa phòng.', ephemeral=True)
        except discord.HTTPException as e:
            log.warning(f'[TempRoom] Lock failed for {channel.id}: {e}')
            await interaction.followup.send('❌ Khóa phòng thất bại. Thử lại sau nhé.', ephemeral=True)

    @discord.ui.button(label='Mở', emoji='🔓', style=discord.ButtonStyle.secondary, row=0, custom_id='room_unlock')
    async def unlock_room(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel, error = _resolve_room_control_channel(interaction)
        if error or channel is None:
            await _send_room_control_error(interaction, error or '❌ Không thể mở phòng.')
            return
        await interaction.response.defer(ephemeral=True)
        overwrite = channel.overwrites_for(interaction.guild.default_role)
        overwrite.connect = None
        try:
            await channel.set_permissions(
                interaction.guild.default_role,
                overwrite=None if overwrite.is_empty() else overwrite,
                reason=f'Temporary room unlocked by {interaction.user} ({interaction.user.id})',
            )
            await interaction.followup.send(f'🔓 Đã mở **{channel.name}**.', ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send('❌ Bot thiếu quyền mở phòng.', ephemeral=True)
        except discord.HTTPException as e:
            log.warning(f'[TempRoom] Unlock failed for {channel.id}: {e}')
            await interaction.followup.send('❌ Mở phòng thất bại. Thử lại sau nhé.', ephemeral=True)

    @discord.ui.button(label='Đổi tên', emoji='📝', style=discord.ButtonStyle.primary, row=0, custom_id='room_rename')
    async def rename_room(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel, error = _resolve_room_control_channel(interaction)
        if error or channel is None:
            await _send_room_control_error(interaction, error or '❌ Không thể đổi tên phòng.')
            return
        await interaction.response.send_modal(RoomRenameModal())

    @discord.ui.button(label='Xóa', emoji='🗑️', style=discord.ButtonStyle.danger, row=0, custom_id='room_delete')
    async def delete_room(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel, error = _resolve_room_control_channel(interaction)
        if error or channel is None:
            await _send_room_control_error(interaction, error or '❌ Không thể xóa phòng.')
            return
        room_name = channel.name
        await interaction.response.defer(ephemeral=True)
        deleted = await _delete_temporary_room(
            channel,
            f'Temporary room deleted by {interaction.user} ({interaction.user.id})',
        )
        if deleted:
            await interaction.followup.send(f'🗑️ Đã xóa **{room_name}**.', ephemeral=True)
        else:
            await interaction.followup.send('❌ Xóa phòng thất bại. Kiểm tra quyền bot rồi thử lại.', ephemeral=True)

    @discord.ui.button(label='Ví tiền', emoji='💰', style=discord.ButtonStyle.primary, row=1, custom_id='eco_balance')
    async def eco_balance(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _panel_show_balance(interaction)

    @discord.ui.button(label='Vay', emoji='🏦', style=discord.ButtonStyle.primary, row=1, custom_id='eco_borrow')
    async def eco_borrow(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(BorrowModal())

    @discord.ui.button(label='Trả nợ', emoji='💳', style=discord.ButtonStyle.secondary, row=1, custom_id='eco_repay')
    async def eco_repay(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RepayModal())

    @discord.ui.button(label='Cho vay', emoji='🤝', style=discord.ButtonStyle.success, row=1, custom_id='eco_lend')
    async def eco_lend(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LendModal())

    @discord.ui.button(label='Nợ/Vay', emoji='📜', style=discord.ButtonStyle.secondary, row=1, custom_id='loan_status')
    async def loan_status(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _panel_loan_status(interaction)


class RoomPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Bảng điều khiển', emoji='🎛️', style=discord.ButtonStyle.primary, custom_id='room_board')
    async def room_board(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title='🎛️ Bảng điều khiển phòng',
            description=(
                'Chọn chức năng bạn muốn dùng:\n\n'
                '**🏠 Phòng**\n'
                '🔒 Khóa · 🔓 Mở · 📝 Đổi tên · 🗑️ Xóa\n\n'
                '**💰 Kinh tế**\n'
                '💰 Ví tiền · 🏦 Vay · 💳 Trả nợ · 🤝 Cho vay · 📜 Nợ/Vay'
            ),
            color=0x5865F2,
        )
        await interaction.response.send_message(embed=embed, view=RoomControlView(), ephemeral=True)


@bot.tree.command(name='room_panel', description='Tạo bảng điều khiển phòng học')
async def slash_room_panel(interaction: discord.Interaction):
    embed = discord.Embed(
        title='Chào mừng đến với phòng học',
        description=(
            f'Chủ phòng: **{interaction.user.display_name}**\n\n'
            '**Bảng điều khiển**\n'
            'Nhấn nút bên dưới để điều khiển phòng tạm bạn đang tham gia\n\n'
            '**Chú ý**\n'
            '• Phòng sẽ mất khi không còn ai trong phòng\n'
            '• Bạn có thể gọi bot trong kênh này\n\n'
            '**Chúc bạn học vui :3**'
        ),
        color=0xFEE75C,
    )
    await interaction.response.send_message(
        content=interaction.user.mention,
        embed=embed,
        view=RoomPanelView()
    )

# ── /roles ─────────────────────────────────────────────────────────────────

@bot.tree.command(name='roles', description='Danh sách vai trò theo money class')
async def slash_roles(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    data  = load_data()
    uid   = str(interaction.user.id)
    my_info = data.get(uid, {}) if uid in data else {}
    my_earned = my_info.get('total_earned', 0)
    my_balance = my_info.get('balance', 0)
    my_debt = _active_debt(my_info) if my_info else 0
    my_lv = get_money_class(my_earned)
    guild = interaction.guild
    lines = [
        '🏷️ **Vai trò theo money class**',
        f'Bạn: `{class_label(my_lv)}` · Balance `{format_coins(my_balance)}` · '
        f'Total earned `{format_coins(my_earned)}` · Debt `{format_coins(my_debt)}`\n',
    ]
    for lv, default_role_name in CLASS_ROLE_NAMES.items():
        role = _find_class_role_by_level(guild, lv) if guild else None
        role_name = role.name if role else default_role_name
        coins_req = CLASS_THRESHOLDS[lv]
        is_mine = (lv == my_lv)
        is_done = (my_lv > lv)
        status  = ' ◀ **bạn đây**' if is_mine else (' ✅' if is_done else '')
        icon    = '✦' if is_mine else ('✔' if is_done else '○')
        lines.append(f'{icon} **{class_label(lv)}** `{format_coins(coins_req)}` → **{role_name}**{status}')
    await interaction.followup.send('\n'.join(lines), ephemeral=True)

# ── /help ──────────────────────────────────────────────────────────────────

def _build_game_help_lines(guild_id: int | None, channel_id: int | None) -> list[str]:
    channel_games = (
        _channel_game_keys(int(guild_id), int(channel_id))
        if guild_id and channel_id else set()
    )
    show_all_games = GAME_CHANNEL_ALL in channel_games
    lines: list[str] = []
    for game_key in GAME_ORDER:
        if not show_all_games and game_key not in channel_games:
            continue
        spec = GAME_CATALOG[game_key]
        commands = ' · '.join(f'`{command}`' for command in spec['commands'])
        detail_lines = ''.join(f'\n- {line}' for line in spec.get('details', ()))
        lines.append(
            f'**{spec["label"]}**\n'
            f'Lệnh: {commands}\n'
            f'Cách chơi: {spec["how_to"]}'
            f'{detail_lines}'
        )
    return lines


def _is_configured_game_channel(guild_id: int | None, channel_id: int | None) -> bool:
    if not guild_id or not channel_id:
        return False
    return int(channel_id) in _game_setup_channel_ids(int(guild_id))


def _build_game_only_help_message(guild_id: int | None, channel_id: int | None) -> str:
    game_lines = _build_game_help_lines(guild_id, channel_id)
    game_help = '\n\n'.join(game_lines) if game_lines else '_Channel này chưa được gán game nào._'
    return (
        '🎰 **LỆNH GAME CỦA KÊNH NÀY**\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        f'{_build_game_economy_help(guild_id)}\n\n'
        f'{game_help}\n\n'
        '`!help` · `!command` · `!commands` — xem lại hướng dẫn game của kênh này.'
    )


def _build_game_economy_help(guild_id: int | None = None) -> str:
    coin_rate = coins_per_minute_for(guild_id)
    return (
        '**💰 Tiền, daily và vay coins**\n'
        f'- Check tiền: `!wallet`, `!balance`, `!bal` hoặc `/balance`. Thêm `@member` để xem ví người khác nếu cần.\n'
        '- Ví hiển thị balance hiện tại, total earned, class, debt và credit score.\n'
        f'- Kiếm tiền học tập: vào phòng học và bật Cam hoặc Stream; bot cộng khoảng `{format_coins(coin_rate)}/phút` theo cấu hình server.\n'
        '- Kiếm tiền mỗi ngày: `/daily`, `!daily` hoặc `daily` nhận ngẫu nhiên `1,000-5,000` coins mỗi 24 giờ.\n'
        '- Kiếm thêm bằng task: `/tasks ideas`, `/tasks preset`, `/tasks add`, rồi `/tasks done <task_id>` để nhận coins.\n'
        '- Kiếm thêm bằng game: thắng game được cộng vào cùng ví; thua game trừ đúng tiền cược.\n'
        f'- Vay bot: `/loan borrow <amount>` vay tối đa `{format_coins(MAX_BOT_LOAN_AMOUNT)}`/khoản, lãi `{BOT_LOAN_INTEREST_PERCENT:g}%`, hạn `{BOT_LOAN_DAYS}` ngày.\n'
        '- Trả nợ/xem nợ: `/loan repay <amount>` để trả, `/loan status` để xem nợ, hạn trả, offer và credit score.\n'
        '- Vay/cho vay người khác: `/loan offer <member> <amount> <interest_percent> <days>`, người nhận dùng `/loan accept <loan_id>`.\n'
        '- Xem lịch sử tiền: `/transactions [limit]`; chuyển tiền cho người khác bằng `/pay <member> <amount>`.'
    )


def _build_full_help_message() -> str:
    return (
        '📚 **STUDY BOT — DANH SÁCH LỆNH**\n'
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n'
        '**📊 Thống kê cá nhân**\n'
        '`/rank [member]` — Ví, class và thống kê\n'
        '`/stats [member]` — Thống kê chi tiết 7 ngày\n'
        '`/card [member]` — Tạo ảnh profile card\n'
        '`/badges [member]` — Xem huy hiệu\n\n'
        '**💰 Economy**\n'
        '`!wallet [@member]` hoặc `!balance [@member]` — Xem balance, total earned, debt\n'
        '`/daily` — Nhận ngẫu nhiên 1,000-5,000 coins mỗi 24h\n'
        '`/pay <member> <amount>` — Chuyển coins ảo\n'
        '`/economy leaderboard` — Top total earned\n'
        '`/transactions [limit]` — Lịch sử giao dịch\n'
        '`/loan borrow|repay|status|offer|accept|cancel|history` — Vay/cho vay coins ảo\n\n'
        '**🎰 Game**\n'
        'Game chỉ hiện trong `!help` của kênh đã được admin gán bằng `/admin game_channels add`.\n\n'
        '**🏆 Xếp hạng**\n'
        '`/leaderboard` — Top hôm nay\n'
        '`/top_alltime` — Top tổng thời gian\n'
        '`/studying` — Ai đang học ngay lúc này\n\n'
        '**🎮 Gamification**\n'
        '`/quest` — Nhiệm vụ hôm nay\n'
        '`/tasks ideas|preset|add|list|done` — Task học tập nhận thêm coins\n'
        '`/setgoal <mô tả> [hours] [minutes]` — Đặt mục tiêu\n\n'
        '**⏰ Tiện ích**\n'
        '`/ask <câu hỏi>` — Hỏi AI đa năng\n'
        '`/roles` — Xem vai trò theo money class\n\n'
        '**📅 Báo cáo tuần**\n'
        '`/weekly preview` — Xem trước báo cáo tuần\n'
        '`/weekly on/off` — Bật/tắt báo cáo tuần\n'
        '`/weekly status` — Trạng thái báo cáo\n'
        '`/weekly leaderboard` — Top học nhiều nhất tuần này\n'
        '`/weekly compare` — So sánh tuần này vs tuần trước\n\n'
        '**⚙️ Admin**\n'
        '`/admin setup` · `/admin setup_status` · `/admin setup_welcome` · `/admin welcome_status` · `/admin setup_roles` · `/admin db_status` · `/admin backup` · `/admin reset_all_data`\n'
        '`/admin game_channels add|remove|list|clear` — Set kênh được phép chơi game theo từng game\n'
        '`/admin coins add|remove|set` · `/admin transactions`\n'
    )


def _build_help_message(guild_id: int | None = None, channel_id: int | None = None) -> str:
    if _is_configured_game_channel(guild_id, channel_id):
        return _build_game_only_help_message(guild_id, channel_id)
    return _build_full_help_message()


def _split_discord_message(message: str, limit: int = 1900) -> list[str]:
    if len(message) <= limit:
        return [message]

    chunks: list[str] = []
    current = ''
    for paragraph in message.split('\n\n'):
        candidate = paragraph if not current else f'{current}\n\n{paragraph}'
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ''
        while len(paragraph) > limit:
            split_at = paragraph.rfind('\n', 0, limit)
            if split_at <= 0:
                split_at = limit
            chunks.append(paragraph[:split_at].strip())
            paragraph = paragraph[split_at:].strip()
        current = paragraph
    if current:
        chunks.append(current)
    return chunks


async def _send_help_to_interaction(interaction: discord.Interaction) -> None:
    chunks = _split_discord_message(_build_help_message(interaction.guild_id, interaction.channel_id))
    await interaction.followup.send(chunks[0], ephemeral=True)
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, ephemeral=True)


async def _send_help_to_channel(channel: discord.abc.Messageable, guild_id: int | None, channel_id: int | None) -> None:
    for chunk in _split_discord_message(_build_help_message(guild_id, channel_id)):
        await channel.send(chunk)


@bot.tree.command(name='help', description='Danh sách tất cả lệnh của bot')
async def slash_help(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _send_help_to_interaction(interaction)


@bot.command(name='help', aliases=['command', 'commands'])
async def cmd_help(ctx: commands.Context):
    await _send_help_to_channel(ctx.channel, ctx.guild.id if ctx.guild else None, ctx.channel.id if ctx.channel else None)

# ── Admin commands ─────────────────────────────────────────────────────────

admin_group = app_commands.Group(
    name='admin',
    description='Quản trị bot',
    default_permissions=discord.Permissions(administrator=True),
)
admin_coins_group = app_commands.Group(
    name='coins',
    description='Quản trị coins',
    default_permissions=discord.Permissions(administrator=True),
)
admin_game_channels_group = app_commands.Group(
    name='game_channels',
    description='Quản trị kênh game',
    default_permissions=discord.Permissions(administrator=True),
)


async def _admin_update_coins(
    interaction: discord.Interaction,
    member: discord.Member,
    *,
    mode: str,
    amount: int,
    reason: str,
):
    if not await _require_admin(interaction, f'admin.coins.{mode}'):
        return
    await interaction.response.defer(ephemeral=True)
    if member.bot:
        await interaction.followup.send('Không thể chỉnh coins cho bot.', ephemeral=True)
        return
    amount = _as_int(amount)
    if amount < 0:
        await interaction.followup.send('Amount không được âm.', ephemeral=True)
        return

    uid = str(member.id)

    def mutator(data: dict):
        account = _ensure_account(data, uid, member.display_name)
        old_balance = _as_int(account.get('balance', 0))
        if mode == 'add':
            delta = amount
            new_balance = old_balance + amount
        elif mode == 'remove':
            delta = -amount
            new_balance = old_balance - amount
        elif mode == 'set':
            delta = amount - old_balance
            new_balance = amount
        else:
            return {'ok': False, 'error': 'Mode không hợp lệ.'}
        if new_balance < 0:
            return {
                'ok': False,
                'error': f'Balance không thể âm. Hiện có {format_coins(old_balance)}.',
            }
        account['balance'] = new_balance
        _sync_money_class(account)
        _append_transaction(
            account,
            f'admin_coins_{mode}',
            delta,
            reason[:120],
            counterparty=str(interaction.user.id),
            meta={'admin_id': interaction.user.id, 'mode': mode},
        )
        return {'ok': True, 'old_balance': old_balance, 'new_balance': new_balance, 'delta': delta}

    result, _ = update_data(mutator)
    if not result.get('ok'):
        await interaction.followup.send(f'Không thể cập nhật: {result.get("error", "unknown error")}', ephemeral=True)
        return
    await interaction.followup.send(
        (
            f'Đã `{mode}` coins cho **{member.display_name}**.\n'
            f'Delta: `{format_coins(result["delta"])}`\n'
            f'Balance: `{format_coins(result["old_balance"])}` -> `{format_coins(result["new_balance"])}`'
        ),
        ephemeral=True,
    )


@admin_coins_group.command(name='add', description='[Admin] Cộng coins cho thành viên')
@app_commands.describe(member='Thành viên', amount='Số coins cần cộng', reason='Lý do')
async def admin_coins_add(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 1_000_000_000],
    reason: str = 'Admin coins add',
):
    await _admin_update_coins(interaction, member, mode='add', amount=int(amount), reason=reason)


@admin_coins_group.command(name='remove', description='[Admin] Trừ coins của thành viên')
@app_commands.describe(member='Thành viên', amount='Số coins cần trừ', reason='Lý do')
async def admin_coins_remove(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 1_000_000_000],
    reason: str = 'Admin coins remove',
):
    await _admin_update_coins(interaction, member, mode='remove', amount=int(amount), reason=reason)


@admin_coins_group.command(name='set', description='[Admin] Set balance coins của thành viên')
@app_commands.describe(member='Thành viên', amount='Balance mới', reason='Lý do')
async def admin_coins_set(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 0, 1_000_000_000],
    reason: str = 'Admin coins set',
):
    await _admin_update_coins(interaction, member, mode='set', amount=int(amount), reason=reason)


@admin_group.command(name='transactions', description='[Admin] Xem transaction gần đây')
@app_commands.describe(member='Lọc theo thành viên', limit='Số dòng muốn xem')
async def admin_transactions(
    interaction: discord.Interaction,
    member: discord.Member | None = None,
    limit: app_commands.Range[int, 1, 25] = 10,
):
    if not await _require_admin(interaction, 'admin.transactions'):
        return
    await interaction.response.defer(ephemeral=True)
    data = load_data()
    rows = []
    if member:
        info = data.get(str(member.id), {})
        for tx in info.get('transactions', []) or []:
            item = deepcopy(tx)
            item['user_name'] = info.get('name', member.display_name)
            rows.append(item)
    else:
        for info in data.values():
            for tx in info.get('transactions', []) or []:
                item = deepcopy(tx)
                item['user_name'] = info.get('name', 'Unknown')
                rows.append(item)
    rows.sort(key=lambda tx: str(tx.get('ts', '')), reverse=True)
    rows = rows[:int(limit)]
    if not rows:
        await interaction.followup.send('Chưa có transaction nào.', ephemeral=True)
        return
    lines = ['**Transactions gần đây**']
    for tx in rows:
        lines.append(f'**{tx.get("user_name", "Unknown")}** · {_tx_line(tx)}')
    await interaction.followup.send('\n'.join(lines)[:1900], ephemeral=True)


def _parse_channel_id_list(raw: str | None) -> list[int]:
    if not raw:
        return []
    ids: list[int] = []
    for match in re.findall(r'\d{15,25}', raw):
        channel_id = _as_int(match)
        if channel_id and channel_id not in ids:
            ids.append(channel_id)
    return ids


def _format_config_channel(guild: discord.Guild, channel_id: int | None) -> str:
    if not channel_id:
        return '`chưa set`'
    channel = guild.get_channel(int(channel_id))
    return channel.mention if channel and hasattr(channel, 'mention') else f'`{channel_id}` (không tìm thấy)'


def _format_config_role(guild: discord.Guild, role_id: int | None) -> str:
    if not role_id:
        return '`chưa set`'
    role = guild.get_role(int(role_id))
    return role.mention if role else f'`{role_id}` (không tìm thấy)'


def _guild_game_channel_ids(guild_id: int) -> list[int]:
    raw_ids = config_manager.get(int(guild_id), 'game_channel_ids', []) or []
    ids: list[int] = []
    for raw in raw_ids:
        channel_id = _as_int(raw)
        if channel_id and channel_id not in ids:
            ids.append(channel_id)
    return ids


GAME_CHANNEL_ALL = 'all'
GAME_CHANNEL_CHOICES = (GAME_CHANNEL_ALL, *GAME_ORDER)


def _normal_game_key(game: str | None) -> str:
    value = str(game or GAME_CHANNEL_ALL).lower().strip()
    return value if value in GAME_CHANNEL_CHOICES else GAME_CHANNEL_ALL


def _guild_game_channel_map(guild_id: int) -> dict[int, list[str]]:
    raw_map = config_manager.get(int(guild_id), 'game_channel_map', {}) or {}
    if not isinstance(raw_map, dict):
        return {}
    result: dict[int, list[str]] = {}
    for raw_channel_id, raw_games in raw_map.items():
        channel_id = _as_int(raw_channel_id)
        if not channel_id:
            continue
        if isinstance(raw_games, str):
            games = [_normal_game_key(raw_games)]
        elif isinstance(raw_games, list):
            games = [_normal_game_key(item) for item in raw_games]
        else:
            continue
        deduped = []
        for game in games:
            if game not in deduped:
                deduped.append(game)
        if deduped:
            result[channel_id] = deduped
    return result


def _set_guild_game_channel_map(guild_id: int, mapping: dict[int, list[str]], *, updated_by: int | None = None):
    payload = {str(channel_id): games for channel_id, games in sorted(mapping.items()) if games}
    config_manager.set(int(guild_id), 'game_channel_map', payload, updated_by=updated_by)


def _channel_game_keys(guild_id: int, channel_id: int | None) -> set[str]:
    if not channel_id:
        return set()
    mapping = _guild_game_channel_map(guild_id)
    games = set(mapping.get(int(channel_id), []))
    if not games and int(channel_id) in _guild_game_channel_ids(guild_id):
        games.add(GAME_CHANNEL_ALL)
    return games


def _format_game_key(game: str) -> str:
    if game == GAME_CHANNEL_ALL:
        return 'all games'
    return GAME_LABELS.get(game, game)


def _format_game_assignments(guild: discord.Guild, mapping: dict[int, list[str]]) -> str:
    if not mapping:
        legacy_ids = _guild_game_channel_ids(guild.id)
        return _format_config_channels(guild, legacy_ids)
    lines = []
    for channel_id, games in sorted(mapping.items()):
        labels = ', '.join(_format_game_key(game) for game in games)
        lines.append(f'{_format_config_channel(guild, channel_id)}: `{labels}`')
    legacy_only = [channel_id for channel_id in _guild_game_channel_ids(guild.id) if channel_id not in mapping]
    for channel_id in legacy_only:
        lines.append(f'{_format_config_channel(guild, channel_id)}: `all games`')
    return '\n'.join(lines) if lines else '`chưa set`'


def _format_config_channels(guild: discord.Guild, channel_ids: list[int]) -> str:
    if not channel_ids:
        return '`chưa set`'
    return ', '.join(_format_config_channel(guild, channel_id) for channel_id in channel_ids)


GAME_CHANNEL_APP_CHOICES = [
    app_commands.Choice(name='all', value='all'),
    app_commands.Choice(name='blackjack', value='blackjack'),
    app_commands.Choice(name='taixiu', value='taixiu'),
    app_commands.Choice(name='slot', value='slot'),
    app_commands.Choice(name='dice', value='dice'),
    app_commands.Choice(name='hilo', value='hilo'),
    app_commands.Choice(name='casino', value='casino'),
]


@admin_game_channels_group.command(name='add', description='[Admin] Thêm/gán kênh được phép chơi game')
@app_commands.describe(channel='Text channel chuyên dùng cho casino/mini game', game='Game được phép trong channel này')
@app_commands.choices(game=GAME_CHANNEL_APP_CHOICES)
async def admin_game_channels_add(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    game: str = GAME_CHANNEL_ALL,
):
    if not await _require_admin(interaction, 'admin.game_channels.add'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    if channel.guild.id != guild.id:
        await interaction.followup.send('Channel phải thuộc server hiện tại.', ephemeral=True)
        return

    game_key = _normal_game_key(game)
    channel_ids = _guild_game_channel_ids(guild.id)
    mapping = _guild_game_channel_map(guild.id)
    if game_key == GAME_CHANNEL_ALL:
        mapping[channel.id] = [GAME_CHANNEL_ALL]
        if channel.id not in channel_ids:
            channel_ids.append(channel.id)
    else:
        channel_ids = [channel_id for channel_id in channel_ids if channel_id != channel.id]
        games = [item for item in mapping.get(channel.id, []) if item != GAME_CHANNEL_ALL]
        if game_key not in games:
            games.append(game_key)
        mapping[channel.id] = games
    config_manager.set(guild.id, 'game_channel_ids', channel_ids, updated_by=interaction.user.id)
    _set_guild_game_channel_map(guild.id, mapping, updated_by=interaction.user.id)
    await interaction.followup.send(
        f'Đã gán **{_format_game_key(game_key)}** cho {channel.mention}.\n'
        f'Danh sách hiện tại:\n{_format_game_assignments(guild, _guild_game_channel_map(guild.id))}',
        ephemeral=True,
    )


@admin_game_channels_group.command(name='remove', description='[Admin] Xóa kênh hoặc game khỏi danh sách game')
@app_commands.describe(channel='Text channel cần xóa khỏi danh sách game', game='Game cần xóa khỏi channel này')
@app_commands.choices(game=GAME_CHANNEL_APP_CHOICES)
async def admin_game_channels_remove(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    game: str = GAME_CHANNEL_ALL,
):
    if not await _require_admin(interaction, 'admin.game_channels.remove'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    if channel.guild.id != guild.id:
        await interaction.followup.send('Channel phải thuộc server hiện tại.', ephemeral=True)
        return

    game_key = _normal_game_key(game)
    channel_ids = _guild_game_channel_ids(guild.id)
    mapping = _guild_game_channel_map(guild.id)
    if game_key == GAME_CHANNEL_ALL:
        channel_ids = [channel_id for channel_id in channel_ids if channel_id != channel.id]
        mapping.pop(channel.id, None)
    else:
        if channel.id in channel_ids:
            channel_ids = [channel_id for channel_id in channel_ids if channel_id != channel.id]
            mapping[channel.id] = [item for item in GAME_CHANNEL_CHOICES if item not in {GAME_CHANNEL_ALL, game_key}]
        else:
            games = [item for item in mapping.get(channel.id, []) if item != game_key]
            if games:
                mapping[channel.id] = games
            else:
                mapping.pop(channel.id, None)
    config_manager.set(guild.id, 'game_channel_ids', channel_ids, updated_by=interaction.user.id)
    _set_guild_game_channel_map(guild.id, mapping, updated_by=interaction.user.id)
    await interaction.followup.send(
        f'Đã xóa **{_format_game_key(game_key)}** khỏi {channel.mention}.\n'
        f'Danh sách hiện tại:\n{_format_game_assignments(guild, _guild_game_channel_map(guild.id))}',
        ephemeral=True,
    )


@admin_game_channels_group.command(name='list', description='[Admin] Xem danh sách kênh game')
async def admin_game_channels_list(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.game_channels.list'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    mapping = _guild_game_channel_map(guild.id)
    await interaction.followup.send(
        f'Kênh game hiện tại:\n{_format_game_assignments(guild, mapping)}',
        ephemeral=True,
    )


@admin_game_channels_group.command(name='clear', description='[Admin] Xóa toàn bộ danh sách kênh game')
async def admin_game_channels_clear(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.game_channels.clear'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    config_manager.set(guild.id, 'game_channel_ids', [], updated_by=interaction.user.id)
    _set_guild_game_channel_map(guild.id, {}, updated_by=interaction.user.id)
    await interaction.followup.send('Đã xóa toàn bộ kênh game đã set.', ephemeral=True)


def is_bot_admin(member: discord.Member) -> bool:
    if not isinstance(member, discord.Member):
        return False
    if member.guild_permissions.administrator:
        return True

    config = repository.get_guild_config(member.guild.id)
    admin_role_id = config.get('admin_role_id') if config else None
    if admin_role_id:
        return any(role.id == int(admin_role_id) for role in member.roles)

    return False


def _admin_role_allowed(interaction: discord.Interaction) -> bool:
    return isinstance(interaction.user, discord.Member) and is_bot_admin(interaction.user)


async def _is_admin_actor(interaction_or_message) -> bool:
    user = getattr(interaction_or_message, 'user', None) or getattr(interaction_or_message, 'author', None)
    return isinstance(user, discord.Member) and is_bot_admin(user)


async def _send_interaction_denial(interaction: discord.Interaction, message: str):
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


async def _require_admin(interaction: discord.Interaction, action_name: str = 'admin') -> bool:
    if not interaction.guild:
        await _send_interaction_denial(interaction, 'Lệnh này chỉ dùng được trong server.')
        return False
    if isinstance(interaction.user, discord.Member) and is_bot_admin(interaction.user):
        return True
    await _send_interaction_denial(interaction, 'Bạn không có quyền dùng lệnh này.')
    return False


async def _require_moderator(interaction: discord.Interaction, action_name: str = 'moderation') -> bool:
    if not interaction.guild:
        await _send_interaction_denial(interaction, 'Lệnh này chỉ dùng được trong server.')
        return False
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not (member and is_bot_admin(member)):
        await _send_interaction_denial(interaction, 'Bạn không có quyền dùng lệnh này.')
        return False
    return True


def _clear_runtime_tracking_for_guild(guild_id: int) -> int:
    """Clear in-memory study runtime state for one guild after data reset."""
    member_ids = _runtime_member_ids_for_guild(int(guild_id))
    for mid in member_ids:
        cancel_task(mid)
        join_times.pop(mid, None)
        last_checkpoint.pop(mid, None)
        milestone_sent.pop(mid, None)
        runtime_member_guild_ids.pop(mid, None)
        media_active_members.discard(mid)
        cam_thanks_sent.discard(mid)
        session_counts.pop(mid, None)
    for day, mid in list(daily_first_join.items()):
        if mid in member_ids:
            daily_first_join.pop(day, None)
    save_runtime_state()
    return len(member_ids)


def _guild_setup_status_lines(guild: discord.Guild, config: dict) -> list[str]:
    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    focus_ids = [int(ch_id) for ch_id in config.get('focus_channel_ids') or [] if _as_int(ch_id)]
    lines = [
        f'Guild: **{guild.name}** (`{guild.id}`)',
        f'Create-room voice: {_format_config_channel(guild, config.get("create_room_channel_id"))}',
        f'Temp room category: {_format_config_channel(guild, config.get("temp_room_category_id"))}',
        f'Report channel: {_format_config_channel(guild, config.get("report_channel_id"))}',
        f'Admin role: {_format_config_role(guild, config.get("admin_role_id"))}',
        f'Coins/minute: `{_as_int(config.get("coins_per_minute"), COINS_PER_MINUTE)}`',
        f'Focus rooms: `{len(focus_ids)}` configured',
        f'Game channels: `{len(_guild_game_channel_ids(guild.id))}` configured',
    ]
    if bot_member:
        lines.append(f'Manage Roles: `{"yes" if bot_member.guild_permissions.manage_roles else "no"}`')
        lines.append(f'Manage Channels: `{"yes" if bot_member.guild_permissions.manage_channels else "no"}`')
        lines.append(f'Move Members: `{"yes" if bot_member.guild_permissions.move_members else "no"}`')
    role_ids = _persisted_class_role_ids(guild.id)
    lines.append(f'Class roles saved: `{len(role_ids)}/{len(CLASS_ROLE_NAMES)}`')
    unmanageable = _unmanageable_class_roles(guild, role_ids)
    if unmanageable:
        lines.append('Role hierarchy warning: ' + ', '.join(role.name for role in unmanageable[:5]))
    return lines


def _apply_legacy_env_config_if_empty(guild: discord.Guild) -> dict:
    config = get_guild_config(guild.id)
    updates = {}
    if (
        LEGACY_CREATE_ROOM_CHANNEL_ID
        and not config.get('create_room_channel_id')
        and guild.get_channel(LEGACY_CREATE_ROOM_CHANNEL_ID)
    ):
        updates['create_room_channel_id'] = LEGACY_CREATE_ROOM_CHANNEL_ID
    if (
        LEGACY_TEMP_ROOM_CATEGORY_ID
        and not config.get('temp_room_category_id')
        and guild.get_channel(LEGACY_TEMP_ROOM_CATEGORY_ID)
    ):
        updates['temp_room_category_id'] = LEGACY_TEMP_ROOM_CATEGORY_ID
    if updates:
        config = save_guild_config(guild.id, updates)
    return config


@admin_group.command(name='setup', description='Lưu cấu hình server hiện tại')
@app_commands.describe(
    create_room_channel='Voice channel người dùng join để bot tạo phòng học tạm',
    temp_room_category='Category chứa các phòng học tạm',
    report_channel='Text channel nhận báo cáo/livestream thống kê',
    admin_role='Role được phép chạy lệnh admin của bot',
    coins_per_minute='Số coins cộng mỗi phút học trong server này',
    focus_channels='Tùy chọn: ID/mention các phòng học cố định, cách nhau bằng dấu phẩy',
)
async def admin_setup(
    interaction: discord.Interaction,
    create_room_channel: discord.VoiceChannel,
    temp_room_category: discord.CategoryChannel,
    report_channel: discord.TextChannel,
    admin_role: discord.Role,
    coins_per_minute: int = None,
    focus_channels: str = None,
):
    if not await _require_admin(interaction, 'admin.setup'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return

    if create_room_channel.guild.id != guild.id or temp_room_category.guild.id != guild.id or report_channel.guild.id != guild.id:
        await interaction.followup.send('Các channel/category phải thuộc server hiện tại.', ephemeral=True)
        return
    if admin_role.guild.id != guild.id:
        await interaction.followup.send('Admin role phải thuộc server hiện tại.', ephemeral=True)
        return

    current = get_guild_config(guild.id)
    focus_ids = _parse_channel_id_list(focus_channels)
    if not focus_ids:
        focus_ids = [int(ch_id) for ch_id in current.get('focus_channel_ids') or [] if _as_int(ch_id)]
    coin_rate = _as_int(coins_per_minute, _as_int(current.get('coins_per_minute'), COINS_PER_MINUTE))
    coin_rate = max(0, coin_rate)

    config = save_guild_config(guild.id, {
        'create_room_channel_id': create_room_channel.id,
        'temp_room_category_id': temp_room_category.id,
        'report_channel_id': report_channel.id,
        'admin_role_id': admin_role.id,
        'coins_per_minute': coin_rate,
        'focus_channel_ids': focus_ids,
    })

    await interaction.followup.send(
        'Đã lưu cấu hình server.\n' + '\n'.join(_guild_setup_status_lines(guild, config)),
        ephemeral=True,
    )


@admin_group.command(name='setup_status', description='Xem cấu hình server hiện tại')
async def admin_setup_status(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.setup_status'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    config = _apply_legacy_env_config_if_empty(guild)
    await interaction.followup.send('\n'.join(_guild_setup_status_lines(guild, config)), ephemeral=True)


@admin_group.command(name='setup_welcome', description='Thiết lập kênh chào mừng thành viên mới')
@app_commands.describe(channel='Text channel nhận welcome message')
async def admin_setup_welcome(interaction: discord.Interaction, channel: discord.TextChannel):
    if not await _require_admin(interaction, 'admin.setup_welcome'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    if channel.guild.id != guild.id:
        await interaction.followup.send('Welcome channel phải thuộc server hiện tại.', ephemeral=True)
        return

    config_manager.set(guild.id, 'welcome_channel_id', channel.id, updated_by=interaction.user.id)
    await interaction.followup.send(f'Đã set welcome channel: {channel.mention}', ephemeral=True)


@admin_group.command(name='welcome_status', description='Xem kênh chào mừng hiện tại')
async def admin_welcome_status(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.welcome_status'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return

    channel_id = config_manager.get(guild.id, 'welcome_channel_id')
    await interaction.followup.send(
        f'Welcome channel: {_format_config_channel(guild, _as_int(channel_id) or None)}',
        ephemeral=True,
    )


@admin_group.command(name='setup_roles', description='Tạo và đồng bộ class roles')
async def admin_setup_roles(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.setup_roles'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('❌ Lệnh này chỉ dùng được trong server!', ephemeral=True)
        return

    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member:
        await interaction.followup.send('❌ Bot chưa đọc được thông tin member của chính nó.', ephemeral=True)
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.followup.send(
            '❌ Bot thiếu quyền **Manage Roles**. Hãy cấp quyền rồi chạy lại `/admin setup_roles`.',
            ephemeral=True,
        )
        return

    role_ids = await ensure_class_roles(guild)
    if not role_ids:
        await interaction.followup.send(
            '❌ Không tạo hoặc tìm thấy class role nào. Kiểm tra quyền role của bot rồi thử lại.',
            ephemeral=True,
        )
        return

    updated, skipped = await _sync_guild_class_members(guild, role_ids)
    unmanageable = _unmanageable_class_roles(guild, role_ids)
    msg = (
        f'✅ Đã setup **{len(role_ids)}/{len(CLASS_ROLE_NAMES)}** class roles '
        f'và sync **{updated}** thành viên.'
    )
    if skipped:
        msg += f' ⚠️ Bỏ qua **{skipped}** (không tìm thấy trong server).'
    if unmanageable:
        role_list = ', '.join(role.name for role in unmanageable[:5])
        msg += (
            '\n⚠️ Bot chưa thể gán một số role vì role bot không cao hơn: '
            f'**{role_list}**. Hãy kéo role bot lên trên các class roles.'
        )
    await interaction.followup.send(msg, ephemeral=True)


@admin_group.command(name='db_status', description='Xem trạng thái database')
async def admin_db_status(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.db_status'):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        status = repository.db_status()
    except Exception as e:
        await interaction.followup.send(f'Lỗi đọc database: `{e}`', ephemeral=True)
        return

    counts = status.get('counts', {})
    count_text = ', '.join(f'{name}={count}' for name, count in sorted(counts.items()))
    lines = [
        f'Backend: `{status.get("backend")}`',
        f'Path: `{status.get("path")}`',
        f'Exists: `{status.get("exists")}` · Size: `{status.get("size_bytes", 0):,}` bytes',
        f'Rows: {count_text or "`none`"}',
    ]
    await interaction.followup.send('\n'.join(lines), ephemeral=True)


async def admin_migrate_json_to_db(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.migrate_json_to_db'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return
    try:
        result = repository.migrate_json_to_db(
            guild.id,
            study_data_path=DATA_FILE,
            guild_config_path=BASE_DIR / 'guild_config.json',
            runtime_state_path=RUNTIME_STATE_FILE,
            role_config_path=ROLE_CONFIG_FILE,
            backup_dir=BACKUP_DIR,
            normalize_fn=_normalize_all_users,
        )
        _apply_legacy_env_config_if_empty(guild)
    except Exception as e:
        log.error(f'[DB] JSON migration failed for {guild.name}: {e}', exc_info=True)
        await interaction.followup.send(f'Migration thất bại: `{e}`', ephemeral=True)
        return

    await interaction.followup.send(
        'Migration hoàn tất.\n'
        f'Users inserted: `{result.get("inserted_users", 0)}` · skipped: `{result.get("skipped_users", 0)}`\n'
        f'Guild config: `{result.get("migrated_guild_config")}` · class roles: `{result.get("migrated_roles", 0)}` · runtime: `{result.get("migrated_runtime")}`\n'
        f'Backups: `{len(result.get("backups", []))}` file(s) trong `{BACKUP_DIR}`',
        ephemeral=True,
    )


@admin_group.command(name='backup', description='Backup SQLite database ngay lập tức')
async def admin_backup(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.backup'):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        dest = repository.backup_db(BACKUP_DIR)
    except Exception as e:
        await interaction.followup.send(f'Backup DB thất bại: `{e}`', ephemeral=True)
        return
    await interaction.followup.send(f'Đã backup DB: `{dest}`', ephemeral=True)


@admin_group.command(name='reset_all_data', description='Reset toàn bộ study/economy data của server hiện tại')
@app_commands.describe(confirm='Gõ chính xác: RESET <guild_id>')
async def admin_reset_all_data(interaction: discord.Interaction, confirm: str):
    if not await _require_admin(interaction, 'admin.reset_all_data'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('Lệnh này chỉ dùng được trong server.', ephemeral=True)
        return

    expected = f'RESET {guild.id}'
    if str(confirm or '').strip() != expected:
        await interaction.followup.send(
            f'Không reset. Để xác nhận, chạy lại với confirm chính xác: `{expected}`',
            ephemeral=True,
        )
        return

    try:
        backup_path = repository.backup_db(BACKUP_DIR)
    except Exception as e:
        log.error('[Admin] reset_all_data blocked because DB backup failed for %s: %s', guild.id, e, exc_info=True)
        await interaction.followup.send(f'Không reset vì backup DB thất bại: `{e}`', ephemeral=True)
        return

    with guild_data_context(guild.id):
        save_data({})
    cleared_runtime = _clear_runtime_tracking_for_guild(guild.id)
    with contextlib.suppress(Exception):
        await update_live_message(guild)

    log.warning(
        '[Admin] %s (%s) reset all study/economy data for guild %s after backup %s',
        interaction.user, interaction.user.id, guild.id, backup_path,
    )
    await interaction.followup.send(
        f'Đã reset study/economy data cho **{guild.name}**.\n'
        f'Backup trước reset: `{backup_path}`\n'
        f'Runtime sessions cleared: `{cleared_runtime}`\n'
        'Server config and class-role setup were kept.',
        ephemeral=True,
    )


admin_group.add_command(admin_coins_group)
admin_group.add_command(admin_game_channels_group)
bot.tree.add_command(admin_group)


async def slash_syncroles(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.syncroles'):
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('❌ Lệnh này chỉ dùng được trong server!', ephemeral=True)
        return
    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member:
        await interaction.followup.send('❌ Bot chưa đọc được thông tin member của chính nó.', ephemeral=True)
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.followup.send(
            '❌ Bot thiếu quyền **Manage Roles**. Hãy cấp quyền rồi chạy lại `/syncroles`.',
            ephemeral=True,
        )
        return
    role_ids = await ensure_class_roles(guild)
    if not role_ids:
        await interaction.followup.send(
            '❌ Không setup được class roles. Kiểm tra role bot rồi thử lại.',
            ephemeral=True,
        )
        return
    updated, skipped = await _sync_guild_class_members(guild, role_ids)

    msg = f'✅ Đã sync **{updated}** thành viên.'
    if skipped:
        msg += f' ⚠️ Bỏ qua **{skipped}** (không tìm thấy trong server).'
    unmanageable = _unmanageable_class_roles(guild, role_ids)
    if unmanageable:
        role_list = ', '.join(role.name for role in unmanageable[:5])
        msg += f'\n⚠️ Role bot cần nằm cao hơn: **{role_list}**.'
    await interaction.followup.send(msg, ephemeral=True)

async def slash_report(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.report'):
        return
    await interaction.response.defer(ephemeral=True)
    await _send_report(interaction.guild)
    await interaction.followup.send('✅ Đã gửi báo cáo!', ephemeral=True)

async def slash_dailyboard(interaction: discord.Interaction, date: str = None):
    if not await _require_admin(interaction, 'admin.dailyboard'):
        return
    if date is not None:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            await interaction.response.send_message(
                '❌ Định dạng ngày không hợp lệ! Dùng `YYYY-MM-DD`, ví dụ: `2025-01-31`',
                ephemeral=True
            )
            return
    await interaction.response.defer(ephemeral=True)
    await _send_daily_board(date, interaction.guild)
    await interaction.followup.send('✅ Đã gửi bảng tổng kết ngày!', ephemeral=True)

async def slash_updatelive(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.updatelive'):
        return
    await interaction.response.defer(ephemeral=True)
    if interaction.guild:
        await update_live_message(interaction.guild)
    else:
        await update_all_live_messages()
    await interaction.followup.send('✅ Đã cập nhật!', ephemeral=True)

async def slash_backup(interaction: discord.Interaction):
    if not await _require_admin(interaction, 'admin.backup'):
        return
    await interaction.response.defer(ephemeral=True)
    backup_data()
    backups = sorted(os.listdir(BACKUP_DIR)) if os.path.exists(BACKUP_DIR) else []
    if backups:
        msg = (
            f'✅ Đã backup! Tổng: **{len(backups)}** bản trong `/{BACKUP_DIR}/`\n'
            f'Mới nhất: `{backups[-1]}`'
        )
    else:
        msg = '✅ Backup xong.'
    await interaction.followup.send(msg, ephemeral=True)

# ─── PREFIX COMMANDS ─────────────────────────────────────────────────────────

@bot.command(name='stats')
async def cmd_stats(ctx, member: discord.Member = None):
    target = member or ctx.author
    data   = load_data()
    uid    = str(target.id)
    if uid not in data:
        await ctx.send(f'❌ **{target.display_name}** chưa có dữ liệu!'); return
    info       = data[uid]
    today      = datetime.now().strftime('%Y-%m-%d')
    saved      = info.get('daily', {}).get(today, 0)
    saved += _get_unsaved_study_seconds(target.id)
    recent = sorted(info.get('daily', {}).items(), reverse=True)[:7]
    class_idx = info.get('class', info.get('level', 0))
    debt = _active_debt(info)
    await ctx.send(
        f'📊 **{target.display_name}** — `{class_label(class_idx)}` 💰{format_coins(info.get("total_earned",0))}\n'
        f'💵 Balance: `{format_coins(info.get("balance",0))}` | Debt: `{format_coins(debt)}`\n'
        f'🔥 Streak: `{info.get("streak",0)} ngày` | 🕐 Hôm nay: `{format_time(saved)}`\n'
        f'📚 Tổng: `{format_time(info.get("total",0))}` | 🏅 Badge: `{len(info.get("badges",[]))}`\n'
        f'📅 7 ngày: ' + ' '.join([f'`{d[5:]}` {format_time(s)}' for d, s in recent])
    )

@bot.command(name='leaderboard', aliases=['lb', 'top'])
async def cmd_leaderboard(ctx):
    data  = _get_live_enriched_data(ctx.guild)
    today = datetime.now().strftime('%Y-%m-%d')
    now   = datetime.now()

    def real_time(uid_str: str, info: dict) -> int:
        s = info.get('daily', {}).get(today, 0)
        try:
            mid = int(uid_str)
        except (ValueError, TypeError):
            return s
        return s + _get_unsaved_study_seconds(mid, now)

    entries = [(u, i, real_time(u, i)) for u, i in data.items()]
    top10   = sorted(
        [e for e in entries if e[2] > 0],
        key=lambda x: x[2], reverse=True
    )[:10]
    lines = ['🏆 **Bảng xếp hạng hôm nay**\n']
    if not top10: lines.append('😴 Chưa có ai!')
    else:
        for i, (uid, info, rt) in enumerate(top10, 1):
            medal   = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
            try:
                is_live = int(uid) in join_times
            except (ValueError, TypeError):
                is_live = False
            class_idx = info.get('class', info.get('level', 0))
            earned = info.get('daily_earnings', {}).get(today, 0)
            debt = _active_debt(info)
            lines.append(f'{medal}{" 🟢" if is_live else ""} **{info["name"]}** `{class_label(class_idx)}`'
                         f' 🔥{info.get("streak",0)} — `{format_time(rt)}` · Today {format_coins(earned)}'
                         f' · Balance {format_coins(info.get("balance",0))} · Debt {format_coins(debt)}')
    await ctx.send('\n'.join(lines))

@bot.command(name='quest')
async def cmd_quest(ctx):
    uid   = str(ctx.author.id)
    today = datetime.now().strftime('%Y-%m-%d')
    data  = load_data()
    if uid not in data:
        await ctx.send('❌ Chưa có dữ liệu! Vào phòng học trước.'); return
    mid        = ctx.author.id
    saved_secs = data[uid]['daily'].get(today, 0)
    real_secs = saved_secs + _get_unsaved_study_seconds(mid)
    generate_daily_quests(uid, today)
    update_quest_progress(uid, today, override_today_secs=real_secs)
    data   = load_data()
    quests = data[uid].get('daily_quests', {}).get(today, [])
    lines  = [f'📋 **Quest hôm nay** _{today}_\n']
    for q in quests:
        info = get_quest_info(q['id'])
        if not info: continue
        pct    = min(100, int(q['progress'] / max(1, info['target']) * 100))
        bar    = '█' * (pct // 10) + '░' * (10 - pct // 10)
        status = '✅' if q.get('done') else '🔲'
        reward = info.get('coins', info.get('xp', 0))
        lines.append(
            f'{status} {info["emoji"]} **{info["desc"]}** — '
            f'`{bar}` {q["progress"]}/{info["target"]} (+{format_coins(reward)})'
        )
    await ctx.send('\n'.join(lines))

@bot.command(name='badges')
async def cmd_badges(ctx, member: discord.Member = None):
    target = member or ctx.author
    uid    = str(target.id)
    data   = load_data()
    if uid not in data:
        await ctx.send('❌ Chưa có dữ liệu!'); return
    earned = data[uid].get('badges', [])
    await ctx.send(
        f'🏅 **Huy hiệu của {target.display_name}** ({len(earned)}/{len(BADGES)})\n'
        + format_badges(earned) + '\n_Dùng `/badges` để xem chi tiết_'
    )

@bot.command(name='rank')
async def cmd_rank(ctx, member: discord.Member = None):
    target = member or ctx.author
    await ctx.send(_build_rank_message(target, load_data()))

@bot.command(name='sync')
@commands.has_permissions(administrator=True)
async def cmd_sync(ctx):
    msg = await ctx.send('⏳ Syncing...')
    total = 0
    for guild in bot.guilds:
        synced_count = await _sync_app_commands_for_guild(guild, reason='manual')
        if synced_count is not None:
            total += synced_count
    await msg.edit(content=f'✅ Sync xong! **{total}** lệnh.')

@bot.command(name='report')
@commands.has_permissions(administrator=True)
async def cmd_report(ctx):
    await _send_report(ctx.guild)
    await ctx.send('✅ Đã gửi báo cáo!')

# ─── EVENTS ──────────────────────────────────────────────────────────────────

def extract_question_from_mention(message: discord.Message) -> str:
    if not bot.user:
        return ''

    raw = message.content
    for mention in (f'<@{bot.user.id}>', f'<@!{bot.user.id}>'):
        raw = raw.replace(mention, '')
    return raw.strip()


def _install_study_context():
    """Expose stable helper APIs to plugins without importing bot.py.

    The feature migration is intentionally gradual. Plugins use this context as
    a narrow compatibility boundary while large legacy commands are moved out of
    this file over time.
    """
    bot.study_context = SimpleNamespace(
        database=database,
        repository=repository,
        config_manager=config_manager,
        is_admin_actor=_is_admin_actor,
        is_bot_admin=is_bot_admin,
        require_admin=_require_admin,
        require_moderator=_require_moderator,
        ask_ai=_ask_ai,
        load_data=load_data,
        save_data=save_data,
        update_data=update_data,
        guild_data_context=guild_data_context,
        add_study_time=add_study_time,
        add_xp_direct=add_xp_direct,
        safe_send_dm=safe_send_dm,
        format_time=format_time,
        sync_member_progress=_sync_member_progress,
        badges=BADGES,
        class_thresholds=CLASS_THRESHOLDS,
        class_names=CLASS_NAMES,
    )


async def _ensure_core_cogs_loaded():
    global _core_cogs_ready
    if _core_cogs_ready:
        return
    for command_name in ('bot', 'config'):
        bot.tree.remove_command(command_name)
    _core_cogs_ready = True


async def _load_startup_extensions():
    for extension in STARTUP_EXTENSIONS:
        if extension in bot.extensions:
            continue
        try:
            await bot.load_extension(extension)
            log.info('[Startup] Extension %s loaded.', extension)
        except commands.ExtensionAlreadyLoaded:
            log.info('[Startup] Extension %s already loaded.', extension)
        except Exception as e:
            log.error('[Startup] Extension %s failed: %s', extension, e, exc_info=True)


def _restore_guild_commands(guild: discord.Guild, commands: list):
    bot.tree.clear_commands(guild=guild)
    for command in commands:
        bot.tree.add_command(command, guild=guild, override=True)


def _prepare_guild_commands_for_sync(guild: discord.Guild, *, reason: str) -> bool:
    previous_commands = list(bot.tree.get_commands(guild=guild))
    try:
        bot.tree.clear_commands(guild=guild)
        bot.tree.copy_global_to(guild=guild)
    except Exception as e:
        log.error('[CommandSync] Could not prepare commands for %s (%s): %s', guild.name, reason, e, exc_info=True)
        try:
            _restore_guild_commands(guild, previous_commands)
        except Exception:
            log.error('[CommandSync] Could not restore previous commands for %s.', guild.name, exc_info=True)
        return False

    if not bot.tree.get_commands(guild=guild):
        log.error('[CommandSync] Refusing to sync zero commands for %s (%s).', guild.name, reason)
        try:
            _restore_guild_commands(guild, previous_commands)
        except Exception:
            log.error('[CommandSync] Could not restore previous commands for %s.', guild.name, exc_info=True)
        return False

    return True


async def _sync_app_commands_for_guild(guild: discord.Guild, *, reason: str) -> int | None:
    if not _prepare_guild_commands_for_sync(guild, reason=reason):
        return None
    try:
        synced = await bot.tree.sync(guild=guild)
        log.info('[CommandSync] Synced %s commands for %s (%s).', len(synced), guild.name, reason)
        return len(synced)
    except Exception as e:
        log.error('[CommandSync] Failed for %s: %s', guild.name, e, exc_info=True)
        return None


async def _sync_app_commands(*, reason: str = 'startup'):
    for guild in bot.guilds:
        await _sync_app_commands_for_guild(guild, reason=reason)


GAME_TEXT_COMMANDS = {'blackjack', 'xidach', 'taixiu', 'slot', 'dice', 'hilo', 'daily', 'casino'}
WALLET_TEXT_COMMANDS = {'wallet', 'balance', 'bal'}
HELP_TEXT_COMMANDS = {'help', 'command', 'commands'}


def _strip_text_command_prefix(message: discord.Message) -> tuple[str, bool]:
    content = str(message.content or '').strip()
    if not content:
        return '', False
    prefixes: list[str] = ['!']
    if getattr(message, 'guild', None):
        with contextlib.suppress(Exception):
            configured = str(get_guild_config(message.guild.id).get('command_prefix') or '!')
            if configured and configured not in prefixes:
                prefixes.insert(0, configured)
    for prefix in sorted(prefixes, key=len, reverse=True):
        if content.startswith(prefix):
            return content[len(prefix):].strip(), True
    return content, False


async def _dispatch_text_command_fallback(message: discord.Message) -> bool:
    if message.author.bot or message.guild is None:
        return False
    content, had_prefix = _strip_text_command_prefix(message)
    if not content:
        return False
    parts = content.split(maxsplit=1)
    command = parts[0].lower()
    known_text_commands = HELP_TEXT_COMMANDS | WALLET_TEXT_COMMANDS | GAME_TEXT_COMMANDS
    if not had_prefix and command not in known_text_commands:
        return False
    if command in HELP_TEXT_COMMANDS:
        await _send_help_to_channel(message.channel, message.guild.id, message.channel.id)
        return True
    if command in WALLET_TEXT_COMMANDS:
        await _send_wallet_text_command(message, parts[1] if len(parts) > 1 else None)
        return True
    if command not in GAME_TEXT_COMMANDS:
        return False
    casino_cog = bot.get_cog('CasinoCog')
    if not casino_cog:
        await message.channel.send('Game chưa sẵn sàng. Thử lại sau vài giây hoặc báo admin kiểm tra plugin casino.')
        return True
    arg = parts[1] if len(parts) > 1 else None
    if command in {'blackjack', 'xidach'}:
        await casino_cog.start_blackjack_message(message, arg)
    elif command == 'taixiu':
        await casino_cog.start_taixiu_message(message)
    elif command == 'slot':
        await casino_cog.start_slot_message(message, arg)
    elif command == 'dice':
        await casino_cog.start_dice_duel_message(message, arg)
    elif command == 'hilo':
        await casino_cog.start_hilo_message(message, arg)
    elif command == 'daily':
        await casino_cog.start_daily_message(message)
    elif command == 'casino':
        subparts = str(arg or '').split(maxsplit=1)
        subcommand = subparts[0].lower() if subparts else ''
        subarg = subparts[1] if len(subparts) > 1 else None
        if subcommand == 'bet':
            await casino_cog.start_casino_bet_message(message, subarg)
        elif subcommand in {'leaderboard', 'lb', 'top'}:
            await casino_cog.start_casino_leaderboard_message(message)
        else:
            await message.channel.send('Dùng `!casino bet <amount>` hoặc `!casino leaderboard`.')
    return True


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    ctx = await bot.get_context(message)
    if ctx.valid:
        await bot.invoke(ctx)
        return
    await _dispatch_text_command_fallback(message)

@bot.event
async def on_guild_join(guild: discord.Guild):
    _capture_guild_context(guild.id)
    initialize_database()
    _apply_legacy_env_config_if_empty(guild)
    await _sync_app_commands_for_guild(guild, reason='guild_join')
    try:
        await ensure_class_roles(guild)
    except Exception as e:
        log.error('[RoleSetup] Guild join setup failed in %s: %s', guild.name, e, exc_info=True)

@bot.event
async def on_ready():
    log.info(f'✅ Bot {bot.user.name} ready!')
    initialize_database()
    _install_study_context()
    await _ensure_core_cogs_loaded()
    for guild in bot.guilds:
        _apply_legacy_env_config_if_empty(guild)
    if not join_times:
        restore_runtime_state()

    global _room_panel_view_registered, _startup_extensions_ready
    if not _startup_extensions_ready:
        if not _room_panel_view_registered:
            bot.add_view(RoomPanelView())
            _room_panel_view_registered = True

        await _load_startup_extensions()
        _startup_extensions_ready = True
    else:
        log.info('[Startup] Extension setup already completed; skipping.')

    await _sync_app_commands(reason='startup')

    guild_class_role_ids: dict[int, dict[int, int]] = {}
    for guild in bot.guilds:
        try:
            guild_class_role_ids[guild.id] = await ensure_class_roles(guild)
        except Exception as e:
            log.error(f'[RoleSetup] Startup setup failed in {guild.name}: {e}', exc_info=True)

    if not scheduled_tasks.is_running():  scheduled_tasks.start()
    if not checkpoint_task.is_running():  checkpoint_task.start()

    global _dashboard_started
    if not _dashboard_started:
        _dashboard_started = True
        threading.Thread(target=run_dashboard, daemon=True).start()
    log.info(f'🌐 Dashboard: http://localhost:{DASHBOARD_PORT}')

    # Sync roles on recovery.
    members_to_sync: list[tuple[discord.Member, int]] = []
    for guild in bot.guilds:
        with guild_data_context(guild.id):
            data = load_data()
        for uid, info in data.items():
            try:
                mid = int(uid)
            except (ValueError, TypeError):
                continue

            member = guild.get_member(mid)
            if not member:
                continue

            # Collect members for batched role sync
            level = info.get('class', info.get('level', 0))
            members_to_sync.append((member, level))

    async def _safe_sync(member: discord.Member, level: int):
        try:
            await _ensure_role_synced(
                member,
                level,
                role_ids=guild_class_role_ids.get(member.guild.id),
            )
        except Exception as e:
            log.error(f'[RoleSync] on_ready sync failed for {member.display_name}: {e}')

    batches = list(_iter_chunks(members_to_sync, ROLE_SYNC_BATCH_SIZE))
    for idx, batch in enumerate(batches):
        await asyncio.gather(
            *[_safe_sync(member, level) for member, level in batch],
            return_exceptions=True,
        )
        if idx < len(batches) - 1:
            await asyncio.sleep(ROLE_SYNC_BATCH_DELAY)
    if batches:
        log.info(f'[RoleSync] on_ready: synced {len(members_to_sync)} members in {len(batches)} batches.')

    # Reconcile runtime state with current voice members on reconnect/restart.
    current_focus_members: dict[int, discord.Member] = {}
    for guild in bot.guilds:
        for ch_id in _guild_focus_channel_ids(guild):
            ch = bot.get_channel(ch_id)
            if not isinstance(ch, discord.VoiceChannel):
                continue
            for m in ch.members:
                if m.bot:
                    continue
                current_focus_members[m.id] = m

    for mid in list(join_times.keys()):
        m = current_focus_members.get(mid)
        if not m:
            join_times.pop(mid, None)
            last_checkpoint.pop(mid, None)
            milestone_sent.pop(mid, None)
            runtime_member_guild_ids.pop(mid, None)
            media_active_members.discard(mid)
            continue
        runtime_member_guild_ids[mid] = m.guild.id
        if m.voice and is_media_active(m.voice):
            media_active_members.add(mid)
        else:
            media_active_members.discard(mid)
            start_check(m, 'bot restart – no cam/stream')

    for mid, m in current_focus_members.items():
        if mid in join_times:
            continue
        record_join(m)
        if not (m.voice and is_media_active(m.voice)):
            start_check(m, 'bot restart – no cam/stream')

    for meta in list(temp_rooms.values()):
        room_id = _as_int(meta.get('room_id'))
        channel = bot.get_channel(room_id)
        if not isinstance(channel, discord.VoiceChannel):
            _remove_temporary_room_tracking(room_id)
            continue
        non_bot_members = [m for m in channel.members if not m.bot]
        if not non_bot_members:
            _schedule_temporary_room_cleanup(channel)

    _update_live_cache()
    save_runtime_state()

@bot.event
async def on_disconnect():
    await _flush_active_sessions('disconnect')


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    _capture_guild_context(guild.id)
    channel_id = config_manager.get(guild.id, 'welcome_channel_id')
    if not channel_id:
        log.info('[Welcome] No welcome_channel_id configured for guild %s; skipping member %s', guild.id, member.id)
        return

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        log.warning('[Welcome] Configured welcome channel %s not found in guild %s', channel_id, guild.id)
        return

    embed = discord.Embed(
        title='Chào mừng',
        description=(
            f'{member.mention}, chào mừng bạn đến với server. '
            'Chúc bạn học tập hiệu quả và tiến bộ mỗi ngày.'
        ),
        color=NOTIFY_GREEN,
    )
    embed.set_footer(text='One percent better every day')

    try:
        await channel.send(
            embed=embed,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
    except discord.Forbidden:
        log.warning('[Welcome] Missing permission to send welcome message in channel %s for guild %s', channel.id, guild.id)
    except discord.HTTPException:
        log.warning('[Welcome] Failed to send welcome message in channel %s for guild %s', channel.id, guild.id, exc_info=True)


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    _capture_guild_context(member.guild.id)
    joined_create_room = (
        _is_create_room_channel(after.channel)
        and (not before.channel or before.channel.id != after.channel.id)
    )
    joined_focus    = after.channel  and is_focus_channel(after.channel.id)
    left_focus      = before.channel and is_focus_channel(before.channel.id)
    stayed_in_focus = joined_focus and left_focus
    moved_channels  = before.channel and after.channel and before.channel.id != after.channel.id
    left_temporary  = before.channel and _is_temporary_room_id(before.channel.id)

    if joined_create_room:
        if left_focus:
            await _handle_focus_leave(member, before.channel)
            if _is_temporary_room_id(before.channel.id):
                _schedule_temporary_room_cleanup(before.channel)
        await _handle_create_room_join(member, after.channel)
        return

    if stayed_in_focus:
        if moved_channels and _is_temporary_room_id(before.channel.id):
            _schedule_temporary_room_cleanup(before.channel)
        was_active = is_media_active(before)
        now_active = is_media_active(after)
        if not was_active and now_active:
            cancel_task(member.id)
            runtime_member_guild_ids[member.id] = member.guild.id
            media_active_members.add(member.id)
            if member.id not in join_times:
                record_join(member)
            else:
                last_checkpoint[member.id] = datetime.now()
                save_runtime_state()
            log.info(f'{member.display_name} bật Cam/Stream → bắt đầu tính giờ từ bây giờ')
            await notify_cam_started(member, after.channel)
        elif was_active and not now_active:
            elapsed, result = await _do_checkpoint(member)
            if result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
            await _handle_progress_notifications(member, result, after.channel)
            await _check_quests_and_badges(member, after.channel)
            media_active_members.discard(member.id)
            save_runtime_state()
            start_check(member, 'tắt Cam/Stream')
            await send_voice_notice(
                channel=after.channel,
                member=member,
                title='Cảnh báo',
                description=(
                    f'Bạn cần bật lại Cam hoặc Stream trong {WAIT_SECONDS}s '
                    'để tiếp tục ở lại phòng.'
                ),
                color=NOTIFY_GOLD,
            )

    elif joined_focus and not stayed_in_focus:
        record_join(member)
        await send_voice_notice(
            channel=after.channel,
            member=member,
            title='Chào mừng',
            description=(
                f'Chào mừng {member.display_name} vào phòng học.'
            ),
            color=NOTIFY_BLUE,
        )
        if not is_media_active(after):
            await send_voice_notice(
                channel=after.channel,
                member=member,
                title='Nhắc nhở',
                description=(
                    f'Bạn cần bật Cam hoặc Stream trong {WAIT_SECONDS}s '
                    'để bắt đầu tính giờ và ở lại phòng.'
                ),
                color=NOTIFY_GOLD,
            )
            start_check(member, 'vào phòng không có Cam/Stream')
        else:
            await notify_cam_started(member, after.channel)
        await _update_live_message_for_channel(after.channel)

    elif left_focus and not stayed_in_focus:
        reset_cam_notification(member.id)
        await _handle_focus_leave(member, before.channel)
        if _is_temporary_room_id(before.channel.id):
            _schedule_temporary_room_cleanup(before.channel)

    if (
        left_temporary
        and not left_focus
        and (not after.channel or after.channel.id != before.channel.id)
    ):
        _schedule_temporary_room_cleanup(before.channel)

# ─── FLASK DASHBOARD ─────────────────────────────────────────────────────────

DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>📚 Study Bot Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body{background:#0f172a;color:#e2e8f0;font-family:"Segoe UI",sans-serif}
    .card{background:#1e293b;border:1px solid #334155}
    .coin-bar{background:linear-gradient(90deg,#6366f1,#8b5cf6)}
    .live-dot{width:8px;height:8px;border-radius:50%;background:#22c55e;animation:blink 1.2s infinite;display:inline-block}
    @keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
    .heatmap-cell{display:inline-block;width:13px;height:13px;border-radius:3px;margin:1.5px;cursor:pointer;transition:opacity .15s}
    .heatmap-cell:hover{opacity:.75;outline:1px solid #94a3b8}
    .tooltip{position:fixed;background:#1e293b;border:1px solid #334155;padding:6px 10px;border-radius:8px;font-size:12px;pointer-events:none;display:none;z-index:99}
    .badge-pill{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;background:#1e3a5f;border-radius:12px;font-size:11px;margin:2px}
  </style>
</head>
<body class="min-h-screen p-4 md:p-6">
<div class="max-w-6xl mx-auto">
  <div class="text-center mb-8">
    <h1 class="text-4xl font-bold text-indigo-400">📚 Study Bot Dashboard</h1>
    <p class="text-gray-400 text-sm mt-1" id="lastUpdate">Đang tải...</p>
  </div>
  <div id="summaryCards" class="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6"></div>
  <div class="card rounded-2xl p-6 mb-6">
    <div class="flex items-center gap-2 mb-3">
      <span class="live-dot"></span>
      <h2 class="text-xl font-semibold text-green-400">Đang học ngay lúc này</h2>
    </div>
    <div id="liveStudying" class="text-gray-500">Đang tải...</div>
  </div>
  <div class="card rounded-2xl p-6 mb-6">
    <h2 class="text-xl font-semibold text-indigo-300 mb-1">🌡️ Hoạt động 365 ngày qua</h2>
    <p class="text-gray-500 text-xs mb-4">Kiểu GitHub – màu càng đậm = học càng nhiều</p>
    <div class="flex items-start gap-3 overflow-x-auto pb-2">
      <div id="heatmapMonths" style="height:14px"></div>
      <div>
        <div class="flex gap-0 mb-1">
          <div id="heatmapDayLabels" class="grid text-xs text-gray-500 mr-1" style="grid-template-rows:repeat(7,16px);width:20px"></div>
          <div id="heatmap" class="flex gap-0"></div>
        </div>
        <div class="flex items-center gap-2 mt-2 text-xs text-gray-500 justify-end">
          <span>Ít</span>
          <span class="heatmap-cell" style="background:#1e293b;width:10px;height:10px"></span>
          <span class="heatmap-cell" style="background:#1e3a5f;width:10px;height:10px"></span>
          <span class="heatmap-cell" style="background:#2563eb;width:10px;height:10px"></span>
          <span class="heatmap-cell" style="background:#6366f1;width:10px;height:10px"></span>
          <span class="heatmap-cell" style="background:#8b5cf6;width:10px;height:10px"></span>
          <span>Nhiều</span>
        </div>
      </div>
    </div>
  </div>
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
    <div class="card rounded-2xl p-6">
      <h2 class="text-xl font-semibold text-indigo-300 mb-4">🏆 Xếp hạng hôm nay</h2>
      <div id="leaderboard" class="text-gray-500">Đang tải...</div>
    </div>
    <div class="card rounded-2xl p-6">
      <h2 class="text-xl font-semibold text-indigo-300 mb-4">🏅 Huy hiệu nổi bật</h2>
      <div id="badgesPanel" class="text-gray-500">Đang tải...</div>
    </div>
  </div>
  <div class="card rounded-2xl p-6">
    <h2 class="text-xl font-semibold text-indigo-300 mb-4">📈 Tổng giờ 7 ngày qua</h2>
    <canvas id="weekChart" height="90"></canvas>
  </div>
</div>
<div class="tooltip" id="tooltip"></div>
<script>
let chartInst=null;
const fmtTime=s=>{if(!s||s<=0)return'0m';const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h>0?`${h}h ${m}m`:`${m}m`;};
const getToday=()=>new Date().toISOString().split('T')[0];
const getPastDays=n=>Array.from({length:n},(_,i)=>{const d=new Date();d.setDate(d.getDate()-(n-1-i));return d.toISOString().split('T')[0];});
const THRES=[0,100,500,1500,5000,15000,50000,100000,250000,500000,1000000];
const getClass=coins=>{for(let i=THRES.length-1;i>=0;i--)if(coins>=THRES[i])return i;return 0;};
const coinPct=coins=>{const l=getClass(coins);if(l>=THRES.length-1)return 100;return Math.round(((coins-THRES[l])/(THRES[l+1]-THRES[l]))*100);};
const tooltip=document.getElementById('tooltip');
function showTip(e,html){tooltip.innerHTML=html;tooltip.style.display='block';}
document.addEventListener('mousemove',e=>{tooltip.style.left=(e.clientX+12)+'px';tooltip.style.top=(e.clientY-8)+'px';});
document.addEventListener('mouseleave',()=>{tooltip.style.display='none';});
const heatColor=m=>{if(m<=0)return'#1e293b';if(m<30)return'#1e3a5f';if(m<60)return'#2563eb';if(m<180)return'#6366f1';return'#8b5cf6';};
function buildHeatmap(data){
  const days365=getPastDays(365);const dayMap={};
  for(const info of Object.values(data))for(const[d,s]of Object.entries(info.daily||{}))dayMap[d]=(dayMap[d]||0)+Math.round(s/60);
  const el=document.getElementById('heatmap');const monthsEl=document.getElementById('heatmapMonths');const dlEl=document.getElementById('heatmapDayLabels');
  el.innerHTML='';monthsEl.innerHTML='';dlEl.innerHTML='';
  ['','Mon','','Wed','','Fri',''].forEach(l=>{const d=document.createElement('div');d.style='line-height:16px;font-size:10px;';d.textContent=l;dlEl.appendChild(d);});
  const weeks=[];let week=[];const fd=new Date(days365[0]).getDay();
  for(let p=0;p<fd;p++)week.push(null);
  for(const d of days365){week.push(d);if(week.length===7){weeks.push(week);week=[];}}
  if(week.length>0){while(week.length<7)week.push(null);weeks.push(week);}
  const MN=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  let lm=-1;const mp=[];
  weeks.forEach((w,wi)=>{const fv=w.find(d=>d!==null);if(fv){const m=new Date(fv).getMonth();if(m!==lm){mp.push({wi,label:MN[m]});lm=m;}}});
  const mc=document.createElement('div');mc.style='display:flex;gap:0;height:14px';
  weeks.forEach((_,wi)=>{const s=document.createElement('span');s.style='width:16px;display:inline-block;font-size:10px;color:#64748b';const p=mp.find(x=>x.wi===wi);s.textContent=p?p.label:'';mc.appendChild(s);});
  monthsEl.appendChild(mc);
  weeks.forEach(w=>{const col=document.createElement('div');col.style='display:flex;flex-direction:column';
    w.forEach(d=>{const cell=document.createElement('span');cell.className='heatmap-cell';
      if(!d){cell.style.background='transparent';cell.style.border='none';}
      else{const mins=dayMap[d]||0;cell.style.background=heatColor(mins);
        cell.addEventListener('mouseenter',e=>{showTip(e,`<b>${d}</b><br>${fmtTime(mins*60)} học`);});
        cell.addEventListener('mouseleave',()=>tooltip.style.display='none');}
      col.appendChild(cell);});el.appendChild(col);});
}
function buildBadges(data){
  const el=document.getElementById('badgesPanel');const freq={};
  for(const info of Object.values(data))for(const b of(info.badges||[]))freq[b]=(freq[b]||0)+1;
  const sorted=Object.entries(freq).sort((a,b)=>b[1]-a[1]).slice(0,12);
  if(!sorted.length){el.innerHTML='<p class="text-gray-500">Chưa có huy hiệu nào.</p>';return;}
  el.innerHTML=sorted.map(([bid,cnt])=>`<span class="badge-pill text-indigo-300">🏅 ${bid} <span class="text-gray-500">×${cnt}</span></span>`).join('');
}
async function loadData(){
  const res=await fetch('/api/stats');const data=await res.json();
  const today=getToday();const users=Object.values(data);
  const totalSecs=users.reduce((s,u)=>s+(u.daily[today]||0),0);
  const activeCount=users.filter(u=>(u.daily[today]||0)>0).length;
  const topStreak=users.reduce((m,u)=>Math.max(m,u.streak||0),0);
  const totalBadges=users.reduce((s,u)=>s+(u.badges||[]).length,0);
  document.getElementById('summaryCards').innerHTML=`
    <div class="card rounded-2xl p-5 text-center"><div class="text-3xl font-bold text-indigo-400">${fmtTime(totalSecs)}</div><div class="text-gray-400 text-xs mt-1">Tổng hôm nay</div></div>
    <div class="card rounded-2xl p-5 text-center"><div class="text-3xl font-bold text-green-400">${activeCount}</div><div class="text-gray-400 text-xs mt-1">Học hôm nay</div></div>
    <div class="card rounded-2xl p-5 text-center"><div class="text-3xl font-bold text-orange-400">${topStreak}🔥</div><div class="text-gray-400 text-xs mt-1">Streak cao nhất</div></div>
    <div class="card rounded-2xl p-5 text-center"><div class="text-3xl font-bold text-pink-400">${totalBadges}🏅</div><div class="text-gray-400 text-xs mt-1">Tổng huy hiệu</div></div>`;
  try{
    const liveData=await(await fetch('/api/live')).json();const liveEl=document.getElementById('liveStudying');
    if(!liveData.length){liveEl.innerHTML='<p class="text-gray-500">😴 Không có ai đang học...</p>';}
    else{liveEl.innerHTML=liveData.map(u=>`<div class="flex items-center gap-3 py-2 px-2 rounded-xl hover:bg-slate-700 transition">
      <div>${u.is_streaming&&u.is_video?'📷📺':u.is_video?'📷':u.is_streaming?'📺':'⏸️'}</div>
      <div class="flex-1"><span class="font-semibold">${u.name}</span><span class="text-xs text-gray-400 ml-2">Class ${u.level}</span></div>
      <div class="text-right"><div class="text-green-400 font-mono font-bold">${fmtTime(u.session_secs)}</div><div class="text-xs text-gray-400">Hôm nay: ${fmtTime(u.today_total)}</div></div>
    </div>`).join('');}
  }catch(e){console.error(e);}
  buildHeatmap(data);buildBadges(data);
  const sorted=Object.entries(data).filter(([,u])=>(u.daily[today]||0)>0).sort(([,a],[,b])=>(b.daily[today]||0)-(a.daily[today]||0)).slice(0,10);
  const medals=['🥇','🥈','🥉'];
  document.getElementById('leaderboard').innerHTML=sorted.length?sorted.map(([,u],i)=>{const lv=(u.class??u.level)||0,pct=coinPct(u.total_earned||0);return `<div class="flex items-center gap-3 py-2 px-1 rounded-xl hover:bg-slate-700 transition">
    <span class="text-xl w-7 text-center">${medals[i]||`${i+1}.`}</span>
    <div class="flex-1 min-w-0"><div class="font-semibold truncate">${u.name}</div>
      <div class="text-xs text-gray-400">Class ${lv} · ${(u.total_earned||0).toLocaleString()} coins · 🔥${u.streak||0}</div>
      <div class="w-full bg-slate-700 rounded-full h-1 mt-1"><div style="width:${pct}%" class="coin-bar h-1 rounded-full"></div></div></div>
    <div class="text-indigo-300 font-mono font-bold">${fmtTime(u.daily[today]||0)}</div></div>`;}).join(''):'<p class="text-gray-500">Chưa có ai học hôm nay!</p>';
  const days=getPastDays(7);const totals=days.map(d=>Math.round(Object.values(data).reduce((s,u)=>s+(u.daily[d]||0),0)/60));
  if(chartInst)chartInst.destroy();
  chartInst=new Chart(document.getElementById('weekChart'),{type:'bar',
    data:{labels:days.map(d=>d.slice(5)),datasets:[{label:'Phút học',data:totals,backgroundColor:'rgba(99,102,241,.7)',borderColor:'#6366f1',borderWidth:2,borderRadius:8}]},
    options:{responsive:true,plugins:{legend:{labels:{color:'#e2e8f0'}}},scales:{y:{ticks:{color:'#94a3b8'},grid:{color:'#334155'}},x:{ticks:{color:'#94a3b8'},grid:{color:'#334155'}}}}});
  document.getElementById('lastUpdate').textContent='Cập nhật: '+new Date().toLocaleTimeString('vi-VN');
}
loadData();setInterval(loadData,30000);
</script></body></html>'''

flask_app = Flask(__name__)
_live_state_cache: list = []
_live_state_lock = threading.RLock()

@flask_app.route('/')
def dashboard(): return render_template_string(DASHBOARD_HTML)

@flask_app.route('/api/stats')
def api_stats():
    merged: dict[str, dict] = {}
    for guild_id, data in load_all_guild_data().items():
        for uid, info in data.items():
            key = uid if uid not in merged else f'{guild_id}:{uid}'
            row = deepcopy(info)
            row['_guild_id'] = guild_id
            merged[key] = row
    return jsonify(merged)

@flask_app.route('/api/live')
def api_live():
    with _live_state_lock:
        snapshot = deepcopy(_live_state_cache)
    return jsonify(snapshot)

@flask_app.route('/api/card/<int:member_id>')
def api_card(member_id: int):
    card = generate_profile_card(member_id)
    if not card:
        return 'Not found', 404
    return send_file(io.BytesIO(card), mimetype='image/png',
                     download_name=f'card_{member_id}.png')

def run_dashboard():
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    flask_app.run(host='0.0.0.0', port=DASHBOARD_PORT, debug=False, use_reloader=False)

def _update_live_cache():
    global _live_state_cache
    now   = datetime.now()
    today = now.strftime('%Y-%m-%d')
    result = []
    for mid, start in list(join_times.items()):
        member_guild = None
        member_obj = None
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m:
                member_guild = guild
                member_obj = m
                break
        if member_guild is None or member_obj is None:
            log.info(f'[Dashboard] Skipping stale live cache entry for member_id={mid}; member not found in current guilds.')
            continue
        with guild_data_context(member_guild.id):
            data = _get_live_enriched_data(member_guild)
        uid      = str(mid)
        info     = data.get(uid, {})
        saved    = info.get('daily', {}).get(today, 0)
        unsaved = _get_unsaved_study_seconds(mid, now)
        is_stream = is_video = False
        if member_obj and member_obj.voice:
            is_stream = bool(member_obj.voice.self_stream)
            is_video  = bool(member_obj.voice.self_video)
        result.append({
            'name':         info.get('name', f'User {mid}'),
            'level':        info.get('class', info.get('level', 0)),
            'class':        info.get('class', info.get('level', 0)),
            'session_secs': int((now - start).total_seconds()),
            'today_total':  saved + unsaved,
            'is_streaming': is_stream,
            'is_video':     is_video,
        })
    result.sort(key=lambda x: x['today_total'], reverse=True)
    with _live_state_lock:
        _live_state_cache = result

# ─── GLOBAL ERROR HANDLER ────────────────────────────────────────────────────

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    cmd_name = interaction.command.name if interaction.command else '?'
    log.error(f'Slash command error [{cmd_name}]: {error}', exc_info=error)
    msg = '❌ Đã xảy ra lỗi! Thử lại sau.'
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(msg, ephemeral=True)
        else:
            await interaction.followup.send(msg, ephemeral=True)
    except Exception:
        pass

# ─── START ───────────────────────────────────────────────────────────────────

async def _run_bot():
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    flushed = False

    def _request_shutdown():
        if not stop_event.is_set():
            log.info('[Runtime] Shutdown signal received.')
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, _request_shutdown)

    bot_task = asyncio.create_task(bot.start(TOKEN))
    stop_task = asyncio.create_task(stop_event.wait())

    try:
        done, _ = await asyncio.wait(
            {bot_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if bot_task in done:
            stop_task.cancel()
            await bot_task
            return

        flushed = True
        await _flush_active_sessions('shutdown')
        await bot.close()
        await bot_task
    finally:
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_task
        if not flushed and not bot.is_closed():
            with contextlib.suppress(Exception):
                await _flush_active_sessions('shutdown')
            await bot.close()


if __name__ == '__main__':
    asyncio.run(_run_bot())
