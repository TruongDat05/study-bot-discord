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
import threading
import io
import shutil
import tempfile               
import math
import httpx
from copy import deepcopy
from pathlib import Path      
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, send_file
from pomodoro import PomodoroSession, create_pomodoro_cog
from weekly_report import setup_weekly_report

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

load_dotenv()

def _env_int(name: str, default: int | None = None) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default

TOKEN              = os.getenv('DISCORD_TOKEN')
CREATE_ROOM_CHANNEL_ID = _env_int('CREATE_ROOM_CHANNEL_ID')
TEMP_ROOM_CATEGORY_ID  = _env_int('TEMP_ROOM_CATEGORY_ID')
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

GEMINI_FLASH_MODEL      = os.getenv('GEMINI_FLASH_MODEL', 'gemini-2.5-flash')
GEMINI_FLASH_LITE_MODEL = os.getenv('GEMINI_FLASH_LITE_MODEL', 'gemini-2.5-flash-lite')
GROQ_MODELS             = os.getenv('GROQ_MODELS', 'llama-3.3-70b-versatile,qwen/qwen3-32b')
OPENROUTER_MODEL        = os.getenv('OPENROUTER_MODEL', 'openai/gpt-oss-20b:free')
HUGGINGFACE_MODEL       = os.getenv('HUGGINGFACE_MODEL', 'deepseek-ai/DeepSeek-R1:fastest')
AI_HTTP_TIMEOUT         = max(1.0, _env_float('AI_HTTP_TIMEOUT', 45.0))
AI_ONE_MESSAGE_LIMIT    = max(1, min(2000, _env_int('AI_ONE_MESSAGE_LIMIT', 1750) or 1750))
AI_MAX_OUTPUT_TOKENS    = max(1, _env_int('AI_MAX_OUTPUT_TOKENS', 900) or 900)

SERVERS = [
    {
        'voice_channels': [
            1483271561036435660, 
            1483301292427186358,
            1489183048665923735, 
            1489183241473626142, 
            1489183303226621992
        ],
        'report_channel': 1483288436369653861,
    },
]


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
BACKUP_DIR          = BASE_DIR / 'backups'

DASHBOARD_PORT      = _env_int('DASHBOARD_PORT', 5000)
ABSENT_DAYS_WARN    = 2
CHECKPOINT_MINUTES  = 5
LIVE_UPDATE_MINUTES = 5
MILESTONE_MINUTES   = [30, 60, 120, 180, 240, 300, 360]
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

LEVEL_ROLES: dict[int, int | None] = {
    0:  None,
    1:  1493178947436019832,   # Học Sinh
    2:  1493179256556224612,   # Chăm Chỉ
    3:  1493179380497907763,   # Tập Trung
    4:  1493179493836132423,   # Xuất Sắc
    5:  1493179683804545084,   # Tinh Anh
    6:  1493179767376318504,   # Huyền Thoại
    7:  1493180760612409446,   # Bậc Thầy
    8:  1493179837261811822,   # Thiên Tài
    9:  1493180024818372759,   # Vô Địch
    10: 1493180158562275390,   # Thần Học
}

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

MOTIVATIONS_BILINGUAL = [
    ('"The secret of getting ahead is getting started." — Mark Twain',
     '🌟 Bí quyết để tiến về phía trước là bắt đầu ngay bây giờ!'),
    ('"It always seems impossible until it\'s done." — Nelson Mandela',
     '💪 Mọi thứ đều có vẻ bất khả thi — cho đến khi bạn làm được!'),
    ('"Don\'t watch the clock; do what it does. Keep going." — Sam Levenson',
     '⏰ Đừng nhìn đồng hồ, hãy cứ tiến lên. Mỗi phút học là một bước đến thành công!'),
    ('"Success is the sum of small efforts repeated day in and day out."',
     '🔥 Thành công là tổng của những nỗ lực nhỏ, lặp đi lặp lại mỗi ngày!'),
    ('"The expert in anything was once a beginner."',
     '🌱 Chuyên gia của ngày mai là người học sinh chăm chỉ của hôm nay!'),
    ('"Focus on your goal. Don\'t look in any direction but ahead."',
     '🎯 Tập trung vào mục tiêu! Đừng nhìn sang ngang, chỉ nhìn về phía trước!'),
    ('"Hard work beats talent when talent doesn\'t work hard." — Tim Notke',
     '⚡ Chăm chỉ sẽ đánh bại tài năng khi tài năng không chịu cố gắng!'),
    ('"Push yourself, because no one else is going to do it for you."',
     '🚀 Hãy tự thúc đẩy bản thân — không ai làm điều đó thay bạn đâu!'),
    ('"Great things never came from comfort zones."',
     '💎 Những điều tuyệt vời không bao giờ đến từ vùng an toàn!'),
    ('"Dream it. Believe it. Build it."',
     '✨ Mơ về nó. Tin vào nó. Xây dựng nó — bắt đầu từ hôm nay!'),
    ('"You don\'t have to be great to start, but you have to start to be great."',
     '🌅 Bạn không cần giỏi để bắt đầu, nhưng phải bắt đầu để trở nên giỏi!'),
    ('"Success doesn\'t come from what you do occasionally. It comes from consistency."',
     '📅 Thành công đến từ sự kiên trì mỗi ngày, không phải từ những lúc hứng khởi!'),
    ('"One day or day one. You decide."',
     '🔑 "Một ngày nào đó" hay "Ngày một"? Chỉ có bạn mới quyết định được!'),
    ('"Discipline is choosing between what you want now and what you want most."',
     '⚖️ Kỷ luật là lựa chọn giữa điều bạn muốn ngay và điều bạn muốn nhất!'),
    ('"Every master was once a disaster." — T. Harv Eker',
     '🎓 Mỗi chuyên gia đều đã từng là người mới. Đừng ngại sai!'),
    ('"Believe you can and you\'re halfway there." — Theodore Roosevelt',
     '🌈 Tin rằng bạn làm được và bạn đã đi được nửa chặng đường rồi!'),
    ('"Your future self will thank you for the work you put in today."',
     '🙏 Bản thân tương lai của bạn sẽ cảm ơn những nỗ lực của ngày hôm nay!'),
    ('"Work hard in silence. Let success be your noise."',
     '🤫 Làm việc chăm chỉ trong im lặng. Để thành công nói thay bạn!'),
    ('"Study now, shine later."',
     '✨ Học chăm chỉ hôm nay — tỏa sáng rực rỡ ngày mai!'),
    ('"Fall seven times, stand up eight." — Japanese proverb',
     '🔄 Ngã bảy lần, đứng dậy tám lần. Đó mới là người chiến thắng!'),
]

def _random_motivation() -> str:
    return _random_motivation_plain()

def _random_motivation_plain() -> str:
    en, vi = random.choice(MOTIVATIONS_BILINGUAL)
    return f'{en}\n{vi}'

MILESTONE_DM = {
    30:  '⏰ Bạn đã học được **30 phút**! 💪 Tiếp tục nhé — bạn đang làm rất tốt!',
    60:  '🌟 **1 tiếng** học tập! Xuất sắc! Uống nước và vươn vai chút nhé! 💧',
    120: '🔥 **2 tiếng** liên tục! Phi thường! Nghỉ 5 phút rồi chiến tiếp! 🧘',
    180: '💎 **3 tiếng**! Bạn đang ở đỉnh cao! Cơ thể cần nghỉ ngơi ngắn đấy! 🍵',
    240: '🚀 **4 tiếng**! Chiến binh thực sự! Ăn nhẹ gì đó để nạp năng lượng nhé! 🍌',
    300: '👑 **5 tiếng**! Vô địch! Đây là phiên học đáng nhớ! 🏆',
    360: '⚡ **6 tiếng**! Huyền thoại! Bạn thật đáng kinh ngạc — hãy nghỉ ngơi xứng đáng! 🌙',
}

NOTIFY_GREEN = 0x2ECC71
NOTIFY_GOLD = 0xF1C40F
NOTIFY_RED = 0xE74C3C
NOTIFY_BLUE = 0x5865F2
NOTIFY_PURPLE = 0x9B59B6

STUDY_MILESTONE_SECONDS = [
    (60 * 60, '1h', '1 giờ'),
    (5 * 60 * 60, '5h', '5 giờ'),
    (10 * 60 * 60, '10h', '10 giờ'),
    (50 * 60 * 60, '50h', '50 giờ'),
    (100 * 60 * 60, '100h', '100 giờ'),
    (200 * 60 * 60, '200h', '200 giờ'),
]
COIN_EARNING_MILESTONES = [1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]

if not TOKEN:
    raise ValueError('Không tìm thấy DISCORD_TOKEN trong file .env!')

STATIC_FOCUS_CHANNEL_IDS = tuple(ch for s in SERVERS for ch in s['voice_channels'])
FOCUS_CHANNEL_IDS = list(STATIC_FOCUS_CHANNEL_IDS)
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
bot = commands.Bot(command_prefix='!', intents=intents)

# ─── STATE ───────────────────────────────────────────────────────────────────

pending_checks:       dict[int, asyncio.Task] = {}
join_times:           dict[int, datetime]     = {}
last_checkpoint:      dict[int, datetime]     = {}
milestone_sent:       dict[int, set]          = {}
live_message_ids:     dict[int, int]          = {}
daily_first_join:     dict[str, int]          = {}
session_counts:       dict[int, int]          = {}
daily_board_sent:     set                     = set()
report_sent_today:    set                     = set()
remind_tasks:         dict[int, tuple]        = {}
media_active_members: set                     = set()
cam_thanks_sent:     set[int]                 = set()
temp_rooms:           dict[int, dict]         = {}
temporary_room_delete_tasks: dict[int, asyncio.Task] = {}
_role_sync_locks:     dict[int, asyncio.Lock] = {}
_dashboard_started:   bool                    = False
_room_panel_view_registered: bool             = False
_startup_extensions_ready: bool               = False

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

def _load_data_unlocked() -> dict:
    try:
        if DATA_FILE.exists():
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            return _normalize_all_users(loaded) if isinstance(loaded, dict) else {}
    except (json.JSONDecodeError, IOError) as e:
        log.error(f'Lỗi đọc data: {e}', exc_info=True)
    except Exception as e:
        log.error(f'Lỗi không xác định khi đọc data: {e}', exc_info=True)
    return {}

def load_data() -> dict:
    with _data_lock:
        return _load_data_unlocked()

def _verify_saved_data_unlocked(expected: dict) -> bool:
    try:
        if not DATA_FILE.exists():
            return False
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            loaded = json.load(f)
        return isinstance(loaded, dict) and loaded == expected
    except Exception as e:
        log.error(f'Lỗi xác minh data sau khi lưu: {e}', exc_info=True)
        return False

def _save_data_unlocked(data: dict):
    global _last_data_save_success
    _last_data_save_success = False
    last_error: Exception | None = None

    for attempt in range(2):
        temp_path: str | None = None
        try:
            temp_fd, temp_path = tempfile.mkstemp(
                dir=DATA_FILE.parent,
                prefix='.study_data_',
                suffix='.json.tmp'
            )

            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            Path(temp_path).replace(DATA_FILE)

            if not _verify_saved_data_unlocked(data):
                raise IOError('Xác minh dữ liệu sau khi lưu không khớp.')

            _last_data_save_success = True
            return
        except Exception as e:
            last_error = e
            if temp_path is not None:
                Path(temp_path).unlink(missing_ok=True)
            if attempt == 0:
                log.error(f'Lỗi lưu data (lần 1/2): {e}', exc_info=True)
                continue

    if isinstance(last_error, IOError):
        log.critical(f'Lỗi lưu data nghiêm trọng: {last_error}', exc_info=True)
        print(f'❌ SAVE ERROR: {last_error}', file=sys.stderr)
    elif last_error is not None:
        log.critical(f'Lỗi không xác định khi lưu data: {last_error}', exc_info=True)
        print(f'❌ CRITICAL SAVE ERROR: {last_error}', file=sys.stderr)

def save_data(data: dict):
    with _data_lock:
        _normalize_all_users(data)
        _save_data_unlocked(data)

def update_data(mutator):
    """
    Thread-safe update that persists changes atomically
    and returns a deepcopy of the saved state.
    """
    with _data_lock:
        data = _load_data_unlocked()
        _normalize_all_users(data)
        result = mutator(data)
        _normalize_all_users(data)
        _save_data_unlocked(data)
        return result, deepcopy(data)

def _serialize_dt(dt: datetime) -> str:
    return dt.isoformat()

def _parse_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

def _serialize_temp_rooms_snapshot() -> dict:
    snapshot: dict[str, dict] = {}
    for room_id, meta in list(temp_rooms.items()):
        if room_id in STATIC_FOCUS_CHANNEL_IDS:
            continue
        created_at = meta.get('created_at')
        if isinstance(created_at, datetime):
            created_at_str = _serialize_dt(created_at)
        else:
            created_at_str = str(created_at or '')
        snapshot[str(room_id)] = {
            'room_id': room_id,
            'owner_id': _as_int(meta.get('owner_id')),
            'guild_id': _as_int(meta.get('guild_id')),
            'created_at': created_at_str,
        }
    return snapshot

def _restore_temp_rooms_from_snapshot(raw: dict):
    restored = 0
    temp_rooms.clear()
    FOCUS_CHANNEL_IDS[:] = list(STATIC_FOCUS_CHANNEL_IDS)

    rooms = raw.get('temp_rooms', {}) if isinstance(raw, dict) else {}
    if not isinstance(rooms, dict):
        return

    for room_id_str, meta in rooms.items():
        if not isinstance(meta, dict):
            continue
        try:
            room_id = int(room_id_str)
        except (TypeError, ValueError):
            continue
        if room_id in STATIC_FOCUS_CHANNEL_IDS:
            log.warning(f'[TempRoom] Ignoring temp room state for static focus channel {room_id}')
            continue

        channel = bot.get_channel(room_id)
        if not isinstance(channel, discord.VoiceChannel):
            continue

        created_at = _parse_dt(str(meta.get('created_at', ''))) or datetime.now()
        temp_rooms[room_id] = {
            'room_id': room_id,
            'owner_id': _as_int(meta.get('owner_id')),
            'guild_id': _as_int(meta.get('guild_id', channel.guild.id)),
            'created_at': created_at,
        }
        if room_id not in FOCUS_CHANNEL_IDS:
            FOCUS_CHANNEL_IDS.append(room_id)
        restored += 1

    if restored:
        log.info(f'[TempRoom] Restored {restored} tracked temporary rooms.')

def save_runtime_state():
    now = datetime.now()
    snapshot = {
        'saved_at': now.strftime('%Y-%m-%d'),
        'saved_at_ts': _serialize_dt(now),
        'join_times': {str(mid): _serialize_dt(ts) for mid, ts in join_times.items()},
        'last_checkpoint': {str(mid): _serialize_dt(ts) for mid, ts in last_checkpoint.items()},
        'milestone_sent': {str(mid): sorted(list(ms)) for mid, ms in milestone_sent.items()},
        'daily_first_join': {d: int(mid) for d, mid in daily_first_join.items()},
        'session_counts': {str(mid): int(cnt) for mid, cnt in session_counts.items()},
        'media_active_members': sorted(list(media_active_members)),
        'temp_rooms': _serialize_temp_rooms_snapshot(),
    }
    with _runtime_lock:
        temp_path: str | None = None
        try:
            temp_fd, temp_path = tempfile.mkstemp(
                dir=RUNTIME_STATE_FILE.parent,
                prefix='.runtime_state_',
                suffix='.json.tmp'
            )
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            Path(temp_path).replace(RUNTIME_STATE_FILE)
        except IOError as e:
            log.error(f'Lỗi lưu runtime state: {e}')
            if temp_path is not None:
                Path(temp_path).unlink(missing_ok=True)
        except Exception as e:
            log.error(f'Lỗi không xác định khi lưu runtime state: {e}', exc_info=True)
            if temp_path is not None:
                Path(temp_path).unlink(missing_ok=True)

def load_runtime_state() -> dict:
    """✅ FIX: Use Path for runtime state file"""
    with _runtime_lock:
        try:
            if RUNTIME_STATE_FILE.exists():
                with open(RUNTIME_STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.error(f'Lỗi đọc runtime state: {e}')
    return {}

def restore_runtime_state():
    raw = load_runtime_state()
    if not raw:
        return

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    saved_at = raw.get('saved_at')
    same_day = saved_at == today
    saved_at_ts = _parse_dt(str(raw.get('saved_at_ts', '')))
    session_restore_allowed = same_day
    if saved_at_ts is not None:
        age_seconds = (now - saved_at_ts).total_seconds()
        session_restore_allowed = 0 <= age_seconds <= RUNTIME_RESTORE_MAX_AGE_SECONDS
    elif not same_day:
        session_restore_allowed = False

    _restore_temp_rooms_from_snapshot(raw)

    if not session_restore_allowed:
        log.info('[Runtime] Stored voice sessions are stale; current voice members will start fresh.')

    restored_join: dict[int, datetime] = {}
    restored_checkpoint: dict[int, datetime] = {}
    restored_milestones: dict[int, set] = {}
    restored_media: set[int] = set()
    restored_sessions: dict[int, int] = {}
    restored_first_join: dict[str, int] = {}

    if session_restore_allowed:
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
            restored_join[mid] = ts

        for mid_str, ts_str in raw.get('last_checkpoint', {}).items():
            try:
                mid = int(mid_str)
            except (ValueError, TypeError):
                continue
            if mid not in restored_join:
                continue
            ts = _parse_dt(ts_str)
            if ts is None:
                continue
            if ts < restored_join[mid]:
                ts = restored_join[mid]
            if ts > now:
                ts = now
            restored_checkpoint[mid] = ts

        for mid in restored_join:
            restored_checkpoint.setdefault(mid, restored_join[mid])

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
        for d, mid in raw.get('daily_first_join', {}).items():
            if isinstance(d, str) and isinstance(mid, int):
                restored_first_join[d] = mid

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
        f'[Runtime] Restored {len(join_times)} phiên, '
        f'{len(media_active_members)} đang active media.'
    )

def backup_data():
    """✅ FIX: Use Path for backup operations"""
    if not DATA_FILE.exists():
        return
    BACKUP_DIR.mkdir(exist_ok=True, parents=True)
    ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = BACKUP_DIR / f'study_data_{ts}.json'
    try:
        shutil.copy2(DATA_FILE, dest)
        files = sorted(
            BACKUP_DIR.glob('study_data_*.json'),
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
        'balance': 0,
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

    now = datetime.now()
    embed = discord.Embed(
        title=title,
        description=_compact_notice_description(description),
        color=color,
        timestamp=now,
    )
    embed.set_footer(text=f'{footer} • Today at {now:%H:%M}')

    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.info(f'Cannot DM notification to {member} ({member.id}). User may have DMs closed.')
    except Exception as e:
        log.warning(f'Failed to send DM notification to {member.id}: {e}')

_NOTICE_EMOJI_RE = re.compile(r'[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE0F]')

def _compact_notice_description(message: str) -> str:
    text = str(message or '').strip()
    text = _NOTICE_EMOJI_RE.sub('', text)
    text = text.replace('**', '')
    text = re.sub(r'[─━]{3,}', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip() or 'Thông báo từ BetterMe.'

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
    if 'pomodoro đã dừng' in text:
        return 'Pomodoro đã dừng', NOTIFY_GOLD
    if 'nhắc học' in text or 'nhắc' in text or 'chưa học' in text:
        return 'Nhắc nhở', NOTIFY_GOLD
    if 'chào mừng' in text or 'phòng học tạm đã được tạo' in text:
        return 'Chào mừng', NOTIFY_BLUE
    if 'study now' in text or 'động lực' in text:
        return 'Động lực hôm nay', NOTIFY_BLUE
    return 'Thông báo', NOTIFY_BLUE

def reset_cam_notification(member_id: int):
    cam_thanks_sent.discard(member_id)

async def notify_cam_started(member: discord.Member, channel=None):
    if member.id in cam_thanks_sent:
        return
    cam_thanks_sent.add(member.id)
    await send_private_notify_embed(
        member=member,
        title='Cảm ơn',
        description=f'{member.display_name}, cảm ơn bạn đã bật Cam hoặc Stream.',
        color=NOTIFY_GREEN,
    )

async def notify_class_up(member: discord.Member, channel, new_class: str):
    key = str(new_class)
    if not _claim_user_notification(member.id, 'notified_classes', key, member.display_name):
        return
    await send_private_notify_embed(
        member=member,
        title='Chúc mừng',
        description=f'Bạn đã đạt được hạng **{new_class}**.',
        color=NOTIFY_GOLD,
        footer='Tiếp tục giữ phong độ nhé.',
    )

async def notify_study_milestones(member: discord.Member, channel=None):
    uid = str(member.id)
    data = load_data()
    info = data.get(uid)
    if not isinstance(info, dict):
        return
    total_seconds = _as_int(info.get('total', 0))
    reached = [
        key for seconds, key, _ in STUDY_MILESTONE_SECONDS
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
    label = next(
        (label for _, key, label in STUDY_MILESTONE_SECONDS if key == highest_key),
        highest_key,
    )
    await send_private_notify_embed(
        member=member,
        title='Chúc mừng',
        description=(
            f'Bạn đã học tổng cộng **{label}** và đạt một cột mốc mới.\n'
            'Hãy tiếp tục duy trì thói quen học tập này nhé!'
        ),
        color=NOTIFY_GOLD,
    )

async def notify_coin_milestones(member: discord.Member, channel=None):
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
    await send_private_notify_embed(
        member=member,
        title='Cột mốc kinh tế',
        description=f'Bạn đã kiếm tổng cộng **{format_coins(amount)}**.',
        color=NOTIFY_PURPLE,
        footer='Economy System',
    )

async def notify_loan_event(
    member: discord.Member,
    channel,
    title: str,
    amount: int,
    detail: str,
):
    await send_private_notify_embed(
        member=member,
        title=title,
        description=(
            f'Số tiền: `{format_coins(amount)}`\n'
            f'{detail}'
        ),
        color=NOTIFY_PURPLE,
        footer='Economy System',
    )

async def notify_overdue_loans(member: discord.Member, channel=None):
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

    await send_private_notify_embed(
        member=member,
        title='Khoản vay quá hạn',
        description=(
            f'Tổng quá hạn: `{format_coins(total_overdue)}`\n'
            + '\n'.join(loan_lines)
        ),
        color=NOTIFY_RED,
        footer='Economy System',
    )

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
    await send_private_notify_embed(
        member=member,
        title='Phiên học kết thúc',
        description=(
            f'Phiên này: `{session_time}`\n'
            f'Hôm nay: `{today_time}`\n'
            f'Earned hôm nay: `{format_coins(earned_today)}`\n'
            f'Balance: `{format_coins(balance)}` · Debt: `{format_coins(debt)}`\n'
            f'Class: `{current_class}` · Total earned: `{format_coins(total_earned)}`\n'
            f'Streak: `{streak} ngày`'
        ),
        color=NOTIFY_GREEN,
    )

async def _handle_progress_notifications(member: discord.Member, result: dict | None = None, channel=None):
    result = result or {}
    if result.get('level_up'):
        await notify_class_up(member, channel, class_label(result['new_level']))
    await notify_study_milestones(member, channel)
    await notify_coin_milestones(member, channel)
    await notify_overdue_loans(member, channel)

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

def _get_live_enriched_data() -> dict:
    data = load_data()
    for mid in list(join_times.keys()):
        uid = str(mid)
        if uid in data:
            continue
        name = f'User {mid}'
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m:
                name = m.display_name
                break
        data[uid] = _default_user(name)
    return data

def get_report_channel_for(member: discord.Member):
    guild_id = member.guild.id
    for server in SERVERS:
        ch = bot.get_channel(server['report_channel'])
        if ch and ch.guild.id == guild_id:
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

def _get_pomodoro_phase_start(sess) -> datetime:
    if getattr(sess, 'phase', None) == 'work':
        return sess.phase_end - timedelta(minutes=sess.work_minutes)
    if getattr(sess, 'phase', None) == 'break':
        return sess.phase_end - timedelta(minutes=sess.break_minutes)
    return sess.phase_end

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
    sess = _get_pomodoro_session(member_id)

    if sess:
        if sess.phase == 'work':
            phase_start = _get_pomodoro_phase_start(sess)
            effective_end = min(effective_end, sess.phase_end)
            if sess.completed_rounds > 0 or checkpoint >= phase_start:
                effective_start = max(effective_start, phase_start)
        elif sess.phase == 'break':
            break_start = _get_pomodoro_phase_start(sess)
            prev_work_start = break_start - timedelta(minutes=sess.work_minutes)
            effective_end = min(effective_end, break_start)
            effective_start = max(effective_start, prev_work_start)
        else:
            return None

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
    start_explicit = start_time is not None
    end_time = end_time or datetime.now()
    start_time = start_time or (end_time - timedelta(seconds=seconds))
    if not start_explicit and member_id in join_times:
        checkpoint = last_checkpoint.get(member_id, join_times[member_id])
        sess = _get_pomodoro_session(member_id)
        if sess and sess.phase == 'work':
            phase_start = _get_pomodoro_phase_start(sess)
            if sess.completed_rounds == 0 and checkpoint < phase_start and start_time >= phase_start:
                start_time = checkpoint

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
            coins_earned = (coin_acc // 60) * COINS_PER_MINUTE
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

def _try_font(paths: list[str], size: int):
    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle([x1, y1, x2, y2], radius=radius, fill=fill, outline=outline, width=width)

def generate_profile_card(member_id: int) -> bytes | None:
    if not PIL_AVAILABLE:
        return None
    data = load_data()
    uid  = str(member_id)
    if uid not in data:
        return None
    info    = data[uid]
    name    = info.get('name', 'Unknown')
    total_earned = info.get('total_earned', 0)
    balance = info.get('balance', 0)
    debt = _active_debt(info)
    class_idx = min(get_money_class(total_earned), len(CLASS_NAMES) - 1)
    streak  = info.get('streak', 0)
    longest = info.get('longest_streak', 0)
    total   = info.get('total', 0)
    badges  = info.get('badges', [])
    today   = datetime.now().strftime('%Y-%m-%d')
    today_secs = info['daily'].get(today, 0)
    today_secs += _get_unsaved_study_seconds(member_id)
    today_earned = info.get('daily_earnings', {}).get(today, 0)

    BG = (15, 23, 42); CARD = (30, 41, 59); ACCENT = (99, 102, 241)
    ACCENT2 = (139, 92, 246); GOLD = (251, 191, 36); GREEN = (34, 197, 94)
    TEXT1 = (226, 232, 240); TEXT2 = (148, 163, 184)
    W, H = 680, 360
    img  = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    BOLD_FONTS = [
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
        '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',
        '/System/Library/Fonts/Helvetica.ttc',
        'C:/Windows/Fonts/arialbd.ttf',
    ]
    REG_FONTS = [
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
        '/System/Library/Fonts/Helvetica.ttc',
        'C:/Windows/Fonts/arial.ttf',
    ]
    f_huge = _try_font(BOLD_FONTS, 56); f_xl  = _try_font(BOLD_FONTS, 28)
    f_lg   = _try_font(BOLD_FONTS, 20); f_md  = _try_font(REG_FONTS,  16)
    f_sm   = _try_font(REG_FONTS,  13)

    _draw_rounded_rect(draw, [16, 16, W-16, H-16], radius=16, fill=CARD, outline=(51, 65, 85), width=1)
    BAR_W = W - 32
    for i in range(BAR_W):
        r = int(ACCENT[0] + (ACCENT2[0]-ACCENT[0]) * i / BAR_W)
        g = int(ACCENT[1] + (ACCENT2[1]-ACCENT[1]) * i / BAR_W)
        b = int(ACCENT[2] + (ACCENT2[2]-ACCENT[2]) * i / BAR_W)
        draw.rectangle([16+i, 16, 17+i, 22], fill=(r, g, b))

    cx, cy, cr = 96, 115, 54
    draw.ellipse([cx-cr, cy-cr, cx+cr, cy+cr], fill=(51, 65, 85), outline=ACCENT, width=3)
    draw.text((cx, cy-10), str(class_idx), font=f_huge, fill=ACCENT, anchor='mm')
    draw.text((cx, cy+26), 'CLASS',   font=f_sm,   fill=TEXT2,   anchor='mm')

    display_name = name if len(name) <= 18 else name[:17] + '…'
    draw.text((168, 78),  display_name,                         font=f_xl, fill=TEXT1)
    draw.text((170, 112), f'Class {class_idx} • {CLASS_NAMES[class_idx]}', font=f_md, fill=TEXT2)

    coin_start = CLASS_THRESHOLDS[class_idx]
    coin_end   = CLASS_THRESHOLDS[min(class_idx+1, len(CLASS_THRESHOLDS)-1)]
    coin_pct   = min(1.0, (total_earned - coin_start) / max(1, coin_end - coin_start))
    BX, BY, BW, BH = 168, 144, 280, 12
    _draw_rounded_rect(draw, [BX, BY, BX+BW, BY+BH], radius=6, fill=(51, 65, 85))
    fw = int(BW * coin_pct)
    if fw > 8:
        for i in range(fw):
            r2 = int(ACCENT[0] + (ACCENT2[0]-ACCENT[0]) * i / max(1, fw))
            g2 = int(ACCENT[1] + (ACCENT2[1]-ACCENT[1]) * i / max(1, fw))
            b2 = int(ACCENT[2] + (ACCENT2[2]-ACCENT[2]) * i / max(1, fw))
            draw.rectangle([BX+i, BY, BX+i+1, BY+BH], fill=(r2, g2, b2))
    draw.text((BX, BY+BH+6), f'{total_earned:,} earned  ({int(coin_pct*100)}%)', font=f_sm, fill=TEXT2)

    draw.rectangle([32, 190, W-32, 191], fill=(51, 65, 85))
    stats = [
        ('Hôm nay',   format_time(today_secs), GREEN),
        ('Tổng cộng', format_time(total),       ACCENT),
        ('Balance',   format_coins(balance),     GOLD),
        ('Earned',    format_coins(today_earned), GREEN),
        ('Debt',      format_coins(debt),        (236, 72, 153)),
        ('Class',     CLASS_NAMES[class_idx],    ACCENT2),
    ]
    cols, col_w = 3, (W-64) // 3
    for i, (label, value, color) in enumerate(stats):
        sx = 32 + (i % cols) * col_w; sy = 206 + (i // cols) * 58
        draw.text((sx, sy),    label, font=f_sm, fill=TEXT2)
        draw.text((sx, sy+18), value, font=f_lg, fill=color)

    CHART_X, CHART_Y, CHART_H = 32, 328, 18
    days_7 = [(datetime.now() - timedelta(days=6-i)).strftime('%Y-%m-%d') for i in range(7)]
    vals   = [info['daily'].get(d, 0) // 60 for d in days_7]
    max_v  = max(1, max(vals)); bw2 = 26; gap = 4
    for i, (d, v) in enumerate(zip(days_7, vals)):
        bx = CHART_X + i * (bw2+gap)
        bh = max(2, int(CHART_H * v / max_v)); by = CHART_Y + CHART_H - bh
        _draw_rounded_rect(draw, [bx, by, bx+bw2, CHART_Y+CHART_H], radius=3,
                           fill=ACCENT if d == today else (51, 65, 85+20))
        draw.text((bx+bw2//2, CHART_Y+CHART_H+5), d[8:], font=f_sm,
                  fill=TEXT1 if d == today else TEXT2, anchor='mt')
    draw.text((CHART_X + 7*(bw2+gap)+8, CHART_Y+4), '7 ngày gần đây', font=f_sm, fill=TEXT2)
    if badges:
        bx_start = W - 200
        draw.text((bx_start, 328), 'Huy hiệu', font=f_sm, fill=TEXT2)
        line = '  '.join(BADGES[b]['name'].split(' ')[-1] for b in badges[:6] if b in BADGES)
        draw.text((bx_start, 344), line[:28], font=f_sm, fill=GOLD)
    ts = datetime.now().strftime('%d/%m/%Y %H:%M')
    draw.text((W-32, H-24), f'study.bot • {ts}', font=f_sm, fill=(71, 85, 105), anchor='rm')

    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
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


async def _ensure_role_synced(member: discord.Member, current_level: int):
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
        expected_role_id = LEVEL_ROLES.get(current_level)
        expected_role = guild.get_role(expected_role_id) if expected_role_id else None

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
        for lvl, role_id in LEVEL_ROLES.items():
            if role_id is None:
                continue
            role = guild.get_role(role_id)
            if role and role in current_roles and role != expected_role:
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

# ─── SESSION MANAGEMENT ──────────────────────────────────────────────────────

def record_join(member: discord.Member):
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

def _get_pomodoro_session(member_id: int):
    pomo_cog = bot.cogs.get('PomodoroCog')
    if not pomo_cog:
        return None
    return getattr(pomo_cog, '_sessions', {}).get(member_id)

def _get_unsaved_study_seconds(member_id: int, now: datetime | None = None) -> int:
    window = _get_pending_study_window(member_id, now or datetime.now())
    if not window:
        return 0
    start_time, end_time = window
    return max(0, int((end_time - start_time).total_seconds()))

async def _do_checkpoint(member: discord.Member, now: datetime | None = None) -> tuple[int, dict]:
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

async def _check_quests_and_badges(member: discord.Member):
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
    await notify_coin_milestones(member)
        
    check_and_award_badges(uid, member)

async def record_leave_and_notify(member: discord.Member, force_in_pomodoro: bool = False) -> int:
    if member.id not in join_times: return 0

    now        = datetime.now()
    _, result = await _do_checkpoint(member, now)
    if result.get('level_up'):
        await _ensure_role_synced(member, result['new_level'])
    await _handle_progress_notifications(member, result)

    total_duration = int((now - join_times.pop(member.id)).total_seconds())
    last_checkpoint.pop(member.id, None)
    milestone_sent.pop(member.id, None)
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

def _server_focus_channel_ids(server: dict, guild: discord.Guild) -> set[int]:
    ids = set(server['voice_channels'])
    for room_id, meta in list(temp_rooms.items()):
        if meta.get('guild_id') == guild.id:
            ids.add(room_id)
    return ids

def _channel_belongs_to_server_focus(channel, server: dict) -> bool:
    if not channel:
        return False
    report_channel = bot.get_channel(server['report_channel'])
    if report_channel and report_channel.guild.id != channel.guild.id:
        return False
    if channel.id in server['voice_channels']:
        return True
    meta = temp_rooms.get(channel.id)
    return bool(meta and meta.get('guild_id') == channel.guild.id)

async def _update_live_message_for_channel(channel):
    for server in SERVERS:
        if _channel_belongs_to_server_focus(channel, server):
            await update_live_message(server)
            break

async def update_live_message(server: dict):
    channel = bot.get_channel(server['report_channel'])
    if not channel: return
    now       = datetime.now()
    guild     = channel.guild
    voice_ids = _server_focus_channel_ids(server, guild)
    today     = now.strftime('%Y-%m-%d')
    data      = _get_live_enriched_data()
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
    for server in SERVERS:
        await update_live_message(server)

# ─── DAILY BOARD ─────────────────────────────────────────────────────────────

async def _send_daily_board(target_date: str | None = None):
    if target_date is None:
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        report_date = target_date

    data        = load_data()
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

    for server in SERVERS:
        ch = bot.get_channel(server['report_channel'])
        if ch:
            try:
                for chunk in chunks:
                    await ch.send(chunk)
                log.info(f'[DailyBoard] Gửi ngày {report_date} → #{ch.name}')
            except Exception as e:
                log.error(f'[DailyBoard] Lỗi: {e}')

# ─── REMIND SYSTEM ───────────────────────────────────────────────────────────

async def _remind_loop(member: discord.Member, hour: int):
    while True:
        try:
            now    = datetime.now()
            target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            delay = (target - now).total_seconds()
            await asyncio.sleep(delay)

            today      = datetime.now().strftime('%Y-%m-%d')
            data       = load_data()
            uid        = str(member.id)
            today_secs = data.get(uid, {}).get('daily', {}).get(today, 0)

            if today_secs == 0:
                await send_private_notify_embed(
                    member=member,
                    title='Nhắc nhở',
                    description=(
                        f'Đến giờ học lúc {hour:02d}:00.\n'
                        'Hôm nay bạn chưa học phút nào. Vào phòng thôi.'
                    ),
                    color=NOTIFY_GOLD,
                )
                await send_private_notify_embed(
                    member=member,
                    title='Động lực hôm nay',
                    description=_random_motivation_plain(),
                    color=NOTIFY_BLUE,
                )
            else:
                await send_private_notify_embed(
                    member=member,
                    title='Nhắc nhở',
                    description=(
                        f'Đến giờ học lúc {hour:02d}:00.\n'
                        f'Bạn đã học `{format_time(today_secs)}` hôm nay. Tiếp tục nhé.'
                    ),
                    color=NOTIFY_GOLD,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f'[Remind] Lỗi vòng lặp nhắc học {member.display_name}: {e}')
            await asyncio.sleep(60)

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

async def _force_stop_pomodoro_if_active(member: discord.Member, reason: str = 'rời phòng học') -> bool:
    """
    Đồng bộ với PomodoroCog để tránh tiếp tục cộng thời gian/coins khi user rời phòng học.
    Giữ logic tương đương `/pomodoro stop` nhưng không cần interaction.
    """
    pomo_cog = bot.cogs.get('PomodoroCog')
    if not pomo_cog or member.id not in getattr(pomo_cog, '_sessions', {}):
        return False

    sess = pomo_cog._sessions.get(member.id)
    if not sess:
        return False

    # Nếu đang work phase thì cộng phần đã học của vòng hiện tại.
    if sess.phase == 'work':
        elapsed_work = int((sess.work_minutes * 60) - sess.phase_remaining)
        if elapsed_work > 60:
            add_study_time(member.id, member.display_name, elapsed_work)

    # Rời group nếu có để tránh vòng kế tiếp tiếp tục tính cho member đã rời.
    if sess.group_id and sess.group_id in getattr(pomo_cog, '_groups', {}):
        grp = pomo_cog._groups[sess.group_id]
        grp.members.pop(member.id, None)
        if grp.announce_msg:
            try:
                await grp.announce_msg.edit(content=pomo_cog._build_group_embed(grp))
            except Exception:
                pass
        if member.id == grp.host.id and grp.members:
            grp.host = next(iter(grp.members.values())).member
        if not grp.members:
            pomo_cog._cancel_group(sess.group_id)

    pomo_cog._cancel_session(member.id)
    try:
        pomo_cog._update_history(member.id, sess)
    except Exception as e:
        log.error(f'Pomodoro history sync error ({member.display_name}): {e}')

    await send_private_notify_embed(
        member=member,
        title='Pomodoro đã dừng',
        description=(
            f'Pomodoro đã dừng tự động vì bạn {reason}.\n'
            'Dùng `/pomodoro start` để bắt đầu lại khi sẵn sàng.'
        ),
        color=NOTIFY_GOLD,
    )
    return True

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
    pending_checks[member.id] = asyncio.create_task(check_media(member))
    log.info(f'{member.display_name} {reason} → {WAIT_SECONDS}s countdown.')

# ─── TEMPORARY ROOM HELPERS ──────────────────────────────────────────────────

def _is_create_room_channel(channel) -> bool:
    return bool(
        CREATE_ROOM_CHANNEL_ID
        and channel
        and channel.id == CREATE_ROOM_CHANNEL_ID
    )

def _is_temporary_room_id(channel_id: int | None) -> bool:
    return bool(channel_id and channel_id in temp_rooms)

def is_focus_channel(channel_id: int | None) -> bool:
    return bool(channel_id and (channel_id in FOCUS_CHANNEL_IDS or channel_id in temp_rooms))

def _temp_room_name(member: discord.Member) -> str:
    clean_name = ' '.join(member.display_name.split()).strip() or f'User {member.id}'
    return f'📚 Phòng của {clean_name[:80]}'

def _register_temporary_room(channel: discord.VoiceChannel, owner: discord.Member):
    temp_rooms[channel.id] = {
        'room_id': channel.id,
        'owner_id': owner.id,
        'guild_id': channel.guild.id,
        'created_at': datetime.now(),
    }
    if channel.id not in FOCUS_CHANNEL_IDS:
        FOCUS_CHANNEL_IDS.append(channel.id)
    save_runtime_state()

def _remove_temporary_room_tracking(channel_id: int):
    temp_rooms.pop(channel_id, None)
    while channel_id not in STATIC_FOCUS_CHANNEL_IDS and channel_id in FOCUS_CHANNEL_IDS:
        FOCUS_CHANNEL_IDS.remove(channel_id)

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
    for member in list(getattr(channel, 'members', [])):
        if member.bot:
            continue
        try:
            await _handle_focus_leave(member, channel, reason=reason)
        except Exception as e:
            log.error(f'[TempRoom] Failed to finalize {member.display_name} before deleting {channel.id}: {e}', exc_info=True)

async def _send_temporary_room_welcome(channel, owner: discord.Member):
    embed = discord.Embed(
        title='Chào mừng đến với phòng học tạm',
        description=(
            f'Chủ phòng: **{owner.display_name}**\n\n'
            '**Hướng dẫn**\n'
            f'• Bật **Cam 📷 hoặc Stream 📺** trong **{WAIT_SECONDS}s** để bắt đầu tính giờ.\n'
            '• Bot chỉ cộng thời gian và coins khi bạn đang bật Cam hoặc Stream.\n'
            '• Phòng sẽ tự xóa sau khi không còn thành viên thật nào ở lại.\n\n'
            '**Chúc bạn học vui :3**'
        ),
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
    bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if not bot_member:
        log.error(f'[TempRoom] Cannot resolve bot member in {guild.name}')
        return False

    category = None
    if TEMP_ROOM_CATEGORY_ID:
        category = guild.get_channel(TEMP_ROOM_CATEGORY_ID)
        if not isinstance(category, discord.CategoryChannel):
            log.error(f'[TempRoom] Category {TEMP_ROOM_CATEGORY_ID} not found in {guild.name}')
            await safe_send_dm(
                member,
                'Bot không tìm thấy category chứa phòng tạm. Báo admin kiểm tra `TEMP_ROOM_CATEGORY_ID` nhé.',
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

    try:
        temp_channel = await guild.create_voice_channel(
            _temp_room_name(member),
            category=category,
            reason=f'Temporary study room for {member} ({member.id})',
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

    _register_temporary_room(temp_channel, member)

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
    pomo_cog = bot.cogs.get('PomodoroCog')
    was_in_pomo = pomo_cog is not None and member.id in getattr(pomo_cog, '_sessions', {})
    _, checkpoint_result = await _do_checkpoint(member)
    media_active_members.discard(member.id)

    await _force_stop_pomodoro_if_active(member, reason=reason)
    if checkpoint_result.get('level_up'):
        await _ensure_role_synced(member, checkpoint_result['new_level'])
    await _handle_progress_notifications(member, checkpoint_result, channel)
    duration = await record_leave_and_notify(member, force_in_pomodoro=was_in_pomo)
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
            _, result = await _do_checkpoint(member)
            if result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
        except Exception as e:
            log.error(f'[Runtime] Failed to flush {member.display_name} before {reason}: {e}', exc_info=True)
    save_runtime_state()

# ─── MEDIA CHECK ─────────────────────────────────────────────────────────────

async def check_media(member: discord.Member):
    try:
        await asyncio.sleep(WAIT_SECONDS - WARN_BEFORE_KICK)
        if not (member.voice and member.voice.channel and
                is_focus_channel(member.voice.channel.id)): return
        if is_media_active(member.voice): return
        await send_private_notify_embed(
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
            pomo_cog = bot.cogs.get('PomodoroCog')
            was_in_pomo = pomo_cog is not None and member.id in getattr(pomo_cog, '_sessions', {})
            _, checkpoint_result = await _do_checkpoint(member)
            media_active_members.discard(member.id)
            await _force_stop_pomodoro_if_active(member, reason='bị kick (không bật camera/stream)')
            if checkpoint_result.get('level_up'):
                await _ensure_role_synced(member, checkpoint_result['new_level'])
            await _handle_progress_notifications(member, checkpoint_result)
            await record_leave_and_notify(member, force_in_pomodoro=was_in_pomo)
            await member.move_to(None)
            await send_private_notify_embed(
                member=member,
                title='Đã rời phòng',
                description=(
                    'Bạn đã bị kick vì chưa bật Cam hoặc Stream.\n'
                    'Hãy bật Cam hoặc Stream khi vào lại.'
                ),
                color=NOTIFY_RED,
            )
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
        await _check_absences()
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

    pomo_cog      = bot.cogs.get('PomodoroCog')
    pomo_sessions = pomo_cog._sessions if pomo_cog else {}
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
            media_active_members.discard(mid)
            reset_cam_notification(mid)
            save_runtime_state()
            continue

        was_active = mid in media_active_members
        now_active = bool(member.voice and is_media_active(member.voice))
        in_pomo    = mid in pomo_sessions
        
        if was_active:
            elapsed, result = await _do_checkpoint(member)
            if result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
            await _handle_progress_notifications(member, result, member.voice.channel)
            await _check_quests_and_badges(member)
        
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

async def _send_report():
    data        = load_data()
    today       = datetime.now().strftime('%Y-%m-%d')
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
    for server in SERVERS:
        ch = bot.get_channel(server['report_channel'])
        if ch:
            for chunk in chunks:
                await ch.send(chunk)

async def _check_absences():
    today     = datetime.now().strftime('%Y-%m-%d')
    warn_date = (datetime.now() - timedelta(days=ABSENT_DAYS_WARN)).strftime('%Y-%m-%d')

    def claim_absence_warnings(current: dict):
        warnings = []
        for uid, info in current.items():
            last_date   = info.get('last_study_date', '')
            last_warned = info.get('last_absent_warn', '')
            if not last_date or last_date >= warn_date or last_warned == today:
                continue
            try:
                member_id = int(uid)
                last_dt = datetime.strptime(last_date, '%Y-%m-%d')
            except (ValueError, TypeError):
                continue
            member = _get_cached_member(member_id)
            if not member:
                continue
            info['last_absent_warn'] = today
            warnings.append((member, last_dt, info.get('streak', 0)))
        return warnings

    warnings, _ = update_data(claim_absence_warnings)
    for member, last_dt, streak in warnings:
        days = (datetime.now() - last_dt).days
        await send_private_notify_embed(
            member=member,
            title='Nhắc nhở',
            description=(
                f'Bạn đã không học trong **{days} ngày**.\n'
                f'Streak hiện tại: `{streak} ngày`. Vào phòng để giữ nhịp học nhé.'
            ),
            color=NOTIFY_GOLD,
        )

async def _check_overdue_loan_notifications():
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
        member = _get_cached_member(member_id)
        if member:
            await notify_overdue_loans(member)

async def _sync_member_progress(member: discord.Member, previous_level: int | None = None):
    if (
        member.id in join_times
        and member.id in media_active_members
        and _get_pomodoro_session(member.id) is None
        and _last_data_save_success
    ):
        last_checkpoint[member.id] = datetime.now()
        save_runtime_state()
    await _check_quests_and_badges(member)
    if previous_level is None:
        return
    info = load_data().get(str(member.id), {})
    current_level = info.get('class', info.get('level', 0))
    await _handle_progress_notifications(
        member,
        {'level_up': current_level > previous_level, 'new_level': current_level},
    )

# ─── AI ──────────────────────────────────────────────────────────────────────

AI_SYSTEM_PROMPT = (
    'Bạn là trợ lý học tập trong Discord. '
    'Trả lời thông minh, ngắn gọn nhưng đủ ý. '
        'Không viết lan man, không mở bài dài, không lặp lại câu hỏi. '
    'Nếu câu hỏi đơn giản, trả lời trong 2-4 câu. '
    'Nếu câu hỏi cần giải thích, dùng tối đa 3-6 bullet points. '
    'Ưu tiên cấu trúc: định nghĩa ngắn → ý chính → ví dụ/công thức nếu cần → kết luận. '
    f'Câu trả lời phải nằm trong một tin nhắn Discord, khoảng dưới {AI_ONE_MESSAGE_LIMIT} ký tự.'
)
AI_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class AIProviderError(Exception):
    def __init__(self, provider: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.provider = provider
        self.retryable = retryable


def _split_env_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(',') if item.strip()]


def smart_cut_at_sentence(text: str, limit: int = AI_ONE_MESSAGE_LIMIT) -> str:
    text = (text or '').strip()
    if len(text) <= limit:
        return text or '❌ AI không trả về nội dung.'

    suffix = '\n\n_(rút gọn)_'
    safe_limit = max(1, limit - len(suffix))
    cut = text[:safe_limit]
    last_sentence = max(
        cut.rfind('. '),
        cut.rfind('! '),
        cut.rfind('? '),
        cut.rfind('。'),
        cut.rfind('\n'),
    )
    if last_sentence > safe_limit * 0.6:
        return cut[:last_sentence + 1].strip() + suffix
    return cut.rstrip(' ,;:-') + '...' + suffix


def _openai_compatible_token_limit(provider: dict) -> dict:
    token_param = provider.get('max_token_param', 'max_tokens')
    return {token_param: AI_MAX_OUTPUT_TOKENS}


def _configured_ai_providers() -> list[dict]:
    providers: list[dict] = []
    if GEMINI_API_KEY:
        providers.extend([
            {'kind': 'gemini', 'name': 'Gemini 2.5 Flash', 'model': GEMINI_FLASH_MODEL, 'api_key': GEMINI_API_KEY},
            {'kind': 'gemini', 'name': 'Gemini 2.5 Flash-Lite', 'model': GEMINI_FLASH_LITE_MODEL, 'api_key': GEMINI_API_KEY},
        ])
    if GROQ_API_KEY:
        for model in _split_env_list(GROQ_MODELS):
            providers.append({
                'kind': 'openai_compatible',
                'name': f'Groq {model}',
                'model': model,
                'api_key': GROQ_API_KEY,
                'base_url': 'https://api.groq.com/openai/v1',
                'max_token_param': 'max_completion_tokens',
            })
    if OPENROUTER_API_KEY:
        providers.append({
            'kind': 'openai_compatible',
            'name': f'OpenRouter {OPENROUTER_MODEL}',
            'model': OPENROUTER_MODEL,
            'api_key': OPENROUTER_API_KEY,
            'base_url': 'https://openrouter.ai/api/v1',
            'max_token_param': 'max_tokens',
            'headers': {
                'HTTP-Referer': OPENROUTER_REFERER,
                'X-Title': OPENROUTER_TITLE,
            },
        })
    if HUGGINGFACE_API_KEY:
        providers.append({
            'kind': 'openai_compatible',
            'name': f'Hugging Face {HUGGINGFACE_MODEL}',
            'model': HUGGINGFACE_MODEL,
            'api_key': HUGGINGFACE_API_KEY,
            'base_url': 'https://router.huggingface.co/v1',
            'max_token_param': 'max_tokens',
        })
    return providers


async def _post_ai_json(client: httpx.AsyncClient, provider: str, url: str, headers: dict, payload: dict):
    try:
        response = await client.post(url, headers=headers, json=payload)
    except (httpx.TimeoutException, httpx.RequestError) as e:
        raise AIProviderError(provider, f'{type(e).__name__}: {e}', retryable=True) from e

    if response.status_code in AI_RETRYABLE_STATUS_CODES:
        raise AIProviderError(provider, f'HTTP {response.status_code}: {response.text[:300]}', retryable=True)
    if response.status_code >= 400:
        raise AIProviderError(provider, f'HTTP {response.status_code}: {response.text[:300]}')

    try:
        return response.json()
    except ValueError as e:
        raise AIProviderError(provider, 'Phản hồi AI không phải JSON hợp lệ.') from e


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
    raise AIProviderError(provider, 'Gemini không trả về nội dung văn bản.')


def _extract_openai_compatible_text(provider: str, payload: dict) -> str:
    choices = payload.get('choices') or []
    if not choices:
        raise AIProviderError(provider, 'Provider không trả về lựa chọn phản hồi.')

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
        raise AIProviderError(provider, 'Provider trả về phản hồi rỗng.')
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
        raise AIProviderError(provider, 'Hugging Face trả về phản hồi rỗng.')
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
            'temperature': 0.5,
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
    async with httpx.AsyncClient(timeout=AI_HTTP_TIMEOUT) as client:
        for provider in providers:
            try:
                answer = await _call_ai_provider(client, provider, question)
                return answer.strip() or '❌ AI không trả về nội dung.'
            except AIProviderError as e:
                errors.append(e)
                level = logging.WARNING if e.retryable else logging.ERROR
                log.log(level, f'AI provider failed ({e.provider}, retryable={e.retryable}): {e}')
            except Exception as e:
                wrapped = AIProviderError(provider['name'], str(e))
                errors.append(wrapped)
                log.exception(f'Unexpected AI error from {provider["name"]}')

    retryable_count = sum(1 for e in errors if e.retryable)
    log.error(f'All AI providers failed ({retryable_count}/{len(errors)} retryable failures).')
    return '❌ Lỗi AI. Tất cả provider hiện chưa phản hồi được, thử lại sau nhé!'


async def _compact_ai_answer(question: str, answer: str, limit: int = AI_ONE_MESSAGE_LIMIT) -> str:
    compact_prompt = (
        f'Rút gọn câu trả lời sau xuống tối đa {limit} ký tự. '
        'Vẫn phải đủ ý chính, rõ ràng, dễ hiểu. '
        'Không thêm mở bài. Không nói rằng bạn đang rút gọn. '
        'Dùng tối đa 3-5 bullet points nếu cần.\n\n'
        f'Câu hỏi: {question}\n\n'
        f'Câu trả lời cần rút gọn:\n{answer}'
    )
    compacted = await _ask_ai_raw(compact_prompt)
    return compacted.strip()


async def _ask_ai(question: str) -> str:
    answer = (await _ask_ai_raw(question)).strip()
    if len(answer) <= AI_ONE_MESSAGE_LIMIT:
        return answer or '❌ AI không trả về nội dung.'

    compacted = await _compact_ai_answer(question, answer, AI_ONE_MESSAGE_LIMIT)
    if compacted.startswith('❌'):
        return smart_cut_at_sentence(answer, AI_ONE_MESSAGE_LIMIT)
    if len(compacted) <= AI_ONE_MESSAGE_LIMIT:
        return compacted or '❌ AI không trả về nội dung.'
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
    role_name = _get_level_role_name(LEVEL_ROLES.get(class_idx), target.guild)
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
        _build_rank_message(target, _get_live_enriched_data()),
        ephemeral=is_self
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

    data = _get_live_enriched_data()

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
    data   = _get_live_enriched_data()
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
    card_bytes = await asyncio.to_thread(generate_profile_card, target.id)
    if not card_bytes:
        await interaction.followup.send(
            f'❌ **{target.display_name}** chưa có dữ liệu học tập!', ephemeral=True
        ); return
    file = discord.File(io.BytesIO(card_bytes), filename=f'card_{target.display_name}.png')
    await interaction.followup.send(f'📸 **Profile card của {target.display_name}**', file=file)

# ── /stats ─────────────────────────────────────────────────────────────────

@bot.tree.command(name='stats', description='Xem thống kê học tập chi tiết')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_stats(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer(ephemeral=True)
    target = member or interaction.user
    uid    = str(target.id)
    if target.id == interaction.user.id:
        raw = load_data()
        if uid not in raw:
            def ensure_user(data: dict):
                data.setdefault(uid, _default_user(interaction.user.display_name))

            update_data(ensure_user)
    data   = _get_live_enriched_data()
    if uid not in data:
        await interaction.followup.send(
            f'❌ **{target.display_name}** chưa có dữ liệu!', ephemeral=True
        ); return
    info        = data[uid]
    today       = datetime.now().strftime('%Y-%m-%d')
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
    remind_h   = info.get('remind_hour')
    msg = (
        f'📊 **Thống kê của {target.display_name}**\n'
        f'🏛️ `{class_label(class_idx)}` | Total earned `{format_coins(total_earned)}` _(còn {format_coins(coins_need)})_\n'
        f'💵 Balance: `{format_coins(balance)}` | Debt: `{format_coins(debt)}` | Net worth: `{format_coins(balance - debt)}`\n'
        f'🪙 Earned hôm nay: `{format_coins(today_earned)}`\n'
        f'🔥 Streak: `{streak} ngày` _(kỷ lục: {info.get("longest_streak",0)})_\n'
        f'🕐 Hôm nay: `{format_time(today_saved)}`\n'
        f'📚 Tổng: `{format_time(info.get("total",0))}`\n'
        f'🏅 Huy hiệu: `{len(badges)}/{len(BADGES)}`\n'
    )
    if goal and goal_secs > 0:
        pct = min(100, int((today_saved / goal_secs) * 100))
        msg += f'🎯 **{goal}**: `{pct}%`\n'
    if remind_h is not None:
        msg += f'⏰ Nhắc học: `{remind_h:02d}:00` hàng ngày\n'
    msg += f'📅 7 ngày:\n{recent_str}\n'
    msg += f'\n_Dùng `/card` để tạo ảnh profile!_'
    await interaction.followup.send(msg, ephemeral=True)

# ── /leaderboard ───────────────────────────────────────────────────────────

@bot.tree.command(name='leaderboard', description='Bảng xếp hạng hôm nay')
async def slash_leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()
    data  = _get_live_enriched_data()
    today = datetime.now().strftime('%Y-%m-%d')
    now   = datetime.now()

    def real_time(uid_str: str, info: dict) -> int:
        s = info['daily'].get(today, 0)
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

    lines = [f'🏆 **Bảng xếp hạng hôm nay** _{today}_\n']
    if not top10:
        lines.append('😴 Chưa có ai học hôm nay!')
    else:
        for i, (uid, info, rt) in enumerate(top10, 1):
            medal  = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
            try:
                is_live = int(uid) in join_times
            except (ValueError, TypeError):
                is_live = False
            active = ' 🟢' if is_live else ''
            class_idx = info.get('class', info.get('level', 0))
            earned = info.get('daily_earnings', {}).get(today, 0)
            debt = _active_debt(info)
            lines.append(
                f'{medal}{active} **{info["name"]}** `{class_label(class_idx)}` '
                f'🔥{info.get("streak",0)} — `{format_time(rt)}`\n'
                f'       💰 Today `{format_coins(earned)}` · Total `{format_coins(info.get("total_earned",0))}`'
                f' · Balance `{format_coins(info.get("balance",0))}` · Debt `{format_coins(debt)}`'
            )
    await interaction.followup.send('\n'.join(lines))

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

@bot.tree.command(name='balance', description='Xem ví coins của bạn hoặc thành viên khác')
@app_commands.describe(member='Thành viên (để trống = bản thân)')
async def slash_balance(interaction: discord.Interaction, member: discord.Member = None):
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

    if uid not in data:
        await interaction.followup.send(f'❌ **{target.display_name}** chưa có ví.', ephemeral=is_self)
        return

    info = data[uid]
    debt = _active_debt(info)
    class_idx = info.get('class', info.get('level', 0))
    msg = (
        f'💼 **Ví của {target.display_name}**\n'
        f'💵 Balance: `{format_coins(info.get("balance", 0))}`\n'
        f'💰 Total earned: `{format_coins(info.get("total_earned", 0))}`\n'
        f'🏛️ Class: `{class_label(class_idx)}`\n'
        f'💳 Debt: `{format_coins(debt)}` · Net worth: `{format_coins(info.get("balance", 0) - debt)}`\n'
        f'⭐ Credit score: `{_credit_score(info)}`'
    )
    await interaction.followup.send(msg, ephemeral=is_self)


@bot.tree.command(name='pay', description='Chuyển coins ảo cho thành viên khác')
@app_commands.describe(member='Người nhận', amount='Số coins muốn chuyển')
async def slash_pay(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 1_000_000_000],
):
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


@economy_group.command(name='adjust', description='[Admin] Điều chỉnh balance và ghi transaction')
@app_commands.default_permissions(administrator=True)
@app_commands.describe(member='Thành viên cần điều chỉnh', amount='Số coins (+/-)', reason='Lý do điều chỉnh')
async def economy_adjust(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: int,
    reason: str = 'Admin adjustment',
):
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
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)

    def ensure_self(data: dict):
        _ensure_account(data, uid, interaction.user.display_name)

    _, data = update_data(ensure_self)
    info = data[uid]
    loans = _active_loans(info)
    lines = [
        f'💳 **Loan status — {interaction.user.display_name}**',
        f'Debt: `{format_coins(_active_debt(info))}` · Balance: `{format_coins(info.get("balance", 0))}` · Credit score: `{_credit_score(info)}`',
    ]
    if not loans:
        lines.append('\n✅ Không có khoản vay active.')
    else:
        lines.append('\n**Khoản vay active:**')
        lines.extend(_loan_line(loan) for loan in loans)
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

notify_group = app_commands.Group(name='notify', description='Cài đặt thông báo riêng')

@notify_group.command(name='on', description='Bật thông báo riêng từ bot')
async def notify_on(interaction: discord.Interaction):
    display_name = getattr(interaction.user, 'display_name', getattr(interaction.user, 'name', 'Unknown'))
    set_notifications_enabled(interaction.user.id, True, display_name)
    await interaction.response.send_message('Đã bật thông báo riêng.', ephemeral=True)

@notify_group.command(name='off', description='Tắt thông báo riêng từ bot')
async def notify_off(interaction: discord.Interaction):
    display_name = getattr(interaction.user, 'display_name', getattr(interaction.user, 'name', 'Unknown'))
    set_notifications_enabled(interaction.user.id, False, display_name)
    await interaction.response.send_message(
        'Đã tắt thông báo riêng. Bot sẽ không gửi DM thông báo tự động cho bạn.',
        ephemeral=True,
    )

@notify_group.command(name='status', description='Xem trạng thái thông báo riêng')
async def notify_status(interaction: discord.Interaction):
    enabled = notifications_enabled_for(interaction.user.id)
    await interaction.response.send_message(
        f"Thông báo riêng hiện đang: **{'Bật' if enabled else 'Tắt'}**",
        ephemeral=True,
    )

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
    data  = _get_live_enriched_data()
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

# ── /remind ────────────────────────────────────────────────────────────────

@bot.tree.command(name='remind', description='Đặt giờ nhắc học hàng ngày qua DM (-1 để tắt)')
@app_commands.describe(hour='Giờ nhắc (0-23), nhập -1 để tắt')
async def slash_remind(interaction: discord.Interaction, hour: app_commands.Range[int, -1, 23]):
    await interaction.response.defer(ephemeral=True)
    uid  = str(interaction.user.id)

    if hour == -1:
        old = remind_tasks.pop(interaction.user.id, None)
        if old:
            task = old[1]
            if task and not task.done(): task.cancel()

        def disable_remind(data: dict):
            if uid in data:
                data[uid]['remind_hour'] = None

        update_data(disable_remind)
        await interaction.followup.send(
            '🔕 Đã tắt nhắc học.\n_Dùng `/remind <giờ>` để bật lại._', ephemeral=True
        )
        return

    def enable_remind(data: dict):
        if uid not in data:
            data[uid] = _default_user(interaction.user.display_name)
        data[uid]['remind_hour'] = hour

    update_data(enable_remind)

    old = remind_tasks.pop(interaction.user.id, None)
    if old:
        task = old[1]
        if task and not task.done(): task.cancel()

    t = asyncio.create_task(_remind_loop(interaction.user, hour))
    remind_tasks[interaction.user.id] = (hour, t)

    await interaction.followup.send(
        f'⏰ Đã đặt nhắc học lúc **{hour:02d}:00** mỗi ngày!\n'
        f'Bot sẽ DM bạn nhắc nhở và động lực. 💪\n'
        f'_Tắt: `/remind -1`_',
        ephemeral=True
    )

# ── /ask ───────────────────────────────────────────────────────────────────

@bot.tree.command(name='ask', description='Hỏi AI học tập')
@app_commands.describe(question='Câu hỏi của bạn')
async def slash_ask(interaction: discord.Interaction, question: str):
    await interaction.response.defer(thinking=True)
    answer = await _ask_ai(question)
    await interaction.followup.send(answer)

# ── /room_panel ────────────────────────────────────────────────────────────

def _resolve_room_control_channel(interaction: discord.Interaction) -> tuple[discord.VoiceChannel | None, str | None]:
    if not interaction.guild:
        return None, '❌ Chức năng này chỉ dùng được trong server.'

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not member or not member.voice or not isinstance(member.voice.channel, discord.VoiceChannel):
        return None, '❌ Bạn cần ở trong phòng học tạm để dùng điều khiển phòng.'

    channel = member.voice.channel
    if not _is_temporary_room_id(channel.id):
        return None, '❌ Điều khiển này chỉ áp dụng cho phòng tạm do bot tạo.'

    meta = temp_rooms.get(channel.id, {})
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


async def _call_pomodoro_command(
    interaction: discord.Interaction,
    command_name: str,
    *args,
):
    pomo_cog = bot.cogs.get('PomodoroCog')
    command = getattr(pomo_cog, command_name, None) if pomo_cog else None
    callback = getattr(command, 'callback', None)
    if not callback:
        await interaction.response.send_message('❌ Pomodoro chưa sẵn sàng. Thử lại sau nhé.', ephemeral=True)
        return

    try:
        await callback(pomo_cog, interaction, *args)
    except Exception as e:
        log.error(f'[RoomPanel] Pomodoro button failed ({command_name}): {e}', exc_info=True)
        message = '❌ Không thể xử lý Pomodoro lúc này. Thử lại sau nhé.'
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)


class _SilentPomodoroChannel:
    async def send(self, *args, **kwargs):
        return None


async def _panel_pomodoro_start(interaction: discord.Interaction):
    pomo_cog = bot.cogs.get('PomodoroCog')
    if not pomo_cog:
        await interaction.response.send_message('❌ Pomodoro chưa sẵn sàng. Thử lại sau nhé.', ephemeral=True)
        return

    member = interaction.user
    if not isinstance(member, discord.Member):
        await interaction.response.send_message('❌ Pomodoro trong dashboard chỉ dùng được trong server.', ephemeral=True)
        return

    sess = getattr(pomo_cog, '_sessions', {}).get(member.id)
    if sess and getattr(sess, 'group_id', None):
        await _call_pomodoro_command(interaction, 'pomo_start', 25, 5, 4)
        return
    if sess:
        await interaction.response.send_message(
            f'⚠️ Bạn đang có phiên Pomodoro chạy rồi!\n'
            f'Phase: `{sess.phase.upper()}` | Còn lại: `{sess.format_remaining()}`\n'
            f'Dùng nút **Dừng** trước khi bắt đầu phiên mới.',
            ephemeral=True,
        )
        return

    work, break_, rounds = 25, 5, 4
    sess = PomodoroSession(
        member=member,
        work_minutes=work,
        break_minutes=break_,
        total_rounds=rounds,
        channel=_SilentPomodoroChannel(),
        phase_end=datetime.now() + timedelta(minutes=work),
    )
    pomo_cog._sessions[member.id] = sess

    await interaction.response.send_message(
        f'🍅 Phiên Pomodoro đã bắt đầu! `{work}m làm / {break_}m nghỉ × {rounds} vòng`\n'
        f'Dùng nút **Trạng thái** để xem tiến độ.',
        ephemeral=True,
    )
    await pomo_cog._send_dm(
        member,
        f'🍅 **Pomodoro bắt đầu!**\n'
        f'⏱️ Làm việc: `{work} phút` × `{rounds} vòng`\n'
        f'☕ Nghỉ: `{break_} phút` giữa mỗi vòng\n'
        f'Tập trung nào! Tắt điện thoại, đóng tab thừa. 💪',
    )
    sess.task = asyncio.create_task(pomo_cog._run_personal(sess))


async def _panel_pomodoro_stop(interaction: discord.Interaction):
    await _call_pomodoro_command(interaction, 'pomo_stop')


async def _panel_pomodoro_status(interaction: discord.Interaction):
    await _call_pomodoro_command(interaction, 'pomo_status')


async def _panel_show_balance(interaction: discord.Interaction):
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

    @discord.ui.button(label='Bắt đầu', emoji='🍅', style=discord.ButtonStyle.success, row=1, custom_id='pomo_start')
    async def pomo_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _panel_pomodoro_start(interaction)

    @discord.ui.button(label='Dừng', emoji='⏹️', style=discord.ButtonStyle.secondary, row=1, custom_id='pomo_stop')
    async def pomo_stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _panel_pomodoro_stop(interaction)

    @discord.ui.button(label='Trạng thái', emoji='📊', style=discord.ButtonStyle.secondary, row=1, custom_id='pomo_status')
    async def pomo_status(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _panel_pomodoro_status(interaction)

    @discord.ui.button(label='Ví tiền', emoji='💰', style=discord.ButtonStyle.primary, row=2, custom_id='eco_balance')
    async def eco_balance(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _panel_show_balance(interaction)

    @discord.ui.button(label='Vay', emoji='🏦', style=discord.ButtonStyle.primary, row=2, custom_id='eco_borrow')
    async def eco_borrow(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(BorrowModal())

    @discord.ui.button(label='Trả nợ', emoji='💳', style=discord.ButtonStyle.secondary, row=2, custom_id='eco_repay')
    async def eco_repay(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RepayModal())

    @discord.ui.button(label='Cho vay', emoji='🤝', style=discord.ButtonStyle.success, row=2, custom_id='eco_lend')
    async def eco_lend(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LendModal())

    @discord.ui.button(label='Nợ/Vay', emoji='📜', style=discord.ButtonStyle.secondary, row=2, custom_id='loan_status')
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
                '**🍅 Pomodoro**\n'
                '🍅 Bắt đầu · ⏹️ Dừng · 📊 Trạng thái\n\n'
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
    for lv, role_id in LEVEL_ROLES.items():
        if role_id is None:
            continue
        role_name = _get_level_role_name(role_id, guild) or f'Unknown ({role_id})'
        coins_req = CLASS_THRESHOLDS[lv]
        is_mine = (lv == my_lv)
        is_done = (my_lv > lv)
        status  = ' ◀ **bạn đây**' if is_mine else (' ✅' if is_done else '')
        icon    = '✦' if is_mine else ('✔' if is_done else '○')
        lines.append(f'{icon} **{class_label(lv)}** `{format_coins(coins_req)}` → **{role_name}**{status}')
    await interaction.followup.send('\n'.join(lines), ephemeral=True)

# ── /help ──────────────────────────────────────────────────────────────────

@bot.tree.command(name='help', description='Danh sách tất cả lệnh của bot')
async def slash_help(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    msg = (
        '📚 **STUDY BOT — DANH SÁCH LỆNH**\n'
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n'
        '**📊 Thống kê cá nhân**\n'
        '`/rank [member]` — Ví, class và thống kê\n'
        '`/stats [member]` — Thống kê chi tiết 7 ngày\n'
        '`/card [member]` — Tạo ảnh profile card\n'
        '`/badges [member]` — Xem huy hiệu\n\n'
        '**💰 Economy**\n'
        '`/balance [member]` — Xem balance, total earned, debt\n'
        '`/pay <member> <amount>` — Chuyển coins ảo\n'
        '`/economy leaderboard` — Top total earned\n'
        '`/transactions [limit]` — Lịch sử giao dịch\n'
        '`/loan borrow|repay|status|offer|accept|cancel|history` — Vay/cho vay coins ảo\n\n'
        '**🏆 Xếp hạng**\n'
        '`/leaderboard` — Top hôm nay\n'
        '`/top_alltime` — Top tổng thời gian\n'
        '`/studying` — Ai đang học ngay lúc này\n\n'
        '**🎮 Gamification**\n'
        '`/quest` — Nhiệm vụ hôm nay\n'
        '`/setgoal <mô tả> [hours] [minutes]` — Đặt mục tiêu\n\n'
        '**⏰ Tiện ích**\n'
        '`/remind <hour>` — Nhắc học hàng ngày (0-23, -1 để tắt)\n'
        '`/ask <câu hỏi>` — Hỏi AI học tập\n'
        '`/roles` — Xem vai trò theo money class\n\n'
        '**🍅 Pomodoro**\n'
        '`/pomodoro start` — Bắt đầu phiên cá nhân\n'
        '`/pomodoro stop` — Dừng phiên\n'
        '`/pomodoro status` — Xem tiến độ\n'
        '`/pomodoro create <tên>` — Tạo phòng nhóm\n'
        '`/pomodoro join <tên>` — Tham gia phòng nhóm\n'
        '`/pomodoro leave` — Rời phòng nhóm\n'
        '`/pomodoro list` — Xem danh sách phòng nhóm\n'
        '`/pomodoro stats` — Lịch sử Pomodoro\n'
        '`/pomodoro preset` — Lưu cấu hình yêu thích\n\n'
        '**📅 Báo cáo tuần**\n'
        '`/weekly preview` — Xem trước báo cáo tuần\n'
        '`/weekly on/off` — Bật/tắt báo cáo tuần\n'
        '`/weekly status` — Trạng thái báo cáo\n'
        '`/weekly leaderboard` — Top học nhiều nhất tuần này\n'
        '`/weekly compare` — So sánh tuần này vs tuần trước\n\n'
        '**⚙️ Admin**\n'
        '`/syncroles` · `/report` · `/dailyboard` · `/updatelive` · `/backup` · `/economy adjust`\n'
    )
    await interaction.followup.send(msg, ephemeral=True)

# ── Admin commands ─────────────────────────────────────────────────────────

@bot.tree.command(name='syncroles', description='Đồng bộ vai trò (Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_syncroles(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send('❌ Lệnh này chỉ dùng được trong server!', ephemeral=True)
        return
    data    = load_data()
    updated = 0
    skipped = 0
    members_to_sync: list[tuple[discord.Member, int]] = []
    for uid, info in data.items():
        try:
            member_id = int(uid)
        except (ValueError, TypeError):
            continue
        m = guild.get_member(member_id) or await _fetch_member_from_guild(guild, member_id)
        if not m:
            skipped += 1
            continue
        members_to_sync.append((m, info.get('class', info.get('level', 0))))
        updated += 1

    batches = list(_iter_chunks(members_to_sync, ROLE_SYNC_BATCH_SIZE))
    for idx, batch in enumerate(batches):
        await asyncio.gather(
            *[_ensure_role_synced(m, lv) for m, lv in batch],
            return_exceptions=True,
        )
        if idx < len(batches) - 1:
            await asyncio.sleep(ROLE_SYNC_BATCH_DELAY)

    msg = f'✅ Đã sync **{updated}** thành viên.'
    if skipped:
        msg += f' ⚠️ Bỏ qua **{skipped}** (không tìm thấy trong server).'
    await interaction.followup.send(msg, ephemeral=True)

@bot.tree.command(name='report', description='Gửi báo cáo ngay (Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_report(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _send_report()
    await interaction.followup.send('✅ Đã gửi báo cáo!', ephemeral=True)

@bot.tree.command(name='dailyboard', description='Gửi bảng tổng kết ngày (Admin)')
@app_commands.default_permissions(administrator=True)
@app_commands.describe(date='Ngày cần báo cáo YYYY-MM-DD (để trống = hôm qua)')
async def slash_dailyboard(interaction: discord.Interaction, date: str = None):
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
    await _send_daily_board(date)
    await interaction.followup.send('✅ Đã gửi bảng tổng kết ngày!', ephemeral=True)

@bot.tree.command(name='updatelive', description='Cập nhật live message (Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_updatelive(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await update_all_live_messages()
    await interaction.followup.send('✅ Đã cập nhật!', ephemeral=True)

@bot.tree.command(name='backup', description='Backup dữ liệu ngay lập tức (Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_backup(interaction: discord.Interaction):
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
    data  = _get_live_enriched_data()
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
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            total += len(synced)
        except Exception as e:
            log.error(f'Sync error {guild.name}: {e}')
    await msg.edit(content=f'✅ Sync xong! **{total}** lệnh.')

@bot.command(name='report')
@commands.has_permissions(administrator=True)
async def cmd_report(ctx):
    await _send_report()
    await ctx.send('✅ Đã gửi báo cáo!')

# ─── EVENTS ──────────────────────────────────────────────────────────────────

def extract_question_from_mention(message: discord.Message) -> str:
    if not bot.user:
        return ''

    raw = message.content
    for mention in (f'<@{bot.user.id}>', f'<@!{bot.user.id}>'):
        raw = raw.replace(mention, '')
    return raw.strip()

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    try:
        mentioned_bot = bot.user is not None and bot.user in message.mentions
        if mentioned_bot:
            question = extract_question_from_mention(message)
            if not question:
                await message.reply(
                    'Bạn hãy tag bot kèm câu hỏi nhé. Ví dụ: `@bot giải thích Markov chain`',
                    mention_author=False,
                )
            else:
                async with message.channel.typing():
                    answer = await _ask_ai(question)
                await message.reply(answer, mention_author=False)
    finally:
        await bot.process_commands(message)

@bot.event
async def on_ready():
    log.info(f'✅ Bot {bot.user.name} ready!')
    if not join_times:
        restore_runtime_state()

    global _room_panel_view_registered, _startup_extensions_ready
    if not _startup_extensions_ready:
        if not _room_panel_view_registered:
            bot.add_view(RoomPanelView())
            _room_panel_view_registered = True

        if not bot.cogs.get('PomodoroCog'):
            try:
                cog = create_pomodoro_cog(
                    bot, add_study_time, safe_send_dm, format_time,
                    load_data_fn=load_data, save_data_fn=save_data,
                    add_xp_fn=add_xp_direct, update_data_fn=update_data,
                    progress_sync_fn=_sync_member_progress,
                )
                await bot.add_cog(cog)
                log.info('✅ Pomodoro Cog loaded')
            except Exception as e:
                log.error(f'Pomodoro error: {e}', exc_info=True)

        try:
            await setup_weekly_report(
                bot, load_data, save_data, BADGES, safe_send_dm,
                update_data_fn=update_data,
                class_thresholds=CLASS_THRESHOLDS,
                class_names=CLASS_NAMES,
            )
        except Exception as e:
            log.error(f'WeeklyReport error: {e}', exc_info=True)

        _startup_extensions_ready = (
            bot.cogs.get('PomodoroCog') is not None
            and bot.cogs.get('WeeklyReport') is not None
        )
    else:
        log.info('[Startup] Extension setup already completed; skipping.')

    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            log.info(f'Sync {len(synced)} cmds → {guild.name}')
        except Exception as e:
            log.error(f'Sync error {guild.name}: {e}')

    if not scheduled_tasks.is_running():  scheduled_tasks.start()
    if not checkpoint_task.is_running():  checkpoint_task.start()

    global _dashboard_started
    if not _dashboard_started:
        _dashboard_started = True
        threading.Thread(target=run_dashboard, daemon=True).start()
    log.info(f'🌐 Dashboard: http://localhost:{DASHBOARD_PORT}')

    # Restore remind tasks and sync roles on recovery
    data = load_data()
    members_to_sync: list[tuple[discord.Member, int]] = []
    for uid, info in data.items():
        try:
            mid = int(uid)
        except (ValueError, TypeError):
            continue
        
        # Restore reminders
        remind_h = info.get('remind_hour')
        if remind_h is not None:
            await _cancel_reminder_task(mid)
            for guild in bot.guilds:
                m = guild.get_member(mid)
                if m:
                    if mid in remind_tasks:
                        break
                    t = asyncio.create_task(_remind_loop(m, remind_h))
                    remind_tasks[mid] = (remind_h, t)
                    log.info(f'[Remind] Khôi phục: {info["name"]} lúc {remind_h:02d}:00')
                    break
        
        # Collect members for batched role sync
        level = info.get('class', info.get('level', 0))
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m:
                members_to_sync.append((m, level))
                break

    async def _safe_sync(member: discord.Member, level: int):
        try:
            await _ensure_role_synced(member, level)
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
    for ch_id in FOCUS_CHANNEL_IDS:
        ch = bot.get_channel(ch_id)
        if ch:
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
            media_active_members.discard(mid)
            continue
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

    for room_id in list(temp_rooms.keys()):
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
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    joined_create_room = (
        _is_create_room_channel(after.channel)
        and (not before.channel or before.channel.id != after.channel.id)
    )
    joined_focus    = after.channel  and is_focus_channel(after.channel.id)
    left_focus      = before.channel and is_focus_channel(before.channel.id)
    stayed_in_focus = joined_focus and left_focus
    moved_channels  = before.channel and after.channel and before.channel.id != after.channel.id

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
        pomo_cog = bot.cogs.get('PomodoroCog')
        in_pomodoro = pomo_cog is not None and member.id in getattr(pomo_cog, '_sessions', {})
        if not was_active and now_active:
            cancel_task(member.id)
            media_active_members.add(member.id)
            if member.id not in join_times:
                record_join(member)
            else:
                last_checkpoint[member.id] = datetime.now()
                save_runtime_state()
            log.info(f'{member.display_name} bật Cam/Stream → bắt đầu tính giờ từ bây giờ')
            await notify_cam_started(member, after.channel)
        elif was_active and not now_active:
            if in_pomodoro:
                current_info = load_data().get(str(member.id), {})
                previous_level = current_info.get('class', current_info.get('level', 0))
                await _do_checkpoint(member)
                await _force_stop_pomodoro_if_active(member, reason='tắt Cam/Stream')
                media_active_members.discard(member.id)
                save_runtime_state()
                await _sync_member_progress(member, previous_level)
            else:
                elapsed, result = await _do_checkpoint(member)
                if result.get('level_up'):
                    await _ensure_role_synced(member, result['new_level'])
                await _handle_progress_notifications(member, result, after.channel)
                await _check_quests_and_badges(member)
                media_active_members.discard(member.id)
                save_runtime_state()
            start_check(member, 'tắt Cam/Stream')
            await send_private_notify_embed(
                member=member,
                title='Nhắc nhở',
                description=(
                    f'Bạn cần bật lại Cam hoặc Stream trong {WAIT_SECONDS}s '
                    'để tiếp tục ở lại phòng.'
                ),
                color=NOTIFY_GOLD,
            )

    elif joined_focus and not stayed_in_focus:
        record_join(member)
        await send_private_notify_embed(
            member=member,
            title='Chào mừng',
            description=(
                f'Chào mừng {member.display_name} vào phòng học.\n'
                f'Nhớ bật Cam hoặc Stream trong {WAIT_SECONDS}s để ở lại.'
            ),
            color=NOTIFY_BLUE,
        )
        await send_private_notify_embed(
            member=member,
            title='Động lực hôm nay',
            description=_random_motivation_plain(),
            color=NOTIFY_BLUE,
        )
        if not is_media_active(after):
            start_check(member, 'vào phòng không có Cam/Stream')
        else:
            await notify_cam_started(member, after.channel)
        await _update_live_message_for_channel(after.channel)

    elif left_focus and not stayed_in_focus:
        reset_cam_notification(member.id)
        await _handle_focus_leave(member, before.channel)
        if _is_temporary_room_id(before.channel.id):
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
    return jsonify(load_data())

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
    data  = _get_live_enriched_data()
    result = []
    for mid, start in list(join_times.items()):
        uid      = str(mid)
        info     = data.get(uid, {})
        saved    = info.get('daily', {}).get(today, 0)
        unsaved = _get_unsaved_study_seconds(mid, now)
        is_stream = is_video = False
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m and m.voice:
                is_stream = bool(m.voice.self_stream)
                is_video  = bool(m.voice.self_video)
                break
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
