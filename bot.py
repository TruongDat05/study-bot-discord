from __future__ import annotations

import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import logging
import os
import json
import random
import threading
import io
import shutil
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, send_file
from pomodoro import create_pomodoro_cog
from weekly_report import setup_weekly_report

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

try:
    from openai import OpenAI
    OPENROUTER_AVAILABLE = True
except ImportError:
    OPENROUTER_AVAILABLE = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

load_dotenv()
TOKEN              = os.getenv('DISCORD_TOKEN')
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')

SERVERS = [
    {
        'voice_channels': [1483271561036435660, 1483301292427186358],
        'report_channel': 1483288436369653861,
    },
    {
        'voice_channels': [1483284081872601098],
        'report_channel': 1483284081872601093,
    },
]

WARN_BEFORE_KICK    = 10
WAIT_SECONDS        = 60
REPORT_HOUR         = 23
REPORT_MINUTE       = 0
DAILY_BOARD_HOUR    = 0
DAILY_BOARD_MINUTE  = 0
DATA_FILE           = 'study_data.json'
RUNTIME_STATE_FILE  = 'runtime_state.json'
BACKUP_DIR          = 'backups'
DASHBOARD_PORT      = 5000
ABSENT_DAYS_WARN    = 2
CHECKPOINT_MINUTES  = 5
LIVE_UPDATE_MINUTES = 5
MILESTONE_MINUTES   = [30, 60, 120, 180, 240, 300, 360]

XP_PER_MINUTE = 10

LEVEL_THRESHOLDS = [0, 500, 1500, 4000, 9000, 18000, 34000, 60000, 100000, 160000, 250000]
LEVEL_NAMES      = [
    'Người mới 🌱', 'Học sinh 📖', 'Chăm chỉ ✏️', 'Tập trung 🎯',
    'Xuất sắc ⭐', 'Tinh anh 💎', 'Huyền thoại 🔮', 'Bậc thầy 🧠',
    'Thiên tài 🚀', 'Vô địch 👑', 'Thần học ⚡',
]

LEVEL_ROLES: dict[int, str | None] = {
    0: None, 1: 'Học Sinh', 2: 'Chăm Chỉ', 3: 'Tập Trung',
    4: 'Xuất Sắc', 5: 'Tinh Anh', 6: 'Huyền Thoại',
    7: 'Bậc Thầy', 8: 'Thiên Tài', 9: 'Vô Địch', 10: 'Thần Học',
}

# ─── QUEST CONFIG ────────────────────────────────────────────────────────────

QUEST_POOL = [
    {'id': 'study_30',     'desc': 'Học đủ 30 phút hôm nay',          'target': 30,  'type': 'minutes',    'xp': 50,  'emoji': '⏱️'},
    {'id': 'study_60',     'desc': 'Học đủ 1 tiếng hôm nay',          'target': 60,  'type': 'minutes',    'xp': 100, 'emoji': '🕐'},
    {'id': 'study_120',    'desc': 'Học đủ 2 tiếng hôm nay',          'target': 120, 'type': 'minutes',    'xp': 200, 'emoji': '🔥'},
    {'id': 'study_180',    'desc': 'Học đủ 3 tiếng hôm nay',          'target': 180, 'type': 'minutes',    'xp': 350, 'emoji': '💎'},
    {'id': 'streak_3',     'desc': 'Duy trì streak 3 ngày liên tiếp',  'target': 3,   'type': 'streak',     'xp': 80,  'emoji': '📅'},
    {'id': 'streak_7',     'desc': 'Duy trì streak 7 ngày liên tiếp',  'target': 7,   'type': 'streak',     'xp': 200, 'emoji': '🗓️'},
    {'id': 'early_bird',   'desc': 'Vào phòng học trước 8h sáng',      'target': 8,   'type': 'hour_before','xp': 75,  'emoji': '🌅'},
    {'id': 'night_owl',    'desc': 'Học sau 22h tối',                  'target': 22,  'type': 'hour_after', 'xp': 75,  'emoji': '🦉'},
    {'id': 'first_in',     'desc': 'Người đầu tiên vào phòng hôm nay', 'target': 1,   'type': 'first_in',   'xp': 60,  'emoji': '🥇'},
    {'id': 'two_sessions', 'desc': 'Học ít nhất 2 phiên trong ngày',   'target': 2,   'type': 'sessions',   'xp': 80,  'emoji': '🔄'},
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
    'level_5':     {'name': 'Xuất sắc ⭐',        'desc': 'Đạt Level 5',         'condition': ('level', 5)},
    'level_10':    {'name': 'Đỉnh cao 👑',         'desc': 'Đạt Level 10 max',    'condition': ('level', 10)},
    'early_bird':  {'name': 'Cú sáng ☀️',        'desc': 'Học trước 8h sáng',   'condition': ('special', 'early_bird')},
    'night_owl':   {'name': 'Cú đêm 🦉',          'desc': 'Học sau 0h đêm',      'condition': ('special', 'night_owl')},
    'quest_10':    {'name': 'Người thực hiện 📋', 'desc': 'Hoàn thành 10 quest', 'condition': ('quests_done', 10)},
    'quest_50':    {'name': 'Siêu nhiệm vụ 🎯',   'desc': 'Hoàn thành 50 quest', 'condition': ('quests_done', 50)},
    'xp_1000':     {'name': 'Nghìn XP 💰',        'desc': 'Đạt 1.000 XP',        'condition': ('xp', 1000)},
    'xp_10000':    {'name': 'Vạn XP 💎',          'desc': 'Đạt 10.000 XP',       'condition': ('xp', 10000)},
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
    en, vi = random.choice(MOTIVATIONS_BILINGUAL)
    return f'💬 _{en}_\n\n{vi}'

MILESTONE_DM = {
    30:  '⏰ Bạn đã học được **30 phút**! 💪 Tiếp tục nhé — bạn đang làm rất tốt!',
    60:  '🌟 **1 tiếng** học tập! Xuất sắc! Uống nước và vươn vai chút nhé! 💧',
    120: '🔥 **2 tiếng** liên tục! Phi thường! Nghỉ 5 phút rồi chiến tiếp! 🧘',
    180: '💎 **3 tiếng**! Bạn đang ở đỉnh cao! Cơ thể cần nghỉ ngơi ngắn đấy! 🍵',
    240: '🚀 **4 tiếng**! Chiến binh thực sự! Ăn nhẹ gì đó để nạp năng lượng nhé! 🍌',
    300: '👑 **5 tiếng**! Vô địch! Đây là phiên học đáng nhớ! 🏆',
    360: '⚡ **6 tiếng**! Huyền thoại! Bạn thật đáng kinh ngạc — hãy nghỉ ngơi xứng đáng! 🌙',
}

if not TOKEN:
    raise ValueError('Không tìm thấy DISCORD_TOKEN trong file .env!')

FOCUS_CHANNEL_IDS = [ch for s in SERVERS for ch in s['voice_channels']]

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── BOT SETUP ───────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.voice_states    = True
intents.members         = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

ai_client = None
if OPENROUTER_AVAILABLE and OPENROUTER_API_KEY:
    ai_client = OpenAI(base_url='https://openrouter.ai/api/v1', api_key=OPENROUTER_API_KEY)

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
_dashboard_started:   bool                    = False

_data_lock = threading.Lock()
_runtime_lock = threading.Lock()

# ─── MEDIA HELPERS ───────────────────────────────────────────────────────────

def is_media_active(vs: discord.VoiceState) -> bool:
    return bool(vs.self_video or vs.self_stream)

def media_status_icon(vs: discord.VoiceState) -> str:
    if vs.self_video and vs.self_stream: return '📷📺'
    if vs.self_video:  return '📷'
    if vs.self_stream: return '📺'
    return '⏸️'

# ─── DATA HELPERS ────────────────────────────────────────────────────────────

def load_data() -> dict:
    with _data_lock:
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.error(f'Lỗi đọc data: {e}')
        return {}

def save_data(data: dict):
    with _data_lock:
        try:
            tmp = DATA_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, DATA_FILE)
        except IOError as e:
            log.error(f'Lỗi lưu data: {e}')

def _serialize_dt(dt: datetime) -> str:
    return dt.isoformat()

def _parse_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

def save_runtime_state():
    now = datetime.now()
    snapshot = {
        'saved_at': now.strftime('%Y-%m-%d'),
        'join_times': {str(mid): _serialize_dt(ts) for mid, ts in join_times.items()},
        'last_checkpoint': {str(mid): _serialize_dt(ts) for mid, ts in last_checkpoint.items()},
        'milestone_sent': {str(mid): sorted(list(ms)) for mid, ms in milestone_sent.items()},
        'daily_first_join': {d: int(mid) for d, mid in daily_first_join.items()},
        'session_counts': {str(mid): int(cnt) for mid, cnt in session_counts.items()},
        'media_active_members': sorted(list(media_active_members)),
    }
    with _runtime_lock:
        try:
            tmp = RUNTIME_STATE_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, ensure_ascii=False, indent=2)
            os.replace(tmp, RUNTIME_STATE_FILE)
        except IOError as e:
            log.error(f'Lỗi lưu runtime state: {e}')

def load_runtime_state() -> dict:
    with _runtime_lock:
        try:
            if os.path.exists(RUNTIME_STATE_FILE):
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

    restored_join: dict[int, datetime] = {}
    restored_checkpoint: dict[int, datetime] = {}
    restored_milestones: dict[int, set] = {}
    restored_media: set[int] = set()
    restored_sessions: dict[int, int] = {}
    restored_first_join: dict[str, int] = {}

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
    if not os.path.exists(DATA_FILE):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = os.path.join(BACKUP_DIR, f'study_data_{ts}.json')
    try:
        shutil.copy2(DATA_FILE, dest)
        files = sorted(
            [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR)],
            key=os.path.getmtime
        )
        for old in files[:-30]:
            os.remove(old)
        log.info(f'[Backup] Saved → {dest}')
    except Exception as e:
        log.error(f'[Backup] Error: {e}')

def _default_user(name: str) -> dict:
    return {
        'name': name,
        'daily': {},
        'total': 0,
        'xp': 0,
        'level': 0,
        'streak': 0,
        'longest_streak': 0,
        'last_study_date': '',
        'goal': None,
        'goal_seconds': 0,
        'last_absent_warn': '',
        'xp_acc_secs': 0,
        'badges': [],
        'badge_dates': {},
        'quests_done_total': 0,
        'daily_quests': {},
        'special_flags': [],
        'remind_hour': None,
    }

def get_level(xp: int) -> int:
    for i in range(len(LEVEL_THRESHOLDS) - 1, -1, -1):
        if xp >= LEVEL_THRESHOLDS[i]:
            return i
    return 0

def xp_to_next_level(xp: int) -> tuple[int, int]:
    level = get_level(xp)
    if level >= len(LEVEL_THRESHOLDS) - 1:
        return level, 0
    return level, LEVEL_THRESHOLDS[level + 1] - xp

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

def add_study_time(member_id: int, member_name: str, seconds: int) -> dict:
    if seconds <= 0:
        return {}
    data  = load_data()
    today = datetime.now().strftime('%Y-%m-%d')
    uid   = str(member_id)
    if uid not in data:
        data[uid] = _default_user(member_name)
    data[uid]['name'] = member_name
    data[uid]['daily'][today] = data[uid]['daily'].get(today, 0) + seconds
    data[uid]['total']        = data[uid].get('total', 0) + seconds
    xp_acc    = data[uid].get('xp_acc_secs', 0) + seconds
    xp_gained = (xp_acc // 60) * XP_PER_MINUTE
    data[uid]['xp_acc_secs'] = xp_acc % 60
    old_xp    = data[uid].get('xp', 0)
    old_level = get_level(old_xp)
    streak, is_new_day = _update_streak(data, uid, today)
    if is_new_day and streak > 1:
        xp_gained += streak * 5
    data[uid]['xp']    = old_xp + xp_gained
    new_level          = get_level(data[uid]['xp'])
    data[uid]['level'] = new_level
    save_data(data)
    return {
        'xp_gained':     xp_gained,
        'level_up':      new_level > old_level,
        'new_level':     new_level,
        'streak':        streak,
        'total_xp':      data[uid]['xp'],
        'goal':          data[uid].get('goal'),
        'goal_seconds':  data[uid].get('goal_seconds', 0),
        'today_seconds': data[uid]['daily'].get(today, 0),
    }

def add_xp_direct(uid: str, xp_amount: int):
    data = load_data()
    if uid not in data:
        return
    data[uid]['xp']    = data[uid].get('xp', 0) + xp_amount
    data[uid]['level'] = get_level(data[uid]['xp'])
    save_data(data)

# ─── QUEST SYSTEM ────────────────────────────────────────────────────────────

def generate_daily_quests(uid: str, today: str, member_name: str = '') -> list[dict]:
    data = load_data()
    if uid not in data:
        data[uid] = _default_user(member_name or f'User {uid}')
        save_data(data)
    existing = data[uid].get('daily_quests', {}).get(today)
    if existing:
        return existing
    streak = data[uid].get('streak', 0)
    pool   = [q for q in QUEST_POOL if not (q['type'] == 'streak' and streak >= q['target'])]
    chosen = random.sample(pool, min(QUEST_DAILY_COUNT, len(pool)))
    quests = [{'id': q['id'], 'progress': 0, 'done': False, 'notified': False} for q in chosen]
    data[uid].setdefault('daily_quests', {})[today] = quests
    save_data(data)
    return quests

def get_quest_info(quest_id: str) -> dict | None:
    return next((q for q in QUEST_POOL if q['id'] == quest_id), None)

def update_quest_progress(uid: str, today: str, override_today_secs: int = None, member_name: str = '') -> list[str]:
    data = load_data()
    if uid not in data:
        data[uid] = _default_user(member_name or f'User {uid}')
        save_data(data)
    quests     = data[uid].get('daily_quests', {}).get(today, [])
    today_secs = override_today_secs if override_today_secs is not None else data[uid]['daily'].get(today, 0)
    streak     = data[uid].get('streak', 0)
    now_hour   = datetime.now().hour
    try:
        sessions = session_counts.get(int(uid), 0)
    except (ValueError, TypeError):
        sessions = 0
    just_done = []
    for q in quests:
        if q.get('done'):
            continue
        info   = get_quest_info(q['id'])
        if not info:
            continue
        t, target = info['type'], info['target']
        if   t == 'minutes':    q['progress'] = min(target, today_secs // 60)
        elif t == 'streak':     q['progress'] = min(target, streak)
        elif t == 'hour_before':
            if now_hour < target: q['progress'] = target
        elif t == 'hour_after':
            if now_hour >= target: q['progress'] = target
        elif t == 'first_in':
            first_id = daily_first_join.get(today)
            if first_id and str(first_id) == uid: q['progress'] = target
        elif t == 'sessions':   q['progress'] = min(target, sessions)

        if q['progress'] >= target and not q.get('done'):
            q['done']     = True
            q['notified'] = False
            just_done.append(q['id'])
            data[uid]['quests_done_total'] = data[uid].get('quests_done_total', 0) + 1
            xp_bonus = info.get('xp', 0)
            data[uid]['xp']    = data[uid].get('xp', 0) + xp_bonus
            data[uid]['level'] = get_level(data[uid]['xp'])
            log.info(f'Quest done [{q["id"]}] → +{xp_bonus} XP cho {data[uid]["name"]}')
    data[uid]['daily_quests'][today] = quests
    save_data(data)
    return just_done

# ─── BADGE SYSTEM ────────────────────────────────────────────────────────────

def check_and_award_badges(uid: str, member: discord.Member = None) -> list[str]:
    data = load_data()
    if uid not in data:
        return []
    info          = data[uid]
    earned        = set(info.get('badges', []))
    total_hours   = info.get('total', 0) / 3600
    xp            = info.get('xp', 0)
    level         = info.get('level', 0)
    streak        = info.get('streak', 0)
    quests_done   = info.get('quests_done_total', 0)
    special_flags = info.get('special_flags', [])
    today         = datetime.now().strftime('%Y-%m-%d')
    today_secs    = info['daily'].get(today, 0)
    new_badges    = []
    for bid, bdef in BADGES.items():
        if bid in earned:
            continue
        ctype, cval = bdef['condition']
        awarded = False
        if   ctype == 'streak'      and streak >= cval:            awarded = True
        elif ctype == 'total_hours' and total_hours >= cval:       awarded = True
        elif ctype == 'daily_hours' and today_secs >= cval * 3600: awarded = True
        elif ctype == 'level'       and level >= cval:             awarded = True
        elif ctype == 'xp'          and xp >= cval:                awarded = True
        elif ctype == 'quests_done' and quests_done >= cval:       awarded = True
        elif ctype == 'special'     and cval in special_flags:     awarded = True
        if awarded:
            new_badges.append(bid)
            earned.add(bid)
    if new_badges:
        data[uid]['badges'] = list(earned)
        badge_dates = data[uid].setdefault('badge_dates', {})
        for bid in new_badges:
            if bid not in badge_dates:
                badge_dates[bid] = today
        save_data(data)
    return new_badges

def award_special_flag(uid: str, flag: str):
    data = load_data()
    if uid not in data:
        return
    flags = data[uid].get('special_flags', [])
    if flag not in flags:
        flags.append(flag)
        data[uid]['special_flags'] = flags
        save_data(data)

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
    xp      = info.get('xp', 0)
    level   = min(info.get('level', 0), len(LEVEL_NAMES) - 1)
    streak  = info.get('streak', 0)
    longest = info.get('longest_streak', 0)
    total   = info.get('total', 0)
    badges  = info.get('badges', [])
    today   = datetime.now().strftime('%Y-%m-%d')
    today_secs = info['daily'].get(today, 0)
    if member_id in join_times and member_id in media_active_members:
        chk = last_checkpoint.get(member_id, join_times[member_id])
        today_secs += int((datetime.now() - chk).total_seconds())

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
    draw.text((cx, cy-10), str(level), font=f_huge, fill=ACCENT, anchor='mm')
    draw.text((cx, cy+26), 'LEVEL',   font=f_sm,   fill=TEXT2,   anchor='mm')

    display_name = name if len(name) <= 18 else name[:17] + '…'
    draw.text((168, 78),  display_name,                         font=f_xl, fill=TEXT1)
    draw.text((170, 112), f'Lv.{level} • {LEVEL_NAMES[level]}', font=f_md, fill=TEXT2)

    xp_start = LEVEL_THRESHOLDS[level]
    xp_end   = LEVEL_THRESHOLDS[min(level+1, len(LEVEL_THRESHOLDS)-1)]
    xp_pct   = min(1.0, (xp - xp_start) / max(1, xp_end - xp_start))
    BX, BY, BW, BH = 168, 144, 280, 12
    _draw_rounded_rect(draw, [BX, BY, BX+BW, BY+BH], radius=6, fill=(51, 65, 85))
    fw = int(BW * xp_pct)
    if fw > 8:
        for i in range(fw):
            r2 = int(ACCENT[0] + (ACCENT2[0]-ACCENT[0]) * i / max(1, fw))
            g2 = int(ACCENT[1] + (ACCENT2[1]-ACCENT[1]) * i / max(1, fw))
            b2 = int(ACCENT[2] + (ACCENT2[2]-ACCENT[2]) * i / max(1, fw))
            draw.rectangle([BX+i, BY, BX+i+1, BY+BH], fill=(r2, g2, b2))
    draw.text((BX, BY+BH+6), f'{xp:,} XP  ({int(xp_pct*100)}%)', font=f_sm, fill=TEXT2)

    draw.rectangle([32, 190, W-32, 191], fill=(51, 65, 85))
    stats = [
        ('Hôm nay',   format_time(today_secs), GREEN),
        ('Tổng cộng', format_time(total),       ACCENT),
        ('Streak',    f'{streak} ngày 🔥',       GOLD),
        ('Kỷ lục',    f'{longest} ngày',         TEXT2),
        ('Huy hiệu',  str(len(badges)),          (236, 72, 153)),
        ('Cấp độ',    str(level),                ACCENT2),
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

async def _ensure_role_synced(member: discord.Member, current_level: int):
    current_level = max(0, min(int(current_level), len(LEVEL_THRESHOLDS) - 1))
    guild = member.guild
    expected_role_name = LEVEL_ROLES.get(current_level)
    expected_role = discord.utils.get(guild.roles, name=expected_role_name) if expected_role_name else None

    roles_to_remove = []
    has_expected = False

    member_roles = member.roles

    for lvl, role_name in LEVEL_ROLES.items():
        if role_name is None: continue
        role = discord.utils.get(guild.roles, name=role_name)
        if role and role in member_roles:
            if role == expected_role:
                has_expected = True
            else:
                roles_to_remove.append(role)

    if roles_to_remove:
        try:
            await member.remove_roles(*roles_to_remove, reason=f'Role sync -> Lv.{current_level}')
        except Exception as e:
            log.error(f'Lỗi thu hồi role của {member.display_name}: {e}')

    if expected_role and not has_expected:
        try:
            await member.add_roles(expected_role, reason=f'Role sync -> Lv.{current_level}')
        except Exception as e:
            log.error(f'Lỗi gán role cho {member.display_name}: {e}')

# ─── SESSION MANAGEMENT ──────────────────────────────────────────────────────

def record_join(member: discord.Member):
    now   = datetime.now()
    today = now.strftime('%Y-%m-%d')
    join_times[member.id]      = now
    last_checkpoint[member.id] = now
    milestone_sent[member.id]  = set()
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

async def _do_checkpoint(member: discord.Member) -> tuple[int, dict]:
    if member.id not in join_times: return 0, {}
    now        = datetime.now()
    checkpoint = last_checkpoint.get(member.id, join_times[member.id])
    elapsed    = int((now - checkpoint).total_seconds())
    result: dict = {}
    if elapsed > 0:
        result = add_study_time(member.id, member.display_name, elapsed)
        last_checkpoint[member.id] = now
        save_runtime_state()
        log.info(f'[Checkpoint] {member.display_name}: +{format_time(elapsed)}')
    return elapsed, result

async def _check_milestones(member: discord.Member):
    if member.id not in join_times: return
    total_min = int((datetime.now() - join_times[member.id]).total_seconds()) // 60
    if member.id not in milestone_sent:
        milestone_sent[member.id] = set()
    for ms in MILESTONE_MINUTES:
        if total_min >= ms and ms not in milestone_sent[member.id]:
            milestone_sent[member.id].add(ms)
            save_runtime_state()
            await safe_send_dm(member, MILESTONE_DM.get(ms, f'⏰ Đã học **{ms} phút**! 💪'))

async def _check_quests_and_badges(member: discord.Member):
    uid   = str(member.id)
    today = datetime.now().strftime('%Y-%m-%d')
    generate_daily_quests(uid, today)
    data       = load_data()
    saved_secs = data.get(uid, {}).get('daily', {}).get(today, 0)
    
    if member.id in join_times and member.id in media_active_members:
        chk       = last_checkpoint.get(member.id, join_times[member.id])
        real_secs = saved_secs + int((datetime.now() - chk).total_seconds())
    else:
        real_secs = saved_secs
        
    update_quest_progress(uid, today, override_today_secs=real_secs)
    data_fresh = load_data()
    quests     = data_fresh.get(uid, {}).get('daily_quests', {}).get(today, [])
    new_level  = data_fresh.get(uid, {}).get('level', 0)
    
    notify_changed = False
    msgs_to_send = []
    for q in quests:
        if q.get('done') and not q.get('notified', False):
            info = get_quest_info(q['id'])
            if info:
                msgs_to_send.append(info)
            q['notified']  = True
            notify_changed = True
            
    if notify_changed:
        data_fresh[uid]['daily_quests'][today] = quests
        save_data(data_fresh)

    await _ensure_role_synced(member, new_level)
    
    for info in msgs_to_send:
        await safe_send_dm(member,
            f'🎉 **Nhiệm vụ hoàn thành!**\n'
            f'{info["emoji"]} _{info["desc"]}_\n'
            f'⚡ Nhận được: **+{info["xp"]} XP bonus!**'
        )
        
    new_badges = check_and_award_badges(uid, member)
    for bid in new_badges:
        bdef = BADGES.get(bid, {})
        await safe_send_dm(member,
            f'🏅 **Huy hiệu mới: {bdef.get("name", bid)}!**\n'
            f'_{bdef.get("desc", "")}_\n'
            f'Dùng `/badges` để xem tất cả huy hiệu!'
        )

async def record_leave_and_notify(member: discord.Member, force_in_pomodoro: bool = False) -> int:
    if member.id not in join_times: return 0

    pomo_cog      = bot.cogs.get('PomodoroCog')
    in_pomodoro   = force_in_pomodoro or (pomo_cog is not None and member.id in getattr(pomo_cog, '_sessions', {}))

    now        = datetime.now()
    checkpoint = last_checkpoint.get(member.id, join_times[member.id])
    remaining  = int((now - checkpoint).total_seconds())
    if remaining > 0 and member.id in media_active_members and not in_pomodoro:
        result = add_study_time(member.id, member.display_name, remaining)
        if result and result.get('level_up'):
            await _ensure_role_synced(member, result['new_level'])
            await safe_send_dm(member, f'🎉 **LEVEL UP! Bạn đã đạt Lv.{result["new_level"]} {LEVEL_NAMES[result["new_level"]]}** 🎊')

    total_duration = int((now - join_times.pop(member.id)).total_seconds())
    last_checkpoint.pop(member.id, None)
    milestone_sent.pop(member.id, None)
    media_active_members.discard(member.id)
    save_runtime_state()

    uid   = str(member.id)
    today = now.strftime('%Y-%m-%d')
    generate_daily_quests(uid, today)
    data_now   = load_data()
    final_secs = data_now.get(uid, {}).get('daily', {}).get(today, 0)

    update_quest_progress(uid, today, override_today_secs=final_secs)
    data_after   = load_data()
    quests_after = data_after.get(uid, {}).get('daily_quests', {}).get(today, [])
    just_done_info: list[dict] = []
    
    for q in quests_after:
        if q.get('done') and not q.get('notified', False):
            info = get_quest_info(q['id'])
            if info:
                just_done_info.append(info)
            q['notified'] = True
            
    if just_done_info:
        data_after[uid]['daily_quests'][today] = quests_after
        save_data(data_after)

    new_badges = check_and_award_badges(uid, member)
    
    data_final = load_data()
    if uid in data_final:
        info       = data_final[uid]
        xp         = info.get('xp', 0)
        level      = min(info.get('level', 0), len(LEVEL_NAMES) - 1)
        streak     = info.get('streak', 0)
        today_secs = info['daily'].get(today, 0)
        goal       = info.get('goal')
        goal_secs  = info.get('goal_seconds', 0)

        await _ensure_role_synced(member, level)

        if total_duration > 30:
            msg = (
                f'✅ **Phiên học kết thúc!**\n'
                f'──────────────────\n'
                f'⏱️ Phiên này: `{format_time(total_duration)}`\n'
                f'📅 Hôm nay: `{format_time(today_secs)}`\n'
                f'⚡ Tổng XP: `{xp:,} XP`\n'
                f'📊 Level: `Lv.{level} {LEVEL_NAMES[level]}`\n'
                f'🔥 Streak: `{streak} ngày`'
            )
            if goal and goal_secs > 0:
                progress = min(100, int((today_secs / goal_secs) * 100))
                bar = '█' * (progress // 10) + '░' * (10 - progress // 10)
                msg += f'\n🎯 **{goal}**: `{bar}` {progress}%'
            if just_done_info:
                names = [qi['emoji'] + ' ' + qi['desc'] for qi in just_done_info]
                msg += f'\n\n🎉 **Quest hoàn thành:** ' + ' · '.join(names)
            if new_badges:
                bnames = [BADGES[b]['name'] for b in new_badges if b in BADGES]
                msg += f'\n🏅 **Badge mới:** ' + ' · '.join(bnames)
            
            await safe_send_dm(member, msg)
        else:
            for info_q in just_done_info:
                await safe_send_dm(member,
                    f'🎉 **Nhiệm vụ hoàn thành!**\n'
                    f'{info_q["emoji"]} _{info_q["desc"]}_\n'
                    f'⚡ Nhận được: **+{info_q["xp"]} XP bonus!**'
                )
            for bid in new_badges:
                bdef = BADGES.get(bid, {})
                await safe_send_dm(member,
                    f'🏅 **Huy hiệu mới: {bdef.get("name", bid)}!**\n'
                    f'_{bdef.get("desc", "")}_\n'
                    f'Dùng `/badges` để xem tất cả huy hiệu!'
                )
            
    return total_duration

# ─── LIVE MESSAGE ─────────────────────────────────────────────────────────────

async def update_live_message(server: dict):
    channel = bot.get_channel(server['report_channel'])
    if not channel: return
    now       = datetime.now()
    guild     = channel.guild
    voice_ids = server['voice_channels']
    today     = now.strftime('%Y-%m-%d')
    data      = _get_live_enriched_data()
    active    = []
    for mid, start_time in list(join_times.items()):
        m = guild.get_member(mid)
        if not m or not m.voice or not m.voice.channel: continue
        if m.voice.channel.id not in voice_ids: continue
        uid   = str(mid)
        saved = data.get(uid, {}).get('daily', {}).get(today, 0)
        if mid in media_active_members:
            chk         = last_checkpoint.get(mid, start_time)
            today_total = saved + int((now - chk).total_seconds())
        else:
            today_total = saved
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
            level  = info.get('level', 0)
            streak = info.get('streak', 0)
            xp     = info.get('xp', 0)
            total  = info.get('total', 0)
            lines.append(
                f'{medal} **{info["name"]}**'
                f' · Lv.`{level}` · 🔥`{streak}d`'
                f'\n       ⏱️ Hôm nay: `{format_time(t)}`'
                f'  |  📚 Tổng: `{format_time(total)}`'
                f'  |  ⚡ `{xp:,} XP`'
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
                motivation = _random_motivation()
                await safe_send_dm(member,
                    f'⏰ **Nhắc học lúc {hour:02d}:00!**\n\n'
                    f'{motivation}\n\n'
                    f'📚 Hôm nay bạn chưa học phút nào. Vào phòng thôi! 🔥\n'
                    f'_Tắt nhắc: `/remind -1`_'
                )
            else:
                await safe_send_dm(member,
                    f'⏰ **Nhắc học lúc {hour:02d}:00!**\n\n'
                    f'✅ Bạn đã học `{format_time(today_secs)}` hôm nay. Giỏi lắm!\n'
                    f'💪 Tiếp tục vào phòng để tăng thêm nhé!\n'
                    f'_Tắt nhắc: `/remind -1`_'
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f'[Remind] Lỗi vòng lặp nhắc học {member.display_name}: {e}')
            await asyncio.sleep(60)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

async def safe_send_dm(member: discord.Member, message: str):
    try:
        await member.send(message)
    except discord.Forbidden:
        log.warning(f'DM blocked: {member.display_name}')
    except Exception as e:
        log.error(f'DM error: {e}')

async def _force_stop_pomodoro_if_active(member: discord.Member, reason: str = 'rời phòng học') -> bool:
    """
    Đồng bộ với PomodoroCog để tránh tiếp tục cộng thời gian/XP khi user rời phòng học.
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

    await safe_send_dm(
        member,
        f'⏹️ **Pomodoro đã dừng tự động** vì bạn {reason}.\n'
        f'Dùng `/pomodoro start` để bắt đầu lại khi sẵn sàng.'
    )
    return True

def bot_can_move(member): return member.guild.me.guild_permissions.move_members

def cancel_task(mid: int):
    t = pending_checks.pop(mid, None)
    if t and not t.done(): t.cancel()

def start_check(member: discord.Member, reason: str):
    cancel_task(member.id)
    pending_checks[member.id] = asyncio.create_task(check_media(member))
    log.info(f'{member.display_name} {reason} → {WAIT_SECONDS}s countdown.')

# ─── MEDIA CHECK ─────────────────────────────────────────────────────────────

async def check_media(member: discord.Member):
    try:
        await asyncio.sleep(WAIT_SECONDS - WARN_BEFORE_KICK)
        if not (member.voice and member.voice.channel and
                member.voice.channel.id in FOCUS_CHANNEL_IDS): return
        if is_media_active(member.voice): return
        await safe_send_dm(member,
            f'⚠️ **Cảnh báo!** Chưa bật **Cam 📷 hoặc Stream 📺**.\n'
            f'Sẽ bị kick sau **{WARN_BEFORE_KICK} giây** nếu không bật!')
        await asyncio.sleep(WARN_BEFORE_KICK)
        if not (member.voice and member.voice.channel and
                member.voice.channel.id in FOCUS_CHANNEL_IDS): return
        if not is_media_active(member.voice):
            if not bot_can_move(member): return
            pomo_cog = bot.cogs.get('PomodoroCog')
            was_in_pomo = pomo_cog is not None and member.id in getattr(pomo_cog, '_sessions', {})
            await _force_stop_pomodoro_if_active(member, reason='bị kick (không bật camera/stream)')
            await record_leave_and_notify(member, force_in_pomodoro=was_in_pomo)
            await member.move_to(None)
            await safe_send_dm(member,
                '🚫 Bị kick vì **không bật Cam 📷 hoặc Stream 📺**.\n'
                'Bật Cam hoặc Stream khi vào lại nhé!')
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

    if now.hour == 4 and now.minute == 0:
        backup_data()

    if now.hour == 0 and now.minute == 0:
        session_counts.clear()
        daily_first_join.clear()
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
    pomo_cog      = bot.cogs.get('PomodoroCog')
    pomo_sessions = pomo_cog._sessions if pomo_cog else {}
    for mid in list(join_times.keys()):
        member = None
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m and m.voice and m.voice.channel and m.voice.channel.id in FOCUS_CHANNEL_IDS:
                member = m; break
        if not member:
            join_times.pop(mid, None)
            last_checkpoint.pop(mid, None)
            milestone_sent.pop(mid, None)
            media_active_members.discard(mid)
            save_runtime_state()
            continue

        was_active = mid in media_active_members
        now_active = bool(member.voice and is_media_active(member.voice))
        in_pomo    = mid in pomo_sessions
        
        if was_active and not in_pomo:
            elapsed, result = await _do_checkpoint(member)
            if result and result.get('level_up'):
                await _ensure_role_synced(member, result['new_level'])
                await safe_send_dm(member, f'🎉 **LEVEL UP! Bạn đã đạt Lv.{result["new_level"]} {LEVEL_NAMES[result["new_level"]]}** 🎊')
            await _check_milestones(member)
            await _check_quests_and_badges(member)
        elif in_pomo and was_active:
            last_checkpoint[mid] = datetime.now()
        
        if now_active and not was_active:
            media_active_members.add(mid)
            last_checkpoint[mid] = datetime.now()
            save_runtime_state()
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
            lines.append(
                f'{medal} **{info["name"]}** `Lv.{info.get("level",0)}` '
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
    data      = load_data()
    today     = datetime.now().strftime('%Y-%m-%d')
    warn_date = (datetime.now() - timedelta(days=ABSENT_DAYS_WARN)).strftime('%Y-%m-%d')
    dirty     = False
    for uid, info in data.items():
        last_date   = info.get('last_study_date', '')
        last_warned = info.get('last_absent_warn', '')
        if not last_date or last_date >= warn_date or last_warned == today: continue
        try:
            member_id = int(uid)
        except (ValueError, TypeError):
            continue
        for guild in bot.guilds:
            m = guild.get_member(member_id)
            if not m: continue
            days = (datetime.now() - datetime.strptime(last_date, '%Y-%m-%d')).days
            await safe_send_dm(m,
                f'😢 Bạn đã không học trong **{days} ngày** rồi!\n'
                f'🔥 Streak: `{info.get("streak",0)} ngày`\n'
                f'💪 Vào phòng ngay trước khi streak reset nhé!\n'
                f'_Dùng `/remind <giờ>` để bot nhắc bạn học mỗi ngày._')
            data[uid]['last_absent_warn'] = today
            dirty = True
            break
    if dirty:
        save_data(data)

# ─── AI ──────────────────────────────────────────────────────────────────────

async def _ask_ai(question: str) -> str:
    if not ai_client:
        return '❌ Thiếu OPENROUTER_API_KEY trong .env.'
    try:
        response = await asyncio.to_thread(
            ai_client.chat.completions.create,
            model='openrouter/auto',
            messages=[
                {'role': 'system', 'content': 'Bạn là trợ lý học tập trong Discord. Trả lời ngắn gọn, dùng emoji, tối đa 400 từ.'},
                {'role': 'user',   'content': question},
            ]
        )
        msg = f'🤖 **{question}**\n\n{response.choices[0].message.content}'
        return msg[:1990] + '...' if len(msg) > 2000 else msg
    except Exception as e:
        log.error(f'AI error: {e}')
        return '❌ Lỗi AI. Thử lại sau!'

# ─── SLASH COMMANDS ──────────────────────────────────────────────────────────

def _build_rank_message(target: discord.Member, data: dict) -> str:
    uid = str(target.id)
    if uid not in data:
        return f'❌ **{target.display_name}** chưa có dữ liệu!'
    info    = data[uid]
    xp      = info.get('xp', 0)
    level   = min(info.get('level', 0), len(LEVEL_NAMES) - 1)
    streak  = info.get('streak', 0)
    longest = info.get('longest_streak', 0)
    total   = info.get('total', 0)
    today   = datetime.now().strftime('%Y-%m-%d')
    saved   = info.get('daily', {}).get(today, 0)
    if target.id in join_times and target.id in media_active_members:
        chk   = last_checkpoint.get(target.id, join_times[target.id])
        saved += int((datetime.now() - chk).total_seconds())
    lv_now = get_level(xp)
    if lv_now >= len(LEVEL_THRESHOLDS) - 1:
        xp_needed = 0; pct = 100; bar_f = 20
    else:
        xp_start  = LEVEL_THRESHOLDS[lv_now]; xp_end = LEVEL_THRESHOLDS[lv_now + 1]
        xp_cur    = xp - xp_start; xp_needed = xp_end - xp
        span      = xp_end - xp_start
        pct       = int((xp_cur / span) * 100)
        bar_f     = int((xp_cur / span) * 20)
    xp_bar    = '█' * bar_f + '░' * (20 - bar_f)
    role_name = LEVEL_ROLES.get(level)
    role_str  = f'🏷️ Role: **{role_name}**\n' if role_name else ''
    recent    = sorted(info.get('daily', {}).items(), reverse=True)[:5]
    recent_str = ' · '.join([f'`{d[5:]}`{format_time(s)}' for d, s in recent])
    badges    = info.get('badges', [])
    badge_str = format_badges(badges[:6]) if badges else '_Chưa có_'
    return (
        f'╔══════════════════════════════╗\n'
        f'   🎓 **{target.display_name}**\n'
        f'╚══════════════════════════════╝\n'
        f'🏅 **Lv.{level}** {LEVEL_NAMES[level]}\n{role_str}'
        f'──────────────────────────────\n'
        f'⚡ XP: `{xp:,}` | `{xp_bar}` **{pct}%**\n'
        f'_{f"còn **{xp_needed} XP** để lên Lv.{lv_now+1}" if xp_needed > 0 else "✨ Max level!"}_\n'
        f'──────────────────────────────\n'
        f'🔥 Streak: `{streak} ngày` _(kỷ lục: {longest})_\n'
        f'🕐 Hôm nay: `{format_time(saved)}`\n'
        f'📚 Tổng: `{format_time(total)}`\n'
        f'🏅 Huy hiệu: {badge_str}\n'
        f'──────────────────────────────\n'
        f'📅 Gần nhất: {recent_str}'
    )

# ── /rank ──────────────────────────────────────────────────────────────────

@bot.tree.command(name='rank', description='Xem bảng XP và thống kê của bạn')
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
        raw[uid] = _default_user(interaction.user.display_name)
        save_data(raw)

    data = _get_live_enriched_data()

    mid         = interaction.user.id
    saved_secs  = data[uid]['daily'].get(today, 0)
    if mid in join_times and mid in media_active_members:
        chk             = last_checkpoint.get(mid, join_times[mid])
        real_today_secs = saved_secs + int((datetime.now() - chk).total_seconds())
    else:
        real_today_secs = saved_secs

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
        xp_str = f'+{info["xp"]} XP ✓' if q.get('done') else f'{info["xp"]} XP'
        lines.append(
            f'{status} {info["emoji"]} **{info["desc"]}**\n'
            f'   `{bar}` {q["progress"]}/{info["target"]}  _{xp_str}_'
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
            raw[uid] = _default_user(interaction.user.display_name)
            save_data(raw)
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
        '🏅 Level':       ['level_5', 'level_10'],
        '⏰ Đặc biệt':    ['early_bird', 'night_owl'],
        '📋 Quest':       ['quest_10', 'quest_50'],
        '💰 XP':          ['xp_1000', 'xp_10000'],
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
            raw[uid] = _default_user(interaction.user.display_name)
            save_data(raw)
    data   = _get_live_enriched_data()
    if uid not in data:
        await interaction.followup.send(
            f'❌ **{target.display_name}** chưa có dữ liệu!', ephemeral=True
        ); return
    info        = data[uid]
    today       = datetime.now().strftime('%Y-%m-%d')
    today_saved = info.get('daily', {}).get(today, 0)
    if target.id in join_times and target.id in media_active_members:
        chk         = last_checkpoint.get(target.id, join_times[target.id])
        today_saved += int((datetime.now() - chk).total_seconds())
    xp         = info.get('xp', 0)
    level      = min(info.get('level', 0), len(LEVEL_NAMES) - 1)
    streak     = info.get('streak', 0)
    _, xp_need = xp_to_next_level(xp)
    recent     = sorted(info.get('daily', {}).items(), reverse=True)[:7]
    recent_str = '\n'.join([f'  `{d}`: {format_time(s)}' for d, s in recent])
    badges     = info.get('badges', [])
    goal       = info.get('goal')
    goal_secs  = info.get('goal_seconds', 0)
    remind_h   = info.get('remind_hour')
    msg = (
        f'📊 **Thống kê của {target.display_name}**\n'
        f'🏅 `Lv.{level} {LEVEL_NAMES[level]}` | ⚡{xp:,} XP _(còn {xp_need})_\n'
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
        if mid in join_times and mid in media_active_members:
            chk = last_checkpoint.get(mid, join_times[mid])
            s  += int((now - chk).total_seconds())
        return s

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
            lines.append(
                f'{medal}{active} **{info["name"]}** `Lv.{info.get("level",0)}` '
                f'🔥{info.get("streak",0)} — `{format_time(rt)}`'
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
            lines.append(
                f'{medal} **{info["name"]}** `Lv.{info.get("level",0)}` '
                f'🔥{info.get("streak",0)} · 📚 `{format_time(info.get("total",0))}`'
                f' · ⚡{info.get("xp",0):,} XP'
            )
    await interaction.followup.send('\n'.join(lines))

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
        if mid in media_active_members:
            chk   = last_checkpoint.get(mid, st)
            total = saved + int((now - chk).total_seconds())
        else:
            total = saved
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
    data = load_data()
    uid  = str(interaction.user.id)
    if uid not in data: data[uid] = _default_user(interaction.user.display_name)
    data[uid]['goal']         = goal
    data[uid]['goal_seconds'] = total
    save_data(data)
    await interaction.followup.send(
        f'✅ Mục tiêu: **"{goal}"** — `{format_time(total)}`/ngày 💪', ephemeral=True
    )

# ── /remind ────────────────────────────────────────────────────────────────

@bot.tree.command(name='remind', description='Đặt giờ nhắc học hàng ngày qua DM (-1 để tắt)')
@app_commands.describe(hour='Giờ nhắc (0-23), nhập -1 để tắt')
async def slash_remind(interaction: discord.Interaction, hour: app_commands.Range[int, -1, 23]):
    await interaction.response.defer(ephemeral=True)
    uid  = str(interaction.user.id)
    data = load_data()

    if hour == -1:
        old = remind_tasks.pop(interaction.user.id, None)
        if old:
            task = old[1]
            if task and not task.done(): task.cancel()
        if uid in data:
            data[uid]['remind_hour'] = None
            save_data(data)
        await interaction.followup.send(
            '🔕 Đã tắt nhắc học.\n_Dùng `/remind <giờ>` để bật lại._', ephemeral=True
        )
        return

    if uid not in data:
        data[uid] = _default_user(interaction.user.display_name)
    data[uid]['remind_hour'] = hour
    save_data(data)

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
    await interaction.followup.send(await _ask_ai(question))

# ── /roles ─────────────────────────────────────────────────────────────────

@bot.tree.command(name='roles', description='Danh sách vai trò theo level')
async def slash_roles(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    data  = load_data()
    uid   = str(interaction.user.id)
    my_xp = data.get(uid, {}).get('xp', 0) if uid in data else 0
    my_lv = get_level(my_xp)
    lines = ['🏷️ **Vai trò theo level**\n']
    for lv, rn in LEVEL_ROLES.items():
        if rn is None: continue
        xp_req  = LEVEL_THRESHOLDS[lv]
        is_mine = (lv == my_lv); is_done = (my_lv > lv)
        status  = ' ◀ **bạn đây**' if is_mine else (' ✅' if is_done else '')
        lines.append(f'{"✦" if is_mine else ("✔" if is_done else "○")} Lv.**{lv}** `{xp_req:,} XP` → **{rn}**{status}')
    await interaction.followup.send('\n'.join(lines), ephemeral=True)

# ── /help ──────────────────────────────────────────────────────────────────

@bot.tree.command(name='help', description='Danh sách tất cả lệnh của bot')
async def slash_help(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    msg = (
        '📚 **STUDY BOT — DANH SÁCH LỆNH**\n'
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n'
        '**📊 Thống kê cá nhân**\n'
        '`/rank [member]` — Bảng XP và thống kê\n'
        '`/stats [member]` — Thống kê chi tiết 7 ngày\n'
        '`/card [member]` — Tạo ảnh profile card\n'
        '`/badges [member]` — Xem huy hiệu\n\n'
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
        '`/roles` — Xem vai trò theo level\n\n'
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
        '`/syncroles` · `/report` · `/dailyboard` · `/updatelive` · `/backup`\n'
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
    for uid, info in data.items():
        try:
            member_id = int(uid)
        except (ValueError, TypeError):
            continue
        m = guild.get_member(member_id)
        if not m: continue
        await _ensure_role_synced(m, info.get('level', 0))
        updated += 1
    await interaction.followup.send(f'✅ Đã sync **{updated}** thành viên.', ephemeral=True)

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
    if target.id in join_times and target.id in media_active_members:
        chk   = last_checkpoint.get(target.id, join_times[target.id])
        saved += int((datetime.now() - chk).total_seconds())
    recent = sorted(info.get('daily', {}).items(), reverse=True)[:7]
    await ctx.send(
        f'📊 **{target.display_name}** — `Lv.{info.get("level",0)}` ⚡{info.get("xp",0):,} XP\n'
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
        if mid in join_times and mid in media_active_members:
            chk = last_checkpoint.get(mid, join_times[mid])
            s  += int((now - chk).total_seconds())
        return s

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
            lines.append(f'{medal}{" 🟢" if is_live else ""} **{info["name"]}** `Lv.{info.get("level",0)}`'
                         f' 🔥{info.get("streak",0)} — `{format_time(rt)}`')
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
    if mid in join_times and mid in media_active_members:
        chk       = last_checkpoint.get(mid, join_times[mid])
        real_secs = saved_secs + int((datetime.now() - chk).total_seconds())
    else:
        real_secs = saved_secs
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
        lines.append(
            f'{status} {info["emoji"]} **{info["desc"]}** — '
            f'`{bar}` {q["progress"]}/{info["target"]} (+{info["xp"]} XP)'
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

@bot.event
async def on_ready():
    log.info(f'✅ Bot {bot.user.name} ready!')
    if not join_times:
        restore_runtime_state()

    if not bot.cogs.get('PomodoroCog'):
        try:
            cog = create_pomodoro_cog(
                bot, add_study_time, safe_send_dm, format_time,
                load_data_fn=load_data, save_data_fn=save_data,
                add_xp_fn=add_xp_direct,
            )
            await bot.add_cog(cog)
            log.info('✅ Pomodoro Cog loaded')
        except Exception as e:
            log.error(f'Pomodoro error: {e}')

    await setup_weekly_report(bot, load_data, save_data, BADGES, safe_send_dm)

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
    for uid, info in data.items():
        try:
            mid = int(uid)
        except (ValueError, TypeError):
            continue
        
        # Restore reminders
        remind_h = info.get('remind_hour')
        if remind_h is not None:
            old = remind_tasks.pop(mid, None)
            if old:
                old_task = old[1]
                if old_task and not old_task.done(): old_task.cancel()
            for guild in bot.guilds:
                m = guild.get_member(mid)
                if m:
                    t = asyncio.create_task(_remind_loop(m, remind_h))
                    remind_tasks[mid] = (remind_h, t)
                    log.info(f'[Remind] Khôi phục: {info["name"]} lúc {remind_h:02d}:00')
                    break
        
        # Sync roles
        level = info.get('level', 0)
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m:
                asyncio.create_task(_ensure_role_synced(m, level))
                break

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

    _update_live_cache()
    save_runtime_state()

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    joined_focus    = after.channel  and after.channel.id in FOCUS_CHANNEL_IDS
    left_focus      = before.channel and before.channel.id in FOCUS_CHANNEL_IDS
    stayed_in_focus = joined_focus and left_focus

    if stayed_in_focus:
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
        elif was_active and not now_active:
            if not in_pomodoro:
                elapsed, result = await _do_checkpoint(member)
                if result and result.get('level_up'):
                    await _ensure_role_synced(member, result['new_level'])
                    await safe_send_dm(member, f'🎉 **LEVEL UP! Bạn đã đạt Lv.{result["new_level"]} {LEVEL_NAMES[result["new_level"]]}** 🎊')
                await _check_quests_and_badges(member)
            media_active_members.discard(member.id)
            save_runtime_state()
            start_check(member, 'tắt Cam/Stream')
            await safe_send_dm(member,
                f'⚠️ Vừa tắt Cam/Stream!\n'
                f'Bật lại trong **{WAIT_SECONDS}s** hoặc bị kick ra khỏi phòng.')

    elif joined_focus and not stayed_in_focus:
        record_join(member)
        motivation = _random_motivation()
        await safe_send_dm(member,
            f'👋 Chào mừng **{member.display_name}** vào phòng học!\n\n'
            f'{motivation}\n\n'
            f'📌 Nhớ bật **Cam 📷 hoặc Stream 📺** trong **{WAIT_SECONDS}s** để ở lại nhé!\n'
            f'💡 Dùng `/quest` để xem nhiệm vụ hôm nay · `/help` để xem tất cả lệnh!'
        )
        if not is_media_active(after):
            start_check(member, 'vào phòng không có Cam/Stream')
        for server in SERVERS:
            if after.channel.id in server['voice_channels']:
                await update_live_message(server); break

    elif left_focus and not stayed_in_focus:
        pomo_cog = bot.cogs.get('PomodoroCog')
        was_in_pomo = pomo_cog is not None and member.id in getattr(pomo_cog, '_sessions', {})
        
        await _force_stop_pomodoro_if_active(member, reason='rời phòng học')
        duration = await record_leave_and_notify(member, force_in_pomodoro=was_in_pomo)
        cancel_task(member.id)
        log.info(f'{member.display_name} rời phòng sau {format_time(duration)}')
        for server in SERVERS:
            if before.channel.id in server['voice_channels']:
                await update_live_message(server); break

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
    .xp-bar{background:linear-gradient(90deg,#6366f1,#8b5cf6)}
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
const THRES=[0,500,1500,4000,9000,18000,34000,60000,100000,160000,250000];
const getLv=xp=>{for(let i=THRES.length-1;i>=0;i--)if(xp>=THRES[i])return i;return 0;};
const xpPct=xp=>{const l=getLv(xp);if(l>=THRES.length-1)return 100;return Math.round(((xp-THRES[l])/(THRES[l+1]-THRES[l]))*100);};
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
      <div class="flex-1"><span class="font-semibold">${u.name}</span><span class="text-xs text-gray-400 ml-2">Lv.${u.level}</span></div>
      <div class="text-right"><div class="text-green-400 font-mono font-bold">${fmtTime(u.session_secs)}</div><div class="text-xs text-gray-400">Hôm nay: ${fmtTime(u.today_total)}</div></div>
    </div>`).join('');}
  }catch(e){console.error(e);}
  buildHeatmap(data);buildBadges(data);
  const sorted=Object.entries(data).filter(([,u])=>(u.daily[today]||0)>0).sort(([,a],[,b])=>(b.daily[today]||0)-(a.daily[today]||0)).slice(0,10);
  const medals=['🥇','🥈','🥉'];
  document.getElementById('leaderboard').innerHTML=sorted.length?sorted.map(([,u],i)=>{const lv=u.level||0,pct=xpPct(u.xp||0);return `<div class="flex items-center gap-3 py-2 px-1 rounded-xl hover:bg-slate-700 transition">
    <span class="text-xl w-7 text-center">${medals[i]||`${i+1}.`}</span>
    <div class="flex-1 min-w-0"><div class="font-semibold truncate">${u.name}</div>
      <div class="text-xs text-gray-400">Lv.${lv} · ${(u.xp||0).toLocaleString()} XP · 🔥${u.streak||0}</div>
      <div class="w-full bg-slate-700 rounded-full h-1 mt-1"><div style="width:${pct}%" class="xp-bar h-1 rounded-full"></div></div></div>
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

@flask_app.route('/')
def dashboard(): return render_template_string(DASHBOARD_HTML)

@flask_app.route('/api/stats')
def api_stats(): return jsonify(load_data())

@flask_app.route('/api/live')
def api_live(): return jsonify(_live_state_cache)

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
    for mid, start in join_times.items():
        uid      = str(mid)
        info     = data.get(uid, {})
        saved    = info.get('daily', {}).get(today, 0)
        if mid in media_active_members:
            chk     = last_checkpoint.get(mid, start)
            unsaved = int((now - chk).total_seconds())
        else:
            unsaved = 0
        is_stream = is_video = False
        for guild in bot.guilds:
            m = guild.get_member(mid)
            if m and m.voice:
                is_stream = bool(m.voice.self_stream)
                is_video  = bool(m.voice.self_video)
                break
        result.append({
            'name':         info.get('name', f'User {mid}'),
            'level':        info.get('level', 0),
            'session_secs': int((now - start).total_seconds()),
            'today_total':  saved + unsaved,
            'is_streaming': is_stream,
            'is_video':     is_video,
        })
    result.sort(key=lambda x: x['today_total'], reverse=True)
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

bot.run(TOKEN)