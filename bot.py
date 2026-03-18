"""
╔══════════════════════════════════════════════════════════════╗
║              STUDY BOT - PHIÊN BẢN NÂNG CẤP                ║
║  ✅ Fix: Lưu thời gian định kỳ (không mất data khi crash)   ║
║  🔴 Live: Cập nhật tin nhắn trực tiếp trong kênh nhóm       ║
║  ⏰ Milestone: DM + thông báo kênh khi đạt mốc thời gian    ║
║  📊 Dashboard: Giao diện web xem thống kê                   ║
╚══════════════════════════════════════════════════════════════╝
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import logging
import os
import json
import random
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string
from pomodoro import create_pomodoro_cog

try:
    from openai import OpenAI
    OPENROUTER_AVAILABLE = True
except ImportError:
    OPENROUTER_AVAILABLE = False

# ─── CONFIG ─────────────────────────────────────────────────────────────────

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

WARN_BEFORE_KICK      = 10       # giây cảnh báo trước khi kick
WAIT_SECONDS          = 60       # giây chờ trước khi kick khi không stream
REPORT_HOUR           = 23       # giờ gửi báo cáo cuối ngày
REPORT_MINUTE         = 0
DATA_FILE             = 'study_data.json'
DASHBOARD_PORT        = 5000
ABSENT_DAYS_WARN      = 2        # số ngày vắng trước khi cảnh báo
CHECKPOINT_MINUTES    = 5        # ⭐ Lưu thời gian mỗi N phút (FIX BUG CHÍNH)
LIVE_UPDATE_MINUTES   = 5        # Cập nhật tin nhắn live mỗi N phút

# Các mốc thời gian nhận thông báo (phút)
MILESTONE_MINUTES = [30, 60, 120, 180, 240, 300, 360]

XP_PER_MINUTE    = 10
LEVEL_THRESHOLDS = [0, 100, 300, 600, 1000, 1500, 2500, 4000, 6000, 9000, 13000]
LEVEL_NAMES      = [
    'Người mới 🌱', 'Học sinh 📖', 'Chăm chỉ ✏️', 'Tập trung 🎯',
    'Xuất sắc ⭐', 'Tinh anh 💎', 'Huyền thoại 🔮', 'Bậc thầy 🧠',
    'Thiên tài 🚀', 'Vô địch 👑', 'Thần học ⚡'
]

# ─── ROLE CONFIG ─────────────────────────────────────────────────────────────
# Tên role trong Discord tương ứng với từng level.
# ⚠️  Bạn phải tạo sẵn các role này trong server Settings → Roles.
# ⚠️  Bot cần có vai trò cao hơn tất cả các role bên dưới.
# Đặt None nếu level đó không có role.
LEVEL_ROLES: dict[int, str | None] = {
    0:  None,               # Người mới – không role
    1:  'Học Sinh',         # 100 XP
    2:  'Chăm Chỉ',         # 300 XP
    3:  'Tập Trung',        # 600 XP
    4:  'Xuất Sắc',         # 1 000 XP
    5:  'Tinh Anh',         # 1 500 XP
    6:  'Huyền Thoại',      # 2 500 XP
    7:  'Bậc Thầy',         # 4 000 XP
    8:  'Thiên Tài',        # 6 000 XP
    9:  'Vô Địch',          # 9 000 XP
    10: 'Thần Học',         # 13 000 XP
}

MOTIVATIONS = [
    "💪 Hôm nay cố lên! Mỗi phút học là một bước tiến!",
    "🔥 Chăm chỉ hôm nay, thành công ngày mai!",
    "📚 Kiến thức là sức mạnh, hãy tích lũy từng ngày!",
    "⭐ Bạn đang làm rất tốt! Tiếp tục phát huy nhé!",
    "🎯 Tập trung! Mục tiêu của bạn đang chờ phía trước!",
    "🚀 Mỗi giờ học hôm nay là đầu tư cho tương lai!",
    "🌟 Không có thành công nào mà không có nỗ lực!",
    "💡 Hãy học như hôm nay là ngày cuối cùng bạn được học!",
]

# Tin nhắn DM khi đạt mốc thời gian
MILESTONE_DM = {
    30:  "⏰ Bạn đã học được **30 phút** rồi! Giữ vững phong độ nhé! 💪",
    60:  "🌟 **1 tiếng** học tập! Thật xuất sắc! Uống nước và nghỉ ngơi 5 phút nhé!",
    120: "🔥 **2 tiếng** học liên tục! Bạn thật kiên trì! Nhớ vươn vai và nghỉ ngơi!",
    180: "💎 **3 tiếng** học tập! Bạn phi thường! Cẩn thận mỏi mắt nhé!",
    240: "🚀 **4 tiếng** học tập! Chiến binh thực thụ! Hãy đứng dậy đi lại một chút!",
    300: "👑 **5 tiếng** học tập! Vô địch! Chắc chắn nghỉ ngơi đầy đủ nhé!",
    360: "⚡ **6 tiếng** học tập! Huyền thoại! Đừng quên chăm sóc bản thân!",
}

# Tin nhắn thông báo kênh nhóm khi đạt mốc
MILESTONE_ANNOUNCE = {
    30:  "⏰ {name} đã đạt mốc **30 phút** học tập hôm nay! 💪",
    60:  "🌟 {name} vừa học được **1 tiếng**! Thật xuất sắc! ⭐",
    120: "🔥 {name} đã học **2 tiếng** liên tục! Quá kiên trì! 🎯",
    180: "💎 {name} học **3 tiếng** rồi! Phi thường! 🔮",
    240: "🚀 {name} đạt mốc **4 tiếng** học tập! Chiến binh! 🧠",
    300: "👑 {name} học **5 tiếng**! Vô địch server! 🏆",
    360: "⚡ {name} học **6 tiếng**! HUYỀN THOẠI! 🎊",
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
        logging.StreamHandler()
    ]
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
    ai_client = OpenAI(
        base_url='https://openrouter.ai/api/v1',
        api_key=OPENROUTER_API_KEY
    )

# ─── STATE TRACKING ──────────────────────────────────────────────────────────

pending_checks: dict[int, asyncio.Task] = {}
join_times: dict[int, datetime]         = {}   # member_id → thời điểm bắt đầu phiên học
last_checkpoint: dict[int, datetime]    = {}   # member_id → lần checkpoint gần nhất
milestone_sent: dict[int, set]          = {}   # member_id → set mốc đã thông báo (phút)
live_message_ids: dict[int, int]        = {}   # channel_id → message_id của tin nhắn live

# ─── DATA HELPERS ────────────────────────────────────────────────────────────

def load_data() -> dict:
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log.error(f'Lỗi đọc file dữ liệu: {e}')
    return {}

def save_data(data: dict):
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except IOError as e:
        log.error(f'Lỗi lưu file dữ liệu: {e}')

def _default_user(name: str) -> dict:
    return {
        'name': name, 'daily': {}, 'total': 0,
        'xp': 0, 'level': 0, 'streak': 0,
        'longest_streak': 0, 'last_study_date': '',
        'goal': None, 'goal_seconds': 0,
        'last_absent_warn': ''
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

    data[uid]['streak']          = streak
    data[uid]['longest_streak']  = max(streak, data[uid].get('longest_streak', 0))
    data[uid]['last_study_date'] = today
    return streak, True

def add_study_time(member_id: int, member_name: str, seconds: int) -> dict:
    """Cộng dồn thời gian học vào DB. Có thể gọi nhiều lần (checkpoint)."""
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

    xp_gained = (seconds // 60) * XP_PER_MINUTE
    old_xp    = data[uid].get('xp', 0)
    old_level = get_level(old_xp)

    streak, is_new_day = _update_streak(data, uid, today)
    if is_new_day and streak > 1:
        xp_gained += streak * 5  # bonus streak

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

def format_time(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m}m"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def get_report_channel_for(member: discord.Member) -> discord.TextChannel | None:
    """Tìm kênh báo cáo phù hợp với server của member."""
    guild_id = member.guild.id
    for server in SERVERS:
        channel = bot.get_channel(server['report_channel'])
        if channel and channel.guild.id == guild_id:
            return channel
    return None

# ─── ROLE MANAGEMENT ─────────────────────────────────────────────────────────

async def assign_level_role(member: discord.Member, new_level: int, old_level: int):
    """
    Tự động gán role mới và thu hồi role cũ khi member lên/xuống level.
    Truyền old_level=-1 để bắt buộc đồng bộ (dùng cho /syncroles).
    Hoạt động im lặng nếu role không tồn tại trong server.
    """
    if new_level == old_level:
        return

    guild = member.guild

    # Thu hồi tất cả role level cũ mà member đang giữ
    for lvl, role_name in LEVEL_ROLES.items():
        if role_name is None:
            continue
        role = discord.utils.get(guild.roles, name=role_name)
        if role and role in member.roles and lvl != new_level:
            try:
                await member.remove_roles(role, reason=f'Level up → Lv.{new_level}')
                log.info(f'Thu hồi role "{role_name}" từ {member.display_name}')
            except discord.Forbidden:
                log.warning(f'Thiếu quyền thu hồi role "{role_name}"')
            except Exception as e:
                log.error(f'Lỗi thu hồi role: {e}')

    # Gán role mới
    new_role_name = LEVEL_ROLES.get(new_level)
    if new_role_name is None:
        return

    new_role = discord.utils.get(guild.roles, name=new_role_name)
    if new_role is None:
        log.warning(
            f'Role "{new_role_name}" không tồn tại trong server "{guild.name}". '
            f'Hãy tạo role này trong Settings → Roles.'
        )
        return

    if new_role not in member.roles:
        try:
            await member.add_roles(new_role, reason=f'Đạt Lv.{new_level}')
            log.info(f'Gán role "{new_role_name}" cho {member.display_name} (Lv.{new_level})')
        except discord.Forbidden:
            log.warning(f'Thiếu quyền gán role "{new_role_name}" – kiểm tra thứ tự role của bot.')
        except Exception as e:
            log.error(f'Lỗi gán role: {e}')


# ─── SESSION MANAGEMENT ──────────────────────────────────────────────────────

def record_join(member: discord.Member):
    """Ghi nhận thời điểm bắt đầu phiên học."""
    join_times[member.id]  = datetime.now()
    milestone_sent[member.id] = set()
    log.info(f'{member.display_name} bắt đầu học lúc {join_times[member.id].strftime("%H:%M:%S")}')

async def _do_checkpoint(member: discord.Member) -> int:
    """
    ⭐ CORE FIX: Lưu thời gian từ checkpoint trước đến bây giờ vào DB.
    Trả về số giây đã lưu trong lần này.
    """
    if member.id not in join_times:
        return 0

    now        = datetime.now()
    checkpoint = last_checkpoint.get(member.id, join_times[member.id])
    elapsed    = int((now - checkpoint).total_seconds())

    if elapsed > 0:
        add_study_time(member.id, member.display_name, elapsed)
        last_checkpoint[member.id] = now
        log.info(f'[Checkpoint] {member.display_name}: +{format_time(elapsed)} đã lưu')

    return elapsed

async def _check_milestones(member: discord.Member):
    """Kiểm tra và gửi thông báo DM + kênh nhóm khi đạt mốc thời gian."""
    if member.id not in join_times:
        return

    total_minutes = int((datetime.now() - join_times[member.id]).total_seconds()) // 60
    if member.id not in milestone_sent:
        milestone_sent[member.id] = set()

    for milestone in MILESTONE_MINUTES:
        if total_minutes >= milestone and milestone not in milestone_sent[member.id]:
            milestone_sent[member.id].add(milestone)

            # Gửi DM riêng
            dm_msg = MILESTONE_DM.get(milestone, f"⏰ Bạn đã học được **{milestone} phút**!")
            await safe_send_dm(member, dm_msg)

            # Thông báo kênh nhóm
            announce_msg = MILESTONE_ANNOUNCE.get(
                milestone, f"⏰ {member.display_name} đã học **{milestone} phút**!"
            ).format(name=f'**{member.display_name}**')

            channel = get_report_channel_for(member)
            if channel:
                try:
                    await channel.send(announce_msg)
                except Exception as e:
                    log.error(f'Lỗi gửi milestone announcement: {e}')

            log.info(f'Milestone {milestone}p: {member.display_name}')

async def record_leave_and_notify(member: discord.Member) -> int:
    """
    Lưu phần thời gian còn lại kể từ checkpoint cuối, gửi DM tổng kết,
    dọn dẹp tracking state. Trả về tổng thời gian phiên (giây).
    """
    if member.id not in join_times:
        return 0

    # Lưu phần còn lại từ checkpoint cuối
    now        = datetime.now()
    checkpoint = last_checkpoint.get(member.id, join_times[member.id])
    remaining  = int((now - checkpoint).total_seconds())
    if remaining > 0:
        add_study_time(member.id, member.display_name, remaining)

    # Tổng thời gian phiên (tính từ lúc join)
    total_duration = int((now - join_times.pop(member.id)).total_seconds())
    last_checkpoint.pop(member.id, None)
    milestone_sent.pop(member.id, None)

    # Gửi DM tổng kết nếu đủ dài
    if total_duration > 30:
        data  = load_data()
        uid   = str(member.id)
        today = now.strftime('%Y-%m-%d')

        if uid in data:
            info       = data[uid]
            xp         = info.get('xp', 0)
            level      = info.get('level', 0)
            streak     = info.get('streak', 0)
            today_secs = info['daily'].get(today, 0)
            goal       = info.get('goal')
            goal_secs  = info.get('goal_seconds', 0)
            _, xp_need = xp_to_next_level(xp)
            level_name = LEVEL_NAMES[level]

            # XP ước tính của phiên này
            xp_session = (total_duration // 60) * XP_PER_MINUTE

            msg = (
                f'✅ **Phiên học kết thúc!**\n'
                f'──────────────────\n'
                f'⏱️ Phiên này: `{format_time(total_duration)}`\n'
                f'📅 Hôm nay tổng: `{format_time(today_secs)}`\n'
                f'⚡ XP nhận được: `~+{xp_session} XP`\n'
                f'📊 Level: `Lv.{level} {level_name}` _(còn {xp_need} XP để lên level)_\n'
                f'🔥 Streak: `{streak} ngày liên tiếp`'
            )

            if goal and goal_secs > 0:
                progress = min(100, int((today_secs / goal_secs) * 100))
                bar      = '█' * (progress // 10) + '░' * (10 - progress // 10)
                msg += (
                    f'\n──────────────────\n'
                    f'🎯 Mục tiêu: **"{goal}"**\n'
                    f'`{bar}` {progress}% ({format_time(today_secs)}/{format_time(goal_secs)})'
                )

            # Level up check + gán role
            old_level_data = get_level(xp - xp_session)
            if level > old_level_data:
                msg += f'\n\n🎉 **LEVEL UP!** Bạn đã lên **Lv.{level} {level_name}**! 🎊'
                new_role_name = LEVEL_ROLES.get(level)
                if new_role_name:
                    msg += f'\n🏷️ Vai trò mới: **{new_role_name}**'
                await assign_level_role(member, level, old_level_data)

            await safe_send_dm(member, msg)

    return total_duration

# ─── LIVE MESSAGE UPDATE ─────────────────────────────────────────────────────

async def update_live_message(server: dict):
    """
    Cập nhật (hoặc tạo mới) tin nhắn 🔴 LIVE trong kênh báo cáo.
    Hiển thị danh sách người đang học và thời gian tích lũy.
    """
    channel = bot.get_channel(server['report_channel'])
    if not channel:
        return

    now        = datetime.now()
    guild      = channel.guild
    voice_ids  = server['voice_channels']

    # Thu thập người đang học trong server này
    active_members = []
    for member_id, start_time in list(join_times.items()):
        member = guild.get_member(member_id)
        if not member or not member.voice or not member.voice.channel:
            continue
        if member.voice.channel.id not in voice_ids:
            continue

        is_streaming = member.voice.self_stream
        session_secs = int((now - start_time).total_seconds())

        # Lấy tổng thời gian hôm nay từ DB + thời gian phiên hiện tại chưa checkpoint
        data  = load_data()
        uid   = str(member_id)
        today = now.strftime('%Y-%m-%d')
        today_saved = data.get(uid, {}).get('daily', {}).get(today, 0)
        checkpoint  = last_checkpoint.get(member_id, start_time)
        unsaved     = int((now - checkpoint).total_seconds())
        today_total = today_saved + unsaved  # tổng = đã lưu + chưa lưu

        active_members.append({
            'member':       member,
            'session_secs': session_secs,
            'today_total':  today_total,
            'is_streaming': is_streaming,
        })

    # Sắp xếp theo thời gian hôm nay (nhiều nhất lên trên)
    active_members.sort(key=lambda x: x['today_total'], reverse=True)

    # Xây dựng nội dung tin nhắn
    lines = [
        f'🔴 **ĐANG HỌC TRỰC TIẾP** · `{now.strftime("%H:%M:%S")}`',
        '━━━━━━━━━━━━━━━━━━━━━━━━',
    ]

    if not active_members:
        lines.append('😴 _Hiện tại không có ai trong phòng học..._')
    else:
        for i, info in enumerate(active_members, 1):
            m             = info['member']
            session_str   = format_time(info['session_secs'])
            today_str     = format_time(info['today_total'])
            stream_icon   = '📺' if info['is_streaming'] else '⏸️'
            rank_icon     = ['🥇', '🥈', '🥉'][i - 1] if i <= 3 else f'`{i}.`'

            lines.append(
                f'{rank_icon} {stream_icon} **{m.display_name}** '
                f'| Phiên: `{session_str}` | Hôm nay: `{today_str}`'
            )

    total_today = sum(a['today_total'] for a in active_members)
    lines += [
        '━━━━━━━━━━━━━━━━━━━━━━━━',
        f'👥 Đang học: `{len(active_members)} người` · '
        f'Tổng hôm nay: `{format_time(total_today)}`',
        f'_⟳ Tự cập nhật mỗi {LIVE_UPDATE_MINUTES} phút_',
    ]

    content = '\n'.join(lines)

    # Chỉnh sửa tin nhắn cũ hoặc tạo mới
    try:
        msg_id = live_message_ids.get(channel.id)
        if msg_id:
            try:
                old_msg = await channel.fetch_message(msg_id)
                await old_msg.edit(content=content)
                return
            except discord.NotFound:
                live_message_ids.pop(channel.id, None)

        new_msg = await channel.send(content)
        live_message_ids[channel.id] = new_msg.id
        log.info(f'Tạo live message mới tại kênh {channel.name}')

    except Exception as e:
        log.error(f'Lỗi cập nhật live message [{channel.name}]: {e}')

async def update_all_live_messages():
    """Cập nhật live message cho tất cả server."""
    for server in SERVERS:
        await update_live_message(server)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

async def safe_send_dm(member: discord.Member, message: str):
    try:
        await member.send(message)
    except discord.Forbidden:
        log.warning(f'Không thể gửi DM cho {member.display_name} (chặn DM)')
    except discord.HTTPException as e:
        log.error(f'Lỗi HTTP gửi DM: {e}')

def bot_can_move(member: discord.Member) -> bool:
    if not member.guild.me.guild_permissions.move_members:
        log.error('Bot thiếu quyền Move Members!')
        return False
    return True

def cancel_task(member_id: int):
    task = pending_checks.pop(member_id, None)
    if task and not task.done():
        task.cancel()

def start_check(member: discord.Member, reason: str):
    cancel_task(member.id)
    task = asyncio.create_task(check_stream(member))
    pending_checks[member.id] = task
    log.info(f'{member.display_name} {reason} → đếm ngược {WAIT_SECONDS}s.')

# ─── STREAM CHECK ────────────────────────────────────────────────────────────

async def check_stream(member: discord.Member):
    """Kiểm tra stream sau WAIT_SECONDS. Kick nếu vẫn không stream."""
    try:
        await asyncio.sleep(WAIT_SECONDS - WARN_BEFORE_KICK)

        if not (member.voice and member.voice.channel and
                member.voice.channel.id in FOCUS_CHANNEL_IDS):
            return
        if member.voice.self_stream:
            return

        await safe_send_dm(member,
            f'⚠️ **Cảnh báo!** Bạn chưa bật stream màn hình trong phòng học.\n'
            f'Bạn sẽ bị kick sau **{WARN_BEFORE_KICK} giây** nếu không bật stream!'
        )
        await asyncio.sleep(WARN_BEFORE_KICK)

        if not (member.voice and member.voice.channel and
                member.voice.channel.id in FOCUS_CHANNEL_IDS):
            return

        if not member.voice.self_stream:
            if not bot_can_move(member):
                return
            await record_leave_and_notify(member)
            await member.move_to(None)
            log.info(f'Đã kick {member.display_name} vì không stream.')
            await safe_send_dm(member,
                '🚫 Bạn đã bị mời ra khỏi phòng vì **không bật stream màn hình**.\n'
                'Vui lòng bật stream khi vào lại phòng!'
            )

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f'Lỗi check_stream với {member.display_name}: {e}')
    finally:
        pending_checks.pop(member.id, None)

# ─── SCHEDULED TASKS ─────────────────────────────────────────────────────────

@tasks.loop(minutes=1)
async def scheduled_tasks():
    """Chạy mỗi phút: kiểm tra báo cáo cuối ngày và vắng mặt."""
    now = datetime.now()
    if now.hour == REPORT_HOUR and now.minute == REPORT_MINUTE:
        await _send_report()
    if now.hour == 9 and now.minute == 0:
        await _check_absences()

@tasks.loop(minutes=CHECKPOINT_MINUTES)
async def checkpoint_task():
    """
    ⭐ TASK CHÍNH FIX BUG:
    Mỗi {CHECKPOINT_MINUTES} phút: checkpoint tất cả thành viên đang stream
    và cập nhật tin nhắn live trong kênh nhóm.
    """
    now = datetime.now()
    log.info(f'[{now.strftime("%H:%M")}] Chạy checkpoint định kỳ...')

    for member_id in list(join_times.keys()):
        # Tìm member trong tất cả guilds
        member = None
        for guild in bot.guilds:
            m = guild.get_member(member_id)
            if m and m.voice and m.voice.channel and \
               m.voice.channel.id in FOCUS_CHANNEL_IDS:
                member = m
                break

        if not member:
            continue

        # Chỉ checkpoint nếu đang stream (đang học chủ động)
        if member.voice and member.voice.self_stream:
            await _do_checkpoint(member)
            await _check_milestones(member)

    # Cập nhật live messages và cache Flask sau checkpoint
    await update_all_live_messages()
    _update_live_cache()

# ─── REPORTS ─────────────────────────────────────────────────────────────────

async def _send_report():
    data        = load_data()
    today       = datetime.now().strftime('%Y-%m-%d')
    sorted_data = sorted(data.items(), key=lambda x: x[1]['daily'].get(today, 0), reverse=True)
    lines       = [f'📊 **Báo cáo học tập ngày {today}**\n']
    has_data    = False

    for i, (uid, info) in enumerate(sorted_data, 1):
        today_time = info['daily'].get(today, 0)
        if today_time > 0:
            has_data = True
            medal    = ['🥇', '🥈', '🥉'][i - 1] if i <= 3 else f'`{i}.`'
            lines.append(
                f'{medal} **{info["name"]}** `Lv.{info.get("level", 0)}` '
                f'🔥{info.get("streak", 0)} — '
                f'Hôm nay: `{format_time(today_time)}` | '
                f'Tổng: `{format_time(info.get("total", 0))}`'
            )

    if not has_data:
        lines.append('😴 Hôm nay chưa có ai học!')

    message = '\n'.join(lines)
    for server in SERVERS:
        channel = bot.get_channel(server['report_channel'])
        if channel:
            await channel.send(message)

async def _check_absences():
    data      = load_data()
    today     = datetime.now().strftime('%Y-%m-%d')
    warn_date = (datetime.now() - timedelta(days=ABSENT_DAYS_WARN)).strftime('%Y-%m-%d')

    for uid, info in data.items():
        last_date   = info.get('last_study_date', '')
        last_warned = info.get('last_absent_warn', '')
        if not last_date or last_date >= warn_date or last_warned == today:
            continue
        for guild in bot.guilds:
            member = guild.get_member(int(uid))
            if not member:
                continue
            days_absent = (datetime.now() - datetime.strptime(last_date, '%Y-%m-%d')).days
            await safe_send_dm(member,
                f'😢 **Ơi {member.display_name}!**\n'
                f'Bạn đã **không học trong {days_absent} ngày** rồi!\n'
                f'🔥 Streak hiện tại: `{info.get("streak", 0)} ngày`\n'
                f'💪 Vào phòng học ngay trước khi streak bị reset nhé!'
            )
            data[uid]['last_absent_warn'] = today
            save_data(data)
            break

# ─── AI HELPER ───────────────────────────────────────────────────────────────

async def _ask_ai(question: str) -> str:
    if not ai_client:
        return '❌ Chức năng AI chưa được cấu hình (thiếu `OPENROUTER_API_KEY` trong .env).'
    try:
        response = await asyncio.to_thread(
            ai_client.chat.completions.create,
            model='openrouter/auto',
            messages=[
                {
                    'role': 'system',
                    'content': (
                        'Bạn là trợ lý học tập thông minh trong Discord server học tập. '
                        'Trả lời ngắn gọn, dễ hiểu bằng tiếng Việt. '
                        'Dùng emoji phù hợp. Tối đa 400 từ.'
                    )
                },
                {'role': 'user', 'content': question}
            ]
        )
        msg = f'🤖 **Câu hỏi:** {question}\n\n📝 **Trả lời:**\n{response.choices[0].message.content}'
        return msg[:1990] + '...' if len(msg) > 2000 else msg
    except Exception as e:
        log.error(f'Lỗi OpenRouter AI: {e}')
        return '❌ Có lỗi xảy ra khi gọi AI. Thử lại sau nhé!'

# ─── SLASH COMMANDS ───────────────────────────────────────────────────────────

def _build_rank_message(target: discord.Member, data: dict, join_times: dict, last_checkpoint: dict) -> str:
    """Tạo tin nhắn /rank với thanh XP trực quan."""
    uid  = str(target.id)
    now  = datetime.now()

    if uid not in data:
        return f'❌ **{target.display_name}** chưa có dữ liệu học tập!'

    info    = data[uid]
    xp      = info.get('xp', 0)
    level   = info.get('level', 0)
    streak  = info.get('streak', 0)
    longest = info.get('longest_streak', 0)
    total   = info.get('total', 0)
    today   = now.strftime('%Y-%m-%d')

    # Tính thời gian thực tế hôm nay (DB + chưa checkpoint)
    today_saved = info['daily'].get(today, 0)
    if target.id in join_times:
        chk         = last_checkpoint.get(target.id, join_times[target.id])
        today_total = today_saved + int((now - chk).total_seconds())
    else:
        today_total = today_saved

    # XP bar
    lv_now   = get_level(xp)
    if lv_now >= len(LEVEL_THRESHOLDS) - 1:
        xp_cur     = xp - LEVEL_THRESHOLDS[lv_now]
        xp_needed  = 0
        bar_filled = 20
        pct        = 100
    else:
        xp_start   = LEVEL_THRESHOLDS[lv_now]
        xp_end     = LEVEL_THRESHOLDS[lv_now + 1]
        xp_cur     = xp - xp_start
        xp_needed  = xp_end - xp
        span       = xp_end - xp_start
        pct        = int((xp_cur / span) * 100)
        bar_filled = int((xp_cur / span) * 20)

    xp_bar = '█' * bar_filled + '░' * (20 - bar_filled)

    # Role hiện tại
    role_name  = LEVEL_ROLES.get(level)
    role_str   = f'🏷️ Vai trò: **{role_name}**\n' if role_name else ''

    # Role kế tiếp
    next_role_level = next(
        (lv for lv in range(level + 1, len(LEVEL_ROLES)) if LEVEL_ROLES.get(lv)),
        None
    )
    next_role_str = ''
    if next_role_level is not None and xp_needed > 0:
        next_role_name = LEVEL_ROLES[next_role_level]
        xp_to_next_role = LEVEL_THRESHOLDS[next_role_level] - xp
        next_role_str = f'🎯 Cần `{xp_to_next_role} XP` để nhận vai trò **{next_role_name}**\n'

    # 5 ngày gần nhất
    recent_days  = sorted(info['daily'].items(), reverse=True)[:5]
    recent_lines = ' · '.join([f'`{d[5:]}`{format_time(s)}' for d, s in recent_days])

    msg = (
        f'╔══════════════════════════════╗\n'
        f'   🎓 **{target.display_name}**\n'
        f'╚══════════════════════════════╝\n'
        f'🏅 **Lv.{level}** {LEVEL_NAMES[level]}\n'
        f'{role_str}'
        f'──────────────────────────────\n'
        f'⚡ **XP:** `{xp:,}` _({xp_cur}/{xp_cur + xp_needed if xp_needed else "MAX"})_\n'
        f'`{xp_bar}` **{pct}%**\n'
        f'{f"_(còn **{xp_needed} XP** để lên Lv.{lv_now + 1})_" if xp_needed > 0 else "_✨ Đã đạt level tối đa!_"}\n'
        f'{next_role_str}'
        f'──────────────────────────────\n'
        f'🔥 Streak: `{streak} ngày` _(kỷ lục: {longest} ngày)_\n'
        f'🕐 Hôm nay: `{format_time(today_total)}`\n'
        f'📚 Tổng cộng: `{format_time(total)}`\n'
        f'──────────────────────────────\n'
        f'📅 Gần nhất: {recent_lines}'
    )
    return msg


@bot.tree.command(name='rank', description='Xem bảng XP và vai trò hiện tại của bạn')
@app_commands.describe(member='Thành viên muốn xem (để trống = bản thân)')
async def slash_rank(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    data   = load_data()
    msg    = _build_rank_message(target, data, join_times, last_checkpoint)
    await interaction.response.send_message(msg, ephemeral=(member is None))


@bot.tree.command(name='roles', description='Xem danh sách tất cả vai trò và mốc XP cần đạt')
async def slash_roles(interaction: discord.Interaction):
    data  = load_data()
    uid   = str(interaction.user.id)
    my_xp = data.get(uid, {}).get('xp', 0) if uid in data else 0
    my_lv = get_level(my_xp)

    lines = ['🏷️ **Danh sách vai trò theo level**\n']
    for lv, role_name in LEVEL_ROLES.items():
        if role_name is None:
            continue
        xp_req  = LEVEL_THRESHOLDS[lv]
        is_mine = (lv == my_lv)
        is_done = (my_lv > lv)
        status  = ' ◀ **bạn đang ở đây**' if is_mine else (' ✅' if is_done else '')
        lines.append(
            f'{"✦" if is_mine else ("✔" if is_done else "○")} '
            f'Lv.**{lv}** `{xp_req:,} XP` → **{role_name}**{status}'
        )

    lines.append(f'\n💡 Dùng `/rank` để xem tiến độ XP chi tiết.')
    await interaction.response.send_message('\n'.join(lines), ephemeral=True)


@bot.tree.command(name='stats', description='Xem thống kê thời gian học của bạn')
@app_commands.describe(member='Thành viên muốn xem (để trống = bản thân)')
async def slash_stats(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    data   = load_data()
    uid    = str(target.id)

    if uid not in data:
        await interaction.response.send_message(
            f'❌ **{target.display_name}** chưa có dữ liệu học tập!', ephemeral=True
        )
        return

    info       = data[uid]
    today      = datetime.now().strftime('%Y-%m-%d')
    today_saved = info['daily'].get(today, 0)
    xp         = info.get('xp', 0)
    level      = info.get('level', 0)
    streak     = info.get('streak', 0)
    longest    = info.get('longest_streak', 0)
    _, xp_need = xp_to_next_level(xp)
    goal       = info.get('goal')
    goal_secs  = info.get('goal_seconds', 0)
    recent     = sorted(info['daily'].items(), reverse=True)[:7]
    recent_str = '\n'.join([f'  `{d}`: {format_time(s)}' for d, s in recent])

    # Thêm thời gian phiên hiện tại (chưa checkpoint) nếu đang học
    current_session = ''
    if target.id in join_times:
        checkpoint  = last_checkpoint.get(target.id, join_times[target.id])
        unsaved_sec = int((datetime.now() - checkpoint).total_seconds())
        today_total = today_saved + unsaved_sec
        session_sec = int((datetime.now() - join_times[target.id]).total_seconds())
        current_session = (
            f'\n🟢 **Đang học:** Phiên `{format_time(session_sec)}` '
            f'| Hôm nay thực tế: `{format_time(today_total)}`'
        )
    else:
        today_total = today_saved

    msg = (
        f'📊 **Thống kê của {target.display_name}**\n'
        f'🏅 Level: `Lv.{level} {LEVEL_NAMES[level]}`\n'
        f'⚡ XP: `{xp}` _(còn {xp_need} XP để lên level)_\n'
        f'🔥 Streak: `{streak} ngày` _(kỷ lục: {longest} ngày)_\n'
        f'🕐 Hôm nay: `{format_time(today_total)}`\n'
        f'📚 Tổng cộng: `{format_time(info.get("total", 0))}`\n'
    )
    if goal and goal_secs > 0:
        progress = min(100, int((today_total / goal_secs) * 100))
        msg += f'🎯 Mục tiêu: **"{goal}"** — `{progress}%`\n'
    msg += f'{current_session}\n📅 7 ngày gần nhất:\n{recent_str}'

    await interaction.response.send_message(msg, ephemeral=True)


@bot.tree.command(name='leaderboard', description='Xem bảng xếp hạng hôm nay')
async def slash_leaderboard(interaction: discord.Interaction):
    data  = load_data()
    today = datetime.now().strftime('%Y-%m-%d')
    now   = datetime.now()

    # Tính thời gian thực tế (DB + chưa checkpoint)
    def get_real_today(uid_str, info):
        saved = info['daily'].get(today, 0)
        mid   = int(uid_str)
        if mid in join_times:
            chk    = last_checkpoint.get(mid, join_times[mid])
            saved += int((now - chk).total_seconds())
        return saved

    all_users = [
        (uid, info, get_real_today(uid, info))
        for uid, info in data.items()
    ]
    top10 = sorted(
        [(u, i, t) for u, i, t in all_users if t > 0],
        key=lambda x: x[2], reverse=True
    )[:10]

    lines = ['🏆 **Bảng xếp hạng hôm nay** _(bao gồm phiên đang học)_\n']
    if not top10:
        lines.append('😴 Hôm nay chưa có ai học!')
    else:
        medals = ['🥇', '🥈', '🥉']
        for i, (uid, info, real_time) in enumerate(top10, 1):
            medal      = medals[i - 1] if i <= 3 else f'`{i}.`'
            is_active  = int(uid) in join_times
            status     = ' 🟢' if is_active else ''
            lines.append(
                f'{medal}{status} **{info["name"]}** `Lv.{info.get("level", 0)}` '
                f'🔥{info.get("streak", 0)} — `{format_time(real_time)}`'
            )
    await interaction.response.send_message('\n'.join(lines))


@bot.tree.command(name='studying', description='Xem danh sách người đang học ngay lúc này')
async def slash_studying(interaction: discord.Interaction):
    """Lệnh mới: hiển thị ai đang học và thời gian phiên hiện tại."""
    now    = datetime.now()
    guild  = interaction.guild
    lines  = ['🟢 **Đang học ngay lúc này**\n']
    count  = 0

    for member_id, start_time in sorted(join_times.items()):
        member = guild.get_member(member_id) if guild else None
        if not member or not member.voice:
            continue

        session_secs = int((now - start_time).total_seconds())
        is_streaming = member.voice.self_stream
        stream_icon  = '📺' if is_streaming else '⏸️'
        count += 1

        # Tổng hôm nay (DB + unsaved)
        data  = load_data()
        uid   = str(member_id)
        today = now.strftime('%Y-%m-%d')
        saved = data.get(uid, {}).get('daily', {}).get(today, 0)
        chk   = last_checkpoint.get(member_id, start_time)
        total = saved + int((now - chk).total_seconds())

        lines.append(
            f'{stream_icon} **{member.display_name}** '
            f'| Phiên: `{format_time(session_secs)}` '
            f'| Hôm nay: `{format_time(total)}`'
        )

    if count == 0:
        lines.append('😴 Không có ai đang học...')
    else:
        lines.append(f'\n👥 Tổng cộng: `{count} người`')

    await interaction.response.send_message('\n'.join(lines))


@bot.tree.command(name='setgoal', description='Đặt mục tiêu học tập hằng ngày')
@app_commands.describe(goal='Mô tả mục tiêu (VD: Học Python)', hours='Số giờ', minutes='Số phút')
async def slash_setgoal(interaction: discord.Interaction, goal: str, hours: int = 0, minutes: int = 0):
    total = hours * 3600 + minutes * 60
    if total <= 0:
        await interaction.response.send_message('❌ Vui lòng nhập ít nhất 1 phút!', ephemeral=True)
        return
    data = load_data()
    uid  = str(interaction.user.id)
    if uid not in data:
        data[uid] = _default_user(interaction.user.display_name)
    data[uid]['goal']         = goal
    data[uid]['goal_seconds'] = total
    save_data(data)
    await interaction.response.send_message(
        f'✅ Đã đặt mục tiêu!\n🎯 **"{goal}"** — {format_time(total)}/ngày\nCố lên! 💪',
        ephemeral=True
    )


@bot.tree.command(name='ask', description='Hỏi AI về bất kỳ điều gì liên quan đến học tập')
@app_commands.describe(question='Câu hỏi của bạn')
async def slash_ask(interaction: discord.Interaction, question: str):
    await interaction.response.defer(thinking=True)
    answer = await _ask_ai(question)
    await interaction.followup.send(answer)


@bot.tree.command(name='syncroles', description='Đồng bộ vai trò cho tất cả thành viên (Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_syncroles(interaction: discord.Interaction):
    """Quét toàn bộ DB và gán/thu hồi role đúng với level hiện tại của mỗi người."""
    await interaction.response.defer(ephemeral=True)
    data    = load_data()
    guild   = interaction.guild
    updated = 0
    skipped = 0

    for uid, info in data.items():
        member = guild.get_member(int(uid))
        if not member:
            skipped += 1
            continue
        level = info.get('level', 0)
        await assign_level_role(member, level, -1)   # -1 → luôn chạy
        updated += 1

    await interaction.followup.send(
        f'✅ Đồng bộ xong!\n'
        f'👥 Đã cập nhật: `{updated}` thành viên\n'
        f'⏭️ Bỏ qua (không trong server): `{skipped}`',
        ephemeral=True
    )


@bot.tree.command(name='report', description='Gửi báo cáo ngay (chỉ Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_report(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _send_report()
    await interaction.followup.send('✅ Đã gửi báo cáo!', ephemeral=True)


@bot.tree.command(name='updatelive', description='Cập nhật tin nhắn live ngay (chỉ Admin)')
@app_commands.default_permissions(administrator=True)
async def slash_updatelive(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await update_all_live_messages()
    await interaction.followup.send('✅ Đã cập nhật tin nhắn live!', ephemeral=True)

# ─── PREFIX COMMANDS ─────────────────────────────────────────────────────────

@bot.command(name='stats')
async def cmd_stats(ctx, member: discord.Member = None):
    target = member or ctx.author
    data   = load_data()
    uid    = str(target.id)
    if uid not in data:
        await ctx.send(f'❌ **{target.display_name}** chưa có dữ liệu!')
        return
    info       = data[uid]
    today      = datetime.now().strftime('%Y-%m-%d')
    level      = info.get('level', 0)
    xp         = info.get('xp', 0)
    streak     = info.get('streak', 0)
    _, xp_need = xp_to_next_level(xp)

    # Thời gian thực (DB + unsaved)
    saved   = info['daily'].get(today, 0)
    if target.id in join_times:
        chk   = last_checkpoint.get(target.id, join_times[target.id])
        saved += int((datetime.now() - chk).total_seconds())

    recent     = sorted(info['daily'].items(), reverse=True)[:7]
    recent_str = '\n'.join([f'  `{d}`: {format_time(s)}' for d, s in recent])
    await ctx.send(
        f'📊 **Thống kê của {target.display_name}**\n'
        f'🏅 `Lv.{level} {LEVEL_NAMES[level]}` | ⚡{xp} XP _(còn {xp_need} XP)_\n'
        f'🔥 Streak: `{streak} ngày`\n'
        f'🕐 Hôm nay: `{format_time(saved)}`\n'
        f'📚 Tổng: `{format_time(info.get("total", 0))}`\n'
        f'📅 7 ngày gần:\n{recent_str}'
    )


@bot.command(name='leaderboard', aliases=['lb', 'top'])
async def cmd_leaderboard(ctx):
    data  = load_data()
    today = datetime.now().strftime('%Y-%m-%d')
    now   = datetime.now()

    def real_time(uid_str, info):
        s = info['daily'].get(today, 0)
        mid = int(uid_str)
        if mid in join_times:
            chk = last_checkpoint.get(mid, join_times[mid])
            s  += int((now - chk).total_seconds())
        return s

    top10 = sorted(
        [(uid, info, real_time(uid, info)) for uid, info in data.items() if real_time(uid, info) > 0],
        key=lambda x: x[2], reverse=True
    )[:10]

    lines = ['🏆 **Bảng xếp hạng hôm nay**\n']
    if not top10:
        lines.append('😴 Hôm nay chưa có ai học!')
    else:
        for i, (uid, info, rt) in enumerate(top10, 1):
            medal   = ['🥇', '🥈', '🥉'][i - 1] if i <= 3 else f'`{i}.`'
            is_live = ' 🟢' if int(uid) in join_times else ''
            lines.append(
                f'{medal}{is_live} **{info["name"]}** `Lv.{info.get("level", 0)}` '
                f'🔥{info.get("streak", 0)} — `{format_time(rt)}`'
            )
    await ctx.send('\n'.join(lines))


@bot.command(name='studying')
async def cmd_studying(ctx):
    """!studying - xem ai đang học ngay lúc này."""
    now   = datetime.now()
    guild = ctx.guild
    data  = load_data()
    today = now.strftime('%Y-%m-%d')
    lines = ['🟢 **Đang học ngay lúc này**\n']
    count = 0
    for member_id, start_time in sorted(join_times.items()):
        member = guild.get_member(member_id) if guild else None
        if not member or not member.voice:
            continue
        secs  = int((now - start_time).total_seconds())
        uid   = str(member_id)
        saved = data.get(uid, {}).get('daily', {}).get(today, 0)
        chk   = last_checkpoint.get(member_id, start_time)
        total = saved + int((now - chk).total_seconds())
        icon  = '📺' if member.voice.self_stream else '⏸️'
        count += 1
        lines.append(
            f'{icon} **{member.display_name}** '
            f'| Phiên: `{format_time(secs)}` | Hôm nay: `{format_time(total)}`'
        )
    if count == 0:
        lines.append('😴 Không có ai đang học...')
    else:
        lines.append(f'\n👥 Tổng: `{count} người`')
    await ctx.send('\n'.join(lines))


@bot.command(name='rank')
async def cmd_rank(ctx, member: discord.Member = None):
    """!rank [@member] – xem bảng XP và vai trò."""
    target = member or ctx.author
    data   = load_data()
    msg    = _build_rank_message(target, data, join_times, last_checkpoint)
    await ctx.send(msg)


@bot.command(name='roles')
async def cmd_roles(ctx):
    """!roles – danh sách vai trò theo level."""
    data  = load_data()
    uid   = str(ctx.author.id)
    my_xp = data.get(uid, {}).get('xp', 0) if uid in data else 0
    my_lv = get_level(my_xp)

    lines = ['🏷️ **Danh sách vai trò theo level**\n']
    for lv, role_name in LEVEL_ROLES.items():
        if role_name is None:
            continue
        xp_req  = LEVEL_THRESHOLDS[lv]
        is_mine = (lv == my_lv)
        is_done = (my_lv > lv)
        status  = ' ◀ **bạn đang ở đây**' if is_mine else (' ✅' if is_done else '')
        lines.append(
            f'{"✦" if is_mine else ("✔" if is_done else "○")} '
            f'Lv.**{lv}** `{xp_req:,} XP` → **{role_name}**{status}'
        )
    await ctx.send('\n'.join(lines))


@bot.command(name='setgoal')
async def cmd_setgoal(ctx, hours: int = 0, minutes: int = 0, *, goal: str = ''):
    if not goal:
        await ctx.send('❌ Dùng: `!setgoal <giờ> <phút> <mô tả>`\nVD: `!setgoal 2 30 Học Python`')
        return
    total = hours * 3600 + minutes * 60
    if total <= 0:
        await ctx.send('❌ Vui lòng nhập ít nhất 1 phút!')
        return
    data = load_data()
    uid  = str(ctx.author.id)
    if uid not in data:
        data[uid] = _default_user(ctx.author.display_name)
    data[uid]['goal']         = goal
    data[uid]['goal_seconds'] = total
    save_data(data)
    await ctx.send(f'✅ Đã đặt mục tiêu **"{goal}"** — {format_time(total)}/ngày! 💪')


@bot.command(name='ask')
async def cmd_ask(ctx, *, question: str = ''):
    if not question:
        await ctx.send('❌ Dùng: `!ask <câu hỏi của bạn>`')
        return
    async with ctx.typing():
        answer = await _ask_ai(question)
        await ctx.send(answer)


@bot.command(name='sync')
@commands.has_permissions(administrator=True)
async def cmd_sync(ctx):
    """!sync — Force sync toàn bộ slash commands ngay lập tức (Admin only)."""
    msg    = await ctx.send('⏳ Đang sync slash commands...')
    total  = 0
    errors = []

    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            total += len(synced)
        except Exception as e:
            errors.append(f'{guild.name}: {e}')

    result = f'✅ Sync xong! **{total}** lệnh đã được cập nhật.\n'
    result += '_Nhấn **Ctrl+R** để reload Discord nếu chưa thấy lệnh mới._'
    if errors:
        result += f'\n⚠️ Lỗi: {", ".join(errors)}'
    await msg.edit(content=result)


@bot.command(name='report')
@commands.has_permissions(administrator=True)
async def cmd_report(ctx):
    await _send_report()
    await ctx.send('✅ Đã gửi báo cáo!')

# ─── EVENTS ──────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    log.info(f'✅ Bot {bot.user.name} đã sẵn sàng!')
    log.info(f'📡 Đang theo dõi {len(FOCUS_CHANNEL_IDS)} phòng voice.')

    # 1. Load Pomodoro Cog — phải add TRƯỚC khi sync
    if not bot.cogs.get('PomodoroCog'):
        try:
            pomo_cog = create_pomodoro_cog(bot, add_study_time, safe_send_dm, format_time)
            await bot.add_cog(pomo_cog)
            log.info('✅ Pomodoro Cog đã được tải')
        except Exception as e:
            log.error(f'Lỗi load Pomodoro Cog: {e}')

    # 2. Sync lệnh vào từng guild — hiệu lực ngay lập tức
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            log.info(f'✅ Sync {len(synced)} lệnh → {guild.name}')
        except Exception as e:
            log.error(f'Lỗi sync guild {guild.name}: {e}')

    if not scheduled_tasks.is_running():
        scheduled_tasks.start()
    if not checkpoint_task.is_running():
        checkpoint_task.start()

    threading.Thread(target=run_dashboard, daemon=True).start()
    log.info(f'🌐 Dashboard chạy tại http://localhost:{DASHBOARD_PORT}')

    # Khôi phục tracking cho thành viên đang có mặt trong phòng
    for channel_id in FOCUS_CHANNEL_IDS:
        channel = bot.get_channel(channel_id)
        if channel:
            for member in channel.members:
                if not member.bot:
                    record_join(member)
                    if not member.voice.self_stream:
                        start_check(member, 'đang trong phòng lúc bot khởi động')





@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    joined_focus    = after.channel  and after.channel.id in FOCUS_CHANNEL_IDS
    left_focus      = before.channel and before.channel.id in FOCUS_CHANNEL_IDS
    stayed_in_focus = joined_focus and left_focus
    stream_started  = stayed_in_focus and not before.self_stream and after.self_stream
    stream_stopped  = stayed_in_focus and before.self_stream and not after.self_stream

    if stream_stopped:
        # Checkpoint ngay khi tắt stream
        await _do_checkpoint(member)
        start_check(member, 'tắt stream')

    elif stream_started:
        # Bắt đầu stream → huỷ task kick
        cancel_task(member.id)
        log.info(f'{member.display_name} bắt đầu stream → huỷ đếm ngược kick.')

    elif joined_focus and not stayed_in_focus:
        # Vào phòng mới
        record_join(member)
        await safe_send_dm(member, random.choice(MOTIVATIONS))
        if not after.self_stream:
            start_check(member, 'vào phòng')
        # Cập nhật live message ngay
        for server in SERVERS:
            if after.channel.id in server['voice_channels']:
                await update_live_message(server)
                break

    elif left_focus and not stayed_in_focus:
        # Rời phòng
        duration = await record_leave_and_notify(member)
        cancel_task(member.id)
        log.info(f'{member.display_name} rời phòng sau {format_time(duration)}.')
        # Cập nhật live message ngay
        for server in SERVERS:
            if before.channel.id in server['voice_channels']:
                await update_live_message(server)
                break

# ─── FLASK DASHBOARD ─────────────────────────────────────────────────────────

DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📚 Study Bot Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { background:#0f172a; color:#e2e8f0; font-family:'Segoe UI',sans-serif; }
        .card { background:#1e293b; border:1px solid #334155; }
        .xp-bar { background:linear-gradient(90deg,#6366f1,#8b5cf6); }
        .live-dot { width:8px;height:8px;border-radius:50%;background:#22c55e;
                    animation:blink 1.2s infinite; display:inline-block; }
        @keyframes blink { 0%,100%{opacity:1}50%{opacity:0.2} }
    </style>
</head>
<body class="min-h-screen p-6">
    <div class="max-w-5xl mx-auto">
        <div class="text-center mb-8">
            <h1 class="text-4xl font-bold text-indigo-400">📚 Study Bot Dashboard</h1>
            <p class="text-gray-400 mt-1 text-sm" id="lastUpdate">Đang tải...</p>
        </div>
        <div id="summaryCards" class="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6"></div>
        <div class="card rounded-2xl p-6 mb-6">
            <div class="flex items-center gap-2 mb-4">
                <span class="live-dot"></span>
                <h2 class="text-xl font-semibold text-green-400">Đang học ngay lúc này</h2>
            </div>
            <div id="liveStudying"><div class="text-gray-500">Đang tải...</div></div>
        </div>
        <div class="card rounded-2xl p-6 mb-6">
            <h2 class="text-xl font-semibold text-indigo-300 mb-4">🏆 Bảng xếp hạng hôm nay</h2>
            <div id="leaderboard"><div class="text-gray-500">Đang tải...</div></div>
        </div>
        <div class="card rounded-2xl p-6">
            <h2 class="text-xl font-semibold text-indigo-300 mb-4">📈 Tổng giờ học 7 ngày qua</h2>
            <canvas id="weekChart" height="100"></canvas>
        </div>
    </div>
<script>
let chartInstance = null;
function fmtTime(s) {
    if (!s || s <= 0) return '0m';
    const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
    return h > 0 ? `${h}h ${m}m` : `${m}m`;
}
function getToday() { return new Date().toISOString().split('T')[0]; }
function getPastDays(n) {
    return Array.from({length:n}, (_,i) => {
        const d = new Date(); d.setDate(d.getDate() - (n-1-i));
        return d.toISOString().split('T')[0];
    });
}
const LEVEL_NAMES = ['Người mới','Học sinh','Chăm chỉ','Tập trung','Xuất sắc',
                     'Tinh anh','Huyền thoại','Bậc thầy','Thiên tài','Vô địch','Thần học'];
const THRESHOLDS  = [0,100,300,600,1000,1500,2500,4000,6000,9000,13000];
function getLevel(xp) {
    for (let i=THRESHOLDS.length-1;i>=0;i--) if (xp>=THRESHOLDS[i]) return i;
    return 0;
}
function xpPct(xp) {
    const lvl=getLevel(xp);
    if (lvl>=THRESHOLDS.length-1) return 100;
    return Math.round(((xp-THRESHOLDS[lvl])/(THRESHOLDS[lvl+1]-THRESHOLDS[lvl]))*100);
}
async function loadData() {
    const res  = await fetch('/api/stats');
    const data = await res.json();
    const today = getToday();
    const users = Object.values(data);
    const totalSecs   = users.reduce((s,u)=>s+(u.daily[today]||0),0);
    const activeCount = users.filter(u=>(u.daily[today]||0)>0).length;
    const topStreak   = users.reduce((m,u)=>Math.max(m,u.streak||0),0);
    document.getElementById('summaryCards').innerHTML = `
        <div class="card rounded-2xl p-6 text-center">
            <div class="text-4xl font-bold text-indigo-400">${fmtTime(totalSecs)}</div>
            <div class="text-gray-400 mt-2 text-sm">Tổng giờ học hôm nay</div>
        </div>
        <div class="card rounded-2xl p-6 text-center">
            <div class="text-4xl font-bold text-green-400">${activeCount}</div>
            <div class="text-gray-400 mt-2 text-sm">Người học hôm nay</div>
        </div>
        <div class="card rounded-2xl p-6 text-center">
            <div class="text-4xl font-bold text-orange-400">${topStreak} 🔥</div>
            <div class="text-gray-400 mt-2 text-sm">Streak cao nhất</div>
        </div>`;

    // Live studying section (from /api/live)
    try {
        const liveRes  = await fetch('/api/live');
        const liveData = await liveRes.json();
        const liveEl   = document.getElementById('liveStudying');
        if (liveData.length === 0) {
            liveEl.innerHTML = '<p class="text-gray-500">😴 Không có ai đang học...</p>';
        } else {
            liveEl.innerHTML = liveData.map((u,i) => `
                <div class="flex items-center gap-3 py-2 px-2 rounded-xl hover:bg-slate-700 transition">
                    <div class="text-lg">${u.is_streaming ? '📺' : '⏸️'}</div>
                    <div class="flex-1">
                        <span class="font-semibold">${u.name}</span>
                        <span class="text-xs text-gray-400 ml-2">Lv.${u.level}</span>
                    </div>
                    <div class="text-right">
                        <div class="text-green-400 font-mono font-bold">${fmtTime(u.session_secs)}</div>
                        <div class="text-xs text-gray-400">Hôm nay: ${fmtTime(u.today_total)}</div>
                    </div>
                </div>`).join('');
        }
    } catch(e) { console.error('Live fetch error:', e); }

    const sorted = Object.entries(data)
        .filter(([,u])=>(u.daily[today]||0)>0)
        .sort(([,a],[,b])=>(b.daily[today]||0)-(a.daily[today]||0))
        .slice(0,10);
    const medals = ['🥇','🥈','🥉'];
    document.getElementById('leaderboard').innerHTML = sorted.length===0
        ? '<p class="text-gray-500">😴 Hôm nay chưa có ai học!</p>'
        : sorted.map(([,u],i)=>{
            const lvl=u.level||0, pct=xpPct(u.xp||0);
            const goal=u.goal_seconds>0?Math.min(100,Math.round(((u.daily[today]||0)/u.goal_seconds)*100)):null;
            return `<div class="flex items-center gap-4 py-3 px-2 rounded-xl hover:bg-slate-700 transition">
                <div class="text-2xl w-8 text-center">${medals[i]||`${i+1}.`}</div>
                <div class="flex-1 min-w-0">
                    <div class="font-semibold truncate">${u.name}</div>
                    <div class="text-xs text-gray-400">Lv.${lvl} ${LEVEL_NAMES[lvl]} · ${u.xp||0} XP · 🔥${u.streak||0} ngày</div>
                    <div class="w-full bg-slate-700 rounded-full h-1.5 mt-1">
                        <div class="xp-bar h-1.5 rounded-full transition-all" style="width:${pct}%"></div>
                    </div>
                    ${goal!==null?`<div class="text-xs text-yellow-400 mt-0.5">🎯 Mục tiêu: ${goal}%</div>`:''}
                </div>
                <div class="text-indigo-300 font-mono font-bold text-right">${fmtTime(u.daily[today]||0)}</div>
            </div>`;
        }).join('');
    const days   = getPastDays(7);
    const totals = days.map(d=>Math.round(Object.values(data).reduce((s,u)=>s+(u.daily[d]||0),0)/60));
    if (chartInstance) chartInstance.destroy();
    chartInstance = new Chart(document.getElementById('weekChart'), {
        type:'bar',
        data:{
            labels: days.map(d=>d.slice(5)),
            datasets:[{label:'Phút học',data:totals,backgroundColor:'rgba(99,102,241,0.7)',
                       borderColor:'#6366f1',borderWidth:2,borderRadius:8}]
        },
        options:{
            responsive:true,
            plugins:{legend:{labels:{color:'#e2e8f0'}}},
            scales:{y:{ticks:{color:'#94a3b8'},grid:{color:'#334155'}},
                    x:{ticks:{color:'#94a3b8'},grid:{color:'#334155'}}}
        }
    });
    document.getElementById('lastUpdate').textContent =
        'Cập nhật lần cuối: ' + new Date().toLocaleTimeString('vi-VN');
}
loadData();
setInterval(loadData, 30000);
</script>
</body>
</html>'''

flask_app = Flask(__name__)

# Chia sẻ state với Flask thông qua closure (safe vì chỉ đọc)
_live_state_cache: list = []

@flask_app.route('/')
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@flask_app.route('/api/stats')
def api_stats():
    return jsonify(load_data())

@flask_app.route('/api/live')
def api_live():
    """API endpoint cho dashboard: danh sách người đang học với thời gian thực."""
    return jsonify(_live_state_cache)

def run_dashboard():
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    flask_app.run(host='0.0.0.0', port=DASHBOARD_PORT, debug=False, use_reloader=False)

def _update_live_cache():
    """Cập nhật cache live state để Flask API /api/live phục vụ dashboard."""
    global _live_state_cache
    now   = datetime.now()
    today = now.strftime('%Y-%m-%d')
    data  = load_data()
    result = []

    for member_id, start_time in join_times.items():
        uid     = str(member_id)
        info    = data.get(uid, {})
        saved   = info.get('daily', {}).get(today, 0)
        chk     = last_checkpoint.get(member_id, start_time)
        unsaved = int((now - chk).total_seconds())
        session = int((now - start_time).total_seconds())

        is_streaming = False
        for guild in bot.guilds:
            m = guild.get_member(member_id)
            if m and m.voice:
                is_streaming = bool(m.voice.self_stream)
                break

        result.append({
            'name':         info.get('name', f'User {member_id}'),
            'level':        info.get('level', 0),
            'session_secs': session,
            'today_total':  saved + unsaved,
            'is_streaming': is_streaming,
        })

    result.sort(key=lambda x: x['today_total'], reverse=True)
    _live_state_cache = result

# ─── BOT START ───────────────────────────────────────────────────────────────

bot.run(TOKEN)