"""
╔══════════════════════════════════════════════════════════════════╗
║                POMODORO MODULE v2 — UPGRADE                      ║
║                                                                  ║
║  Cách dùng: thêm vào bot.py                                     ║
║    from pomodoro import create_pomodoro_cog                      ║
║    cog = create_pomodoro_cog(                                    ║
║        bot, add_study_time, safe_send_dm, format_time,           ║
║        load_data_fn=load_data,    # MỚI v2                     ║
║        save_data_fn=save_data,    # MỚI v2                     ║
║        add_xp_fn=add_xp_direct,   # MỚI v2                     ║
║    )                                                              ║
║    await bot.add_cog(cog)                                        ║
║                                                                  ║
║  Lệnh slash:                                                     ║
║    /pomodoro start  – bắt đầu phiên cá nhân                     ║
║    /pomodoro stop   – dừng phiên đang chạy                      ║
║    /pomodoro status – xem trạng thái + tiến độ                  ║
║    /pomodoro create – tạo phòng Pomodoro nhóm                   ║
║    /pomodoro join   – tham gia phòng Pomodoro nhóm              ║
║    /pomodoro leave  – rời phòng nhóm                            ║
║    /pomodoro list   – danh sách phòng nhóm đang hoạt động       ║
║    /pomodoro stats  – xem lịch sử Pomodoro của bạn              ║
║    /pomodoro preset – lưu cấu hình yêu thích         [MỚI v2]  ║
║                                                                  ║
║  Thay đổi v2:                                                    ║
║  • Sửa bug XP nhân đôi: _update_history chỉ tính 1 lần         ║
║  • _award_pomodoro_xp hoạt động thực sự (+50 XP/vòng)          ║
║  • Lịch sử Pomodoro persistent vào study_data.json              ║
║  • Giờ nghỉ có tip sức khoẻ ngẫu nhiên                         ║
║  • /pomodoro preset — lưu/load cấu hình yêu thích               ║
║  • Thông báo hoàn thành nhóm bật notification                   ║
║  • Giữ nguyên toàn bộ cấu trúc, comment, định dạng gốc         ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

POMODORO_DEFAULTS = {
    'work':   25,   # phút làm việc mặc định
    'break':   5,   # phút nghỉ ngắn mặc định
    'rounds':  4,   # số vòng mặc định
}

POMODORO_LIMITS = {
    'work_min':    5,
    'work_max':   90,
    'break_min':   1,
    'break_max':  30,
    'rounds_min':  1,
    'rounds_max': 12,
}

XP_PER_ROUND   = 50   # XP thưởng khi hoàn thành 1 vòng Pomodoro
EDIT_INTERVAL  = 30   # giây: cập nhật countdown message mỗi N giây

# ─── 💡 BREAK TIPS — Mẹo nghỉ ngơi ngẫu nhiên ──────────────────────────────

BREAK_TIPS = [
    '🧘 Vươn vai và hít thở sâu — thư giãn cơ cổ, vai, gáy nhé!',
    '💧 Uống một ly nước — não cần nước để hoạt động hiệu quả!',
    '👁️ Nhìn ra xa 20 giây để mắt được nghỉ ngơi (quy tắc 20-20-20)!',
    '🚶 Đi lại vài bước quanh phòng để máu lưu thông tốt hơn!',
    '🍌 Ăn nhẹ gì đó bổ dưỡng để nạp năng lượng cho vòng tiếp theo!',
    '😌 Nhắm mắt 60 giây, hít thở đều và reset tâm trí!',
    '📝 Ghi nhanh những điều bạn vừa học được trong vòng này!',
    '🎵 Nghe một bài nhạc yêu thích để nạp lại tinh thần!',
    '🤸 Thực hiện vài động tác nhẹ nhàng cho cơ thể tỉnh táo!',
    '☕ Uống trà hoặc cà phê — nhưng đừng nhìn điện thoại nhé!',
]

# ─── DATA CLASSES ────────────────────────────────────────────────────────────

@dataclass
class PomodoroSession:
    """Phiên Pomodoro cá nhân."""
    member:          discord.Member
    work_minutes:    int
    break_minutes:   int
    total_rounds:    int
    channel:         discord.TextChannel

    current_round:   int      = 1
    phase:           str      = 'work'      # 'work' | 'break' | 'done'
    phase_end:       datetime = field(default_factory=datetime.now)
    completed_rounds: int     = 0
    live_message:    Optional[discord.Message] = None
    task:            Optional[asyncio.Task]    = None
    group_id:        Optional[str]             = None   # None = cá nhân

    @property
    def phase_remaining(self) -> int:
        """Giây còn lại trong phase hiện tại."""
        return max(0, int((self.phase_end - datetime.now()).total_seconds()))

    @property
    def progress_bar(self) -> str:
        """Thanh tiến độ 10 ô."""
        total   = (self.work_minutes if self.phase == 'work' else self.break_minutes) * 60
        elapsed = total - self.phase_remaining
        filled  = int((elapsed / total) * 10) if total > 0 else 0
        return '█' * filled + '░' * (10 - filled)

    @property
    def rounds_bar(self) -> str:
        """Hiển thị số vòng: 🍅🍅⬜⬜"""
        done  = '🍅' * self.completed_rounds
        empty = '⬜' * (self.total_rounds - self.completed_rounds)
        return done + empty

    def format_remaining(self) -> str:
        secs = self.phase_remaining
        m, s = divmod(secs, 60)
        return f'{m:02d}:{s:02d}'


@dataclass
class GroupSession:
    """Phòng Pomodoro dùng chung nhiều người."""
    session_id:    str
    host:          discord.Member
    name:          str
    work_minutes:  int
    break_minutes: int
    total_rounds:  int
    channel:       discord.TextChannel
    guild_id:      int

    members:       dict[int, PomodoroSession] = field(default_factory=dict)
    current_round: int      = 1
    phase:         str      = 'waiting'   # 'waiting' | 'work' | 'break' | 'done'
    phase_end:     datetime = field(default_factory=datetime.now)
    announce_msg:  Optional[discord.Message] = None
    task:          Optional[asyncio.Task]    = None

    @property
    def member_count(self) -> int:
        return len(self.members)


# ─── COG ─────────────────────────────────────────────────────────────────────

class PomodoroCog(commands.Cog):
    """
    Toàn bộ logic Pomodoro gói trong một Cog.
    Inject `add_study_time`, `safe_send_dm`, `format_time` từ bot.py qua hàm khởi tạo.
    v2: thêm load_data_fn, save_data_fn, add_xp_fn để lưu lịch sử persistent và cộng XP thực.
    """

    def __init__(
        self,
        bot:              commands.Bot,
        add_study_time_fn,              # hàm add_study_time từ bot.py
        safe_send_dm_fn,                # hàm safe_send_dm từ bot.py
        format_time_fn,                 # hàm format_time từ bot.py
        load_data_fn:     Callable = None,   # [v2] hàm load_data từ bot.py
        save_data_fn:     Callable = None,   # [v2] hàm save_data từ bot.py
        add_xp_fn:        Callable = None,   # [v2] hàm add_xp_direct từ bot.py
    ):
        self.bot        = bot
        self._add_study = add_study_time_fn
        self._send_dm   = safe_send_dm_fn
        self._fmt       = format_time_fn

        # [v2] fallback no-op nếu không được truyền (backward compatible)
        self._load    = load_data_fn  if load_data_fn is not None else lambda: {}
        self._save    = save_data_fn  if save_data_fn is not None else lambda _: None
        self._add_xp  = add_xp_fn    if add_xp_fn    is not None else lambda uid, xp: None

        # state: member_id → PomodoroSession (phiên cá nhân hoặc đã join nhóm)
        self._sessions: dict[int, PomodoroSession] = {}
        # state: session_id → GroupSession
        self._groups:   dict[str, GroupSession]    = {}
        # lịch sử in-memory (vẫn giữ để tương thích) — v2 cũng lưu vào file
        self._history:  dict[int, dict]            = {}

    # ─── SLASH COMMAND GROUP ─────────────────────────────────────────────────

    pomo = app_commands.Group(
        name='pomodoro',
        description='🍅 Quản lý phiên Pomodoro học tập'
    )

    # ── /pomodoro start ──────────────────────────────────────────────────────

    @pomo.command(name='start', description='Bắt đầu phiên Pomodoro cá nhân')
    @app_commands.describe(
        work   = f'Phút làm việc (mặc định {POMODORO_DEFAULTS["work"]})',
        break_ = f'Phút nghỉ (mặc định {POMODORO_DEFAULTS["break"]})',
        rounds = f'Số vòng (mặc định {POMODORO_DEFAULTS["rounds"]})',
    )
    @app_commands.rename(break_='break')
    async def pomo_start(
        self,
        interaction: discord.Interaction,
        work:   app_commands.Range[int, 5, 90] = POMODORO_DEFAULTS['work'],
        break_: app_commands.Range[int, 1, 30] = POMODORO_DEFAULTS['break'],
        rounds: app_commands.Range[int, 1, 12] = POMODORO_DEFAULTS['rounds'],
    ):
        member = interaction.user
        sess   = self._sessions.get(member.id)

        # Nếu đang trong phòng nhóm → host bấm start để bắt đầu nhóm
        if sess and sess.group_id:
            handled = await self._start_group_if_host(interaction, sess)
            if not handled:
                grp = self._groups.get(sess.group_id)
                if not grp:
                    self._sessions.pop(member.id, None)
                    await interaction.response.send_message(
                        '❌ Phòng nhóm không còn tồn tại.\n'
                        'Dùng `/pomodoro start` lại để bắt đầu phiên cá nhân.',
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        f'ℹ️ Phòng **"{grp.name}"** đang ở phase `{grp.phase.upper()}`.\n'
                        f'Chỉ có thể bắt đầu khi phòng đang ở trạng thái **waiting**.',
                        ephemeral=True
                    )
            return

        # Đang có phiên cá nhân rồi
        if sess:
            await interaction.response.send_message(
                f'⚠️ Bạn đang có phiên Pomodoro chạy rồi!\n'
                f'Phase: `{sess.phase.upper()}` | '
                f'Còn lại: `{sess.format_remaining()}`\n'
                f'Dùng `/pomodoro stop` để dừng trước.',
                ephemeral=True
            )
            return

        # [v2] Load preset nếu có, nhưng tham số tường minh được ưu tiên
        uid  = str(member.id)
        data = self._load()
        if uid in data and 'pomo_preset' in data[uid]:
            preset = data[uid]['pomo_preset']
            # Chỉ dùng preset khi người dùng không truyền tường minh
            if work   == POMODORO_DEFAULTS['work']:   work   = preset.get('work',   work)
            if break_ == POMODORO_DEFAULTS['break']:  break_ = preset.get('break',  break_)
            if rounds == POMODORO_DEFAULTS['rounds']: rounds = preset.get('rounds', rounds)

        sess = PomodoroSession(
            member        = member,
            work_minutes  = work,
            break_minutes = break_,
            total_rounds  = rounds,
            channel       = interaction.channel,
            phase_end     = datetime.now() + timedelta(minutes=work),
        )
        self._sessions[member.id] = sess

        # Respond to interaction FIRST — prevents timeout if channel.send fails
        await interaction.response.send_message(
            f'🍅 Phiên Pomodoro đã bắt đầu! `{work}m làm / {break_}m nghỉ × {rounds} vòng`\n'
            f'Tin nhắn bên dưới sẽ tự cập nhật mỗi {EDIT_INTERVAL} giây.',
            ephemeral=True
        )

        # Gửi tin nhắn countdown trong kênh (sau khi đã respond interaction)
        try:
            msg = await interaction.channel.send(
                self._build_personal_embed(sess),
                silent=True
            )
            sess.live_message = msg
        except discord.Forbidden:
            log.warning(f'[Pomodoro] No permission to send in channel {interaction.channel}')
        except Exception as e:
            log.error(f'[Pomodoro] channel.send error: {e}')

        # DM bắt đầu
        await self._send_dm(
            member,
            f'🍅 **Pomodoro bắt đầu!**\n'
            f'⏱️ Làm việc: `{work} phút` × `{rounds} vòng`\n'
            f'☕ Nghỉ: `{break_} phút` giữa mỗi vòng\n'
            f'Tập trung nào! Tắt điện thoại, đóng tab thừa. 💪'
        )

        # Khởi chạy task điều phối
        sess.task = asyncio.create_task(self._run_personal(sess))

    # ── /pomodoro stop ───────────────────────────────────────────────────────

    @pomo.command(name='stop', description='Dừng phiên Pomodoro đang chạy')
    async def pomo_stop(self, interaction: discord.Interaction):
        member = interaction.user
        sess   = self._sessions.get(member.id)

        if not sess:
            await interaction.response.send_message(
                '❌ Bạn không có phiên Pomodoro nào đang chạy.',
                ephemeral=True
            )
            return

        # Nếu đang trong phase work, lưu thời gian đã học
        if sess.phase == 'work':
            elapsed_work = int(
                (sess.work_minutes * 60 - sess.phase_remaining)
            )
            if elapsed_work > 60:
                self._add_study(member.id, member.display_name, elapsed_work)

        # Xoá khỏi group nếu có
        if sess.group_id and sess.group_id in self._groups:
            grp = self._groups[sess.group_id]
            grp.members.pop(member.id, None)
            if grp.announce_msg:
                try:
                    await grp.announce_msg.edit(
                        content=self._build_group_embed(grp)
                    )
                except Exception:
                    pass

        self._cancel_session(member.id)
        summary = self._update_history(member.id, sess)

        await interaction.response.send_message(
            f'⏹️ Đã dừng Pomodoro.\n'
            f'✅ Hoàn thành: `{sess.completed_rounds}/{sess.total_rounds} vòng`\n'
            f'📚 Thời gian học: `{self._fmt(summary["added_seconds"])}`\n'
            f'⚡ XP từ Pomodoro: `+{summary["xp_bonus"]} XP`',
            ephemeral=True
        )

    # ── /pomodoro status ─────────────────────────────────────────────────────

    @pomo.command(name='status', description='Xem trạng thái phiên Pomodoro hiện tại')
    async def pomo_status(self, interaction: discord.Interaction):
        sess = self._sessions.get(interaction.user.id)
        if not sess:
            await interaction.response.send_message(
                '😴 Bạn chưa có phiên Pomodoro nào.\n'
                'Dùng `/pomodoro start` để bắt đầu!',
                ephemeral=True
            )
            return
        await interaction.response.send_message(
            self._build_personal_embed(sess),
            ephemeral=True
        )

    # ── /pomodoro create ─────────────────────────────────────────────────────

    @pomo.command(name='create', description='Tạo phòng Pomodoro nhóm cho mọi người tham gia')
    @app_commands.describe(
        name   = 'Tên phòng (VD: Nhóm IELTS)',
        work   = f'Phút làm việc (mặc định {POMODORO_DEFAULTS["work"]})',
        break_ = f'Phút nghỉ (mặc định {POMODORO_DEFAULTS["break"]})',
        rounds = f'Số vòng (mặc định {POMODORO_DEFAULTS["rounds"]})',
    )
    @app_commands.rename(break_='break')
    async def pomo_create(
        self,
        interaction: discord.Interaction,
        name:   str,
        work:   app_commands.Range[int, 5, 90] = POMODORO_DEFAULTS['work'],
        break_: app_commands.Range[int, 1, 30] = POMODORO_DEFAULTS['break'],
        rounds: app_commands.Range[int, 1, 12] = POMODORO_DEFAULTS['rounds'],
    ):
        member   = interaction.user
        guild_id = interaction.guild_id

        # Kiểm tra member đang có session không
        if member.id in self._sessions:
            await interaction.response.send_message(
                '⚠️ Bạn đang trong một phiên Pomodoro rồi. Dùng `/pomodoro stop` trước.',
                ephemeral=True
            )
            return

        # Tạo ID ngắn gọn từ tên
        session_id = f'{guild_id}_{name.lower().replace(" ", "_")}'
        if session_id in self._groups:
            await interaction.response.send_message(
                f'❌ Phòng **"{name}"** đã tồn tại trong server này!\n'
                f'Dùng `/pomodoro join {name}` để tham gia, hoặc chọn tên khác.',
                ephemeral=True
            )
            return

        grp = GroupSession(
            session_id    = session_id,
            host          = member,
            name          = name,
            work_minutes  = work,
            break_minutes = break_,
            total_rounds  = rounds,
            channel       = interaction.channel,
            guild_id      = guild_id,
        )
        self._groups[session_id] = grp

        # Host tự join
        await self._join_group(member, grp)

        # Respond to interaction FIRST — avoids 3-second Discord timeout
        # if channel.send() takes too long (e.g. rate-limited, slow shard).
        await interaction.response.send_message(
            f'✅ Đã tạo phòng **"{name}"**!\n'
            f'⚙️ `{work}m làm / {break_}m nghỉ × {rounds} vòng`\n'
            f'📢 Mọi người có thể dùng `/pomodoro join {name}` để vào cùng.\n'
            f'▶️ Dùng `/pomodoro start` để bắt đầu khi đủ người!',
            ephemeral=True
        )

        # Đăng tin nhắn nhóm (sau khi đã respond interaction)
        try:
            announce = await interaction.channel.send(
                self._build_group_embed(grp),
                silent=True
            )
            grp.announce_msg = announce
        except discord.Forbidden:
            log.warning(f'[Pomodoro] No permission to send in channel {interaction.channel}')
        except Exception as e:
            log.error(f'[Pomodoro] pomo_create channel.send error: {e}')

    # ── /pomodoro join ───────────────────────────────────────────────────────

    @pomo.command(name='join', description='Tham gia phòng Pomodoro nhóm đang chờ')
    @app_commands.describe(name='Tên phòng cần tham gia')
    async def pomo_join(self, interaction: discord.Interaction, name: str):
        member     = interaction.user
        session_id = f'{interaction.guild_id}_{name.lower().replace(" ", "_")}'
        grp        = self._groups.get(session_id)

        if not grp:
            # Gợi ý các phòng đang có
            existing = [
                g.name for g in self._groups.values()
                if g.guild_id == interaction.guild_id
            ]
            hint = (
                f'\n💡 Phòng đang có: {", ".join(f"**{n}**" for n in existing)}'
                if existing else ''
            )
            await interaction.response.send_message(
                f'❌ Không tìm thấy phòng **"{name}"**.{hint}',
                ephemeral=True
            )
            return

        if grp.phase not in ('waiting',):
            await interaction.response.send_message(
                f'⚠️ Phòng **"{name}"** đang chạy rồi (vòng {grp.current_round}).\n'
                f'Chờ vòng kế tiếp hoặc tạo phòng mới nhé!',
                ephemeral=True
            )
            return

        if member.id in grp.members:
            await interaction.response.send_message(
                f'ℹ️ Bạn đã ở trong phòng **"{name}"** rồi!',
                ephemeral=True
            )
            return

        if member.id in self._sessions:
            await interaction.response.send_message(
                '⚠️ Bạn đang có phiên cá nhân. Dùng `/pomodoro stop` trước.',
                ephemeral=True
            )
            return

        await self._join_group(member, grp)
        if grp.announce_msg:
            await grp.announce_msg.edit(content=self._build_group_embed(grp))

        await interaction.response.send_message(
            f'✅ Đã tham gia phòng **"{grp.name}"**!\n'
            f'👥 Hiện có `{grp.member_count}` người.\n'
            f'Chờ host bắt đầu nhé! 🍅',
            ephemeral=True
        )

    # ── /pomodoro leave ──────────────────────────────────────────────────────

    @pomo.command(name='leave', description='Rời khỏi phòng Pomodoro nhóm')
    async def pomo_leave(self, interaction: discord.Interaction):
        member = interaction.user
        sess   = self._sessions.get(member.id)

        if not sess or not sess.group_id:
            await interaction.response.send_message(
                '❌ Bạn không ở trong phòng nhóm nào.',
                ephemeral=True
            )
            return

        grp = self._groups.get(sess.group_id)
        self._cancel_session(member.id)

        if grp:
            grp.members.pop(member.id, None)
            if grp.announce_msg:
                try:
                    await grp.announce_msg.edit(
                        content=self._build_group_embed(grp)
                    )
                except Exception:
                    pass
            # Nếu host rời, chuyển host cho người tiếp theo
            if member.id == grp.host.id and grp.members:
                grp.host = next(iter(grp.members.values())).member
            # Nếu phòng trống, xoá luôn
            if not grp.members:
                self._cancel_group(sess.group_id)

        await interaction.response.send_message(
            f'👋 Đã rời phòng nhóm.',
            ephemeral=True
        )

    # ── /pomodoro list ───────────────────────────────────────────────────────

    @pomo.command(name='list', description='Xem danh sách phòng Pomodoro nhóm đang hoạt động')
    async def pomo_list(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        rooms    = [g for g in self._groups.values() if g.guild_id == guild_id]

        if not rooms:
            await interaction.response.send_message(
                '😴 Chưa có phòng Pomodoro nhóm nào.\n'
                'Tạo phòng mới bằng `/pomodoro create`!',
                ephemeral=True
            )
            return

        lines = ['🍅 **Phòng Pomodoro đang hoạt động**\n']
        for g in rooms:
            phase_icon = {'waiting': '⏳', 'work': '📖', 'break': '☕', 'done': '✅'}.get(g.phase, '❓')
            lines.append(
                f'{phase_icon} **{g.name}** · `{g.work_minutes}m/{g.break_minutes}m × {g.total_rounds}` '
                f'· 👥 {g.member_count} người '
                f'· Host: {g.host.display_name}'
            )

        await interaction.response.send_message('\n'.join(lines), ephemeral=True)

    # ── /pomodoro stats ──────────────────────────────────────────────────────

    @pomo.command(name='stats', description='Xem lịch sử Pomodoro của bạn')
    async def pomo_stats(self, interaction: discord.Interaction):
        member = interaction.user

        # [v2] Đọc từ file data trước (persistent), fallback về in-memory
        uid      = str(member.id)
        data     = self._load()
        today    = datetime.now().strftime('%Y-%m-%d')
        file_h   = data.get(uid, {}).get('pomo_history', {})
        mem_h    = self._history.get(member.id, {})

        # Ưu tiên dữ liệu từ file, fallback về memory
        history = file_h if file_h else mem_h

        total_rounds  = history.get('total_rounds', 0)
        total_minutes = history.get('total_minutes', 0)
        total_xp      = history.get('total_xp', 0)
        best_streak   = history.get('best_streak', 0)
        # today_rounds: đọc từ file nếu today_date khớp
        if file_h.get('today_date') == today:
            today_rounds = file_h.get('today_rounds', 0)
        else:
            today_rounds = mem_h.get('today_rounds', 0) if mem_h.get('today_date') == today else 0

        msg = (
            f'🍅 **Lịch sử Pomodoro của {member.display_name}**\n'
            f'──────────────────\n'
            f'✅ Tổng vòng hoàn thành: `{total_rounds} vòng`\n'
            f'⏱️ Tổng thời gian tập trung: `{self._fmt(total_minutes * 60)}`\n'
            f'⚡ XP từ Pomodoro: `{total_xp:,} XP`\n'
            f'🔥 Streak liên tiếp tốt nhất: `{best_streak} vòng`\n'
            f'📅 Hôm nay: `{today_rounds} vòng`'
        )

        if not history:
            msg = (
                f'📭 **{member.display_name}** chưa có lịch sử Pomodoro.\n'
                f'Dùng `/pomodoro start` để bắt đầu! 🍅'
            )

        await interaction.response.send_message(msg, ephemeral=True)

    # ── /pomodoro preset [MỚI v2] ────────────────────────────────────────────

    @pomo.command(name='preset', description='[v2] Lưu cấu hình Pomodoro yêu thích')
    @app_commands.describe(
        work   = f'Phút làm việc (mặc định {POMODORO_DEFAULTS["work"]})',
        break_ = f'Phút nghỉ (mặc định {POMODORO_DEFAULTS["break"]})',
        rounds = f'Số vòng (mặc định {POMODORO_DEFAULTS["rounds"]})',
    )
    @app_commands.rename(break_='break')
    async def pomo_preset(
        self,
        interaction: discord.Interaction,
        work:   app_commands.Range[int, 5, 90] = POMODORO_DEFAULTS['work'],
        break_: app_commands.Range[int, 1, 30] = POMODORO_DEFAULTS['break'],
        rounds: app_commands.Range[int, 1, 12] = POMODORO_DEFAULTS['rounds'],
    ):
        uid  = str(interaction.user.id)
        data = self._load()

        if uid not in data:
            data[uid] = {
                'name': interaction.user.display_name,
                'daily': {}, 'total': 0, 'xp': 0, 'level': 0,
                'streak': 0, 'longest_streak': 0, 'last_study_date': '',
                'goal': None, 'goal_seconds': 0, 'last_absent_warn': '',
                'badges': [], 'badge_dates': {}, 'quests_done_total': 0,
                'daily_quests': {}, 'special_flags': [], 'remind_hour': None,
            }
            self._save(data)

        # Lưu preset vào file
        data[uid]['pomo_preset'] = {
            'work':   work,
            'break':  break_,
            'rounds': rounds,
        }
        self._save(data)

        await interaction.response.send_message(
            f'✅ Đã lưu preset Pomodoro yêu thích!\n'
            f'⚙️ `{work}m làm / {break_}m nghỉ × {rounds} vòng`\n'
            f'Preset sẽ tự động dùng khi `/pomodoro start` (có thể override bằng tham số).',
            ephemeral=True
        )

    # ─── INTERNAL LOGIC ──────────────────────────────────────────────────────

    async def _join_group(self, member: discord.Member, grp: GroupSession):
        """Tạo PomodoroSession cho member và thêm vào GroupSession."""
        sess = PomodoroSession(
            member        = member,
            work_minutes  = grp.work_minutes,
            break_minutes = grp.break_minutes,
            total_rounds  = grp.total_rounds,
            channel       = grp.channel,
            phase         = 'waiting',
            group_id      = grp.session_id,
        )
        self._sessions[member.id] = sess
        grp.members[member.id]    = sess

    async def _run_personal(self, sess: PomodoroSession):
        """
        Task điều phối vòng lặp work → break → work...
        Chạy độc lập, tự dừng khi xong hoặc bị cancel.
        """
        try:
            while sess.current_round <= sess.total_rounds:
                # ── WORK PHASE ──
                sess.phase     = 'work'
                sess.phase_end = datetime.now() + timedelta(minutes=sess.work_minutes)

                # Cập nhật countdown liên tục
                edit_task = asyncio.create_task(
                    self._countdown_loop(sess)
                )

                # Chờ hết giờ làm
                try:
                    await asyncio.sleep(sess.work_minutes * 60)
                finally:
                    edit_task.cancel()

                # Lưu thời gian học vào data file
                self._add_study(
                    sess.member.id,
                    sess.member.display_name,
                    sess.work_minutes * 60
                )

                # Cộng XP Pomodoro (50 XP/vòng) — thực sự ghi vào data file
                self._award_pomodoro_xp(str(sess.member.id))

                sess.completed_rounds += 1

                # Kiểm tra xong chưa
                if sess.completed_rounds >= sess.total_rounds:
                    break

                # ── BREAK PHASE ──
                sess.phase     = 'break'
                sess.phase_end = datetime.now() + timedelta(minutes=sess.break_minutes)

                # [v2] Gửi tip ngẫu nhiên khi nghỉ
                tip = random.choice(BREAK_TIPS)
                await self._send_dm(
                    sess.member,
                    f'☕ **Nghỉ ngơi thôi!**\n'
                    f'✅ Vừa hoàn thành vòng `{sess.completed_rounds}/{sess.total_rounds}`\n'
                    f'⏰ Nghỉ `{sess.break_minutes} phút` — {tip}\n'
                    f'🔔 Bot sẽ nhắc khi hết giờ nghỉ.'
                )

                # Cập nhật tin nhắn kênh
                if sess.live_message:
                    try:
                        await sess.live_message.edit(
                            content=self._build_personal_embed(sess)
                        )
                    except Exception:
                        pass

                edit_task = asyncio.create_task(self._countdown_loop(sess))
                try:
                    await asyncio.sleep(sess.break_minutes * 60)
                finally:
                    edit_task.cancel()

                # ── CHUẨN BỊ VÒNG MỚI ──
                sess.current_round += 1
                sess.phase     = 'work'
                sess.phase_end = datetime.now() + timedelta(minutes=sess.work_minutes)

                await self._send_dm(
                    sess.member,
                    f'📖 **Vòng {sess.current_round} bắt đầu!**\n'
                    f'⏱️ Tập trung `{sess.work_minutes} phút` tiếp theo nào!\n'
                    f'🍅 {sess.rounds_bar}'
                )

            # ── HOÀN THÀNH TẤT CẢ ──
            sess.phase = 'done'
            # Gọi _update_history một lần duy nhất ở cuối (tránh nhân đôi XP)
            summary    = self._update_history(sess.member.id, sess)

            completion_msg = (
                f'🎉 **HOÀN THÀNH POMODORO!**\n'
                f'──────────────────\n'
                f'🍅 {sess.rounds_bar}\n'
                f'✅ `{sess.completed_rounds}/{sess.total_rounds}` vòng hoàn thành\n'
                f'⏱️ Tổng thời gian học: `{self._fmt(sess.completed_rounds * sess.work_minutes * 60)}`\n'
                f'⚡ XP Pomodoro: `+{summary["xp_bonus"]} XP`\n\n'
                f'💪 Tuyệt vời! Nghỉ ngơi đầy đủ nhé!'
            )

            await self._send_dm(sess.member, completion_msg)

            if sess.live_message:
                try:
                    await sess.live_message.edit(
                        content=self._build_personal_embed(sess)
                    )
                except Exception:
                    pass

            # Thông báo kênh (bật notification — không dùng silent=True)
            try:
                await sess.channel.send(
                    f'🎊 **{sess.member.display_name}** vừa hoàn thành '
                    f'`{sess.completed_rounds}` vòng Pomodoro! '
                    f'`{self._fmt(sess.completed_rounds * sess.work_minutes * 60)}` học tập! '
                    f'⚡ +{summary["xp_bonus"]} XP'
                )
            except Exception:
                pass

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f'Lỗi _run_personal cho {sess.member.display_name}: {e}')
        finally:
            self._sessions.pop(sess.member.id, None)

    async def _run_group(self, grp: GroupSession):
        """
        Task điều phối vòng lặp Pomodoro cho cả nhóm.
        """
        try:
            while grp.current_round <= grp.total_rounds:
                # ── WORK PHASE ──
                grp.phase     = 'work'
                grp.phase_end = datetime.now() + timedelta(minutes=grp.work_minutes)

                for sess in list(grp.members.values()):
                    sess.phase     = 'work'
                    sess.phase_end = grp.phase_end
                    await self._send_dm(
                        sess.member,
                        f'📖 **[{grp.name}] Vòng {grp.current_round} bắt đầu!**\n'
                        f'⏱️ Tập trung `{grp.work_minutes} phút`\n'
                        f'👥 Cùng nhau với `{grp.member_count}` người! 💪'
                    )

                # Khởi edit_task countdown (chỉ khi có announce_msg)
                edit_task = None
                if grp.announce_msg:
                    edit_task = asyncio.create_task(
                        self._group_countdown_loop(grp)
                    )

                try:
                    await asyncio.sleep(grp.work_minutes * 60)
                finally:
                    if edit_task and not edit_task.done():
                        edit_task.cancel()

                # Lưu thời gian học và cộng XP cho từng thành viên
                for sess in list(grp.members.values()):
                    self._add_study(
                        sess.member.id,
                        sess.member.display_name,
                        grp.work_minutes * 60
                    )
                    # [v2] Cộng XP thực sự cho mỗi thành viên mỗi vòng
                    self._award_pomodoro_xp(str(sess.member.id))
                    sess.completed_rounds += 1

                grp.current_round += 1

                # Kiểm tra xong chưa
                if grp.current_round > grp.total_rounds:
                    break

                # ── BREAK PHASE ──
                grp.phase     = 'break'
                grp.phase_end = datetime.now() + timedelta(minutes=grp.break_minutes)

                # [v2] Tip ngẫu nhiên cho giờ nghỉ nhóm
                tip = random.choice(BREAK_TIPS)
                for sess in list(grp.members.values()):
                    sess.phase     = 'break'
                    sess.phase_end = grp.phase_end
                    await self._send_dm(
                        sess.member,
                        f'☕ **[{grp.name}] Nghỉ ngơi!**\n'
                        f'✅ Vòng {grp.current_round - 1} xong!\n'
                        f'⏰ `{grp.break_minutes} phút` — {tip}'
                    )

                if grp.announce_msg:
                    try:
                        await grp.announce_msg.edit(
                            content=self._build_group_embed(grp)
                        )
                    except Exception:
                        pass

                await asyncio.sleep(grp.break_minutes * 60)

            # ── HOÀN THÀNH ──
            grp.phase = 'done'
            names_str = ', '.join(
                f'**{s.member.display_name}**'
                for s in grp.members.values()
            )

            if grp.announce_msg:
                try:
                    await grp.announce_msg.edit(
                        content=self._build_group_embed(grp)
                    )
                except Exception:
                    pass

            # Thông báo kênh (bật notification — không dùng silent=True)
            try:
                total_time = self._fmt(grp.total_rounds * grp.work_minutes * 60)
                await grp.channel.send(
                    f'🎊 **Phòng "{grp.name}" hoàn thành Pomodoro!**\n'
                    f'👥 Thành viên: {names_str}\n'
                    f'🍅 `{grp.total_rounds}` vòng | ⏱️ `{total_time}` học tập\n'
                    f'Cả nhóm thật xuất sắc! 🏆'
                )
            except Exception:
                pass

            # Gọi _update_history cho từng thành viên một lần duy nhất ở cuối
            for sess in list(grp.members.values()):
                self._update_history(sess.member.id, sess)
                await self._send_dm(
                    sess.member,
                    f'🎉 **[{grp.name}] Hoàn thành!**\n'
                    f'🍅 `{grp.total_rounds}` vòng Pomodoro cùng nhóm!\n'
                    f'💪 Học nhóm thật hiệu quả!'
                )
                self._sessions.pop(sess.member.id, None)

            self._cancel_group(grp.session_id)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f'Lỗi _run_group [{grp.name}]: {e}')
        finally:
            # Cleanup: remove all member sessions and the group itself
            for mid in list(grp.members.keys()):
                self._sessions.pop(mid, None)
            self._groups.pop(grp.session_id, None)

    async def _countdown_loop(self, sess: PomodoroSession):
        """Cập nhật tin nhắn countdown cá nhân mỗi EDIT_INTERVAL giây."""
        try:
            while sess.phase_remaining > 0:
                await asyncio.sleep(EDIT_INTERVAL)
                if sess.live_message and sess.phase_remaining > 0:
                    try:
                        await sess.live_message.edit(
                            content=self._build_personal_embed(sess)
                        )
                    except discord.NotFound:
                        sess.live_message = None
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass

    async def _group_countdown_loop(self, grp: GroupSession):
        """Cập nhật tin nhắn nhóm mỗi EDIT_INTERVAL giây."""
        try:
            while True:
                await asyncio.sleep(EDIT_INTERVAL)
                if grp.announce_msg:
                    try:
                        await grp.announce_msg.edit(
                            content=self._build_group_embed(grp)
                        )
                    except discord.NotFound:
                        grp.announce_msg = None
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass

    # ─── MESSAGE BUILDERS ────────────────────────────────────────────────────

    def _build_personal_embed(self, sess: PomodoroSession) -> str:
        phase_icon  = {'work': '📖', 'break': '☕', 'done': '🎉', 'waiting': '⏳'}
        phase_label = {'work': 'LÀM VIỆC', 'break': 'NGHỈ NGƠI', 'done': 'HOÀN THÀNH', 'waiting': 'CHỜ'}

        icon  = phase_icon.get(sess.phase, '🍅')
        label = phase_label.get(sess.phase, sess.phase.upper())

        if sess.phase == 'done':
            return (
                f'{icon} **POMODORO HOÀN THÀNH** · **{sess.member.display_name}**\n'
                f'━━━━━━━━━━━━━━━━━━━━━━\n'
                f'🍅 {sess.rounds_bar}\n'
                f'✅ `{sess.completed_rounds}/{sess.total_rounds}` vòng hoàn thành\n'
                f'⏱️ Tổng: `{self._fmt(sess.completed_rounds * sess.work_minutes * 60)}`'
            )

        if sess.phase == 'waiting':
            return (
                f'⏳ **POMODORO CHỜ BẮT ĐẦU** · **{sess.member.display_name}**\n'
                f'⚙️ `{sess.work_minutes}m làm / {sess.break_minutes}m nghỉ × {sess.total_rounds} vòng`'
            )

        total_secs = (sess.work_minutes if sess.phase == 'work' else sess.break_minutes) * 60
        pct        = int((1 - sess.phase_remaining / total_secs) * 100) if total_secs > 0 else 0
        return (
            f'{icon} **{label}** · **{sess.member.display_name}**\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
            f'🍅 {sess.rounds_bar} · Vòng `{sess.current_round}/{sess.total_rounds}`\n'
            f'⏰ Còn lại: `{sess.format_remaining()}`\n'
            f'`{sess.progress_bar}` {pct}%\n'
            f'⚙️ `{sess.work_minutes}m/{sess.break_minutes}m` · _Cập nhật mỗi {EDIT_INTERVAL}s_'
        )

    def _build_group_embed(self, grp: GroupSession) -> str:
        phase_icon  = {'waiting': '⏳', 'work': '📖', 'break': '☕', 'done': '🎉'}
        phase_label = {'waiting': 'ĐANG CHỜ', 'work': 'LÀM VIỆC', 'break': 'NGHỈ NGƠI', 'done': 'HOÀN THÀNH'}

        icon  = phase_icon.get(grp.phase, '🍅')
        label = phase_label.get(grp.phase, grp.phase.upper())

        member_lines = []
        for sess in grp.members.values():
            bar = '🍅' * sess.completed_rounds + '⬜' * (grp.total_rounds - sess.completed_rounds)
            member_lines.append(f'  👤 **{sess.member.display_name}** {bar}')
        members_str = '\n'.join(member_lines) if member_lines else '  _Chưa có ai_'

        header = (
            f'{icon} **POMODORO NHÓM — {grp.name.upper()}** · {label}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━\n'
        )

        if grp.phase == 'waiting':
            body = (
                f'⚙️ `{grp.work_minutes}m làm / {grp.break_minutes}m nghỉ × {grp.total_rounds} vòng`\n'
                f'👑 Host: **{grp.host.display_name}**\n\n'
                f'👥 **Thành viên ({grp.member_count}):**\n{members_str}\n\n'
                f'_Dùng `/pomodoro join {grp.name}` để vào · '
                f'Host dùng `/pomodoro start` để bắt đầu_'
            )
        elif grp.phase == 'done':
            body = (
                f'🎊 Cả nhóm đã hoàn thành `{grp.total_rounds}` vòng!\n\n'
                f'👥 **Thành viên:**\n{members_str}'
            )
        else:
            remaining = max(0, int((grp.phase_end - datetime.now()).total_seconds()))
            m, s      = divmod(remaining, 60)
            total     = (grp.work_minutes if grp.phase == 'work' else grp.break_minutes) * 60
            elapsed   = total - remaining
            pct       = int((elapsed / total) * 100) if total > 0 else 0
            bar       = '█' * (pct // 10) + '░' * (10 - pct // 10)
            body      = (
                f'Vòng `{grp.current_round}/{grp.total_rounds}` · '
                f'Còn lại: `{m:02d}:{s:02d}`\n'
                f'`{bar}` {pct}%\n\n'
                f'👥 **Thành viên ({grp.member_count}):**\n{members_str}\n\n'
                f'_⟳ Cập nhật mỗi {EDIT_INTERVAL}s_'
            )

        return header + body

    # ─── UTILITIES ───────────────────────────────────────────────────────────

    def _award_pomodoro_xp(self, uid: str):
        """
        [v2] Cộng XP thực sự cho 1 vòng Pomodoro hoàn thành.
        Gọi add_xp_fn từ bot.py để ghi trực tiếp vào data file.
        Backward compatible: nếu add_xp_fn không được truyền thì không làm gì.
        """
        try:
            self._add_xp(uid, XP_PER_ROUND)
            log.info(f'[Pomodoro] +{XP_PER_ROUND} XP → uid {uid}')
        except Exception as e:
            log.error(f'[Pomodoro] _award_pomodoro_xp error: {e}')

    def _update_history(
        self,
        member_id: int,
        sess:      PomodoroSession,
        partial:   bool = False,
    ) -> dict:
        """
        Cập nhật lịch sử Pomodoro in-memory và lưu vào file data.
        partial=True: tính 1 vòng (dùng trong legacy code nếu cần).
        partial=False (mặc định): tính tổng sess.completed_rounds.

        LƯU Ý v2: _award_pomodoro_xp đã được gọi riêng sau mỗi vòng,
        nên _update_history chỉ cập nhật lịch sử (total_rounds, total_minutes)
        và KHÔNG gọi _add_xp thêm lần nữa để tránh nhân đôi.
        """
        today       = datetime.now().strftime('%Y-%m-%d')
        h           = self._history.setdefault(member_id, {
            'total_rounds':   0,
            'total_minutes':  0,
            'total_xp':       0,
            'best_streak':    0,
            'today_rounds':   0,
            'today_date':     today,
            'current_streak': 0,
        })

        if h.get('today_date') != today:
            h['today_rounds'] = 0
            h['today_date']   = today

        rounds_done = 1 if partial else sess.completed_rounds
        xp_bonus    = rounds_done * XP_PER_ROUND
        minutes     = rounds_done * sess.work_minutes

        h['total_rounds']   += rounds_done
        h['total_minutes']  += minutes
        h['total_xp']       += xp_bonus
        h['today_rounds']   += rounds_done
        h['current_streak'] += rounds_done
        h['best_streak']     = max(h['best_streak'], h['current_streak'])

        # [v2] Lưu vào file data (persistent)
        try:
            uid  = str(member_id)
            data = self._load()
            if uid in data:
                # Sync lịch sử từ memory vào file
                data[uid]['pomo_history'] = {
                    'total_rounds':   h['total_rounds'],
                    'total_minutes':  h['total_minutes'],
                    'total_xp':       h['total_xp'],
                    'best_streak':    h['best_streak'],
                    'today_rounds':   h['today_rounds'],
                    'today_date':     h['today_date'],
                    'current_streak': h['current_streak'],
                }
                self._save(data)
        except Exception as e:
            log.error(f'[Pomodoro] _update_history save error: {e}')

        return {
            'xp_bonus':      xp_bonus,
            'added_seconds': minutes * 60,
            'total_rounds':  h['total_rounds'],
        }

    def _cancel_session(self, member_id: int):
        sess = self._sessions.pop(member_id, None)
        if sess and sess.task and not sess.task.done():
            sess.task.cancel()

    def _cancel_group(self, session_id: str):
        grp = self._groups.pop(session_id, None)
        if grp and grp.task and not grp.task.done():
            grp.task.cancel()

    # ─── HOST CONTROL: bắt đầu nhóm qua /pomodoro start khi đang trong nhóm ─

    async def _start_group_if_host(
        self,
        interaction: discord.Interaction,
        sess:        PomodoroSession,
    ) -> bool:
        """Nếu người dùng là host của nhóm và nhóm đang waiting, khởi chạy nhóm."""
        if not sess.group_id:
            return False
        grp = self._groups.get(sess.group_id)
        if not grp or grp.phase != 'waiting':
            return False
        if grp.host.id != interaction.user.id:
            await interaction.response.send_message(
                f'⚠️ Chỉ host (**{grp.host.display_name}**) mới có thể bắt đầu phòng nhóm.',
                ephemeral=True
            )
            return True

        # Cập nhật phase_end cho tất cả members
        grp.phase         = 'work'
        grp.phase_end     = datetime.now() + timedelta(minutes=grp.work_minutes)
        grp.current_round = 1
        for s in grp.members.values():
            s.phase     = 'work'
            s.phase_end = grp.phase_end

        grp.task = asyncio.create_task(self._run_group(grp))
        await interaction.response.send_message(
            f'▶️ Đã bắt đầu phòng **"{grp.name}"** với `{grp.member_count}` người!\n'
            f'🍅 `{grp.work_minutes}m làm / {grp.break_minutes}m nghỉ × {grp.total_rounds} vòng`',
            ephemeral=True
        )
        return True


# ─── INTEGRATION HELPER ──────────────────────────────────────────────────────

def create_pomodoro_cog(
    bot,
    add_study_time,
    safe_send_dm,
    format_time,
    load_data_fn=None,   # [v2] hàm load_data từ bot.py
    save_data_fn=None,   # [v2] hàm save_data từ bot.py
    add_xp_fn=None,      # [v2] hàm add_xp_direct từ bot.py
):
    """
    Hàm tiện ích tạo và trả về PomodoroCog đã được cấu hình.

    Cách dùng trong bot.py (trong on_ready hoặc setup_hook):

        from pomodoro import create_pomodoro_cog

        # Cách dùng v1 (vẫn hoạt động — backward compatible):
        pomo_cog = create_pomodoro_cog(bot, add_study_time, safe_send_dm, format_time)

        # Cách dùng v2 (đầy đủ — lưu lịch sử persistent và XP thực):
        pomo_cog = create_pomodoro_cog(
            bot, add_study_time, safe_send_dm, format_time,
            load_data_fn=load_data,
            save_data_fn=save_data,
            add_xp_fn=add_xp_direct,
        )

        await bot.add_cog(pomo_cog)
    """
    return PomodoroCog(
        bot,
        add_study_time,
        safe_send_dm,
        format_time,
        load_data_fn=load_data_fn,
        save_data_fn=save_data_fn,
        add_xp_fn=add_xp_fn,
    )