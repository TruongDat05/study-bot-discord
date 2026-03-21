"""
╔══════════════════════════════════════════════════════════════════╗
║         WEEKLY REPORT MODULE v2 — UPGRADE                        ║
║                                                                  ║
║  Thay đổi v2:                                                    ║
║  • /weekly leaderboard — top người học nhiều nhất tuần này      ║
║  • /weekly compare — so sánh cá nhân 2 tuần gần nhất            ║
║  • Badge tuần hoạt động vì badge_dates đã được lưu trong v4     ║
║  • Báo cáo tuần bao gồm thống kê Quest đã làm                   ║
║  • Nhận xét thông minh hơn, nhiều phân nhánh hơn                ║
║  • Thêm emoji calendar vào biểu đồ ASCII                        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import discord
from discord import app_commands
from discord.ext import tasks
import discord.ext.commands
from datetime import datetime, timedelta
import logging
import asyncio

log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

WEEKLY_SEND_WEEKDAY = 6      # 0=T2 … 6=CN
WEEKLY_SEND_HOUR    = 20
WEEKLY_SEND_MINUTE  = 0
WEEKLY_OPT_OUT_KEY  = 'weekly_opt_out'

TIER_EXCELLENT  = 20    # >= 20h/tuần
TIER_GOOD       = 10    # >= 10h
TIER_OK         = 3     # >= 3h

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _week_dates(offset: int = 0) -> list[str]:
    today  = datetime.now().date()
    monday = today - timedelta(days=today.weekday()) + timedelta(weeks=offset)
    return [(monday + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

def _week_total(info: dict, dates: list[str]) -> int:
    return sum(info.get('daily', {}).get(d, 0) for d in dates)

def _ascii_bar(minutes: int, max_minutes: int, width: int = 10) -> str:
    if max_minutes <= 0: return '░' * width
    filled = round(width * minutes / max_minutes)
    return '█' * filled + '░' * (width - filled)

def _trend_icon(this_week: int, last_week: int) -> str:
    if last_week == 0:    return '🆕'
    diff = (this_week - last_week) / last_week * 100
    if diff >= 20:        return '📈'
    if diff <= -20:       return '📉'
    return '➡️'

def _format_time(seconds: int) -> str:
    if seconds <= 0: return '0 phút'
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0 and m > 0: return f'{h}h {m}m'
    if h > 0:            return f'{h}h'
    return f'{m}m'

def _diff_str(this_week: int, last_week: int) -> str:
    diff = this_week - last_week
    if diff == 0: return 'bằng tuần trước'
    sign = '+' if diff > 0 else ''
    return f'{sign}{_format_time(abs(diff))} so với tuần trước'

def _badges_this_week(info: dict, week_dates: list[str], all_badges: dict) -> list[str]:
    badge_dates = info.get('badge_dates', {})
    return [bid for bid, date_str in badge_dates.items() if date_str in week_dates]

def _day_emoji(day_index: int) -> str:
    """Trả về emoji cho thứ trong tuần."""
    return ['🌙', '📚', '📖', '✏️', '🎯', '⭐', '🌅'][day_index]

def _personalized_advice(
    this_secs:  int,
    last_secs:  int,
    streak:     int,
    daily_secs: list[int],
    quests_done: int = 0,
) -> str:
    this_h      = this_secs / 3600
    active_days = sum(1 for s in daily_secs if s > 0)

    if active_days == 0:
        return (
            '😴 Tuần này bạn chưa học ngày nào. Đừng để streak về 0 nhé!\n'
            'Chỉ cần 15 phút mỗi ngày là đủ để giữ động lực. 💪\n'
            '_Mẹo: Dùng `/remind <giờ>` để bot nhắc bạn học mỗi ngày!_'
        )

    if active_days == 7:
        if this_h >= TIER_EXCELLENT:
            return (
                f'🔥🏆 **TUYỆT VỜI!** Bạn học **{active_days}/7 ngày** VÀ đạt **{this_h:.1f}h**!\n'
                f'Streak {streak} ngày — bạn đang ở đẳng cấp ELITE! 👑'
            )
        return (
            f'🔥 Bạn học **đủ 7/7 ngày** tuần này! Kỷ luật đỉnh cao!\n'
            f'Streak hiện tại: **{streak} ngày** — giữ vững nhé! 👑'
        )

    if active_days <= 2:
        return (
            f'⚠️ Bạn chỉ học **{active_days}/7 ngày** tuần này.\n'
            f'Học đều quan trọng hơn học nhiều một lần. Thử đặt reminder nhé!\n'
            f'_Dùng `/remind <giờ>` để bot nhắc bạn học mỗi ngày!_'
        )

    if last_secs > 0:
        diff_pct = (this_secs - last_secs) / last_secs * 100
        if diff_pct >= 50:
            return (
                f'🚀 WOW! Tuần này bạn học hơn tuần trước **{diff_pct:.0f}%**!\n'
                f'Đà lên rất mạnh — tiếp tục phát huy! ⭐'
            )
        if diff_pct >= 20:
            return (
                f'📈 Tuần này bạn học hơn tuần trước **{diff_pct:.0f}%**!\n'
                f'Đà tốt lắm, hãy duy trì nhé!'
            )
        if diff_pct <= -40:
            return (
                f'📉 Tuần này giảm mạnh {abs(diff_pct):.0f}% so với tuần trước.\n'
                f'Không sao — tuần sau bắt đầu lại! 💪\n'
                f'_Dùng `/quest` để có thêm động lực mỗi ngày._'
            )

    # Nhận xét dựa trên tổng giờ
    if this_h >= TIER_EXCELLENT:
        return (
            f'🚀 **{this_h:.1f} tiếng** trong tuần — đỉnh cao!\n'
            f'Bạn đang ở top tier. Nhớ nghỉ ngơi đủ giấc nhé! 💎'
        )
    if this_h >= TIER_GOOD:
        need = max(1, TIER_EXCELLENT - this_h)
        return (
            f'✨ **{this_h:.1f} tiếng** — kết quả tốt!\n'
            f'Thêm ~{need:.0f}h nữa sẽ đạt mức xuất sắc. Bạn làm được! 💪'
        )
    if this_h >= TIER_OK:
        extra_tip = f'\n🎯 Bạn đã làm {quests_done} quest — thêm quest để tăng XP nhanh hơn!' if quests_done > 0 else ''
        return (
            f'👍 **{this_h:.1f} tiếng** — ổn, nhưng còn tiềm năng hơn!\n'
            f'Đặt goal {TIER_GOOD}h/tuần bằng lệnh `/setgoal`.{extra_tip}'
        )
    return (
        f'💡 **{this_h:.1f} tiếng** tuần này — hơi ít đấy!\n'
        f'Thử 30 phút mỗi tối là đủ để cải thiện nhiều.\n'
        f'_Dùng `/remind <giờ>` để bot nhắc bạn! ⏰_'
    )

def _build_weekly_dm(
    member_name: str,
    info:        dict,
    this_week:   list[str],
    last_week:   list[str],
    all_badges:  dict,
) -> str:
    this_secs   = _week_total(info, this_week)
    last_secs   = _week_total(info, last_week)
    trend       = _trend_icon(this_secs, last_secs)
    diff_text   = _diff_str(this_secs, last_secs)
    streak      = info.get('streak', 0)
    longest     = info.get('longest_streak', 0)
    xp          = info.get('xp', 0)
    level       = info.get('level', 0)
    total_secs  = info.get('total', 0)
    quests_done = info.get('quests_done_total', 0)

    day_names  = ['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'CN']
    daily_secs = [info.get('daily', {}).get(d, 0) for d in this_week]
    max_day    = max(daily_secs) if daily_secs else 1
    today_str  = datetime.now().strftime('%Y-%m-%d')

    chart_lines = []
    for i, (day, secs) in enumerate(zip(day_names, daily_secs)):
        bar          = _ascii_bar(secs // 60, max_day // 60, width=10)
        time_s       = _format_time(secs) if secs > 0 else '—'
        today_marker = ' ◀ hôm nay' if this_week[i] == today_str else ''
        emoji        = _day_emoji(i)
        chart_lines.append(f'{emoji} `{day}` `{bar}` `{time_s}`{today_marker}')

    # Badge tuần này
    new_badge_ids = _badges_this_week(info, this_week, all_badges)
    badge_section = ''
    if new_badge_ids:
        names = [all_badges.get(bid, {}).get('name', bid) for bid in new_badge_ids]
        badge_section = f'\n🏅 **Huy hiệu nhận tuần này:** {" · ".join(names)}\n'

    advice      = _personalized_advice(this_secs, last_secs, streak, daily_secs, quests_done)
    active_days = sum(1 for s in daily_secs if s > 0)
    day_icons   = ''.join('🟢' if s > 0 else '⬜' for s in daily_secs)
    week_start  = this_week[0][5:]
    week_end    = this_week[6][5:]

    # Tính % so với tuần trước
    pct_str = ''
    if last_secs > 0:
        pct = int((this_secs - last_secs) / last_secs * 100)
        pct_str = f' (`{"+"+str(pct) if pct >= 0 else str(pct)}%`)'

    msg = (
        f'📊 **BÁO CÁO TUẦN · {week_start} → {week_end}**\n'
        f'Xin chào **{member_name}**! Đây là tóm tắt tuần của bạn.\n\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'⏱️ **Tổng tuần này:** `{_format_time(this_secs)}`{pct_str}\n'
        f'{trend} _{diff_text}_\n'
        f'📚 **Tổng toàn thời gian:** `{_format_time(total_secs)}`\n\n'
        f'**📅 Hoạt động 7 ngày:**\n'
        + '\n'.join(chart_lines) + '\n\n'
        f'**Độ đều đặn:** {day_icons} `{active_days}/7 ngày`\n'
        f'🔥 **Streak:** `{streak} ngày` _(kỷ lục: {longest})_\n'
        f'⚡ **Level:** `Lv.{level}` · `{xp:,} XP`\n'
        + badge_section + '\n'
        f'━━━━━━━━━━━━━━━━━━━━━━\n'
        f'💬 **Nhận xét:**\n{advice}\n\n'
        f'_`/weekly off` tắt báo cáo · `/weekly preview` xem trước bất lúc nào_'
    )
    return msg

# ─── COG FACTORY ─────────────────────────────────────────────────────────────

def create_weekly_report_cog(
    bot,
    load_data_fn,
    save_data_fn,
    all_badges: dict,
    safe_send_dm_fn,
):
    class WeeklyReportCog(discord.ext.commands.Cog, name='WeeklyReport'):

        def __init__(self):
            self._weekly_task_started = False

        weekly_group = app_commands.Group(
            name='weekly',
            description='Cài đặt báo cáo tuần tự động'
        )

        # ── /weekly preview ───────────────────────────────────────────────

        @weekly_group.command(name='preview', description='Xem trước báo cáo tuần ngay bây giờ')
        async def weekly_preview(self, interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True)
            uid  = str(interaction.user.id)
            data = load_data_fn()
            if uid not in data:
                await interaction.followup.send(
                    '❌ Bạn chưa có dữ liệu! Hãy vào phòng học trước.', ephemeral=True
                ); return
            msg = _build_weekly_dm(
                data[uid].get('name', interaction.user.display_name),
                data[uid], _week_dates(0), _week_dates(-1), all_badges,
            )
            # Discord limit 2000 chars per message
            if len(msg) > 1950:
                msg = msg[:1950] + '\n_...(rút gọn)_'
            await interaction.followup.send(
                f'👁️ **Xem trước báo cáo tuần:**\n\n{msg}', ephemeral=True
            )

        # ── /weekly off ───────────────────────────────────────────────────

        @weekly_group.command(name='off', description='Tắt nhận báo cáo tuần qua DM')
        async def weekly_off(self, interaction: discord.Interaction):
            uid  = str(interaction.user.id)
            data = load_data_fn()
            if uid not in data:
                await interaction.response.send_message('❌ Chưa có dữ liệu!', ephemeral=True); return
            data[uid][WEEKLY_OPT_OUT_KEY] = True
            save_data_fn(data)
            await interaction.response.send_message(
                '✅ Đã tắt báo cáo tuần.\n_`/weekly on` để bật lại._', ephemeral=True
            )
            log.info(f'[WeeklyReport] {interaction.user.display_name} opt-out.')

        # ── /weekly on ────────────────────────────────────────────────────

        @weekly_group.command(name='on', description='Bật lại nhận báo cáo tuần tự động')
        async def weekly_on(self, interaction: discord.Interaction):
            uid  = str(interaction.user.id)
            data = load_data_fn()
            if uid not in data:
                await interaction.response.send_message('❌ Chưa có dữ liệu!', ephemeral=True); return
            data[uid].pop(WEEKLY_OPT_OUT_KEY, None)
            save_data_fn(data)
            await interaction.response.send_message(
                f'✅ Đã bật báo cáo tuần!\n'
                f'Bot DM bạn mỗi **Chủ nhật {WEEKLY_SEND_HOUR:02d}:{WEEKLY_SEND_MINUTE:02d}**.',
                ephemeral=True
            )

        # ── /weekly status ────────────────────────────────────────────────

        @weekly_group.command(name='status', description='Trạng thái báo cáo tuần')
        async def weekly_status(self, interaction: discord.Interaction):
            uid       = str(interaction.user.id)
            data      = load_data_fn()
            opted_out = data.get(uid, {}).get(WEEKLY_OPT_OUT_KEY, False)
            now       = datetime.now()
            days_until = (6 - now.weekday()) % 7
            if days_until == 0 and now.hour >= WEEKLY_SEND_HOUR:
                days_until = 7
            next_send  = (now + timedelta(days=days_until)).replace(
                hour=WEEKLY_SEND_HOUR, minute=0, second=0
            )
            diff       = next_send - now
            h_left     = int(diff.total_seconds() // 3600)
            m_left     = int((diff.total_seconds() % 3600) // 60)

            # Thống kê tuần hiện tại
            this_week  = _week_dates(0)
            this_secs  = _week_total(data.get(uid, {}), this_week)
            active_days = sum(1 for d in this_week if data.get(uid, {}).get('daily', {}).get(d, 0) > 0)

            await interaction.response.send_message(
                f'**Báo cáo tuần:** {"🔴 Đã tắt" if opted_out else "🟢 Đang bật"}\n'
                f'📅 Gửi tiếp theo: **Chủ nhật {WEEKLY_SEND_HOUR:02d}h** (còn `{h_left}h {m_left}m`)\n\n'
                f'**Tuần này của bạn:**\n'
                f'⏱️ Học: `{_format_time(this_secs)}` · 📅 Hoạt động: `{active_days}/7 ngày`\n\n'
                f'_`{"​/weekly off" if not opted_out else "/weekly on"}` để '
                f'{"tắt" if not opted_out else "bật"} báo cáo._',
                ephemeral=True
            )

        # ── /weekly leaderboard ───────────────────────────────────────────

        @weekly_group.command(name='leaderboard', description='Top người học nhiều nhất tuần này')
        async def weekly_leaderboard(self, interaction: discord.Interaction):
            data      = load_data_fn()
            this_week = _week_dates(0)
            top10 = sorted(
                [(uid, info, _week_total(info, this_week))
                 for uid, info in data.items()
                 if _week_total(info, this_week) > 0],
                key=lambda x: x[2], reverse=True
            )[:10]

            week_start = this_week[0][5:]
            week_end   = this_week[6][5:]
            lines = [f'🏆 **Top học tuần này** ({week_start} → {week_end})\n']

            if not top10:
                lines.append('😴 Chưa có ai học tuần này!')
            else:
                for i, (uid, info, secs) in enumerate(top10, 1):
                    medal  = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
                    streak = info.get('streak', 0)
                    level  = info.get('level', 0)
                    # So sánh với tuần trước
                    last_w = _week_total(info, _week_dates(-1))
                    trend  = _trend_icon(secs, last_w)
                    lines.append(
                        f'{medal} {trend} **{info["name"]}** `Lv.{level}` 🔥{streak}\n'
                        f'       ⏱️ Tuần này: `{_format_time(secs)}`'
                    )

            await interaction.response.send_message('\n'.join(lines))

        # ── /weekly compare ───────────────────────────────────────────────

        @weekly_group.command(name='compare', description='So sánh tuần này vs tuần trước của bạn')
        async def weekly_compare(self, interaction: discord.Interaction):
            uid  = str(interaction.user.id)
            data = load_data_fn()
            if uid not in data:
                await interaction.response.send_message(
                    '❌ Chưa có dữ liệu!', ephemeral=True
                ); return
            info      = data[uid]
            this_week = _week_dates(0)
            last_week = _week_dates(-1)
            this_secs = _week_total(info, this_week)
            last_secs = _week_total(info, last_week)

            day_names = ['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'CN']
            this_days = [info.get('daily', {}).get(d, 0) for d in this_week]
            last_days = [info.get('daily', {}).get(d, 0) for d in last_week]
            max_val   = max(1, max(this_days + last_days))

            lines = [f'📊 **So sánh tuần — {interaction.user.display_name}**\n']
            lines.append(f'`{"Ngày":<4}` `{"Tuần này":>9}` `{"Tuần trước":>10}`')
            lines.append('─' * 32)
            for i, day in enumerate(day_names):
                this_bar = _ascii_bar(this_days[i] // 60, max_val // 60, width=5)
                last_bar = _ascii_bar(last_days[i] // 60, max_val // 60, width=5)
                this_t   = _format_time(this_days[i]) if this_days[i] > 0 else '—'
                last_t   = _format_time(last_days[i]) if last_days[i] > 0 else '—'
                lines.append(f'`{day}` `{this_bar}` {this_t:<8} `{last_bar}` {last_t}')

            diff     = this_secs - last_secs
            diff_str = f'+{_format_time(diff)}' if diff >= 0 else f'-{_format_time(abs(diff))}'
            pct_str  = ''
            if last_secs > 0:
                pct     = int((this_secs - last_secs) / last_secs * 100)
                pct_str = f' (`{"+"+str(pct) if pct >= 0 else str(pct)}%`)'

            lines.append('─' * 32)
            lines.append(
                f'📊 Tuần này: `{_format_time(this_secs)}` | '
                f'Tuần trước: `{_format_time(last_secs)}`\n'
                f'{_trend_icon(this_secs, last_secs)} **{diff_str}**{pct_str}'
            )
            await interaction.response.send_message('\n'.join(lines), ephemeral=True)

        # ── /weekly send (admin) ──────────────────────────────────────────

        @weekly_group.command(name='send', description='[Admin] Gửi báo cáo tuần ngay')
        @app_commands.default_permissions(administrator=True)
        @app_commands.describe(target='Gửi cho 1 member cụ thể (để trống = tất cả)')
        async def weekly_send(
            self, interaction: discord.Interaction,
            target: discord.Member = None,
        ):
            await interaction.response.defer(ephemeral=True)
            sent, skipped = await self._do_send_reports(specific_member=target)
            if target:
                await interaction.followup.send(
                    f'✅ Đã gửi báo cáo cho **{target.display_name}**.', ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f'✅ Gửi: **{sent}** | Bỏ qua: **{skipped}**', ephemeral=True
                )

        # ── Prefix ───────────────────────────────────────────────────────

        @discord.ext.commands.command(name='weekly')
        async def cmd_weekly(self, ctx, action: str = 'status'):
            uid    = str(ctx.author.id)
            data   = load_data_fn()
            action = action.lower().strip()

            if action == 'preview':
                if uid not in data:
                    await ctx.send('❌ Chưa có dữ liệu!'); return
                msg = _build_weekly_dm(
                    data[uid].get('name', ctx.author.display_name),
                    data[uid], _week_dates(0), _week_dates(-1), all_badges,
                )
                try:
                    await ctx.author.send(f'👁️ **Xem trước báo cáo tuần:**\n\n{msg[:1900]}')
                    await ctx.send('✅ Đã gửi DM preview!')
                except discord.Forbidden:
                    await ctx.send('❌ Không thể gửi DM (bạn đang chặn DM bot).')

            elif action == 'off':
                if uid not in data:
                    await ctx.send('❌ Chưa có dữ liệu!'); return
                data[uid][WEEKLY_OPT_OUT_KEY] = True
                save_data_fn(data)
                await ctx.send('✅ Đã tắt báo cáo tuần.')

            elif action == 'on':
                if uid not in data:
                    await ctx.send('❌ Chưa có dữ liệu!'); return
                data[uid].pop(WEEKLY_OPT_OUT_KEY, None)
                save_data_fn(data)
                await ctx.send('✅ Đã bật lại báo cáo tuần!')

            elif action == 'send' and ctx.author.guild_permissions.administrator:
                sent, skipped = await self._do_send_reports()
                await ctx.send(f'✅ Đã gửi {sent} DM, bỏ qua {skipped}.')

            elif action == 'lb' or action == 'leaderboard':
                data      = load_data_fn()
                this_week = _week_dates(0)
                top5 = sorted(
                    [(uid, info, _week_total(info, this_week))
                     for uid, info in data.items()
                     if _week_total(info, this_week) > 0],
                    key=lambda x: x[2], reverse=True
                )[:5]
                lines = ['🏆 **Top học tuần này**\n']
                for i, (_, info, secs) in enumerate(top5, 1):
                    medal = ['🥇', '🥈', '🥉'][i-1] if i <= 3 else f'`{i}.`'
                    lines.append(f'{medal} **{info["name"]}** — `{_format_time(secs)}`')
                await ctx.send('\n'.join(lines))

            else:
                opted_out = data.get(uid, {}).get(WEEKLY_OPT_OUT_KEY, False)
                status    = '🔴 Đã tắt' if opted_out else '🟢 Đang bật'
                await ctx.send(
                    f'**Báo cáo tuần:** {status}\n'
                    f'Lệnh: `!weekly preview|off|on|lb`'
                )

        # ── Core send ─────────────────────────────────────────────────────

        async def _do_send_reports(
            self,
            specific_member: discord.Member = None,
        ) -> tuple[int, int]:
            data      = load_data_fn()
            this_week = _week_dates(0)
            last_week = _week_dates(-1)
            sent = skipped = 0

            if specific_member:
                targets = [(str(specific_member.id), specific_member)]
            else:
                member_map: dict[str, discord.Member] = {}
                for guild in bot.guilds:
                    for m in guild.members:
                        if not m.bot and str(m.id) not in member_map:
                            member_map[str(m.id)] = m
                targets = list(member_map.items())

            for uid, member in targets:
                info = data.get(uid)
                if not info:
                    skipped += 1; continue

                if info.get(WEEKLY_OPT_OUT_KEY, False):
                    skipped += 1
                    log.info(f'[WeeklyReport] skip {member.display_name} (opt-out)')
                    continue

                this_total = _week_total(info, this_week)
                last_total = _week_total(info, last_week)
                if this_total == 0 and last_total == 0:
                    skipped += 1; continue

                msg = _build_weekly_dm(
                    info.get('name', member.display_name),
                    info, this_week, last_week, all_badges,
                )
                if len(msg) > 1950:
                    msg = msg[:1950] + '\n_...(rút gọn)_'
                await safe_send_dm_fn(member, msg)
                sent += 1
                await asyncio.sleep(0.5)

            log.info(f'[WeeklyReport] Gửi: {sent} DM, bỏ qua: {skipped}')
            return sent, skipped

        # ── Scheduled task ────────────────────────────────────────────────

        @tasks.loop(minutes=1)
        async def _weekly_ticker(self):
            now = datetime.now()
            if (now.weekday() == WEEKLY_SEND_WEEKDAY
                    and now.hour   == WEEKLY_SEND_HOUR
                    and now.minute == WEEKLY_SEND_MINUTE):
                log.info('[WeeklyReport] Bắt đầu gửi báo cáo tuần...')
                sent, skipped = await self._do_send_reports()
                log.info(f'[WeeklyReport] Hoàn tất: {sent} gửi, {skipped} bỏ qua.')

        @_weekly_ticker.before_loop
        async def _before_ticker(self):
            await bot.wait_until_ready()

        def cog_load(self):
            if not self._weekly_task_started:
                self._weekly_ticker.start()
                self._weekly_task_started = True
                log.info('[WeeklyReport] Ticker khởi động.')

        def cog_unload(self):
            self._weekly_ticker.cancel()

    return WeeklyReportCog()

# ─── SETUP HELPER ────────────────────────────────────────────────────────────

async def setup_weekly_report(
    bot,
    load_data_fn,
    save_data_fn,
    all_badges: dict,
    safe_send_dm_fn,
):
    if bot.cogs.get('WeeklyReport'):
        return
    cog = create_weekly_report_cog(bot, load_data_fn, save_data_fn, all_badges, safe_send_dm_fn)
    await bot.add_cog(cog)
    # Đăng ký slash group
    try:
        bot.tree.add_command(cog.weekly_group)
    except Exception:
        pass  # Đã được đăng ký rồi
    log.info('[WeeklyReport] Cog đã được load.')