from __future__ import annotations

import os
import re
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from dateutil import parser as date_parser
from discord import app_commands
from discord.ext import commands, tasks as discord_tasks


TIME_RE = re.compile(r'^(\d{1,2}):(\d{2})$')


class ScheduleCog(commands.Cog, name='ScheduleCog'):
    schedule = app_commands.Group(name='schedule', description='Lịch accountability học tập')

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        if not self.schedule_settlement_loop.is_running():
            self.schedule_settlement_loop.start()

    async def cog_unload(self):
        self.schedule_settlement_loop.cancel()

    def _timezone(self, guild_id: int) -> ZoneInfo:
        raw = (
            self.bot.study_context.config_manager.get(guild_id, 'timezone')
            or os.getenv('TZ')
            or 'UTC'
        )
        try:
            return ZoneInfo(str(raw))
        except ZoneInfoNotFoundError:
            return ZoneInfo('UTC')

    def _parse_start(self, guild_id: int, raw: str) -> datetime:
        tz = self._timezone(guild_id)
        text = str(raw or '').strip()
        lowered = text.lower()
        now = datetime.now(tz)

        for prefix, offset in (('today ', 0), ('tomorrow ', 1)):
            if lowered.startswith(prefix):
                clock = text[len(prefix):].strip()
                match = TIME_RE.match(clock)
                if not match:
                    raise ValueError('Dùng dạng `today 20:00` hoặc `tomorrow 20:00`.')
                hour, minute = int(match.group(1)), int(match.group(2))
                if hour > 23 or minute > 59:
                    raise ValueError('Giờ không hợp lệ.')
                day = (now + timedelta(days=offset)).date()
                return datetime.combine(day, time(hour, minute), tzinfo=tz)

        parsed = date_parser.parse(text, fuzzy=True)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=tz)
        return parsed.astimezone(tz)

    @staticmethod
    def _utc_iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).isoformat(timespec='seconds')

    def _format_start(self, guild_id: int, raw: str) -> str:
        tz = self._timezone(guild_id)
        dt = datetime.fromisoformat(raw).astimezone(tz)
        return dt.strftime('%Y-%m-%d %H:%M %Z')

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        if not await self.bot.study_context.acl_check(interaction, 'schedule.use'):
            await interaction.response.send_message('ACL đang chặn bạn dùng schedule.', ephemeral=True)
            return False
        return True

    @schedule.command(name='book', description='Book một phiên học có trách nhiệm')
    @app_commands.describe(
        start='Ví dụ: tomorrow 20:00 hoặc 2026-06-09 20:00',
        duration_minutes='Thời lượng phiên học',
        deposit_coins='Coins đặt cọc tùy chọn',
    )
    async def book(
        self,
        interaction: discord.Interaction,
        start: str,
        duration_minutes: app_commands.Range[int, 15, 480] = 60,
        deposit_coins: app_commands.Range[int, 0, 1_000_000] = 0,
    ):
        if not await self._guard(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            start_dt = self._parse_start(interaction.guild_id, start)
        except (ValueError, OverflowError) as e:
            await interaction.followup.send(f'Không đọc được thời gian: {e}', ephemeral=True)
            return
        if start_dt <= datetime.now(start_dt.tzinfo) + timedelta(minutes=1):
            await interaction.followup.send('Thời gian bắt đầu phải ở tương lai.', ephemeral=True)
            return
        try:
            session = self.bot.study_context.repository.create_scheduled_session(
                guild_id=interaction.guild_id,
                user_id=interaction.user.id,
                display_name=interaction.user.display_name,
                start_at=self._utc_iso(start_dt),
                duration_minutes=int(duration_minutes),
                deposit_coins=int(deposit_coins),
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        deposit = int(session.get('deposit_coins') or 0)
        deposit_line = f'\nDeposit: `{deposit:,}` coins' if deposit else ''
        await interaction.followup.send(
            (
                f'Đã book lịch học `#{session["id"]}`.\n'
                f'Bắt đầu: `{self._format_start(interaction.guild_id, session["start_at"])}`\n'
                f'Thời lượng: `{session["duration_minutes"]}` phút'
                f'{deposit_line}'
            ),
            ephemeral=True,
        )

    @schedule.command(name='list', description='Xem lịch học đã book')
    async def list_sessions(self, interaction: discord.Interaction):
        if not await self._guard(interaction):
            return
        rows = self.bot.study_context.repository.list_scheduled_sessions(
            interaction.guild_id,
            interaction.user.id,
            include_done=False,
        )
        if not rows:
            await interaction.response.send_message('Bạn chưa có lịch học nào.', ephemeral=True)
            return
        lines = ['**Lịch học của bạn**']
        for row in rows:
            deposit = int(row.get('deposit_coins') or 0)
            deposit_text = f' · deposit `{deposit:,}`' if deposit else ''
            lines.append(
                f'`#{row["id"]}` `{self._format_start(interaction.guild_id, row["start_at"])}` '
                f'· `{row["duration_minutes"]}` phút{deposit_text}'
            )
        await interaction.response.send_message('\n'.join(lines)[:1900], ephemeral=True)

    @schedule.command(name='cancel', description='Hủy lịch học')
    @app_commands.describe(session_id='ID trong /schedule list')
    async def cancel(self, interaction: discord.Interaction, session_id: int):
        if not await self._guard(interaction):
            return
        is_admin = await self.bot.study_context.is_admin_actor(interaction)
        result = self.bot.study_context.repository.cancel_scheduled_session(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            display_name=interaction.user.display_name,
            session_id=session_id,
            admin_override=is_admin,
        )
        await interaction.response.send_message(result['message'], ephemeral=True)

    @discord_tasks.loop(minutes=5)
    async def schedule_settlement_loop(self):
        due_at = datetime.now(timezone.utc).isoformat(timespec='seconds')
        bonus = 10
        try:
            bonus = int(self.bot.study_context.config_manager.defaults.get('schedule_completion_bonus_coins', 10))
        except Exception:
            bonus = 10
        settled = self.bot.study_context.repository.process_due_scheduled_sessions(
            due_at=due_at,
            completion_bonus_coins=bonus,
        )
        for session in settled:
            await self._notify_settlement(session)

    @schedule_settlement_loop.before_loop
    async def before_schedule_settlement_loop(self):
        await self.bot.wait_until_ready()

    async def _notify_settlement(self, session: dict):
        try:
            user = self.bot.get_user(int(session['user_id'])) or await self.bot.fetch_user(int(session['user_id']))
        except discord.HTTPException:
            return
        status = session.get('status')
        if status == 'completed':
            title = 'Lịch học hoàn thành'
            color = 0x2ECC71
            description = (
                f'Bạn đã học `{self.bot.study_context.format_time(session.get("studied_seconds", 0))}` '
                f'cho lịch `#{session["id"]}`.\n'
                f'Hoàn lại/bonus: `{int(session.get("refunded_coins") or 0):,}` coins.'
            )
        else:
            title = 'Lịch học bị miss'
            color = 0xE74C3C
            description = (
                f'Lịch `#{session["id"]}` cần tối thiểu '
                f'`{self.bot.study_context.format_time(session.get("required_seconds", 0))}`.\n'
                'Deposit, nếu có, đã được giữ lại.'
            )
        embed = discord.Embed(title=title, description=description, color=color)
        try:
            await user.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except discord.HTTPException:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(ScheduleCog(bot))
