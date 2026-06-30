from __future__ import annotations

import os
import re
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from dateutil import parser as date_parser
from discord import app_commands
from discord.ext import commands, tasks as discord_tasks


DURATION_RE = re.compile(r'^(?:in\s+)?(\d{1,4})\s*([mhd])$', re.IGNORECASE)
TIME_RE = re.compile(r'^(\d{1,2}):(\d{2})$')


class RemindersCog(commands.Cog, name='RemindersCog'):
    reminders = app_commands.Group(name='reminders', description='Reminders học tập đã tắt')

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        return

    async def cog_unload(self):
        if self.reminder_loop.is_running():
            self.reminder_loop.cancel()

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

    def _parse_when(self, guild_id: int, raw: str) -> datetime:
        tz = self._timezone(guild_id)
        text = str(raw or '').strip()
        lowered = text.lower()
        now = datetime.now(tz)

        duration = DURATION_RE.match(lowered)
        if duration:
            amount = int(duration.group(1))
            unit = duration.group(2).lower()
            if unit == 'm':
                return now + timedelta(minutes=amount)
            if unit == 'h':
                return now + timedelta(hours=amount)
            return now + timedelta(days=amount)

        for prefix, offset in (('today ', 0), ('tomorrow ', 1)):
            if lowered.startswith(prefix):
                clock = text[len(prefix):].strip()
                match = TIME_RE.match(clock)
                if not match:
                    raise ValueError('Dùng dạng `tomorrow 20:00`, `30m`, hoặc `2h`.')
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

    def _format_when(self, guild_id: int, raw: str) -> str:
        tz = self._timezone(guild_id)
        return datetime.fromisoformat(raw).astimezone(tz).strftime('%Y-%m-%d %H:%M %Z')

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        return True

    @app_commands.command(name='remindme', description='Đặt reminder một lần')
    @app_commands.describe(when='Ví dụ: 30m, 2h, tomorrow 20:00', message='Nội dung nhắc')
    async def remindme(self, interaction: discord.Interaction, when: str, message: str):
        if not await self._guard(interaction):
            return
        await interaction.response.send_message(
            'Tính năng reminder học tập đã được tắt. Bot sẽ không gửi DM hoặc thông báo nhắc học.',
            ephemeral=True,
        )

    @reminders.command(name='list', description='Xem reminder chưa gửi')
    async def list_reminders(self, interaction: discord.Interaction):
        if not await self._guard(interaction):
            return
        rows = self.bot.study_context.repository.list_reminders(interaction.guild_id, interaction.user.id)
        if not rows:
            await interaction.response.send_message('Bạn chưa có reminder nào.', ephemeral=True)
            return
        lines = ['**Reminders của bạn**']
        for row in rows:
            lines.append(
                f'`#{row["id"]}` `{self._format_when(interaction.guild_id, row["remind_at"])}` · {row["message"]}'
            )
        await interaction.response.send_message('\n'.join(lines)[:1900], ephemeral=True)

    @reminders.command(name='cancel', description='Hủy reminder chưa gửi')
    @app_commands.describe(reminder_id='ID trong /reminders list')
    async def cancel_reminder(self, interaction: discord.Interaction, reminder_id: int):
        if not await self._guard(interaction):
            return
        ok = self.bot.study_context.repository.cancel_reminder(
            interaction.guild_id,
            interaction.user.id,
            reminder_id,
        )
        await interaction.response.send_message('Đã hủy reminder.' if ok else 'Không tìm thấy reminder.', ephemeral=True)

    @discord_tasks.loop(minutes=1)
    async def reminder_loop(self):
        return

    @reminder_loop.before_loop
    async def before_reminder_loop(self):
        await self.bot.wait_until_ready()

    async def _send_reminder(self, reminder: dict):
        return


async def setup(bot: commands.Bot):
    await bot.add_cog(RemindersCog(bot))
