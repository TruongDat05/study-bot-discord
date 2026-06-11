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
    reminders = app_commands.Group(name='reminders', description='Quản lý reminders học tập')

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        if not self.reminder_loop.is_running():
            self.reminder_loop.start()

    async def cog_unload(self):
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
        try:
            remind_at = self._parse_when(interaction.guild_id, when)
        except (ValueError, OverflowError) as e:
            await interaction.response.send_message(f'Không đọc được thời gian: {e}', ephemeral=True)
            return
        if remind_at <= datetime.now(remind_at.tzinfo) + timedelta(seconds=30):
            await interaction.response.send_message('Reminder phải ở tương lai.', ephemeral=True)
            return
        reminder_id = self.bot.study_context.repository.create_reminder(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            display_name=interaction.user.display_name,
            remind_at=self._utc_iso(remind_at),
            message=message,
            channel_id=interaction.channel_id,
        )
        await interaction.response.send_message(
            f'Đã đặt reminder `#{reminder_id}` lúc `{self._format_when(interaction.guild_id, self._utc_iso(remind_at))}`.',
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
        due_at = datetime.now(timezone.utc).isoformat(timespec='seconds')
        reminders = self.bot.study_context.repository.claim_due_reminders(due_at, limit=25)
        for reminder in reminders:
            await self._send_reminder(reminder)

    @reminder_loop.before_loop
    async def before_reminder_loop(self):
        await self.bot.wait_until_ready()

    async def _send_reminder(self, reminder: dict):
        guild_id = int(reminder['guild_id'])
        guild = self.bot.get_guild(guild_id)
        user = self.bot.get_user(int(reminder['user_id']))
        if user is None:
            try:
                user = await self.bot.fetch_user(int(reminder['user_id']))
            except discord.HTTPException:
                user = None

        embed = discord.Embed(
            title='Nhắc học',
            description=str(reminder.get('message') or 'Đến giờ học rồi.'),
            color=0x5865F2,
        )
        embed.add_field(name='Thời gian', value=self._format_when(guild_id, reminder['remind_at']), inline=False)

        mode = str(self.bot.study_context.config_manager.get(guild_id, 'reminder_delivery', 'dm') or 'dm').lower()
        channel = None
        channel_id = reminder.get('channel_id')
        if guild and channel_id:
            channel = guild.get_channel(int(channel_id))
        if not channel and guild:
            report_channel_id = self.bot.study_context.config_manager.get(guild_id, 'report_channel_id')
            if report_channel_id:
                channel = guild.get_channel(int(report_channel_id))

        destinations = []
        if mode == 'channel':
            destinations = [channel, user]
        else:
            destinations = [user, channel]

        for destination in destinations:
            if destination is None:
                continue
            try:
                await destination.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                return
            except (discord.Forbidden, discord.HTTPException):
                continue


async def setup(bot: commands.Bot):
    await bot.add_cog(RemindersCog(bot))
