from __future__ import annotations

import logging
from contextlib import suppress
from datetime import timedelta

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)


class ModerationPlugin(commands.Cog, name='ModerationPlugin'):
    """Basic moderation commands guarded by ACL.

    These commands are intentionally small: they provide warn/timeout controls
    without touching the study/economy data model.
    """

    moderation = app_commands.Group(
        name='moderation',
        description='Lệnh moderation',
        default_permissions=discord.Permissions(administrator=True),
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.ctx = bot.study_context

    async def _guard(self, interaction: discord.Interaction, action: str) -> bool:
        if not await self.ctx.require_admin(interaction, action):
            return False
        return True

    @moderation.command(name='warn', description='Cảnh báo một thành viên')
    @app_commands.default_permissions(administrator=True)
    async def warn(self, interaction: discord.Interaction, member: discord.Member, reason: str = 'No reason provided'):
        if not await self._guard(interaction, 'moderation.warn'):
            return
        await interaction.response.send_message(
            f'Đã ghi cảnh báo cho **{member.display_name}**: `{reason[:300]}`',
            ephemeral=True,
        )
        with suppress(discord.Forbidden, discord.HTTPException):
            await member.send(f'Bạn đã nhận cảnh báo trong **{interaction.guild.name}**: {reason[:1500]}')

    @moderation.command(name='mute', description='Timeout một thành viên')
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(minutes='Số phút timeout', reason='Lý do')
    async def mute(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        minutes: app_commands.Range[int, 1, 40320],
        reason: str = 'Muted by moderator',
    ):
        if not await self._guard(interaction, 'moderation.mute'):
            return
        try:
            await member.timeout(timedelta(minutes=int(minutes)), reason=reason[:512])
        except discord.Forbidden:
            await interaction.response.send_message('Bot thiếu quyền timeout member này.', ephemeral=True)
            return
        except discord.HTTPException as e:
            await interaction.response.send_message(f'Timeout thất bại: `{e}`', ephemeral=True)
            return
        await interaction.response.send_message(
            f'Đã timeout **{member.display_name}** trong `{minutes}` phút.',
            ephemeral=True,
        )

    @moderation.command(name='unmute', description='Gỡ timeout một thành viên')
    @app_commands.default_permissions(administrator=True)
    async def unmute(self, interaction: discord.Interaction, member: discord.Member, reason: str = 'Unmuted by moderator'):
        if not await self._guard(interaction, 'moderation.unmute'):
            return
        try:
            await member.timeout(None, reason=reason[:512])
        except discord.Forbidden:
            await interaction.response.send_message('Bot thiếu quyền gỡ timeout member này.', ephemeral=True)
            return
        except discord.HTTPException as e:
            await interaction.response.send_message(f'Unmute thất bại: `{e}`', ephemeral=True)
            return
        await interaction.response.send_message(f'Đã gỡ timeout cho **{member.display_name}**.', ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ModerationPlugin(bot))
