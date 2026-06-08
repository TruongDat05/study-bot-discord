from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)


class AIChatPlugin(commands.Cog, name='AIChatPlugin'):
    """AI slash command and mention replies.

    The heavy provider/fallback implementation remains in the shared bot
    context for now. This plugin owns the Discord entry points and applies ACL
    plus per-guild channel config before any provider call is made.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.ctx = bot.study_context

    def _extract_question_from_mention(self, message: discord.Message) -> str:
        if not self.bot.user:
            return ''
        raw = message.content
        for mention in (f'<@{self.bot.user.id}>', f'<@!{self.bot.user.id}>'):
            raw = raw.replace(mention, '')
        return raw.strip()

    async def _can_use_ai(self, target, *, send_denial: bool = True) -> bool:
        if not await self.ctx.acl_check(target, 'ai.ask'):
            if send_denial:
                await self._deny(target, 'Bạn không có quyền dùng AI trong server này.')
            return False

        guild_id = getattr(target, 'guild_id', None) or getattr(getattr(target, 'guild', None), 'id', None)
        if not guild_id or await self.ctx.is_admin_actor(target):
            return True

        channel = getattr(target, 'channel', None)
        channel_id = getattr(channel, 'id', None)
        allowed = self.ctx.config_manager.get(int(guild_id), 'ai_enabled_channels') or []
        if not allowed:
            allowed = self.ctx.config_manager.get(int(guild_id), 'focus_channel_ids') or []
        allowed_ids = {int(ch_id) for ch_id in allowed if str(ch_id).isdigit()}

        # Empty config keeps backward compatibility. Once admins configure
        # ai_enabled_channels, AI is restricted to those study channels.
        if allowed_ids and channel_id not in allowed_ids:
            if send_denial:
                await self._deny(target, 'AI chỉ bật trong các kênh học đã cấu hình.')
            return False
        return True

    async def _deny(self, target, message: str) -> None:
        if isinstance(target, discord.Interaction):
            if target.response.is_done():
                await target.followup.send(message, ephemeral=True)
            else:
                await target.response.send_message(message, ephemeral=True)
        elif isinstance(target, discord.Message):
            await target.reply(message, mention_author=False)

    @app_commands.command(name='ask', description='Hỏi AI đa năng')
    @app_commands.describe(question='Câu hỏi của bạn')
    async def ask(self, interaction: discord.Interaction, question: str):
        if not await self._can_use_ai(interaction):
            return
        await interaction.response.defer(thinking=True)
        answer = await self.ctx.ask_ai(question)
        await interaction.followup.send(answer)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        mentioned_bot = self.bot.user is not None and self.bot.user in message.mentions
        if not mentioned_bot:
            return
        question = self._extract_question_from_mention(message)
        if not question:
            await message.reply(
                'Bạn hãy tag bot kèm câu hỏi nhé. Ví dụ: `@bot giải thích Markov chain`',
                mention_author=False,
            )
            return
        if not await self._can_use_ai(message):
            return
        async with message.channel.typing():
            answer = await self.ctx.ask_ai(question)
        await message.reply(answer, mention_author=False)


async def setup(bot: commands.Bot):
    await bot.add_cog(AIChatPlugin(bot))

