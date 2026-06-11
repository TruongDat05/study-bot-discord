from __future__ import annotations

import logging
import os
import re
import unicodedata

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


SHORT_TERM_MEMORY_LIMIT = max(1, min(200, _env_int('SHORT_TERM_MEMORY_LIMIT', 30) or 30))
SHORT_TERM_MEMORY_MAX_CHARS = max(200, min(2000, _env_int('SHORT_TERM_MEMORY_MAX_CHARS', 1800) or 1800))
SHORT_TERM_MEMORY_CONTEXT_CHARS = max(1000, min(20000, _env_int('SHORT_TERM_MEMORY_CONTEXT_CHARS', 6000) or 6000))
SHORT_TERM_MEMORY_SUMMARY_MIN = max(3, _env_int('SHORT_TERM_MEMORY_SUMMARY_MIN', 3) or 3)

SECRET_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r'-----BEGIN [A-Z ]*PRIVATE KEY-----',
        r'\b(?:sk|rk|pk|xox[baprs]|gh[pousr]|github_pat|hf)_[A-Za-z0-9_=-]{20,}\b',
        r'\bAIza[0-9A-Za-z\-_]{30,}\b',
        r'\bAKIA[0-9A-Z]{16}\b',
        r'\b[A-Za-z0-9_\-]{23,28}\.[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{27,}\b',
        r'\bmfa\.[A-Za-z0-9_\-]{20,}\b',
        r'\b(?:api[_-]?key|token|secret|password|passwd|pwd)\s*[:=]\s*[^\s`\'"]{8,}',
    )
)

SUMMARY_REGEXES = tuple(
    re.compile(pattern)
    for pattern in (
        r'\b(?:summarize|summarise|recap)\b.*\b(?:chat|conversation|thread|messages|above)\b',
        r'\b(?:chat|conversation|thread|messages|above)\b.*\b(?:summary|summarize|summarise|recap)\b',
        r'\btom tat\b.*\b(?:chat|doan|noi dung|tren|nay gio|cuoc tro chuyen)\b',
        r'\b(?:doan chat|cuoc tro chuyen|noi dung tren)\b.*\b(?:noi gi|dang noi gi|tom tat)\b',
        r'\b(?:nay gio|o tren|vua roi)\b.*\b(?:noi gi|ban gi|tom tat)\b',
    )
)


def _normalize_intent_text(text: str) -> str:
    normalized = unicodedata.normalize('NFD', str(text or '').lower())
    normalized = ''.join(ch for ch in normalized if unicodedata.category(ch) != 'Mn')
    normalized = normalized.replace('đ', 'd')
    return re.sub(r'\s+', ' ', normalized).strip()


class AIChatPlugin(commands.Cog, name='AIChatPlugin'):
    """AI slash command and mention replies.

    The heavy provider/fallback implementation remains in the shared bot
    context for now. This plugin owns the Discord entry points and applies
    per-guild channel config before any provider call is made.
    """

    memory = app_commands.Group(name='memory', description='Quản lý short-term AI memory')

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

    def _scope(self, target) -> tuple[int | None, int | None]:
        guild_id = getattr(target, 'guild_id', None) or getattr(getattr(target, 'guild', None), 'id', None)
        channel_id = getattr(target, 'channel_id', None) or getattr(getattr(target, 'channel', None), 'id', None)
        return (int(guild_id) if guild_id else None, int(channel_id) if channel_id else None)

    @staticmethod
    def _created_at(target) -> str | None:
        created_at = getattr(target, 'created_at', None)
        if not created_at:
            return None
        try:
            return created_at.isoformat(timespec='seconds')
        except TypeError:
            return created_at.isoformat()

    @staticmethod
    def _author_name(author) -> str:
        return str(getattr(author, 'display_name', None) or getattr(author, 'name', None) or 'Unknown')

    @staticmethod
    def _memory_content(content: str) -> str:
        return re.sub(r'\s+', ' ', str(content or '').strip())

    @staticmethod
    def _looks_sensitive(content: str) -> bool:
        return any(pattern.search(content or '') for pattern in SECRET_PATTERNS)

    def _is_memory_eligible(self, content: str) -> bool:
        content = self._memory_content(content)
        if not content:
            return False
        if len(content) > SHORT_TERM_MEMORY_MAX_CHARS:
            return False
        return not self._looks_sensitive(content)

    def _remember(
        self,
        *,
        guild_id: int | None,
        channel_id: int | None,
        content: str,
        user_id: int | None,
        author_name: str,
        author_is_bot: bool,
        source: str,
        message_id: int | None = None,
        created_at: str | None = None,
    ) -> bool:
        if not guild_id or not channel_id:
            return False
        content = self._memory_content(content)
        if not self._is_memory_eligible(content):
            return False
        try:
            return self.ctx.repository.add_chat_memory_message(
                guild_id=guild_id,
                channel_id=channel_id,
                message_id=message_id,
                user_id=user_id,
                author_name=author_name,
                author_is_bot=author_is_bot,
                source=source,
                content=content,
                created_at=created_at,
                limit=SHORT_TERM_MEMORY_LIMIT,
            )
        except Exception:
            log.warning('[AI memory] Failed to save chat memory message.', exc_info=True)
            return False

    def _remember_message(self, message: discord.Message, *, source: str = 'normal', content: str | None = None) -> bool:
        guild_id, channel_id = self._scope(message)
        return self._remember(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message.id,
            user_id=message.author.id,
            author_name=self._author_name(message.author),
            author_is_bot=message.author.bot,
            source=source,
            content=content if content is not None else getattr(message, 'clean_content', message.content),
            created_at=self._created_at(message),
        )

    def _remember_interaction(self, interaction: discord.Interaction, question: str) -> bool:
        guild_id, channel_id = self._scope(interaction)
        return self._remember(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=interaction.id,
            user_id=interaction.user.id,
            author_name=self._author_name(interaction.user),
            author_is_bot=False,
            source='ask',
            content=question,
            created_at=self._created_at(interaction),
        )

    def _remember_ai_reply(
        self,
        *,
        guild_id: int | None,
        channel_id: int | None,
        answer: str,
        message_id: int | None = None,
        created_at: str | None = None,
    ) -> bool:
        bot_user = self.bot.user
        return self._remember(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
            user_id=getattr(bot_user, 'id', None),
            author_name=self._author_name(bot_user),
            author_is_bot=True,
            source='ai_reply',
            content=answer,
            created_at=created_at,
        )

    def _load_history(self, guild_id: int | None, channel_id: int | None) -> list[dict]:
        if not guild_id or not channel_id:
            return []
        try:
            return self.ctx.repository.list_chat_memory(
                guild_id,
                channel_id,
                limit=SHORT_TERM_MEMORY_LIMIT,
            )
        except Exception:
            log.warning('[AI memory] Failed to load chat memory.', exc_info=True)
            return []

    @staticmethod
    def _history_label(row: dict) -> str:
        if row.get('author_is_bot') or row.get('source') == 'ai_reply':
            return 'AI bot'
        name = str(row.get('author_name') or 'Unknown')
        return re.sub(r'\s+', ' ', name).strip()[:80] or 'Unknown'

    def _format_history(self, history: list[dict]) -> str:
        lines: list[str] = []
        for row in history:
            content = self._memory_content(str(row.get('content') or ''))
            if not content:
                continue
            lines.append(f'{self._history_label(row)}: {content}')

        while lines and len('\n'.join(lines)) > SHORT_TERM_MEMORY_CONTEXT_CHARS:
            lines.pop(0)
        return '\n'.join(lines)

    def _is_summary_request(self, question: str) -> bool:
        normalized = _normalize_intent_text(question)
        return any(regex.search(normalized) for regex in SUMMARY_REGEXES)

    def _build_context_prompt(self, question: str, history: list[dict]) -> str:
        history_text = self._format_history(history)
        if not history_text:
            return question
        return (
            'Ngữ cảnh hội thoại gần đây bên dưới chỉ đến từ cùng server và cùng channel Discord. '
            'Nó có thể không đầy đủ. Dùng ngữ cảnh này nếu liên quan, bỏ qua nếu không liên quan. '
            'Không chép lại transcript thô hoặc liệt kê toàn bộ lịch sử chat; chỉ tóm tắt/diễn giải phần cần thiết.\n\n'
            f'Ngữ cảnh gần đây:\n{history_text}\n\n'
            f'Câu hỏi hiện tại: {question}'
        )

    def _build_summary_prompt(self, history: list[dict]) -> str:
        history_text = self._format_history(history)
        return (
            'Người dùng muốn tóm tắt đoạn chat gần đây trong cùng channel Discord. '
            'Chỉ dùng lịch sử dưới đây, không bịa thêm, không chép transcript thô. '
            'Trả lời bằng tiếng Việt trong đúng một tin nhắn Discord với 4 mục rõ ràng:\n'
            '1. Chủ đề chính\n'
            '2. Ý chính\n'
            '3. Quyết định/hành động\n'
            '4. Câu hỏi còn mở\n\n'
            f'Lịch sử gần đây:\n{history_text}'
        )

    async def _send_followup(self, interaction: discord.Interaction, content: str):
        try:
            return await interaction.followup.send(content, wait=True)
        except TypeError:
            await interaction.followup.send(content)
            return None

    async def _answer_with_memory(
        self,
        question: str,
        guild_id: int | None,
        channel_id: int | None,
        *,
        history: list[dict] | None = None,
    ) -> str:
        if history is None:
            history = self._load_history(guild_id, channel_id)
        if self._is_summary_request(question):
            if len(history) < SHORT_TERM_MEMORY_SUMMARY_MIN:
                return 'Chưa đủ lịch sử chat gần đây để tóm tắt. Hãy trò chuyện thêm một chút rồi thử lại nhé.'
            return await self.ctx.ask_ai(self._build_summary_prompt(history))
        return await self.ctx.ask_ai(self._build_context_prompt(question, history))

    async def _can_use_ai(self, target, *, send_denial: bool = True) -> bool:
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
        guild_id, channel_id = self._scope(interaction)
        await interaction.response.defer(thinking=True)
        answer = await self._answer_with_memory(question, guild_id, channel_id)
        sent = await self._send_followup(interaction, answer)
        self._remember_interaction(interaction, question)
        self._remember_ai_reply(
            guild_id=guild_id,
            channel_id=channel_id,
            answer=answer,
            message_id=getattr(sent, 'id', None),
            created_at=self._created_at(sent),
        )

    async def _memory_guard(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not interaction.channel_id:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        return True

    @memory.command(name='status', description='Xem thống kê memory của channel hiện tại')
    async def memory_status(self, interaction: discord.Interaction):
        if not await self._memory_guard(interaction):
            return
        stats = self.ctx.repository.chat_memory_stats(interaction.guild_id, interaction.channel_id)
        lines = [
            '**Short-term memory channel này**',
            f'Limit: `{SHORT_TERM_MEMORY_LIMIT}` tin nhắn',
            f'Đang lưu: `{stats["total"]}` tin nhắn',
            f'User/AI: `{stats["user_messages"]}` / `{stats["bot_messages"]}`',
            f'Người tham gia: `{stats["unique_users"]}`',
        ]
        if stats.get('oldest_at') and stats.get('newest_at'):
            lines.append(f'Cũ nhất: `{stats["oldest_at"]}`')
            lines.append(f'Mới nhất: `{stats["newest_at"]}`')
        await interaction.response.send_message('\n'.join(lines), ephemeral=True)

    @memory.command(name='clear_my', description='Xóa tin nhắn của bạn khỏi memory channel này')
    async def memory_clear_my(self, interaction: discord.Interaction):
        if not await self._memory_guard(interaction):
            return
        count = self.ctx.repository.clear_chat_memory_for_user(
            interaction.guild_id,
            interaction.channel_id,
            interaction.user.id,
        )
        await interaction.response.send_message(f'Đã xóa `{count}` tin nhắn của bạn khỏi memory channel này.', ephemeral=True)

    @memory.command(name='clear_channel', description='Admin: xóa memory của channel hiện tại')
    @app_commands.default_permissions(administrator=True)
    async def memory_clear_channel(self, interaction: discord.Interaction):
        if not await self._memory_guard(interaction):
            return
        if not await self.ctx.require_admin(interaction, 'memory.clear_channel'):
            return
        count = self.ctx.repository.clear_chat_memory_for_channel(
            interaction.guild_id,
            interaction.channel_id,
        )
        await interaction.response.send_message(f'Đã xóa `{count}` tin nhắn khỏi memory channel này.', ephemeral=True)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        guild_id, channel_id = self._scope(message)
        if not guild_id or not channel_id:
            return
        mentioned_bot = self.bot.user is not None and self.bot.user in message.mentions
        if not mentioned_bot:
            self._remember_message(message, source='normal')
            return
        question = self._extract_question_from_mention(message)
        if not question:
            await message.reply(
                'Bạn hãy tag bot kèm câu hỏi nhé. Ví dụ: `@bot giải thích Markov chain`',
                mention_author=False,
            )
            return
        history = self._load_history(guild_id, channel_id)
        self._remember_message(message, source='mention', content=question)
        if not await self._can_use_ai(message):
            return
        async with message.channel.typing():
            answer = await self._answer_with_memory(question, guild_id, channel_id, history=history)
        sent = await message.reply(answer, mention_author=False)
        self._remember_ai_reply(
            guild_id=guild_id,
            channel_id=channel_id,
            answer=answer,
            message_id=getattr(sent, 'id', None),
            created_at=self._created_at(sent),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AIChatPlugin(bot))
