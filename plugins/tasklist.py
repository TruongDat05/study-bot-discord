from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands


TASK_REWARD_COINS = 500
TASK_DAILY_REWARD_CAP = 5

TASK_PRESETS = {
    'focus25': {
        'label': 'Focus 25 phút',
        'content': 'Học tập trung 25 phút và ghi lại mình đã làm gì',
        'reward': 800,
    },
    'review_notes': {
        'label': 'Ôn lại ghi chú',
        'content': 'Ôn lại ghi chú/bài cũ và tóm tắt 5 ý chính',
        'reward': 700,
    },
    'practice': {
        'label': 'Làm bài tập',
        'content': 'Hoàn thành ít nhất 5 bài tập hoặc 20 phút luyện đề',
        'reward': 1_000,
    },
    'flashcards': {
        'label': 'Flashcards',
        'content': 'Ôn 20 flashcards hoặc tự tạo 10 flashcards mới',
        'reward': 600,
    },
    'plan_tomorrow': {
        'label': 'Lên kế hoạch',
        'content': 'Lên kế hoạch học ngày mai với 3 việc cụ thể',
        'reward': 500,
    },
}

TASK_PRESET_CHOICES = [
    app_commands.Choice(name=f'{item["label"]} (+{item["reward"]:,} coins)', value=key)
    for key, item in TASK_PRESETS.items()
]


class TasklistCog(commands.Cog, name='TasklistCog'):
    tasks = app_commands.Group(name='tasks', description='To-do list học tập')

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        return True

    def _create_task(self, interaction: discord.Interaction, content: str, reward: int) -> int:
        return self.bot.study_context.repository.create_task(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            display_name=interaction.user.display_name,
            content=content,
            reward_coins=reward,
        )

    @tasks.command(name='add', description='Thêm task học tập')
    @app_commands.describe(content='Nội dung task')
    async def add_task(self, interaction: discord.Interaction, content: str):
        if not await self._guard(interaction):
            return
        content = content.strip()
        if not content:
            await interaction.response.send_message('Nội dung task không được trống.', ephemeral=True)
            return
        task_id = self._create_task(interaction, content, TASK_REWARD_COINS)
        await interaction.response.send_message(
            f'Đã thêm task `#{task_id}`. Hoàn thành nhận `+{TASK_REWARD_COINS:,} coins`.',
            ephemeral=True,
        )

    @tasks.command(name='ideas', description='Xem task gợi ý để kiếm thêm coins')
    async def task_ideas(self, interaction: discord.Interaction):
        if not await self._guard(interaction):
            return
        lines = [
            '**Task gợi ý kiếm coins**',
            f'Mỗi ngày nhận reward tối đa `{TASK_DAILY_REWARD_CAP}` task. Task tự tạo nhận `+{TASK_REWARD_COINS:,} coins`.',
            '',
        ]
        for key, item in TASK_PRESETS.items():
            lines.append(
                f'`{key}` · **{item["label"]}** `+{item["reward"]:,}`\n{item["content"]}'
            )
        lines.append('\nDùng `/tasks preset <task>` để thêm nhanh.')
        await interaction.response.send_message('\n'.join(lines)[:1900], ephemeral=True)

    @tasks.command(name='preset', description='Thêm nhanh một task học tập gợi ý')
    @app_commands.describe(task='Task gợi ý')
    @app_commands.choices(task=TASK_PRESET_CHOICES)
    async def preset_task(self, interaction: discord.Interaction, task: app_commands.Choice[str]):
        if not await self._guard(interaction):
            return
        preset = TASK_PRESETS.get(task.value)
        if not preset:
            await interaction.response.send_message('Không tìm thấy task gợi ý đó.', ephemeral=True)
            return
        task_id = self._create_task(interaction, preset['content'], int(preset['reward']))
        await interaction.response.send_message(
            f'Đã thêm task `#{task_id}`: **{preset["label"]}**. '
            f'Hoàn thành nhận `+{int(preset["reward"]):,} coins`.',
            ephemeral=True,
        )

    @tasks.command(name='list', description='Xem task của bạn')
    @app_commands.describe(show_done='Hiện cả task đã hoàn thành')
    async def list_tasks(self, interaction: discord.Interaction, show_done: bool = False):
        if not await self._guard(interaction):
            return
        rows = self.bot.study_context.repository.list_tasks(
            interaction.guild_id,
            interaction.user.id,
            include_completed=show_done,
            limit=25,
        )
        if not rows:
            await interaction.response.send_message('Bạn chưa có task nào.', ephemeral=True)
            return
        lines = ['**Tasks của bạn**']
        for task in rows:
            marker = 'x' if task.get('completed') else ' '
            reward = int(task.get('reward_coins') or 0)
            suffix = f' (+{reward:,} coins)' if reward else ''
            lines.append(f'`#{task["id"]}` [{marker}] {task["content"]}{suffix}')
        await interaction.response.send_message('\n'.join(lines)[:1900], ephemeral=True)

    @tasks.command(name='done', description='Đánh dấu hoàn thành task')
    @app_commands.describe(task_id='ID task trong /tasks list')
    async def done_task(self, interaction: discord.Interaction, task_id: int):
        if not await self._guard(interaction):
            return
        result = self.bot.study_context.repository.complete_task(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            display_name=interaction.user.display_name,
            task_id=task_id,
            reward_coins=TASK_REWARD_COINS,
            daily_reward_cap=TASK_DAILY_REWARD_CAP,
        )
        await interaction.response.send_message(result['message'], ephemeral=True)

    @tasks.command(name='remove', description='Xóa một task')
    @app_commands.describe(task_id='ID task trong /tasks list')
    async def remove_task(self, interaction: discord.Interaction, task_id: int):
        if not await self._guard(interaction):
            return
        ok = self.bot.study_context.repository.delete_task(
            interaction.guild_id,
            interaction.user.id,
            task_id,
        )
        await interaction.response.send_message('Đã xóa task.' if ok else 'Không tìm thấy task.', ephemeral=True)

    @tasks.command(name='clear', description='Xóa task của bạn')
    @app_commands.describe(completed_only='Chỉ xóa task đã hoàn thành')
    async def clear_tasks(self, interaction: discord.Interaction, completed_only: bool = False):
        if not await self._guard(interaction):
            return
        count = self.bot.study_context.repository.clear_tasks(
            interaction.guild_id,
            interaction.user.id,
            completed_only=completed_only,
        )
        await interaction.response.send_message(f'Đã xóa `{count}` task.', ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(TasklistCog(bot))
