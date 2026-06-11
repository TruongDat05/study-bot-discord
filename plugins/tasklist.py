from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands


TASK_REWARD_COINS = 5
TASK_DAILY_REWARD_CAP = 10


class TasklistCog(commands.Cog, name='TasklistCog'):
    tasks = app_commands.Group(name='tasks', description='To-do list học tập')

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        return True

    @tasks.command(name='add', description='Thêm task học tập')
    @app_commands.describe(content='Nội dung task')
    async def add_task(self, interaction: discord.Interaction, content: str):
        if not await self._guard(interaction):
            return
        content = content.strip()
        if not content:
            await interaction.response.send_message('Nội dung task không được trống.', ephemeral=True)
            return
        task_id = self.bot.study_context.repository.create_task(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            display_name=interaction.user.display_name,
            content=content,
        )
        await interaction.response.send_message(f'Đã thêm task `#{task_id}`.', ephemeral=True)

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
