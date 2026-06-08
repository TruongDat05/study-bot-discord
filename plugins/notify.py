from __future__ import annotations

from discord.ext import commands


class NotifyPlugin(commands.Cog, name='NotifyPlugin'):
    """Migration boundary for notification commands and voice notices."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot


async def setup(bot: commands.Bot):
    await bot.add_cog(NotifyPlugin(bot))

