from __future__ import annotations

from discord.ext import commands


class LoansPlugin(commands.Cog, name='LoansPlugin'):
    """Migration boundary for loan commands and loan notifications."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot


async def setup(bot: commands.Bot):
    await bot.add_cog(LoansPlugin(bot))

