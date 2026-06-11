from __future__ import annotations

from discord.ext import commands


class EconomyPlugin(commands.Cog, name='EconomyPlugin'):
    """Migration boundary for economy commands.

    Economy slash commands are still registered in ``bot.py`` to preserve the
    current command names and data flow. Sensitive economy admin actions use
    the shared bot admin check.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot


async def setup(bot: commands.Bot):
    await bot.add_cog(EconomyPlugin(bot))
