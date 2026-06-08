from __future__ import annotations

from discord.ext import commands


class LeaderboardPlugin(commands.Cog, name='LeaderboardPlugin'):
    """Migration boundary for leaderboard, chart, rank, badge, and card commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot


async def setup(bot: commands.Bot):
    await bot.add_cog(LeaderboardPlugin(bot))

