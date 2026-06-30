from __future__ import annotations

from discord.ext import commands


class StudyVoicePlugin(commands.Cog, name='StudyVoicePlugin'):
    """Migration boundary for study voice tracking.

    The existing voice-state implementation still lives in ``bot.py`` because it
    shares runtime state with temporary rooms, role sync, and dashboard updates.
    Keeping this plugin loadable reserves a clean home for
    the next incremental extraction.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot


async def setup(bot: commands.Bot):
    await bot.add_cog(StudyVoicePlugin(bot))
