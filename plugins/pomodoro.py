from __future__ import annotations

import logging

from discord.ext import commands

from pomodoro import create_pomodoro_cog

log = logging.getLogger(__name__)


async def setup(bot: commands.Bot):
    """Load the Pomodoro cog as an independent plugin."""
    if bot.cogs.get('PomodoroCog'):
        log.info('[Plugin:Pomodoro] PomodoroCog already loaded.')
        return
    ctx = bot.study_context
    cog = create_pomodoro_cog(
        bot,
        ctx.add_study_time,
        ctx.safe_send_dm,
        ctx.format_time,
        load_data_fn=ctx.load_data,
        save_data_fn=ctx.save_data,
        add_xp_fn=ctx.add_xp_direct,
        update_data_fn=ctx.update_data,
        progress_sync_fn=ctx.sync_member_progress,
    )
    await bot.add_cog(cog)
    log.info('[Plugin:Pomodoro] Loaded.')

