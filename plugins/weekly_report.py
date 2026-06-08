from __future__ import annotations

import logging

from discord.ext import commands

from weekly_report import setup_weekly_report

log = logging.getLogger(__name__)


async def setup(bot: commands.Bot):
    """Load the weekly report cog as an independent plugin."""
    if bot.cogs.get('WeeklyReport'):
        log.info('[Plugin:WeeklyReport] WeeklyReport already loaded.')
        return
    ctx = bot.study_context
    await setup_weekly_report(
        bot,
        ctx.load_data,
        ctx.save_data,
        ctx.badges,
        ctx.safe_send_dm,
        update_data_fn=ctx.update_data,
        class_thresholds=ctx.class_thresholds,
        class_names=ctx.class_names,
        guild_context_fn=ctx.guild_data_context,
        require_admin_fn=ctx.require_admin,
    )
    log.info('[Plugin:WeeklyReport] Loaded.')
