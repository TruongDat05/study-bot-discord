from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

import discord
from discord import app_commands
from discord.ext import commands

from services.database import DatabaseService

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _action_matches(rule_action: str, action: str) -> bool:
    rule_action = str(rule_action or '*').strip().lower()
    action = str(action or '').strip().lower()
    return rule_action == '*' or rule_action == action or (
        rule_action.endswith('.*') and action.startswith(rule_action[:-1])
    )


class ACLManager:
    """Access-control rules for commands and feature actions.

    The default is allow so existing commands keep working. Admins can then add
    explicit deny/allow rules by user, role, channel, category, or guild default.
    Evaluation order matches the operational policy documented in README:
    owner/admin bypass, user rules, role rules, channel/category rules, then
    guild default rules.
    """

    def __init__(
        self,
        database: DatabaseService,
        *,
        bot: commands.Bot,
        config_manager=None,
    ):
        self.database = database
        self.bot = bot
        self.config_manager = config_manager

    def initialize(self) -> None:
        self.database.initialize()

    async def is_owner(self, user: discord.abc.User) -> bool:
        try:
            return await self.bot.is_owner(user)
        except Exception:
            return False

    async def is_admin_actor(self, interaction_or_message: Any) -> bool:
        user = getattr(interaction_or_message, 'user', None) or getattr(interaction_or_message, 'author', None)
        if user is None:
            return False
        if await self.is_owner(user):
            return True
        if isinstance(user, discord.Member):
            perms = getattr(user, 'guild_permissions', None)
            if perms and (perms.administrator or perms.manage_guild):
                return True
            guild_id = getattr(interaction_or_message, 'guild_id', None) or getattr(getattr(interaction_or_message, 'guild', None), 'id', None)
            if self.config_manager and guild_id:
                admin_role_id = self.config_manager.get(int(guild_id), 'admin_role_id')
                return bool(admin_role_id and any(role.id == int(admin_role_id) for role in user.roles))
        return False

    def add_rule(
        self,
        *,
        guild_id: int,
        action: str,
        effect: str,
        user_id: int | None = None,
        role_id: int | None = None,
        channel_id: int | None = None,
        category_id: int | None = None,
        priority: int = 100,
        enabled: bool = True,
        created_by: int | None = None,
    ) -> int:
        effect = str(effect).lower().strip()
        if effect not in ('allow', 'deny'):
            raise ValueError('ACL effect must be allow or deny.')
        action = str(action or '*').strip().lower()
        if not action:
            raise ValueError('ACL action is required.')
        self.initialize()
        now = _now()
        with self.database.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO acl_rules (
                    guild_id, action, effect, user_id, role_id, channel_id,
                    category_id, priority, enabled, created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(guild_id), action, effect, user_id, role_id, channel_id,
                    category_id, int(priority), 1 if enabled else 0, created_by, now, now,
                ),
            )
            return int(cur.lastrowid)

    def remove_rule(self, guild_id: int, rule_id: int) -> bool:
        self.initialize()
        with self.database.transaction() as conn:
            cur = conn.execute(
                'DELETE FROM acl_rules WHERE guild_id = ? AND id = ?',
                (int(guild_id), int(rule_id)),
            )
            return cur.rowcount > 0

    def list_rules(self, guild_id: int) -> list[dict[str, Any]]:
        self.initialize()
        with self.database.read_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM acl_rules
                WHERE guild_id = ?
                ORDER BY enabled DESC, action ASC, priority DESC, id ASC
                """,
                (int(guild_id),),
            ).fetchall()
        return [dict(row) for row in rows]

    def _matching_rules(self, guild_id: int, action: str) -> list[dict[str, Any]]:
        rules = self.list_rules(guild_id)
        return [rule for rule in rules if rule.get('enabled') and _action_matches(rule.get('action'), action)]

    @staticmethod
    def _pick_effect(rules: list[dict[str, Any]]) -> str | None:
        if not rules:
            return None
        rules = sorted(
            rules,
            key=lambda rule: (int(rule.get('priority') or 0), 1 if rule.get('effect') == 'deny' else 0, -int(rule.get('id') or 0)),
            reverse=True,
        )
        return str(rules[0]['effect'])

    async def check(self, interaction_or_message: Any, action_name: str) -> bool:
        guild = getattr(interaction_or_message, 'guild', None)
        guild_id = getattr(interaction_or_message, 'guild_id', None) or getattr(guild, 'id', None)
        if not guild_id:
            return True

        user = getattr(interaction_or_message, 'user', None) or getattr(interaction_or_message, 'author', None)
        if user is None:
            return False
        if await self.is_owner(user):
            return True
        if isinstance(user, discord.Member) and user.guild_permissions.administrator:
            return True

        channel = getattr(interaction_or_message, 'channel', None)
        channel_id = getattr(channel, 'id', None)
        category_id = getattr(getattr(channel, 'category', None), 'id', None)
        role_ids = {role.id for role in getattr(user, 'roles', [])}

        rules = self._matching_rules(int(guild_id), action_name)

        user_effect = self._pick_effect([rule for rule in rules if rule.get('user_id') == getattr(user, 'id', None)])
        if user_effect:
            return user_effect == 'allow'

        role_effect = self._pick_effect([rule for rule in rules if rule.get('role_id') in role_ids])
        if role_effect:
            return role_effect == 'allow'

        channel_effect = self._pick_effect([
            rule for rule in rules
            if (channel_id and rule.get('channel_id') == channel_id)
            or (category_id and rule.get('category_id') == category_id)
        ])
        if channel_effect:
            return channel_effect == 'allow'

        guild_effect = self._pick_effect([
            rule for rule in rules
            if not rule.get('user_id')
            and not rule.get('role_id')
            and not rule.get('channel_id')
            and not rule.get('category_id')
        ])
        if guild_effect:
            return guild_effect == 'allow'
        return True


class ACLCog(commands.Cog, name='ACLCore'):
    acl = app_commands.Group(name='acl', description='Quản lý quyền ACL của bot')

    def __init__(
        self,
        manager: ACLManager,
        *,
        require_admin: Callable[[discord.Interaction, str], Any],
    ):
        self.manager = manager
        self.require_admin = require_admin

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        return await self.require_admin(interaction, 'acl.manage')

    async def _add_subject_rule(
        self,
        interaction: discord.Interaction,
        *,
        action: str,
        effect: str,
        user: discord.Member | discord.User | None = None,
        role: discord.Role | None = None,
        channel: discord.abc.GuildChannel | None = None,
    ):
        if not await self._guard(interaction):
            return
        category_id = channel.id if isinstance(channel, discord.CategoryChannel) else None
        channel_id = None if isinstance(channel, discord.CategoryChannel) else getattr(channel, 'id', None)
        rule_id = self.manager.add_rule(
            guild_id=interaction.guild_id,
            action=action,
            effect=effect,
            user_id=getattr(user, 'id', None),
            role_id=getattr(role, 'id', None),
            channel_id=channel_id,
            category_id=category_id,
            created_by=interaction.user.id,
        )
        await interaction.response.send_message(f'Đã thêm ACL rule `#{rule_id}`: `{effect}` `{action}`.', ephemeral=True)

    @acl.command(name='list', description='Liệt kê ACL rules')
    async def acl_list(self, interaction: discord.Interaction):
        if not await self._guard(interaction):
            return
        rules = self.manager.list_rules(interaction.guild_id)
        if not rules:
            await interaction.response.send_message('Chưa có ACL rule nào. Mặc định là allow.', ephemeral=True)
            return
        lines = ['**ACL rules**']
        for rule in rules[:40]:
            subject = (
                f'user `{rule["user_id"]}`' if rule.get('user_id') else
                f'role `{rule["role_id"]}`' if rule.get('role_id') else
                f'channel `{rule["channel_id"]}`' if rule.get('channel_id') else
                f'category `{rule["category_id"]}`' if rule.get('category_id') else
                'guild default'
            )
            state = 'on' if rule.get('enabled') else 'off'
            lines.append(f'`#{rule["id"]}` `{rule["effect"]}` `{rule["action"]}` for {subject} p={rule["priority"]} {state}')
        await interaction.response.send_message('\n'.join(lines)[:1900], ephemeral=True)

    @acl.command(name='allow_user', description='Cho phép user dùng action')
    async def allow_user(self, interaction: discord.Interaction, action: str, user: discord.Member):
        await self._add_subject_rule(interaction, action=action, effect='allow', user=user)

    @acl.command(name='deny_user', description='Chặn user dùng action')
    async def deny_user(self, interaction: discord.Interaction, action: str, user: discord.Member):
        await self._add_subject_rule(interaction, action=action, effect='deny', user=user)

    @acl.command(name='allow_role', description='Cho phép role dùng action')
    async def allow_role(self, interaction: discord.Interaction, action: str, role: discord.Role):
        await self._add_subject_rule(interaction, action=action, effect='allow', role=role)

    @acl.command(name='deny_role', description='Chặn role dùng action')
    async def deny_role(self, interaction: discord.Interaction, action: str, role: discord.Role):
        await self._add_subject_rule(interaction, action=action, effect='deny', role=role)

    @acl.command(name='allow_channel', description='Cho phép channel/category dùng action')
    async def allow_channel(self, interaction: discord.Interaction, action: str, channel: discord.abc.GuildChannel):
        await self._add_subject_rule(interaction, action=action, effect='allow', channel=channel)

    @acl.command(name='deny_channel', description='Chặn channel/category dùng action')
    async def deny_channel(self, interaction: discord.Interaction, action: str, channel: discord.abc.GuildChannel):
        await self._add_subject_rule(interaction, action=action, effect='deny', channel=channel)

    @acl.command(name='allow_guild', description='Cho phép action mặc định cho server')
    async def allow_guild(self, interaction: discord.Interaction, action: str):
        await self._add_subject_rule(interaction, action=action, effect='allow')

    @acl.command(name='deny_guild', description='Chặn action mặc định cho server')
    async def deny_guild(self, interaction: discord.Interaction, action: str):
        await self._add_subject_rule(interaction, action=action, effect='deny')

    @acl.command(name='remove', description='Xóa ACL rule')
    async def acl_remove(self, interaction: discord.Interaction, rule_id: int):
        if not await self._guard(interaction):
            return
        removed = self.manager.remove_rule(interaction.guild_id, rule_id)
        await interaction.response.send_message('Đã xóa rule.' if removed else 'Không tìm thấy rule.', ephemeral=True)

    @acl.command(name='test', description='Test ACL cho action và user')
    async def acl_test(self, interaction: discord.Interaction, action: str, user: discord.Member | None = None):
        if not await self._guard(interaction):
            return
        target = user or interaction.user
        fake = type('ACLProbe', (), {
            'guild': interaction.guild,
            'guild_id': interaction.guild_id,
            'channel': interaction.channel,
            'user': target,
        })()
        allowed = await self.manager.check(fake, action)
        await interaction.response.send_message(
            f'ACL `{action}` cho **{getattr(target, "display_name", target.name)}**: `{"allow" if allowed else "deny"}`',
            ephemeral=True,
        )
