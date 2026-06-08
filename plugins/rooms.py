from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks as discord_tasks


DURATION_RE = re.compile(r'^(\d{1,4})\s*([mh])$', re.IGNORECASE)


class RoomsCog(commands.Cog, name='RoomsCog'):
    room = app_commands.Group(name='room', description='Phòng học riêng')

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        if not self.room_cleanup_loop.is_running():
            self.room_cleanup_loop.start()

    async def cog_unload(self):
        self.room_cleanup_loop.cancel()

    async def _guard(self, interaction: discord.Interaction, action: str = 'room.use') -> bool:
        if not interaction.guild:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        if not await self.bot.study_context.acl_check(interaction, action):
            await interaction.response.send_message('ACL đang chặn bạn dùng room.', ephemeral=True)
            return False
        return True

    async def _can_control(self, interaction: discord.Interaction, room: dict, action: str) -> bool:
        if not room:
            await interaction.response.send_message('Bạn cần ở trong phòng học riêng do bot tạo.', ephemeral=True)
            return False
        if int(room.get('owner_id') or 0) == interaction.user.id:
            return True
        if await self.bot.study_context.is_admin_actor(interaction):
            return True
        await interaction.response.send_message('Bạn không phải chủ phòng.', ephemeral=True)
        return False

    def _configured_category(self, guild: discord.Guild) -> discord.CategoryChannel | None:
        category_id = self.bot.study_context.config_manager.get(guild.id, 'temp_room_category_id')
        category = guild.get_channel(int(category_id)) if category_id else None
        return category if isinstance(category, discord.CategoryChannel) else None

    async def _current_room(self, interaction: discord.Interaction) -> tuple[discord.VoiceChannel | None, dict]:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        channel = member.voice.channel if member and member.voice else None
        if not isinstance(channel, discord.VoiceChannel):
            return None, {}
        room = self.bot.study_context.repository.get_private_room(interaction.guild_id, channel.id)
        return channel, room

    @staticmethod
    def _parse_duration(raw: str) -> int:
        match = DURATION_RE.match(str(raw or '').strip())
        if not match:
            raise ValueError('Dùng dạng `30m`, `90m`, hoặc `2h`.')
        amount = int(match.group(1))
        unit = match.group(2).lower()
        minutes = amount if unit == 'm' else amount * 60
        if minutes < 15 or minutes > 24 * 60:
            raise ValueError('Duration phải từ 15 phút đến 24 giờ.')
        return minutes

    async def _create_channel(
        self,
        interaction: discord.Interaction,
        *,
        name: str | None,
        user_limit: int | None,
        expires_at: str | None = None,
        rent_paid_coins: int = 0,
    ) -> discord.VoiceChannel:
        guild = interaction.guild
        assert guild is not None
        category = self._configured_category(guild)
        owner = interaction.user
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=True, connect=True),
            owner: discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                manage_channels=True,
                move_members=True,
            ),
        }
        channel = await guild.create_voice_channel(
            name=(name or f'Phòng của {owner.display_name}')[:100],
            category=category,
            overwrites=overwrites,
            user_limit=int(user_limit or 0),
            reason='Private study room',
        )
        try:
            self.bot.study_context.repository.create_private_room(
                guild_id=guild.id,
                channel_id=channel.id,
                owner_id=owner.id,
                owner_name=owner.display_name,
                expires_at=expires_at,
                rent_paid_coins=rent_paid_coins,
            )
        except Exception:
            await channel.delete(reason='Private room DB create failed')
            raise

        member = owner if isinstance(owner, discord.Member) else None
        if member and member.voice and member.voice.channel:
            try:
                await member.move_to(channel, reason='Move owner to private study room')
            except discord.HTTPException:
                pass
        return channel

    @room.command(name='create', description='Tạo phòng học riêng')
    @app_commands.describe(name='Tên phòng tùy chọn', user_limit='Giới hạn người vào phòng')
    async def create(
        self,
        interaction: discord.Interaction,
        name: str | None = None,
        user_limit: app_commands.Range[int, 0, 99] = 0,
    ):
        if not await self._guard(interaction, 'room.create'):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            channel = await self._create_channel(interaction, name=name, user_limit=int(user_limit or 0))
        except discord.Forbidden:
            await interaction.followup.send('Bot thiếu quyền Manage Channels/Move Members.', ephemeral=True)
            return
        await interaction.followup.send(f'Đã tạo phòng: {channel.mention}', ephemeral=True)

    @room.command(name='rent', description='Thuê phòng học riêng bằng coins')
    @app_commands.describe(duration='Ví dụ: 30m, 2h', name='Tên phòng tùy chọn')
    async def rent(self, interaction: discord.Interaction, duration: str, name: str | None = None):
        if not await self._guard(interaction, 'room.rent'):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            minutes = self._parse_duration(duration)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return
        rate = int(self.bot.study_context.config_manager.get(interaction.guild_id, 'room_rent_coin_per_minute', 2) or 2)
        cost = max(0, rate * minutes)
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat(timespec='seconds')
        try:
            channel = await self._create_channel(
                interaction,
                name=name,
                user_limit=0,
                expires_at=expires_at,
                rent_paid_coins=cost,
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return
        except discord.Forbidden:
            await interaction.followup.send('Bot thiếu quyền Manage Channels/Move Members.', ephemeral=True)
            return
        await interaction.followup.send(
            f'Đã thuê {channel.mention} trong `{minutes}` phút với giá `{cost:,}` coins.',
            ephemeral=True,
        )

    @room.command(name='invite', description='Mời thành viên vào phòng hiện tại')
    async def invite(self, interaction: discord.Interaction, member: discord.Member):
        if not await self._guard(interaction, 'room.invite'):
            return
        channel, room = await self._current_room(interaction)
        if not await self._can_control(interaction, room, 'room.invite'):
            return
        await channel.set_permissions(member, view_channel=True, connect=True)
        await interaction.response.send_message(f'Đã mời **{member.display_name}**.', ephemeral=True)

    @room.command(name='kick', description='Đưa thành viên khỏi phòng hiện tại')
    async def kick(self, interaction: discord.Interaction, member: discord.Member):
        if not await self._guard(interaction, 'room.kick'):
            return
        channel, room = await self._current_room(interaction)
        if not await self._can_control(interaction, room, 'room.kick'):
            return
        if member.voice and member.voice.channel and member.voice.channel.id == channel.id:
            await member.move_to(None, reason='Kicked from private study room')
        await channel.set_permissions(member, connect=False)
        await interaction.response.send_message(f'Đã kick **{member.display_name}** khỏi phòng.', ephemeral=True)

    @room.command(name='lock', description='Khóa phòng hiện tại')
    async def lock(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'room.lock'):
            return
        channel, room = await self._current_room(interaction)
        if not await self._can_control(interaction, room, 'room.lock'):
            return
        await channel.set_permissions(interaction.guild.default_role, connect=False)
        self.bot.study_context.repository.set_private_room_locked(interaction.guild_id, channel.id, True)
        await interaction.response.send_message('Đã khóa phòng.', ephemeral=True)

    @room.command(name='unlock', description='Mở khóa phòng hiện tại')
    async def unlock(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'room.unlock'):
            return
        channel, room = await self._current_room(interaction)
        if not await self._can_control(interaction, room, 'room.unlock'):
            return
        await channel.set_permissions(interaction.guild.default_role, connect=True)
        self.bot.study_context.repository.set_private_room_locked(interaction.guild_id, channel.id, False)
        await interaction.response.send_message('Đã mở khóa phòng.', ephemeral=True)

    @room.command(name='delete', description='Xóa phòng hiện tại')
    async def delete(self, interaction: discord.Interaction):
        if not await self._guard(interaction, 'room.delete'):
            return
        channel, room = await self._current_room(interaction)
        if not await self._can_control(interaction, room, 'room.delete'):
            return
        await interaction.response.defer(ephemeral=True)
        self.bot.study_context.repository.delete_private_room(interaction.guild_id, channel.id)
        await channel.delete(reason='Private study room deleted by owner/admin')
        await interaction.followup.send('Đã xóa phòng.', ephemeral=True)

    @discord_tasks.loop(minutes=1)
    async def room_cleanup_loop(self):
        now = datetime.now(timezone.utc)
        for room in self.bot.study_context.repository.list_active_private_rooms():
            guild = self.bot.get_guild(int(room['guild_id']))
            channel = guild.get_channel(int(room['channel_id'])) if guild else None
            if not isinstance(channel, discord.VoiceChannel):
                self.bot.study_context.repository.delete_private_room(room['guild_id'], room['channel_id'])
                continue
            expires_at = room.get('expires_at')
            expired = False
            if expires_at:
                try:
                    expired = datetime.fromisoformat(expires_at).astimezone(timezone.utc) <= now
                except ValueError:
                    expired = False
            if expired or not channel.members:
                self.bot.study_context.repository.delete_private_room(room['guild_id'], room['channel_id'])
                try:
                    await channel.delete(reason='Private study room expired or empty')
                except discord.HTTPException:
                    pass

    @room_cleanup_loop.before_loop
    async def before_room_cleanup_loop(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(RoomsCog(bot))
