from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands

try:
    from PIL import Image, ImageDraw, ImageFont
except ModuleNotFoundError:
    Image = ImageDraw = ImageFont = None


log = logging.getLogger(__name__)

INITIAL_BALANCE = 100_000
DAILY_REWARD_MIN = 1_000
DAILY_REWARD_MAX = 5_000
DAILY_COOLDOWN = timedelta(hours=24)
MIN_BET = 1_000
MAX_BET = 1_000_000
BLACKJACK_COOLDOWN_SECONDS = 5
BLACKJACK_PANEL_SECONDS = 600
TAIXIU_COOLDOWN_SECONDS = 3
TAIXIU_BETTING_SECONDS = 30
TAIXIU_LOCKED_SECONDS = 5
TAIXIU_RESULT_SECONDS = 8
TAIXIU_PRIVATE_DELETE_SECONDS = 30
TAIXIU_BOARD_SIZE = (1280, 720)
TAIXIU_RENDER_IMAGE = True
TAIXIU_CHIP_AMOUNTS = (1_000, 10_000, 50_000, 100_000, 500_000)
DICE_PANEL_SECONDS = 60
HILO_PANEL_SECONDS = 60
SLOT_PANEL_SECONDS = 60
HILO_MULTIPLIERS = (1.2, 1.5, 2.0, 3.0, 5.0)

BLUE = 0x3498DB
GREEN = 0x2ECC71
RED = 0xE74C3C
GREY = 0x95A5A6
GOLD = 0xF1C40F

SUITS = ('♠', '♥', '♦', '♣')
RANKS = ('A', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K')
SUIT_CODES = {'♠': 'S', '♥': 'H', '♦': 'D', '♣': 'C'}
CARD_ASSET_DIR = Path(__file__).resolve().parent.parent / 'assets' / 'cards'
CARD_BACK_NAME = 'back.png'
SLOT_ASSET_DIR = Path(__file__).resolve().parent.parent / 'assets' / 'slot'
ALL_GAME_KEY = 'all'
BLACKJACK_GAME_KEY = 'blackjack'
TAIXIU_GAME_KEY = 'taixiu'
SLOT_GAME_KEY = 'slot'
DICE_GAME_KEY = 'dice'
HILO_GAME_KEY = 'hilo'
CASINO_GAME_KEY = 'casino'
KNOWN_GAME_KEYS = {
    ALL_GAME_KEY,
    BLACKJACK_GAME_KEY,
    TAIXIU_GAME_KEY,
    SLOT_GAME_KEY,
    DICE_GAME_KEY,
    HILO_GAME_KEY,
    CASINO_GAME_KEY,
}
BLACKJACK_REACTIONS = {
    '🃏': 'hit',
    '✋': 'stand',
    '2️⃣': 'double',
    '🏳️': 'surrender',
}
SLOT_WEIGHTED_SYMBOLS = (
    ('🍒', 28),
    ('🍋', 24),
    ('🍊', 22),
    ('🍇', 18),
    ('🔔', 8),
    ('💎', 4),
    ('7️⃣', 2),
)
SLOT_FRUIT_SYMBOLS = {'🍋', '🍊', '🍇'}
SLOT_TRIPLE_MULTIPLIERS = {
    '🍒': 2,
    '🔔': 5,
    '💎': 10,
    '7️⃣': 25,
}
SLOT_SYMBOL_ASSETS = {
    '🍒': 'cherry.png',
    '🍋': 'lemon.png',
    '🍊': 'orange.png',
    '🍇': 'grape.png',
    '🔔': 'bell.png',
    '💎': 'diamond.png',
    '7️⃣': 'seven.png',
}
SLOT_BOARD_SIZE = (980, 560)
GAME_BUTTON_COOLDOWN_SECONDS = 0.35

_FONT_CACHE: dict[tuple[int, bool, bool], object] = {}
_ASSET_IMAGE_CACHE: dict[tuple[str, tuple[int, int]], object] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def money(amount: int | float) -> str:
    try:
        amount_i = int(round(float(amount)))
    except (TypeError, ValueError):
        amount_i = 0
    return f'{amount_i:,} coins'


def compact_money(amount: int | float) -> str:
    try:
        return f'{int(round(float(amount))):,}'
    except (TypeError, ValueError):
        return '0'


def chip_label(amount: int) -> str:
    if amount >= 1_000_000:
        value = amount / 1_000_000
        return f'{value:g}M'
    if amount >= 1_000:
        value = amount / 1_000
        return f'{value:g}K'
    return str(int(amount))


def casino_font(size: int, *, bold: bool = False, serif: bool = False):
    if ImageFont is None:
        return None
    cache_key = (int(size), bool(bold), bool(serif))
    cached = _FONT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    names = []
    if serif:
        names.extend([
            '/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf' if bold else '/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf',
            '/usr/share/fonts/truetype/liberation2/LiberationSerif-Bold.ttf' if bold else '/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf',
        ])
    names.extend([
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf' if bold else '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf' if bold else '/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf',
    ])
    for name in names:
        try:
            font = ImageFont.truetype(name, size)
            _FONT_CACHE[cache_key] = font
            return font
        except OSError:
            continue
    font = ImageFont.load_default()
    _FONT_CACHE[cache_key] = font
    return font


def cached_resized_asset(path: Path, size: tuple[int, int]):
    if Image is None:
        return None
    cache_key = (str(path), (int(size[0]), int(size[1])))
    cached = _ASSET_IMAGE_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()
    if not path.exists():
        return None
    with Image.open(path) as source:
        image = source.convert('RGBA').resize(size, Image.Resampling.LANCZOS)
    _ASSET_IMAGE_CACHE[cache_key] = image
    return image.copy()


def image_state_token(*parts: object, length: int = 12) -> str:
    raw = repr(parts).encode('utf-8', 'replace')
    return hashlib.blake2s(raw, digest_size=8).hexdigest()[:length]


def text_size(draw, text: str, font) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), str(text), font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def draw_centered_text(draw, box: tuple[int, int, int, int], text: str, font, fill, *, stroke_width: int = 0, stroke_fill=None):
    x1, y1, x2, y2 = box
    width, height = text_size(draw, text, font)
    draw.text(
        (x1 + (x2 - x1 - width) / 2, y1 + (y2 - y1 - height) / 2),
        str(text),
        font=font,
        fill=fill,
        stroke_width=stroke_width,
        stroke_fill=stroke_fill,
    )


def draw_dice(draw, box: tuple[int, int, int, int], value: int | None, *, angle: int = 0):
    x1, y1, x2, y2 = box
    shadow = (x1 + 8, y1 + 10, x2 + 8, y2 + 10)
    draw.rounded_rectangle(shadow, radius=18, fill=(0, 0, 0, 115))
    draw.rounded_rectangle(box, radius=18, fill=(238, 235, 225), outline=(255, 252, 231), width=3)
    draw.arc((x1 + 5, y1 + 5, x2 - 5, y2 - 5), 205, 320, fill=(160, 145, 122), width=2)
    if value is None:
        font = casino_font(44, bold=True)
        draw_centered_text(draw, box, '?', font, (120, 36, 36), stroke_width=1, stroke_fill=(255, 255, 255))
        return
    size = min(x2 - x1, y2 - y1)
    r = max(5, size // 12)
    cx, cy = (x1 + x2) // 2, (y1 + y2) // 2
    left, right = x1 + size // 4, x2 - size // 4
    top, bottom = y1 + size // 4, y2 - size // 4
    positions = {
        1: [(cx, cy)],
        2: [(left, top), (right, bottom)],
        3: [(left, top), (cx, cy), (right, bottom)],
        4: [(left, top), (right, top), (left, bottom), (right, bottom)],
        5: [(left, top), (right, top), (cx, cy), (left, bottom), (right, bottom)],
        6: [(left, top), (right, top), (left, cy), (right, cy), (left, bottom), (right, bottom)],
    }
    for i, (px, py) in enumerate(positions.get(int(value), [])):
        color = (181, 12, 16) if i < min(value, 4) and value in {1, 4} else (25, 25, 25)
        draw.ellipse((px - r, py - r, px + r, py + r), fill=color)


def display_name(user: discord.abc.User) -> str:
    return str(getattr(user, 'display_name', None) or getattr(user, 'global_name', None) or user.name)


def new_tx_id() -> str:
    return f'tx_{uuid.uuid4().hex}'


def ensure_wallet(data: dict, user_id: int, name: str) -> dict:
    uid = str(user_id)
    if uid not in data or not isinstance(data.get(uid), dict):
        data[uid] = {
            'name': name,
            'balance': INITIAL_BALANCE,
            'total_earned': 0,
            'transactions': [],
        }
    account = data[uid]
    account['name'] = name or account.get('name') or f'User {uid}'
    account.setdefault('balance', INITIAL_BALANCE)
    account.setdefault('total_earned', 0)
    account.setdefault('transactions', [])
    account['balance'] = int(account.get('balance') or 0)
    return account


def append_tx(account: dict, tx_type: str, amount: int, description: str, *, meta: dict | None = None) -> None:
    tx = {
        'id': new_tx_id(),
        'ts': now_iso(),
        'type': tx_type,
        'amount': int(amount),
        'balance': int(account.get('balance') or 0),
        'balance_after': int(account.get('balance') or 0),
        'description': description,
    }
    if meta:
        tx['meta'] = meta
    account.setdefault('transactions', []).append(tx)
    account['transactions'] = account.get('transactions', [])[-100:]


def make_deck() -> list[tuple[str, str]]:
    deck = [(rank, suit) for suit in SUITS for rank in RANKS]
    rng = secrets.SystemRandom()
    rng.shuffle(deck)
    return deck


def card_text(card: tuple[str, str]) -> str:
    return f'{card[0]}{card[1]}'


def hand_text(hand: list[tuple[str, str]]) -> str:
    return ' '.join(card_text(card) for card in hand) or '-'


def card_asset_name(card: tuple[str, str]) -> str:
    return f'{card[0]}{SUIT_CODES[card[1]]}.png'


def hand_total(hand: list[tuple[str, str]]) -> int:
    total = 0
    aces = 0
    for rank, _ in hand:
        if rank == 'A':
            aces += 1
            total += 11
        elif rank in {'J', 'Q', 'K'}:
            total += 10
        else:
            total += int(rank)
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total


def is_blackjack(hand: list[tuple[str, str]]) -> bool:
    return len(hand) == 2 and hand_total(hand) == 21


def is_xibang(hand: list[tuple[str, str]]) -> bool:
    return len(hand) == 2 and hand[0][0] == 'A' and hand[1][0] == 'A'


def is_ngulinh(hand: list[tuple[str, str]]) -> bool:
    return len(hand) >= 5 and hand_total(hand) <= 21


@dataclass
class BlackjackSession:
    guild_id: int
    user_id: int
    user_name: str
    bet: int
    total_bet: int
    deck: list[tuple[str, str]]
    player: list[tuple[str, str]]
    dealer: list[tuple[str, str]]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message_id: int | None = None
    finished: bool = False
    doubled: bool = False

    @property
    def can_double(self) -> bool:
        return len(self.player) == 2 and not self.doubled and not self.finished

    @property
    def can_surrender(self) -> bool:
        return len(self.player) == 2 and not self.doubled and not self.finished


@dataclass
class TaixiuBet:
    user_id: int
    user_name: str
    choice: str
    amount: int


@dataclass
class TaixiuSession:
    guild_id: int
    round_id: int
    round_number: int
    status: str
    created_at: datetime
    bets: dict[int, TaixiuBet] = field(default_factory=dict)
    channel_id: int | None = None
    message_id: int | None = None
    task: asyncio.Task | None = None
    dice: tuple[int, int, int] | None = None
    result: str | None = None
    finishes_at: datetime | None = None
    settled: bool = False
    board_cache_key: tuple | None = None
    board_cache_png: bytes | None = None
    notice: str | None = None
    private_messages: dict[int, list[object]] = field(default_factory=dict)
    private_result_messages: dict[int, object] = field(default_factory=dict)
    settlements: dict[int, dict] = field(default_factory=dict)

    def seconds_left(self) -> int:
        if self.finishes_at is None:
            return 0
        return max(0, int((self.finishes_at - datetime.now(timezone.utc)).total_seconds()))


@dataclass
class HiLoSession:
    guild_id: int
    channel_id: int
    user_id: int
    user_name: str
    bet: int
    current_number: int
    round_number: int = 0
    history: list[dict] = field(default_factory=list)
    message_id: int | None = None
    finished: bool = False
    awaiting_continue: bool = False

    @property
    def current_multiplier(self) -> float:
        if self.round_number <= 0:
            return 1.0
        return HILO_MULTIPLIERS[min(self.round_number, len(HILO_MULTIPLIERS)) - 1]

    @property
    def next_multiplier(self) -> float:
        index = min(self.round_number, len(HILO_MULTIPLIERS) - 1)
        return HILO_MULTIPLIERS[index]


@dataclass
class ReactionPanel:
    game_key: str
    guild_id: int
    channel_id: int
    message_id: int
    expires_at: datetime | None = None

    def expired(self) -> bool:
        return self.expires_at is not None and datetime.now(timezone.utc) >= self.expires_at


class BlackjackView(discord.ui.View):
    def __init__(self, cog: CasinoCog, session: BlackjackSession):
        super().__init__(timeout=600)
        self.cog = cog
        self.session = session
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.custom_id == 'casino_blackjack_double':
                item.disabled = not session.can_double
            if isinstance(item, discord.ui.Button) and item.custom_id == 'casino_blackjack_surrender':
                item.disabled = not session.can_surrender

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message('Đây không phải ván Blackjack của bạn.', ephemeral=True)
            return False
        return True

    def disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    @discord.ui.button(label='Hit', emoji='🃏', style=discord.ButtonStyle.primary, custom_id='casino_blackjack_hit')
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.blackjack_hit(interaction, self.session)

    @discord.ui.button(label='Stand', emoji='✋', style=discord.ButtonStyle.secondary, custom_id='casino_blackjack_stand')
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.blackjack_stand(interaction, self.session)

    @discord.ui.button(label='Double', emoji='2️⃣', style=discord.ButtonStyle.success, custom_id='casino_blackjack_double')
    async def double(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.blackjack_double(interaction, self.session)

    @discord.ui.button(label='Surrender', emoji='🏳️', style=discord.ButtonStyle.danger, custom_id='casino_blackjack_surrender')
    async def surrender(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.blackjack_surrender(interaction, self.session)


class TaixiuView(discord.ui.View):
    def __init__(self, cog: CasinoCog, guild_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        session = cog.taixiu_sessions.get(guild_id)
        locked = not session or session.status != 'BETTING'
        for item in list(self.children):
            if isinstance(item, discord.ui.Button) and item.custom_id == 'casino_taixiu_confirm':
                self.remove_item(item)
                continue
            if (
                isinstance(item, discord.ui.Button)
                and item.custom_id != 'casino_taixiu_history'
                and str(item.custom_id or '').startswith('casino_taixiu_')
            ):
                item.disabled = locked

    @discord.ui.button(label='ĐẶT TÀI', emoji='🔴', style=discord.ButtonStyle.danger, row=0, custom_id='casino_taixiu_tai')
    async def tai(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_choice(interaction, 'TAI')

    @discord.ui.button(label='ĐẶT XỈU', emoji='🔵', style=discord.ButtonStyle.primary, row=0, custom_id='casino_taixiu_xiu')
    async def xiu(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_choice(interaction, 'XIU')

    @discord.ui.button(label='1K', style=discord.ButtonStyle.secondary, row=1, custom_id='casino_taixiu_chip_1000')
    async def chip_1k(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_amount(interaction, 1_000)

    @discord.ui.button(label='10K', style=discord.ButtonStyle.secondary, row=1, custom_id='casino_taixiu_chip_10000')
    async def chip_10k(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_amount(interaction, 10_000)

    @discord.ui.button(label='50K', style=discord.ButtonStyle.secondary, row=1, custom_id='casino_taixiu_chip_50000')
    async def chip_50k(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_amount(interaction, 50_000)

    @discord.ui.button(label='100K', style=discord.ButtonStyle.secondary, row=1, custom_id='casino_taixiu_chip_100000')
    async def chip_100k(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_amount(interaction, 100_000)

    @discord.ui.button(label='500K', style=discord.ButtonStyle.secondary, row=1, custom_id='casino_taixiu_chip_500000')
    async def chip_500k(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_amount(interaction, 500_000)

    @discord.ui.button(label='ALL-IN', emoji='🪙', style=discord.ButtonStyle.secondary, row=2, custom_id='casino_taixiu_all_in')
    async def all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stage_taixiu_all_in(interaction)

    @discord.ui.button(label='ĐẶT CƯỢC', emoji='✅', style=discord.ButtonStyle.success, row=2, custom_id='casino_taixiu_confirm')
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.confirm_taixiu_bet(interaction)

    @discord.ui.button(label='HỦY CƯỢC', emoji='↩️', style=discord.ButtonStyle.danger, row=2, custom_id='casino_taixiu_cancel')
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.cancel_taixiu_bet(interaction)

    @discord.ui.button(label='LỊCH SỬ', emoji='📜', style=discord.ButtonStyle.secondary, row=2, custom_id='casino_taixiu_history')
    async def history(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        guild_id = interaction.guild_id or self.guild_id
        session = self.cog.taixiu_sessions.get(int(guild_id))
        if not session:
            await interaction.response.defer()
            return
        recent = self.cog.recent_taixiu_results(interaction.guild_id or self.guild_id, limit=20)
        text = self.cog.format_recent_results(recent) or 'Chưa có lịch sử round.'
        await self.cog.send_taixiu_ephemeral(interaction, f'**20 round gần nhất**\n{text}')


class DiceDuelView(discord.ui.View):
    def __init__(self, cog: CasinoCog, *, guild_id: int, user_id: int, user_name: str, bet: int):
        super().__init__(timeout=DICE_PANEL_SECONDS)
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.user_name = user_name
        self.bet = bet
        self.rolled = False
        self.message: discord.Message | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message('Đây không phải ván Dice Duel của bạn.', ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        self.disable_all()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    def disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    @discord.ui.button(label='Roll', emoji='🎲', style=discord.ButtonStyle.primary, custom_id='casino_dice_roll')
    async def roll(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.play_dice_duel(interaction, self)


class DicePlayAgainView(discord.ui.View):
    def __init__(self, cog: CasinoCog, *, guild_id: int, user_id: int, user_name: str, bet: int):
        super().__init__(timeout=DICE_PANEL_SECONDS)
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.user_name = user_name
        self.bet = bet
        self.message: discord.Message | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message('Đây không phải ván Dice Duel của bạn.', ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        self.disable_all()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    def disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    @discord.ui.button(label='Play Again', emoji='🔁', style=discord.ButtonStyle.success, custom_id='casino_dice_again')
    async def play_again(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.reset_dice_duel(interaction, self)


class HiLoView(discord.ui.View):
    def __init__(self, cog: CasinoCog, session: HiLoSession):
        super().__init__(timeout=HILO_PANEL_SECONDS)
        self.cog = cog
        self.session = session
        self.message: discord.Message | None = None
        self.refresh_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message('Đây không phải ván Hi-Lo của bạn.', ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        if not self.session.finished:
            self.cog.finish_hilo(self.session, result='TIMEOUT', label='Hết giờ, ván bị hủy', payout=0)
        self.disable_all()
        if self.message:
            try:
                await self.message.edit(embed=self.cog.hilo_embed(self.session, status='timeout'), view=self)
            except discord.HTTPException:
                pass

    def disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    def refresh_buttons(self) -> None:
        for item in self.children:
            if not isinstance(item, discord.ui.Button):
                continue
            item.disabled = self.session.finished
            if item.custom_id in {'casino_hilo_higher', 'casino_hilo_lower'}:
                item.disabled = self.session.finished or self.session.awaiting_continue
            if item.custom_id in {'casino_hilo_cashout', 'casino_hilo_continue'}:
                item.disabled = self.session.finished or not self.session.awaiting_continue

    @discord.ui.button(label='Higher', emoji='⬆️', style=discord.ButtonStyle.success, custom_id='casino_hilo_higher')
    async def higher(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.hilo_guess(interaction, self.session, 'HIGHER')

    @discord.ui.button(label='Lower', emoji='⬇️', style=discord.ButtonStyle.danger, custom_id='casino_hilo_lower')
    async def lower(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.hilo_guess(interaction, self.session, 'LOWER')

    @discord.ui.button(label='Cash Out', emoji='💰', style=discord.ButtonStyle.primary, custom_id='casino_hilo_cashout')
    async def cashout(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.hilo_cashout(interaction, self.session)

    @discord.ui.button(label='Continue', emoji='▶️', style=discord.ButtonStyle.secondary, custom_id='casino_hilo_continue')
    async def continue_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.hilo_continue(interaction, self.session)


class SlotMachineView(discord.ui.View):
    def __init__(self, cog: CasinoCog, *, guild_id: int, user_id: int, user_name: str, bet: int):
        super().__init__(timeout=SLOT_PANEL_SECONDS)
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.user_name = user_name
        self.bet = bet
        self.message: discord.Message | None = None
        self.spins = 0
        self.spinning = False
        self.last_reels: tuple[str, str, str] | None = None
        self.last_result: dict | None = None
        self.total_profit = 0

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message('Đây không phải Slot Machine của bạn.', ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        self.disable_all()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    def disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    def refresh_buttons(self, balance: int | None = None) -> None:
        if balance is None:
            balance = self.cog._ensure_wallet(self.guild_id, self.user_id, self.user_name)['balance']
        for item in self.children:
            if not isinstance(item, discord.ui.Button):
                continue
            if item.custom_id == 'casino_slot_spin':
                item.label = 'Spin Again' if self.spins else 'Spin'
                item.disabled = self.spinning or int(balance) < int(self.bet)
            elif item.custom_id == 'casino_slot_bet_down':
                item.disabled = self.spinning or self.bet <= MIN_BET
            elif item.custom_id == 'casino_slot_bet_up':
                item.disabled = self.spinning or self.bet >= min(MAX_BET, int(balance))
            elif item.custom_id == 'casino_slot_max_bet':
                item.disabled = self.spinning or self.bet >= min(MAX_BET, int(balance))
            else:
                item.disabled = self.spinning

    @discord.ui.button(label='SPIN', emoji='🎰', style=discord.ButtonStyle.success, row=0, custom_id='casino_slot_spin')
    async def spin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.play_slot_spin(interaction, self)

    @discord.ui.button(label='BET+', emoji='➕', style=discord.ButtonStyle.primary, row=0, custom_id='casino_slot_bet_up')
    async def bet_up(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.adjust_slot_bet(interaction, self, MIN_BET)

    @discord.ui.button(label='BET-', emoji='➖', style=discord.ButtonStyle.secondary, row=0, custom_id='casino_slot_bet_down')
    async def bet_down(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.adjust_slot_bet(interaction, self, -MIN_BET)

    @discord.ui.button(label='MAX BET', emoji='💰', style=discord.ButtonStyle.danger, row=1, custom_id='casino_slot_max_bet')
    async def max_bet(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.max_slot_bet(interaction, self)

    @discord.ui.button(label='PAY TABLE', emoji='📜', style=discord.ButtonStyle.secondary, row=1, custom_id='casino_slot_pay_table')
    async def pay_table(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_slot_pay_table(interaction)


class CasinoCog(commands.Cog, name='CasinoCog'):
    casino = app_commands.Group(name='casino', description='Trò chơi dùng chung ví coins')

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.blackjack_sessions: dict[tuple[int, int], BlackjackSession] = {}
        self.taixiu_sessions: dict[int, TaixiuSession] = {}
        self.hilo_sessions: dict[tuple[int, int], HiLoSession] = {}
        self.default_taixiu_bets: dict[tuple[int, int], int] = {}
        self.pending_taixiu_bets: dict[tuple[int, int], dict[str, int | str]] = {}
        self.reaction_panels: dict[int, ReactionPanel] = {}
        self._taixiu_base_image = None
        self._action_locks: dict[tuple[int, int, str], asyncio.Lock] = {}
        self._last_action_at: dict[tuple[int, int, str], float] = {}

    async def cog_load(self):
        self.bot.study_context.database.initialize()

    async def cog_unload(self):
        for session in list(self.taixiu_sessions.values()):
            if session.task:
                session.task.cancel()

    async def _guild_guard(self, interaction: discord.Interaction, game_key: str = ALL_GAME_KEY) -> bool:
        if interaction.guild_id is None:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        if interaction.user.bot:
            await interaction.response.send_message('Bot không thể chơi casino.', ephemeral=True)
            return False
        if not await self._game_channel_guard(interaction, game_key):
            return False
        return True

    async def _send_error(self, interaction: discord.Interaction, message: str) -> None:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    def _action_key(self, guild_id: int | None, user_id: int, game_key: str) -> tuple[int, int, str]:
        return (int(guild_id or 0), int(user_id), str(game_key))

    def _action_lock(self, guild_id: int | None, user_id: int, game_key: str) -> asyncio.Lock:
        key = self._action_key(guild_id, user_id, game_key)
        lock = self._action_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._action_locks[key] = lock
        return lock

    async def _button_spam_guard(
        self,
        interaction: discord.Interaction,
        game_key: str,
        *,
        cooldown: float = GAME_BUTTON_COOLDOWN_SECONDS,
    ) -> asyncio.Lock | None:
        key = self._action_key(interaction.guild_id, interaction.user.id, game_key)
        lock = self._action_lock(interaction.guild_id, interaction.user.id, game_key)
        now = time.monotonic()
        last_at = self._last_action_at.get(key, 0.0)
        if lock.locked() or now - last_at < cooldown:
            await self._send_error(interaction, 'Thao tác trước đang xử lý, chờ một nhịp rồi bấm lại.')
            return None
        self._last_action_at[key] = now
        return lock

    def _log_interaction_perf(self, interaction: discord.Interaction, game_key: str, action: str, started_at: float) -> None:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        data = getattr(interaction, 'data', None)
        custom_id = data.get('custom_id') if isinstance(data, dict) else None
        log.info(
            'casino.interaction game=%s action=%s guild_id=%s user_id=%s latency_ms=%.1f custom_id=%s',
            game_key,
            action,
            interaction.guild_id,
            interaction.user.id,
            elapsed_ms,
            custom_id,
        )

    async def _defer_component_update(self, interaction: discord.Interaction) -> None:
        if not interaction.response.is_done():
            await interaction.response.defer()

    def _configured_game_channel_ids(self, guild_id: int) -> set[int]:
        raw_ids = self.bot.study_context.config_manager.get(int(guild_id), 'game_channel_ids', []) or []
        ids: set[int] = set()
        for raw in raw_ids:
            try:
                ids.add(int(raw))
            except (TypeError, ValueError):
                continue
        return ids

    def _configured_game_channel_map(self, guild_id: int) -> dict[int, set[str]]:
        raw_map = self.bot.study_context.config_manager.get(int(guild_id), 'game_channel_map', {}) or {}
        result: dict[int, set[str]] = {}
        if not isinstance(raw_map, dict):
            return result
        for raw_channel_id, raw_games in raw_map.items():
            try:
                channel_id = int(raw_channel_id)
            except (TypeError, ValueError):
                continue
            if isinstance(raw_games, str):
                games = {raw_games.lower().strip()}
            elif isinstance(raw_games, list):
                games = {str(item).lower().strip() for item in raw_games}
            else:
                continue
            games = {game for game in games if game in KNOWN_GAME_KEYS}
            if games:
                result[channel_id] = games
        return result

    def _channel_games(self, guild_id: int, channel_id: int) -> set[str]:
        channel_map = self._configured_game_channel_map(guild_id)
        games = set(channel_map.get(int(channel_id), set()))
        if not games and int(channel_id) in self._configured_game_channel_ids(guild_id):
            games.add(ALL_GAME_KEY)
        return games

    def _is_game_channel(self, guild_id: int, channel_id: int, game_key: str) -> bool:
        games = self._channel_games(guild_id, channel_id)
        return ALL_GAME_KEY in games or game_key in games

    def _is_any_game_channel(self, guild_id: int, channel_id: int) -> bool:
        return bool(self._channel_games(guild_id, channel_id))

    def _format_game_channels(self, guild: discord.Guild | None, channel_ids: set[int]) -> str:
        if not guild or not channel_ids:
            return ''
        labels = []
        for channel_id in sorted(channel_ids):
            channel = guild.get_channel(channel_id)
            labels.append(channel.mention if channel and hasattr(channel, 'mention') else f'`{channel_id}`')
        return ', '.join(labels)

    async def _game_channel_guard(self, interaction: discord.Interaction, game_key: str = ALL_GAME_KEY) -> bool:
        guild_id = int(interaction.guild_id or 0)
        channel_id = int(interaction.channel_id or 0)
        channel_map = self._configured_game_channel_map(guild_id)
        allowed_ids = self._configured_game_channel_ids(guild_id) | set(channel_map.keys())
        if not allowed_ids:
            await self._send_error(
                interaction,
                'Admin chưa set kênh game. Dùng `/admin game_channels add <channel>` trước khi chơi.',
            )
            return False
        if not self._is_game_channel(guild_id, channel_id, game_key):
            allowed_text = self._format_game_channels(interaction.guild, allowed_ids)
            suffix = f'\nKênh game hiện tại: {allowed_text}' if allowed_text else ''
            await self._send_error(
                interaction,
                f'Lệnh game `{game_key}` chỉ dùng được trong kênh đã gán game đó.' + suffix,
            )
            return False
        return True

    async def _prefix_guild_guard(self, ctx: commands.Context, game_key: str = ALL_GAME_KEY) -> bool:
        if ctx.guild is None:
            await ctx.send('❌ Lệnh này chỉ dùng được trong server.')
            return False
        if ctx.author.bot:
            return False
        guild_id = int(ctx.guild.id)
        channel_id = int(ctx.channel.id)
        channel_map = self._configured_game_channel_map(guild_id)
        allowed_ids = self._configured_game_channel_ids(guild_id) | set(channel_map.keys())
        if not allowed_ids:
            await ctx.send('❌ Admin chưa set kênh game. Dùng `/admin game_channels add <channel>` trước khi chơi.')
            return False
        if not self._is_game_channel(guild_id, channel_id, game_key):
            allowed_text = self._format_game_channels(ctx.guild, allowed_ids)
            suffix = f'\nKênh game hiện tại: {allowed_text}' if allowed_text else ''
            await ctx.send(f'❌ Lệnh game `{game_key}` chỉ dùng được trong kênh đã gán game đó.' + suffix)
            return False
        return True

    async def _any_game_channel_guard(self, interaction: discord.Interaction) -> bool:
        if interaction.guild_id is None:
            await interaction.response.send_message('Lệnh này chỉ dùng được trong server.', ephemeral=True)
            return False
        if interaction.user.bot:
            await interaction.response.send_message('Bot không thể nhận daily.', ephemeral=True)
            return False
        guild_id = int(interaction.guild_id)
        channel_id = int(interaction.channel_id or 0)
        channel_map = self._configured_game_channel_map(guild_id)
        allowed_ids = self._configured_game_channel_ids(guild_id) | set(channel_map.keys())
        if not allowed_ids:
            await self._send_error(
                interaction,
                'Admin chưa set kênh game. Dùng `/admin game_channels add <channel>` trước.',
            )
            return False
        if not self._is_any_game_channel(guild_id, channel_id):
            allowed_text = self._format_game_channels(interaction.guild, allowed_ids)
            suffix = f'\nKênh game hiện tại: {allowed_text}' if allowed_text else ''
            await self._send_error(
                interaction,
                'Daily chỉ dùng được trong kênh đã set game.' + suffix,
            )
            return False
        return True

    async def _prefix_any_game_channel_guard(self, ctx: commands.Context) -> bool:
        if ctx.guild is None:
            await ctx.send('❌ Lệnh này chỉ dùng được trong server.')
            return False
        if ctx.author.bot:
            return False
        guild_id = int(ctx.guild.id)
        channel_id = int(ctx.channel.id)
        channel_map = self._configured_game_channel_map(guild_id)
        allowed_ids = self._configured_game_channel_ids(guild_id) | set(channel_map.keys())
        if not allowed_ids:
            await ctx.send('❌ Admin chưa set kênh game. Dùng `/admin game_channels add <channel>` trước.')
            return False
        if not self._is_any_game_channel(guild_id, channel_id):
            allowed_text = self._format_game_channels(ctx.guild, allowed_ids)
            suffix = f'\nKênh game hiện tại: {allowed_text}' if allowed_text else ''
            await ctx.send('❌ Daily chỉ dùng được trong kênh đã set game.' + suffix)
            return False
        return True

    @staticmethod
    def _parse_bet(raw: str | None) -> tuple[int | None, str | None]:
        if raw is None or str(raw).strip() == '':
            return None, f'Nhập số tiền cược, ví dụ `dice {MIN_BET}` hoặc `hilo {MIN_BET}`.'
        try:
            amount = int(str(raw).replace(',', '').strip())
        except ValueError:
            return None, 'Số cược phải là số nguyên.'
        if amount < MIN_BET:
            return None, f'Cược tối thiểu là {money(MIN_BET)}.'
        if amount > MAX_BET:
            return None, f'Cược tối đa là {money(MAX_BET)}.'
        return amount, None

    async def _add_reactions(self, message: discord.Message, emojis: list[str]) -> None:
        for emoji in emojis:
            try:
                await message.add_reaction(emoji)
            except discord.HTTPException:
                log.warning('Could not add reaction %s to message %s', emoji, message.id, exc_info=True)

    async def _clear_game_reactions(self, message: discord.Message) -> None:
        try:
            await message.clear_reactions()
        except discord.HTTPException:
            pass

    async def _remove_user_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        if not payload.guild_id or not payload.channel_id:
            return
        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(payload.channel_id)
            except discord.HTTPException:
                return
        try:
            message = await channel.fetch_message(payload.message_id)
            user = payload.member or self.bot.get_user(payload.user_id) or await self.bot.fetch_user(payload.user_id)
            await message.remove_reaction(payload.emoji, user)
        except discord.HTTPException:
            pass

    async def _fetch_reaction_message(self, panel: ReactionPanel) -> discord.Message | None:
        channel = self.bot.get_channel(panel.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(panel.channel_id)
            except discord.HTTPException:
                return None
        try:
            return await channel.fetch_message(panel.message_id)
        except discord.HTTPException:
            return None

    def _register_panel(
        self,
        *,
        game_key: str,
        guild_id: int,
        channel_id: int,
        message_id: int,
        expires_at: datetime | None = None,
    ) -> None:
        self.reaction_panels[int(message_id)] = ReactionPanel(
            game_key=game_key,
            guild_id=int(guild_id),
            channel_id=int(channel_id),
            message_id=int(message_id),
            expires_at=expires_at,
        )

    def _unregister_panel(self, message_id: int | None) -> None:
        if message_id:
            self.reaction_panels.pop(int(message_id), None)

    def _wallet_change(
        self,
        *,
        guild_id: int,
        user_id: int,
        user_name: str,
        amount: int,
        tx_type: str,
        description: str,
        meta: dict | None = None,
    ) -> dict:
        repository = getattr(self.bot.study_context, 'repository', None)
        if repository and hasattr(repository, 'change_balance'):
            return repository.change_balance(
                guild_id=guild_id,
                user_id=user_id,
                display_name=user_name,
                amount=amount,
                tx_type=tx_type,
                description=description,
                payload=meta,
            )

        def mutator(data: dict):
            account = ensure_wallet(data, user_id, user_name)
            balance_before = int(account.get('balance') or 0)
            balance_after = balance_before + int(amount)
            if balance_after < 0:
                return {
                    'ok': False,
                    'error': f'Balance không đủ. Bạn có {money(balance_before)}.',
                    'balance': balance_before,
                }
            account['balance'] = balance_after
            append_tx(account, tx_type, int(amount), description, meta=meta)
            return {'ok': True, 'balance': balance_after, 'balance_before': balance_before}

        result, _ = self.bot.study_context.update_data(mutator, guild_id)
        if not result.get('ok'):
            raise ValueError(result.get('error') or 'Không thể cập nhật ví.')
        return result

    def _ensure_wallet(self, guild_id: int, user_id: int, user_name: str) -> dict:
        repository = getattr(self.bot.study_context, 'repository', None)
        if repository and hasattr(repository, 'get_account_balance'):
            return repository.get_account_balance(guild_id=guild_id, user_id=user_id, display_name=user_name)

        def mutator(data: dict):
            account = ensure_wallet(data, user_id, user_name)
            return {'balance': int(account.get('balance') or 0)}

        result, _ = self.bot.study_context.update_data(mutator, guild_id)
        return result

    async def _wallet_change_async(self, **kwargs) -> dict:
        return await asyncio.to_thread(self._wallet_change, **kwargs)

    async def _ensure_wallet_async(self, guild_id: int, user_id: int, user_name: str) -> dict:
        return await asyncio.to_thread(self._ensure_wallet, guild_id, user_id, user_name)

    async def _record_game_history_async(self, **kwargs) -> None:
        await asyncio.to_thread(self.record_game_history, **kwargs)

    def record_game_history(
        self,
        *,
        guild_id: int,
        user_id: int,
        game_type: str,
        bet_amount: int,
        result: str,
        profit: int,
        metadata: dict,
    ) -> None:
        try:
            with self.bot.study_context.database.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO casino_game_history (
                        guild_id, user_id, game_type, bet_amount, result, profit,
                        metadata_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(guild_id),
                        int(user_id),
                        game_type,
                        int(bet_amount),
                        result,
                        int(profit),
                        json.dumps(metadata, ensure_ascii=False, separators=(',', ':')),
                        now_iso(),
                    ),
                )
        except Exception:
            log.exception(
                'Could not record casino history: guild_id=%s user_id=%s game_type=%s result=%s',
                guild_id,
                user_id,
                game_type,
                result,
            )

    @staticmethod
    def _slot_symbol() -> str:
        total_weight = sum(weight for _, weight in SLOT_WEIGHTED_SYMBOLS)
        pick = secrets.randbelow(total_weight)
        running = 0
        for symbol, weight in SLOT_WEIGHTED_SYMBOLS:
            running += weight
            if pick < running:
                return symbol
        return SLOT_WEIGHTED_SYMBOLS[0][0]

    def spin_slot_reels(self) -> tuple[str, str, str]:
        return (self._slot_symbol(), self._slot_symbol(), self._slot_symbol())

    @staticmethod
    def evaluate_slot_result(reels: tuple[str, str, str], bet: int) -> dict:
        counts = {symbol: reels.count(symbol) for symbol in set(reels)}
        if len(counts) == 1:
            symbol = reels[0]
            if symbol in SLOT_TRIPLE_MULTIPLIERS:
                multiplier = SLOT_TRIPLE_MULTIPLIERS[symbol]
                payout = int(bet) * multiplier
                return {
                    'result': 'WIN',
                    'label': f'3 {symbol} - x{multiplier}',
                    'payout': payout,
                    'multiplier': multiplier,
                }
        if all(symbol in SLOT_FRUIT_SYMBOLS for symbol in reels):
            payout = int(bet) * 3
            return {'result': 'WIN', 'label': '3 fruit - x3', 'payout': payout, 'multiplier': 3}
        if any(count == 2 for count in counts.values()):
            payout = int(bet) // 2
            return {'result': 'PARTIAL', 'label': '2 biểu tượng giống nhau - hoàn 50%', 'payout': payout, 'multiplier': 0.5}
        return {'result': 'LOSE', 'label': 'Không trúng', 'payout': 0, 'multiplier': 0}

    def _slot_symbol_image(self, symbol: str | None, size: tuple[int, int]):
        if Image is None or ImageDraw is None:
            return None

        if symbol:
            asset_name = SLOT_SYMBOL_ASSETS.get(symbol)
            if asset_name:
                image = cached_resized_asset(SLOT_ASSET_DIR / asset_name, size)
                if image is not None:
                    return image

        width, height = size
        icon = Image.new('RGBA', size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(icon, 'RGBA')
        colors = {
            '🍒': ((212, 26, 40), 'CH'),
            '🍋': ((241, 216, 42), 'LM'),
            '🍊': ((238, 132, 31), 'OR'),
            '🍇': ((130, 55, 204), 'GR'),
            '🔔': ((241, 185, 44), 'BL'),
            '💎': ((58, 200, 240), 'DM'),
            '7️⃣': ((215, 30, 34), '7'),
            None: ((95, 108, 126), '?'),
        }
        fill, label = colors.get(symbol, ((95, 108, 126), '?'))
        shadow = (0, 0, 0, 72)
        draw.ellipse((18, 22, width - 10, height - 6), fill=shadow)
        if symbol == '💎':
            points = [(width // 2, 10), (width - 12, height // 2), (width // 2, height - 10), (12, height // 2)]
            draw.polygon(points, fill=(*fill, 255), outline=(255, 255, 255, 210))
        elif symbol == '7️⃣':
            font = casino_font(92, bold=True, serif=True)
            draw_centered_text(draw, (0, 0, width, height), '7', font, fill, stroke_width=5, stroke_fill=(255, 222, 89))
            return icon
        elif symbol == '🔔':
            draw.pieslice((18, 12, width - 18, height + 18), 200, 340, fill=(*fill, 255), outline=(255, 239, 169, 230), width=4)
            draw.ellipse((width // 2 - 16, height - 30, width // 2 + 16, height - 2), fill=(176, 104, 20, 255))
        else:
            draw.ellipse((18, 12, width - 18, height - 12), fill=(*fill, 255), outline=(255, 255, 255, 210), width=4)
        font = casino_font(30 if len(label) > 1 else 58, bold=True)
        draw_centered_text(draw, (0, 0, width, height), label, font, (255, 255, 255, 245), stroke_width=2, stroke_fill=(20, 22, 30))
        return icon

    def _slot_board_image(
        self,
        *,
        user_id: int,
        bet: int,
        balance: int,
        total_profit: int,
        reels: tuple[str, str, str] | None = None,
        result: dict | None = None,
    ):
        if Image is None or ImageDraw is None:
            return None

        width, height = SLOT_BOARD_SIZE
        image = Image.new('RGBA', (width, height), (4, 12, 32, 255))
        draw = ImageDraw.Draw(image, 'RGBA')
        for y in range(height):
            blend = y / height
            draw.line((0, y, width, y), fill=(4, int(15 + 15 * (1 - blend)), int(42 + 18 * (1 - blend)), 255))
        for radius, alpha in ((390, 28), (275, 32), (165, 42)):
            draw.ellipse(
                (width // 2 - radius, 42, width // 2 + radius, 220),
                fill=(255, 184, 48, alpha),
            )

        gold = (244, 180, 44, 255)
        deep_gold = (137, 78, 17, 255)
        pale_gold = (255, 232, 140, 255)
        red = (212, 24, 30, 255)
        dark = (5, 9, 18, 238)
        white = (247, 246, 240, 255)
        green = (89, 210, 74, 255)

        jackpot_font = casino_font(76, bold=True, serif=True)
        title_font = casino_font(50, bold=True, serif=True)
        small_font = casino_font(20, bold=True)
        value_font = casino_font(32, bold=True)

        draw.rounded_rectangle((26, 22, width - 26, height - 22), radius=34, fill=(2, 8, 24, 190), outline=gold, width=4)
        draw.rounded_rectangle((48, 44, width - 48, height - 44), radius=28, outline=deep_gold, width=2)

        draw_centered_text(draw, (0, 22, width, 98), '777', jackpot_font, red, stroke_width=5, stroke_fill=pale_gold)
        draw.rounded_rectangle((286, 95, 694, 150), radius=18, fill=(202, 18, 22, 255), outline=gold, width=4)
        draw_centered_text(draw, (286, 94, 694, 148), 'JACKPOT', title_font, pale_gold, stroke_width=2, stroke_fill=(92, 34, 8))
        for i in range(5):
            cx = 384 + i * 52
            cy = 168
            points = [
                (cx, cy - 22),
                (cx + 7, cy - 7),
                (cx + 23, cy - 7),
                (cx + 10, cy + 3),
                (cx + 15, cy + 19),
                (cx, cy + 9),
                (cx - 15, cy + 19),
                (cx - 10, cy + 3),
                (cx - 23, cy - 7),
                (cx - 7, cy - 7),
            ]
            draw.polygon(points, fill=gold, outline=(255, 246, 161, 255))

        status = 'READY TO SPIN'
        status_color = pale_gold
        if result:
            profit = int(result.get('payout') or 0) - int(bet)
            sign = '+' if profit > 0 else ''
            status = f'{result["label"]}  {sign}{compact_money(profit)}'
            status_color = (127, 242, 132, 255) if profit > 0 else (255, 105, 105, 255)
        draw_centered_text(draw, (0, 190, width, 220), status, small_font, status_color)

        reel_frame = (132, 230, 848, 410)
        draw.rounded_rectangle((reel_frame[0] + 8, reel_frame[1] + 10, reel_frame[2] + 8, reel_frame[3] + 10), radius=30, fill=(0, 0, 0, 120))
        draw.rounded_rectangle(reel_frame, radius=30, fill=(191, 73, 13, 255), outline=gold, width=6)
        for index in range(18):
            x = 154 + index * 39
            for y in (244, 394):
                draw.ellipse((x - 7, y - 7, x + 7, y + 7), fill=(255, 244, 143, 255), outline=(142, 82, 14, 255))
        for index in range(4):
            y = 266 + index * 32
            for x in (148, 832):
                draw.ellipse((x - 7, y - 7, x + 7, y + 7), fill=(255, 244, 143, 255), outline=(142, 82, 14, 255))
        draw.rounded_rectangle((168, 256, 812, 384), radius=18, fill=(249, 243, 224, 255), outline=(96, 55, 31, 255), width=3)

        active_reels: tuple[str | None, str | None, str | None] = reels if reels else (None, None, None)
        cell_w = 206
        icon_size = (112, 112)
        for index, symbol in enumerate(active_reels):
            x1 = 190 + index * cell_w
            x2 = x1 + 150
            draw.rounded_rectangle((x1, 268, x2, 372), radius=16, fill=(255, 251, 234, 255), outline=(174, 148, 105, 255), width=2)
            if index:
                draw.line((x1 - 26, 262, x1 - 26, 378), fill=(82, 44, 28, 170), width=3)
            icon = self._slot_symbol_image(symbol, icon_size)
            if icon:
                image.paste(icon, (x1 + 19, 264), icon)

        stats = [
            ('BALANCE', compact_money(balance), (51, 150, 39, 245)),
            ('BET', compact_money(bet), (22, 104, 176, 245)),
            ('TOTAL WIN', f'{total_profit:+,}', (148, 28, 112, 245)),
        ]
        stats_y = 432
        stat_w = 260
        for idx, (label, value, fill) in enumerate(stats):
            x = 84 + idx * 308
            draw.rounded_rectangle((x, stats_y, x + stat_w, stats_y + 72), radius=18, fill=fill, outline=gold, width=3)
            draw_centered_text(draw, (x + 8, stats_y + 5, x + stat_w - 8, stats_y + 30), label, small_font, pale_gold)
            value_color = green if label == 'TOTAL WIN' and total_profit > 0 else (255, 136, 136, 255) if label == 'TOTAL WIN' and total_profit < 0 else white
            draw_centered_text(draw, (x + 8, stats_y + 29, x + stat_w - 8, stats_y + 68), value, value_font, value_color)

        draw.rounded_rectangle((250, 516, 730, 546), radius=15, fill=dark, outline=deep_gold, width=2)
        draw_centered_text(draw, (250, 516, 730, 546), 'SPIN  |  BET+  |  BET-  |  MAX BET  |  PAY TABLE', casino_font(19, bold=True), pale_gold)
        return image.convert('RGB')

    def slot_board_file(
        self,
        *,
        user_id: int,
        bet: int,
        balance: int,
        total_profit: int,
        reels: tuple[str, str, str] | None = None,
        result: dict | None = None,
    ) -> discord.File | None:
        image = self._slot_board_image(
            user_id=user_id,
            bet=bet,
            balance=balance,
            total_profit=total_profit,
            reels=reels,
            result=result,
        )
        if image is None:
            return None
        buffer = io.BytesIO()
        image.save(buffer, format='PNG', optimize=False, compress_level=1)
        buffer.seek(0)
        filename = f'slot-board-{image_state_token(user_id, bet, balance, total_profit, reels, result)}.png'
        return discord.File(buffer, filename=filename)

    async def slot_send_payload_async(self, **kwargs) -> dict:
        return await asyncio.to_thread(self.slot_send_payload, **kwargs)

    async def slot_edit_payload_async(self, **kwargs) -> dict:
        return await asyncio.to_thread(self.slot_edit_payload, **kwargs)

    def slot_embed(
        self,
        *,
        user_id: int,
        bet: int,
        balance: int,
        total_profit: int = 0,
        reels: tuple[str, str, str] | None = None,
        result: dict | None = None,
    ) -> discord.Embed:
        color = BLUE
        if result:
            color = GREEN if result['result'] == 'WIN' else GOLD if result['result'] == 'PARTIAL' else RED
        embed = discord.Embed(
            title='🎰 SLOT MACHINE',
            description=(
                f'👤 Player: <@{user_id}>\n'
                f'💰 Balance: `{money(balance)}`\n'
                f'🎯 Bet: `{money(bet)}`\n'
                f'🏆 Tổng thắng: `{money(total_profit)}`'
            ),
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        if result:
            payout = int(result.get('payout') or 0)
            profit = payout - int(bet)
            sign = '+' if profit > 0 else ''
            reel_text = ' | '.join(reels or ())
            embed.add_field(name='Reels', value=f'**{reel_text}**', inline=True)
            embed.add_field(name='Result', value=f'**{result["label"]}** `{sign}{money(profit)}`', inline=True)
        else:
            embed.add_field(name='Payouts', value='3 🍒 x2 · 3 fruit x3 · 3 🔔 x5 · 3 💎 x10 · 3 7️⃣ x25', inline=False)
        embed.set_footer(text='BET+/BET- đổi cược. MAX BET dùng tối đa balance hiện có. Timeout 60s.')
        return embed

    def slot_send_payload(
        self,
        *,
        user_id: int,
        bet: int,
        balance: int,
        total_profit: int = 0,
        reels: tuple[str, str, str] | None = None,
        result: dict | None = None,
    ) -> dict:
        embed = self.slot_embed(
            user_id=user_id,
            bet=bet,
            balance=balance,
            total_profit=total_profit,
            reels=reels,
            result=result,
        )
        file = self.slot_board_file(
            user_id=user_id,
            bet=bet,
            balance=balance,
            total_profit=total_profit,
            reels=reels,
            result=result,
        )
        if file:
            embed.set_image(url=f'attachment://{file.filename}')
            return {'embed': embed, 'file': file}
        return {'embed': embed}

    def slot_edit_payload(
        self,
        *,
        user_id: int,
        bet: int,
        balance: int,
        total_profit: int = 0,
        reels: tuple[str, str, str] | None = None,
        result: dict | None = None,
    ) -> dict:
        payload = self.slot_send_payload(
            user_id=user_id,
            bet=bet,
            balance=balance,
            total_profit=total_profit,
            reels=reels,
            result=result,
        )
        file = payload.pop('file', None)
        if file:
            payload['attachments'] = [file]
        else:
            payload['attachments'] = []
        return payload

    async def start_slot_message(self, message: discord.Message, raw_bet: str | None) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, SLOT_GAME_KEY):
            return
        bet, error = self._parse_bet(raw_bet)
        if raw_bet is None or str(raw_bet).strip() == '':
            error = f'Nhập số tiền cược, ví dụ `slot {MIN_BET}`.'
        if error:
            await message.channel.send(f'❌ {error}')
            return
        guild_id = int(message.guild.id)
        user_id = int(message.author.id)
        user_name = display_name(message.author)
        balance = self._ensure_wallet(guild_id, user_id, user_name)['balance']
        if balance < int(bet):
            await message.channel.send(f'❌ Balance không đủ. Bạn có {money(balance)}.')
            return
        view = SlotMachineView(self, guild_id=guild_id, user_id=user_id, user_name=user_name, bet=int(bet))
        view.refresh_buttons(balance)
        sent = await message.channel.send(
            **self.slot_send_payload(user_id=user_id, bet=int(bet), balance=balance),
            view=view,
        )
        view.message = sent

    @commands.command(name='slot')
    async def slot_prefix(self, ctx: commands.Context, bet: str = None):
        await self.start_slot_message(ctx.message, bet)

    @app_commands.command(name='slot', description='Chơi Slot Machine 3 ô bằng coins')
    @app_commands.describe(bet='Số coins muốn cược')
    async def slot(self, interaction: discord.Interaction, bet: app_commands.Range[int, MIN_BET, MAX_BET]):
        await self.start_slot(interaction, int(bet))

    async def start_slot(self, interaction: discord.Interaction, bet: int) -> None:
        if not await self._guild_guard(interaction, SLOT_GAME_KEY):
            return
        guild_id = int(interaction.guild_id)
        user_id = int(interaction.user.id)
        user_name = display_name(interaction.user)
        await interaction.response.defer()
        balance = (await self._ensure_wallet_async(guild_id, user_id, user_name))['balance']
        if balance < int(bet):
            await interaction.followup.send(f'❌ Balance không đủ. Bạn có {money(balance)}.', ephemeral=True)
            return
        view = SlotMachineView(self, guild_id=guild_id, user_id=user_id, user_name=user_name, bet=int(bet))
        view.refresh_buttons(balance)
        sent = await interaction.followup.send(
            **await self.slot_send_payload_async(user_id=user_id, bet=int(bet), balance=balance),
            view=view,
            wait=True,
        )
        view.message = sent

    async def update_slot_panel(self, interaction: discord.Interaction, view: SlotMachineView, *, balance: int) -> None:
        view.refresh_buttons(balance)
        payload = await self.slot_edit_payload_async(
            user_id=view.user_id,
            bet=view.bet,
            balance=balance,
            total_profit=view.total_profit,
            reels=view.last_reels,
            result=view.last_result,
        )
        if interaction.response.is_done():
            if interaction.message is not None:
                await interaction.message.edit(**payload, view=view)
        else:
            await interaction.response.edit_message(**payload, view=view)
        view.message = interaction.message

    async def adjust_slot_bet(self, interaction: discord.Interaction, view: SlotMachineView, delta: int) -> None:
        if not await self._guild_guard(interaction, SLOT_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, SLOT_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._defer_component_update(interaction)
            await self._adjust_slot_bet_locked(interaction, view, delta)
            self._log_interaction_perf(interaction, SLOT_GAME_KEY, 'bet_adjust', started_at)

    async def _adjust_slot_bet_locked(self, interaction: discord.Interaction, view: SlotMachineView, delta: int) -> None:
        if view.spinning:
            await self._send_error(interaction, 'Slot đang quay, chờ kết quả chút nhé.')
            return
        balance = (await self._ensure_wallet_async(view.guild_id, view.user_id, view.user_name))['balance']
        max_allowed = min(MAX_BET, int(balance))
        new_bet = max(MIN_BET, min(max_allowed, int(view.bet) + int(delta)))
        if new_bet == view.bet:
            await self._send_error(interaction, 'Không thể chỉnh cược thêm theo hướng đó.')
            return
        view.bet = new_bet
        await self.update_slot_panel(interaction, view, balance=balance)

    async def max_slot_bet(self, interaction: discord.Interaction, view: SlotMachineView) -> None:
        if not await self._guild_guard(interaction, SLOT_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, SLOT_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._defer_component_update(interaction)
            await self._max_slot_bet_locked(interaction, view)
            self._log_interaction_perf(interaction, SLOT_GAME_KEY, 'max_bet', started_at)

    async def _max_slot_bet_locked(self, interaction: discord.Interaction, view: SlotMachineView) -> None:
        if view.spinning:
            await self._send_error(interaction, 'Slot đang quay, chờ kết quả chút nhé.')
            return
        balance = (await self._ensure_wallet_async(view.guild_id, view.user_id, view.user_name))['balance']
        max_allowed = min(MAX_BET, int(balance))
        if max_allowed < MIN_BET:
            await self._send_error(interaction, f'❌ Balance không đủ. Bạn có {money(balance)}.')
            return
        view.bet = max_allowed
        await self.update_slot_panel(interaction, view, balance=balance)

    async def show_slot_pay_table(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(
            title='📜 SLOT PAY TABLE',
            description='Bảng trả thưởng tính theo mức cược hiện tại của panel.',
            color=GOLD,
        )
        embed.add_field(name='3 🍒', value='x2', inline=True)
        embed.add_field(name='3 fruit', value='x3', inline=True)
        embed.add_field(name='3 🔔', value='x5', inline=True)
        embed.add_field(name='3 💎', value='x10', inline=True)
        embed.add_field(name='3 7️⃣', value='x25', inline=True)
        embed.add_field(name='Near miss', value='2 biểu tượng giống nhau hoàn 50%', inline=False)
        embed.set_footer(text='Fruit hiện gồm lemon, orange, grape.')
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def play_slot_spin(self, interaction: discord.Interaction, view: SlotMachineView) -> None:
        if not await self._guild_guard(interaction, SLOT_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, SLOT_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._play_slot_spin_locked(interaction, view)
            self._log_interaction_perf(interaction, SLOT_GAME_KEY, 'spin', started_at)

    async def _play_slot_spin_locked(self, interaction: discord.Interaction, view: SlotMachineView) -> None:
        if view.spinning:
            await self._send_error(interaction, 'Slot đang quay, chờ kết quả chút nhé.')
            return
        await self._defer_component_update(interaction)
        balance_before = (await self._ensure_wallet_async(view.guild_id, view.user_id, view.user_name))['balance']
        if balance_before < view.bet:
            view.refresh_buttons(balance_before)
            await self._send_error(interaction, f'❌ Balance không đủ. Bạn có {money(balance_before)}.')
            return
        view.spinning = True
        view.refresh_buttons(balance_before)
        if interaction.message is not None:
            await interaction.message.edit(view=view)
        try:
            await self._wallet_change_async(
                guild_id=view.guild_id,
                user_id=view.user_id,
                user_name=view.user_name,
                amount=-view.bet,
                tx_type='casino_slot_bet',
                description='Slot Machine bet',
                meta={'game': 'SLOT_MACHINE', 'bet': view.bet},
            )
        except ValueError as e:
            view.spinning = False
            view.refresh_buttons(balance_before)
            if interaction.message is not None:
                await interaction.message.edit(view=view)
            await self._send_error(interaction, f'❌ {e}')
            return

        reels = self.spin_slot_reels()
        result = self.evaluate_slot_result(reels, view.bet)
        payout = int(result.get('payout') or 0)
        if payout:
            change = await self._wallet_change_async(
                guild_id=view.guild_id,
                user_id=view.user_id,
                user_name=view.user_name,
                amount=payout,
                tx_type='casino_slot_payout',
                description=f'Slot Machine payout: {result["label"]}',
                meta={'game': 'SLOT_MACHINE', 'payout': payout, 'reels': reels, 'round_result': result['result']},
            )
            balance = int(change['balance'])
        else:
            balance = (await self._ensure_wallet_async(view.guild_id, view.user_id, view.user_name))['balance']

        profit = payout - view.bet
        await self._record_game_history_async(
            guild_id=view.guild_id,
            user_id=view.user_id,
            game_type='SLOT_MACHINE',
            bet_amount=view.bet,
            result=result['result'],
            profit=profit,
            metadata={
                'reels': reels,
                'label': result['label'],
                'payout': payout,
                'multiplier': result.get('multiplier', 0),
            },
        )

        view.spins += 1
        view.last_reels = reels
        view.last_result = result
        view.total_profit += profit
        view.spinning = False
        view.refresh_buttons(balance)
        if interaction.message is None:
            await self._send_error(interaction, '❌ Không tìm thấy message Slot để cập nhật.')
            return
        await interaction.message.edit(
            **await self.slot_edit_payload_async(
                user_id=view.user_id,
                bet=view.bet,
                balance=balance,
                total_profit=view.total_profit,
                reels=reels,
                result=result,
            ),
            view=view,
        )
        view.message = interaction.message

    def dice_waiting_embed(self, *, user_id: int, bet: int, balance: int) -> discord.Embed:
        embed = discord.Embed(
            title='🎲 DICE DUEL',
            description=(
                f'👤 Player: <@{user_id}>\n'
                f'💰 Bet: `{money(bet)}`\n'
                f'💵 Balance: `{money(balance)}`\n\n'
                'Bấm **Roll** để tung 2 xúc xắc đấu với bot.'
            ),
            color=BLUE,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text='Tổng cao hơn thắng. Hòa hoàn tiền. Virtual coins only.')
        return embed

    def dice_result_embed(
        self,
        *,
        user_id: int,
        bet: int,
        player_dice: tuple[int, int],
        bot_dice: tuple[int, int],
        result: str,
        profit: int,
        balance: int,
    ) -> discord.Embed:
        color = GREEN if result == 'WIN' else RED if result == 'LOSE' else GREY
        player_total = sum(player_dice)
        bot_total = sum(bot_dice)
        label = 'Bạn thắng' if result == 'WIN' else 'Bạn thua' if result == 'LOSE' else 'Hòa, hoàn tiền'
        sign = '+' if profit > 0 else ''
        embed = discord.Embed(
            title='🎲 DICE DUEL RESULT',
            description=f'👤 Player: <@{user_id}>\n💰 Bet: `{money(bet)}`',
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(
            name='Your Roll',
            value=f'`{player_dice[0]}` + `{player_dice[1]}` = **{player_total}**',
            inline=True,
        )
        embed.add_field(
            name='Bot Roll',
            value=f'`{bot_dice[0]}` + `{bot_dice[1]}` = **{bot_total}**',
            inline=True,
        )
        embed.add_field(name='Result', value=f'**{label}** `{sign}{money(profit)}`', inline=False)
        embed.add_field(name='New Balance', value=f'`{money(balance)}`', inline=False)
        embed.set_footer(text='Play Again dùng cùng mức cược. Virtual coins only.')
        return embed

    async def start_dice_duel_message(self, message: discord.Message, raw_bet: str | None) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, DICE_GAME_KEY):
            return
        bet, error = self._parse_bet(raw_bet)
        if error:
            await message.channel.send(f'❌ {error}')
            return
        guild_id = int(message.guild.id)
        user_id = int(message.author.id)
        user_name = display_name(message.author)
        balance = self._ensure_wallet(guild_id, user_id, user_name)['balance']
        if balance < int(bet):
            await message.channel.send(f'❌ Balance không đủ. Bạn có {money(balance)}.')
            return
        view = DiceDuelView(self, guild_id=guild_id, user_id=user_id, user_name=user_name, bet=int(bet))
        sent = await message.channel.send(embed=self.dice_waiting_embed(user_id=user_id, bet=int(bet), balance=balance), view=view)
        view.message = sent

    @commands.command(name='dice')
    async def dice_prefix(self, ctx: commands.Context, bet: str = None):
        await self.start_dice_duel_message(ctx.message, bet)

    async def reset_dice_duel(self, interaction: discord.Interaction, view: DicePlayAgainView) -> None:
        if not await self._guild_guard(interaction, DICE_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, DICE_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._defer_component_update(interaction)
            balance = (await self._ensure_wallet_async(view.guild_id, view.user_id, view.user_name))['balance']
            if balance < view.bet:
                await self._send_error(interaction, f'❌ Balance không đủ. Bạn có {money(balance)}.')
                return
            new_view = DiceDuelView(
                self,
                guild_id=view.guild_id,
                user_id=view.user_id,
                user_name=view.user_name,
                bet=view.bet,
            )
            if interaction.message is not None:
                await interaction.message.edit(
                    embed=self.dice_waiting_embed(user_id=view.user_id, bet=view.bet, balance=balance),
                    view=new_view,
                )
            new_view.message = interaction.message
            self._log_interaction_perf(interaction, DICE_GAME_KEY, 'play_again', started_at)

    async def play_dice_duel(self, interaction: discord.Interaction, view: DiceDuelView) -> None:
        if not await self._guild_guard(interaction, DICE_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, DICE_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._play_dice_duel_locked(interaction, view)
            self._log_interaction_perf(interaction, DICE_GAME_KEY, 'roll', started_at)

    async def _play_dice_duel_locked(self, interaction: discord.Interaction, view: DiceDuelView) -> None:
        if view.rolled:
            await self._send_error(interaction, 'Ván này đã roll rồi.')
            return
        await self._defer_component_update(interaction)
        try:
            await self._wallet_change_async(
                guild_id=view.guild_id,
                user_id=view.user_id,
                user_name=view.user_name,
                amount=-view.bet,
                tx_type='casino_dice_bet',
                description='Dice Duel bet',
                meta={'game': 'DICE_DUEL', 'bet': view.bet},
            )
        except ValueError as e:
            await self._send_error(interaction, f'❌ {e}')
            return
        view.rolled = True

        player_dice = (secrets.randbelow(6) + 1, secrets.randbelow(6) + 1)
        bot_dice = (secrets.randbelow(6) + 1, secrets.randbelow(6) + 1)
        player_total = sum(player_dice)
        bot_total = sum(bot_dice)
        if player_total > bot_total:
            result = 'WIN'
            payout = view.bet * 2
        elif player_total == bot_total:
            result = 'DRAW'
            payout = view.bet
        else:
            result = 'LOSE'
            payout = 0

        if payout:
            change = await self._wallet_change_async(
                guild_id=view.guild_id,
                user_id=view.user_id,
                user_name=view.user_name,
                amount=payout,
                tx_type='casino_dice_payout',
                description=f'Dice Duel payout: {result}',
                meta={'game': 'DICE_DUEL', 'payout': payout, 'round_result': result},
            )
            balance = int(change['balance'])
        else:
            balance = (await self._ensure_wallet_async(view.guild_id, view.user_id, view.user_name))['balance']

        profit = payout - view.bet
        await self._record_game_history_async(
            guild_id=view.guild_id,
            user_id=view.user_id,
            game_type='DICE_DUEL',
            bet_amount=view.bet,
            result=result,
            profit=profit,
            metadata={
                'player_dice': player_dice,
                'bot_dice': bot_dice,
                'player_total': player_total,
                'bot_total': bot_total,
            },
        )

        next_view = DicePlayAgainView(
            self,
            guild_id=view.guild_id,
            user_id=view.user_id,
            user_name=view.user_name,
            bet=view.bet,
        )
        if interaction.message is not None:
            await interaction.message.edit(
                embed=self.dice_result_embed(
                    user_id=view.user_id,
                    bet=view.bet,
                    player_dice=player_dice,
                    bot_dice=bot_dice,
                    result=result,
                    profit=profit,
                    balance=balance,
                ),
                view=next_view,
            )
        next_view.message = interaction.message

    def hilo_embed(
        self,
        session: HiLoSession,
        *,
        status: str = 'playing',
        last_guess: str | None = None,
        next_number: int | None = None,
        label: str | None = None,
        payout: int = 0,
        balance: int | None = None,
    ) -> discord.Embed:
        if status == 'lost':
            color = RED
            title = '📈📉 HI-LO RESULT'
        elif status in {'cashed', 'max_win'}:
            color = GREEN
            title = '📈📉 HI-LO CASH OUT'
        elif status == 'timeout':
            color = GREY
            title = '📈📉 HI-LO TIMEOUT'
        elif session.awaiting_continue:
            color = GOLD
            title = '📈📉 HI-LO - WIN STREAK'
        else:
            color = BLUE
            title = '📈📉 HI-LO'

        embed = discord.Embed(
            title=title,
            description=f'👤 Player: <@{session.user_id}>\n💰 Bet: `{money(session.bet)}`',
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name='Current Number', value=f'**{session.current_number}**', inline=True)
        embed.add_field(name='Round', value=f'`{session.round_number}/5`', inline=True)
        embed.add_field(name='Multiplier', value=f'`x{session.current_multiplier:g}`', inline=True)
        if last_guess and next_number is not None:
            embed.add_field(
                name='Last Flip',
                value=f'Bạn chọn **{last_guess.title()}** · Số mới: **{next_number}**',
                inline=False,
            )
        if session.finished:
            profit = payout - session.bet
            sign = '+' if profit > 0 else ''
            embed.add_field(name='Result', value=f'**{label or "Kết thúc"}** `{sign}{money(profit)}`', inline=False)
            if balance is not None:
                embed.add_field(name='New Balance', value=f'`{money(balance)}`', inline=False)
        elif session.awaiting_continue:
            cashout = int(round(session.bet * session.current_multiplier))
            embed.add_field(
                name='Bạn đang thắng',
                value=f'Cash Out ngay để nhận `{money(cashout)}` hoặc Continue lên `x{session.next_multiplier:g}`.',
                inline=False,
            )
        else:
            embed.add_field(
                name='Choose',
                value=f'Số tiếp theo sẽ **Higher** hay **Lower**? Round thắng kế tiếp trả `x{session.next_multiplier:g}`.',
                inline=False,
            )
        embed.set_footer(text='Bằng nhau tính là thua. Timeout 60s. Virtual coins only.')
        return embed

    async def start_hilo_message(self, message: discord.Message, raw_bet: str | None) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, HILO_GAME_KEY):
            return
        bet, error = self._parse_bet(raw_bet)
        if error:
            await message.channel.send(f'❌ {error}')
            return
        guild_id = int(message.guild.id)
        channel_id = int(message.channel.id)
        user_id = int(message.author.id)
        user_name = display_name(message.author)
        key = (guild_id, user_id)
        if key in self.hilo_sessions:
            await message.channel.send('❌ Bạn đang có một ván Hi-Lo chưa kết thúc.')
            return
        try:
            self._wallet_change(
                guild_id=guild_id,
                user_id=user_id,
                user_name=user_name,
                amount=-int(bet),
                tx_type='casino_hilo_bet',
                description='Hi-Lo bet',
                meta={'game': 'HI_LO', 'bet': int(bet)},
            )
        except ValueError as e:
            await message.channel.send(f'❌ {e}')
            return

        session = HiLoSession(
            guild_id=guild_id,
            channel_id=channel_id,
            user_id=user_id,
            user_name=user_name,
            bet=int(bet),
            current_number=secrets.randbelow(100) + 1,
        )
        self.hilo_sessions[key] = session
        view = HiLoView(self, session)
        sent = await message.channel.send(embed=self.hilo_embed(session), view=view)
        session.message_id = sent.id
        view.message = sent

    @commands.command(name='hilo')
    async def hilo_prefix(self, ctx: commands.Context, bet: str = None):
        await self.start_hilo_message(ctx.message, bet)

    def finish_hilo(self, session: HiLoSession, *, result: str, label: str, payout: int) -> dict:
        if session.finished:
            balance = self._ensure_wallet(session.guild_id, session.user_id, session.user_name)['balance']
            return {'balance': balance, 'profit': payout - session.bet}
        balance = self._ensure_wallet(session.guild_id, session.user_id, session.user_name)['balance']
        if payout > 0:
            change = self._wallet_change(
                guild_id=session.guild_id,
                user_id=session.user_id,
                user_name=session.user_name,
                amount=payout,
                tx_type='casino_hilo_payout',
                description=f'Hi-Lo payout: {label}',
                meta={'game': 'HI_LO', 'payout': payout, 'round_result': result},
            )
            balance = int(change['balance'])
        session.finished = True
        self.hilo_sessions.pop((session.guild_id, session.user_id), None)
        profit = int(payout) - int(session.bet)
        self.record_game_history(
            guild_id=session.guild_id,
            user_id=session.user_id,
            game_type='HI_LO',
            bet_amount=session.bet,
            result=result,
            profit=profit,
            metadata={
                'label': label,
                'rounds_won': session.round_number,
                'multiplier': session.current_multiplier,
                'history': session.history,
            },
        )
        return {'balance': balance, 'profit': profit}

    async def hilo_guess(self, interaction: discord.Interaction, session: HiLoSession, guess: str) -> None:
        if not await self._guild_guard(interaction, HILO_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, HILO_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._hilo_guess_locked(interaction, session, guess)
            self._log_interaction_perf(interaction, HILO_GAME_KEY, f'guess_{guess.lower()}', started_at)

    async def _hilo_guess_locked(self, interaction: discord.Interaction, session: HiLoSession, guess: str) -> None:
        if session.finished:
            await self._send_error(interaction, 'Ván này đã kết thúc.')
            return
        if session.awaiting_continue:
            await self._send_error(interaction, 'Hãy Cash Out hoặc Continue trước.')
            return
        await self._defer_component_update(interaction)

        old_number = session.current_number
        next_number = secrets.randbelow(100) + 1
        won = (guess == 'HIGHER' and next_number > old_number) or (guess == 'LOWER' and next_number < old_number)
        session.history.append({'round': session.round_number + 1, 'from': old_number, 'to': next_number, 'guess': guess})
        session.current_number = next_number

        if not won:
            result = self.finish_hilo(session, result='LOSE', label='Đoán sai, mất cược', payout=0)
            view = HiLoView(self, session)
            view.disable_all()
            if interaction.message is not None:
                await interaction.message.edit(
                    embed=self.hilo_embed(
                        session,
                        status='lost',
                        last_guess=guess.lower(),
                        next_number=next_number,
                        label='Đoán sai, mất cược',
                        payout=0,
                        balance=result['balance'],
                    ),
                    view=view,
                )
            view.message = interaction.message
            return

        session.round_number += 1
        if session.round_number >= len(HILO_MULTIPLIERS):
            payout = int(round(session.bet * session.current_multiplier))
            result = self.finish_hilo(session, result='WIN', label='Thắng đủ 5 round', payout=payout)
            view = HiLoView(self, session)
            view.disable_all()
            if interaction.message is not None:
                await interaction.message.edit(
                    embed=self.hilo_embed(
                        session,
                        status='max_win',
                        last_guess=guess.lower(),
                        next_number=next_number,
                        label='Thắng đủ 5 round',
                        payout=payout,
                        balance=result['balance'],
                    ),
                    view=view,
                )
            view.message = interaction.message
            return

        session.awaiting_continue = True
        view = HiLoView(self, session)
        if interaction.message is not None:
            await interaction.message.edit(
                embed=self.hilo_embed(session, last_guess=guess.lower(), next_number=next_number),
                view=view,
            )
        view.message = interaction.message

    async def hilo_cashout(self, interaction: discord.Interaction, session: HiLoSession) -> None:
        if not await self._guild_guard(interaction, HILO_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, HILO_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._hilo_cashout_locked(interaction, session)
            self._log_interaction_perf(interaction, HILO_GAME_KEY, 'cashout', started_at)

    async def _hilo_cashout_locked(self, interaction: discord.Interaction, session: HiLoSession) -> None:
        if session.finished:
            await self._send_error(interaction, 'Ván này đã kết thúc.')
            return
        if not session.awaiting_continue or session.round_number <= 0:
            await self._send_error(interaction, 'Bạn cần thắng ít nhất 1 round để Cash Out.')
            return
        await self._defer_component_update(interaction)
        payout = int(round(session.bet * session.current_multiplier))
        result = self.finish_hilo(session, result='WIN', label='Cash Out thành công', payout=payout)
        view = HiLoView(self, session)
        view.disable_all()
        if interaction.message is not None:
            await interaction.message.edit(
                embed=self.hilo_embed(
                    session,
                    status='cashed',
                    label='Cash Out thành công',
                    payout=payout,
                    balance=result['balance'],
                ),
                view=view,
            )
        view.message = interaction.message

    async def hilo_continue(self, interaction: discord.Interaction, session: HiLoSession) -> None:
        if not await self._guild_guard(interaction, HILO_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, HILO_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._hilo_continue_locked(interaction, session)
            self._log_interaction_perf(interaction, HILO_GAME_KEY, 'continue', started_at)

    async def _hilo_continue_locked(self, interaction: discord.Interaction, session: HiLoSession) -> None:
        if session.finished:
            await self._send_error(interaction, 'Ván này đã kết thúc.')
            return
        if not session.awaiting_continue:
            await self._send_error(interaction, 'Bạn đang ở lượt đoán Higher/Lower.')
            return
        await self._defer_component_update(interaction)
        session.awaiting_continue = False
        view = HiLoView(self, session)
        if interaction.message is not None:
            await interaction.message.edit(embed=self.hilo_embed(session), view=view)
        view.message = interaction.message

    @staticmethod
    def random_daily_reward() -> int:
        return DAILY_REWARD_MIN + secrets.randbelow(DAILY_REWARD_MAX - DAILY_REWARD_MIN + 1)

    def claim_daily_reward(self, *, guild_id: int, user_id: int, user_name: str, reward: int | None = None) -> dict:
        reward_amount = int(reward or self.random_daily_reward())
        repository = getattr(self.bot.study_context, 'repository', None)
        if repository and hasattr(repository, 'claim_casino_daily'):
            return repository.claim_casino_daily(
                guild_id=guild_id,
                user_id=user_id,
                display_name=user_name,
                reward=reward_amount,
                cooldown_seconds=int(DAILY_COOLDOWN.total_seconds()),
            )

        now = datetime.now(timezone.utc)

        def mutator(data: dict):
            account = ensure_wallet(data, user_id, user_name)
            last_at = parse_dt(account.get('casino_last_daily_at'))
            if last_at and now - last_at < DAILY_COOLDOWN:
                remaining = DAILY_COOLDOWN - (now - last_at)
                total_seconds = max(1, int(remaining.total_seconds()))
                hours, rem = divmod(total_seconds, 3600)
                minutes, seconds = divmod(rem, 60)
                return {
                    'ok': False,
                    'error': f'Bạn đã nhận daily rồi. Còn {hours:02d}:{minutes:02d}:{seconds:02d}.',
                    'balance': int(account.get('balance') or 0),
                }
            account['balance'] = int(account.get('balance') or 0) + reward_amount
            account['total_earned'] = int(account.get('total_earned') or 0) + reward_amount
            today = now.date().isoformat()
            account.setdefault('daily_earnings', {})
            account['daily_earnings'][today] = int(account['daily_earnings'].get(today) or 0) + reward_amount
            account['casino_last_daily_at'] = now.isoformat(timespec='seconds')
            append_tx(account, 'casino_daily', reward_amount, 'Casino daily reward')
            return {'ok': True, 'balance': int(account['balance']), 'reward': reward_amount}

        result, _ = self.bot.study_context.update_data(mutator, guild_id)
        return result

    @staticmethod
    def daily_embed(balance: int, reward: int) -> discord.Embed:
        return discord.Embed(
            title='💰 Daily Reward',
            description=f'Bạn nhận được **{money(reward)}**.\nBalance mới: `{money(balance)}`',
            color=GREEN,
        )

    async def start_daily_message(self, message: discord.Message) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_any_game_channel_guard(ctx):
            return
        result = self.claim_daily_reward(
            guild_id=int(message.guild.id),
            user_id=int(message.author.id),
            user_name=display_name(message.author),
        )
        if not result.get('ok'):
            await message.channel.send(f'⏳ {result["error"]}')
            return
        await message.channel.send(embed=self.daily_embed(int(result['balance']), int(result.get('reward') or 0)))

    @commands.command(name='daily')
    async def daily_prefix(self, ctx: commands.Context):
        await self.start_daily_message(ctx.message)

    @app_commands.command(name='daily', description='Nhận random 1,000-5,000 coins mỗi 24 giờ')
    async def daily(self, interaction: discord.Interaction):
        if not await self._any_game_channel_guard(interaction):
            return

        result = self.claim_daily_reward(
            guild_id=int(interaction.guild_id),
            user_id=int(interaction.user.id),
            user_name=display_name(interaction.user),
        )
        if not result.get('ok'):
            await interaction.response.send_message(f'⏳ {result["error"]}', ephemeral=True)
            return
        await interaction.response.send_message(
            embed=self.daily_embed(int(result['balance']), int(result.get('reward') or 0)),
            ephemeral=True,
        )

    @app_commands.command(name='blackjack', description='Chơi Xì Dách / Blackjack bằng coins')
    @app_commands.describe(bet='Số coins muốn cược')
    @app_commands.checks.cooldown(1, BLACKJACK_COOLDOWN_SECONDS)
    async def blackjack(self, interaction: discord.Interaction, bet: app_commands.Range[int, MIN_BET, MAX_BET]):
        await self.start_blackjack(interaction, int(bet))

    @app_commands.command(name='xidach', description='Alias của /blackjack')
    @app_commands.describe(bet='Số coins muốn cược')
    @app_commands.checks.cooldown(1, BLACKJACK_COOLDOWN_SECONDS)
    async def xidach(self, interaction: discord.Interaction, bet: app_commands.Range[int, MIN_BET, MAX_BET]):
        await self.start_blackjack(interaction, int(bet))

    async def start_blackjack(self, interaction: discord.Interaction, bet: int):
        if not await self._guild_guard(interaction, BLACKJACK_GAME_KEY):
            return
        guild_id = int(interaction.guild_id)
        user_id = int(interaction.user.id)
        key = (guild_id, user_id)
        if key in self.blackjack_sessions:
            await interaction.response.send_message('Bạn đã có một ván Blackjack đang chạy.', ephemeral=True)
            return

        await interaction.response.defer()
        user_name = display_name(interaction.user)
        try:
            await self._wallet_change_async(
                guild_id=guild_id,
                user_id=user_id,
                user_name=user_name,
                amount=-bet,
                tx_type='casino_blackjack_bet',
                description='Blackjack bet',
                meta={'game': 'BLACKJACK', 'bet': bet},
            )
        except ValueError as e:
            await interaction.followup.send(f'❌ {e}', ephemeral=True)
            return

        deck = make_deck()
        player = [deck.pop(), deck.pop()]
        dealer = [deck.pop(), deck.pop()]
        session = BlackjackSession(
            guild_id=guild_id,
            user_id=user_id,
            user_name=user_name,
            bet=bet,
            total_bet=bet,
            deck=deck,
            player=player,
            dealer=dealer,
        )

        if is_xibang(player) or is_blackjack(player):
            result = self.finish_blackjack(session, reason='instant')
            await interaction.followup.send(**await self._blackjack_payload_async(session, result=result))
            return

        self.blackjack_sessions[key] = session
        msg = await interaction.followup.send(
            **await self._blackjack_payload_async(session),
            view=BlackjackView(self, session),
            wait=True,
        )
        try:
            session.message_id = msg.id
            self._register_panel(
                game_key=BLACKJACK_GAME_KEY,
                guild_id=guild_id,
                channel_id=msg.channel.id,
                message_id=msg.id,
                expires_at=session.created_at + timedelta(seconds=BLACKJACK_PANEL_SECONDS),
            )
            await self._add_reactions(msg, list(BLACKJACK_REACTIONS.keys()))
        except discord.HTTPException:
            pass

    async def start_blackjack_message(self, message: discord.Message, raw_bet: str | None) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, BLACKJACK_GAME_KEY):
            return
        bet, error = self._parse_bet(raw_bet)
        if error:
            await message.channel.send(f'❌ {error}')
            return
        guild_id = int(message.guild.id)
        user_id = int(message.author.id)
        key = (guild_id, user_id)
        if key in self.blackjack_sessions:
            await message.channel.send('❌ Bạn đã có một ván Blackjack đang chạy.')
            return

        user_name = display_name(message.author)
        try:
            self._wallet_change(
                guild_id=guild_id,
                user_id=user_id,
                user_name=user_name,
                amount=-int(bet),
                tx_type='casino_blackjack_bet',
                description='Blackjack bet',
                meta={'game': 'BLACKJACK', 'bet': int(bet)},
            )
        except ValueError as e:
            await message.channel.send(f'❌ {e}')
            return

        deck = make_deck()
        player = [deck.pop(), deck.pop()]
        dealer = [deck.pop(), deck.pop()]
        session = BlackjackSession(
            guild_id=guild_id,
            user_id=user_id,
            user_name=user_name,
            bet=int(bet),
            total_bet=int(bet),
            deck=deck,
            player=player,
            dealer=dealer,
        )
        if is_xibang(player) or is_blackjack(player):
            result = self.finish_blackjack(session, reason='instant')
            await message.channel.send(**self._blackjack_payload(session, result=result))
            return

        self.blackjack_sessions[key] = session
        sent = await message.channel.send(**self._blackjack_payload(session), view=BlackjackView(self, session))
        session.message_id = sent.id
        self._register_panel(
            game_key=BLACKJACK_GAME_KEY,
            guild_id=guild_id,
            channel_id=sent.channel.id,
            message_id=sent.id,
            expires_at=session.created_at + timedelta(seconds=BLACKJACK_PANEL_SECONDS),
        )
        await self._add_reactions(sent, list(BLACKJACK_REACTIONS.keys()))

    @commands.command(name='blackjack', aliases=['xidach'])
    async def blackjack_prefix(self, ctx: commands.Context, bet: str = None):
        await self.start_blackjack_message(ctx.message, bet)

    @staticmethod
    def _blackjack_font(size: int, *, bold: bool = False):
        return casino_font(size, bold=bold)

    def _blackjack_card_image(self, card: tuple[str, str], size: tuple[int, int]):
        if Image is None or ImageDraw is None:
            return None
        image = cached_resized_asset(CARD_ASSET_DIR / card_asset_name(card), size)
        if image is not None:
            return image

        width, height = size
        rank, suit = card
        red = suit in {'♥', '♦'}
        ink = (190, 18, 28) if red else (16, 18, 22)
        card_image = Image.new('RGBA', size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(card_image)
        draw.rounded_rectangle(
            (0, 0, width - 1, height - 1),
            radius=12,
            fill=(250, 250, 248),
            outline=(20, 20, 20),
            width=2,
        )
        draw.text((12, 10), rank, fill=ink, font=self._blackjack_font(30, bold=True))
        draw.text((14, 44), suit, fill=ink, font=self._blackjack_font(24, bold=True))
        draw.text((width // 2, height // 2), suit, fill=ink, font=self._blackjack_font(58, bold=True), anchor='mm')
        draw.text((width - 12, height - 10), rank, fill=ink, font=self._blackjack_font(30, bold=True), anchor='rd')
        draw.text((width - 14, height - 44), suit, fill=ink, font=self._blackjack_font(24, bold=True), anchor='rd')
        return card_image

    def _blackjack_card_back(self, size: tuple[int, int]):
        if Image is None or ImageDraw is None:
            return None
        image = cached_resized_asset(CARD_ASSET_DIR / CARD_BACK_NAME, size)
        if image is not None:
            return image

        width, height = size
        card = Image.new('RGBA', size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(card)
        draw.rounded_rectangle((0, 0, width - 1, height - 1), radius=12, fill=(248, 248, 248), outline=(20, 20, 20), width=2)
        draw.rounded_rectangle((12, 12, width - 13, height - 13), radius=8, fill=(151, 32, 44), outline=(255, 230, 230), width=3)
        for x in range(-height, width, 18):
            draw.line((x, height - 12, x + height, 12), fill=(111, 22, 36), width=2)
            draw.line((x, 12, x + height, height - 12), fill=(184, 55, 66), width=2)
        draw.text(
            (width // 2, height // 2),
            'BJ',
            fill=(255, 244, 220),
            font=self._blackjack_font(32, bold=True),
            anchor='mm',
        )
        return card

    def _blackjack_image_file(
        self,
        session: BlackjackSession,
        *,
        reveal_dealer: bool,
        result: dict | None = None,
    ) -> discord.File | None:
        if Image is None or ImageDraw is None:
            return None

        visible_dealer = session.dealer if reveal_dealer else [session.dealer[0], None]

        card_size = (126, 182)
        gap = 16
        row_width = lambda count: (count * card_size[0]) + max(0, count - 1) * gap
        table_width = max(760, row_width(max(len(session.player), len(visible_dealer))) + 120)
        table_height = 560
        image = Image.new('RGB', (table_width, table_height), (18, 96, 62))
        draw = ImageDraw.Draw(image)

        draw.rounded_rectangle((24, 24, table_width - 24, table_height - 24), radius=24, outline=(235, 206, 130), width=4)
        draw.rounded_rectangle((42, 42, table_width - 42, table_height - 42), radius=20, outline=(39, 122, 81), width=2)
        draw.text(
            (table_width // 2, 58),
            'BLACKJACK',
            fill=(255, 235, 174),
            font=self._blackjack_font(36, bold=True),
            anchor='mm',
        )

        if result:
            result_line = f'{result["label"]} | {money(result["profit"])}'
            draw.text(
                (table_width // 2, 100),
                result_line,
                fill=(245, 245, 245),
                font=self._blackjack_font(18, bold=True),
                anchor='mm',
            )

        def paste_row(cards: list[tuple[str, str] | None], y: int, label: str, total: str) -> None:
            draw.text((62, y + 16), label, fill=(230, 230, 230), font=self._blackjack_font(20, bold=True))
            draw.text((62, y + 44), total, fill=(255, 235, 174), font=self._blackjack_font(18, bold=True))
            x = (table_width - row_width(len(cards))) // 2
            for index, card in enumerate(cards):
                card_image = self._blackjack_card_back(card_size) if card is None else self._blackjack_card_image(card, card_size)
                if card_image is None:
                    continue
                image.paste(card_image, (x + index * (card_size[0] + gap), y), card_image)

        dealer_total = f'Total: {hand_total(session.dealer)}' if reveal_dealer else 'Total: ?'
        paste_row(visible_dealer, 122, 'DEALER', dealer_total)
        paste_row(session.player, 336, 'PLAYER', f'Total: {hand_total(session.player)}')

        buffer = io.BytesIO()
        image.save(buffer, format='PNG', optimize=False, compress_level=1)
        buffer.seek(0)
        state = 'result' if reveal_dealer else 'play'
        filename = f'blackjack_{session.user_id}_{state}_{len(session.player)}_{len(session.dealer)}.png'
        return discord.File(buffer, filename=filename)

    async def _blackjack_payload_async(self, session: BlackjackSession, *, result: dict | None = None) -> dict:
        return await asyncio.to_thread(self._blackjack_payload, session, result=result)

    def _blackjack_payload(self, session: BlackjackSession, *, result: dict | None = None) -> dict:
        if result:
            embed = self.blackjack_result_embed(session, result)
            file = self._blackjack_image_file(session, reveal_dealer=True, result=result)
        else:
            embed = self.blackjack_play_embed(session)
            file = self._blackjack_image_file(session, reveal_dealer=False)
        if file:
            embed.set_image(url=f'attachment://{file.filename}')
            return {'embed': embed, 'file': file}
        return {'embed': embed}

    def blackjack_play_embed(self, session: BlackjackSession) -> discord.Embed:
        embed = discord.Embed(
            title='🎴 XÌ DÁCH / BLACKJACK',
            description=f'👤 Player: <@{session.user_id}>\n💰 Bet: `{money(session.total_bet)}`',
            color=BLUE,
        )
        embed.add_field(
            name='Your Hand',
            value=f'{hand_text(session.player)}\nTotal: `{hand_total(session.player)}`',
            inline=False,
        )
        embed.add_field(
            name='Dealer Hand',
            value=f'{card_text(session.dealer[0])} ❓\nTotal: `?`',
            inline=False,
        )
        embed.set_footer(text='React: 🃏 Hit · ✋ Stand · 2️⃣ Double · 🏳️ Surrender')
        return embed

    def blackjack_result_embed(self, session: BlackjackSession, result: dict) -> discord.Embed:
        color = GREEN if result['result'] == 'WIN' else RED if result['result'] == 'LOSE' else GREY
        embed = discord.Embed(title='🎴 BLACKJACK RESULT', color=color)
        embed.add_field(
            name='Your Hand',
            value=f'{hand_text(session.player)}\nTotal: `{hand_total(session.player)}`',
            inline=False,
        )
        embed.add_field(
            name='Dealer Hand',
            value=f'{hand_text(session.dealer)}\nTotal: `{hand_total(session.dealer)}`',
            inline=False,
        )
        net = int(result['profit'])
        if result['result'] == 'WIN':
            result_text = f'✅ {result["label"]} `+{money(net)}`'
        elif result['result'] == 'DRAW':
            result_text = f'⚪ {result["label"]} `{money(0)}`'
        else:
            result_text = f'❌ {result["label"]} `{money(net)}`'
        embed.add_field(name='Result', value=result_text, inline=False)
        embed.add_field(name='New Balance', value=f'`{money(result["balance"])}`', inline=False)
        embed.set_footer(text='Virtual coins only. No real money gambling.')
        return embed

    def finish_blackjack(self, session: BlackjackSession, *, reason: str) -> dict:
        if session.finished:
            raise ValueError('Ván này đã kết thúc.')

        result = 'LOSE'
        label = 'Bạn thua'
        payout = 0

        if reason == 'surrender':
            payout = session.bet // 2
            label = f'Surrender, nhận lại {money(payout)}'
        elif hand_total(session.player) > 21:
            label = 'Quắc / bust'
        elif is_xibang(session.player):
            result = 'WIN'
            payout = session.bet * 3
            label = 'Xì Bàng'
        elif is_ngulinh(session.player):
            result = 'WIN'
            payout = session.total_bet * 3
            label = 'Ngũ Linh'
        else:
            while hand_total(session.dealer) < 17:
                session.dealer.append(session.deck.pop())

            player_total = hand_total(session.player)
            dealer_total = hand_total(session.dealer)
            dealer_bust = dealer_total > 21
            if is_blackjack(session.player) and not is_blackjack(session.dealer):
                result = 'WIN'
                payout = session.bet + int(session.bet * 1.5)
                label = 'Blackjack'
            elif is_blackjack(session.player) and is_blackjack(session.dealer):
                result = 'DRAW'
                payout = session.bet
                label = 'Push'
            elif dealer_bust or player_total > dealer_total:
                result = 'WIN'
                payout = session.total_bet * 2
                label = 'Bạn thắng'
            elif player_total == dealer_total:
                result = 'DRAW'
                payout = session.total_bet
                label = 'Hòa / push'
            else:
                label = 'Dealer thắng'

        balance = None
        if payout > 0:
            change = self._wallet_change(
                guild_id=session.guild_id,
                user_id=session.user_id,
                user_name=session.user_name,
                amount=payout,
                tx_type='casino_blackjack_payout',
                description=f'Blackjack payout: {label}',
                meta={'game': 'BLACKJACK', 'payout': payout, 'round_result': result},
            )
            balance = int(change['balance'])
        else:
            balance = self._ensure_wallet(session.guild_id, session.user_id, session.user_name)['balance']

        session.finished = True
        self.blackjack_sessions.pop((session.guild_id, session.user_id), None)
        profit = int(payout) - int(session.total_bet)
        self.record_game_history(
            guild_id=session.guild_id,
            user_id=session.user_id,
            game_type='BLACKJACK',
            bet_amount=session.total_bet,
            result=result,
            profit=profit,
            metadata={
                'label': label,
                'player': [card_text(card) for card in session.player],
                'dealer': [card_text(card) for card in session.dealer],
                'player_total': hand_total(session.player),
                'dealer_total': hand_total(session.dealer),
            },
        )
        return {'result': result, 'label': label, 'payout': payout, 'profit': profit, 'balance': balance}

    def _blackjack_session_for_message(self, message_id: int) -> BlackjackSession | None:
        for session in self.blackjack_sessions.values():
            if session.message_id == int(message_id):
                return session
        return None

    async def _edit_blackjack_panel(self, message: discord.Message, session: BlackjackSession, *, result: dict | None = None) -> None:
        payload = await self._blackjack_payload_async(session, result=result)
        file = payload.pop('file', None)
        if file:
            payload['attachments'] = [file]
        else:
            payload['attachments'] = []
        if result:
            await message.edit(**payload, view=None)
            await self._clear_game_reactions(message)
            self._unregister_panel(session.message_id)
            return
        await message.edit(**payload, view=BlackjackView(self, session))

    async def handle_blackjack_action(self, message: discord.Message, user_id: int, action: str) -> bool:
        session = self._blackjack_session_for_message(message.id)
        if not session or session.finished:
            return False
        if user_id != session.user_id:
            return False
        if not self._is_game_channel(session.guild_id, message.channel.id, BLACKJACK_GAME_KEY):
            return False

        if action == 'hit':
            session.player.append(session.deck.pop())
            if hand_total(session.player) > 21 or is_ngulinh(session.player):
                result = self.finish_blackjack(session, reason='hit')
                await self._edit_blackjack_panel(message, session, result=result)
            else:
                await self._edit_blackjack_panel(message, session)
            return True

        if action == 'stand':
            result = self.finish_blackjack(session, reason='stand')
            await self._edit_blackjack_panel(message, session, result=result)
            return True

        if action == 'double':
            if not session.can_double:
                return False
            try:
                self._wallet_change(
                    guild_id=session.guild_id,
                    user_id=session.user_id,
                    user_name=session.user_name,
                    amount=-session.bet,
                    tx_type='casino_blackjack_double',
                    description='Blackjack double bet',
                    meta={'game': 'BLACKJACK', 'bet': session.bet},
                )
            except ValueError:
                return False
            session.total_bet += session.bet
            session.doubled = True
            session.player.append(session.deck.pop())
            result = self.finish_blackjack(session, reason='double')
            await self._edit_blackjack_panel(message, session, result=result)
            return True

        if action == 'surrender':
            if not session.can_surrender:
                return False
            result = self.finish_blackjack(session, reason='surrender')
            await self._edit_blackjack_panel(message, session, result=result)
            return True

        return False

    async def blackjack_hit(self, interaction: discord.Interaction, session: BlackjackSession):
        if not await self._guild_guard(interaction, BLACKJACK_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, BLACKJACK_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._blackjack_hit_locked(interaction, session)
            self._log_interaction_perf(interaction, BLACKJACK_GAME_KEY, 'hit', started_at)

    async def _blackjack_hit_locked(self, interaction: discord.Interaction, session: BlackjackSession):
        if session.finished:
            await self._send_error(interaction, 'Ván này đã kết thúc.')
            return
        await interaction.response.defer()
        await self.handle_blackjack_action(interaction.message, interaction.user.id, 'hit')

    async def blackjack_stand(self, interaction: discord.Interaction, session: BlackjackSession):
        if not await self._guild_guard(interaction, BLACKJACK_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, BLACKJACK_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._blackjack_stand_locked(interaction, session)
            self._log_interaction_perf(interaction, BLACKJACK_GAME_KEY, 'stand', started_at)

    async def _blackjack_stand_locked(self, interaction: discord.Interaction, session: BlackjackSession):
        if session.finished:
            await self._send_error(interaction, 'Ván này đã kết thúc.')
            return
        await interaction.response.defer()
        await self.handle_blackjack_action(interaction.message, interaction.user.id, 'stand')

    async def blackjack_double(self, interaction: discord.Interaction, session: BlackjackSession):
        if not await self._guild_guard(interaction, BLACKJACK_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, BLACKJACK_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._blackjack_double_locked(interaction, session)
            self._log_interaction_perf(interaction, BLACKJACK_GAME_KEY, 'double', started_at)

    async def _blackjack_double_locked(self, interaction: discord.Interaction, session: BlackjackSession):
        if not session.can_double:
            await self._send_error(interaction, 'Double chỉ dùng được ngay lượt đầu.')
            return
        await interaction.response.defer()
        handled = await self.handle_blackjack_action(interaction.message, interaction.user.id, 'double')
        if not handled:
            await self._send_error(interaction, 'Không thể double lúc này.')

    async def blackjack_surrender(self, interaction: discord.Interaction, session: BlackjackSession):
        if not await self._guild_guard(interaction, BLACKJACK_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, BLACKJACK_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._blackjack_surrender_locked(interaction, session)
            self._log_interaction_perf(interaction, BLACKJACK_GAME_KEY, 'surrender', started_at)

    async def _blackjack_surrender_locked(self, interaction: discord.Interaction, session: BlackjackSession):
        if not session.can_surrender:
            await self._send_error(interaction, 'Surrender chỉ dùng được ngay lượt đầu.')
            return
        await interaction.response.defer()
        await self.handle_blackjack_action(interaction.message, interaction.user.id, 'surrender')

    def _taixiu_base_board_image(self):
        if Image is None:
            return None

        width, height = TAIXIU_BOARD_SIZE
        if self._taixiu_base_image is not None:
            return self._taixiu_base_image.copy()

        scale = 8
        base_width, base_height = width // scale, height // scale
        image = Image.new('RGBA', (base_width, base_height), (7, 5, 4, 255))
        pixels = image.load()
        for y in range(base_height):
            source_y = y * scale
            heat = int(28 * (1 - source_y / height))
            for x in range(base_width):
                source_x = x * scale
                distance = abs(source_x - width / 2) / (width / 2)
                glow = max(0, int(38 * (1 - distance) * (1 - abs(source_y - 360) / 420)))
                pixels[x, y] = (9 + glow + heat // 2, 6 + heat // 4, 4, 255)
        resampling = getattr(getattr(Image, 'Resampling', Image), 'BILINEAR')
        image = image.resize((width, height), resampling)
        self._taixiu_base_image = image
        return image.copy()

    def _taixiu_board_image(self, session: TaixiuSession):
        image = self._taixiu_base_board_image()
        if image is None:
            return None

        width, height = TAIXIU_BOARD_SIZE
        draw = ImageDraw.Draw(image, 'RGBA')
        gold = (236, 186, 84, 255)
        pale_gold = (255, 226, 151, 255)
        deep_gold = (126, 82, 28, 255)
        teal = (8, 67, 61, 235)
        red = (99, 20, 24, 235)
        black = (7, 10, 10, 240)
        white = (246, 241, 230, 255)

        title_font = casino_font(72, bold=True, serif=True)
        sub_font = casino_font(30, bold=True)
        body_font = casino_font(28, bold=True)
        small_font = casino_font(22, bold=True)
        panel_title_font = casino_font(84, bold=True, serif=True)
        number_font = casino_font(48, bold=True)
        amount_font = casino_font(42, bold=True)
        chip_font = casino_font(20, bold=True)

        draw_centered_text(draw, (0, 28, width, 112), 'TÀI XỈU', title_font, pale_gold, stroke_width=3, stroke_fill=(66, 39, 9))
        draw_centered_text(draw, (0, 105, width, 145), f'#{session.round_number:06d}', sub_font, pale_gold)

        table_box = (55, 145, 1225, 625)
        draw.rounded_rectangle((table_box[0] + 8, table_box[1] + 12, table_box[2] + 8, table_box[3] + 12), radius=70, fill=(0, 0, 0, 150))
        draw.rounded_rectangle(table_box, radius=70, fill=(16, 13, 10, 245), outline=gold, width=5)
        draw.rounded_rectangle((75, 165, 1205, 605), radius=58, outline=deep_gold, width=3)

        left_box = (90, 178, 580, 585)
        right_box = (700, 178, 1190, 585)
        draw.rounded_rectangle(left_box, radius=48, fill=teal, outline=(189, 128, 46, 220), width=3)
        draw.rounded_rectangle(right_box, radius=48, fill=red, outline=(189, 128, 46, 220), width=3)
        for i in range(4):
            y = 232 + i * 74
            draw.line((110, y, 560, y), fill=(255, 232, 170, 35), width=1)
            draw.line((720, y, 1170, y), fill=(255, 232, 170, 35), width=1)

        tai_bets = [bet for bet in session.bets.values() if bet.choice == 'TAI']
        xiu_bets = [bet for bet in session.bets.values() if bet.choice == 'XIU']
        tai_total = sum(b.amount for b in tai_bets)
        xiu_total = sum(b.amount for b in xiu_bets)

        draw_centered_text(draw, (115, 235, 555, 330), 'TÀI', panel_title_font, white, stroke_width=3, stroke_fill=(30, 30, 30))
        draw_centered_text(draw, (725, 235, 1165, 330), 'XỈU', panel_title_font, white, stroke_width=3, stroke_fill=(30, 30, 30))
        draw_centered_text(draw, (115, 327, 555, 365), '1:1', body_font, pale_gold)
        draw_centered_text(draw, (725, 327, 1165, 365), '1:1', body_font, pale_gold)

        draw_centered_text(draw, (115, 405, 555, 455), f'{len(tai_bets):,} PLAYERS', small_font, pale_gold)
        draw_centered_text(draw, (725, 405, 1165, 455), f'{len(xiu_bets):,} PLAYERS', small_font, pale_gold)
        draw_centered_text(draw, (115, 462, 505, 525), compact_money(tai_total), amount_font, pale_gold, stroke_width=2, stroke_fill=(45, 30, 8))
        draw_centered_text(draw, (775, 462, 1165, 525), compact_money(xiu_total), amount_font, pale_gold, stroke_width=2, stroke_fill=(45, 30, 8))

        draw.rounded_rectangle((180, 535, 490, 590), radius=26, fill=(10, 105, 96, 240), outline=gold, width=3)
        draw.rounded_rectangle((790, 535, 1100, 590), radius=26, fill=(124, 18, 52, 240), outline=gold, width=3)
        draw_centered_text(draw, (180, 535, 490, 590), 'CHỌN TÀI', body_font, white, stroke_width=1, stroke_fill=(0, 0, 0))
        draw_centered_text(draw, (790, 535, 1100, 590), 'CHỌN XỈU', body_font, white, stroke_width=1, stroke_fill=(0, 0, 0))

        center = (640, 385)
        draw.ellipse((448, 193, 832, 577), fill=black, outline=gold, width=5)
        draw.ellipse((475, 220, 805, 550), outline=(87, 57, 26, 255), width=3)
        dice = session.dice if session.status == 'FINISHED' else (None, None, None)
        draw_dice(draw, (570, 300, 665, 395), dice[0])
        draw_dice(draw, (505, 385, 600, 480), dice[1])
        draw_dice(draw, (655, 385, 750, 480), dice[2])

        total = sum(session.dice or ()) if session.dice else None
        result_text = 'TÀI' if session.result == 'TAI' else 'XỈU' if session.result == 'XIU' else ''
        total_text = f'TỔNG: {total}' if total is not None else 'TỔNG: --'
        draw.rounded_rectangle((500, 515, 780, 574), radius=24, fill=(18, 14, 10, 235), outline=(107, 72, 30, 220), width=2)
        draw_centered_text(draw, (500, 512, 780, 572), total_text, body_font, white)
        if result_text:
            draw_centered_text(draw, (500, 558, 780, 603), f'KẾT QUẢ: {result_text}', small_font, pale_gold)

        timer_box = (570, 135, 710, 275)
        timer_fill = (20, 21, 18, 250) if session.status != 'LOCKED' else (72, 62, 52, 250)
        draw.ellipse(timer_box, fill=timer_fill, outline=gold, width=5)
        timer_value = str(session.seconds_left()) if session.status != 'FINISHED' else '0'
        draw_centered_text(draw, (570, 155, 710, 218), timer_value, casino_font(50, bold=True), pale_gold, stroke_width=2, stroke_fill=(46, 24, 0))
        draw_centered_text(draw, (570, 212, 710, 250), 'GIÂY', small_font, pale_gold)
        recent = self.recent_taixiu_results(session.guild_id, limit=12)
        start_x = 752
        y = 656
        for index, result in enumerate(recent):
            x = start_x + index * 39
            color = (20, 110, 82, 255) if result == 'TAI' else (129, 35, 31, 255)
            label = 'T' if result == 'TAI' else 'X'
            draw.ellipse((x, y, x + 34, y + 34), fill=color, outline=gold, width=2)
            draw_centered_text(draw, (x, y - 1, x + 34, y + 34), label, small_font, white)

        chip_y = 667
        chip_specs = [
            (70, '1K', (19, 105, 64)),
            (160, '10K', (27, 91, 136)),
            (255, '50K', (90, 38, 132)),
            (365, '100K', (155, 109, 18)),
            (485, '500K', (126, 35, 29)),
            (615, '1M', (32, 70, 68)),
        ]
        for x, label, color in chip_specs:
            draw.ellipse((x, chip_y - 35, x + 62, chip_y + 27), fill=(*color, 245), outline=gold, width=3)
            draw_centered_text(draw, (x, chip_y - 35, x + 62, chip_y + 27), label, chip_font, white)

        draw.rounded_rectangle((45, 34, 275, 113), radius=22, fill=(9, 9, 8, 205), outline=deep_gold, width=2)
        draw_centered_text(draw, (45, 38, 275, 72), 'DISCORD CASINO', small_font, pale_gold)
        draw_centered_text(draw, (45, 72, 275, 108), 'Virtual coins only', casino_font(18, bold=True), (217, 198, 149, 255))
        for x, label in ((970, 'DS'), (1045, 'TOP'), (1120, '?'), (1195, 'SET')):
            draw.ellipse((x, 42, x + 58, 100), fill=(18, 15, 11, 225), outline=gold, width=3)
            draw_centered_text(draw, (x, 40, x + 58, 100), label, casino_font(20 if len(label) > 1 else 30, bold=True), pale_gold)

        return image.convert('RGB')

    @staticmethod
    def _taixiu_board_cache_key(session: TaixiuSession) -> tuple:
        tai_bets = [bet for bet in session.bets.values() if bet.choice == 'TAI']
        xiu_bets = [bet for bet in session.bets.values() if bet.choice == 'XIU']
        return (
            session.round_id,
            session.round_number,
            session.status,
            session.seconds_left() if session.status != 'FINISHED' else 0,
            session.dice,
            session.result,
            len(tai_bets),
            sum(bet.amount for bet in tai_bets),
            len(xiu_bets),
            sum(bet.amount for bet in xiu_bets),
        )

    def taixiu_board_file(self, session: TaixiuSession) -> discord.File | None:
        if not TAIXIU_RENDER_IMAGE:
            return None
        cache_key = self._taixiu_board_cache_key(session)
        if session.board_cache_key != cache_key or session.board_cache_png is None:
            image = self._taixiu_board_image(session)
            if image is None:
                return None
            buffer = io.BytesIO()
            image.save(buffer, format='PNG')
            session.board_cache_key = cache_key
            session.board_cache_png = buffer.getvalue()
        buffer = io.BytesIO()
        buffer.write(session.board_cache_png)
        buffer.seek(0)
        return discord.File(buffer, filename=f'taixiu-board-{image_state_token(cache_key)}.png')

    def taixiu_send_payload(self, session: TaixiuSession) -> dict:
        embed = self.taixiu_embed(session)
        file = self.taixiu_board_file(session)
        if file:
            embed.set_image(url=f'attachment://{file.filename}')
            return {'embed': embed, 'file': file}
        return {'embed': embed}

    def taixiu_edit_payload(self, session: TaixiuSession) -> dict:
        embed = self.taixiu_embed(session)
        file = self.taixiu_board_file(session)
        if file:
            embed.set_image(url=f'attachment://{file.filename}')
            return {'embed': embed, 'attachments': [file]}
        return {'embed': embed, 'attachments': []}

    @app_commands.command(name='taixiu', description='Mở bàn Tài Xỉu bằng coins')
    @app_commands.checks.cooldown(1, TAIXIU_COOLDOWN_SECONDS)
    async def taixiu(self, interaction: discord.Interaction):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        guild_id = int(interaction.guild_id)
        session = await self.ensure_taixiu_session(guild_id)

        if session.message_id and session.channel_id:
            await self.edit_taixiu_board(session)
            if session.message_id and session.channel_id:
                jump = f'https://discord.com/channels/{guild_id}/{session.channel_id}/{session.message_id}'
                await interaction.response.send_message(f'Bàn Tài Xỉu đang chạy ở đây: {jump}', ephemeral=True)
                return

        await interaction.response.send_message(**self.taixiu_send_payload(session), view=TaixiuView(self, guild_id))
        try:
            msg = await interaction.original_response()
            session.channel_id = msg.channel.id
            session.message_id = msg.id
        except discord.HTTPException:
            pass

    async def start_taixiu_message(self, message: discord.Message) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, TAIXIU_GAME_KEY):
            return
        guild_id = int(message.guild.id)
        session = await self.ensure_taixiu_session(guild_id)
        if session.message_id and session.channel_id:
            await self.edit_taixiu_board(session)
            jump = f'https://discord.com/channels/{guild_id}/{session.channel_id}/{session.message_id}'
            await message.channel.send(f'Bàn Tài Xỉu đang chạy ở đây: {jump}')
            return

        sent = await message.channel.send(**self.taixiu_send_payload(session), view=TaixiuView(self, guild_id))
        session.channel_id = sent.channel.id
        session.message_id = sent.id

    @commands.command(name='taixiu')
    async def taixiu_prefix(self, ctx: commands.Context):
        await self.start_taixiu_message(ctx.message)

    async def ensure_taixiu_session(self, guild_id: int) -> TaixiuSession:
        session = self.taixiu_sessions.get(guild_id)
        if session and session.status != 'FINISHED':
            return session
        session = self.create_taixiu_round(guild_id)
        self.taixiu_sessions[guild_id] = session
        session.task = asyncio.create_task(self.run_taixiu_round(session))
        return session

    def create_taixiu_round(self, guild_id: int) -> TaixiuSession:
        created_at = datetime.now(timezone.utc)
        with self.bot.study_context.database.transaction() as conn:
            row = conn.execute(
                'SELECT COALESCE(MAX(round_number), 0) + 1 FROM casino_taixiu_rounds WHERE guild_id = ?',
                (int(guild_id),),
            ).fetchone()
            round_number = int(row[0] or 1)
            cur = conn.execute(
                """
                INSERT INTO casino_taixiu_rounds (guild_id, round_number, status, created_at)
                VALUES (?, ?, 'BETTING', ?)
                """,
                (int(guild_id), round_number, created_at.isoformat(timespec='seconds')),
            )
            round_id = int(cur.lastrowid)
        return TaixiuSession(
            guild_id=int(guild_id),
            round_id=round_id,
            round_number=round_number,
            status='BETTING',
            created_at=created_at,
            finishes_at=created_at + timedelta(seconds=TAIXIU_BETTING_SECONDS),
        )

    async def run_taixiu_round(self, session: TaixiuSession):
        try:
            while session.status == 'BETTING' and session.seconds_left() > 0:
                await self.edit_taixiu_board(session)
                await asyncio.sleep(min(5, max(1, session.seconds_left())))

            session.status = 'LOCKED'
            session.finishes_at = datetime.now(timezone.utc) + timedelta(seconds=TAIXIU_LOCKED_SECONDS)
            self.update_taixiu_round_status(session)
            await self.edit_taixiu_board(session)
            await asyncio.sleep(TAIXIU_LOCKED_SECONDS)

            await asyncio.to_thread(self.finish_taixiu_round, session)
            await self.edit_taixiu_board(session)
            await self.send_taixiu_result_notifications(session)
            if self.taixiu_sessions.get(session.guild_id) is session:
                self.taixiu_sessions.pop(session.guild_id, None)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('Tài Xỉu round failed for guild_id=%s', session.guild_id)

    def update_taixiu_round_status(self, session: TaixiuSession):
        with self.bot.study_context.database.transaction() as conn:
            conn.execute(
                'UPDATE casino_taixiu_rounds SET status = ? WHERE id = ?',
                (session.status, session.round_id),
            )

    def finish_taixiu_round(self, session: TaixiuSession):
        if session.settled or session.status == 'FINISHED':
            return
        dice = tuple(secrets.randbelow(6) + 1 for _ in range(3))
        total = sum(dice)
        result = 'TAI' if total >= 11 else 'XIU'
        session.status = 'FINISHED'
        session.dice = dice
        session.result = result
        session.finishes_at = None
        session.settled = True

        winners = [bet for bet in session.bets.values() if bet.choice == result]
        losers = [bet for bet in session.bets.values() if bet.choice != result]
        house_profit = sum(bet.amount for bet in losers) - sum(bet.amount for bet in winners)

        for bet in winners:
            payout = bet.amount * 2
            change = self._wallet_change(
                guild_id=session.guild_id,
                user_id=bet.user_id,
                user_name=bet.user_name,
                amount=payout,
                tx_type='casino_taixiu_payout',
                description=f'Tài Xỉu round #{session.round_number} payout',
                meta={'game': 'TAIXIU', 'round_number': session.round_number, 'choice': bet.choice},
            )
            session.settlements[int(bet.user_id)] = {
                'won': True,
                'profit': int(bet.amount),
                'balance': int(change.get('balance') or 0),
            }

        for bet in losers:
            wallet = self._ensure_wallet(session.guild_id, bet.user_id, bet.user_name)
            session.settlements[int(bet.user_id)] = {
                'won': False,
                'profit': -int(bet.amount),
                'balance': int(wallet.get('balance') or 0),
            }

        for bet in session.bets.values():
            won = bet.choice == result
            self.record_game_history(
                guild_id=session.guild_id,
                user_id=bet.user_id,
                game_type='TAIXIU',
                bet_amount=bet.amount,
                result='WIN' if won else 'LOSE',
                profit=bet.amount if won else -bet.amount,
                metadata={
                    'round_id': session.round_id,
                    'round_number': session.round_number,
                    'choice': bet.choice,
                    'dice': dice,
                    'total': total,
                    'result': result,
                    'house_profit': house_profit,
                },
            )

        try:
            with self.bot.study_context.database.transaction() as conn:
                conn.execute(
                    """
                    UPDATE casino_taixiu_rounds
                    SET status = 'FINISHED', dice1 = ?, dice2 = ?, dice3 = ?,
                        total = ?, result = ?, finished_at = ?
                    WHERE id = ?
                    """,
                    (*dice, total, result, now_iso(), session.round_id),
                )
        except Exception:
            log.exception('Could not persist Tài Xỉu round result for round_id=%s', session.round_id)

    async def edit_taixiu_board(self, session: TaixiuSession, message: discord.Message | None = None):
        if not session.channel_id or not session.message_id:
            return
        if not self._is_game_channel(session.guild_id, session.channel_id, TAIXIU_GAME_KEY):
            session.channel_id = None
            session.message_id = None
            return
        if message is None:
            channel = self.bot.get_channel(session.channel_id)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(session.channel_id)
                except discord.HTTPException:
                    return
            try:
                message = await channel.fetch_message(session.message_id)
            except discord.HTTPException:
                session.channel_id = None
                session.message_id = None
                return
        try:
            await message.edit(**self.taixiu_edit_payload(session), view=TaixiuView(self, session.guild_id))
            if session.status == 'FINISHED':
                self._unregister_panel(session.message_id)
                await self._clear_game_reactions(message)
        except discord.HTTPException:
            session.channel_id = None
            session.message_id = None

    def taixiu_embed(self, session: TaixiuSession) -> discord.Embed:
        if session.status == 'FINISHED':
            dice = session.dice or (0, 0, 0)
            total = sum(dice)
            result_label = '🔴 TÀI' if session.result == 'TAI' else '🔵 XỈU'
            embed = discord.Embed(
                title='🎲 TÀI XỈU CASINO',
                description=(
                    '━━━━━━━━━━━━━━━━━━━━\n'
                    f'**Round #{session.round_number:06d}** · `{dice[0]} + {dice[1]} + {dice[2]} = {total}` · **{result_label}**\n'
                    'Bàn này đã kết thúc. Gõ lệnh Tài Xỉu lần nữa để mở sàn mới.'
                ),
                color=GREEN if session.result == 'TAI' else BLUE,
            )
            winners = sum(1 for bet in session.bets.values() if bet.choice == session.result)
            losers = len(session.bets) - winners
            embed.add_field(name='Winners', value=f'{winners} players', inline=True)
            embed.add_field(name='Losers', value=f'{losers} players', inline=True)
            if session.notice:
                embed.add_field(name='Trạng thái', value=session.notice[:1024], inline=False)
            embed.set_footer(text='Virtual coins only. No real money gambling.')
            return embed

        tai_bets = [bet for bet in session.bets.values() if bet.choice == 'TAI']
        xiu_bets = [bet for bet in session.bets.values() if bet.choice == 'XIU']
        status_line = 'Đang nhận cược' if session.status == 'BETTING' else 'Đã khóa cược'
        embed = discord.Embed(
            title='🎲 TÀI XỈU CASINO',
            description=(
                '━━━━━━━━━━━━━━━━━━━━\n'
                f'**Round #{session.round_number:06d}** · **{status_line}** · `{session.seconds_left()}s`\n'
                f'Chọn chip nếu muốn đổi mức cược, rồi bấm **ĐẶT TÀI** hoặc **ĐẶT XỈU**.'
            ),
            color=BLUE if session.status == 'BETTING' else GREY,
        )
        embed.add_field(
            name='🔴 TÀI',
            value=f'Players: `{len(tai_bets)}`\nTotal bet: `{money(sum(b.amount for b in tai_bets))}`',
            inline=True,
        )
        embed.add_field(
            name='🔵 XỈU',
            value=f'Players: `{len(xiu_bets)}`\nTotal bet: `{money(sum(b.amount for b in xiu_bets))}`',
            inline=True,
        )
        embed.add_field(
            name='Chip nhanh',
            value='`1K` `10K` `50K` `100K` `500K` · **ALL-IN** tối đa `1,000,000`.',
            inline=False,
        )
        if session.notice:
            embed.add_field(name='Trạng thái', value=session.notice[:1024], inline=False)
        embed.set_footer(text='Virtual coins only. No real money gambling.')
        return embed

    def recent_taixiu_results(self, guild_id: int, *, limit: int = 20) -> list[str]:
        with self.bot.study_context.database.read_connection() as conn:
            rows = conn.execute(
                """
                SELECT result
                FROM casino_taixiu_rounds
                WHERE guild_id = ? AND status = 'FINISHED' AND result IS NOT NULL
                ORDER BY round_number DESC
                LIMIT ?
                """,
                (int(guild_id), int(limit)),
            ).fetchall()
        return [str(row['result']) for row in reversed(rows)]

    @staticmethod
    def format_recent_results(results: list[str]) -> str:
        labels = ['T' if result == 'TAI' else 'X' for result in results]
        if not labels:
            return ''
        rows = [' '.join(labels[i:i + 10]) for i in range(0, len(labels), 10)]
        return '\n'.join(rows)

    def _pending_taixiu_state(self, guild_id: int, user_id: int) -> dict[str, int | str]:
        key = (int(guild_id), int(user_id))
        state = self.pending_taixiu_bets.setdefault(key, {})
        state.setdefault('amount', self.default_taixiu_bets.get(key, MIN_BET))
        return state

    def _pending_taixiu_label(self, state: dict[str, int | str]) -> str:
        choice = state.get('choice')
        amount = int(state.get('amount') or MIN_BET)
        choice_label = 'TÀI' if choice == 'TAI' else 'XỈU' if choice == 'XIU' else 'chưa chọn cửa'
        return f'{choice_label} · {money(amount)}'

    async def send_taixiu_ephemeral(self, interaction: discord.Interaction, message: str):
        if interaction.response.is_done():
            return await interaction.followup.send(message, ephemeral=True, wait=True)
        await interaction.response.send_message(message, ephemeral=True)
        try:
            return await interaction.original_response()
        except discord.HTTPException:
            return None

    async def _delete_taixiu_private_message(self, message: object) -> None:
        try:
            await message.delete()
        except (discord.HTTPException, AttributeError, TypeError):
            pass

    async def _edit_taixiu_private_message(self, message: object, content: str) -> bool:
        try:
            await message.edit(content=content)
        except (discord.HTTPException, AttributeError, TypeError):
            return False
        return True

    def track_taixiu_private_message(
        self,
        session: TaixiuSession,
        user_id: int,
        message: object | None,
        *,
        result_target: bool = False,
    ) -> None:
        if message is None:
            return
        user_id = int(user_id)
        session.private_messages.setdefault(user_id, []).append(message)
        if result_target:
            session.private_result_messages[user_id] = message

    def taixiu_result_private_message(self, session: TaixiuSession, bet: TaixiuBet) -> str:
        dice = session.dice or (0, 0, 0)
        total = sum(dice)
        result_label = 'TÀI' if session.result == 'TAI' else 'XỈU'
        choice_label = 'TÀI' if bet.choice == 'TAI' else 'XỈU'
        settlement = session.settlements.get(int(bet.user_id), {})
        won = bool(settlement.get('won'))
        profit = int(settlement.get('profit') or 0)
        balance = int(settlement.get('balance') or 0)
        outcome = 'Bạn đã thắng' if won else 'Bạn đã thua'
        sign = '+' if profit > 0 else ''
        return (
            f'**{outcome}** round #{session.round_number}.\n'
            f'Bạn đã đặt **{choice_label}** `{money(bet.amount)}`.\n'
            f'Kết quả: `{dice[0]} + {dice[1]} + {dice[2]} = {total}` · **{result_label}**.\n'
            f'Lãi/lỗ: `{sign}{money(profit)}`.\n'
            f'Số tiền tài khoản sau khi chơi: `{money(balance)}`.\n'
            f'Tin nhắn này sẽ tự xóa sau {TAIXIU_PRIVATE_DELETE_SECONDS} giây.'
        )

    async def send_taixiu_result_notifications(self, session: TaixiuSession) -> None:
        if session.status != 'FINISHED':
            return
        for user_id, bet in list(session.bets.items()):
            content = self.taixiu_result_private_message(session, bet)
            target = session.private_result_messages.get(int(user_id))
            if target is not None:
                await self._edit_taixiu_private_message(target, content)
        if session.private_messages:
            asyncio.create_task(self.delete_taixiu_private_messages_later(session))

    async def delete_taixiu_private_messages_later(self, session: TaixiuSession) -> None:
        await asyncio.sleep(TAIXIU_PRIVATE_DELETE_SECONDS)
        messages = [
            message
            for user_messages in session.private_messages.values()
            for message in user_messages
        ]
        seen: set[int] = set()
        for message in messages:
            message_id = id(message)
            if message_id in seen:
                continue
            seen.add(message_id)
            await self._delete_taixiu_private_message(message)
        session.private_messages.clear()
        session.private_result_messages.clear()

    async def refresh_taixiu_from_interaction(
        self,
        interaction: discord.Interaction,
        session: TaixiuSession,
        notice: str | None = None,
    ) -> None:
        if notice is not None:
            session.notice = notice
        if interaction.message is not None:
            session.channel_id = interaction.message.channel.id
            session.message_id = interaction.message.id
            await self.edit_taixiu_board(session, interaction.message)
            return
        await self.edit_taixiu_board(session)

    async def update_taixiu_interaction_board(
        self,
        interaction: discord.Interaction,
        session: TaixiuSession,
        notice: str | None = None,
    ) -> None:
        if notice is not None:
            session.notice = notice
        if interaction.message is not None:
            session.channel_id = interaction.message.channel.id
            session.message_id = interaction.message.id
        payload = self.taixiu_edit_payload(session)
        view = TaixiuView(self, session.guild_id)
        if interaction.response.is_done():
            if interaction.message is not None:
                await interaction.message.edit(**payload, view=view)
            return
        await interaction.response.edit_message(**payload, view=view)

    async def stage_taixiu_choice(self, interaction: discord.Interaction, choice: str):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, TAIXIU_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._stage_taixiu_choice_locked(interaction, choice)
            self._log_interaction_perf(interaction, TAIXIU_GAME_KEY, f'choice_{choice.lower()}', started_at)

    async def _stage_taixiu_choice_locked(self, interaction: discord.Interaction, choice: str):
        guild_id = int(interaction.guild_id)
        key = (guild_id, interaction.user.id)
        session = self.taixiu_sessions.get(guild_id)
        if not session or session.status != 'BETTING':
            await interaction.response.defer()
            return
        await self._defer_component_update(interaction)
        state = self._pending_taixiu_state(guild_id, interaction.user.id)
        amount = int(state.get('amount') or self.default_taixiu_bets.get(key, MIN_BET))
        ok, message = await self.set_taixiu_choice(
            guild_id=guild_id,
            channel_id=int(interaction.channel_id),
            user=interaction.user,
            choice=choice,
            amount=amount,
        )
        if ok:
            self.pending_taixiu_bets.pop(key, None)
        prefix = '' if ok else '❌ '
        private_message = await self.send_taixiu_ephemeral(interaction, prefix + message)
        if ok:
            self.track_taixiu_private_message(
                session,
                interaction.user.id,
                private_message,
                result_target=True,
            )
        await self.refresh_taixiu_from_interaction(interaction, session, prefix + message)

    async def stage_taixiu_amount(self, interaction: discord.Interaction, amount: int):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, TAIXIU_GAME_KEY, cooldown=0.15)
        if lock is None:
            return
        async with lock:
            await self._stage_taixiu_amount_locked(interaction, amount)
            self._log_interaction_perf(interaction, TAIXIU_GAME_KEY, 'chip', started_at)

    async def _stage_taixiu_amount_locked(self, interaction: discord.Interaction, amount: int):
        guild_id = int(interaction.guild_id)
        session = self.taixiu_sessions.get(guild_id)
        if not session or session.status != 'BETTING':
            await interaction.response.defer()
            return
        amount = max(MIN_BET, min(int(amount), MAX_BET))
        state = self._pending_taixiu_state(guild_id, interaction.user.id)
        state['amount'] = amount
        self.default_taixiu_bets[(guild_id, interaction.user.id)] = amount
        await self.send_taixiu_ephemeral(
            interaction,
            f'Đã chọn chip `{chip_label(amount)}` ({money(amount)}). Bấm **ĐẶT TÀI** hoặc **ĐẶT XỈU** để cược ngay.',
        )

    async def stage_taixiu_all_in(self, interaction: discord.Interaction):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, TAIXIU_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._stage_taixiu_all_in_locked(interaction)
            self._log_interaction_perf(interaction, TAIXIU_GAME_KEY, 'all_in', started_at)

    async def _stage_taixiu_all_in_locked(self, interaction: discord.Interaction):
        guild_id = int(interaction.guild_id)
        session = self.taixiu_sessions.get(guild_id)
        if not session or session.status != 'BETTING':
            await interaction.response.defer()
            return
        user_name = display_name(interaction.user)
        await self._defer_component_update(interaction)
        wallet = await self._ensure_wallet_async(guild_id, interaction.user.id, user_name)
        balance = int(wallet.get('balance') or 0)
        if balance < MIN_BET:
            await self.send_taixiu_ephemeral(
                interaction,
                f'Không đủ balance để all-in. Balance hiện tại: `{money(balance)}`.',
            )
            return
        amount = min(balance, MAX_BET)
        state = self._pending_taixiu_state(guild_id, interaction.user.id)
        state['amount'] = amount
        self.default_taixiu_bets[(guild_id, interaction.user.id)] = amount
        await self.send_taixiu_ephemeral(
            interaction,
            f'Đã chọn **ALL-IN** `{money(amount)}`. Bấm **ĐẶT TÀI** hoặc **ĐẶT XỈU** để cược ngay.',
        )

    async def confirm_taixiu_bet(self, interaction: discord.Interaction):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, TAIXIU_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._confirm_taixiu_bet_locked(interaction)
            self._log_interaction_perf(interaction, TAIXIU_GAME_KEY, 'confirm', started_at)

    async def _confirm_taixiu_bet_locked(self, interaction: discord.Interaction):
        guild_id = int(interaction.guild_id)
        key = (guild_id, interaction.user.id)
        session = self.taixiu_sessions.get(guild_id)
        if not session or session.status != 'BETTING':
            await interaction.response.defer()
            return
        await self._defer_component_update(interaction)
        state = self._pending_taixiu_state(guild_id, interaction.user.id)
        choice = str(state.get('choice') or '')
        amount = int(state.get('amount') or self.default_taixiu_bets.get(key, MIN_BET))
        if choice not in {'TAI', 'XIU'}:
            await self.send_taixiu_ephemeral(
                interaction,
                'Bấm **ĐẶT TÀI** hoặc **ĐẶT XỈU** để cược ngay. Nút xác nhận chỉ dùng cho lựa chọn cũ đang pending.',
            )
            return
        ok, message = await self.set_taixiu_choice(
            guild_id=guild_id,
            channel_id=int(interaction.channel_id),
            user=interaction.user,
            choice=choice,
            amount=amount,
        )
        if ok:
            self.pending_taixiu_bets.pop(key, None)
        prefix = '' if ok else '❌ '
        private_message = await self.send_taixiu_ephemeral(interaction, prefix + message)
        if ok:
            self.track_taixiu_private_message(
                session,
                interaction.user.id,
                private_message,
                result_target=True,
            )
        await self.refresh_taixiu_from_interaction(interaction, session, prefix + message)

    async def cancel_taixiu_bet(self, interaction: discord.Interaction):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        started_at = time.perf_counter()
        lock = await self._button_spam_guard(interaction, TAIXIU_GAME_KEY)
        if lock is None:
            return
        async with lock:
            await self._cancel_taixiu_bet_locked(interaction)
            self._log_interaction_perf(interaction, TAIXIU_GAME_KEY, 'cancel', started_at)

    async def _cancel_taixiu_bet_locked(self, interaction: discord.Interaction):
        guild_id = int(interaction.guild_id)
        key = (guild_id, interaction.user.id)
        self.pending_taixiu_bets.pop(key, None)
        session = self.taixiu_sessions.get(guild_id)
        if not session or session.status != 'BETTING':
            await interaction.response.defer()
            return
        await self._defer_component_update(interaction)
        existing = session.bets.get(int(interaction.user.id))
        if not existing:
            await self.send_taixiu_ephemeral(
                interaction,
                'Đã xóa lựa chọn pending. Bạn chưa có cược đang chạy.',
            )
            return
        removed = await self.remove_taixiu_choice(
            guild_id=guild_id,
            channel_id=int(interaction.channel_id),
            user_id=int(interaction.user.id),
            choice=existing.choice,
        )
        if removed:
            notice = f'<@{interaction.user.id}> đã hủy cược và hoàn `{money(existing.amount)}`.'
        else:
            notice = f'<@{interaction.user.id}> không hủy được cược lúc này.'
        await self.send_taixiu_ephemeral(interaction, notice)
        await self.refresh_taixiu_from_interaction(interaction, session, notice)

    async def set_taixiu_choice(
        self,
        *,
        guild_id: int,
        channel_id: int,
        user: discord.abc.User,
        choice: str,
        amount: int,
    ) -> tuple[bool, str]:
        if not self._is_game_channel(guild_id, channel_id, TAIXIU_GAME_KEY):
            return False, 'Channel này không được gán Tài Xỉu.'
        session = await self.ensure_taixiu_session(guild_id)
        if session.status != 'BETTING':
            return False, 'Round này đã khóa cược.'
        if amount < MIN_BET:
            return False, f'Cược tối thiểu là {money(MIN_BET)}.'
        if amount > MAX_BET:
            return False, f'Cược tối đa là {money(MAX_BET)}.'

        user_id = int(user.id)
        user_name = display_name(user)
        existing = session.bets.get(user_id)
        if existing:
            if existing.choice == choice and existing.amount == amount:
                return True, 'Bạn đã đặt cược này rồi.'
            old_choice = existing.choice
            old_amount = int(existing.amount)
            delta = int(amount) - old_amount
            if delta > 0:
                try:
                    await self._wallet_change_async(
                        guild_id=guild_id,
                        user_id=user_id,
                        user_name=user_name,
                        amount=-delta,
                        tx_type='casino_taixiu_bet_adjust',
                        description=f'Tài Xỉu round #{session.round_number} bet increase',
                        meta={'game': 'TAIXIU', 'round_number': session.round_number, 'choice': choice},
                    )
                except ValueError as e:
                    return False, f'❌ {e}'
            try:
                with self.bot.study_context.database.transaction() as conn:
                    conn.execute(
                        """
                        UPDATE casino_taixiu_bets
                        SET choice = ?, amount = ?
                        WHERE guild_id = ? AND round_id = ? AND user_id = ?
                        """,
                        (choice, amount, guild_id, session.round_id, user_id),
                    )
            except Exception:
                log.exception('Failed to update Tài Xỉu choice for user_id=%s', user_id)
                if delta > 0:
                    await self._wallet_change_async(
                        guild_id=guild_id,
                        user_id=user_id,
                        user_name=user_name,
                        amount=delta,
                        tx_type='casino_taixiu_refund',
                        description='Refund failed Tài Xỉu bet adjustment',
                        meta={'game': 'TAIXIU', 'round_number': session.round_number},
                    )
                return False, 'Không cập nhật được lựa chọn. Thử lại sau.'
            if delta < 0:
                await self._wallet_change_async(
                    guild_id=guild_id,
                    user_id=user_id,
                    user_name=user_name,
                    amount=-delta,
                    tx_type='casino_taixiu_refund',
                    description=f'Tài Xỉu round #{session.round_number} bet decrease refund',
                    meta={'game': 'TAIXIU', 'round_number': session.round_number, 'choice': choice},
                )
            existing.choice = choice
            existing.amount = int(amount)
            self.default_taixiu_bets[(guild_id, user_id)] = int(amount)
            label = 'Tài' if choice == 'TAI' else 'Xỉu'
            changes = []
            if old_choice != choice:
                changes.append(f'đổi cửa sang {label}')
            if old_amount != amount:
                changes.append(f'đổi cược thành {money(amount)}')
            return True, 'Đã ' + ' và '.join(changes) + '.'

        try:
            await self._wallet_change_async(
                guild_id=guild_id,
                user_id=user_id,
                user_name=user_name,
                amount=-amount,
                tx_type='casino_taixiu_bet',
                description=f'Tài Xỉu round #{session.round_number} bet',
                meta={'game': 'TAIXIU', 'round_number': session.round_number, 'choice': choice},
            )
        except ValueError as e:
            return False, f'❌ {e}'

        bet = TaixiuBet(user_id, user_name, choice, amount)
        try:
            with self.bot.study_context.database.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO casino_taixiu_bets (guild_id, round_id, user_id, choice, amount, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (guild_id, session.round_id, user_id, choice, amount, now_iso()),
                )
        except Exception:
            log.exception('Failed to persist Tài Xỉu bet; refunding user_id=%s', user_id)
            await self._wallet_change_async(
                guild_id=guild_id,
                user_id=user_id,
                user_name=user_name,
                amount=amount,
                tx_type='casino_taixiu_refund',
                description='Refund failed Tài Xỉu bet',
                meta={'game': 'TAIXIU', 'round_number': session.round_number},
            )
            return False, 'Không lưu được bet, đã hoàn tiền. Thử lại sau.'

        session.bets[user_id] = bet
        self.default_taixiu_bets[(guild_id, user_id)] = amount
        label = 'Tài' if choice == 'TAI' else 'Xỉu'
        return True, f'✅ Đã đặt {money(amount)} vào {label} round #{session.round_number}.'

    async def remove_taixiu_choice(
        self,
        *,
        guild_id: int,
        channel_id: int,
        user_id: int,
        choice: str,
    ) -> bool:
        if not self._is_game_channel(guild_id, channel_id, TAIXIU_GAME_KEY):
            return False
        session = self.taixiu_sessions.get(guild_id)
        if not session or session.status != 'BETTING':
            return False
        existing = session.bets.get(int(user_id))
        if not existing or existing.choice != choice:
            return False
        session.bets.pop(int(user_id), None)
        try:
            with self.bot.study_context.database.transaction() as conn:
                conn.execute(
                    'DELETE FROM casino_taixiu_bets WHERE guild_id = ? AND round_id = ? AND user_id = ?',
                    (guild_id, session.round_id, int(user_id)),
                )
        except Exception:
            log.exception('Failed to delete Tài Xỉu choice for user_id=%s', user_id)
            return False
        try:
            await self._wallet_change_async(
                guild_id=guild_id,
                user_id=int(user_id),
                user_name=existing.user_name,
                amount=existing.amount,
                tx_type='casino_taixiu_refund',
                description=f'Removed Tài Xỉu round #{session.round_number} bet',
                meta={'game': 'TAIXIU', 'round_number': session.round_number, 'choice': choice},
            )
        except ValueError:
            log.exception('Failed to refund removed Tài Xỉu choice for user_id=%s', user_id)
            return False
        return True

    @casino.command(name='bet', description='Đặt mức cược mặc định cho Tài Xỉu')
    @app_commands.describe(amount='Số coins mặc định khi đặt Tài/Xỉu')
    async def casino_bet(self, interaction: discord.Interaction, amount: app_commands.Range[int, MIN_BET, MAX_BET]):
        if not await self._guild_guard(interaction, TAIXIU_GAME_KEY):
            return
        self.default_taixiu_bets[(int(interaction.guild_id), interaction.user.id)] = int(amount)
        await interaction.response.send_message(
            f'Đã đặt mức cược mặc định Tài Xỉu: `{money(int(amount))}`.',
            ephemeral=True,
        )

    async def start_casino_bet_message(self, message: discord.Message, raw_amount: str | None) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, TAIXIU_GAME_KEY):
            return
        amount, error = self._parse_bet(raw_amount)
        if error:
            await message.channel.send(f'❌ {error}')
            return
        self.default_taixiu_bets[(int(message.guild.id), message.author.id)] = int(amount)
        await message.channel.send(f'Đã đặt mức cược mặc định Tài Xỉu: `{money(int(amount))}`.')

    def leaderboard_embed(self, guild_id: int) -> discord.Embed:
        data = self.bot.study_context.load_data(guild_id)
        rows = sorted(
            data.values(),
            key=lambda info: int(info.get('balance') or 0),
            reverse=True,
        )[:10]
        embed = discord.Embed(title='💰 Coins Leaderboard', color=GREEN)
        if not rows:
            embed.description = 'Chưa có dữ liệu.'
        else:
            lines = []
            for idx, info in enumerate(rows, 1):
                lines.append(f'`{idx}.` **{info.get("name", "Unknown")}** · `{money(info.get("balance", 0))}`')
            embed.description = '\n'.join(lines)
        return embed

    async def start_casino_leaderboard_message(self, message: discord.Message) -> None:
        ctx = await self.bot.get_context(message)
        if not await self._prefix_guild_guard(ctx, CASINO_GAME_KEY):
            return
        await message.channel.send(embed=self.leaderboard_embed(int(message.guild.id)))

    @commands.command(name='casino')
    async def casino_prefix(self, ctx: commands.Context, subcommand: str = None, amount: str = None):
        subcommand = str(subcommand or '').lower().strip()
        if subcommand == 'bet':
            await self.start_casino_bet_message(ctx.message, amount)
            return
        if subcommand in {'leaderboard', 'lb', 'top'}:
            await self.start_casino_leaderboard_message(ctx.message)
            return
        await ctx.send('Dùng `!casino bet <amount>` hoặc `!casino leaderboard`.')

    @casino.command(name='leaderboard', description='Top 10 balance coins')
    async def casino_leaderboard(self, interaction: discord.Interaction):
        if not await self._guild_guard(interaction, CASINO_GAME_KEY):
            return
        await interaction.response.send_message(embed=self.leaderboard_embed(int(interaction.guild_id)))

    async def _reaction_user(self, payload: discord.RawReactionActionEvent) -> discord.abc.User | None:
        if payload.member:
            return payload.member
        user = self.bot.get_user(payload.user_id)
        if user:
            return user
        try:
            return await self.bot.fetch_user(payload.user_id)
        except discord.HTTPException:
            return None

    def _valid_panel_for_payload(self, payload: discord.RawReactionActionEvent) -> ReactionPanel | None:
        panel = self.reaction_panels.get(int(payload.message_id))
        if not panel:
            return None
        if payload.guild_id and int(payload.guild_id) != panel.guild_id:
            return None
        if int(payload.channel_id or 0) != panel.channel_id:
            return None
        if panel.expired():
            self._unregister_panel(panel.message_id)
            session = self._blackjack_session_for_message(panel.message_id)
            if session and not session.finished:
                self.blackjack_sessions.pop((session.guild_id, session.user_id), None)
            return None
        return panel

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.user_id == getattr(self.bot.user, 'id', None):
            return
        panel = self._valid_panel_for_payload(payload)
        if not panel:
            return
        user = await self._reaction_user(payload)
        if not user or getattr(user, 'bot', False):
            return

        emoji = str(payload.emoji)
        if panel.game_key == BLACKJACK_GAME_KEY:
            action = BLACKJACK_REACTIONS.get(emoji)
            if not action:
                await self._remove_user_reaction(payload)
                return
            message = await self._fetch_reaction_message(panel)
            if not message:
                return
            handled = await self.handle_blackjack_action(message, int(payload.user_id), action)
            if not handled or self._blackjack_session_for_message(panel.message_id):
                await self._remove_user_reaction(payload)
            return

async def setup(bot: commands.Bot):
    await bot.add_cog(CasinoCog(bot))
