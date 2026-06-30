from __future__ import annotations

from .blackjack import GAME as BLACKJACK
from .casino import GAME as CASINO
from .dice import GAME as DICE
from .hilo import GAME as HILO
from .slot import GAME as SLOT
from .taixiu import GAME as TAIXIU

GAME_ORDER = ('blackjack', 'taixiu', 'slot', 'dice', 'hilo', 'casino')
GAME_CATALOG = {
    item['key']: item
    for item in (BLACKJACK, TAIXIU, SLOT, DICE, HILO, CASINO)
}
GAME_LABELS = {key: item['label'] for key, item in GAME_CATALOG.items()}
