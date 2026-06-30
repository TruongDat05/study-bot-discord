from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


RANKS = ('A', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K')
SUITS = {
    'spades': 'S',
    'hearts': 'H',
    'diamonds': 'D',
    'clubs': 'C',
}

# Coordinates are normalized from the sheet images in function.md's prompt.
# Each crop keeps the whole card, including the original soft shadow.
REFERENCE_SIZE = (1460, 1092)
REFERENCE_BOXES = (
    (110, 42, 330, 356),
    (363, 42, 583, 356),
    (614, 42, 835, 356),
    (866, 42, 1087, 356),
    (1118, 42, 1340, 356),
    (110, 389, 330, 704),
    (363, 389, 583, 704),
    (614, 389, 835, 704),
    (866, 389, 1087, 704),
    (1118, 389, 1340, 704),
    (363, 736, 583, 1052),
    (614, 736, 835, 1052),
    (866, 736, 1087, 1052),
)


def scaled_box(box: tuple[int, int, int, int], image_size: tuple[int, int]) -> tuple[int, int, int, int]:
    ref_w, ref_h = REFERENCE_SIZE
    img_w, img_h = image_size
    left, top, right, bottom = box
    return (
        round(left * img_w / ref_w),
        round(top * img_h / ref_h),
        round(right * img_w / ref_w),
        round(bottom * img_h / ref_h),
    )


def make_card_back(path: Path, size: tuple[int, int]) -> None:
    width, height = size
    card = Image.new('RGBA', size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(card)
    draw.rounded_rectangle((0, 0, width - 1, height - 1), radius=12, fill=(248, 248, 248), outline=(20, 20, 20), width=2)
    draw.rounded_rectangle((12, 12, width - 13, height - 13), radius=8, fill=(151, 32, 44), outline=(255, 230, 230), width=3)
    for x in range(-height, width, 18):
        draw.line((x, height - 12, x + height, 12), fill=(111, 22, 36), width=2)
        draw.line((x, 12, x + height, height - 12), fill=(184, 55, 66), width=2)
    try:
        font = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 36)
    except OSError:
        font = ImageFont.load_default()
    draw.text((width // 2, height // 2), 'BJ', fill=(255, 244, 220), font=font, anchor='mm')
    card.save(path)


def split_sheet(sheet_path: Path, suit_name: str, output_dir: Path) -> tuple[int, tuple[int, int]]:
    suit_code = SUITS[suit_name]
    with Image.open(sheet_path) as sheet:
        sheet = sheet.convert('RGBA')
        first_size = (0, 0)
        for rank, reference_box in zip(RANKS, REFERENCE_BOXES, strict=True):
            crop = sheet.crop(scaled_box(reference_box, sheet.size))
            if first_size == (0, 0):
                first_size = crop.size
            crop.save(output_dir / f'{rank}{suit_code}.png')
    return len(RANKS), first_size


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Split 4 playing-card suit sheets into individual card PNG files.')
    parser.add_argument('--spades', type=Path, required=True, help='Path to the spades sheet image.')
    parser.add_argument('--clubs', type=Path, required=True, help='Path to the clubs sheet image.')
    parser.add_argument('--hearts', type=Path, required=True, help='Path to the hearts sheet image.')
    parser.add_argument('--diamonds', type=Path, required=True, help='Path to the diamonds sheet image.')
    parser.add_argument('--out', type=Path, default=Path('assets/cards'), help='Output directory for card PNG files.')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    total = 0
    card_size = (220, 314)
    for suit_name in SUITS:
        sheet_path = getattr(args, suit_name)
        if not sheet_path.exists():
            raise FileNotFoundError(f'Missing {suit_name} sheet: {sheet_path}')
        count, card_size = split_sheet(sheet_path, suit_name, args.out)
        total += count

    make_card_back(args.out / 'back.png', card_size)
    print(f'Wrote {total} card files plus back.png to {args.out}')


if __name__ == '__main__':
    main()
