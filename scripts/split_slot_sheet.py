from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image


REFERENCE_SIZE = (1460, 1092)

ICON_BOXES = {
    'seven': (198, 24, 390, 220),
    'apple': (580, 24, 708, 150),
    'lemon': (744, 24, 895, 154),
    'orange': (916, 24, 1056, 154),
    'grape': (1077, 28, 1220, 154),
    'cherry': (1248, 28, 1384, 150),
    'watermelon': (575, 166, 706, 286),
    'diamond': (742, 166, 870, 288),
    'bomb': (912, 166, 1044, 296),
    'heart_pink': (1072, 168, 1208, 290),
    'heart_purple': (1232, 168, 1368, 290),
    'ruby': (584, 288, 692, 410),
    'coin': (744, 288, 862, 402),
    'bell': (910, 286, 1042, 404),
    'star': (1080, 288, 1202, 408),
    'crown': (1238, 288, 1372, 402),
    'horseshoe': (548, 428, 668, 532),
    'spade': (676, 428, 790, 534),
    'club': (804, 428, 918, 534),
    'red_diamond': (928, 426, 1026, 534),
}

BUTTON_BOXES = {
    'button_line': (42, 572, 276, 672),
    'button_bet': (290, 572, 526, 672),
    'button_spin': (546, 552, 842, 688),
    'button_pay_table': (858, 562, 1135, 662),
    'button_bet_max': (1160, 566, 1392, 662),
    'button_total_bet': (44, 704, 316, 802),
    'button_max_bet': (336, 704, 642, 802),
    'button_play': (666, 696, 758, 778),
    'button_pause': (774, 696, 866, 778),
    'button_settings': (882, 696, 974, 778),
    'button_info': (980, 696, 1080, 778),
    'button_sound': (1096, 696, 1206, 778),
    'button_next': (1218, 696, 1310, 778),
    'button_back': (1326, 696, 1418, 778),
    'button_plus': (666, 786, 758, 870),
    'button_minus': (774, 786, 866, 870),
    'button_home': (882, 786, 974, 870),
    'button_gift': (980, 786, 1080, 870),
    'button_trophy': (1096, 786, 1206, 870),
    'button_refresh': (1218, 786, 1310, 870),
    'button_help': (1326, 786, 1418, 870),
}


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


def remove_dark_background(image: Image.Image) -> Image.Image:
    image = image.convert('RGBA')
    pixels = image.load()
    width, height = image.size
    for y in range(height):
        for x in range(width):
            r, g, b, a = pixels[x, y]
            if a and r < 38 and g < 66 and b < 112:
                pixels[x, y] = (r, g, b, 0)
    return image


def crop_all(sheet_path: Path, output_dir: Path) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    with Image.open(sheet_path) as sheet:
        sheet = sheet.convert('RGBA')
        icon_count = 0
        button_count = 0
        for name, box in ICON_BOXES.items():
            crop = sheet.crop(scaled_box(box, sheet.size))
            crop = remove_dark_background(crop)
            crop.save(output_dir / f'{name}.png')
            icon_count += 1
        for name, box in BUTTON_BOXES.items():
            crop = sheet.crop(scaled_box(box, sheet.size))
            crop.save(output_dir / f'{name}.png')
            button_count += 1
    return icon_count, button_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Split the slot asset sheet into individual icon/button PNG files.')
    parser.add_argument('--sheet', type=Path, default=Path('assets/slot/slot-sheet.png'), help='Path to the slot sheet image.')
    parser.add_argument('--out', type=Path, default=Path('assets/slot'), help='Output directory for cropped PNG files.')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.sheet.exists():
        raise FileNotFoundError(f'Missing slot sheet: {args.sheet}')
    icon_count, button_count = crop_all(args.sheet, args.out)
    print(f'Wrote {icon_count} icons and {button_count} buttons to {args.out}')


if __name__ == '__main__':
    main()
