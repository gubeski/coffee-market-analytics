"""
stars_parser.py
===============
Парсер прайс-листа STARS Coffee — эталонного бенчмарка системы.

РОЛЬ В СИСТЕМЕ
--------------
STARS — единственный конкурент с is_benchmark=TRUE (competitor_id=1).
Именно его цены используются как базовые в view v_benchmark_delta:

    delta_pct = (competitor_price - stars_price) / stars_price * 100

Без загрузки STARS данный view будет возвращать пустую выборку.

СПЕЦИФИКА МЕНЮ STARS
--------------------
1. Три фиксированных размера с брендовыми названиями:
       Маленький = 350 мл
       Средний   = 450 мл
       Большой   = 550 мл
   Исключение: Эспрессо — 46 мл (одинарный шот).

2. Один напиток = до трёх строк в DataFrame (по одной на каждый размер).
   Флэт Уайт и ряд авторских продаются только в Маленьком.

3. Некоторые позиции — слэш-варианты («Мокка / Мокка Белый Шоколад»,
   «Горячий Шоколад / Белый Горячий Шоколад»). Это разные SKU с одной ценой —
   фиксируем первый вариант как каноническое название, второй — в примечании.

4. Фраппе существуют в двух подкатегориях: С КОФЕ и БЕЗ КОФЕ.
   Оба маппятся в category_slug='frappe', флаг is_signature сигнализирует
   на авторские позиции.

ДАННЫЕ
------
Источник: PDF-меню STARS Coffee (2 страницы), прочитанный визуально.
STARS_MENU_DATA — единственное место для обновления при смене цен.

ВЫХОДНОЙ DATAFRAME
------------------
drink_id | size_id | price_rub | volume_ml
Каждый напиток × каждый доступный размер = отдельная строка.
Готов к run_price_pipeline() без трансформаций.

Зависимости
-----------
    pip install sqlalchemy pandas
    (shared_mapper.py должен быть в PYTHONPATH)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import Engine

from shared_mapper import DrinkMapper, RawMenuItem, normalize_drink_name

logger = logging.getLogger(__name__)


# ===========================================================================
# КОНСТАНТЫ
# ===========================================================================

STARS_COMPETITOR_ID: int = 1  # is_benchmark = TRUE в таблице competitors

# Канонические объёмы стаканов STARS.
# Ключ используется в STARS_MENU_DATA для краткости.
STARS_SIZES: dict[str, int] = {
    "мал": 350,   # Маленький
    "сред": 450,  # Средний
    "бол": 550,   # Большой
    "эспр": 46,   # Эспрессо (исключение — одинарный шот)
}


# ===========================================================================
# ДАННЫЕ МЕНЮ
# ===========================================================================
#
# Формат каждой записи:
#   (name, category_slug, is_signature, prices_by_size)
#
# prices_by_size — dict вида {"мал": price, "сред": price, "бол": price}
#   Отсутствующий ключ = размер не продаётся для этой позиции.
#   Для эспрессо используется ключ "эспр" вместо "мал".
#
# Категории (slug из таблицы drink_categories):
#   "espresso"  — классический и авторский кофе
#   "signature" — авторские напитки с уникальными вкусами
#   "tea"       — чай и чайные латте
#   "frappe"    — фраппе
#   "cacao"     — горячий шоколад
#   "smoothie"  — не используется у STARS
# ===========================================================================

STARS_MENU_DATA: list[tuple] = [

    # ── ЭСПРЕССО & КОФЕ ─────────────────────────────────────────────────────

    # Классика — три размера (кроме эспрессо и Флэт Уайт)
    ("Американо",    "espresso", False, {"мал": 305, "сред": 315, "бол": 345}),
    ("Капучино",     "espresso", False, {"мал": 350, "сред": 370, "бол": 400}),
    ("Латте",        "espresso", False, {"мал": 355, "сред": 375, "бол": 405}),
    ("Мокка",        "espresso", False, {"мал": 390, "сред": 420, "бол": 445}),
    ("Раф Ванилла",  "espresso", False, {"мал": 410, "сред": 430, "бол": 460}),
    ("Флэт Уайт",    "espresso", False, {"мал": 400}),                          # только Маленький
    ("Эспрессо",     "espresso", False, {"эспр": 240}),                         # 46 мл
    ("Фильтр-кофе",  "espresso", False, {"мал": 245, "сред": 255, "бол": 265}),

    # Авторские кофейные (карточки с иллюстрациями, все три размера)
    ("Мокка Белый Шоколад",  "signature", True, {"мал": 390, "сред": 420, "бол": 445}),
    ("Голден Макиато",       "signature", True, {"мал": 400, "сред": 420, "бол": 445}),

    # ── АВТОРСКИЕ НАПИТКИ ────────────────────────────────────────────────────

    # Хиты (значок HIT в меню) — is_seasonal=False, но флагируем как is_signature
    ("Флэт Уайт Двойной Шоколад",      "signature", True, {"мал": 415}),
    ("Латте Лимонный Курд с Шоколадом", "signature", True, {"мал": 410, "сред": 435, "бол": 455}),
    ("Латте Сливочная Фисташка",        "signature", True, {"мал": 410, "сред": 435, "бол": 455}),
    ("Эспрессо-Тоник / Маття-Тоник",   "signature", True, {"мал": 370, "сред": 385, "бол": 415}),
    ("Бамбл Кофе / Бамбл Маття",       "signature", True, {"мал": 405, "сред": 430, "бол": 455}),

    # Авторские с иллюстрациями (карточки внизу секции)
    ("Горячий Шоколад",                "cacao",     False, {"мал": 360, "сред": 375, "бол": 400}),
    ("Белый Горячий Шоколад",          "cacao",     False, {"мал": 360, "сред": 375, "бол": 400}),
    ("Латте Макадамия с Солёной Карамелью", "signature", True, {"мал": 425, "сред": 440, "бол": 455}),

    # ── ЧАЙ & ФРАППЕ ────────────────────────────────────────────────────────

    # Чайные напитки (карточки)
    ("Мятный Чай Малина-Маракуйя", "tea", True, {"мал": 340, "сред": 370, "бол": 400}),
    ("Цикорий Сливочная Карамель", "tea", True, {"мал": 360, "сред": 375, "бол": 395}),

    # Чай из таблицы
    ("Свежезаваренный Чай",        "tea", False, {"мал": 260, "сред": 270, "бол": 285}),
    ("Зелёный Чай Жасмин-Манго",   "tea", True,  {"мал": 340, "сред": 370, "бол": 400}),
    ("Пряный Чай Латте",           "tea", True,  {"мал": 375, "сред": 390, "бол": 420}),
    ("Маття Чай Латте",            "tea", True,  {"мал": 380, "сред": 395, "бол": 425}),
    ("Кокосовая Маття с Малиной",  "tea", True,  {"мал": 445, "сред": 465, "бол": 480}),

    # Фраппе С КОФЕ
    ("Фраппе Карамельный",             "frappe", True,  {"мал": 400, "сред": 420, "бол": 440}),
    ("Фраппе Классический",            "frappe", False, {"мал": 380, "сред": 390, "бол": 420}),
    ("Фраппе Мокка / Мокка Белый Шоколад", "frappe", True, {"мал": 390, "сред": 420, "бол": 440}),

    # Фраппе БЕЗ КОФЕ
    ("Фраппе Сочная Малина",       "frappe", True,  {"мал": 410, "сред": 425, "бол": 440}),
    ("Фраппе Шоколадный",          "frappe", False, {"мал": 380, "сред": 400, "бол": 420}),
    ("Фраппе Ванилла",             "frappe", False, {"мал": 380, "сред": 400, "бол": 420}),
    ("Фраппе Маття",               "frappe", True,  {"мал": 400, "сред": 420, "бол": 440}),
]


# ===========================================================================
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: разворачиваем мультиразмерную запись
# ===========================================================================

def _expand_to_items(
    name: str,
    category_slug: str,
    is_signature: bool,
    prices_by_size: dict[str, int],
) -> list[RawMenuItem]:
    """
    Преобразует одну мультиразмерную запись в список RawMenuItem.

    Каждый доступный размер → отдельный RawMenuItem.
    Ключи "мал" / "сред" / "бол" / "эспр" резолвятся через STARS_SIZES.

    Пример:
        ("Капучино", "espresso", False, {"мал": 350, "сред": 370, "бол": 400})
        →  [RawMenuItem("Капучино", 350, 350, ...), # 350₽, 350мл
            RawMenuItem("Капучино", 370, 450, ...), # 370₽, 450мл
            RawMenuItem("Капучино", 400, 550, ...)] # 400₽, 550мл
    """
    items = []
    for size_key, price in prices_by_size.items():
        volume_ml = STARS_SIZES.get(size_key)
        if volume_ml is None:
            logger.warning("Неизвестный ключ размера «%s» для «%s» — пропускаем", size_key, name)
            continue
        if price <= 0:
            logger.warning("Нулевая цена для «%s» размер «%s» — пропускаем", name, size_key)
            continue
        items.append(RawMenuItem(
            name=name,
            price_rub=float(price),
            volume_ml=volume_ml,
            category_slug=category_slug,
            is_signature=is_signature,
            is_seasonal=False,
        ))
    return items


# ===========================================================================
# ПАРСЕР
# ===========================================================================

class StarsMenuParser:
    """
    Разворачивает STARS_MENU_DATA в плоский список RawMenuItem.

    Одна запись с тремя размерами → три RawMenuItem (drink_id одинаковый,
    size_id разные). Это обеспечивает корректный JOIN в v_benchmark_delta
    по паре (name_normalized, volume_ml).
    """

    def parse(self) -> list[RawMenuItem]:
        all_items: list[RawMenuItem] = []
        for name, cat_slug, is_sig, prices in STARS_MENU_DATA:
            expanded = _expand_to_items(name, cat_slug, is_sig, prices)
            all_items.extend(expanded)

        logger.info(
            "StarsMenuParser: %d записей меню → %d строк (напиток × размер)",
            len(STARS_MENU_DATA), len(all_items),
        )
        return all_items


# ===========================================================================
# ТОЧКА ВХОДА
# ===========================================================================

def parse_stars_menu(
    engine: Engine,
    competitor_id: int = STARS_COMPETITOR_ID,
    auto_register: bool = True,
) -> pd.DataFrame:
    """
    Полный цикл: парсинг → маппинг → DataFrame, готовый для пайплайна.

    Интерфейс идентичен parse_surf_menu() и parse_drinkit_menu().

    Parameters
    ----------
    engine        : SQLAlchemy Engine к целевой БД
    competitor_id : PK бенчмарка (STARS = 1, is_benchmark=TRUE)
    auto_register : регистрировать новые напитки/размеры автоматически

    Returns
    -------
    pd.DataFrame колонки: drink_id, size_id, price_rub, volume_ml
    Каждая строка — уникальная пара (напиток, размер).

    Пример
    ------
    from sqlalchemy import create_engine
    from stars_parser import parse_stars_menu
    from price_pipeline import run_price_pipeline

    engine = create_engine("postgresql+psycopg2://user:pass@host/db")
    df     = parse_stars_menu(engine)
    result = run_price_pipeline(engine, competitor_id=1, scraped_df=df)
    # [run=1 | competitor=1] status=success | scraped=74 | new=74 | alerts=74
    print(result)
    """
    started   = datetime.now(timezone.utc)
    parser    = StarsMenuParser()
    raw_items = parser.parse()

    rows: list[dict] = []
    skipped: list[str] = []

    with engine.begin() as conn:
        mapper = DrinkMapper(conn, competitor_id, auto_register)

        for item in raw_items:
            drink_id = mapper.resolve_drink(item)
            if drink_id is None:
                skipped.append(f"{item.name} ({item.volume_ml}мл)")
                continue
            size_id = mapper.resolve_size(item.volume_ml)
            rows.append({
                "drink_id":  drink_id,
                "size_id":   size_id,
                "price_rub": item.price_rub,
                "volume_ml": item.volume_ml,
            })

    elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)

    df = pd.DataFrame(rows, columns=["drink_id", "size_id", "price_rub", "volume_ml"])
    # Защита от дублей: при повторном запуске парсера на том же меню
    df = df.drop_duplicates(subset=["drink_id", "size_id"], keep="first").reset_index(drop=True)

    logger.info(
        "parse_stars_menu завершён за %d мс: "
        "%d позиций → DataFrame %d строк | %s",
        elapsed_ms, len(raw_items), len(df), mapper.stats,
    )
    if skipped:
        logger.warning("Пропущено (не смапировано): %s", skipped)

    return df


# ===========================================================================
# CLI — просмотр без БД
# ===========================================================================

if __name__ == "__main__":
    """
    python stars_parser.py
    python stars_parser.py --show-normalized
    python stars_parser.py --show-per100          # показывает price_per_100ml
    python stars_parser.py --category espresso    # фильтр по категории
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    show_norm   = "--show-normalized" in sys.argv
    show_per100 = "--show-per100"     in sys.argv
    cat_filter  = None
    if "--category" in sys.argv:
        idx = sys.argv.index("--category")
        if idx + 1 < len(sys.argv):
            cat_filter = sys.argv[idx + 1]

    parser    = StarsMenuParser()
    raw_items = parser.parse()

    if cat_filter:
        raw_items = [i for i in raw_items if i.category_slug == cat_filter]

    # Группируем: напиток → строки по размерам
    from itertools import groupby

    def item_key(i: RawMenuItem) -> tuple:
        return (i.category_slug, i.name)

    sorted_items = sorted(raw_items, key=item_key)

    print(f"\n{'─'*80}")
    title = f"STARS Coffee — прайс-лист ({len(STARS_MENU_DATA)} напитков, "  \
            f"{len(raw_items)} строк с размерами)"
    if cat_filter:
        title += f" | фильтр: {cat_filter}"
    print(f"  {title}")
    print(f"  Система объёмов: Мал=350мл  Сред=450мл  Бол=550мл  Эспр=46мл")
    print(f"{'─'*80}")

    current_cat = ""
    for key, group_iter in groupby(sorted_items, key=item_key):
        cat_slug, name = key
        sizes = list(group_iter)

        if cat_slug != current_cat:
            current_cat = cat_slug
            print(f"\n  [{current_cat.upper()}]")

        sig = "★" if sizes[0].is_signature else " "

        # Строка размеров: Мал=350₽  Сред=370₽  Бол=400₽
        size_label = {350: "Мал", 450: "Сред", 550: "Бол", 46: "Эспр"}
        size_parts = []
        for s in sizes:
            lbl = size_label.get(s.volume_ml, f"{s.volume_ml}мл")
            if show_per100:
                pp = s.price_rub / s.volume_ml * 100
                size_parts.append(f"{lbl}={s.price_rub:.0f}₽({pp:.0f}₽/100мл)")
            else:
                size_parts.append(f"{lbl}={s.price_rub:.0f}₽")

        norm = f"  → {normalize_drink_name(name)}" if show_norm else ""
        print(f"  {sig} {name:<45}  {'  '.join(size_parts)}{norm}")

    # Итоги
    prices_all = [i.price_rub for i in raw_items]
    pp100_all  = [i.price_rub / i.volume_ml * 100 for i in raw_items]

    print(f"\n{'─'*80}")
    print(f"  Напитков в меню: {len(STARS_MENU_DATA)}")
    print(f"  Строк (напиток × размер): {len(raw_items)}")
    print(f"  Авторских SKU: {sum(1 for i in STARS_MENU_DATA if i[2])}")
    print(f"  Ценовой диапазон: {min(prices_all):.0f}₽ – {max(prices_all):.0f}₽")

    import statistics
    print(f"  price_per_100ml:"
          f"  мин={min(pp100_all):.1f}"
          f"  макс={max(pp100_all):.1f}"
          f"  медиана={statistics.median(pp100_all):.1f}")

    print(f"{'─'*80}\n")
    print("  Для записи в БД:")
    print("    from stars_parser import parse_stars_menu")
    print("    df = parse_stars_menu(engine)")
    print("    result = run_price_pipeline(engine, competitor_id=1, scraped_df=df)")
