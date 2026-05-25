"""
drinkit_parser.py
=================
Парсер меню Drinkit (мобильное приложение, скриншоты из PDF-архива).

СПЕЦИФИКА DRINKIT
-----------------
В отличие от Surf Coffee (русские названия, цена + объём на каждой карточке),
Drinkit имеет принципиально другую структуру:

  • Названия — английские, авторские (Raf Taro, Bumble Coffee, Raf Blackberry)
  • Объём НЕ указан на карточках — единый стандарт сети: горячие = 300 мл,
    холодные (iced) = 350 мл, эспрессо = 60 мл, пур-овер = 250 мл
  • Категории на английском: milk coffee, black coffee, pour over,
    iced coffee with milk, iced black coffee, coming back soon
  • «coming back soon» — сезонные позиции (is_seasonal=True)

РЕФАКТОРИНГ vs surf_parser.py
-------------------------------
DrinkMapper и RawMenuItem вынесены в shared_mapper.py.
Здесь только данные и точка входа parse_drinkit_menu().

МАСШТАБИРОВАНИЕ
---------------
При появлении публичного API Drinkit — добавить parse_from_api() с тем же
выходным форматом. DRINKIT_MENU_DATA обновлять при смене меню.

Зависимости
-----------
    pip install sqlalchemy pandas
    (shared_mapper.py должен быть в PYTHONPATH)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import Engine

from shared_mapper import DrinkMapper, RawMenuItem, normalize_drink_name

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# КОНСТАНТЫ
# ---------------------------------------------------------------------------

# competitor_id Drinkit из таблицы competitors (seed: id=3)
DRINKIT_COMPETITOR_ID: int = 3

# Объёмы по умолчанию по типу напитка.
# У Drinkit нет граммажа на карточках — стандарт сети.
_VOLUME_HOT_ML:      int = 300   # горячие молочные и чёрные
_VOLUME_ICED_ML:     int = 350   # холодные (iced)
_VOLUME_ESPRESSO_ML: int = 60    # эспрессо
_VOLUME_POUROVER_ML: int = 250   # пур-овер (V60)


# ---------------------------------------------------------------------------
# ДАННЫЕ МЕНЮ
# Извлечены из 9 скриншотов Drinkit мобильного приложения.
# Формат: (name_raw, price_rub, volume_ml, category_slug, is_signature, is_seasonal)
#
# Категории Drinkit → slug:
#   milk coffee           → espresso   (стандартные молочные напитки)
#   black coffee          → espresso   (чёрный кофе)
#   pour over             → espresso   (пур-овер)
#   iced coffee with milk → signature  (холодные молочные — авторские)
#   iced black coffee     → signature  (холодные чёрные)
#   coming back soon      → signature  (сезонные, is_seasonal=True)
# ---------------------------------------------------------------------------

DRINKIT_MENU_DATA: list[tuple] = [

    # ── milk coffee (горячие молочные) ──────────────────────────────────────
    # Стандартные напитки
    ("Cappuccino",               225, _VOLUME_HOT_ML, "espresso", False, False),
    ("Decaf Cappuccino",         225, _VOLUME_HOT_ML, "espresso", False, False),
    ("Latte",                    285, _VOLUME_HOT_ML, "espresso", False, False),
    ("Flat White",               275, _VOLUME_HOT_ML, "espresso", False, False),

    # Авторские молочные напитки
    ("Raf Blackberry",           375, _VOLUME_HOT_ML, "signature", True,  False),
    ("Raf Taro",                 375, _VOLUME_HOT_ML, "signature", True,  False),
    ("Cappuccino Taro",          375, _VOLUME_HOT_ML, "signature", True,  False),
    ("Protein Vanilla Latte",    405, _VOLUME_HOT_ML, "signature", True,  False),
    ("Salted Caramel Latte",     325, _VOLUME_HOT_ML, "signature", True,  False),
    ("Raspberry Mocha",          375, _VOLUME_HOT_ML, "signature", True,  False),
    ("Hazelnut Mousse Latte",    395, _VOLUME_HOT_ML, "signature", True,  False),
    ("Hazelnut Milk Mocha",      375, _VOLUME_HOT_ML, "signature", True,  False),
    ("Vanila Raf",               365, _VOLUME_HOT_ML, "signature", True,  False),
    ("Caffe Mocha",              365, _VOLUME_HOT_ML, "signature", True,  False),

    # ── black coffee (горячий чёрный кофе) ──────────────────────────────────
    ("Americano",                195, _VOLUME_HOT_ML, "espresso", False, False),
    ("Decaf Americano",          235, _VOLUME_HOT_ML, "espresso", False, False),
    ("Double Espresso",          195, _VOLUME_ESPRESSO_ML, "espresso", False, False),

    # Авторский горячий чёрный
    ("Hot Bumble Coffee",        465, _VOLUME_HOT_ML, "signature", True,  False),
    ("Hot Bumble Coffee with Caramel Syrup", 465, _VOLUME_HOT_ML, "signature", True, False),

    # ── pour over ────────────────────────────────────────────────────────────
    ("V60 Sumatra Gayo Belangi", 355, _VOLUME_POUROVER_ML, "espresso", True, False),
    ("V60 Rwanda Muteteli",      355, _VOLUME_POUROVER_ML, "espresso", True, False),

    # ── iced coffee with milk (холодные молочные) ────────────────────────────
    ("Iced Latte",               335, _VOLUME_ICED_ML, "signature", True,  False),
    ("Iced Hazelnut Mousse Latte", 395, _VOLUME_ICED_ML, "signature", True, False),
    ("Iced Latte Blackberry-Taro", 445, _VOLUME_ICED_ML, "signature", True, False),
    ("Iced Protein Vanilla Latte", 405, _VOLUME_ICED_ML, "signature", True, False),
    ("Iced Caffe Mocha",         385, _VOLUME_ICED_ML, "signature", True,  False),
    ("Iced Hazelnut Milk Mocha", 375, _VOLUME_ICED_ML, "signature", True,  False),
    ("Iced Raspberry Mocha",     405, _VOLUME_ICED_ML, "signature", True,  False),

    # ── iced black coffee (холодный чёрный) ─────────────────────────────────
    ("Iced Americano",           275, _VOLUME_ICED_ML, "espresso", False, False),
    ("Iced Coffee Hibiscus",     385, _VOLUME_ICED_ML, "signature", True,  False),
    ("Bumble Coffee",            475, _VOLUME_ICED_ML, "signature", True,  False),
    ("Iced Bumble Coffee with Caramel Syrup", 475, _VOLUME_ICED_ML, "signature", True, False),
    ("Coffee Tonic",             345, _VOLUME_ICED_ML, "signature", True,  False),
    ("Coffee Tonic Hibiscus",    395, _VOLUME_ICED_ML, "signature", True,  False),

    # ── coming back soon (сезонные, временно отсутствуют) ───────────────────
    # is_seasonal=True → пайплайн запишет status='returned' когда появятся
    # Цены неизвестны — зафиксируем 0, парсер пропустит их через _validate_scraped_df
    # Поэтому is_seasonal-позиции без цены НЕ включаем в дата-словарь:
    # они появятся автоматически при следующем парсинге когда вернутся в меню.
]


# ===========================================================================
# ПАРСЕР
# ===========================================================================

class DrinkitMenuParser:
    """
    Извлекает позиции меню из встроенного словаря DRINKIT_MENU_DATA.

    Аналог SurfMenuParser — офлайн-источник данных. Когда Drinkit откроет
    публичный API или появится возможность скрейпинга — добавить
    parse_from_api() с идентичным выходным форматом.
    """

    def parse(self) -> list[RawMenuItem]:
        """
        Возвращает список RawMenuItem из DRINKIT_MENU_DATA.
        Пропускает позиции с нулевой ценой (coming back soon без цены).
        """
        items = []
        skipped_zero = 0

        for name, price, volume, cat_slug, is_sig, is_seas in DRINKIT_MENU_DATA:
            if price <= 0 or volume <= 0:
                skipped_zero += 1
                logger.debug("Пропускаем «%s» — нулевая цена/объём", name)
                continue
            items.append(RawMenuItem(
                name=name,
                price_rub=float(price),
                volume_ml=volume,
                category_slug=cat_slug,
                is_signature=is_sig,
                is_seasonal=is_seas,
            ))

        logger.info(
            "DrinkitMenuParser: %d позиций | пропущено (нет цены): %d",
            len(items), skipped_zero,
        )
        return items


# ===========================================================================
# ТОЧКА ВХОДА
# ===========================================================================

def parse_drinkit_menu(
    engine: Engine,
    competitor_id: int = DRINKIT_COMPETITOR_ID,
    auto_register: bool = True,
) -> pd.DataFrame:
    """
    Полный цикл: парсинг → маппинг → DataFrame.

    Интерфейс идентичен parse_surf_menu() — оба парсера взаимозаменяемы
    с точки зрения пайплайна.

    Parameters
    ----------
    engine        : SQLAlchemy Engine к целевой БД
    competitor_id : PK конкурента (по умолчанию Drinkit = 3)
    auto_register : если True — новые напитки и размеры регистрируются в БД

    Returns
    -------
    pd.DataFrame с колонками: drink_id, size_id, price_rub, volume_ml
    Готов для передачи в run_price_pipeline().

    Пример
    ------
    from sqlalchemy import create_engine
    from drinkit_parser import parse_drinkit_menu
    from price_pipeline import run_price_pipeline

    engine = create_engine("postgresql+psycopg2://user:pass@host/db")
    df = parse_drinkit_menu(engine)
    result = run_price_pipeline(engine, competitor_id=3, scraped_df=df)
    print(result)
    """
    started   = datetime.now(timezone.utc)
    parser    = DrinkitMenuParser()
    raw_items = parser.parse()

    rows: list[dict] = []
    skipped: list[str] = []

    with engine.begin() as conn:
        mapper = DrinkMapper(conn, competitor_id, auto_register)

        for item in raw_items:
            drink_id = mapper.resolve_drink(item)
            if drink_id is None:
                skipped.append(item.name)
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
    df = df.drop_duplicates(subset=["drink_id", "size_id"], keep="first").reset_index(drop=True)

    logger.info(
        "parse_drinkit_menu завершён за %d мс: %d позиций → DataFrame %d строк | %s",
        elapsed_ms, len(raw_items), len(df), mapper.stats,
    )

    if skipped:
        logger.warning("Пропущено позиций (не смапировано): %s", skipped)

    return df


# ===========================================================================
# CLI
# ===========================================================================

if __name__ == "__main__":
    """
    Быстрый просмотр меню без БД.

    Использование:
        python drinkit_parser.py
        python drinkit_parser.py --show-normalized
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    show_norm = "--show-normalized" in sys.argv

    parser = DrinkitMenuParser()
    items  = parser.parse()

    print(f"\n{'─'*80}")
    print(f"  Drinkit — меню ({len(items)} позиций)")
    print(f"{'─'*80}")
    print(f"  {'Название':<50} {'Цена':>6}  {'Объём':>6}  {'Кат.':<10} {'Авт.'}")
    print(f"{'─'*80}")

    current_cat = ""
    for item in sorted(items, key=lambda x: (x.category_slug, x.name)):
        if item.category_slug != current_cat:
            current_cat = item.category_slug
            print(f"\n  [{current_cat.upper()}]")
        sig = "★" if item.is_signature else " "
        sea = "~" if item.is_seasonal else " "
        norm = f"  → {normalize_drink_name(item.name)}" if show_norm else ""
        print(
            f"  {item.name:<50} {item.price_rub:>6.0f}₽  {item.volume_ml:>5}мл  "
            f"{item.category_slug:<10} {sig}{sea}{norm}"
        )

    print(f"\n{'─'*80}")
    hot   = [i for i in items if not i.name.lower().startswith("iced") and "bumble" not in i.name.lower()]
    iced  = [i for i in items if i.name.lower().startswith("iced") or "bumble" in i.name.lower() or "tonic" in i.name.lower()]
    print(
        f"  Итого: {len(items)} | авторских: {sum(1 for i in items if i.is_signature)} | "
        f"сезонных: {sum(1 for i in items if i.is_seasonal)}"
    )
    if items:
        prices = [i.price_rub for i in items]
        print(f"  Диапазон цен: {min(prices):.0f}₽ – {max(prices):.0f}₽")
        pp100 = [i.price_rub / i.volume_ml * 100 for i in items]
        print(f"  price_per_100ml: мин={min(pp100):.1f}  макс={max(pp100):.1f}  "
              f"медиана={sorted(pp100)[len(pp100)//2]:.1f}")
    print(f"{'─'*80}\n")

    print("  Для записи в БД:")
    print("    from drinkit_parser import parse_drinkit_menu")
    print("    df = parse_drinkit_menu(engine)  # → run_price_pipeline(engine, 3, df)")
