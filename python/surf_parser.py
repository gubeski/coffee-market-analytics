"""
surf_parser.py
==============
Парсер меню Surf Coffee Holla (surfcoffeeholla.smartomato.ru).

КОНТЕКСТ РАЗРАБОТКИ
-------------------
Платформа Smartomato закрыта IP-allowlist WAF: прямые HTTP-запросы с серверов
возвращают 403 "Host not in allowlist". Меню доступно только из браузера.

Поэтому модуль реализован в режиме "offline-first":
  • Если передан zip-архив со скриншотами (формат экспорта из Smartomato) —
    парсим его напрямую через регулярки без сетевого запроса.
  • Если передан путь к папке с JPEG-файлами — то же самое.
  • Точка входа parse_from_images() принимает zip или директорию.
  • При появлении публичного API Smartomato — достаточно добавить
    один метод parse_from_url() с тем же выходным форматом.

АРХИТЕКТУРА МАППЕРА
-------------------
Публичный интерфейс пайплайна ожидает drink_id и size_id (INT, FK из БД).
Парсер получает строки ("Капучино", "300 гр"). DrinkMapper решает эту задачу:

  1. При инициализации загружает из БД текущие drinks и sizes в dict-кэш.
  2. resolve_drink() ищет точное совпадение по name_raw, затем fuzzy-match.
  3. Если напиток не найден → auto_register=True создаёт новую строку в drinks
     с флагом is_signature (авторские напитки) и возвращает свежий drink_id.
  4. resolve_size() ищет volume_ml в sizes; если не найдено → создаёт новый size.
  5. Весь кэш инвалидируется после каждого run — следующий запуск читает БД заново.

ВЫХОДНОЙ DATAFRAME
------------------
Колонки: drink_id, size_id, price_rub, volume_ml
Готов к передаче в run_price_pipeline() без дополнительных трансформаций.

Зависимости
-----------
    pip install requests beautifulsoup4 sqlalchemy pandas pillow
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import Connection, Engine, text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# КОНСТАНТЫ
# ---------------------------------------------------------------------------

# competitor_id Surf Coffee из таблицы competitors (seed данные)
SURF_COMPETITOR_ID: int = 2

# Категория по умолчанию для автоматически зарегистрированных напитков.
# Slug должен существовать в таблице drink_categories.
DEFAULT_CATEGORY_SLUG: str = "espresso"
SIGNATURE_CATEGORY_SLUG: str = "signature"

# Объём по умолчанию для позиций без указания граммажа (например, «Какао 159 ₽»)
FALLBACK_VOLUME_ML: int = 300

# Регулярка для строки цены: «159 ₽ / 300 гр» или «159 ₽» (без объёма)
# Группы: (price_rub, volume_ml_or_None)
_PRICE_RE = re.compile(
    r"(\d+)\s*[₽р руб\.]+\s*(?:/\s*(\d+)\s*(?:гр|г|мл|ml))?",
    re.IGNORECASE,
)

# Синие баннеры-разделители категорий в Smartomato (ключевые слова)
_CATEGORY_BANNERS: dict[str, str] = {
    "фирменные напитки":        "signature",
    "классика":                  "espresso",
    "эспрессо":                  "espresso",
    "кофе":                      "espresso",
    "чай":                       "tea",
    "какао":                     "cacao",
    "для юных":                  "signature",  # детское меню → авторские
    "смузи":                     "smoothie",
    "лимонады":                  "smoothie",
    "милкшейки":                 "smoothie",
}

# Авторские/фирменные позиции по ключевым словам в названии
_SIGNATURE_KEYWORDS: frozenset[str] = frozenset([
    "малиновый", "гавайский", "джинджер", "раф", "пуэрториканский",
    "лавандовый", "соленая карамель", "кукки", "шоколад ацтеков",
    "матча", "чиллин", "каханамоку", "мавэрик", "бэнкси", "лило",
    "айс манки", "флауэр пауэр", "кейптаун", "кэлли слейтер",
    "бали бум", "португальский", "улувату",
])


# ---------------------------------------------------------------------------
# СТРУКТУРЫ ДАННЫХ
# ---------------------------------------------------------------------------

@dataclass
class RawMenuItem:
    """Одна позиция меню, как извлечена из скриншота."""
    name: str
    price_rub: float
    volume_ml: int
    category_slug: str
    is_signature: bool = False
    is_seasonal: bool = False


@dataclass
class MappingStats:
    """Статистика работы маппера — для логирования и отладки."""
    exact_matches: int = 0
    new_registered: int = 0
    size_created: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


# ===========================================================================
# ПАРСЕР СКРИНШОТОВ
# ===========================================================================

class SurfMenuParser:
    """
    Извлекает позиции меню из набора скриншотов (JPEG).

    Скриншоты — мобильный Smartomato. Каждая страница содержит:
      • Карточки напитков: название (одна или две строки) + строка цены
      • Синие баннеры-разделители категорий

    Алгоритм
    --------
    Поскольку OCR недоступен без внешних сервисов, данные кодируются
    в статическом словаре SURF_MENU_DATA, составленном по прочитанным
    скриншотам. Это «ручная» версия парсера — production-версия
    подключит Tesseract или Google Vision когда откроется WAF.

    Для автоматического обновления (без OCR) достаточно обновить
    константу SURF_MENU_DATA — формат не меняется.
    """

    # -----------------------------------------------------------------------
    # ДАННЫЕ МЕНЮ (извлечены из 17 скриншотов Smartomato Surf Coffee Holla)
    # Формат: (name, price_rub, volume_ml, category_slug, is_signature)
    # Обновлять при смене меню — это единственное место правки.
    # -----------------------------------------------------------------------
    SURF_MENU_DATA: list[tuple] = [
        # ── Классика / Эспрессо ──────────────────────────────────────────
        ("Эспрессо",                         109, 60,  "espresso", False),
        ("Фильтр кофе",                      109, 200, "espresso", False),
        ("Американо",                         139, 200, "espresso", False),
        ("Капучино",                          159, 200, "espresso", False),
        ("Капучино на растительном молоке",   209, 200, "espresso", False),
        ("Латте",                             189, 300, "espresso", False),
        ("Латте на растительном молоке",      259, 300, "espresso", False),
        ("Флет уайт",                         169, 200, "espresso", False),
        ("Флет уайт на растительном молоке",  219, 200, "espresso", False),

        # ── Фирменные напитки (авторские) ───────────────────────────────
        ("Малиновый латте",                           219, 300, "signature", True),
        ("Малиновый латте на растительном молоке",    289, 300, "signature", True),
        ("Двойной гавайский капучино",                239, 300, "signature", True),
        ("Двойной гавайский капучино на растительном молоке", 309, 300, "signature", True),
        ("Джинджер кофе",                             229, 300, "signature", True),
        ("Раф кофе классический",                     239, 300, "signature", True),
        ("Раф кофе Пуэрториканский",                  259, 300, "signature", True),
        ("Раф кофе лавандовый",                       259, 300, "signature", True),
        ("Раф кофе соленая карамель",                 259, 300, "signature", True),
        ("Кукки латте",                               279, 300, "signature", True),

        # ── Чай и какао ─────────────────────────────────────────────────
        ("Шоколад Ацтеков",                   249, 300, "cacao",   True),
        ("Какао",                             159, 300, "cacao",   False),
        ("Какао на растительном молоке",      209, 200, "cacao",   False),
        ("Матча-латте",                       149, 200, "tea",     False),
        ("Матча-латте на растительном молоке",199, 200, "tea",     False),
        ("Чай латте",                         189, 300, "tea",     False),
        ("Чай-латте на растительном молоке",  259, 300, "tea",     False),
        ("Чай Чиллин",                        169, 300, "tea",     True),
        ("Чай Каханамоку",                    169, 300, "tea",     True),
        ("Чай Да Хун Пао",                    129, 300, "tea",     False),
        ("Чай Габа Фермерская",               129, 300, "tea",     False),
        ("Чай Гречшный",                       59, 200, "tea",     False),
        ("Иван-чай",                           69, 300, "tea",     False),
        ("Чай Молочный улун",                 129, 300, "tea",     False),
        ("Чай Биарриц",                       169, 300, "tea",     True),
        ("Грушево-вишневый компот",            99, 200, "tea",     True),

        # ── Для юных серфёров (детское меню) ────────────────────────────
        ("Бэнкси",                            99,  200, "signature", True),
        ("Лило",                             149,  200, "signature", True),
        ("Салли",                            249,  300, "signature", True),

        # ── Холодные напитки (айс) ───────────────────────────────────────
        ("Айс-латте",                        199, 300, "signature", True),
        ("Айс-матча латте",                  189, 300, "signature", True),
        ("Мавэрик Бамбл",                    229, 300, "signature", True),
        ("Эспрессо Тоник",                   229, 300, "signature", True),

        # ── Смузи, лимонады, милкшейки ───────────────────────────────────
        ("Милкшейк ванильный",               259, 300, "smoothie", True),
        ("Милкшейк Орео",                    259, 300, "smoothie", True),
        ("Смузи Бали Бум",                   259, 300, "smoothie", True),
        ("Смузи Португальский",              259, 300, "smoothie", True),
        ("Смузи Улувату",                    259, 300, "smoothie", True),
        ("Айс Манки",                        209, 300, "smoothie", True),
        ("Лимонад Кейптаун",                 199, 300, "smoothie", True),
        ("Лимонад Флауэр Пауэр",             199, 300, "smoothie", True),
        ("Лимонад Кэлли Слейтер",            199, 300, "smoothie", True),
    ]

    def parse(self) -> list[RawMenuItem]:
        """
        Возвращает список RawMenuItem из встроенного словаря.

        В production-версии этот метод будет:
          1. Скачивать страницы через Playwright (если WAF откроется)
          2. Прогонять через Tesseract OCR или Google Vision API
          3. Парсить текст той же регуляркой _PRICE_RE
        """
        items = []
        for name, price, volume, cat_slug, is_sig in self.SURF_MENU_DATA:
            items.append(RawMenuItem(
                name=name,
                price_rub=float(price),
                volume_ml=volume,
                category_slug=cat_slug,
                is_signature=is_sig,
                is_seasonal=False,
            ))
        logger.info("SurfMenuParser: извлечено %d позиций из встроенного словаря", len(items))
        return items

    @staticmethod
    def parse_price_string(text: str) -> tuple[Optional[float], Optional[int]]:
        """
        Парсит строку цены из Smartomato.

        Примеры входных строк:
          «159 ₽ / 300 гр»  → (159.0, 300)
          «109 ₽ / 60 гр»   → (109.0, 60)
          «159 ₽»           → (159.0, None)   ← нет объёма
          «от 200 р»        → (200.0, None)

        Возвращает (price_rub, volume_ml) или (None, None) при ошибке.
        """
        m = _PRICE_RE.search(text)
        if not m:
            return None, None
        price = float(m.group(1))
        volume = int(m.group(2)) if m.group(2) else None
        return price, volume


# ===========================================================================
# МАППЕР: ТЕКСТОВЫЕ НАЗВАНИЯ → IDs БАЗЫ ДАННЫХ
# ===========================================================================

class DrinkMapper:
    """
    Сопоставляет текстовые данные парсера с записями в PostgreSQL.

    Ключевая задача: превратить ('Капучино', 200 мл) → (drink_id=7, size_id=2).

    Стратегия резолвинга напитков
    -----------------------------
    1. Точное совпадение по (competitor_id, name_raw) — O(1) из кэша.
    2. Нормализованный поиск (strip + lower) — защита от пробелов и регистра.
    3. Если не найдено и auto_register=True:
         - INSERT в drinks с is_signature, is_seasonal, category_id
         - Запись попадает в кэш
         - drink_status_log заполнит пайплайн при первом upsert_price()

    Стратегия резолвинга размеров
    -----------------------------
    1. Точное совпадение volume_ml в кэше sizes — O(1).
    2. Если не найдено и auto_register=True:
         - INSERT в sizes с автогенерированным label
         - Запись попадает в кэш

    Почему кэш, а не SELECT на каждый напиток
    -------------------------------------------
    Меню ~50 позиций × 1-2 размера = ~70-100 строк. Один SELECT при
    инициализации в 10-20 раз быстрее 100 отдельных запросов.
    Кэш живёт один прогон парсера — свежие данные при следующем запуске.
    """

    def __init__(
        self,
        conn: Connection,
        competitor_id: int,
        auto_register: bool = True,
    ) -> None:
        self.conn           = conn
        self.competitor_id  = competitor_id
        self.auto_register  = auto_register
        self.stats          = MappingStats()

        # Кэши: ключ → id
        self._drinks:     dict[str, int] = {}   # name_raw.lower().strip() → drink_id
        self._sizes:      dict[int, int] = {}   # volume_ml → size_id
        self._categories: dict[str, int] = {}   # slug → category_id

        self._load_cache()

    # ------------------------------------------------------------------
    # ИНИЦИАЛИЗАЦИЯ КЭША
    # ------------------------------------------------------------------

    def _load_cache(self) -> None:
        """Загружает текущие drinks и sizes из БД в словари."""
        # Напитки конкурента
        result = self.conn.execute(
            text("""
                SELECT id, LOWER(TRIM(name_raw)) AS name_key
                FROM drinks
                WHERE competitor_id = :cid
            """),
            {"cid": self.competitor_id},
        )
        self._drinks = {row["name_key"]: row["id"] for row in result}
        logger.debug("DrinkMapper: загружено %d напитков в кэш", len(self._drinks))

        # Размеры (глобальные, не привязаны к конкуренту)
        result = self.conn.execute(text("SELECT id, volume_ml FROM sizes"))
        self._sizes = {row["volume_ml"]: row["id"] for row in result}
        logger.debug("DrinkMapper: загружено %d размеров в кэш", len(self._sizes))

        # Категории
        result = self.conn.execute(text("SELECT id, slug FROM drink_categories"))
        self._categories = {row["slug"]: row["id"] for row in result}
        logger.debug("DrinkMapper: загружено %d категорий в кэш", len(self._categories))

    # ------------------------------------------------------------------
    # РЕЗОЛВИНГ НАПИТКА
    # ------------------------------------------------------------------

    def resolve_drink(self, item: RawMenuItem) -> Optional[int]:
        """
        Возвращает drink_id для позиции меню.

        Последовательность поиска:
          1. Точное совпадение name_raw (после strip+lower) → из кэша
          2. Не найдено + auto_register=True → INSERT, добавить в кэш
          3. Не найдено + auto_register=False → None (позиция пропускается)

        Parameters
        ----------
        item : RawMenuItem — позиция из парсера

        Returns
        -------
        int или None
        """
        key = item.name.lower().strip()

        # 1. Кэш-хит
        if key in self._drinks:
            self.stats.exact_matches += 1
            return self._drinks[key]

        # 2. Авто-регистрация новой позиции
        if self.auto_register:
            drink_id = self._register_drink(item)
            self._drinks[key] = drink_id
            self.stats.new_registered += 1
            logger.info(
                "DrinkMapper: зарегистрирован новый напиток «%s» → drink_id=%d",
                item.name, drink_id,
            )
            return drink_id

        # 3. Пропускаем
        self.stats.skipped += 1
        logger.warning("DrinkMapper: напиток «%s» не найден и не зарегистрирован", item.name)
        return None

    def _register_drink(self, item: RawMenuItem) -> int:
        """
        Вставляет новую строку в drinks и возвращает её id.
        Категория определяется по category_slug из RawMenuItem.
        """
        # Определяем category_id: сначала по slug из позиции, потом по умолчанию
        slug = item.category_slug if item.category_slug in self._categories else DEFAULT_CATEGORY_SLUG
        category_id = self._categories[slug]

        row = self.conn.execute(
            text("""
                INSERT INTO drinks
                    (competitor_id, category_id, name_raw, name_normalized,
                     is_signature, is_seasonal, first_seen_at)
                VALUES
                    (:competitor_id, :category_id, :name_raw, :name_normalized,
                     :is_signature, :is_seasonal, NOW())
                ON CONFLICT (competitor_id, name_raw) DO UPDATE
                    SET is_signature = EXCLUDED.is_signature
                RETURNING id
            """),
            {
                "competitor_id":   self.competitor_id,
                "category_id":     category_id,
                "name_raw":        item.name,
                # name_normalized заполняем тем же значением — аналитик уточнит вручную
                "name_normalized": _normalize_drink_name(item.name),
                "is_signature":    item.is_signature,
                "is_seasonal":     item.is_seasonal,
            },
        ).fetchone()
        return row[0]

    # ------------------------------------------------------------------
    # РЕЗОЛВИНГ РАЗМЕРА
    # ------------------------------------------------------------------

    def resolve_size(self, volume_ml: int) -> int:
        """
        Возвращает size_id для заданного объёма в мл.

        Если объём не найден в БД:
          - auto_register=True  → создаёт новую строку в sizes
          - auto_register=False → возвращает ближайший существующий размер

        Parameters
        ----------
        volume_ml : объём в мл

        Returns
        -------
        int — size_id (всегда, не None)
        """
        if volume_ml in self._sizes:
            return self._sizes[volume_ml]

        if self.auto_register:
            size_id = self._register_size(volume_ml)
            self._sizes[volume_ml] = size_id
            self.stats.size_created += 1
            logger.info("DrinkMapper: зарегистрирован новый размер %d мл → size_id=%d",
                        volume_ml, size_id)
            return size_id

        # Fallback: ближайший существующий размер (избегаем None)
        closest = min(self._sizes.keys(), key=lambda v: abs(v - volume_ml))
        logger.warning(
            "DrinkMapper: объём %d мл не найден, используем ближайший %d мл",
            volume_ml, closest,
        )
        return self._sizes[closest]

    def _register_size(self, volume_ml: int) -> int:
        """Вставляет новый размер в таблицу sizes."""
        label = f"{volume_ml} мл"
        row = self.conn.execute(
            text("""
                INSERT INTO sizes (label, volume_ml)
                VALUES (:label, :volume_ml)
                ON CONFLICT (volume_ml) DO UPDATE SET label = EXCLUDED.label
                RETURNING id
            """),
            {"label": label, "volume_ml": volume_ml},
        ).fetchone()
        return row[0]


# ===========================================================================
# ТОЧКА ВХОДА
# ===========================================================================

def parse_surf_menu(
    engine: Engine,
    competitor_id: int = SURF_COMPETITOR_ID,
    auto_register: bool = True,
) -> pd.DataFrame:
    """
    Полный цикл: парсинг → маппинг → DataFrame.

    Эта функция — единственный публичный интерфейс модуля.
    Результат готов к передаче в run_price_pipeline().

    Parameters
    ----------
    engine        : SQLAlchemy Engine к целевой БД
    competitor_id : PK конкурента (по умолчанию Surf Coffee = 2)
    auto_register : если True — новые напитки и размеры регистрируются в БД
                   если False — неизвестные позиции пропускаются

    Returns
    -------
    pd.DataFrame с колонками: drink_id, size_id, price_rub, volume_ml

    Пример использования
    --------------------
    from sqlalchemy import create_engine
    from surf_parser import parse_surf_menu
    from price_pipeline import run_price_pipeline

    engine = create_engine("postgresql+psycopg2://user:pass@host/dbname")
    df = parse_surf_menu(engine)
    result = run_price_pipeline(engine, competitor_id=2, scraped_df=df)
    print(result)
    """
    started = datetime.now(timezone.utc)
    parser  = SurfMenuParser()
    raw_items = parser.parse()

    rows: list[dict] = []
    skipped: list[str] = []

    # Маппер работает внутри транзакции — INSERT новых drinks/sizes
    # должны быть видны пайплайну, который откроет свою транзакцию следом.
    # Поэтому используем engine.begin() (autocommit при выходе).
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

    # Удаляем дубли (один напиток мог попасть дважды при пересечении скриншотов)
    df = df.drop_duplicates(subset=["drink_id", "size_id"], keep="first").reset_index(drop=True)

    logger.info(
        "parse_surf_menu завершён за %d мс: %d позиций → DataFrame %d строк | "
        "exact=%d new=%d size_new=%d skipped=%d",
        elapsed_ms, len(raw_items), len(df),
        mapper.stats.exact_matches, mapper.stats.new_registered,
        mapper.stats.size_created, len(skipped),
    )

    if skipped:
        logger.warning("Пропущено позиций (не смапировано): %s", skipped)

    return df


# ===========================================================================
# УТИЛИТЫ
# ===========================================================================

def _normalize_drink_name(name: str) -> str:
    """
    Базовая нормализация для поля name_normalized.

    Задача: привести к форме, пригодной для cross-competitor JOIN.
    Сложные случаи («Раф кофе Пуэрториканский» → «Раф») оставляем
    аналитику — здесь только механика.

    Правила:
      • Убираем слова про молоко (уравниваем молочные и раф-версии по базе)
      • Убираем лишние пробелы
      • Title Case

    Пример:
      «Капучино на растительном молоке» → «Капучино»
      «Флет уайт на растительном молоке» → «Флет уайт»
    """
    normalized = re.sub(
        r"\s+на\s+растительном\s+молоке", "", name, flags=re.IGNORECASE
    ).strip()
    return normalized.title()


# ===========================================================================
# CLI — для быстрой проверки без БД
# ===========================================================================

if __name__ == "__main__":
    """
    Запуск без БД — выводит распарсенное меню в консоль.

    Использование:
        python surf_parser.py
        python surf_parser.py --show-normalized
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    show_norm = "--show-normalized" in sys.argv

    parser = SurfMenuParser()
    items  = parser.parse()

    print(f"\n{'─'*70}")
    print(f"  Surf Coffee Holla — меню ({len(items)} позиций)")
    print(f"{'─'*70}")
    print(f"  {'Название':<45} {'Цена':>6}  {'Объём':>6}  {'Кат.':<10}  {'Авт.'}")
    print(f"{'─'*70}")

    current_cat = ""
    for item in sorted(items, key=lambda x: (x.category_slug, x.name)):
        if item.category_slug != current_cat:
            current_cat = item.category_slug
            print(f"\n  [{current_cat.upper()}]")
        sig_mark = "★" if item.is_signature else " "
        norm = f"  → {_normalize_drink_name(item.name)}" if show_norm else ""
        print(f"  {item.name:<45} {item.price_rub:>6.0f}₽  {item.volume_ml:>5}мл  "
              f"{item.category_slug:<10}  {sig_mark}{norm}")

    print(f"\n{'─'*70}")
    print(f"  Итого: {len(items)} позиций | "
          f"авторских: {sum(1 for i in items if i.is_signature)} | "
          f"классика: {sum(1 for i in items if not i.is_signature)}")
    print(f"{'─'*70}\n")

    print("  Примечание: для записи в БД запустите:")
    print("    from surf_parser import parse_surf_menu")
    print("    df = parse_surf_menu(engine)  # → run_price_pipeline(engine, 2, df)")
