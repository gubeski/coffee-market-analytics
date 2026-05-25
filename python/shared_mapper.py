"""
shared_mapper.py
================
Общий модуль маппинга: текстовые данные парсеров → integer ID из PostgreSQL.

Используется всеми парсерами проекта:
    from shared_mapper import DrinkMapper, RawMenuItem, normalize_drink_name

Архитектура
-----------
DrinkMapper — единственный класс, который знает о структуре таблиц drinks и
sizes. Парсеры работают только с RawMenuItem и не импортируют ничего из
SQLAlchemy напрямую — вся DB-логика изолирована здесь.

Стратегия резолвинга (одинакова для всех конкурентов)
------------------------------------------------------
Напиток:
  1. Точное совпадение name_raw.lower().strip() → из dict-кэша (O(1))
  2. Не найдено + auto_register=True → INSERT drinks, добавить в кэш
  3. Не найдено + auto_register=False → вернуть None (позиция пропускается)

Размер:
  1. Точное совпадение volume_ml → из dict-кэша (O(1))
  2. Не найдено + auto_register=True → INSERT sizes, добавить в кэш
  3. Не найдено + auto_register=False → ближайший существующий (безопасный fallback)

Зависимости
-----------
    pip install sqlalchemy
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import Connection, text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Slug категории по умолчанию — используется при авторегистрации,
# если категория не определена. Slug обязан существовать в drink_categories.
# ---------------------------------------------------------------------------
DEFAULT_CATEGORY_SLUG: str = "espresso"


# ===========================================================================
# СТРУКТУРЫ ДАННЫХ
# ===========================================================================

@dataclass
class RawMenuItem:
    """
    Позиция меню в формате, который возвращает парсер.

    Поля
    ----
    name          : название напитка (оригинальное, как в меню)
    price_rub     : цена в рублях
    volume_ml     : объём в мл
    category_slug : slug из drink_categories (espresso / signature / tea / ...)
    is_signature  : True = авторский напиток
    is_seasonal   : True = сезонный (может исчезать и возвращаться)
    """
    name: str
    price_rub: float
    volume_ml: int
    category_slug: str
    is_signature: bool = False
    is_seasonal: bool = False


@dataclass
class MappingStats:
    """Счётчики операций маппера — для логов и тестов."""
    exact_matches: int = 0
    new_registered: int = 0
    size_created: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"exact={self.exact_matches} new={self.new_registered} "
            f"size_new={self.size_created} skipped={self.skipped}"
        )


# ===========================================================================
# МАППЕР
# ===========================================================================

class DrinkMapper:
    """
    Переводит (name_raw, volume_ml) → (drink_id, size_id) через кэш + БД.

    Создавать один экземпляр на прогон парсера. Кэш живёт в памяти
    до конца объекта — при следующем прогоне пересоздать DrinkMapper.

    Parameters
    ----------
    conn          : активное SQLAlchemy-соединение (внутри транзакции)
    competitor_id : PK конкурента из таблицы competitors
    auto_register : если True — новые напитки и размеры регистрируются в БД
    """

    def __init__(
        self,
        conn: Connection,
        competitor_id: int,
        auto_register: bool = True,
    ) -> None:
        self.conn          = conn
        self.competitor_id = competitor_id
        self.auto_register = auto_register
        self.stats         = MappingStats()

        # Кэши: ключ → id (заполняются один раз при инициализации)
        self._drinks:     dict[str, int] = {}   # name_raw.lower().strip() → drink_id
        self._sizes:      dict[int, int] = {}   # volume_ml → size_id
        self._categories: dict[str, int] = {}   # slug → category_id

        self._load_cache()

    # ------------------------------------------------------------------
    # ИНИЦИАЛИЗАЦИЯ КЭША
    # ------------------------------------------------------------------

    def _load_cache(self) -> None:
        """
        Один SELECT на старте — кладём все нужные данные в dict.
        Значительно быстрее N отдельных SELECT при 50-100 позициях в меню.
        """
        # Напитки конкурента
        rows = self.conn.execute(
            text("""
                SELECT id, LOWER(TRIM(name_raw)) AS name_key
                FROM drinks
                WHERE competitor_id = :cid
            """),
            {"cid": self.competitor_id},
        ).mappings()
        self._drinks = {row["name_key"]: row["id"] for row in rows}

        # Размеры (глобальные — не привязаны к конкуренту)
        rows = self.conn.execute(text("SELECT id, volume_ml FROM sizes")).mappings()
        self._sizes = {row["volume_ml"]: row["id"] for row in rows}

        # Категории
        rows = self.conn.execute(text("SELECT id, slug FROM drink_categories")).mappings()
        self._categories = {row["slug"]: row["id"] for row in rows}

        logger.debug(
            "DrinkMapper[competitor=%d]: кэш загружен — "
            "drinks=%d, sizes=%d, categories=%d",
            self.competitor_id, len(self._drinks),
            len(self._sizes), len(self._categories),
        )

    # ------------------------------------------------------------------
    # ПУБЛИЧНЫЙ API
    # ------------------------------------------------------------------

    def resolve_drink(self, item: RawMenuItem) -> Optional[int]:
        """
        Возвращает drink_id для позиции меню.

        Последовательность поиска:
          1. Точное совпадение (strip + lower) → кэш
          2. Не найдено + auto_register → INSERT в drinks → кэш
          3. Не найдено + not auto_register → None

        Returns
        -------
        int | None  — None означает «пропустить позицию»
        """
        key = item.name.lower().strip()

        if key in self._drinks:
            self.stats.exact_matches += 1
            return self._drinks[key]

        if self.auto_register:
            drink_id = self._register_drink(item)
            self._drinks[key] = drink_id
            self.stats.new_registered += 1
            logger.info(
                "DrinkMapper: зарегистрирован «%s» → drink_id=%d "
                "(competitor=%d, category=%s, signature=%s)",
                item.name, drink_id, self.competitor_id,
                item.category_slug, item.is_signature,
            )
            return drink_id

        self.stats.skipped += 1
        logger.warning(
            "DrinkMapper: «%s» не найден и auto_register=False → пропущен",
            item.name,
        )
        return None

    def resolve_size(self, volume_ml: int) -> int:
        """
        Возвращает size_id для заданного объёма.

        Всегда возвращает int — при отсутствии volume_ml либо создаёт
        новый size (auto_register=True), либо берёт ближайший (fallback).

        Returns
        -------
        int — size_id, никогда None
        """
        if volume_ml in self._sizes:
            return self._sizes[volume_ml]

        if self.auto_register:
            size_id = self._register_size(volume_ml)
            self._sizes[volume_ml] = size_id
            self.stats.size_created += 1
            logger.info("DrinkMapper: зарегистрирован размер %d мл → size_id=%d",
                        volume_ml, size_id)
            return size_id

        # Безопасный fallback: ближайший существующий объём
        if not self._sizes:
            raise RuntimeError(
                "Таблица sizes пуста. Запустите seed-скрипт перед парсером."
            )
        closest_vol = min(self._sizes, key=lambda v: abs(v - volume_ml))
        logger.warning(
            "DrinkMapper: объём %d мл отсутствует в sizes, "
            "используем ближайший %d мл",
            volume_ml, closest_vol,
        )
        return self._sizes[closest_vol]

    # ------------------------------------------------------------------
    # ПРИВАТНЫЕ МЕТОДЫ РЕГИСТРАЦИИ
    # ------------------------------------------------------------------

    def _register_drink(self, item: RawMenuItem) -> int:
        """
        INSERT в drinks. ON CONFLICT DO UPDATE обновляет флаги — идемпотентен.

        name_normalized заполняется функцией normalize_drink_name().
        Аналитик может уточнить его вручную для cross-competitor JOIN.
        """
        slug = item.category_slug if item.category_slug in self._categories \
               else DEFAULT_CATEGORY_SLUG
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
                    SET is_signature = EXCLUDED.is_signature,
                        is_seasonal  = EXCLUDED.is_seasonal
                RETURNING id
            """),
            {
                "competitor_id":   self.competitor_id,
                "category_id":     category_id,
                "name_raw":        item.name,
                "name_normalized": normalize_drink_name(item.name),
                "is_signature":    item.is_signature,
                "is_seasonal":     item.is_seasonal,
            },
        ).fetchone()
        return row[0]

    def _register_size(self, volume_ml: int) -> int:
        """INSERT в sizes. ON CONFLICT DO UPDATE — идемпотентен."""
        row = self.conn.execute(
            text("""
                INSERT INTO sizes (label, volume_ml)
                VALUES (:label, :volume_ml)
                ON CONFLICT (volume_ml) DO UPDATE SET label = EXCLUDED.label
                RETURNING id
            """),
            {"label": f"{volume_ml} мл", "volume_ml": volume_ml},
        ).fetchone()
        return row[0]


# ===========================================================================
# УТИЛИТЫ НОРМАЛИЗАЦИИ
# ===========================================================================

# Словари для нормализации: оригинальное слово → стандартное.
# Используются при кросс-конкурентном JOIN в v_benchmark_delta.
_EN_TO_RU: dict[str, str] = {
    # Базовые напитки
    "espresso":        "Эспрессо",
    "double espresso": "Двойной Эспрессо",
    "americano":       "Американо",
    "cappuccino":      "Капучино",
    "latte":           "Латте",
    "flat white":      "Флет Уайт",
    "raf":             "Раф",
    # Молочные вариации
    "caffe mocha":     "Мокко",
    "mocha":           "Мокко",
    # Холодные
    "iced latte":      "Айс-Латте",
    "iced americano":  "Айс Американо",
}

# Слова и фразы, которые убираем при нормализации
_STRIP_PHRASES: list[str] = [
    r"\s+на\s+растительном\s+молоке",   # Surf Coffee: «Капучино на раст. молоке»
    r"\s+with\s+caramel\s+syrup",        # Drinkit
    r"\s+decaf\b",                       # «Decaf Americano» → «Americano»
    r"\bdecaf\s+",
    r"\biced\s+",                        # «Iced Latte» → «Latte» (затем EN→RU)
    r"\bprotein\s+",
    r"\s+milk\s+mocha",
    r"\s+mousse",
]


def normalize_drink_name(name: str) -> str:
    """
    Приводит название напитка к стандартной форме для кросс-конкурентного JOIN.

    Алгоритм:
      1. Убираем модификаторы (растительное молоко, iced, decaf, protein...)
      2. Пробуем перевести EN → RU по словарю
      3. Title Case

    Примеры
    -------
    «Капучино на растительном молоке» → «Капучино»
    «Decaf Americano»                 → «Американо»
    «Iced Latte»                      → «Айс-Латте»  ← iced убираем для сравнения
    «Flat White»                      → «Флет Уайт»
    «Cappuccino»                      → «Капучино»
    «Raf Taro»                        → «Раф Taro»   ← только «Raf» маппится
    """
    result = name.strip()

    # Шаг 1: убираем стоп-фразы
    for pattern in _STRIP_PHRASES:
        result = re.sub(pattern, "", result, flags=re.IGNORECASE).strip()

    # Шаг 2: попытка перевода по словарю (точное совпадение после нормализации)
    key = result.lower().strip()
    if key in _EN_TO_RU:
        return _EN_TO_RU[key]

    # Шаг 3: частичная замена известных корней
    for en_word, ru_word in _EN_TO_RU.items():
        if result.lower().startswith(en_word):
            suffix = result[len(en_word):].strip()
            result = f"{ru_word} {suffix}".strip().title()
            break

    return result.title()


def parse_price_string(text_str: str) -> tuple[Optional[float], Optional[int]]:
    """
    Универсальный парсер строки цены.
    Работает с форматами Smartomato (Surf) и Drinkit.

    Форматы:
      «159 ₽ / 300 гр»  → (159.0, 300)
      «109 ₽ / 60 гр»   → (109.0, 60)
      «225 ₽»           → (225.0, None)   ← Drinkit: объём на карточке напитка
      «355 ₽»           → (355.0, None)

    Returns
    -------
    (price_rub, volume_ml) — volume_ml может быть None
    """
    _PRICE_RE = re.compile(
        r"(\d+)\s*[₽р руб\.]+\s*(?:/\s*(\d+)\s*(?:гр|г|мл|ml))?",
        re.IGNORECASE,
    )
    m = _PRICE_RE.search(text_str)
    if not m:
        return None, None
    price = float(m.group(1))
    volume = int(m.group(2)) if m.group(2) else None
    return price, volume
