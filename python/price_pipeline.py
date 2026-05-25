"""
price_pipeline.py
=================
Пайплайн записи результатов парсинга в PostgreSQL.

Публичный интерфейс
-------------------
    run_price_pipeline(engine, competitor_id, scraped_df) -> PipelineResult

Внутренние функции (не вызывать напрямую)
-----------------------------------------
    _open_parse_run()       — создаёт запись в parse_runs, возвращает run_id
    _load_current_prices()  — читает актуальные цены из price_history (is_current=TRUE)
    detect_changes()        — Pandas-диффинг: new / price_up / price_down / removed
    upsert_price()          — SCD Type 2: закрывает старые записи, открывает новые
    _write_status_log()     — вставляет события в drink_status_log
    _write_alert_events()   — вставляет алерты в alert_events
    _close_parse_run()      — обновляет parse_runs: status / finished_at / items_scraped

Требования к входному DataFrame (scraped_df)
--------------------------------------------
    drink_id     : int    — PK из таблицы drinks (парсер должен резолвить заранее)
    size_id      : int    — PK из таблицы sizes
    price_rub    : float  — цена в рублях
    volume_ml    : int    — объём стакана в мл (нужен для price_per_100ml)

Зависимости
-----------
    pip install sqlalchemy pandas psycopg2-binary
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import pandas as pd
from sqlalchemy import Connection, Engine, text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

# Порог изменения цены для алерта price_spike / price_drop (в процентах).
# Если delta_pct >= 5% — пишем алерт. Настраивается под бизнес-логику.
PRICE_CHANGE_ALERT_THRESHOLD_PCT: float = 5.0

# Sentinel-дата «запись актуальна» для SCD Type 2.
SCD_INFINITY: date = date(9999, 12, 31)

# Имена столбцов входного DataFrame — фиксируем контракт в одном месте.
_REQUIRED_COLUMNS: set[str] = {"drink_id", "size_id", "price_rub", "volume_ml"}


# ---------------------------------------------------------------------------
# Результирующий объект (вместо голого dict — удобнее логировать и тестировать)
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    """Итог одного прогона пайплайна для одного конкурента."""
    parse_run_id: int
    competitor_id: int
    items_scraped: int = 0
    new_drinks: int = 0
    price_changes: int = 0
    removed_drinks: int = 0
    alerts_written: int = 0
    status: str = "success"          # 'success' | 'failed'
    error: Optional[str] = None
    timing: dict = field(default_factory=dict)

    def __str__(self) -> str:  # noqa: D105
        return (
            f"[run={self.parse_run_id} | competitor={self.competitor_id}] "
            f"status={self.status} | scraped={self.items_scraped} | "
            f"new={self.new_drinks} | changes={self.price_changes} | "
            f"removed={self.removed_drinks} | alerts={self.alerts_written}"
        )


# ===========================================================================
# ПУБЛИЧНЫЙ ИНТЕРФЕЙС
# ===========================================================================

def run_price_pipeline(
    engine: Engine,
    competitor_id: int,
    scraped_df: pd.DataFrame,
) -> PipelineResult:
    """
    Точка входа пайплайна. Принимает результат парсинга и записывает
    всё в БД в рамках одной транзакции.

    Последовательность шагов
    ------------------------
    1. Валидация входного DataFrame.
    2. Открытие parse_run (строка-маркер в parse_runs).
    3. Загрузка актуальных цен из БД (is_current=TRUE).
    4. detect_changes() — Pandas-диффинг нового vs текущего.
    5. upsert_price()   — SCD Type 2 для изменившихся и новых позиций.
    6. Запись drink_status_log.
    7. Запись alert_events.
    8. Закрытие parse_run (финальный статус + счётчики).

    Всё внутри одной BEGIN/COMMIT транзакции SQLAlchemy.
    При любом исключении — автоматический ROLLBACK.

    Parameters
    ----------
    engine        : SQLAlchemy Engine, подключённый к целевой БД.
    competitor_id : PK конкурента из таблицы competitors.
    scraped_df    : DataFrame с колонками drink_id, size_id, price_rub, volume_ml.

    Returns
    -------
    PipelineResult — итог прогона (даже при ошибке, со status='failed').
    """
    started = datetime.now(timezone.utc)

    # Валидация: ловим ошибки до открытия транзакции
    _validate_scraped_df(scraped_df)

    result = PipelineResult(parse_run_id=-1, competitor_id=competitor_id)

    try:
        # Одна транзакция на весь пайплайн.
        # with engine.begin() — автоматически делает COMMIT при выходе
        # и ROLLBACK при исключении. Partial commits невозможны.
        with engine.begin() as conn:

            # 1. Открываем parse_run — он нужен как FK для всех последующих вставок
            run_id = _open_parse_run(conn, competitor_id)
            result.parse_run_id = run_id
            logger.info("parse_run открыт: run_id=%d, competitor_id=%d", run_id, competitor_id)

            # 2. Загружаем текущие цены из БД в Pandas DataFrame
            t0 = datetime.now(timezone.utc)
            current_df = _load_current_prices(conn, competitor_id)
            result.timing["load_current_ms"] = _ms(t0)
            logger.debug("Загружено текущих записей: %d", len(current_df))

            # 3. Pandas-диффинг: определяем что изменилось
            t0 = datetime.now(timezone.utc)
            changes = detect_changes(scraped_df, current_df)
            result.timing["detect_changes_ms"] = _ms(t0)
            logger.info(
                "Изменений: new=%d, price_up=%d, price_down=%d, removed=%d",
                len(changes["new"]), len(changes["price_up"]),
                len(changes["price_down"]), len(changes["removed"]),
            )

            # 4. SCD Type 2 — закрываем старые / открываем новые записи
            t0 = datetime.now(timezone.utc)
            new_ph_ids = upsert_price(conn, run_id, changes)
            result.timing["upsert_price_ms"] = _ms(t0)

            # 5. drink_status_log — записываем события жизненного цикла
            t0 = datetime.now(timezone.utc)
            _write_status_log(conn, run_id, changes)
            result.timing["status_log_ms"] = _ms(t0)

            # 6. alert_events — формируем очередь алертов
            t0 = datetime.now(timezone.utc)
            alerts_count = _write_alert_events(conn, run_id, changes, new_ph_ids)
            result.timing["alert_events_ms"] = _ms(t0)

            # 7. Закрываем parse_run с финальными счётчиками
            items_scraped = len(scraped_df)
            _close_parse_run(conn, run_id, status="success", items_scraped=items_scraped)

            # Заполняем результат
            result.items_scraped   = items_scraped
            result.new_drinks      = len(changes["new"])
            result.price_changes   = len(changes["price_up"]) + len(changes["price_down"])
            result.removed_drinks  = len(changes["removed"])
            result.alerts_written  = alerts_count
            result.timing["total_ms"] = _ms(started)

        logger.info("Пайплайн завершён: %s", result)

    except Exception as exc:
        # Транзакция уже откачена SQLAlchemy (engine.begin() поймал исключение).
        # Открываем отдельное соединение, чтобы отметить run как failed.
        result.status = "failed"
        result.error  = str(exc)
        logger.exception("Пайплайн упал: competitor_id=%d", competitor_id)

        if result.parse_run_id != -1:
            try:
                with engine.begin() as conn:
                    _close_parse_run(conn, result.parse_run_id, status="failed", error=str(exc))
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось закрыть parse_run после ошибки")

    return result


# ===========================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ: ПАРС-РАН
# ===========================================================================

def _open_parse_run(conn: Connection, competitor_id: int) -> int:
    """
    Создаёт строку в parse_runs со статусом 'running'.
    Возвращает сгенерированный id.
    """
    row = conn.execute(
        text("""
            INSERT INTO parse_runs (competitor_id, started_at, status, items_scraped)
            VALUES (:competitor_id, NOW(), 'running', 0)
            RETURNING id
        """),
        {"competitor_id": competitor_id},
    ).fetchone()
    return row[0]


def _close_parse_run(
    conn: Connection,
    run_id: int,
    status: str,
    items_scraped: int = 0,
    error: Optional[str] = None,
) -> None:
    """Финализирует parse_run: ставит статус, время окончания и счётчик."""
    conn.execute(
        text("""
            UPDATE parse_runs
               SET status        = :status,
                   finished_at   = NOW(),
                   items_scraped = :items_scraped,
                   error_message = :error
             WHERE id = :run_id
        """),
        {"run_id": run_id, "status": status,
         "items_scraped": items_scraped, "error": error},
    )


# ===========================================================================
# ЗАГРУЗКА ТЕКУЩИХ ЦЕН
# ===========================================================================

def _load_current_prices(conn: Connection, competitor_id: int) -> pd.DataFrame:
    """
    Читает из price_history все актуальные записи для конкурента
    (is_current = TRUE). Результат используется как «левая» часть диффинга.

    Возвращаемые колонки
    --------------------
    price_history_id, drink_id, size_id, price_rub, price_per_100ml, valid_from
    """
    query = text("""
        SELECT
            ph.id              AS price_history_id,
            ph.drink_id,
            ph.size_id,
            ph.price_rub,
            ph.price_per_100ml,
            ph.valid_from
        FROM price_history ph
        JOIN drinks d ON d.id = ph.drink_id
        WHERE d.competitor_id = :competitor_id
          AND ph.is_current   = TRUE
    """)
    result = conn.execute(query, {"competitor_id": competitor_id})
    rows = result.mappings().fetchall()
    df = pd.DataFrame([dict(r) for r in rows])

    # Приводим числовые типы: Decimal из psycopg2 → float для Pandas-арифметики
    for col in ("price_rub", "price_per_100ml"):
        if col in df.columns:
            df[col] = df[col].astype(float)

    return df


# ===========================================================================
# DETECT_CHANGES — PANDAS ДИФФИНГ
# ===========================================================================

def detect_changes(
    scraped_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Сравнивает свежеспаршенные цены с актуальными в БД.
    Классифицирует каждую позицию по типу события.

    Логика диффинга
    ---------------
    Ключ сравнения: (drink_id, size_id) — уникальная комбинация.

    Категории результата
    --------------------
    'new'        — позиция есть в scraped, но отсутствует в current.
                   Первое появление напитка.

    'price_up'   — позиция есть в обоих, цена в scraped ВЫШЕ current.
                   Конкурент поднял цену.

    'price_down' — позиция есть в обоих, цена в scraped НИЖЕ current.
                   Конкурент снизил цену или провёл акцию.

    'unchanged'  — цена не изменилась. Алертов и SCD-операций не требует.

    'removed'    — позиция есть в current, но отсутствует в scraped.
                   Напиток исчез из меню.

    Parameters
    ----------
    scraped_df  : DataFrame от парсера. Обязательные колонки:
                  drink_id, size_id, price_rub, volume_ml
    current_df  : DataFrame из _load_current_prices(). Колонки:
                  price_history_id, drink_id, size_id, price_rub,
                  price_per_100ml, valid_from

    Returns
    -------
    dict с ключами 'new', 'price_up', 'price_down', 'unchanged', 'removed'.
    Каждое значение — DataFrame с нужными полями для следующих шагов.
    Для 'price_up' и 'price_down' добавлены поля old_price_rub и delta_pct.
    """
    # Нормализуем типы ключей, чтобы merge не упал на int64 vs object
    scraped = scraped_df.copy()
    current = current_df.copy()
    scraped[["drink_id", "size_id"]] = scraped[["drink_id", "size_id"]].astype(int)
    if not current.empty:
        current[["drink_id", "size_id"]] = current[["drink_id", "size_id"]].astype(int)

    # Вычисляем price_per_100ml для свежих данных
    # Формула: price_rub / volume_ml * 100, округляем до 2 знаков
    scraped["price_per_100ml"] = (
        scraped["price_rub"] / scraped["volume_ml"] * 100
    ).round(2)

    # Если текущих цен нет (первый запуск) — все позиции новые
    if current.empty:
        scraped_new = scraped.copy()
        scraped_new["price_rub_new"] = scraped_new["price_rub"]
        return {
            "new":        scraped_new,
            "price_up":   pd.DataFrame(),
            "price_down": pd.DataFrame(),
            "unchanged":  pd.DataFrame(),
            "removed":    pd.DataFrame(),
        }

    # Outer join по ключу (drink_id, size_id)
    # suffixes: _new — из парсера, _cur — из БД
    merged = scraped.merge(
        current[["drink_id", "size_id", "price_rub", "price_history_id"]],
        on=["drink_id", "size_id"],
        how="outer",
        suffixes=("_new", "_cur"),
        indicator=True,  # добавляет колонку _merge: left_only / right_only / both
    )

    # --- Маски по типу события ---

    mask_new      = merged["_merge"] == "left_only"   # есть только у парсера
    mask_removed  = merged["_merge"] == "right_only"  # есть только в БД
    mask_both     = merged["_merge"] == "both"         # есть в обоих

    # Для позиций, присутствующих в обоих — сравниваем цены
    # Округляем до копеек перед сравнением, чтобы избежать float-артефактов
    if mask_both.any():
        price_changed = mask_both & (
            merged["price_rub_new"].round(2) != merged["price_rub_cur"].round(2)
        )
        mask_price_up   = price_changed & (merged["price_rub_new"] > merged["price_rub_cur"])
        mask_price_down = price_changed & (merged["price_rub_new"] < merged["price_rub_cur"])
        mask_unchanged  = mask_both & ~price_changed
    else:
        mask_price_up   = pd.Series(False, index=merged.index)
        mask_price_down = pd.Series(False, index=merged.index)
        mask_unchanged  = mask_both

    # --- Хелпер: добавляет дельты для price_up / price_down ---
    def _add_deltas(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["old_price_rub"] = df["price_rub_cur"]
        df["delta_rub"]     = (df["price_rub_new"] - df["price_rub_cur"]).round(2)
        df["delta_pct"]     = (
            df["delta_rub"] / df["price_rub_cur"] * 100
        ).round(2)
        return df

    # --- Финальные субсеты ---
    # Для 'new' и 'removed' переименовываем price_rub из нужного источника
    def _cols_new(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"price_rub_new": "price_rub"})

    def _cols_removed(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"price_rub_cur": "price_rub"})

    return {
        "new":        _cols_new(merged[mask_new].copy()),
        "price_up":   _add_deltas(merged[mask_price_up].copy()),
        "price_down": _add_deltas(merged[mask_price_down].copy()),
        "unchanged":  merged[mask_unchanged].copy(),
        "removed":    _cols_removed(merged[mask_removed].copy()),
    }


# ===========================================================================
# UPSERT_PRICE — SCD TYPE 2
# ===========================================================================

def upsert_price(
    conn: Connection,
    run_id: int,
    changes: dict[str, pd.DataFrame],
) -> dict[str, list[int]]:
    """
    Реализует SCD Type 2 для price_history.

    Операции
    --------
    Новые напитки ('new')
        INSERT в price_history (valid_from=today, valid_to=9999-12-31, is_current=TRUE).

    Изменение цены ('price_up', 'price_down')
        1. UPDATE старой записи: valid_to = yesterday, is_current = FALSE.
        2. INSERT новой записи:  valid_from = today, is_current = TRUE.

    Удалённые напитки ('removed')
        UPDATE: valid_to = yesterday, is_current = FALSE.
        Новая запись НЕ создаётся — напиток исчез из меню.

    Неизменённые ('unchanged')
        Ничего не делаем.

    Parameters
    ----------
    conn    : активное SQLAlchemy-соединение внутри транзакции.
    run_id  : id текущего parse_run (FK для новых строк price_history).
    changes : dict из detect_changes().

    Returns
    -------
    dict с ключами 'new', 'price_up', 'price_down':
    каждый содержит список новых price_history_id.
    Используется в _write_alert_events() для FK-ссылок.
    """
    today     = date.today()
    yesterday = date.fromordinal(today.toordinal() - 1)
    new_ids: dict[str, list[int]] = {"new": [], "price_up": [], "price_down": []}

    # ------------------------------------------------------------------
    # 1. НОВЫЕ напитки — просто INSERT
    # ------------------------------------------------------------------
    new_df = changes.get("new", pd.DataFrame())
    if not new_df.empty:
        new_ids["new"] = _insert_price_records(conn, new_df, run_id, today)
        logger.debug("Вставлено новых записей price_history: %d", len(new_ids["new"]))

    # ------------------------------------------------------------------
    # 2. ИЗМЕНЕНИЕ ЦЕНЫ — закрываем старую, открываем новую
    # ------------------------------------------------------------------
    for change_type in ("price_up", "price_down"):
        change_df = changes.get(change_type, pd.DataFrame())
        if change_df.empty:
            continue

        # Шаг 2а: закрываем старые записи пачкой через ANY
        old_ids = change_df["price_history_id"].dropna().astype(int).tolist()
        if old_ids:
            _close_price_records(conn, old_ids, valid_to=yesterday)
            logger.debug(
                "Закрыто записей price_history (%s): %d", change_type, len(old_ids)
            )

        # Шаг 2б: вставляем новые записи
        inserted = _insert_price_records(conn, change_df, run_id, today)
        new_ids[change_type] = inserted
        logger.debug(
            "Открыто новых записей price_history (%s): %d", change_type, len(inserted)
        )

    # ------------------------------------------------------------------
    # 3. УДАЛЁННЫЕ напитки — только закрываем, без новой записи
    # ------------------------------------------------------------------
    removed_df = changes.get("removed", pd.DataFrame())
    if not removed_df.empty:
        removed_ph_ids = removed_df["price_history_id"].dropna().astype(int).tolist()
        if removed_ph_ids:
            _close_price_records(conn, removed_ph_ids, valid_to=yesterday)
            logger.debug("Закрыто удалённых записей: %d", len(removed_ph_ids))

    return new_ids


def _insert_price_records(
    conn: Connection,
    df: pd.DataFrame,
    run_id: int,
    valid_from: date,
) -> list[int]:
    """
    Вставляет строки в price_history и возвращает список новых PK.
    Вычисляет price_per_100ml если не задан в df.
    """
    if df.empty:
        return []

    inserted_ids = []
    today = valid_from

    # Если price_per_100ml не посчитан (например, для 'new') — считаем здесь
    work_df = df.copy()
    if "price_per_100ml" not in work_df.columns or work_df["price_per_100ml"].isna().any():
        work_df["price_per_100ml"] = (
            work_df["price_rub_new"] / work_df["volume_ml"] * 100
        ).round(2)

    # Нормализуем имя колонки: для новых строк источник — price_rub_new
    price_col = "price_rub_new" if "price_rub_new" in work_df.columns else "price_rub"

    for _, row in work_df.iterrows():
        result = conn.execute(
            text("""
                INSERT INTO price_history
                    (drink_id, size_id, parse_run_id,
                     price_rub, price_per_100ml,
                     valid_from, valid_to, is_current)
                VALUES
                    (:drink_id, :size_id, :run_id,
                     :price_rub, :price_per_100ml,
                     :valid_from, :valid_to, TRUE)
                RETURNING id
            """),
            {
                "drink_id":        int(row["drink_id"]),
                "size_id":         int(row["size_id"]),
                "run_id":          run_id,
                "price_rub":       round(float(row[price_col]), 2),
                "price_per_100ml": round(float(row["price_per_100ml"]), 2),
                "valid_from":      today,
                "valid_to":        SCD_INFINITY,
            },
        )
        inserted_ids.append(result.fetchone()[0])

    return inserted_ids


def _close_price_records(
    conn: Connection,
    price_history_ids: list[int],
    valid_to: date,
) -> None:
    """
    Закрывает записи price_history пакетным UPDATE через ANY.
    Ставит valid_to и сбрасывает is_current = FALSE.
    """
    conn.execute(
        text("""
            UPDATE price_history
               SET valid_to   = :valid_to,
                   is_current = FALSE
             WHERE id = ANY(:ids)
               AND is_current = TRUE
        """),
        {"valid_to": valid_to, "ids": price_history_ids},
    )


# ===========================================================================
# DRINK_STATUS_LOG
# ===========================================================================

def _write_status_log(
    conn: Connection,
    run_id: int,
    changes: dict[str, pd.DataFrame],
) -> None:
    """
    Пишет события жизненного цикла в drink_status_log.

    Маппинг change_type → drink_status (PostgreSQL ENUM)
    -------------------------------------------------------
    'new'        → 'new'
    'price_up'   → 'price_up'
    'price_down' → 'price_down'
    'removed'    → 'removed'
    """
    # Маппинг совпадает по имени, но явно выносим для читаемости
    status_map = {
        "new":        "new",
        "price_up":   "price_up",
        "price_down": "price_down",
        "removed":    "removed",
    }

    rows_to_insert = []
    for change_type, status in status_map.items():
        df = changes.get(change_type, pd.DataFrame())
        if df.empty:
            continue
        for _, row in df.iterrows():
            rows_to_insert.append({
                "drink_id":     int(row["drink_id"]),
                "parse_run_id": run_id,
                "status":       status,
                "note":         _build_status_note(change_type, row),
            })

    if not rows_to_insert:
        return

    conn.execute(
        text("""
            INSERT INTO drink_status_log (drink_id, parse_run_id, status, detected_at, note)
            VALUES (:drink_id, :parse_run_id, :status, NOW(), :note)
        """),
        rows_to_insert,
    )
    logger.debug("Записано событий в drink_status_log: %d", len(rows_to_insert))


def _build_status_note(change_type: str, row: pd.Series) -> str:
    """Формирует читаемую заметку для drink_status_log.note."""
    if change_type in ("price_up", "price_down"):
        old  = row.get("old_price_rub", "?")
        new  = row.get("price_rub_new", "?")
        pct  = row.get("delta_pct", "?")
        sign = "+" if change_type == "price_up" else ""
        return f"{old} → {new} руб. ({sign}{pct}%)"
    if change_type == "new":
        return f"Первое появление. Цена: {row.get('price_rub_new', row.get('price_rub', '?'))} руб."
    if change_type == "removed":
        return "Позиция исчезла из меню."
    return ""


# ===========================================================================
# ALERT_EVENTS
# ===========================================================================

def _write_alert_events(
    conn: Connection,
    run_id: int,
    changes: dict[str, pd.DataFrame],
    new_ph_ids: dict[str, list[int]],
) -> int:
    """
    Формирует алерты в alert_events для трёх сценариев:

    1. Новый напиток ('new')
       alert_type = 'new_drink' или 'new_syrup' (если is_signature=TRUE в drinks).
       delta_rub / delta_pct = NULL.

    2. Изменение цены ('price_up' / 'price_down')
       alert_type = 'price_spike' / 'price_drop'.
       Алерт пишется только если |delta_pct| >= PRICE_CHANGE_ALERT_THRESHOLD_PCT.
       delta_rub / delta_pct заполняются.

    3. Удаление напитка ('removed')
       Не пишем в alert_events — только в drink_status_log.
       Удаление — внутреннее событие, не требующее немедленного оповещения.
       (Логика может быть пересмотрена по задаче.)

    payload JSONB
    -------------
    Хранит контекст для отправщика: competitor_name, drink_name,
    old_price, new_price, delta_pct. Резолвится запросом к drinks.

    Returns
    -------
    Количество записанных алертов.
    """
    alerts_inserted = 0

    # ------------------------------------------------------------------
    # 1. Алерты на НОВЫЕ напитки
    # ------------------------------------------------------------------
    new_df   = changes.get("new", pd.DataFrame())
    new_pids = new_ph_ids.get("new", [])

    if not new_df.empty and new_pids:
        # Подтягиваем метаданные drinks одним запросом
        drink_ids  = new_df["drink_id"].astype(int).tolist()
        drink_meta = _fetch_drink_meta(conn, drink_ids)

        for (_, row), ph_id in zip(new_df.iterrows(), new_pids):
            d_id  = int(row["drink_id"])
            meta  = drink_meta.get(d_id, {})
            a_type = "new_syrup" if meta.get("is_signature") else "new_drink"

            payload = {
                "competitor_name": meta.get("competitor_name", ""),
                "drink_name":      meta.get("name_raw", ""),
                "new_price_rub":   float(row.get("price_rub_new", row.get("price_rub", 0))),
                "size_label":      meta.get("size_label", ""),
                "is_signature":    meta.get("is_signature", False),
                "is_seasonal":     meta.get("is_seasonal", False),
            }

            conn.execute(
                text("""
                    INSERT INTO alert_events
                        (drink_id, price_history_id, alert_type,
                         delta_rub, delta_pct, payload, is_notified)
                    VALUES
                        (:drink_id, :ph_id, :alert_type,
                         NULL, NULL, :payload, FALSE)
                """),
                {
                    "drink_id":   d_id,
                    "ph_id":      ph_id,
                    "alert_type": a_type,
                    "payload":    json.dumps(payload, ensure_ascii=False),
                },
            )
            alerts_inserted += 1

    # ------------------------------------------------------------------
    # 2. Алерты на ИЗМЕНЕНИЕ ЦЕНЫ
    # ------------------------------------------------------------------
    for change_type in ("price_up", "price_down"):
        change_df = changes.get(change_type, pd.DataFrame())
        pids      = new_ph_ids.get(change_type, [])

        if change_df.empty or not pids:
            continue

        drink_ids  = change_df["drink_id"].astype(int).tolist()
        drink_meta = _fetch_drink_meta(conn, drink_ids)

        for (_, row), ph_id in zip(change_df.iterrows(), pids):
            delta_pct = float(row.get("delta_pct", 0))

            # Фильтр по порогу: мелкое колебание — не алерт
            if abs(delta_pct) < PRICE_CHANGE_ALERT_THRESHOLD_PCT:
                logger.debug(
                    "Изменение цены ниже порога (%.1f%%) для drink_id=%d, пропускаем алерт.",
                    delta_pct, int(row["drink_id"]),
                )
                continue

            d_id   = int(row["drink_id"])
            meta   = drink_meta.get(d_id, {})
            a_type = "price_spike" if change_type == "price_up" else "price_drop"

            payload = {
                "competitor_name": meta.get("competitor_name", ""),
                "drink_name":      meta.get("name_raw", ""),
                "old_price_rub":   float(row.get("old_price_rub", 0)),
                "new_price_rub":   float(row.get("price_rub_new", 0)),
                "delta_rub":       float(row.get("delta_rub", 0)),
                "delta_pct":       delta_pct,
                "size_label":      meta.get("size_label", ""),
            }

            conn.execute(
                text("""
                    INSERT INTO alert_events
                        (drink_id, price_history_id, alert_type,
                         delta_rub, delta_pct, payload, is_notified)
                    VALUES
                        (:drink_id, :ph_id, :alert_type,
                         :delta_rub, :delta_pct, :payload, FALSE)
                """),
                {
                    "drink_id":   d_id,
                    "ph_id":      ph_id,
                    "alert_type": a_type,
                    "delta_rub":  round(float(row.get("delta_rub", 0)), 2),
                    "delta_pct":  round(delta_pct, 2),
                    "payload":    json.dumps(payload, ensure_ascii=False),
                },
            )
            alerts_inserted += 1

    logger.debug("Записано алертов в alert_events: %d", alerts_inserted)
    return alerts_inserted


def _fetch_drink_meta(conn: Connection, drink_ids: list[int]) -> dict[int, dict]:
    """
    Загружает метаданные напитков для формирования payload алертов.
    Возвращает dict[drink_id -> dict с полями].
    """
    if not drink_ids:
        return {}

    result = conn.execute(
        text("""
            SELECT
                d.id               AS drink_id,
                d.name_raw,
                d.is_signature,
                d.is_seasonal,
                c.name             AS competitor_name,
                s.label            AS size_label
            FROM drinks d
            JOIN competitors c ON c.id = d.competitor_id
            LEFT JOIN price_history ph
                   ON ph.drink_id   = d.id
                  AND ph.is_current = TRUE
            LEFT JOIN sizes s ON s.id = ph.size_id
            WHERE d.id = ANY(:ids)
        """),
        {"ids": drink_ids},
    )
    return {
        row["drink_id"]: dict(row)
        for row in result.mappings().fetchall()
    }


# ===========================================================================
# УТИЛИТЫ
# ===========================================================================

def _validate_scraped_df(df: pd.DataFrame) -> None:
    """
    Проверяет входной DataFrame на соответствие контракту.
    Бросает ValueError с описанием проблемы — до открытия транзакции.
    """
    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"scraped_df не содержит обязательных колонок: {missing}")

    if df.empty:
        raise ValueError("scraped_df пуст. Нечего загружать.")

    if df["price_rub"].le(0).any():
        bad = df[df["price_rub"] <= 0][["drink_id", "price_rub"]]
        raise ValueError(f"Найдены нулевые или отрицательные цены:\n{bad}")

    if df["volume_ml"].le(0).any():
        raise ValueError("Найдены нулевые или отрицательные значения volume_ml.")

    # Предупреждение о дублях ключей (не блокирует, но логируем)
    dupes = df.duplicated(subset=["drink_id", "size_id"])
    if dupes.any():
        logger.warning(
            "В scraped_df найдены дублирующиеся (drink_id, size_id): %d строк. "
            "Будет использована последняя встреченная строка.",
            dupes.sum(),
        )
        # Оставляем последнюю копию — логика парсера должна это исключать
        df.drop_duplicates(subset=["drink_id", "size_id"], keep="last", inplace=True)


def _ms(since: datetime) -> int:
    """Миллисекунды с момента since до сейчас."""
    return int((datetime.now(timezone.utc) - since).total_seconds() * 1000)


