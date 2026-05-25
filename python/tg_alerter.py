"""
tg_alerter.py
=============
Data Activation слой: читает очередь алертов из PostgreSQL
и рассылает красивые сообщения в Telegram.

Запуск вручную:
    python tg_alerter.py

Запуск по крону (каждые 30 минут):
    */30 * * * * cd /path/to/project && python tg_alerter.py >> logs/alerter.log 2>&1

Переменные окружения (.env):
    TG_BOT_TOKEN   — токен бота от @BotFather
    TG_CHAT_ID     — числовой ID чата или @username канала
    DB_HOST        — хост PostgreSQL (default: localhost)
    DB_PORT        — порт (default: 5432)
    DB_NAME        — имя базы (default: coffee_analytics)
    DB_USER        — пользователь
    DB_PASSWORD    — пароль

Зависимости:
    pip install sqlalchemy psycopg2-binary python-dotenv requests
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine

# ──────────────────────────────────────────────────────────────────────────────
# ИНИЦИАЛИЗАЦИЯ
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()  # читает .env из текущей директории

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("tg_alerter")


# ──────────────────────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ──────────────────────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    """Читает переменную окружения. Падает с понятной ошибкой если не задана."""
    value = os.getenv(name)
    if not value:
        logger.error("Переменная окружения %s не задана. Добавь её в .env", name)
        sys.exit(1)
    return value


TG_BOT_TOKEN: str = _require_env("TG_BOT_TOKEN")
TG_CHAT_ID:   str = _require_env("TG_CHAT_ID")

DB_URL: str = (
    "postgresql+psycopg2://"
    f"{_require_env('DB_USER')}:{_require_env('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'coffee_analytics')}"
)

TG_API_URL    = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"
TG_TIMEOUT    = 10   # секунд на один запрос к Telegram API
TG_BATCH_SIZE = 20   # максимум алертов за один прогон (защита от флуда)


# ──────────────────────────────────────────────────────────────────────────────
# ФОРМАТИРОВАНИЕ СООБЩЕНИЙ
# ──────────────────────────────────────────────────────────────────────────────

# Эмодзи и заголовок для каждого типа алерта
_ALERT_META: dict[str, tuple[str, str]] = {
    "new_drink":       ("🆕", "Новый напиток"),
    "new_syrup":       ("✨", "Новый авторский"),
    "seasonal_return": ("🌸", "Сезонный вернулся"),
    "price_spike":     ("📈", "Рост цены"),
    "price_drop":      ("📉", "Снижение цены"),
    "benchmark_gap":   ("⚠️",  "Отклонение от STARS"),
}


def format_alert(row: dict) -> str:
    """
    Формирует текст Telegram-сообщения для одного алерта.

    Использует поля из JOIN-запроса:
        alert_type, competitor_name, drink_name, price_rub,
        delta_rub, delta_pct, payload (JSONB → dict)
    """
    alert_type     = row["alert_type"]
    competitor     = row["competitor_name"]
    drink          = row["drink_name"]
    delta_rub      = row.get("delta_rub")
    delta_pct      = row.get("delta_pct")
    created_at     = row["created_at"]
    payload: dict  = row.get("payload") or {}

    # Если payload — строка (из psycopg2 без десериализации) — парсим
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            payload = {}

    emoji, title = _ALERT_META.get(alert_type, ("🔔", alert_type))

    # Базовая часть — одинакова для всех типов
    lines = [
        f"{emoji} *{title}*",
        f"🏪 Сеть: *{_escape(competitor)}*",
        f"☕ Напиток: *{_escape(drink)}*",
    ]

    # Цена из payload или из price_rub
    price = payload.get("new_price_rub") or payload.get("price_rub") or row.get("price_rub")
    if price:
        lines.append(f"💰 Цена: *{price:.0f} ₽*")

    # Дельта — только для ценовых событий
    if alert_type in ("price_spike", "price_drop", "benchmark_gap"):
        if delta_rub is not None and delta_pct is not None:
            sign       = "+" if float(delta_rub) > 0 else ""
            delta_line = f"📊 Изменение: *{sign}{float(delta_rub):.1f} ₽* ({sign}{float(delta_pct):.1f}%)"
            lines.append(delta_line)

        # Для price_spike/drop добавляем старую → новую цену если есть в payload
        if alert_type in ("price_spike", "price_drop"):
            old = payload.get("old_price_rub")
            new = payload.get("new_price_rub")
            if old and new:
                lines.append(f"   {old:.0f} ₽  →  {new:.0f} ₽")

    # Для benchmark_gap — показываем цену STARS рядом
    if alert_type == "benchmark_gap":
        stars_price = payload.get("stars_price") or payload.get("benchmark_price_per_100ml")
        comp_price  = payload.get("competitor_price_per_100ml")
        if stars_price and comp_price:
            lines.append(
                f"   STARS: {float(stars_price):.1f} ₽/100мл  |  "
                f"{_escape(competitor)}: {float(comp_price):.1f} ₽/100мл"
            )

    # Временна́я метка
    if created_at:
        ts = created_at.strftime("%d.%m.%Y %H:%M") if hasattr(created_at, "strftime") \
             else str(created_at)[:16].replace("T", " ")
        lines.append(f"\n🕐 _{ts}_")

    return "\n".join(lines)


def _escape(text: str) -> str:
    """Экранирует спецсимволы Markdown v1 для Telegram."""
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM API
# ──────────────────────────────────────────────────────────────────────────────

def send_telegram(text: str) -> bool:
    """
    Отправляет сообщение в Telegram через Bot API.

    Returns:
        True  — сообщение доставлено
        False — ошибка (уже залогирована)
    """
    url  = f"{TG_API_URL}/sendMessage"
    data = {
        "chat_id":    TG_CHAT_ID,
        "text":       text,
        "parse_mode": "Markdown",
        # Отключаем предпросмотр ссылок — алерты читаются быстрее без превью
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(url, json=data, timeout=TG_TIMEOUT)
        resp.raise_for_status()
        result = resp.json()

        if not result.get("ok"):
            logger.error(
                "Telegram API вернул ошибку: %s", result.get("description", "unknown")
            )
            return False

        logger.debug("Telegram: сообщение доставлено (message_id=%s)",
                     result.get("result", {}).get("message_id"))
        return True

    except requests.Timeout:
        logger.error("Telegram API: таймаут (%d сек)", TG_TIMEOUT)
        return False
    except requests.ConnectionError as exc:
        logger.error("Telegram API: нет соединения — %s", exc)
        return False
    except requests.HTTPError as exc:
        logger.error("Telegram API: HTTP ошибка — %s", exc)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# РАБОТА С БАЗОЙ ДАННЫХ
# ──────────────────────────────────────────────────────────────────────────────

# SQL: выбираем неотправленные алерты с JOIN для получения человекочитаемых полей
FETCH_SQL = text("""
    SELECT
        ae.id               AS alert_id,
        ae.alert_type       :: TEXT AS alert_type,
        ae.delta_rub,
        ae.delta_pct,
        ae.payload,
        ae.created_at,
        d.name_raw          AS drink_name,
        d.is_signature,
        c.name              AS competitor_name,
        ph.price_rub
    FROM alert_events ae
    JOIN drinks         d   ON d.id  = ae.drink_id
    JOIN competitors    c   ON c.id  = d.competitor_id
    LEFT JOIN price_history ph
           ON ph.id = ae.price_history_id
    WHERE ae.is_notified = FALSE
    ORDER BY ae.created_at ASC
    LIMIT :batch_size
""")

# SQL: помечаем отправленные алерты (пакетный UPDATE по списку ID)
MARK_SQL = text("""
    UPDATE alert_events
       SET is_notified = TRUE
     WHERE id = ANY(:ids)
""")


def fetch_pending(engine: Engine) -> list[dict]:
    """Читает очередь неотправленных алертов. Возвращает список словарей."""
    with engine.connect() as conn:
        rows = conn.execute(FETCH_SQL, {"batch_size": TG_BATCH_SIZE}).mappings().fetchall()
    return [dict(r) for r in rows]


def mark_notified(engine: Engine, alert_ids: list[int]) -> None:
    """Помечает алерты как отправленные — отдельной транзакцией."""
    if not alert_ids:
        return
    with engine.begin() as conn:
        conn.execute(MARK_SQL, {"ids": alert_ids})
    logger.info("Помечено как отправленные: %d алертов", len(alert_ids))


# ──────────────────────────────────────────────────────────────────────────────
# ОСНОВНАЯ ЛОГИКА
# ──────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Главный цикл:
    1. Подключиться к БД
    2. Прочитать очередь алертов
    3. Для каждого: сформировать сообщение → отправить в Telegram
    4. Пометить успешно отправленные как is_notified=TRUE
    """
    logger.info("── tg_alerter запущен ──────────────────────────────────────")
    start = datetime.now(timezone.utc)

    # ── подключение к БД ──────────────────────────────────────────────────
    try:
        engine = create_engine(DB_URL, pool_pre_ping=True, echo=False)
        # pool_pre_ping=True — проверяет соединение перед запросом,
        # защищает от ошибок после долгого простоя
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("БД: подключение установлено")
    except Exception as exc:
        logger.critical("БД: не удалось подключиться — %s", exc)
        sys.exit(1)

    # ── чтение очереди ────────────────────────────────────────────────────
    try:
        pending = fetch_pending(engine)
    except Exception as exc:
        logger.error("БД: ошибка при чтении alert_events — %s", exc)
        sys.exit(1)

    if not pending:
        logger.info("Новых алертов нет. Завершаем работу.")
        return

    logger.info("Найдено алертов для отправки: %d", len(pending))

    # ── отправка в Telegram ───────────────────────────────────────────────
    sent_ids:   list[int] = []   # ID успешно отправленных
    failed_ids: list[int] = []   # ID проваленных (не помечаем — попробуем в следующий раз)

    for alert in pending:
        alert_id = alert["alert_id"]

        try:
            message = format_alert(alert)
        except Exception as exc:
            # Ошибка форматирования не должна ронять весь скрипт
            logger.error("Ошибка форматирования алерта id=%d: %s", alert_id, exc)
            # Отправляем fallback-сообщение
            message = (
                f"🔔 *Новый алерт* (id={alert_id})\n"
                f"Тип: {alert.get('alert_type', '?')}\n"
                f"Конкурент: {alert.get('competitor_name', '?')}\n"
                f"Напиток: {alert.get('drink_name', '?')}"
            )

        success = send_telegram(message)

        if success:
            sent_ids.append(alert_id)
            logger.info(
                "  ✓ отправлен алерт id=%-4d  type=%-18s  [%s]",
                alert_id, alert.get("alert_type", "?"), alert.get("competitor_name", "?")
            )
        else:
            failed_ids.append(alert_id)
            logger.warning(
                "  ✗ не удалось отправить алерт id=%d — пропускаем, попробуем позже",
                alert_id
            )

    # ── обновление статусов ───────────────────────────────────────────────
    try:
        mark_notified(engine, sent_ids)
    except Exception as exc:
        # Критичная ситуация: сообщения отправлены, но статус не обновился.
        # При следующем запуске алерты отправятся повторно.
        # Логируем с максимальным приоритетом.
        logger.critical(
            "БД: не удалось обновить is_notified=TRUE для id=%s — %s. "
            "При следующем запуске возможна повторная отправка!",
            sent_ids, exc
        )

    # ── итог ─────────────────────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(
        "Готово за %.1f сек. Отправлено: %d / Ошибок: %d",
        elapsed, len(sent_ids), len(failed_ids)
    )
    logger.info("────────────────────────────────────────────────────────────")


# ──────────────────────────────────────────────────────────────────────────────
# ТОЧКА ВХОДА
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run()
