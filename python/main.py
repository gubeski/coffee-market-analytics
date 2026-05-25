"""
main.py
=======
Главный запускающий скрипт системы мониторинга цен кофеен.

Порядок работы
--------------
1. Подключение к PostgreSQL (coffee_analytics) через SQLAlchemy.
2. Запуск парсеров: STARS → Surf Coffee → Drinkit.
3. Передача каждого DataFrame в run_price_pipeline().
4. Итоговый отчёт по всем прогонам + контрольный SELECT из v_benchmark_delta.
"""

import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

# ── парсеры ────────────────────────────────────────────────────────────────
from stars_parser   import parse_stars_menu,   STARS_COMPETITOR_ID
from surf_parser    import parse_surf_menu,    SURF_COMPETITOR_ID
from drinkit_parser import parse_drinkit_menu, DRINKIT_COMPETITOR_ID

# ── пайплайн ───────────────────────────────────────────────────────────────
from price_pipeline import run_price_pipeline

# ===========================================================================
# КОНФИГ
# ===========================================================================

DB_URL = "postgresql+psycopg2://coffee_user:coffee_pass@127.0.0.1:5432/coffee_analytics"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

# ===========================================================================
# ТОЧКА ВХОДА
# ===========================================================================

def main() -> None:
    started_at = datetime.now(timezone.utc)
    logger.info("═" * 60)
    logger.info("  Coffee Analytics — загрузка данных")
    logger.info("═" * 60)

    # ── подключение ──────────────────────────────────────────────────────
    logger.info("Подключение к БД: %s", DB_URL.split("@")[1])
    engine = create_engine(DB_URL, echo=False)

    with engine.connect() as conn:
        v = conn.execute(text("SELECT version()")).scalar()
        logger.info("PostgreSQL: %s", v.split(",")[0])

    # ── прогоны парсеров ─────────────────────────────────────────────────
    pipeline_runs = [
        ("STARS Coffee  [benchmark]", STARS_COMPETITOR_ID,   parse_stars_menu),
        ("Surf Coffee               ", SURF_COMPETITOR_ID,    parse_surf_menu),
        ("Drinkit                   ", DRINKIT_COMPETITOR_ID, parse_drinkit_menu),
    ]

    results = []
    for label, cid, parse_fn in pipeline_runs:
        logger.info("─" * 60)
        logger.info("▶  %s  (competitor_id=%d)", label.strip(), cid)

        df = parse_fn(engine)
        logger.info("   Парсер → DataFrame: %d строк  (напиток × размер)", len(df))

        result = run_price_pipeline(engine, competitor_id=cid, scraped_df=df)
        results.append((label.strip(), result))

        status_icon = "✓" if result.status == "success" else "✗"
        logger.info(
            "   %s  scraped=%-3d  new=%-3d  changes=%-3d  alerts=%-3d  %dмс",
            status_icon,
            result.items_scraped,
            result.new_drinks,
            result.price_changes,
            result.alerts_written,
            result.timing.get("total_ms", 0),
        )
        if result.error:
            logger.error("   Ошибка: %s", result.error)

    # ── итоговый отчёт ───────────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("═" * 60)
    logger.info("  Итог загрузки  (%.1f сек)", elapsed)
    logger.info("═" * 60)

    total_drinks  = sum(r.new_drinks      for _, r in results)
    total_alerts  = sum(r.alerts_written  for _, r in results)
    total_scraped = sum(r.items_scraped   for _, r in results)
    failed        = [lbl for lbl, r in results if r.status != "success"]

    logger.info("  Загружено позиций (строк):  %d", total_scraped)
    logger.info("  Зарегистрировано напитков:  %d", total_drinks)
    logger.info("  Алертов в очереди:          %d", total_alerts)
    if failed:
        logger.error("  ОШИБКИ в прогонах: %s", ", ".join(failed))
    else:
        logger.info("  Все прогоны завершены успешно ✓")

    # ── контрольный SELECT: v_benchmark_delta ───────────────────────────
    logger.info("─" * 60)
    logger.info("  Топ-5 отклонений от бенчмарка (v_benchmark_delta)")
    logger.info("─" * 60)

    query = text("""
        SELECT
            competitor_name,
            name_normalized,
            size_label,
            benchmark_price_per_100ml   AS stars_pp100,
            competitor_price_per_100ml  AS comp_pp100,
            delta_rub,
            delta_pct
        FROM v_benchmark_delta
        WHERE name_normalized IS NOT NULL
        ORDER BY ABS(delta_pct) DESC NULLS LAST
        LIMIT 10
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    if rows:
        logger.info(
            "  %-12s  %-28s  %-14s  %8s  %8s  %8s  %7s",
            "Конкурент", "Напиток", "Размер",
            "STARS", "Конкур.", "Δ руб", "Δ %"
        )
        logger.info("  " + "─" * 90)
        for r in rows:
            logger.info(
                "  %-12s  %-28s  %-14s  %8.1f  %8.1f  %+8.1f  %+6.1f%%",
                r.competitor_name[:12],
                (r.name_normalized or "—")[:28],
                (r.size_label or "—")[:14],
                r.stars_pp100,
                r.comp_pp100,
                r.delta_rub,
                r.delta_pct,
            )
    else:
        logger.info("  (нет совпадений по name_normalized — требуется ручной маппинг)")

    # ── проверяем очередь алертов ────────────────────────────────────────
    logger.info("─" * 60)
    logger.info("  Очередь алертов (alert_events, is_notified=FALSE)")
    logger.info("─" * 60)

    alert_q = text("""
        SELECT ae.alert_type, COUNT(*) AS cnt
        FROM alert_events ae
        WHERE ae.is_notified = FALSE
        GROUP BY ae.alert_type
        ORDER BY cnt DESC
    """)
    with engine.connect() as conn:
        alert_rows = conn.execute(alert_q).fetchall()

    if alert_rows:
        for ar in alert_rows:
            logger.info("  %-20s  %d алертов", ar.alert_type, ar.cnt)
    else:
        logger.info("  Очередь пуста")

    logger.info("═" * 60)
    logger.info("  Готово. Витрина v_benchmark_delta заполнена.")
    logger.info("  Подключай Power BI: %s", DB_URL.split("@")[1])
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
