# coffee-market-analytics
End-to-end coffee market analytics: Python ETL, PostgreSQL (SCD Type 2), Power BI dashboard, and Telegram alerts.

# ☕ Coffee Market Analytics & Alerting Pipeline

**Бизнес-задача:** Автоматизированный мониторинг конкурентного
ценообразования на рынке кофеен Санкт-Петербурга.
Система собирает цены трёх сетей (STARS Coffee, Surf Coffee, Drinkit),
рассчитывает отклонения от эталона и проактивно уведомляет бизнес
о новинках и изменениях цен через Telegram.

## 🛠 Стек технологий

| Слой | Технологии |
|---|---|
| База данных | PostgreSQL, DDL, Views, SCD Type 2 |
| Сбор и трансформация | Python 3, Pandas, NumPy, SQLAlchemy, psycopg2 |
| Визуализация | Power BI, DAX, JSON-темы |
| Data Activation | Python 3, requests, Telegram Bot API, python-dotenv |

---

## 🏗 Архитектура
Парсеры (PDF/Web)
↓
shared_mapper.py — DrinkMapper (кэш-слой text→ID)
↓
price_pipeline.py — SCD Type 2 upsert, алерты
↓
PostgreSQL — price_history, alert_events, views
↓                    
Power BI Dashboard    tg_alerter.py → Telegram

### Слой 1 — Data Layer (PostgreSQL)

Реляционная схема с тремя логическими группами таблиц:
справочники (competitors, drinks, sizes), ценовая история (SCD Type 2),
алертинг (drink_status_log, alert_events).

**SCD Type 2** в таблице `price_history`:
поля `valid_from / valid_to / is_current` позволяют
восстановить прайс на любую прошлую дату без потери истории.

**Витрины данных:**
- `v_current_prices` — актуальный срез, фильтрация выброса
  эспрессо (46 мл, 521 ₽/100мл)
- `v_benchmark_delta` — LATERAL JOIN для сравнения конкурентов
  с ближайшим размером STARS

### Слой 2 — Collection Layer (Python)

- **`shared_mapper.py`** — `DrinkMapper`: один SELECT при старте,
  dict-кэш для разрешения `name_raw → drink_id`.
  Поддерживает авторегистрацию новых напитков и fallback
  на ближайший размер
- **`stars_parser.py`** — мультиразмерное разворачивание:
  32 записи словаря → 90 строк DataFrame (напиток × размер)
- **`surf_parser.py`**, **`drinkit_parser.py`** — унифицированный
  выход: DataFrame с колонками `drink_id, size_id, price_rub, volume_ml`
- **`price_pipeline.py`** — атомарный SCD-апсерт в одной транзакции:
  early-return на первом прогоне, `detect_changes()` через Pandas merge,
  автозапись в `drink_status_log` и `alert_events`

### Слой 3 — BI Layer (Power BI)

DAX-меры: `MEDIANX` для защиты от выброса эспрессо,
`USERELATIONSHIP` для SCD-таймлайна по `valid_from`.
Условное форматирование KPI по знаку дельты,
нулевая ось на бенчмарк-чарте, корпоративная тема.

### Слой 4 — Data Activation Layer (Telegram)

`tg_alerter.py`: раздельные массивы `sent_ids / failed_ids`,
`is_notified=TRUE` только после `200 OK` от Telegram API,
`pool_pre_ping=True` для надёжной работы по cron.

---

## 📂 Структура репозитория
├── sql/
│   ├── 01_schema.sql          # DDL: таблицы, индексы, enum-типы
│   └── 02_views.sql           # Витрины v_current_prices, v_benchmark_delta
├── python/
│   ├── shared_mapper.py       # DrinkMapper, нормализация названий
│   ├── stars_parser.py        # Парсер STARS Coffee (бенчмарк)
│   ├── surf_parser.py         # Парсер Surf Coffee
│   ├── drinkit_parser.py      # Парсер Drinkit
│   ├── price_pipeline.py      # SCD Type 2 upsert, алерты
│   ├── main.py                # Точка входа: парсинг → пайплайн
│   ├── tg_alerter.py          # Telegram-рассылка алертов
│   └── .env.example           # Шаблон переменных окружения
├── bi/
│   ├── coffee_analytics_theme.json
│   └── screenshots/
└── README.md

---

## 🚀 Быстрый старт

```bash
git clone https://github.com/your-username/coffee-market-analytics
cd coffee-market-analytics

pip install sqlalchemy psycopg2-binary pandas python-dotenv requests

# Накатить схему
psql -U user -d coffee_analytics -f sql/01_schema.sql
psql -U user -d coffee_analytics -f sql/02_views.sql

# Настроить окружение
cp python/.env.example python/.env
# Заполнить .env: DB_*, TG_BOT_TOKEN, TG_CHAT_ID

# Первый прогон — загрузка данных
python python/main.py

# Разовая отправка алертов
python python/tg_alerter.py

# Добавить в cron для регулярной работы
# */30 * * * * cd /path/to/project && python python/tg_alerter.py
```
