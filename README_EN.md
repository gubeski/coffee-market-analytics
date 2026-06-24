# ☕ Coffee Market Analytics & Alerting Pipeline

An end-to-end analytics project for monitoring competitive coffee pricing in Saint Petersburg. The pipeline collects menu prices from **STARS Coffee**, **Surf Coffee**, and **Drinkit**, stores a complete history of price changes, compares competitors with a benchmark, powers a Power BI dashboard, and sends proactive Telegram alerts.

## Business Goal

Coffee chains need a reliable way to track competitors without manually reviewing menus every day. This project automates that process and helps answer questions such as:

- How do competitors' prices compare with the STARS Coffee benchmark?
- Which drinks have increased or decreased in price?
- Which products are new, seasonal, or no longer available?
- How has a drink's price changed over time?
- Which market changes require immediate attention?

## Technology Stack

| Layer | Technologies |
|---|---|
| Database | PostgreSQL, SQL, views, SCD Type 2 |
| Collection and transformation | Python 3, Pandas, NumPy, SQLAlchemy, psycopg2 |
| Business intelligence | Power BI, DAX, JSON themes |
| Data activation | Python 3, Requests, Telegram Bot API, python-dotenv |

## Architecture

```text
PDF / web menu parsers
          │
          ▼
shared_mapper.py — cached text-to-ID mapping
          │
          ▼
price_pipeline.py — change detection and SCD Type 2 upsert
          │
          ▼
PostgreSQL — price history, alert events, analytical views
          │
          ├──────────────► Power BI dashboard
          │
          └──────────────► tg_alerter.py ──► Telegram
```

## How It Works

### 1. PostgreSQL Data Layer

The relational model separates the data into three logical areas:

- reference data for competitors, drinks, and sizes;
- historical pricing stored in `price_history`;
- lifecycle and notification events stored in `drink_status_log` and `alert_events`.

Price history follows the **Slowly Changing Dimension Type 2** pattern. Each version contains `valid_from`, `valid_to`, and `is_current`, making it possible to reconstruct prices for any historical date without losing previous values.

The database exposes two analytical views:

- `v_current_prices` provides the latest available price for each drink and size;
- `v_benchmark_delta` uses a `LATERAL JOIN` to compare each competitor with the closest STARS Coffee serving size.

### 2. Python Collection and Transformation Layer

- `shared_mapper.py` contains `DrinkMapper`, which loads lookup data once and resolves raw drink names and serving volumes through in-memory dictionaries. It can automatically register new drinks and sizes or fall back to the nearest known size.
- `stars_parser.py` expands multi-size menu definitions into one DataFrame row per drink and serving size.
- `surf_parser.py` and `drinkit_parser.py` produce the same normalized DataFrame contract.
- `price_pipeline.py` compares the latest scrape with current database records, classifies changes, performs an atomic SCD Type 2 upsert, and creates status and alert events in a single transaction.
- `main.py` runs all three parsers and sends their results through the pricing pipeline.

Each parser returns a DataFrame with the following columns:

| Column | Description |
|---|---|
| `drink_id` | Drink identifier from PostgreSQL |
| `size_id` | Serving-size identifier from PostgreSQL |
| `price_rub` | Current price in Russian rubles |
| `volume_ml` | Serving volume in millilitres |

### 3. Power BI Layer

The Power BI model is designed for both current-price analysis and historical exploration:

- `MEDIANX` protects summary metrics from extreme price-per-100-ml values;
- `USERELATIONSHIP` activates the SCD timeline through `valid_from`;
- KPI colors reflect the direction of the benchmark delta;
- benchmark charts use a zero reference axis;
- a reusable JSON theme provides consistent dashboard styling.

The Power BI project is stored in `bi/project.pbix.zip`, with its theme in `bi/coffee_analytics_theme.json`.

### 4. Telegram Alerting Layer

`tg_alerter.py` reads unsent events from PostgreSQL and delivers formatted messages through the Telegram Bot API. It tracks successful and failed deliveries separately and sets `is_notified = TRUE` only after Telegram confirms delivery. Connection health checks are enabled with `pool_pre_ping=True`, which makes scheduled execution more resilient.

Alert categories include new drinks, seasonal returns, price increases, price decreases, and benchmark gaps.

## Repository Structure

```text
.
├── sql/
│   ├── 01_schema.sql                 # Tables, indexes, and enum types
│   └── 02_views.sql                  # Current-price and benchmark views
├── python/
│   ├── shared_mapper.py              # Name normalization and cached ID mapping
│   ├── stars_parser.py               # STARS Coffee benchmark parser
│   ├── surf_parser.py                # Surf Coffee parser
│   ├── drinkit_parser.py             # Drinkit parser
│   ├── price_pipeline.py             # Change detection and SCD Type 2 upsert
│   ├── main.py                       # Main pipeline entry point
│   ├── tg_alerter.py                 # Telegram alert delivery
│   └── env.example                   # Environment variable template
├── bi/
│   ├── coffee_analytics_theme.json   # Power BI theme
│   └── project.pbix.zip              # Packaged Power BI project
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.10 or later
- PostgreSQL
- Power BI Desktop, if you want to open the dashboard
- A Telegram bot and chat ID, if you want to receive alerts

### 1. Clone the repository

```bash
git clone https://github.com/your-username/coffee-market-analytics.git
cd coffee-market-analytics
```

### 2. Install Python dependencies

```bash
python -m pip install pandas numpy sqlalchemy psycopg2-binary python-dotenv requests
```

### 3. Create the database objects

Create a PostgreSQL database, then apply the schema and analytical views:

```bash
psql -U your_user -d coffee_analytics -f sql/01_schema.sql
psql -U your_user -d coffee_analytics -f sql/02_views.sql
```

### 4. Configure the application

Copy the environment template:

```bash
cp python/env.example python/.env
```

Set the database and Telegram values in `python/.env`:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=coffee_analytics
DB_USER=your_user
DB_PASSWORD=your_password
TG_BOT_TOKEN=your_bot_token
TG_CHAT_ID=your_chat_id
```

Before running the collection pipeline, update `DB_URL` in `python/main.py` to match your PostgreSQL connection. Run the Telegram alerter from the `python` directory so `python-dotenv` can load `.env` automatically.

### 5. Run the pipeline

From the repository root:

```bash
python python/main.py
```

The first run loads the initial price snapshot. Later runs compare new data with the current snapshot, preserve changed prices as historical records, and enqueue relevant alert events.

### 6. Send pending Telegram alerts

```bash
cd python
python tg_alerter.py
```

For recurring delivery, schedule the alerter with cron. The following example checks the queue every 30 minutes:

```cron
*/30 * * * * cd /path/to/coffee-market-analytics/python && /usr/bin/python3 tg_alerter.py
```

## SCD Type 2 Example

When a price changes, the pipeline closes the current record and inserts a new current version:

| Price | Valid from | Valid to | Current |
|---:|---|---|:---:|
| 290 ₽ | 2026-01-01 | 2026-02-14 | No |
| 310 ₽ | 2026-02-15 | 9999-12-31 | Yes |

This approach keeps the latest price easy to query while retaining a complete and auditable history.

## Key Engineering Features

- Atomic ingestion: each competitor run is committed or rolled back as one transaction.
- Efficient mapping: lookup tables are cached instead of queried once per menu item.
- Idempotent comparison: unchanged prices do not create unnecessary history records.
- Comparable pricing: prices are normalized per 100 ml and matched to the nearest benchmark size.
- Reliable notifications: only confirmed Telegram deliveries are marked as sent.
- BI-ready model: current-state views and historical SCD dates support both operational and trend reporting.

## Possible Extensions

- Replace static menu inputs with scheduled web or PDF ingestion.
- Move all connection settings to environment variables.
- Add automated tests for parsers, change detection, and SQL views.
- Package the services with Docker Compose.
- Add orchestration and monitoring with Airflow, Prefect, or another scheduler.
- Publish anonymized dashboard screenshots in `bi/screenshots/`.

## License

This repository does not currently specify a license. Add a `LICENSE` file before distributing or reusing the project publicly.
