-- 1. Создание enum-типа для типов уведомлений
CREATE TYPE alert_type_enum AS ENUM (
    'new_drink', 
    'new_syrup', 
    'seasonal_return', 
    'price_spike', 
    'price_drop', 
    'drink_disappeared'
);

-- 2. Справочник конкурентов
CREATE TABLE competitors (
    competitor_id SERIAL PRIMARY KEY,
    competitor_name VARCHAR(100) NOT NULL UNIQUE,
    is_benchmark BOOLEAN DEFAULT FALSE
);

-- 3. Справочник объемов/размеров
CREATE TABLE sizes (
    size_id SERIAL PRIMARY KEY,
    size_name VARCHAR(50), -- Short, Tall, Grande и т.д.
    volume_ml INT NOT NULL UNIQUE
);

-- 4. Справочник напитков
CREATE TABLE drinks (
    drink_id SERIAL PRIMARY KEY,
    competitor_id INT REFERENCES competitors(competitor_id) ON DELETE CASCADE,
    drink_name VARCHAR(255) NOT NULL,
    category VARCHAR(100), -- Кофе, Чай, Сезонные
    UNIQUE(competitor_id, drink_name)
);

-- 5. Ценовая история (Реализация SCD Type 2)
CREATE TABLE price_history (
    price_id SERIAL PRIMARY KEY,
    drink_id INT REFERENCES drinks(drink_id) ON DELETE CASCADE,
    size_id INT REFERENCES sizes(size_id) ON DELETE CASCADE,
    price NUMERIC(10, 2) NOT NULL,
    price_per_100ml NUMERIC(10, 2) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- 6. Лог статусов напитков (для отслеживания ротации меню)
CREATE TABLE drink_status_log (
    log_id SERIAL PRIMARY KEY,
    drink_id INT REFERENCES drinks(drink_id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL, -- 'active', 'inactive'
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. Таблица событий/алертов для Telegram-бота
CREATE TABLE alert_events (
    alert_id SERIAL PRIMARY KEY,
    alert_type alert_type_enum NOT NULL,
    drink_id INT REFERENCES drinks(drink_id) ON DELETE CASCADE,
    payload JSONB, -- Хранит старые/новые цены и метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_notified BOOLEAN DEFAULT FALSE
);

-- 8. Индексы для оптимизации производительности пайплайна и BI
CREATE INDEX idx_price_history_current ON price_history(drink_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_price_history_dates ON price_history(valid_from, valid_to);
CREATE INDEX idx_alert_events_notified ON alert_events(is_notified) WHERE is_notified = FALSE;