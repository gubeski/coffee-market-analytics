-- 1. Витрина текущих актуальных цен
CREATE OR REPLACE VIEW v_current_prices AS
SELECT 
    ph.price_id,
    ph.drink_id,
    d.drink_name,
    s.volume_ml,
    c.competitor_name,
    CASE WHEN c.is_benchmark THEN 'TRUE' ELSE 'FALSE' END as is_benchmark,
    ph.price,
    ph.price_per_100ml
FROM price_history ph
JOIN drinks d ON ph.drink_id = d.drink_id
JOIN sizes s ON ph.size_id = s.size_id
JOIN competitors c ON d.competitor_id = c.competitor_id
WHERE ph.is_current = TRUE;

-- 2. Аналитическая витрина сравнения цен с эталоном через LATERAL JOIN
CREATE OR REPLACE VIEW v_benchmark_delta AS
SELECT 
    cp.price_id,
    cp.drink_id,
    cp.drink_name,
    cp.volume_ml,
    cp.competitor_name,
    cp.price_per_100ml,
    sp.stars_pp100,
    sp.stars_volume,
    -- Расчет отклонения в %
    ((cp.price_per_100ml - sp.stars_pp100) / sp.stars_pp100) * 100 AS delta_pct,
    -- Расчет отклонения в абсолютных рублях на 100мл
    (cp.price_per_100ml - sp.stars_pp100) AS delta_rub
FROM v_current_prices cp
-- Подзапрос LATERAL ищет для каждого напитка конкурента ближайший по объему размер в STARS
CROSS JOIN LATERAL (
    SELECT 
        sp_inner.price_per_100ml AS stars_pp100,
        sp_inner.volume_ml AS stars_volume
    FROM v_current_prices sp_inner
    WHERE sp_inner.is_benchmark = 'TRUE'
      AND sp_inner.drink_name = cp.drink_name
    ORDER BY ABS(sp_inner.volume_ml - cp.volume_ml) ASC
    LIMIT 1
) sp
WHERE cp.competitor_name <> 'STARS Coffee';