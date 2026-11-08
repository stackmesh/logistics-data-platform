-- Example queries for mart.mart_market_corridor. Snapshot as_of = 2026-10-26.
-- 2026-10 is the as-of month and holds price dates up to 2026-10-26 only.
-- Run: psql -f sql/mart/examples.sql

\pset footer off

\echo '1a. Moscow -> Kazan, every body group and weight category, 2026-10'
SELECT body_group_name, weight_category, trips_count, carriers_count,
       p25_trip, median_trip, p75_trip, median_km, mom_pct, yoy_pct, reliability_level
FROM mart.mart_market_corridor AS m
JOIN core.dim_city AS o ON o.gc_id = m.loading_city_gcid
JOIN core.dim_city AS d ON d.gc_id = m.unloading_city_gcid
WHERE o.name = 'Москва' AND d.name = 'Казань' AND m.price_month = DATE '2026-10-01'
ORDER BY trips_count DESC, body_group_name, weight_category;

\echo '1b. Moscow -> Kazan, Тент FTL, last 13 months'
SELECT price_month, trips_count, carriers_count, p25_trip, median_trip, p75_trip,
       mom_pct, yoy_pct, freshness_days, reliability_level
FROM mart.mart_market_corridor
WHERE loading_city_gcid = 1001 AND unloading_city_gcid = 1003
  AND body_group_code = 'TENT' AND weight_category = 'FTL'
  AND price_month BETWEEN DATE '2025-10-01' AND DATE '2026-10-01'
ORDER BY price_month;

\echo '2. Top 20 most expensive routes by median trip price, 2026-10, MEDIUM or HIGH'
SELECT origin_city_name || ' -> ' || destination_city_name AS route, body_group_name, weight_category,
       median_trip, p25_trip, p75_trip, median_km, trips_count, reliability_level
FROM mart.mart_market_corridor
WHERE price_month = DATE '2026-10-01' AND reliability_level IN ('HIGH', 'MEDIUM')
ORDER BY median_trip DESC
LIMIT 20;

\echo '3a. Reliability mix, 2026-10'
SELECT reliability_level, count(*) AS cells, sum(trips_count) AS observations,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_cells
FROM mart.mart_market_corridor
WHERE price_month = DATE '2026-10-01'
GROUP BY reliability_level
ORDER BY cells DESC;

\echo '3b. LOW_DATA routes closest to MEDIUM, 2026-10'
SELECT origin_city_name || ' -> ' || destination_city_name AS route, body_group_name, weight_category,
       trips_count, carriers_count, median_trip, cv_rate, freshness_days
FROM mart.mart_market_corridor
WHERE price_month = DATE '2026-10-01' AND reliability_level = 'LOW_DATA'
ORDER BY trips_count DESC, carriers_count DESC, route, body_group_name, weight_category
LIMIT 20;

\echo '4. Highest MoM increase of the median, 2026-10, MEDIUM or HIGH'
SELECT origin_city_name || ' -> ' || destination_city_name AS route, body_group_name, weight_category,
       median_trip, round(median_trip / (1 + mom_pct / 100), 2) AS prev_month_median,
       mom_pct, yoy_pct, trips_count, reliability_level
FROM mart.mart_market_corridor
WHERE price_month = DATE '2026-10-01' AND mom_pct IS NOT NULL AND reliability_level IN ('HIGH', 'MEDIUM')
ORDER BY mom_pct DESC
LIMIT 20;

\echo '5. Trip price vs kilometre price, 2026-10, MEDIUM or HIGH'
SELECT origin_city_name || ' -> ' || destination_city_name AS route, body_group_name, weight_category,
       median_trip, median_km,
       round(median_trip / median_km) AS implied_km,
       rank() OVER (ORDER BY median_trip DESC) AS trip_rank,
       rank() OVER (ORDER BY median_km DESC) AS km_rank,
       trips_count
FROM mart.mart_market_corridor
WHERE price_month = DATE '2026-10-01' AND reliability_level IN ('HIGH', 'MEDIUM')
ORDER BY km_rank, route, body_group_name, weight_category
LIMIT 20;

\echo '6. Largest carrier competition, 2026-10'
SELECT origin_city_name || ' -> ' || destination_city_name AS route, body_group_name, weight_category,
       carriers_count, trips_count,
       round(carriers_count::numeric / trips_count, 2) AS carriers_per_trip,
       median_trip, cv_rate, reliability_level
FROM mart.mart_market_corridor
WHERE price_month = DATE '2026-10-01'
ORDER BY carriers_count DESC, trips_count DESC, route, body_group_name, weight_category
LIMIT 20;
