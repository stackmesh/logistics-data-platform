-- BI layer for Apache Superset: read-only views in schema mart with consistent filter column names
-- (month, origin_city, destination_city, body_group, weight_category, reliability_level).
-- No new business rules: the views rename mart and ML columns, and expose the mart's own population.
-- Lives under sql/ml because v_fact_vs_forecast depends on ml objects. Repeatable.

BEGIN;

-- SECURITY DEFINER so the BI role can read the mart population without privileges on core.
-- Uses the as_of_date of the current mart rows, so observations and mart cells describe one snapshot.
CREATE OR REPLACE FUNCTION mart.bi_market_observations()
RETURNS TABLE (
    price_month date,
    price_date date,
    body_group_code text,
    price_id bigint,
    loading_city_gcid integer,
    unloading_city_gcid integer,
    weight_category text,
    price_rub numeric,
    price_km numeric,
    organization_id integer,
    price_type smallint,
    as_of_date date
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
SELECT o.*, s.as_of_date
FROM (SELECT max(m.as_of_date) AS as_of_date FROM mart.mart_market_corridor AS m) AS s
CROSS JOIN LATERAL mart.market_corridor_observations(s.as_of_date) AS o
WHERE s.as_of_date IS NOT NULL
$$;

REVOKE ALL ON FUNCTION mart.bi_market_observations() FROM PUBLIC;

COMMENT ON FUNCTION mart.bi_market_observations() IS
    'mart.market_corridor_observations at the as_of_date of mart.mart_market_corridor, for the BI role.';

-- One row per observation of the market corridor (deal x price month x body group), with its cell's
-- reliability. Distinct deals, distinct carriers, and percentiles over any filter combination come from here.
CREATE OR REPLACE VIEW mart.v_market_observation AS
SELECT o.price_month,
       to_char(o.price_month, 'YYYY-MM') AS month,
       extract(year FROM o.price_month)::integer AS year,
       extract(month FROM o.price_month)::integer AS month_of_year,
       o.price_date,
       o.price_id,
       c.origin_city_name AS origin_city,
       c.destination_city_name AS destination_city,
       c.body_group_name AS body_group,
       o.weight_category,
       o.price_rub,
       o.price_km,
       CASE WHEN o.price_type IN (3, 9) THEN o.organization_id END AS carrier_id,
       c.reliability_level,
       o.as_of_date
FROM mart.bi_market_observations() AS o
JOIN mart.mart_market_corridor AS c
  ON c.price_month = o.price_month
 AND c.loading_city_gcid = o.loading_city_gcid
 AND c.unloading_city_gcid = o.unloading_city_gcid
 AND c.body_group_code = o.body_group_code
 AND c.weight_category = o.weight_category;

COMMENT ON VIEW mart.v_market_observation IS
    'Market corridor population, one row per deal, price month, and body group. carrier_id follows the mart '
    'rule (organization of price_type 3 or 9). SUM over a cell reproduces trips_count, carriers_count, and percentiles.';

-- mart.mart_market_corridor with BI column names. One row per month, route, body group, weight category.
CREATE OR REPLACE VIEW mart.v_route_pricing AS
SELECT price_month,
       to_char(price_month, 'YYYY-MM') AS month,
       origin_city_name AS origin_city,
       destination_city_name AS destination_city,
       body_group_name AS body_group,
       weight_category,
       p25_trip,
       median_trip AS p50_trip,
       p75_trip,
       avg_trip,
       p25_km,
       median_km AS p50_km,
       p75_km,
       mom_pct,
       yoy_pct,
       trips_count,
       carriers_count,
       reliability_level,
       cv_rate,
       max_price_date,
       freshness_days,
       as_of_date
FROM mart.mart_market_corridor;

COMMENT ON VIEW mart.v_route_pricing IS 'mart.mart_market_corridor with BI column names (median_* as p50_*).';

-- ml.v_fact_vs_forecast with BI column names. month is the loading month.
CREATE OR REPLACE VIEW mart.v_fact_vs_forecast AS
SELECT v.record_type,
       v.split_role,
       v.model_name,
       v.model_version,
       v.price_id,
       v.loading_date,
       to_char(v.loading_date, 'YYYY-MM') AS month,
       v.origin_city::text AS origin_city,
       v.destination_city::text AS destination_city,
       v.body_group_name AS body_group,
       f.weight_category::text AS weight_category,
       v.distance_km,
       v.actual_price_rub,
       v.predicted_price_rub,
       v.error_rub,
       v.abs_error_rub,
       v.pct_error,
       v.ape_pct,
       v.ape_band,
       v.scored_at
FROM ml.v_fact_vs_forecast AS v
JOIN core.fact_deal AS f ON f.price_id = v.price_id;

COMMENT ON VIEW mart.v_fact_vs_forecast IS
    'Actual vs predicted per deal for BI. record_type historical has actuals and errors; future has predictions only.';

-- The BI role is created by docker/postgres/03_bi_reader.sh. Grants apply once it exists.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader') THEN
        GRANT USAGE ON SCHEMA mart TO bi_reader;
        GRANT SELECT ON ALL TABLES IN SCHEMA mart TO bi_reader;
        GRANT EXECUTE ON FUNCTION mart.bi_market_observations() TO bi_reader;
        ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO bi_reader;
    END IF;
END
$$;

COMMIT;
