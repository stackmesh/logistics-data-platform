-- Market corridor mart: one row per month, origin, destination, body group, and weight category.
-- Reads validated core rows only. Repeatable.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.mart_refresh (
    refresh_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mart_name      text NOT NULL,
    as_of_date     date NOT NULL,
    status         text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz,
    rows_deleted   bigint CHECK (rows_deleted >= 0),
    rows_inserted  bigint CHECK (rows_inserted >= 0),
    error_message  text,
    CHECK (
        (status = 'running' AND completed_at IS NULL)
        OR (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS mart_refresh_started_idx ON audit.mart_refresh (mart_name, started_at);

ALTER TABLE audit.mart_refresh ADD COLUMN IF NOT EXISTS input_fingerprint text;

COMMENT ON COLUMN audit.mart_refresh.input_fingerprint IS
    'mart.market_corridor_input_fingerprint() under the refresh lock. Equal value and as_of means the rebuild can be skipped.';

CREATE TABLE IF NOT EXISTS mart.mart_market_corridor (
    price_month            date NOT NULL,
    loading_city_gcid      integer NOT NULL REFERENCES core.dim_city (gc_id),
    unloading_city_gcid    integer NOT NULL REFERENCES core.dim_city (gc_id),
    body_group_code        text NOT NULL REFERENCES core.dim_body_group (body_group_code),
    weight_category        text NOT NULL,
    origin_city_name       text NOT NULL,
    destination_city_name  text NOT NULL,
    body_group_name        text NOT NULL,
    p25_trip               numeric(18, 2) NOT NULL,
    median_trip            numeric(18, 2) NOT NULL,
    p75_trip               numeric(18, 2) NOT NULL,
    avg_trip               numeric(18, 2) NOT NULL,
    p25_km                 numeric(18, 4) NOT NULL,
    median_km              numeric(18, 4) NOT NULL,
    p75_km                 numeric(18, 4) NOT NULL,
    trips_count            integer NOT NULL,
    carriers_count         integer NOT NULL,
    cv_rate                numeric(12, 6),
    mom_pct                numeric(12, 4),
    yoy_pct                numeric(12, 4),
    max_price_date         date NOT NULL,
    freshness_days         integer NOT NULL,
    reliability_level      text NOT NULL,
    as_of_date             date NOT NULL,
    refresh_id             bigint NOT NULL REFERENCES audit.mart_refresh (refresh_id),
    PRIMARY KEY (price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category),
    CONSTRAINT mart_market_corridor_month_check
        CHECK (price_month = date_trunc('month', price_month)::date),
    CONSTRAINT mart_market_corridor_weight_check
        CHECK (weight_category IN ('G20T', 'FTL', '10T', '5T', 'L5T', 'OTH')),
    CONSTRAINT mart_market_corridor_band_check
        CHECK (p25_trip <= median_trip AND median_trip <= p75_trip AND p25_km <= median_km AND median_km <= p75_km),
    CONSTRAINT mart_market_corridor_counts_check
        CHECK (trips_count > 0 AND carriers_count >= 0 AND carriers_count <= trips_count),
    CONSTRAINT mart_market_corridor_dates_check
        CHECK (max_price_date <= as_of_date
               AND max_price_date >= price_month
               AND freshness_days = as_of_date - max_price_date),
    CONSTRAINT mart_market_corridor_reliability_check
        CHECK (reliability_level IN ('HIGH', 'MEDIUM', 'LOW_DATA'))
);

CREATE INDEX IF NOT EXISTS mart_market_corridor_route_idx
    ON mart.mart_market_corridor (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, price_month);

CREATE INDEX IF NOT EXISTS mart_market_corridor_month_reliability_idx
    ON mart.mart_market_corridor (price_month, reliability_level);

COMMENT ON TABLE mart.mart_market_corridor IS
    'Market price corridor per month (from price_dates), city pair, body group, and weight category. '
    'Completed direct non-outlier deals with price_date <= as_of. LOW_DATA rows are kept, not dropped.';
COMMENT ON COLUMN mart.mart_market_corridor.price_month IS 'date_trunc(month, core.fact_deal_date.price_date).';
COMMENT ON COLUMN mart.mart_market_corridor.trips_count IS
    'COUNT(DISTINCT price_id) in the cell. A deal spanning several months counts once in each month.';
COMMENT ON COLUMN mart.mart_market_corridor.carriers_count IS
    'COUNT(DISTINCT organization_id) over price_type 3 and 9 rows of the cell.';
COMMENT ON COLUMN mart.mart_market_corridor.cv_rate IS
    'stddev_samp(price_rub) / avg(price_rub). NULL below two observations or with a zero mean. A ratio, not a percent.';
COMMENT ON COLUMN mart.mart_market_corridor.mom_pct IS
    'Percent change of median_trip against the same cell in the exact previous calendar month, else NULL.';
COMMENT ON COLUMN mart.mart_market_corridor.yoy_pct IS
    'Percent change of median_trip against the same cell exactly twelve months earlier, else NULL.';
COMMENT ON COLUMN mart.mart_market_corridor.freshness_days IS 'as_of_date - max_price_date.';

-- The transformation. Values are unrounded; the refresh rounds on insert.
CREATE OR REPLACE FUNCTION mart.build_market_corridor(p_as_of date)
RETURNS TABLE (
    price_month date,
    loading_city_gcid integer,
    unloading_city_gcid integer,
    body_group_code text,
    weight_category text,
    origin_city_name text,
    destination_city_name text,
    body_group_name text,
    p25_trip double precision,
    median_trip double precision,
    p75_trip double precision,
    avg_trip numeric,
    p25_km double precision,
    median_km double precision,
    p75_km double precision,
    trips_count bigint,
    carriers_count bigint,
    cv_rate numeric,
    mom_pct double precision,
    yoy_pct double precision,
    max_price_date date,
    freshness_days integer,
    reliability_level text
)
LANGUAGE sql
STABLE
AS $$
WITH observation AS (
    -- One row per deal, month, and body group. The bridge already holds one row per
    -- (price_id, body_group_code); two dates of one deal in a month collapse to the latest.
    SELECT d.price_id, d.price_month, max(d.price_date) AS price_date, b.body_group_code
    FROM core.fact_deal_date AS d
    JOIN core.bridge_deal_body_group AS b ON b.price_id = d.price_id
    WHERE d.price_date <= p_as_of
    GROUP BY d.price_id, d.price_month, b.body_group_code
),
eligible AS (
    SELECT o.price_month,
           o.price_date,
           o.body_group_code,
           f.price_id,
           f.loading_city_gcid,
           f.unloading_city_gcid,
           f.weight_category,
           f.price_rub,
           f.price_rub / f.distance_km AS price_km,
           f.organization_id,
           f.price_type
    FROM observation AS o
    JOIN core.fact_deal AS f ON f.price_id = o.price_id
    WHERE f.is_fact
      AND f.price_type IN (1, 2, 3, 4, 5, 9)
      AND NOT f.complex
      AND NOT f.outlier_mark
      AND f.price_rub > 0
      AND f.distance_km > 0
      AND f.currency_code IN ('643', '840', '978')
      AND f.fx_rate > 0
),
cell AS (
    SELECT e.price_month,
           e.loading_city_gcid,
           e.unloading_city_gcid,
           e.body_group_code,
           e.weight_category,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY e.price_rub) AS p25_trip,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY e.price_rub) AS median_trip,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY e.price_rub) AS p75_trip,
           avg(e.price_rub) AS avg_trip,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY e.price_km) AS p25_km,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY e.price_km) AS median_km,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY e.price_km) AS p75_km,
           count(DISTINCT e.price_id) AS trips_count,
           count(DISTINCT e.organization_id) FILTER (WHERE e.price_type IN (3, 9)) AS carriers_count,
           CASE
               WHEN count(*) < 2 OR avg(e.price_rub) = 0 THEN NULL
               ELSE stddev_samp(e.price_rub) / avg(e.price_rub)
           END AS cv_rate,
           max(e.price_date) AS max_price_date
    FROM eligible AS e
    GROUP BY e.price_month, e.loading_city_gcid, e.unloading_city_gcid, e.body_group_code, e.weight_category
)
SELECT c.price_month,
       c.loading_city_gcid,
       c.unloading_city_gcid,
       c.body_group_code,
       c.weight_category,
       oc.name::text,
       dc.name::text,
       g.body_group_name,
       c.p25_trip,
       c.median_trip,
       c.p75_trip,
       c.avg_trip,
       c.p25_km,
       c.median_km,
       c.p75_km,
       c.trips_count,
       c.carriers_count,
       c.cv_rate,
       CASE WHEN prev.median_trip IS NULL OR prev.median_trip = 0 THEN NULL
            ELSE (c.median_trip / prev.median_trip - 1) * 100 END,
       CASE WHEN ly.median_trip IS NULL OR ly.median_trip = 0 THEN NULL
            ELSE (c.median_trip / ly.median_trip - 1) * 100 END,
       c.max_price_date,
       p_as_of - c.max_price_date,
       CASE
           WHEN c.trips_count >= 30 AND c.carriers_count >= 5 AND p_as_of - c.max_price_date <= 30 THEN 'HIGH'
           WHEN c.trips_count >= 10 THEN 'MEDIUM'
           ELSE 'LOW_DATA'
       END
FROM cell AS c
JOIN core.dim_city AS oc ON oc.gc_id = c.loading_city_gcid
JOIN core.dim_city AS dc ON dc.gc_id = c.unloading_city_gcid
JOIN core.dim_body_group AS g ON g.body_group_code = c.body_group_code
LEFT JOIN cell AS prev
       ON prev.price_month = (c.price_month - interval '1 month')::date
      AND prev.loading_city_gcid = c.loading_city_gcid
      AND prev.unloading_city_gcid = c.unloading_city_gcid
      AND prev.body_group_code = c.body_group_code
      AND prev.weight_category = c.weight_category
LEFT JOIN cell AS ly
       ON ly.price_month = (c.price_month - interval '1 year')::date
      AND ly.loading_city_gcid = c.loading_city_gcid
      AND ly.unloading_city_gcid = c.unloading_city_gcid
      AND ly.body_group_code = c.body_group_code
      AND ly.weight_category = c.weight_category
$$;

COMMENT ON FUNCTION mart.build_market_corridor(date) IS
    'Market corridor aggregate for a snapshot date. Unrounded. Used by mart.refresh_market_corridor.';

-- Full, atomic replacement. Runs inside the caller's transaction; readers keep seeing the
-- previous rows until commit. Serialized with core loads by the shared ETL advisory lock.
CREATE OR REPLACE PROCEDURE mart.refresh_market_corridor(
    p_as_of date,
    p_refresh_id bigint,
    INOUT rows_deleted bigint DEFAULT NULL,
    INOUT rows_inserted bigint DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('logistics_dwh.core_etl'));

    DELETE FROM mart.mart_market_corridor;
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    INSERT INTO mart.mart_market_corridor (
        price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category,
        origin_city_name, destination_city_name, body_group_name,
        p25_trip, median_trip, p75_trip, avg_trip, p25_km, median_km, p75_km,
        trips_count, carriers_count, cv_rate, mom_pct, yoy_pct,
        max_price_date, freshness_days, reliability_level, as_of_date, refresh_id
    )
    SELECT b.price_month, b.loading_city_gcid, b.unloading_city_gcid, b.body_group_code, b.weight_category,
           b.origin_city_name, b.destination_city_name, b.body_group_name,
           b.p25_trip, b.median_trip, b.p75_trip, b.avg_trip, b.p25_km, b.median_km, b.p75_km,
           b.trips_count, b.carriers_count, b.cv_rate, b.mom_pct, b.yoy_pct,
           b.max_price_date, b.freshness_days, b.reliability_level, p_as_of, p_refresh_id
    FROM mart.build_market_corridor(p_as_of) AS b;
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
END
$$;

COMMENT ON PROCEDURE mart.refresh_market_corridor(date, bigint, bigint, bigint) IS
    'Replace mart.mart_market_corridor for a snapshot date inside the caller''s transaction.';

-- Hash of every core value the build reads, plus the build and refresh definitions.
CREATE OR REPLACE FUNCTION mart.market_corridor_input_fingerprint()
RETURNS text
LANGUAGE sql
STABLE
AS $$
SELECT md5(concat_ws('|',
    (SELECT md5(coalesce(string_agg(md5(ROW(price_id, loading_city_gcid, unloading_city_gcid, weight_category,
                                            price_rub, distance_km, organization_id, price_type, is_fact,
                                            complex, outlier_mark, currency_code, fx_rate)::text),
                                    '' ORDER BY price_id), ''))
     FROM core.fact_deal),
    (SELECT md5(coalesce(string_agg(md5(ROW(price_id, price_date)::text), '' ORDER BY price_id, price_date), ''))
     FROM core.fact_deal_date),
    (SELECT md5(coalesce(string_agg(md5(ROW(price_id, body_group_code)::text), ''
                                    ORDER BY price_id, body_group_code), ''))
     FROM core.bridge_deal_body_group),
    (SELECT md5(coalesce(string_agg(md5(ROW(gc_id, name)::text), '' ORDER BY gc_id), '')) FROM core.dim_city),
    (SELECT md5(coalesce(string_agg(md5(ROW(body_group_code, body_group_name)::text), ''
                                    ORDER BY body_group_code), ''))
     FROM core.dim_body_group),
    md5(pg_get_functiondef('mart.build_market_corridor(date)'::regprocedure)),
    md5(pg_get_functiondef('mart.refresh_market_corridor(date, bigint, bigint, bigint)'::regprocedure))
))
$$;

COMMENT ON FUNCTION mart.market_corridor_input_fingerprint() IS
    'Change detector for mart.mart_market_corridor inputs. Used to skip a rebuild when nothing it reads changed.';

COMMIT;
