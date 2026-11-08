-- Model registry, scoring audit, price predictions, and fact-versus-forecast views.
-- Repeatable. Views read ml and core only.

BEGIN;

CREATE TABLE IF NOT EXISTS ml.ml_model_registry (
    model_name            text NOT NULL,
    model_version         text NOT NULL,
    algorithm             text NOT NULL,
    as_of_date            date NOT NULL,
    train_start           date NOT NULL,
    train_end             date NOT NULL,
    test_start            date NOT NULL,
    test_end              date NOT NULL,
    random_seed           integer NOT NULL,
    feature_columns       text[] NOT NULL,
    categorical_features  text[] NOT NULL,
    training_config       jsonb NOT NULL,
    metrics               jsonb NOT NULL,
    baseline_metrics      jsonb NOT NULL,
    model_sha256          text NOT NULL,
    training_rows         integer NOT NULL,
    validation_rows       integer NOT NULL,
    best_iteration        integer NOT NULL,
    core_batch_id         bigint,
    registered_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (model_name, model_version),
    CONSTRAINT ml_model_registry_period_check CHECK (
        train_start <= train_end AND train_end < test_start AND test_start <= test_end AND test_end <= as_of_date
    )
);

COMMENT ON TABLE ml.ml_model_registry IS
    'One trained model version. Artifacts live in ml/artifacts/<model_version>/.';

CREATE TABLE IF NOT EXISTS audit.ml_scoring_batch (
    scoring_batch_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_name         text NOT NULL,
    model_version      text NOT NULL,
    as_of_date         date NOT NULL,
    status             text NOT NULL,
    started_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at       timestamptz,
    rows_test          integer,
    rows_future        integer,
    rows_inserted      integer,
    rows_updated       integer,
    rows_unchanged     integer,
    rows_deleted       integer,
    test_metrics       jsonb,
    future_summary     jsonb,
    error_message      text,
    FOREIGN KEY (model_name, model_version) REFERENCES ml.ml_model_registry (model_name, model_version),
    CONSTRAINT ml_scoring_batch_status_check CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT ml_scoring_batch_completed_check CHECK ((status = 'running') = (completed_at IS NULL))
);

CREATE INDEX IF NOT EXISTS ml_scoring_batch_model_idx
    ON audit.ml_scoring_batch (model_name, model_version, started_at);

CREATE TABLE IF NOT EXISTS ml.ml_price_predictions (
    price_id              bigint NOT NULL REFERENCES core.fact_deal (price_id) ON DELETE CASCADE,
    model_name            text NOT NULL,
    model_version         text NOT NULL,
    split_role            text NOT NULL,
    as_of_date            date NOT NULL,
    loading_date          date NOT NULL,
    loading_city_gcid     integer NOT NULL REFERENCES core.dim_city (gc_id),
    unloading_city_gcid   integer NOT NULL REFERENCES core.dim_city (gc_id),
    body_group            text NOT NULL REFERENCES core.dim_body_group (body_group_code),
    actual_price_rub      numeric(18, 2),
    predicted_price_rub   numeric(18, 2) NOT NULL,
    abs_error_rub         numeric(18, 2),
    ape_pct               numeric(12, 4),
    scored_at             timestamptz NOT NULL,
    scoring_batch_id      bigint NOT NULL REFERENCES audit.ml_scoring_batch (scoring_batch_id),
    PRIMARY KEY (price_id, model_name, model_version),
    FOREIGN KEY (model_name, model_version) REFERENCES ml.ml_model_registry (model_name, model_version),
    CONSTRAINT ml_price_predictions_role_check CHECK (
        (split_role = 'test' AND loading_date <= as_of_date
         AND actual_price_rub > 0 AND abs_error_rub = abs(actual_price_rub - predicted_price_rub)
         AND ape_pct IS NOT NULL)
        OR
        (split_role = 'future' AND loading_date > as_of_date
         AND actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ml_price_predictions_version_role_idx
    ON ml.ml_price_predictions (model_name, model_version, split_role, loading_date);
CREATE INDEX IF NOT EXISTS ml_price_predictions_route_idx
    ON ml.ml_price_predictions (loading_city_gcid, unloading_city_gcid, body_group);

COMMENT ON TABLE ml.ml_price_predictions IS
    'One prediction per deal and model version: held-out test deals with actuals, future deals without.';
COMMENT ON COLUMN ml.ml_price_predictions.actual_price_rub IS
    'core.fact_deal.price_rub for test rows. Always NULL for future rows, whatever the source price says.';
COMMENT ON COLUMN ml.ml_price_predictions.ape_pct IS 'abs_error_rub / actual_price_rub * 100.';

-- Staged predictions in, typed rows out. actual comes from core for test rows only.
CREATE OR REPLACE FUNCTION ml.upsert_price_predictions(
    p_model_name text, p_model_version text, p_as_of date, p_batch_id bigint
)
RETURNS TABLE (rows_inserted bigint, rows_updated bigint, rows_deleted bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted bigint;
    v_updated bigint;
    v_deleted bigint;
BEGIN
    WITH upserted AS (
        INSERT INTO ml.ml_price_predictions AS t (
            price_id, model_name, model_version, split_role, as_of_date, loading_date,
            loading_city_gcid, unloading_city_gcid, body_group, actual_price_rub,
            predicted_price_rub, abs_error_rub, ape_pct, scored_at, scoring_batch_id
        )
        SELECT s.price_id, p_model_name, p_model_version, s.split_role, p_as_of, f.loading_date,
               f.loading_city_gcid, f.unloading_city_gcid, f.body_group_code, a.actual,
               s.predicted, abs(a.actual - s.predicted),
               round(abs(a.actual - s.predicted) / a.actual * 100, 4),
               now(), p_batch_id
        FROM (SELECT price_id, split_role, round(predicted_price_rub::numeric, 2) AS predicted
              FROM stage_price_predictions) AS s
        JOIN core.fact_deal AS f ON f.price_id = s.price_id
        CROSS JOIN LATERAL (
            SELECT CASE WHEN s.split_role = 'test' THEN f.price_rub END AS actual
        ) AS a
        ON CONFLICT (price_id, model_name, model_version) DO UPDATE SET
            split_role = EXCLUDED.split_role,
            as_of_date = EXCLUDED.as_of_date,
            loading_date = EXCLUDED.loading_date,
            loading_city_gcid = EXCLUDED.loading_city_gcid,
            unloading_city_gcid = EXCLUDED.unloading_city_gcid,
            body_group = EXCLUDED.body_group,
            actual_price_rub = EXCLUDED.actual_price_rub,
            predicted_price_rub = EXCLUDED.predicted_price_rub,
            abs_error_rub = EXCLUDED.abs_error_rub,
            ape_pct = EXCLUDED.ape_pct,
            scored_at = EXCLUDED.scored_at,
            scoring_batch_id = EXCLUDED.scoring_batch_id
        WHERE (t.split_role, t.as_of_date, t.loading_date, t.loading_city_gcid, t.unloading_city_gcid,
               t.body_group, t.actual_price_rub, t.predicted_price_rub)
              IS DISTINCT FROM
              (EXCLUDED.split_role, EXCLUDED.as_of_date, EXCLUDED.loading_date, EXCLUDED.loading_city_gcid,
               EXCLUDED.unloading_city_gcid, EXCLUDED.body_group, EXCLUDED.actual_price_rub,
               EXCLUDED.predicted_price_rub)
        RETURNING (xmax = 0) AS inserted
    )
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted)
    INTO v_inserted, v_updated
    FROM upserted;

    DELETE FROM ml.ml_price_predictions AS t
    WHERE t.model_name = p_model_name
      AND t.model_version = p_model_version
      AND NOT EXISTS (SELECT 1 FROM stage_price_predictions AS s WHERE s.price_id = t.price_id);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RETURN QUERY SELECT v_inserted, v_updated, v_deleted;
END;
$$;

CREATE OR REPLACE VIEW ml.v_fact_vs_forecast AS
SELECT p.model_name,
       p.model_version,
       p.split_role,
       CASE p.split_role WHEN 'test' THEN 'historical' ELSE 'future' END AS record_type,
       p.price_id,
       p.loading_date,
       p.loading_city_gcid,
       o.name AS origin_city,
       p.unloading_city_gcid,
       d.name AS destination_city,
       p.body_group,
       b.body_group_name,
       f.distance_km,
       p.actual_price_rub,
       p.predicted_price_rub,
       p.predicted_price_rub - p.actual_price_rub AS error_rub,
       p.abs_error_rub,
       round((p.predicted_price_rub - p.actual_price_rub) / p.actual_price_rub * 100, 4) AS pct_error,
       p.ape_pct,
       CASE
           WHEN p.ape_pct IS NULL THEN NULL
           WHEN p.ape_pct < 5 THEN '00-05%'
           WHEN p.ape_pct < 10 THEN '05-10%'
           WHEN p.ape_pct < 20 THEN '10-20%'
           WHEN p.ape_pct < 30 THEN '20-30%'
           WHEN p.ape_pct < 50 THEN '30-50%'
           ELSE '50%+'
       END AS ape_band,
       p.scored_at,
       p.scoring_batch_id
FROM ml.ml_price_predictions AS p
JOIN core.fact_deal AS f ON f.price_id = p.price_id
JOIN core.dim_city AS o ON o.gc_id = p.loading_city_gcid
JOIN core.dim_city AS d ON d.gc_id = p.unloading_city_gcid
JOIN core.dim_body_group AS b ON b.body_group_code = p.body_group;

COMMENT ON VIEW ml.v_fact_vs_forecast IS
    'Actual vs predicted per deal. pct_error is signed (positive = over-prediction). Future rows have no error.';

CREATE OR REPLACE VIEW ml.v_prediction_error_summary AS
SELECT model_name,
       model_version,
       record_type,
       count(*) AS deals,
       min(loading_date) AS first_loading_date,
       max(loading_date) AS last_loading_date,
       round(avg(actual_price_rub), 2) AS mean_actual_rub,
       round(avg(predicted_price_rub), 2) AS mean_predicted_rub,
       round(avg(abs_error_rub), 2) AS mae_rub,
       round(sqrt(avg(power(error_rub, 2))), 2) AS rmse_rub,
       round(avg(ape_pct), 4) AS mape_pct,
       round(1 - sum(power(error_rub, 2)) / nullif(
           sum(power(actual_price_rub, 2)) - power(sum(actual_price_rub), 2) / nullif(count(actual_price_rub), 0), 0
       ), 6) AS r2,
       round(avg(error_rub), 2) AS bias_rub,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY ape_pct))::numeric, 4) AS median_ape_pct,
       round((percentile_cont(0.9) WITHIN GROUP (ORDER BY ape_pct))::numeric, 4) AS p90_ape_pct,
       round(100.0 * count(*) FILTER (WHERE ape_pct < 10) / nullif(count(ape_pct), 0), 2) AS within_10_pct,
       round(100.0 * count(*) FILTER (WHERE ape_pct < 20) / nullif(count(ape_pct), 0), 2) AS within_20_pct
FROM ml.v_fact_vs_forecast
GROUP BY model_name, model_version, record_type;

COMMENT ON VIEW ml.v_prediction_error_summary IS
    'Per model version: historical rows carry R2, MAE, RMSE, MAPE; future rows carry counts and mean prediction.';

CREATE OR REPLACE VIEW ml.v_prediction_error_distribution AS
SELECT model_name,
       model_version,
       ape_band,
       count(*) AS deals,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY model_name, model_version), 2) AS pct_deals,
       round(avg(pct_error), 4) AS mean_signed_pct_error
FROM ml.v_fact_vs_forecast
WHERE record_type = 'historical'
GROUP BY model_name, model_version, ape_band;

COMMIT;
