-- Fact versus forecast over ml.ml_price_predictions. Run: psql -f sql/ml/fact_vs_forecast.sql
-- Historical = test window 2026-10-05..2026-10-26 with actuals. Future = loading_date > 2026-10-26, no actuals.

\pset footer off

-- Queries 3-9 describe one model: the most recently registered version.
SELECT model_version AS latest_version FROM ml.ml_model_registry
ORDER BY registered_at DESC, model_version LIMIT 1 \gset
\echo 'Latest model version:' :latest_version

\echo '1. Summary per model version and record type'
SELECT model_version, record_type, deals, first_loading_date, last_loading_date, mean_actual_rub,
       mean_predicted_rub, mae_rub, rmse_rub, mape_pct, r2, bias_rub, median_ape_pct, p90_ape_pct,
       within_10_pct, within_20_pct
FROM ml.v_prediction_error_summary
ORDER BY model_version, record_type;

\echo '2. Absolute percentage error distribution, historical'
SELECT model_version, ape_band, deals, pct_deals, mean_signed_pct_error
FROM ml.v_prediction_error_distribution
ORDER BY model_version, ape_band;

\echo '3. Actual vs predicted, largest absolute errors, historical'
SELECT price_id, loading_date, origin_city || ' -> ' || destination_city AS route, body_group, distance_km,
       actual_price_rub, predicted_price_rub, error_rub, ape_pct
FROM ml.v_fact_vs_forecast
WHERE model_version = :'latest_version' AND record_type = 'historical'
ORDER BY abs_error_rub DESC, price_id
LIMIT 10;

\echo '4. Error by body group, historical'
SELECT body_group_name, count(*) AS deals, round(avg(actual_price_rub)) AS mean_actual,
       round(avg(predicted_price_rub)) AS mean_predicted, round(avg(abs_error_rub)) AS mae_rub,
       round(avg(ape_pct), 2) AS mape_pct, round(avg(pct_error), 2) AS mean_signed_pct
FROM ml.v_fact_vs_forecast
WHERE model_version = :'latest_version' AND record_type = 'historical'
GROUP BY body_group_name
ORDER BY mape_pct DESC;

\echo '5. Error by distance band, historical'
SELECT CASE WHEN distance_km < 300 THEN '1: <300 km' WHEN distance_km < 1000 THEN '2: 300-999 km'
            WHEN distance_km < 3000 THEN '3: 1000-2999 km' ELSE '4: 3000+ km' END AS distance_band,
       count(*) AS deals, round(avg(actual_price_rub)) AS mean_actual, round(avg(abs_error_rub)) AS mae_rub,
       round(avg(ape_pct), 2) AS mape_pct, round(avg(pct_error), 2) AS mean_signed_pct
FROM ml.v_fact_vs_forecast
WHERE model_version = :'latest_version' AND record_type = 'historical'
GROUP BY distance_band
ORDER BY distance_band;

\echo '6. Error by loading week, historical'
SELECT date_trunc('week', loading_date)::date AS week, count(*) AS deals, round(avg(abs_error_rub)) AS mae_rub,
       round(avg(ape_pct), 2) AS mape_pct, round(avg(pct_error), 2) AS mean_signed_pct
FROM ml.v_fact_vs_forecast
WHERE model_version = :'latest_version' AND record_type = 'historical'
GROUP BY week
ORDER BY week;

\echo '7. Calibration: deciles of the prediction against the mean actual, historical'
SELECT decile, count(*) AS deals, round(avg(predicted_price_rub)) AS mean_predicted,
       round(avg(actual_price_rub)) AS mean_actual,
       round(100 * (avg(predicted_price_rub) / avg(actual_price_rub) - 1), 2) AS over_prediction_pct
FROM (SELECT *, ntile(10) OVER (ORDER BY predicted_price_rub) AS decile
      FROM ml.v_fact_vs_forecast
      WHERE model_version = :'latest_version' AND record_type = 'historical') AS ranked
GROUP BY decile
ORDER BY decile;

\echo '8. Future predictions by loading month and body group'
SELECT date_trunc('month', loading_date)::date AS loading_month, body_group_name, count(*) AS deals,
       round(avg(predicted_price_rub)) AS mean_predicted,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY predicted_price_rub / distance_km))::numeric, 2)
           AS median_predicted_rub_per_km
FROM ml.v_fact_vs_forecast
WHERE model_version = :'latest_version' AND record_type = 'future'
GROUP BY loading_month, body_group_name
ORDER BY loading_month, deals DESC
LIMIT 15;

\echo '9. Moscow -> Kazan, Тент: historical vs future'
SELECT record_type, count(*) AS deals, round(avg(actual_price_rub)) AS mean_actual,
       round(avg(predicted_price_rub)) AS mean_predicted, round(avg(ape_pct), 2) AS mape_pct,
       min(loading_date) AS first_date, max(loading_date) AS last_date
FROM ml.v_fact_vs_forecast
WHERE model_version = :'latest_version' AND origin_city = 'Москва' AND destination_city = 'Казань' AND body_group = 'TENT'
GROUP BY record_type
ORDER BY record_type DESC;

\echo '10. Scoring batches'
SELECT scoring_batch_id, model_version, status, rows_test, rows_future, rows_inserted, rows_updated,
       rows_unchanged, rows_deleted, round(extract(epoch FROM completed_at - started_at)::numeric, 2) AS seconds
FROM audit.ml_scoring_batch
ORDER BY scoring_batch_id;
