-- Prediction mart with the columns named in the assignment brief. ml.ml_price_predictions remains the table.
-- Lives under sql/ml because it depends on ml objects, which are created after sql/mart.
-- Repeatable.

BEGIN;

CREATE OR REPLACE VIEW mart.ml_price_predictions AS
SELECT price_id,
       loading_date,
       loading_city_gcid,
       unloading_city_gcid,
       body_group,
       actual_price_rub,
       predicted_price_rub,
       abs_error_rub,
       ape_pct,
       model_name,
       scored_at,
       model_version,
       split_role
FROM ml.ml_price_predictions;

COMMENT ON VIEW mart.ml_price_predictions IS
    'Fact vs forecast per deal and model version. Future rows (split_role = future) have NULL actual and error.';

COMMIT;
