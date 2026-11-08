-- Quarantine under the name used in the assignment brief. audit.quarantine_price remains the table.
-- Repeatable.

BEGIN;

CREATE OR REPLACE VIEW audit.dq_quarantine_prices AS
SELECT price_id,
       reason_code,
       reason_detail,
       loading_date,
       loading_city_gcid,
       unloading_city_gcid,
       distance,
       total_weight,
       total_volume,
       price,
       currency_code,
       nearest_rate_date,
       price_type,
       is_fact,
       created_at,
       batch_id,
       rejected_at,
       source_system,
       source_record
FROM audit.quarantine_price;

COMMENT ON VIEW audit.dq_quarantine_prices IS
    'Rejected source deals, one row per deal and failed rule. Read-only view over audit.quarantine_price.';

COMMIT;
