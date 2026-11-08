-- ETL load contract: batch counters, quarantine detail and payload, and the FX key.
-- Repeatable. Written for tables that are still empty or already in this shape.

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'audit' AND table_name = 'etl_batch' AND column_name = 'finished_at'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'audit' AND table_name = 'etl_batch' AND column_name = 'completed_at'
    ) THEN
        ALTER TABLE audit.etl_batch RENAME COLUMN finished_at TO completed_at;
    END IF;
END
$$;

ALTER TABLE audit.etl_batch DROP CONSTRAINT IF EXISTS etl_batch_row_counts_check;
ALTER TABLE audit.etl_batch DROP CONSTRAINT IF EXISTS etl_batch_counts_check;
ALTER TABLE audit.etl_batch DROP CONSTRAINT IF EXISTS etl_batch_completed_check;

ALTER TABLE audit.etl_batch
    DROP COLUMN IF EXISTS rows_read,
    DROP COLUMN IF EXISTS rows_landed,
    DROP COLUMN IF EXISTS rows_quarantined,
    DROP COLUMN IF EXISTS rows_core,
    DROP COLUMN IF EXISTS rows_mart,
    DROP COLUMN IF EXISTS rows_scored,
    ADD COLUMN IF NOT EXISTS completed_at timestamptz,
    ADD COLUMN IF NOT EXISTS extracted_count bigint,
    ADD COLUMN IF NOT EXISTS accepted_count bigint,
    ADD COLUMN IF NOT EXISTS rejected_count bigint,
    ADD COLUMN IF NOT EXISTS inserted_count bigint,
    ADD COLUMN IF NOT EXISTS updated_count bigint,
    ADD COLUMN IF NOT EXISTS unchanged_count bigint,
    ADD COLUMN IF NOT EXISTS deleted_count bigint,
    ADD COLUMN IF NOT EXISTS summary jsonb;

ALTER TABLE audit.etl_batch
    ADD CONSTRAINT etl_batch_counts_check CHECK (
        (extracted_count IS NULL OR extracted_count >= 0)
        AND (accepted_count IS NULL OR accepted_count >= 0)
        AND (rejected_count IS NULL OR rejected_count >= 0)
        AND (inserted_count IS NULL OR inserted_count >= 0)
        AND (updated_count IS NULL OR updated_count >= 0)
        AND (unchanged_count IS NULL OR unchanged_count >= 0)
        AND (deleted_count IS NULL OR deleted_count >= 0)
    ),
    ADD CONSTRAINT etl_batch_completed_check CHECK (
        (status = 'running' AND completed_at IS NULL)
        OR (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL AND completed_at >= started_at)
    );

COMMENT ON COLUMN audit.etl_batch.start_date IS 'Inclusive prices.loading_date lower bound. NULL for a full load.';
COMMENT ON COLUMN audit.etl_batch.end_date IS 'Inclusive prices.loading_date upper bound. NULL for a full load.';
COMMENT ON COLUMN audit.etl_batch.inserted_count IS 'core.fact_deal rows inserted by this batch.';
COMMENT ON COLUMN audit.etl_batch.updated_count IS 'core.fact_deal rows whose values changed in this batch.';
COMMENT ON COLUMN audit.etl_batch.deleted_count IS 'core.fact_deal rows removed because they left the window or failed checks.';
COMMENT ON COLUMN audit.etl_batch.summary IS 'Full execution summary: per-layer counts, reason counts, and timings.';

ALTER TABLE audit.dq_statistic DROP CONSTRAINT IF EXISTS dq_statistic_reason_check;
ALTER TABLE audit.dq_statistic
    ADD CONSTRAINT dq_statistic_reason_check CHECK (reason_code IN (
        'PRICE_NON_POSITIVE',
        'RUB_PRICE_LE_1000',
        'DISTANCE_NON_POSITIVE',
        'WEIGHT_NEGATIVE',
        'VOLUME_NEGATIVE',
        'ORIGIN_CITY_NOT_FOUND',
        'DESTINATION_CITY_NOT_FOUND',
        'CURRENCY_UNSUPPORTED',
        'FX_RATE_MISSING'
    ));

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'audit' AND table_name = 'quarantine_price' AND column_name = 'detected_at'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'audit' AND table_name = 'quarantine_price' AND column_name = 'rejected_at'
    ) THEN
        ALTER TABLE audit.quarantine_price RENAME COLUMN detected_at TO rejected_at;
    END IF;
END
$$;

ALTER TABLE audit.quarantine_price
    ADD COLUMN IF NOT EXISTS reason_detail text,
    ADD COLUMN IF NOT EXISTS source_record jsonb;

ALTER TABLE audit.quarantine_price
    ALTER COLUMN reason_detail SET NOT NULL,
    ALTER COLUMN source_record SET NOT NULL;

ALTER TABLE audit.quarantine_price DROP CONSTRAINT IF EXISTS quarantine_price_reason_check;
ALTER TABLE audit.quarantine_price
    ADD CONSTRAINT quarantine_price_reason_check CHECK (reason_code IN (
        'PRICE_NON_POSITIVE',
        'RUB_PRICE_LE_1000',
        'DISTANCE_NON_POSITIVE',
        'WEIGHT_NEGATIVE',
        'VOLUME_NEGATIVE',
        'ORIGIN_CITY_NOT_FOUND',
        'DESTINATION_CITY_NOT_FOUND',
        'CURRENCY_UNSUPPORTED',
        'FX_RATE_MISSING'
    ));

CREATE INDEX IF NOT EXISTS quarantine_price_loading_date_idx
    ON audit.quarantine_price (loading_date);

COMMENT ON COLUMN audit.quarantine_price.rejected_at IS 'When this reason was first recorded for the current source version.';
COMMENT ON COLUMN audit.quarantine_price.reason_detail IS 'Human-readable value that failed the rule.';
COMMENT ON COLUMN audit.quarantine_price.source_record IS 'Complete source prices row. raw.prices.id is the same price_id.';

ALTER TABLE core.fact_deal ADD COLUMN IF NOT EXISTS fx_rate_date date;

ALTER TABLE core.fact_deal DROP CONSTRAINT IF EXISTS fact_deal_exchange_rate_fkey;
ALTER TABLE core.fact_deal DROP CONSTRAINT IF EXISTS fact_deal_fx_check;

ALTER TABLE core.fact_deal
    ADD CONSTRAINT fact_deal_exchange_rate_fkey
        FOREIGN KEY (currency_code, fx_rate_date)
        REFERENCES core.dim_exchange_rate (currency_code, rate_date),
    ADD CONSTRAINT fact_deal_fx_check CHECK (
        (currency_code = '643' AND fx_rate = 1 AND fx_rate_date IS NULL)
        OR (
            currency_code IN ('840', '978')
            AND fx_rate_date IS NOT NULL
            AND fx_rate_date = nearest_rate_date
        )
    );

COMMENT ON COLUMN core.fact_deal.nearest_rate_date IS
    'Source prices.nearest_rate_date, unchanged.';
COMMENT ON COLUMN core.fact_deal.fx_rate_date IS
    'Exchange-rate date used for conversion. Equals nearest_rate_date for USD and EUR, NULL for RUB.';

COMMIT;
