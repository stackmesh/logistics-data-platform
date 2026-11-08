-- ETL batch history and data-quality statistics.
-- Repeatable. Does not load deals.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.etl_batch (
    batch_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_system     text NOT NULL,
    mode              text NOT NULL,
    status            text NOT NULL,
    start_date        date,
    end_date          date,
    as_of_date        date,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    rows_read         bigint,
    rows_landed       bigint,
    rows_quarantined  bigint,
    rows_core         bigint,
    rows_mart         bigint,
    rows_scored       bigint,
    error_message     text,
    CONSTRAINT etl_batch_mode_check CHECK (mode IN ('full', 'window')),
    CONSTRAINT etl_batch_status_check CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT etl_batch_window_check CHECK (
        (mode = 'full' AND start_date IS NULL AND end_date IS NULL)
        OR (mode = 'window' AND start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= end_date)
    ),
    CONSTRAINT etl_batch_row_counts_check CHECK (
        (rows_read IS NULL OR rows_read >= 0)
        AND (rows_landed IS NULL OR rows_landed >= 0)
        AND (rows_quarantined IS NULL OR rows_quarantined >= 0)
        AND (rows_core IS NULL OR rows_core >= 0)
        AND (rows_mart IS NULL OR rows_mart >= 0)
        AND (rows_scored IS NULL OR rows_scored >= 0)
    )
);

CREATE INDEX IF NOT EXISTS etl_batch_started_at_idx
    ON audit.etl_batch (started_at);

COMMENT ON TABLE audit.etl_batch IS
    'One pipeline execution. Source extracts, quarantine, and core rows point here.';

CREATE TABLE IF NOT EXISTS audit.dq_statistic (
    batch_id       bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    reason_code    text NOT NULL,
    rows_rejected  bigint NOT NULL,
    logged_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, reason_code),
    CONSTRAINT dq_statistic_reason_check CHECK (reason_code IN (
        'PRICE_NON_POSITIVE',
        'RUB_PRICE_LE_1000',
        'DISTANCE_NON_POSITIVE',
        'WEIGHT_NEGATIVE',
        'VOLUME_NEGATIVE',
        'CITY_NOT_FOUND',
        'FX_RATE_MISSING',
        'CURRENCY_UNSUPPORTED'
    )),
    CONSTRAINT dq_statistic_rows_check CHECK (rows_rejected >= 0)
);

COMMENT ON TABLE audit.dq_statistic IS
    'Reject counts by reason for one batch, including zeros.';

CREATE TABLE IF NOT EXISTS audit.quarantine_price (
    price_id              bigint NOT NULL,
    reason_code           text NOT NULL,
    batch_id              bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    detected_at           timestamptz NOT NULL DEFAULT now(),
    source_system         text NOT NULL,
    created_at            timestamp(0),
    loading_date          date,
    loading_city_gcid     integer,
    unloading_city_gcid   integer,
    distance              double precision,
    total_weight          double precision,
    total_volume          double precision,
    price                 double precision,
    currency_code         varchar(3),
    nearest_rate_date     date,
    price_type            smallint,
    is_fact               boolean,
    PRIMARY KEY (price_id, reason_code),
    CONSTRAINT quarantine_price_reason_check CHECK (reason_code IN (
        'PRICE_NON_POSITIVE',
        'RUB_PRICE_LE_1000',
        'DISTANCE_NON_POSITIVE',
        'WEIGHT_NEGATIVE',
        'VOLUME_NEGATIVE',
        'CITY_NOT_FOUND',
        'FX_RATE_MISSING',
        'CURRENCY_UNSUPPORTED'
    ))
);

CREATE INDEX IF NOT EXISTS quarantine_price_reason_idx
    ON audit.quarantine_price (reason_code);

CREATE INDEX IF NOT EXISTS quarantine_price_batch_idx
    ON audit.quarantine_price (batch_id);

COMMENT ON TABLE audit.quarantine_price IS
    'Current rejected source deals, one row per price_id and reason. Not a core fact.';
COMMENT ON COLUMN audit.quarantine_price.price_id IS
    'Source prices.id. There is no foreign key to core.fact_deal.';

COMMIT;
