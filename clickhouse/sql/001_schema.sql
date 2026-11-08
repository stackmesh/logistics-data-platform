-- ClickHouse analytical copy of the PostgreSQL warehouse. PostgreSQL stays authoritative; every table here
-- is rebuilt from it by etl.clickhouse.sync. {database} is substituted by etl.clickhouse.migrate. Repeatable.

CREATE DATABASE IF NOT EXISTS {database};

-- Dimensions: a few dozen rows, replaced as a whole when their fingerprint changes.
CREATE TABLE IF NOT EXISTS {database}.dim_city
(
    gc_id UInt32,
    name  String
)
ENGINE = MergeTree
ORDER BY gc_id;

CREATE TABLE IF NOT EXISTS {database}.dim_body_group
(
    body_group_code LowCardinality(String),
    body_group_name String
)
ENGINE = MergeTree
ORDER BY body_group_code;

-- Normalized transportation facts: one row per accepted deal of core.fact_deal.
-- MergeTree, not ReplacingMergeTree: a changed deal can move between months and routes, so a sorting-key
-- based merge could not collapse its old and new versions. Each monthly partition is instead replaced
-- atomically from a verified staging copy, which gives exactly one version of every deal without FINAL.
CREATE TABLE IF NOT EXISTS {database}.fact_deal
(
    price_id                UInt64,
    loading_date            Date,
    end_loading_date        Nullable(Date),
    loading_city_gcid       UInt32,
    unloading_city_gcid     UInt32,
    distance_km             Decimal(12, 3),
    total_weight_t          Decimal(12, 3),
    total_volume_m3         Decimal(12, 3),
    body_type_id            Nullable(Int32),
    body_group_code         LowCardinality(String),
    weight_category         LowCardinality(String),
    volume_category         LowCardinality(String),
    weight_category_imputed Bool,
    volume_category_imputed Bool,
    price_src               Decimal(18, 2),
    currency_code           LowCardinality(String),
    fx_rate                 Decimal(18, 6),
    nearest_rate_date       Nullable(Date),
    price_rub               Decimal(18, 2),
    price_type              Int16,
    is_fact                 Bool,
    complex                 Bool,
    outlier_mark            Bool,
    organization_id         Nullable(Int32),
    subscriber_id           Nullable(Int32),
    created_at              Nullable(DateTime('UTC')),
    updated_at              Nullable(DateTime('UTC')),
    source_name             Nullable(String),
    item_id                 Nullable(Int32),
    meta_item_id            Nullable(Int64),
    external_item_uuid      Nullable(UUID),
    batch_id                Int64,
    _sync_run_id            UUID,
    _loaded_at              DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loading_date)
ORDER BY (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, loading_date, price_id);

-- The market corridor's population: mart.market_corridor_observations(as_of) of PostgreSQL, one row per
-- eligible deal, price month, and body group. Eligibility stays in PostgreSQL; ClickHouse aggregates.
-- Partition replacement for the same reason as fact_deal.
CREATE TABLE IF NOT EXISTS {database}.fact_price_observation
(
    price_month         Date,
    price_date          Date,
    body_group_code     LowCardinality(String),
    price_id            UInt64,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    weight_category     LowCardinality(String),
    price_rub           Decimal(18, 2),
    distance_km         Decimal(12, 3),
    price_km            Float64,
    organization_id     Nullable(Int32),
    price_type          Int16,
    as_of_date          Date,
    _sync_run_id        UUID,
    _loaded_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(price_month)
ORDER BY (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, price_month, price_id);

-- Monthly route statistics as partial aggregate states, one row per mart cell. States merge exactly:
-- a query can roll cells up to any coarser grain (route across months, all body groups) and still get
-- exact distinct trips, distinct carriers, and exact percentiles, which sums of cell values cannot give.
-- SummingMergeTree could keep only the additive columns.
CREATE TABLE IF NOT EXISTS {database}.route_month_stats
(
    price_month         Date,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    body_group_code     LowCardinality(String),
    weight_category     LowCardinality(String),
    observations        SimpleAggregateFunction(sum, UInt64),
    price_rub_sum       SimpleAggregateFunction(sum, Decimal(38, 2)),
    price_km_sum        SimpleAggregateFunction(sum, Float64),
    max_price_date      SimpleAggregateFunction(max, Date),
    trips               AggregateFunction(uniqExact, UInt64),
    carriers            AggregateFunction(uniqExactIf, Nullable(Int32), UInt8),
    price_quantiles     AggregateFunction(quantilesExactInclusive(0.25, 0.5, 0.75), Float64),
    price_km_quantiles  AggregateFunction(quantilesExactInclusive(0.25, 0.5, 0.75), Float64),
    price_stddev        AggregateFunction(stddevSampStable, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(price_month)
ORDER BY (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, price_month);

-- Market pricing corridor computed in ClickHouse with the mart's definitions, from route_month_stats.
-- A derived snapshot: rebuilt in a staging table and swapped in with EXCHANGE TABLES, so plain MergeTree.
CREATE TABLE IF NOT EXISTS {database}.market_corridor
(
    price_month           Date,
    loading_city_gcid     UInt32,
    unloading_city_gcid   UInt32,
    body_group_code       LowCardinality(String),
    weight_category       LowCardinality(String),
    origin_city_name      String,
    destination_city_name String,
    body_group_name       String,
    p25_trip              Float64,
    median_trip           Float64,
    p75_trip              Float64,
    avg_trip              Float64,
    p25_km                Float64,
    median_km             Float64,
    p75_km                Float64,
    trips_count           UInt64,
    carriers_count        UInt64,
    cv_rate               Nullable(Float64),
    mom_pct               Nullable(Float64),
    yoy_pct               Nullable(Float64),
    max_price_date        Date,
    freshness_days        Int32,
    reliability_level     LowCardinality(String),
    as_of_date            Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(price_month)
ORDER BY (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, price_month);

-- ML predictions: one row per deal and model version, ml.ml_price_predictions of PostgreSQL.
-- ReplacingMergeTree(scored_at): the incremental load re-reads an overlap window after its watermark, and
-- a re-scored deal arrives as a new row with a later scored_at. The sorting key is the PostgreSQL primary
-- key, which never changes for a row, so merges keep exactly the latest version. Until a merge runs,
-- duplicates exist on disk: read with FINAL. Deleted predictions are removed by partition replacement
-- when the partition fingerprint differs from PostgreSQL.
CREATE TABLE IF NOT EXISTS {database}.ml_price_predictions
(
    model_name          LowCardinality(String),
    model_version       LowCardinality(String),
    price_id            UInt64,
    split_role          LowCardinality(String),
    as_of_date          Date,
    loading_date        Date,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    body_group          LowCardinality(String),
    actual_price_rub    Nullable(Decimal(18, 2)),
    predicted_price_rub Decimal(18, 2),
    abs_error_rub       Nullable(Decimal(18, 2)),
    ape_pct             Nullable(Decimal(12, 4)),
    scored_at           DateTime64(6, 'UTC'),
    scoring_batch_id    Int64,
    _sync_run_id        UUID,
    _loaded_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(scored_at)
PARTITION BY model_version
ORDER BY (model_name, model_version, price_id)
SETTINGS non_replicated_deduplication_window = 10000;

-- Additive prediction error components per model version, record type, day, and route. Sums of sums are
-- exact, so MAE, RMSE, MAPE, and bias at any coarser grain are ratios of sums. SummingMergeTree collapses
-- equal keys on merge; partitions are rebuilt atomically, never appended twice, so nothing is summed
-- twice. Medians are not additive and are read from ml_price_predictions.
CREATE TABLE IF NOT EXISTS {database}.ml_prediction_daily
(
    model_name          LowCardinality(String),
    model_version       LowCardinality(String),
    record_type         LowCardinality(String),
    loading_date        Date,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    body_group          LowCardinality(String),
    deals               UInt64,
    actual_deals        UInt64,
    actual_sum          Decimal(38, 2),
    predicted_sum       Decimal(38, 2),
    signed_error_sum    Decimal(38, 2),
    abs_error_sum       Decimal(38, 2),
    sq_error_sum        Decimal(38, 4),
    ape_sum             Decimal(38, 4)
)
ENGINE = SummingMergeTree
PARTITION BY model_version
ORDER BY (model_name, model_version, record_type, loading_date, loading_city_gcid, unloading_city_gcid, body_group);

-- Incremental-load state: the latest row per source wins.
CREATE TABLE IF NOT EXISTS {database}.sync_watermark
(
    source     LowCardinality(String),
    watermark  DateTime64(6, 'UTC'),
    run_id     UUID,
    updated_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY source;

-- One row per partition action and one summary row per run. Append-only.
CREATE TABLE IF NOT EXISTS {database}.sync_log
(
    run_id      UUID,
    started_at  DateTime64(3, 'UTC'),
    logged_at   DateTime64(3, 'UTC') DEFAULT now64(3),
    table_name  LowCardinality(String),
    partition   String,
    action      LowCardinality(String),
    pg_rows     Nullable(UInt64),
    ch_rows     Nullable(UInt64),
    pg_hash     Nullable(String),
    ch_hash     Nullable(String),
    duration_s  Float64,
    status      LowCardinality(String),
    detail      String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, run_id, table_name, partition);
