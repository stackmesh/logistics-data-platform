-- Source snapshots. gc_id duplicates stay here.
-- Repeatable. Does not copy rows from deals_bigdata.

BEGIN;

CREATE TABLE IF NOT EXISTS raw.cities (
    id               bigint PRIMARY KEY,
    full_name_local  varchar(255) NOT NULL,
    full_name_gc     varchar(255) NOT NULL,
    gc_id            integer NOT NULL,
    name             varchar(255) NOT NULL,
    longitude        double precision,
    latitude         double precision,
    created_at       timestamp(0),
    updated_at       timestamp(0),
    source_system    text NOT NULL,
    extracted_at     timestamptz NOT NULL,
    batch_id         bigint NOT NULL REFERENCES audit.etl_batch (batch_id)
);

CREATE INDEX IF NOT EXISTS raw_cities_gc_id_id_idx
    ON raw.cities (gc_id, id);

COMMENT ON TABLE raw.cities IS
    'One source cities row. gc_id is not unique here; core.dim_city keeps the minimum id.';

CREATE TABLE IF NOT EXISTS raw.body_types (
    id             bigint PRIMARY KEY,
    name           varchar(255) NOT NULL,
    id_from_1c     integer NOT NULL,
    std_id         integer,
    source_system  text NOT NULL,
    extracted_at   timestamptz NOT NULL,
    batch_id       bigint NOT NULL REFERENCES audit.etl_batch (batch_id)
);

COMMENT ON TABLE raw.body_types IS
    'One source body type. Business grouping is applied in core, not here.';

CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    id             bigint PRIMARY KEY,
    num_code       varchar(255) NOT NULL,
    value          double precision NOT NULL,
    setting_date   date NOT NULL,
    source_system  text NOT NULL,
    extracted_at   timestamptz NOT NULL,
    batch_id       bigint NOT NULL REFERENCES audit.etl_batch (batch_id)
);

CREATE INDEX IF NOT EXISTS raw_exchange_rates_lookup_idx
    ON raw.exchange_rates (num_code, setting_date, id DESC);

COMMENT ON TABLE raw.exchange_rates IS
    'Source rate rows. The source has no unique key on currency and date.';

CREATE TABLE IF NOT EXISTS raw.prices (
    id                   bigint PRIMARY KEY,
    created_at           timestamp(0),
    updated_at           timestamp(0),
    subscriber_id        integer,
    organization_id      integer,
    loading_date         date NOT NULL,
    loading_city_gcid    integer NOT NULL,
    unloading_city_gcid  integer NOT NULL,
    distance             double precision NOT NULL,
    total_weight         double precision NOT NULL,
    total_volume         double precision NOT NULL,
    body_type            integer,
    price                double precision NOT NULL,
    currency_code        varchar(3) NOT NULL,
    complex              boolean NOT NULL,
    price_type           smallint NOT NULL,
    item_id              integer,
    is_fact              boolean NOT NULL,
    nearest_rate_date    date,
    meta_item_id         bigint,
    end_loading_date     date,
    source               varchar(255),
    external_item_uuid   uuid,
    outlier_mark         boolean NOT NULL,
    weight_category      varchar(4),
    volume_category      varchar(4),
    price_group_id       bigint,
    source_system        text NOT NULL,
    extracted_at         timestamptz NOT NULL,
    batch_id             bigint NOT NULL REFERENCES audit.etl_batch (batch_id)
);

CREATE INDEX IF NOT EXISTS raw_prices_created_at_idx
    ON raw.prices (created_at);

CREATE INDEX IF NOT EXISTS raw_prices_updated_at_idx
    ON raw.prices (updated_at);

CREATE INDEX IF NOT EXISTS raw_prices_loading_date_idx
    ON raw.prices (loading_date);

CREATE INDEX IF NOT EXISTS raw_prices_batch_idx
    ON raw.prices (batch_id);

COMMENT ON TABLE raw.prices IS
    'One source deal, including rows that later fail quality checks.';
COMMENT ON COLUMN raw.prices.id IS 'Source prices.id.';
COMMENT ON COLUMN raw.prices.price_group_id IS
    'Source key only. No foreign key, and core does not carry this column.';

CREATE TABLE IF NOT EXISTS raw.price_body_types (
    id             bigint PRIMARY KEY,
    price_id       bigint NOT NULL REFERENCES raw.prices (id) ON DELETE CASCADE,
    body_type_id   integer NOT NULL,
    source_system  text NOT NULL,
    extracted_at   timestamptz NOT NULL,
    batch_id       bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    UNIQUE (price_id, body_type_id)
);

CREATE INDEX IF NOT EXISTS raw_price_body_types_price_idx
    ON raw.price_body_types (price_id);

CREATE TABLE IF NOT EXISTS raw.price_dates (
    id             bigint PRIMARY KEY,
    price_id       bigint NOT NULL REFERENCES raw.prices (id) ON DELETE CASCADE,
    price_date     date NOT NULL,
    source_system  text NOT NULL,
    extracted_at   timestamptz NOT NULL,
    batch_id       bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    UNIQUE (price_id, price_date)
);

CREATE INDEX IF NOT EXISTS raw_price_dates_price_date_idx
    ON raw.price_dates (price_date);

-- The source aggregate price_group_stats is not extracted: the mart recomputes corridors from deals.
DROP TABLE IF EXISTS raw.price_group_stats;

COMMIT;
