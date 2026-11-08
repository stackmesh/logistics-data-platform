-- Normalized deals. One row per source price_id.
-- A deal may have several body groups, and only one row per group.
-- Repeatable. price_group_id is not a column of these tables.

BEGIN;

CREATE TABLE IF NOT EXISTS core.fact_deal (
    price_id              bigint PRIMARY KEY,
    loading_date          date NOT NULL,
    end_loading_date      date,
    loading_city_gcid     integer NOT NULL REFERENCES core.dim_city (gc_id),
    unloading_city_gcid   integer NOT NULL REFERENCES core.dim_city (gc_id),
    distance_km           numeric(12, 3) NOT NULL,
    total_weight_t        numeric(12, 3) NOT NULL,
    total_volume_m3       numeric(12, 3) NOT NULL,
    body_type_id          integer REFERENCES core.dim_body_type (body_type_id),
    body_group_code       text NOT NULL REFERENCES core.dim_body_group (body_group_code),
    weight_category       varchar(4) NOT NULL,
    volume_category       varchar(4) NOT NULL,
    weight_category_imputed boolean NOT NULL,
    volume_category_imputed boolean NOT NULL,
    price_src             numeric(18, 2) NOT NULL,
    currency_code         char(3) NOT NULL,
    fx_rate               numeric(18, 6) NOT NULL,
    nearest_rate_date     date,
    price_rub             numeric(18, 2) NOT NULL,
    price_type            smallint NOT NULL,
    is_fact               boolean NOT NULL,
    complex               boolean NOT NULL,
    outlier_mark          boolean NOT NULL,
    organization_id       integer,
    subscriber_id         integer,
    created_at            timestamp(0),
    updated_at            timestamp(0),
    source_name           varchar(255),
    item_id               integer,
    meta_item_id          bigint,
    external_item_uuid    uuid,
    source_system         text NOT NULL,
    extracted_at          timestamptz NOT NULL,
    batch_id              bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    CONSTRAINT fact_deal_distance_check CHECK (distance_km > 0),
    CONSTRAINT fact_deal_weight_check CHECK (total_weight_t >= 0),
    CONSTRAINT fact_deal_volume_check CHECK (total_volume_m3 >= 0),
    CONSTRAINT fact_deal_price_src_check CHECK (price_src > 0),
    CONSTRAINT fact_deal_fx_rate_check CHECK (fx_rate > 0),
    CONSTRAINT fact_deal_price_rub_check CHECK (price_rub > 0),
    CONSTRAINT fact_deal_currency_check CHECK (currency_code IN ('643', '840', '978')),
    CONSTRAINT fact_deal_weight_category_check CHECK (weight_category IN (
        'G20T', 'FTL', '10T', '5T', 'L5T', 'OTH'
    )),
    CONSTRAINT fact_deal_volume_category_check CHECK (volume_category IN ('FTL', 'OTH')),
    CONSTRAINT fact_deal_price_type_check CHECK (price_type IN (1, 2, 3, 4, 5, 6, 9)),
    CONSTRAINT fact_deal_body_group_check CHECK (
        (body_type_id IS NULL AND body_group_code = 'UNSPECIFIED')
        OR (
            body_type_id IS NOT NULL
            AND body_group_code = core.body_group_for_type(body_type_id)
        )
    ),
    CONSTRAINT fact_deal_exchange_rate_fkey
        FOREIGN KEY (currency_code, nearest_rate_date)
        REFERENCES core.dim_exchange_rate (currency_code, rate_date)
);

CREATE INDEX IF NOT EXISTS fact_deal_loading_date_idx
    ON core.fact_deal (loading_date);

CREATE INDEX IF NOT EXISTS fact_deal_created_at_idx
    ON core.fact_deal (created_at);

CREATE INDEX IF NOT EXISTS fact_deal_updated_at_idx
    ON core.fact_deal (updated_at);

CREATE INDEX IF NOT EXISTS fact_deal_route_idx
    ON core.fact_deal (loading_city_gcid, unloading_city_gcid);

CREATE INDEX IF NOT EXISTS fact_deal_population_idx
    ON core.fact_deal (is_fact, complex, outlier_mark, loading_date);

CREATE INDEX IF NOT EXISTS fact_deal_batch_idx
    ON core.fact_deal (batch_id);

COMMENT ON TABLE core.fact_deal IS
    'One validated deal per source prices.id. Auctions, outliers, and future dates remain here.';
COMMENT ON COLUMN core.fact_deal.price_id IS 'Source prices.id.';
COMMENT ON COLUMN core.fact_deal.price_src IS 'Original trip price in currency_code, without VAT.';
COMMENT ON COLUMN core.fact_deal.price_rub IS 'price_src converted with fx_rate, in RUB without VAT.';
COMMENT ON COLUMN core.fact_deal.body_group_code IS
    'Group of prices.body_type, or UNSPECIFIED when that column is null.';
COMMENT ON COLUMN core.fact_deal.nearest_rate_date IS
    'Source rate date used for foreign currency. NULL for RUB identity conversion.';
COMMENT ON COLUMN core.fact_deal.source_system IS 'Originating database, deals_bigdata.';
COMMENT ON COLUMN core.fact_deal.extracted_at IS 'Warehouse extraction timestamp for this version of the row.';
COMMENT ON COLUMN core.fact_deal.batch_id IS 'audit.etl_batch that published this version.';

CREATE TABLE IF NOT EXISTS core.fact_deal_date (
    price_id     bigint NOT NULL REFERENCES core.fact_deal (price_id) ON DELETE CASCADE,
    price_date   date NOT NULL,
    price_month  date GENERATED ALWAYS AS ((date_trunc('month', price_date::timestamp))::date) STORED,
    PRIMARY KEY (price_id, price_date)
);

CREATE INDEX IF NOT EXISTS fact_deal_date_month_idx
    ON core.fact_deal_date (price_month);

CREATE INDEX IF NOT EXISTS fact_deal_date_month_price_idx
    ON core.fact_deal_date (price_month, price_id);

COMMENT ON TABLE core.fact_deal_date IS
    'One source price_dates row of a conformed deal. price_month is the first day of that month.';

CREATE TABLE IF NOT EXISTS core.bridge_deal_body_group (
    price_id          bigint NOT NULL REFERENCES core.fact_deal (price_id) ON DELETE CASCADE,
    body_group_code   text NOT NULL REFERENCES core.dim_body_group (body_group_code),
    resolution        text NOT NULL,
    PRIMARY KEY (price_id, body_group_code),
    CONSTRAINT bridge_deal_body_group_resolution_check CHECK (resolution IN (
        'links', 'fallback_body_type', 'unspecified'
    ))
);

CREATE INDEX IF NOT EXISTS bridge_deal_body_group_group_idx
    ON core.bridge_deal_body_group (body_group_code, price_id);

COMMENT ON TABLE core.bridge_deal_body_group IS
    'One business group per deal. Child types that share a group do not add another row.';

COMMIT;
