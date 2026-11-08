-- Conformed dimensions: one city per gc_id, body groups, and exchange rates.
-- Repeatable. Seeds the body-group map. Does not read the source database.

BEGIN;

CREATE OR REPLACE FUNCTION core.body_group_for_type(body_type_id integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN body_type_id IS NULL THEN 'UNSPECIFIED'
        WHEN body_type_id BETWEEN 56 AND 61 THEN 'TENT'
        WHEN body_type_id IN (4, 46, 47, 48) THEN 'REF'
        WHEN body_type_id = 2 THEN 'ISO'
        WHEN body_type_id IN (1, 15) THEN 'BORT'
        ELSE 'OTHER'
    END
$$;

COMMENT ON FUNCTION core.body_group_for_type(integer) IS
    'Maps a source body type id to one business group. NULL becomes Not specified.';

CREATE TABLE IF NOT EXISTS core.dim_body_group (
    body_group_code  text PRIMARY KEY,
    body_group_name  text NOT NULL UNIQUE,
    std_id           integer,
    CONSTRAINT dim_body_group_code_check CHECK (body_group_code IN (
        'TENT', 'REF', 'ISO', 'BORT', 'OTHER', 'UNSPECIFIED'
    )),
    CONSTRAINT dim_body_group_std_check CHECK (
        (body_group_code = 'TENT' AND std_id = 56)
        OR (body_group_code = 'REF' AND std_id = 4)
        OR (body_group_code = 'ISO' AND std_id = 2)
        OR (body_group_code = 'BORT' AND std_id = 15)
        OR (body_group_code IN ('OTHER', 'UNSPECIFIED') AND std_id IS NULL)
    )
);

COMMENT ON TABLE core.dim_body_group IS
    'Tent 56, refrigerator 4, isotherm 2, flatbed 15, other, and not specified.';

INSERT INTO core.dim_body_group (body_group_code, body_group_name, std_id) VALUES
    ('TENT', 'Тент', 56),
    ('REF', 'Рефрижератор', 4),
    ('ISO', 'Изотерм', 2),
    ('BORT', 'Борт', 15),
    ('OTHER', 'Прочие', NULL),
    ('UNSPECIFIED', 'Не указан', NULL)
ON CONFLICT (body_group_code) DO UPDATE
SET body_group_name = EXCLUDED.body_group_name,
    std_id = EXCLUDED.std_id;

CREATE TABLE IF NOT EXISTS core.dim_body_type (
    body_type_id     integer PRIMARY KEY,
    name             text NOT NULL,
    id_from_1c       integer NOT NULL,
    source_std_id    integer,
    body_group_code  text NOT NULL REFERENCES core.dim_body_group (body_group_code),
    source_system    text NOT NULL,
    extracted_at     timestamptz,
    batch_id         bigint REFERENCES audit.etl_batch (batch_id),
    CONSTRAINT dim_body_type_group_check CHECK (
        body_group_code = core.body_group_for_type(body_type_id)
    )
);

COMMENT ON TABLE core.dim_body_type IS
    'One source body type id, including child ids, mapped to exactly one business group.';

INSERT INTO core.dim_body_type (
    body_type_id, name, id_from_1c, source_std_id, body_group_code, source_system
) VALUES
    (1,  'бортовая', 1, 15, 'BORT', 'deals_bigdata'),
    (2,  'изотермический', 2, 2, 'ISO', 'deals_bigdata'),
    (3,  'контейнер', 3, 3, 'OTHER', 'deals_bigdata'),
    (4,  'рефрижератор', 4, 4, 'REF', 'deals_bigdata'),
    (5,  'трал', 5, 5, 'OTHER', 'deals_bigdata'),
    (15, 'борт', 15, 15, 'BORT', 'deals_bigdata'),
    (26, 'контейнерная площадка', 26, 3, 'OTHER', 'deals_bigdata'),
    (46, 'рефрижератор мультирежимный', 46, 4, 'REF', 'deals_bigdata'),
    (47, 'рефрижератор с перегородкой', 47, 4, 'REF', 'deals_bigdata'),
    (48, 'рефрижератор-тушевоз', 48, 4, 'REF', 'deals_bigdata'),
    (50, 'самосвал', 50, 50, 'OTHER', 'deals_bigdata'),
    (56, 'тентованный', 56, 56, 'TENT', 'deals_bigdata'),
    (57, 'тент бок', 57, 56, 'TENT', 'deals_bigdata'),
    (58, 'тент верх', 58, 56, 'TENT', 'deals_bigdata'),
    (59, 'тент зад и бок', 59, 56, 'TENT', 'deals_bigdata'),
    (60, 'тент задняя загрузка', 60, 56, 'TENT', 'deals_bigdata'),
    (61, 'тент полная растентовка', 61, 56, 'TENT', 'deals_bigdata'),
    (65, 'фургон', 65, 65, 'OTHER', 'deals_bigdata'),
    (68, 'цистерна', 68, 68, 'OTHER', 'deals_bigdata')
ON CONFLICT (body_type_id) DO UPDATE
SET name = EXCLUDED.name,
    id_from_1c = EXCLUDED.id_from_1c,
    source_std_id = EXCLUDED.source_std_id,
    body_group_code = EXCLUDED.body_group_code,
    source_system = EXCLUDED.source_system;

CREATE TABLE IF NOT EXISTS core.dim_city (
    gc_id             integer PRIMARY KEY,
    source_city_id    bigint NOT NULL UNIQUE,
    name              text NOT NULL,
    full_name_local   text NOT NULL,
    region_name       text,
    latitude          double precision,
    longitude         double precision,
    duplicate_count   integer NOT NULL,
    source_system     text NOT NULL,
    extracted_at      timestamptz NOT NULL,
    batch_id          bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    CONSTRAINT dim_city_duplicate_count_check CHECK (duplicate_count >= 1)
);

COMMENT ON TABLE core.dim_city IS
    'One row per gc_id. source_city_id is the smallest source cities.id for that code.';
COMMENT ON COLUMN core.dim_city.region_name IS
    'Text after the first comma of the winning full_name_local. NULL when there is no comma.';

CREATE INDEX IF NOT EXISTS dim_city_batch_idx
    ON core.dim_city (batch_id);

CREATE TABLE IF NOT EXISTS core.dim_exchange_rate (
    currency_code   char(3) NOT NULL,
    rate_date       date NOT NULL,
    rate_value      numeric(18, 6) NOT NULL,
    source_rate_id  bigint NOT NULL UNIQUE,
    source_system   text NOT NULL,
    extracted_at    timestamptz NOT NULL,
    batch_id        bigint NOT NULL REFERENCES audit.etl_batch (batch_id),
    PRIMARY KEY (currency_code, rate_date),
    CONSTRAINT dim_exchange_rate_currency_check CHECK (currency_code IN ('643', '840', '978')),
    CONSTRAINT dim_exchange_rate_value_check CHECK (rate_value > 0)
);

COMMENT ON TABLE core.dim_exchange_rate IS
    'One rate per currency and setting date. Duplicate source rows keep the greatest source id.';

CREATE INDEX IF NOT EXISTS dim_exchange_rate_batch_idx
    ON core.dim_exchange_rate (batch_id);

COMMIT;
