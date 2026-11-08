-- Stage 1 warehouse bootstrap.
-- Creates empty analytical schemas and one readiness row.
-- Does not create ETL tables or ML objects.

GRANT CONNECT, CREATE ON DATABASE :"dwh_db" TO :"dwh_user";
REVOKE ALL ON DATABASE :"dwh_db" FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION :"dwh_user";
CREATE SCHEMA IF NOT EXISTS core AUTHORIZATION :"dwh_user";
CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION :"dwh_user";
CREATE SCHEMA IF NOT EXISTS ml AUTHORIZATION :"dwh_user";
CREATE SCHEMA IF NOT EXISTS audit AUTHORIZATION :"dwh_user";

COMMENT ON SCHEMA raw IS 'Landed source extracts. Empty until ETL.';
COMMENT ON SCHEMA core IS 'Conformed warehouse facts. Empty until ETL.';
COMMENT ON SCHEMA mart IS 'Analytical marts. Empty until ETL.';
COMMENT ON SCHEMA ml IS 'Feature matrices and predictions. Empty until ML.';
COMMENT ON SCHEMA audit IS 'Initialization marker and later pipeline audit.';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

SET ROLE :"dwh_user";

CREATE TABLE IF NOT EXISTS audit.bootstrap (
    id                  smallint PRIMARY KEY CHECK (id = 1),
    initialized_at      timestamptz NOT NULL DEFAULT now(),
    source_database     text NOT NULL,
    source_price_count  bigint NOT NULL CHECK (source_price_count >= 0),
    n_prices_requested  bigint NOT NULL CHECK (n_prices_requested > 0),
    server_version      text NOT NULL
);

RESET ROLE;

INSERT INTO audit.bootstrap (
    id,
    source_database,
    source_price_count,
    n_prices_requested,
    server_version
) VALUES (
    1,
    :'source_db',
    :source_price_count,
    :n_prices,
    current_setting('server_version')
)
ON CONFLICT (id) DO UPDATE
SET source_database = EXCLUDED.source_database,
    source_price_count = EXCLUDED.source_price_count,
    n_prices_requested = EXCLUDED.n_prices_requested,
    server_version = EXCLUDED.server_version,
    initialized_at = now();
