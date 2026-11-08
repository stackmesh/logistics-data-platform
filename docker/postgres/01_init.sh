#!/bin/bash
# Initializes deals_bigdata from the supplied reference SQL, then logistics_dwh.
# schema.sql and synthetic.sql are mounted read-only and are not modified here.
set -euo pipefail

SOURCE_SQL_DIR="${SOURCE_SQL_DIR:-/source-sql}"
WAREHOUSE_SQL="${WAREHOUSE_SQL:-/docker/init/02_warehouse.sql}"

log() {
  printf '[init %s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_ident() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    log "invalid identifier for ${name}: ${value}"
    exit 1
  fi
}

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    log "missing environment variable ${name}"
    exit 1
  fi
}

for name in \
  POSTGRES_USER POSTGRES_DB \
  SOURCE_DB_NAME SOURCE_DB_USER SOURCE_DB_PASSWORD \
  DWH_DB_NAME DWH_DB_USER DWH_DB_PASSWORD \
  N_PRICES
do
  require_var "$name"
done

require_ident POSTGRES_USER "$POSTGRES_USER"
require_ident POSTGRES_DB "$POSTGRES_DB"
require_ident SOURCE_DB_NAME "$SOURCE_DB_NAME"
require_ident SOURCE_DB_USER "$SOURCE_DB_USER"
require_ident DWH_DB_NAME "$DWH_DB_NAME"
require_ident DWH_DB_USER "$DWH_DB_USER"

if [[ "$SOURCE_DB_NAME" != "$POSTGRES_DB" ]]; then
  log "SOURCE_DB_NAME (${SOURCE_DB_NAME}) must match POSTGRES_DB (${POSTGRES_DB})"
  exit 1
fi

if [[ ! "$N_PRICES" =~ ^[1-9][0-9]*$ ]]; then
  log "N_PRICES must be a positive integer, got ${N_PRICES}"
  exit 1
fi

if [[ ! -f "${SOURCE_SQL_DIR}/schema.sql" || ! -f "${SOURCE_SQL_DIR}/synthetic.sql" ]]; then
  log "reference SQL is missing under ${SOURCE_SQL_DIR}"
  exit 1
fi

if ! grep -q 'setseed(0.20261026)' "${SOURCE_SQL_DIR}/synthetic.sql"; then
  log "synthetic.sql no longer contains setseed(0.20261026); refusing to generate"
  exit 1
fi

log "PostgreSQL $(postgres -V)"
log "source database ${SOURCE_DB_NAME}; warehouse database ${DWH_DB_NAME}; n_prices=${N_PRICES}"
log "generator seed stays inside synthetic.sql (setseed(0.20261026))"

log "creating login roles ${SOURCE_DB_USER} and ${DWH_DB_USER}"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v source_user="$SOURCE_DB_USER" \
  -v source_password="$SOURCE_DB_PASSWORD" \
  -v dwh_user="$DWH_DB_USER" \
  -v dwh_password="$DWH_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE', :'source_user', :'source_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'source_user')
\gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE', :'dwh_user', :'dwh_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'dwh_user')
\gexec
SQL

log "applying source schema ${SOURCE_SQL_DIR}/schema.sql"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$SOURCE_DB_NAME" \
  -f "${SOURCE_SQL_DIR}/schema.sql"

if [[ "$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SOURCE_DB_NAME" -tAc "SELECT to_regclass('public.prices')")" != "prices" ]]; then
  log "public.prices is missing after schema.sql; synthetic.sql will not run"
  exit 1
fi

log "applying source generator ${SOURCE_SQL_DIR}/synthetic.sql"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$SOURCE_DB_NAME" \
  -v n_prices="$N_PRICES" \
  -f "${SOURCE_SQL_DIR}/synthetic.sql"

price_count="$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SOURCE_DB_NAME" -tAc "SELECT COUNT(*) FROM prices")"
log "source prices=${price_count}"
if [[ "$price_count" != "$N_PRICES" ]]; then
  log "expected ${N_PRICES} prices, found ${price_count}"
  exit 1
fi

log "granting read-only access on ${SOURCE_DB_NAME} to ${SOURCE_DB_USER}"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$SOURCE_DB_NAME" \
  -v source_db="$SOURCE_DB_NAME" \
  -v source_user="$SOURCE_DB_USER" <<'SQL'
REVOKE ALL ON DATABASE :"source_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"source_db" TO :"source_user";
GRANT USAGE ON SCHEMA public TO :"source_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"source_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :"source_user";
SQL

log "creating warehouse database ${DWH_DB_NAME}"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$SOURCE_DB_NAME" \
  -v dwh_db="$DWH_DB_NAME" <<'SQL'
SELECT format('CREATE DATABASE %I', :'dwh_db')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'dwh_db')
\gexec
SQL

log "creating warehouse schemas raw, core, mart, ml, audit"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$DWH_DB_NAME" \
  -v dwh_db="$DWH_DB_NAME" \
  -v dwh_user="$DWH_DB_USER" \
  -v source_db="$SOURCE_DB_NAME" \
  -v source_price_count="$price_count" \
  -v n_prices="$N_PRICES" \
  -f "$WAREHOUSE_SQL"

log "source table counts"
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$SOURCE_DB_NAME" \
  -c "SELECT 'cities' AS table_name, COUNT(*) AS rows FROM cities
      UNION ALL SELECT 'body_types', COUNT(*) FROM body_types
      UNION ALL SELECT 'exchange_rates', COUNT(*) FROM exchange_rates
      UNION ALL SELECT 'prices', COUNT(*) FROM prices
      UNION ALL SELECT 'price_body_types', COUNT(*) FROM price_body_types
      UNION ALL SELECT 'price_dates', COUNT(*) FROM price_dates
      UNION ALL SELECT 'price_group_stats', COUNT(*) FROM price_group_stats
      UNION ALL SELECT 'price_participants', COUNT(*) FROM price_participants
      UNION ALL SELECT 'selected_directions', COUNT(*) FROM selected_directions
      UNION ALL SELECT 'favorite_directions', COUNT(*) FROM favorite_directions
      ORDER BY table_name"

log "initialization complete; prices=${price_count}; warehouse schemas are empty except audit.bootstrap"
