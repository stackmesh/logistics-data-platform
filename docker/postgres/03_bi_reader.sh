#!/bin/bash
# Creates the read-only BI login used by Superset. Repeatable.
# Runs as an init script on a fresh volume; on an existing volume:
#   docker compose exec postgres bash /docker-entrypoint-initdb.d/03_bi_reader.sh
# Object grants on schema mart come from sql/ml/003_bi_views.sql (python -m etl.migrate).
set -euo pipefail

for name in POSTGRES_USER DWH_DB_NAME BI_DB_PASSWORD; do
  if [[ -z "${!name:-}" ]]; then
    echo "[bi_reader] missing environment variable ${name}" >&2
    exit 1
  fi
done

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$DWH_DB_NAME" \
  -v dwh_db="$DWH_DB_NAME" \
  -v bi_password="$BI_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE bi_reader LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE', :'bi_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader')
\gexec
ALTER ROLE bi_reader PASSWORD :'bi_password';
ALTER ROLE bi_reader SET default_transaction_read_only = on;
ALTER ROLE bi_reader SET statement_timeout = '120s';
GRANT CONNECT ON DATABASE :"dwh_db" TO bi_reader;
SQL

echo "[bi_reader] role bi_reader can connect to ${DWH_DB_NAME}; run python -m etl.migrate to grant schema mart"
