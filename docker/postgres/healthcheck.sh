#!/bin/bash
# Ready only after the source generator and the warehouse bootstrap have finished.
set -euo pipefail

pg_isready -q -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"

count="$(psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${DWH_DB_NAME}" -tAc \
  "SELECT source_price_count FROM audit.bootstrap WHERE id = 1")"

[[ "$count" =~ ^[1-9][0-9]*$ ]]
