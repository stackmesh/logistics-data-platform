#!/bin/bash
# Apply sql/core migrations in order. Each file has its own transaction.
# Connection settings come from the environment (PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD).
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"

for file in "$root"/[0-9]*.sql; do
  printf '[migrate %s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(basename "$file")"
  psql -v ON_ERROR_STOP=1 -f "$file"
done

printf '[migrate %s] core migrations applied\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
