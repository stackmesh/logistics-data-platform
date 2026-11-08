#!/bin/bash
# Compose injects the environment. Drop the superuser password before the check runs.
set -euo pipefail

unset POSTGRES_PASSWORD

if [[ $# -eq 0 ]]; then
  set -- python scripts/check_databases.py
fi

exec "$@"
