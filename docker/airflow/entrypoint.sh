#!/bin/bash
# Compose injects the environment. Build the metadata DB URL from AIRFLOW_DB_*, drop the warehouse
# superuser password, then hand over to the official Airflow entrypoint.
set -euo pipefail

unset POSTGRES_PASSWORD

if [[ -z "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-}" ]]; then
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(python3 -c '
import os
from urllib.parse import quote
e = os.environ
print("postgresql+psycopg2://{}:{}@{}:{}/{}".format(
    quote(e["AIRFLOW_DB_USER"], safe=""), quote(e["AIRFLOW_DB_PASSWORD"], safe=""),
    e.get("AIRFLOW_DB_HOST", "airflow-postgres"), e.get("AIRFLOW_DB_PORT", "5432"), e["AIRFLOW_DB_NAME"]))
')"
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
fi

exec /entrypoint "$@"
