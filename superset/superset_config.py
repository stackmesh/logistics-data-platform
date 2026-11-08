"""Superset settings for the Smart Logistics deployment. Every secret comes from the environment."""

import os
from urllib.parse import quote

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_METADATA_URI") or (
    "postgresql+psycopg2://"
    f"{quote(os.environ['SUPERSET_DB_USER'], safe='')}:{quote(os.environ['SUPERSET_DB_PASSWORD'], safe='')}"
    f"@{os.environ.get('SUPERSET_DB_HOST', 'superset-postgres')}:{os.environ.get('SUPERSET_DB_PORT', '5432')}"
    f"/{os.environ['SUPERSET_DB_NAME']}"
)

# Dashboards read a warehouse that changes once per batch; cache chart results for an hour.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 3600, "CACHE_KEY_PREFIX": "superset_"}
DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_data_"}

ROW_LIMIT = 50_000
SQL_MAX_ROW = 250_000
SUPERSET_WEBSERVER_TIMEOUT = 120

FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": False,
    "ENABLE_TEMPLATE_PROCESSING": False,
}

# The only database is the read-only warehouse connection; no file uploads into it.
PREVENT_UNSAFE_DB_CONNECTIONS = True
CSV_EXTENSIONS: set = set()
EXCEL_EXTENSIONS: set = set()
