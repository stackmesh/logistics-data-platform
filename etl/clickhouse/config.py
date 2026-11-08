"""ClickHouse and PostgreSQL connections for the ClickHouse sync. Settings come only from the environment."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

import psycopg

from etl.config import EtlConfig
from logistics.settings import SettingsError, _required

DEFAULT_INSERT_BATCH_ROWS = 50_000
DEFAULT_WATERMARK_OVERLAP_S = 3_600
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool
    insert_batch_rows: int
    watermark_overlap_s: int
    connect_timeout: int = 10
    send_receive_timeout: int = 600


def _positive_int(name: str, default: int) -> int:
    text = os.environ.get(name, "").strip()
    try:
        value = int(text) if text else default
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer") from exc
    if value <= 0:
        raise SettingsError(f"{name} must be positive")
    return value


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise SettingsError(f"invalid ClickHouse identifier {name!r}")
    return name


def load_clickhouse_settings(database: str | None = None) -> ClickHouseSettings:
    return ClickHouseSettings(
        host=_required("CLICKHOUSE_HOST"),
        port=_positive_int("CLICKHOUSE_PORT", 8123),
        user=_required("CLICKHOUSE_USER"),
        password=_required("CLICKHOUSE_PASSWORD"),
        database=validate_identifier(database or _required("CLICKHOUSE_DB")),
        secure=os.environ.get("CLICKHOUSE_SECURE", "").strip().lower() in {"1", "true", "yes"},
        insert_batch_rows=_positive_int("CLICKHOUSE_INSERT_BATCH_ROWS", DEFAULT_INSERT_BATCH_ROWS),
        watermark_overlap_s=_positive_int("CLICKHOUSE_WATERMARK_OVERLAP_S", DEFAULT_WATERMARK_OVERLAP_S),
    )


def clickhouse_client(settings: ClickHouseSettings, database: str | None = "default"):
    """HTTP client. The target database is always qualified in SQL, so the session database is 'default'."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=database,
        secure=settings.secure,
        connect_timeout=settings.connect_timeout,
        send_receive_timeout=settings.send_receive_timeout,
        compress="lz4",
        settings={"join_use_nulls": 1, "mutations_sync": 2},
    )


def postgres_connection(config: EtlConfig, application_name: str, **kwargs) -> psycopg.Connection:
    settings = config.database
    target = settings.warehouse
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=target.name,
        user=target.user,
        password=target.password,
        connect_timeout=settings.connect_timeout,
        application_name=application_name,
        **kwargs,
    )
