"""Database settings loaded only from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote_plus


class SettingsError(RuntimeError):
    """Raised when a required connection setting is missing or invalid."""


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SettingsError(f"missing environment variable {name}")
    return value


@dataclass(frozen=True)
class DatabaseTarget:
    name: str
    user: str
    password: str

    def url(self, host: str, port: int) -> str:
        return (
            "postgresql+psycopg://"
            f"{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{host}:{port}/{quote_plus(self.name)}"
        )


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    connect_timeout: int
    n_prices: int
    source: DatabaseTarget
    warehouse: DatabaseTarget

    def engine_kwargs(self) -> dict:
        return {
            "pool_pre_ping": True,
            "connect_args": {
                "connect_timeout": self.connect_timeout,
                "application_name": "logistics_stage1_smoke",
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 3,
            },
        }


def load_settings() -> Settings:
    port_text = _required("POSTGRES_PORT")
    timeout_text = os.environ.get("PGCONNECT_TIMEOUT", "10").strip() or "10"
    n_prices_text = _required("N_PRICES")
    try:
        port = int(port_text)
        connect_timeout = int(timeout_text)
        n_prices = int(n_prices_text)
    except ValueError as exc:
        raise SettingsError("POSTGRES_PORT, PGCONNECT_TIMEOUT, and N_PRICES must be integers") from exc
    if port <= 0 or connect_timeout <= 0 or n_prices <= 0:
        raise SettingsError("POSTGRES_PORT, PGCONNECT_TIMEOUT, and N_PRICES must be positive")

    return Settings(
        host=_required("POSTGRES_HOST"),
        port=port,
        connect_timeout=connect_timeout,
        n_prices=n_prices,
        source=DatabaseTarget(
            name=_required("SOURCE_DB_NAME"),
            user=_required("SOURCE_DB_USER"),
            password=_required("SOURCE_DB_PASSWORD"),
        ),
        warehouse=DatabaseTarget(
            name=_required("DWH_DB_NAME"),
            user=_required("DWH_DB_USER"),
            password=_required("DWH_DB_PASSWORD"),
        ),
    )
