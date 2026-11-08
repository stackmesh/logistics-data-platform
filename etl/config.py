"""ETL configuration from the environment, and the extraction window."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from logistics.settings import DatabaseTarget, Settings, SettingsError, load_settings

SOURCE_SYSTEM = "deals_bigdata"
DEFAULT_AS_OF_DATE = date(2026, 10, 26)
DEFAULT_FETCH_SIZE = 50_000


@dataclass(frozen=True)
class DateWindow:
    """Inclusive prices.loading_date window. Both bounds None means a full load."""

    start_date: date | None = None
    end_date: date | None = None

    def __post_init__(self) -> None:
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("start_date and end_date must be given together")
        if self.start_date is not None and self.start_date > self.end_date:
            raise ValueError(f"start_date {self.start_date} is after end_date {self.end_date}")

    @property
    def is_full(self) -> bool:
        return self.start_date is None

    @property
    def mode(self) -> str:
        return "full" if self.is_full else "window"

    def sql_params(self) -> dict:
        return {"full": self.is_full, "start_date": self.start_date, "end_date": self.end_date}

    def describe(self) -> str:
        if self.is_full:
            return "full history"
        return f"loading_date {self.start_date.isoformat()}..{self.end_date.isoformat()}"


@dataclass(frozen=True)
class EtlConfig:
    database: Settings
    as_of_date: date
    fetch_size: int
    source_system: str = SOURCE_SYSTEM


def load_config() -> EtlConfig:
    as_of_text = os.environ.get("AS_OF_DATE", "").strip()
    fetch_text = os.environ.get("ETL_FETCH_SIZE", "").strip()
    try:
        as_of_date = date.fromisoformat(as_of_text) if as_of_text else DEFAULT_AS_OF_DATE
    except ValueError as exc:
        raise SettingsError("AS_OF_DATE must be YYYY-MM-DD") from exc
    try:
        fetch_size = int(fetch_text) if fetch_text else DEFAULT_FETCH_SIZE
    except ValueError as exc:
        raise SettingsError("ETL_FETCH_SIZE must be an integer") from exc
    if fetch_size <= 0:
        raise SettingsError("ETL_FETCH_SIZE must be positive")
    return EtlConfig(database=load_settings(), as_of_date=as_of_date, fetch_size=fetch_size)


def _engine(config: EtlConfig, target: DatabaseTarget, application_name: str) -> Engine:
    kwargs = config.database.engine_kwargs()
    kwargs["connect_args"] = {**kwargs["connect_args"], "application_name": application_name}
    return create_engine(
        target.url(config.database.host, config.database.port),
        pool_size=2,
        max_overflow=0,
        **kwargs,
    )


def source_engine(config: EtlConfig) -> Engine:
    return _engine(config, config.database.source, "logistics_etl_extract")


def warehouse_engine(config: EtlConfig) -> Engine:
    return _engine(config, config.database.warehouse, "logistics_etl_load")
