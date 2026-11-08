"""Apply sql/core, then sql/mart migrations, in file-name order. Each file manages its own transaction."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import psycopg

from etl.config import load_config
from etl.log import configure_logging, log_event

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
MIGRATION_DIRS = (SQL_DIR / "core", SQL_DIR / "mart", SQL_DIR / "ml")
logger = logging.getLogger("etl.migrate")


def apply_migrations(directories: tuple[Path, ...] = MIGRATION_DIRS) -> list[str]:
    settings = load_config().database
    target = settings.warehouse
    files = [path for directory in directories for path in sorted(directory.glob("[0-9]*.sql"))]
    if not files:
        raise FileNotFoundError(f"no migrations in {', '.join(map(str, directories))}")
    with psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=target.name,
        user=target.user,
        password=target.password,
        connect_timeout=settings.connect_timeout,
        application_name="logistics_etl_migrate",
        autocommit=True,
    ) as conn:
        for path in files:
            conn.execute(path.read_text(encoding="utf-8"))
            log_event(logger, "migration_applied", file=f"{path.parent.name}/{path.name}")
    return [f"{path.parent.name}/{path.name}" for path in files]


def main() -> int:
    configure_logging()
    try:
        apply_migrations()
    except Exception as exc:
        log_event(logger, "migration_failed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
