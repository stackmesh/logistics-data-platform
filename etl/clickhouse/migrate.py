"""Apply clickhouse/sql/[0-9]*.sql in file-name order. Every statement is idempotent."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

from etl.clickhouse.config import clickhouse_client, load_clickhouse_settings, validate_identifier
from etl.clickhouse.schema import CLICKHOUSE_SQL_DIR
from etl.log import configure_logging, log_event

logger = logging.getLogger("etl.clickhouse.migrate")
_LINE_COMMENT = re.compile(r"--[^\n]*")


def split_statements(sql: str) -> list[str]:
    """Statements separated by ';'. The DDL files contain no string literals with ';' or '--'."""
    text = _LINE_COMMENT.sub("", sql)
    return [statement.strip() for statement in text.split(";") if statement.strip()]


def render(sql: str, database: str) -> str:
    return sql.replace("{database}", validate_identifier(database))


def apply_schema(client, database: str, directory: Path = CLICKHOUSE_SQL_DIR) -> list[str]:
    files = sorted(directory.glob("[0-9]*.sql"))
    if not files:
        raise FileNotFoundError(f"no ClickHouse migrations in {directory}")
    for path in files:
        for statement in split_statements(render(path.read_text(encoding="utf-8"), database)):
            client.command(statement)
        log_event(logger, "clickhouse_migration_applied", file=path.name, database=database)
    return [path.name for path in files]


def main() -> int:
    configure_logging()
    try:
        settings = load_clickhouse_settings()
        apply_schema(clickhouse_client(settings), settings.database)
    except Exception as exc:
        log_event(logger, "clickhouse_migration_failed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
