"""Bulk COPY of Polars frames over the SQLAlchemy connection's psycopg session."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl
from sqlalchemy.engine import Connection


def copy_frame(conn: Connection, table: str, columns: Sequence[str], frame: pl.DataFrame) -> int:
    """COPY frame into table inside the caller's transaction. Returns rows written."""
    driver = conn.connection.driver_connection
    column_list = ", ".join(columns)
    with driver.cursor() as cursor:
        with cursor.copy(f"COPY {table} ({column_list}) FROM STDIN") as copy:
            for row in frame.select(list(columns)).iter_rows():
                copy.write_row(row)
    return frame.height
