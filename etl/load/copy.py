"""Bulk COPY of Polars frames over the SQLAlchemy connection's psycopg session."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl
from sqlalchemy.engine import Connection

NULL_TOKEN = r"\N"
CHUNK_ROWS = 100_000


def copy_frame(conn: Connection, table: str, columns: Sequence[str], frame: pl.DataFrame) -> int:
    """COPY frame into table inside the caller's transaction. Returns rows written.

    Polars renders CSV natively in chunks, so Python never holds per-row objects and at most
    one chunk of text is buffered. Floats are written in shortest round-trip form.
    """
    driver = conn.connection.driver_connection
    column_list = ", ".join(columns)
    selected = frame.select(list(columns))
    sql = f"COPY {table} ({column_list}) FROM STDIN WITH (FORMAT csv, NULL '{NULL_TOKEN}')"
    with driver.cursor() as cursor:
        with cursor.copy(sql) as copy:
            for offset in range(0, selected.height, CHUNK_ROWS):
                chunk = selected.slice(offset, CHUNK_ROWS)
                copy.write(chunk.write_csv(include_header=False, null_value=NULL_TOKEN).encode("utf-8"))
    return frame.height
