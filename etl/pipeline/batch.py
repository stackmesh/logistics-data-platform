"""audit.etl_batch lifecycle."""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl.config import DateWindow, EtlConfig

MAX_ERROR_LENGTH = 4000


def start_batch(engine: Engine, config: EtlConfig, window: DateWindow) -> int:
    """Record a running batch in its own committed transaction so failures stay visible."""
    with engine.begin() as conn:
        return int(
            conn.execute(
                text(
                    """
                    INSERT INTO audit.etl_batch (source_system, mode, status, start_date, end_date, as_of_date)
                    VALUES (:source_system, :mode, 'running', :start_date, :end_date, :as_of_date)
                    RETURNING batch_id
                    """
                ),
                {
                    "source_system": config.source_system,
                    "mode": window.mode,
                    "start_date": window.start_date,
                    "end_date": window.end_date,
                    "as_of_date": config.as_of_date,
                },
            ).scalar_one()
        )


def complete_batch(conn: Connection, batch_id: int, summary: dict) -> None:
    """Mark success inside the load transaction, so status and data commit together."""
    fact = summary["load"]["core"]["fact_deal"]
    conn.execute(
        text(
            """
            UPDATE audit.etl_batch
            SET status = 'succeeded',
                completed_at = clock_timestamp(),
                extracted_count = :extracted_count,
                accepted_count = :accepted_count,
                rejected_count = :rejected_count,
                inserted_count = :inserted_count,
                updated_count = :updated_count,
                unchanged_count = :unchanged_count,
                deleted_count = :deleted_count,
                summary = CAST(:summary AS jsonb),
                error_message = NULL
            WHERE batch_id = :batch_id AND status = 'running'
            """
        ),
        {
            "batch_id": batch_id,
            "extracted_count": summary["extracted_count"],
            "accepted_count": summary["accepted_count"],
            "rejected_count": summary["rejected_count"],
            "inserted_count": fact["inserted"],
            "updated_count": fact["updated"],
            "unchanged_count": fact["unchanged"],
            "deleted_count": fact["deleted"],
            "summary": json.dumps(summary, default=str),
        },
    )


def fail_batch(engine: Engine, batch_id: int, error: BaseException) -> None:
    message = f"{type(error).__name__}: {error}"[:MAX_ERROR_LENGTH]
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE audit.etl_batch
                SET status = 'failed', completed_at = clock_timestamp(), error_message = :message
                WHERE batch_id = :batch_id AND status = 'running'
                """
            ),
            {"batch_id": batch_id, "message": message},
        )
