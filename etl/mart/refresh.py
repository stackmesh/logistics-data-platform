"""Refresh mart.mart_market_corridor atomically.

    python -m etl.mart.refresh
    python -m etl.mart.refresh --as-of 2026-10-26
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import load_config, warehouse_engine
from etl.log import configure_logging, log_event

MART_NAME = "mart.mart_market_corridor"
MAX_ERROR_LENGTH = 4000
logger = logging.getLogger("etl.mart")

SUMMARY_SQL = """
SELECT count(*) AS cells,
       count(DISTINCT price_month) AS months,
       count(DISTINCT (loading_city_gcid, unloading_city_gcid)) AS city_pairs,
       min(price_month) AS first_month,
       max(price_month) AS last_month,
       count(*) FILTER (WHERE reliability_level = 'HIGH') AS high,
       count(*) FILTER (WHERE reliability_level = 'MEDIUM') AS medium,
       count(*) FILTER (WHERE reliability_level = 'LOW_DATA') AS low_data,
       count(mom_pct) AS with_mom,
       count(yoy_pct) AS with_yoy,
       sum(trips_count) AS observations
FROM mart.mart_market_corridor
"""


def refresh_market_corridor(engine: Engine, as_of: date) -> dict:
    """Replace the mart in one transaction and record the refresh in audit.mart_refresh."""
    with engine.begin() as conn:
        refresh_id = conn.execute(
            text("INSERT INTO audit.mart_refresh (mart_name, as_of_date, status) "
                 "VALUES (:mart, :as_of, 'running') RETURNING refresh_id"),
            {"mart": MART_NAME, "as_of": as_of},
        ).scalar_one()
    log_event(logger, "mart_refresh_started", refresh_id=refresh_id, mart=MART_NAME, as_of_date=as_of)

    started = time.perf_counter()
    try:
        with engine.begin() as conn:
            deleted, inserted = conn.execute(
                text("CALL mart.refresh_market_corridor(:as_of, :refresh_id, NULL, NULL)"),
                {"as_of": as_of, "refresh_id": refresh_id},
            ).one()
            conn.execute(text("ANALYZE mart.mart_market_corridor"))
            conn.execute(
                text("UPDATE audit.mart_refresh SET status = 'succeeded', completed_at = clock_timestamp(), "
                     "rows_deleted = :deleted, rows_inserted = :inserted WHERE refresh_id = :refresh_id"),
                {"deleted": deleted, "inserted": inserted, "refresh_id": refresh_id},
            )
            summary = dict(conn.execute(text(SUMMARY_SQL)).mappings().one())
    except Exception as exc:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE audit.mart_refresh SET status = 'failed', completed_at = clock_timestamp(), "
                     "error_message = :message WHERE refresh_id = :refresh_id AND status = 'running'"),
                {"message": f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH], "refresh_id": refresh_id},
            )
        log_event(logger, "mart_refresh_failed", logging.ERROR, refresh_id=refresh_id,
                  error=f"{type(exc).__name__}: {exc}")
        raise

    result = {
        "refresh_id": refresh_id,
        "mart": MART_NAME,
        "as_of_date": as_of,
        "rows_deleted": int(deleted),
        "rows_inserted": int(inserted),
        "duration_s": round(time.perf_counter() - started, 3),
        **summary,
    }
    log_event(logger, "mart_refresh_completed", **result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m etl.mart.refresh")
    parser.add_argument("--as-of", type=date.fromisoformat, help="snapshot date (default AS_OF_DATE)")
    args = parser.parse_args(argv)
    configure_logging()
    config = load_config()
    engine = warehouse_engine(config)
    try:
        refresh_market_corridor(engine, args.as_of or config.as_of_date)
    except Exception:
        return 1
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
