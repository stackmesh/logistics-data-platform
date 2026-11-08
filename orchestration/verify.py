"""Post-run integrity checks on the warehouse, in one read-only snapshot.

Constraint checks are generated from pg_constraint, so a constraint added later is checked too.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

SCHEMAS = ("raw", "core", "mart", "ml", "audit")

CORE_TABLES = (
    "raw.prices", "raw.cities", "raw.body_types", "raw.exchange_rates", "raw.price_body_types", "raw.price_dates",
    "core.dim_city", "core.dim_body_group", "core.dim_body_type", "core.dim_exchange_rate",
    "core.fact_deal", "core.fact_deal_date", "core.bridge_deal_body_group", "audit.etl_batch",
)
MART_TABLES = ("mart.mart_market_corridor", "audit.mart_refresh")
ML_TABLES = ("ml.ml_model_registry", "ml.ml_price_predictions", "audit.ml_scoring_batch")
MODE_LAYERS = {
    "full": ("core", "mart", "ml"),
    "etl": ("core",),
    "mart": ("core", "mart"),
    "train": ("core", "ml_model"),
    "score": ("core", "ml"),
    "backfill": ("core", "mart"),
}

CONSTRAINTS_SQL = """
SELECT c.conname, c.contype, c.conrelid::regclass::text AS child, c.confrelid::regclass::text AS parent,
       ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
             JOIN pg_attribute AS a ON a.attrelid = c.conrelid AND a.attnum = k.attnum ORDER BY k.ord) AS columns,
       ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY AS k(attnum, ord)
             JOIN pg_attribute AS a ON a.attrelid = c.confrelid AND a.attnum = k.attnum ORDER BY k.ord) AS ref_columns
FROM pg_constraint AS c
WHERE c.connamespace::regnamespace::text = ANY (:schemas) AND c.contype IN ('f', 'p', 'u')
ORDER BY c.conrelid::regclass::text, c.conname
"""

NOT_VALIDATED_SQL = """
SELECT coalesce(array_agg(conrelid::regclass::text || '.' || conname ORDER BY conname), '{}')
FROM pg_constraint
WHERE connamespace::regnamespace::text = ANY (:schemas) AND NOT convalidated
"""

FUTURE_SQL = """
SELECT count(*) AS rows,
       count(*) FILTER (WHERE actual_price_rub IS NOT NULL OR abs_error_rub IS NOT NULL OR ape_pct IS NOT NULL)
           AS with_actuals,
       count(*) FILTER (WHERE p.loading_date <= p.as_of_date) AS not_after_as_of,
       count(*) FILTER (WHERE p.loading_date <= r.as_of_date) AS not_after_model_as_of
FROM ml.ml_price_predictions AS p
JOIN ml.ml_model_registry AS r USING (model_name, model_version)
WHERE p.split_role = 'future'
"""

TEST_SQL = """
SELECT count(*) AS rows,
       count(*) FILTER (WHERE p.actual_price_rub IS DISTINCT FROM f.price_rub) AS actual_differs_from_core,
       count(*) FILTER (WHERE p.loading_date NOT BETWEEN r.test_start AND r.test_end) AS outside_test_window,
       count(*) FILTER (WHERE p.ape_pct IS DISTINCT FROM
                        round(abs(p.actual_price_rub - p.predicted_price_rub) / p.actual_price_rub * 100, 4))
           AS ape_mismatch
FROM ml.ml_price_predictions AS p
JOIN ml.ml_model_registry AS r USING (model_name, model_version)
JOIN core.fact_deal AS f USING (price_id)
WHERE p.split_role = 'test'
"""

CURRENT_MODEL_SQL = """
SELECT count(*) FILTER (WHERE split_role = 'test') AS test_rows,
       count(*) FILTER (WHERE split_role = 'future') AS future_rows
FROM ml.ml_price_predictions
WHERE model_version = :model_version
"""


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _check(name: str, ok: bool, **detail) -> dict:
    return {"name": name, "ok": bool(ok), "detail": detail}


def foreign_key_orphans(conn: Connection, fk: dict) -> int:
    child_cols = [_ident(column) for column in fk["columns"]]
    parent_cols = [_ident(column) for column in fk["ref_columns"]]
    not_null = " AND ".join(f"c.{column} IS NOT NULL" for column in child_cols)
    join = " AND ".join(f"p.{parent} = c.{child}" for parent, child in zip(parent_cols, child_cols))
    sql = (f"SELECT count(*) FROM {fk['child']} AS c WHERE {not_null} "
           f"AND NOT EXISTS (SELECT 1 FROM {fk['parent']} AS p WHERE {join})")
    return int(conn.execute(text(sql)).scalar_one())


def duplicate_keys(conn: Connection, key: dict) -> int:
    columns = ", ".join(_ident(column) for column in key["columns"])
    not_null = " AND ".join(f"{_ident(column)} IS NOT NULL" for column in key["columns"])
    sql = (f"SELECT count(*) FROM (SELECT 1 FROM {key['child']} WHERE {not_null} "
           f"GROUP BY {columns} HAVING count(*) > 1) AS d")
    return int(conn.execute(text(sql)).scalar_one())


def table_counts(conn: Connection, tables) -> dict[str, int]:
    return {table: int(conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()) for table in tables}


def _constraint_checks(conn: Connection) -> list[dict]:
    not_validated = conn.execute(text(NOT_VALIDATED_SQL), {"schemas": list(SCHEMAS)}).scalar_one()
    constraints = [dict(row) for row in conn.execute(text(CONSTRAINTS_SQL), {"schemas": list(SCHEMAS)}).mappings()]
    foreign = [c for c in constraints if c["contype"] == "f"]
    keys = [c for c in constraints if c["contype"] in ("p", "u")]
    orphans = {f"{c['child']}.{c['conname']}": foreign_key_orphans(conn, c) for c in foreign}
    duplicates = {f"{c['child']}.{c['conname']}": duplicate_keys(conn, c) for c in keys}
    return [
        _check("constraints_validated", not not_validated, not_validated=list(not_validated)),
        _check("foreign_keys_hold", not any(orphans.values()), checked=len(orphans),
               violations={name: n for name, n in orphans.items() if n}),
        _check("unique_keys_hold", not any(duplicates.values()), checked=len(duplicates),
               violations={name: n for name, n in duplicates.items() if n}),
    ]


def _core_checks(ctx, conn: Connection) -> list[dict]:
    both = int(conn.execute(text(
        "SELECT count(*) FROM core.fact_deal AS f "
        "WHERE EXISTS (SELECT 1 FROM audit.quarantine_price AS q WHERE q.price_id = f.price_id)"
    )).scalar_one())
    checks = [_check("deal_in_core_xor_quarantine", both == 0, deals_in_both=both)]
    summary = ctx.state.get("etl_summary")
    if summary and summary["mode"] == "full":
        fact_rows = int(conn.execute(text("SELECT count(*) FROM core.fact_deal")).scalar_one())
        quarantined = int(conn.execute(text("SELECT count(DISTINCT price_id) FROM audit.quarantine_price")).scalar_one())
        raw_rows = int(conn.execute(text("SELECT count(*) FROM raw.prices")).scalar_one())
        source = ctx.results.get("source_ready")
        source_rows = source.output.get("source_prices") if source and source.output else summary["extracted_count"]
        ok = (summary["extracted_count"] == source_rows == raw_rows
              and summary["accepted_count"] + summary["rejected_count"] == summary["extracted_count"]
              and fact_rows == summary["accepted_count"] and quarantined == summary["rejected_count"])
        checks.append(_check("full_load_reconciles", ok, source=source_rows, raw=raw_rows,
                             extracted=summary["extracted_count"], accepted=summary["accepted_count"],
                             rejected=summary["rejected_count"], core=fact_rows, quarantined_deals=quarantined))
    return checks


def _ml_checks(ctx, conn: Connection, require_predictions: bool) -> list[dict]:
    future = conn.execute(text(FUTURE_SQL)).mappings().one()
    test = conn.execute(text(TEST_SQL)).mappings().one()
    checks = [
        _check("future_actuals_null", future["with_actuals"] == 0, future_rows=future["rows"],
               with_actuals=future["with_actuals"]),
        _check("future_after_as_of", future["not_after_as_of"] == 0 and future["not_after_model_as_of"] == 0,
               not_after_as_of=future["not_after_as_of"], not_after_model_as_of=future["not_after_model_as_of"]),
        _check("test_actuals_match_core", test["actual_differs_from_core"] == 0 and test["ape_mismatch"] == 0,
               test_rows=test["rows"], actual_differs_from_core=test["actual_differs_from_core"],
               ape_mismatch=test["ape_mismatch"]),
        _check("test_rows_in_test_window", test["outside_test_window"] == 0,
               outside_test_window=test["outside_test_window"]),
    ]
    version = ctx.model_version()
    if require_predictions:
        current = conn.execute(text(CURRENT_MODEL_SQL), {"model_version": version}).mappings().one()
        checks.append(_check("current_model_scored", current["test_rows"] > 0 and current["future_rows"] > 0,
                             model_version=version, **current))
    return checks


def run_integrity_checks(ctx) -> list[dict]:
    layers = MODE_LAYERS[ctx.mode]
    if ctx.mode == "backfill" and ctx.model_version() is not None:
        layers = (*layers, "ml")
    required = list(CORE_TABLES)
    if "mart" in layers:
        required += MART_TABLES
    if "ml" in layers:
        required += ML_TABLES
    if "ml_model" in layers:
        required.append("ml.ml_model_registry")
    with ctx.warehouse().connect() as conn, conn.begin():
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        counts = table_counts(conn, required)
        empty = [table for table, n in counts.items() if n == 0]
        checks = [_check("required_tables_populated", not empty, empty=empty, counts=counts)]
        checks += _constraint_checks(conn)
        checks += _core_checks(ctx, conn)
        checks += _ml_checks(ctx, conn, require_predictions="ml" in layers)
    return checks
