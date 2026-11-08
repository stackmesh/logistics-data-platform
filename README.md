# Smart Logistics Data & ML Platform

A local PostgreSQL 17 environment with the supplied operational snapshot, a core warehouse schema, and a batch ETL that loads validated deals into it. Marts and ML are not built yet.

The source files `requirements/reference/sql/schema.sql` and `requirements/reference/sql/synthetic.sql` are mounted into the database container and applied in that order. Their business logic is unchanged. The generator keeps `SELECT setseed(0.20261026)` and receives `n_prices` from `N_PRICES` (default `300000`).

## Prerequisites

Docker Engine and Docker Compose v2.

## Start

```bash
docker compose up --build
```

The first start builds the Python image, creates a data volume, applies the source schema, generates 300,000 deals, then creates the warehouse. Generation usually takes several minutes. Later starts reuse the volume and do not generate the snapshot again.

`postgres` stays up. `pipeline` applies `sql/core` migrations, runs the connectivity smoke test, runs a full ETL load, and exits 0. An exited pipeline container after a zero status is the expected end.

## What is created

Both databases are in one PostgreSQL 17 instance, with separate login roles.

| Database | Role | Purpose |
| --- | --- | --- |
| `deals_bigdata` | `source_reader` | Operational snapshot. Read-only role. |
| `logistics_dwh` | `dwh_etl` | Analytical warehouse. Owns `raw`, `core`, `mart`, `ml`, and `audit`. |

`audit.bootstrap` stores the initialized price count so the health check can tell that generation finished. The other warehouse schemas are empty until a later ETL stage.

The pipeline container uses the two application roles. Its entrypoint removes `POSTGRES_PASSWORD` from the process environment before the check starts.

## Configuration

Local defaults live in `.env.example`. They are for this container only. Change them before any shared use.

To override a value without editing the template:

```bash
cp .env.example .env
```

Compose loads `.env.example` into both containers. A file named `.env` is optional and overrides those values when it exists. Do not commit `.env`.

| Variable | Meaning |
| --- | --- |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | Superuser used only by initialization and the health check |
| `SOURCE_DB_*` | Read-only source connection |
| `DWH_DB_*` | Warehouse connection |
| `POSTGRES_HOST` | `postgres` on the compose network |
| `POSTGRES_PORT` | Port inside the network (`5432`) |
| `POSTGRES_PUBLISH_PORT` | Host port published to the machine (default `5432`) |
| `N_PRICES` | Deal count passed to `synthetic.sql` |
| `PGCONNECT_TIMEOUT` | Client connect timeout in seconds |
| `AS_OF_DATE` | Snapshot date stored on each batch and used in the run profile (default `2026-10-26`) |
| `ETL_FETCH_SIZE` | Rows fetched from the source per round trip |
| `LOG_LEVEL` | ETL log level |

No application path is absolute. SQL files are mounted from the project directory.

## Logs

```bash
docker compose logs -f postgres
docker compose logs pipeline
```

Initialization lines are prefixed with `[init ...]`. Smoke-test lines are prefixed with `[smoke]`. PostgreSQL logs connections to stderr with a UTC timestamp, user, and database.

## Smoke test

The pipeline command checks the server version, both connections, source tables, the price count, the warehouse schemas, and `audit.bootstrap`.

Run it again, or run the same checks under pytest:

```bash
docker compose run --rm pipeline python scripts/check_databases.py
docker compose run --rm pipeline python -m pytest tests/test_db_smoke.py -q
```

From the host, point the client at the published port:

```bash
set -a
source .env.example
set +a
export POSTGRES_HOST=localhost
export POSTGRES_PORT="${POSTGRES_PUBLISH_PORT:-5432}"
python scripts/check_databases.py
```

The host command needs the Python dependencies in `requirements.txt` and uses the machine's Python. The container image is Python 3.12.

## Core warehouse schema

`sql/core/` creates the raw snapshots, conformed dimensions and deal fact, quarantine, and batch history. It does not copy source rows. Apply it after the database is healthy, using the warehouse role:

```bash
export PGHOST=localhost PGPORT="${POSTGRES_PUBLISH_PORT:-5432}"
export PGUSER="$DWH_DB_USER" PGPASSWORD="$DWH_DB_PASSWORD" PGDATABASE="$DWH_DB_NAME"
bash sql/core/apply.sh
```

The script is safe to run again. `python -m etl.migrate` applies the same files without `psql`. Schema checks:

```bash
pytest tests/test_core_schema.py -q
```

## ETL

The ETL reads `deals_bigdata` in one read-only, repeatable-read transaction. It validates and normalizes the deals in Polars, then loads `raw`, `core`, and `audit` in a single warehouse transaction.

```bash
# window on prices.loading_date, both bounds included
python -m etl.main --start-date 2026-07-26 --end-date 2026-10-26

# complete history
python -m etl.main --full

# same arguments from a checkout
python scripts/run_etl.py --full

# inside compose
docker compose run --rm pipeline python -m etl.main --start-date 2026-07-26 --end-date 2026-10-26
```

Exit code 0 means the batch committed, 1 means it failed and was recorded as `failed`, and 2 means a usage or configuration error. Log lines are JSON on stdout: `batch_started`, `extract_completed`, `transform_completed`, then `batch_completed` or `batch_failed`. A readable summary goes to stderr.

### What a batch does

1. Records a `running` row in `audit.etl_batch` and commits it, so a crash stays visible.
2. Extracts the window's `prices` with their `price_body_types` and `price_dates`, plus all of `cities`, `body_types`, and `exchange_rates`.
3. Applies the quality rules below. A deal that fails any rule goes to `audit.quarantine_price` with one row per failed rule: the reason code, a readable detail, the full source row as JSONB, the batch, and `rejected_at`.
4. Normalizes accepted deals:
   - Cities: one row per `gc_id`, taken from the smallest `cities.id`.
   - Body groups: one bridge row per deal and group.
   - FX: RUB uses rate 1. USD and EUR use the rate whose date equals `nearest_rate_date`, with no fallback.
   - Categories: valid source values are kept. Missing or invalid ones are calculated and flagged `*_imputed`.
5. COPYs everything into temporary staging tables, then merges with `INSERT ... ON CONFLICT DO UPDATE ... WHERE row IS DISTINCT FROM`. Window rows that left the source or now fail validation are removed from core.
6. Writes `audit.dq_statistic` and marks the batch `succeeded` with its counts, in the same transaction as the data.

| Reason code | Rule |
| --- | --- |
| `PRICE_NON_POSITIVE` | `price <= 0` |
| `RUB_PRICE_LE_1000` | currency 643 and `0 < price <= 1000` |
| `DISTANCE_NON_POSITIVE` | `distance <= 0` |
| `WEIGHT_NEGATIVE` | `total_weight < 0` |
| `VOLUME_NEGATIVE` | `total_volume < 0` |
| `ORIGIN_CITY_NOT_FOUND` | `loading_city_gcid` not in `cities` |
| `DESTINATION_CITY_NOT_FOUND` | `unloading_city_gcid` not in `cities` |
| `CURRENCY_UNSUPPORTED` | currency not 643, 840, or 978 |
| `FX_RATE_MISSING` | USD or EUR with `nearest_rate_date` NULL or no rate on that exact date |

Same-city routes are not rejected. Non-fact deals, future loading dates, and `outlier_mark` are kept as the source has them.

### Idempotency, retries, and backfill

- Rows are rewritten only when a value changes. A repeated run reports zero inserts and updates, and keeps the original `batch_id` and `extracted_at` on unchanged rows.
- Any window can be reloaded at any time. Only deals whose `loading_date` is in that window, or that were extracted in it, are touched.
- A failed batch rolls back completely, so retrying the same command is the recovery.
- A transaction-level advisory lock serializes concurrent runs.

Amounts are exact. `price_src` is rounded to 2 decimals and the rate to 6, both half away from zero. `price_rub = ROUND(price_src * fx_rate, 2)`, computed with integer arithmetic and stored as `numeric`.

### Snapshot limitation: outlier marks on 2026-10-26

The source marks outliers in an overnight job. Deals from 2026-10-26 have not necessarily passed it. In this snapshot the generator sets `outlier_mark` only for deals created before `2026-10-26 02:30:00`, so later deals have `false` even when their price is extreme.

The ETL does not recalculate the flag. Downstream statistics that exclude outliers will include those unmarked late deals. Each batch reports `created_on_as_of_date` and how many of those are outlier-marked.

### Existing volumes

Volumes created before this stage lack `TEMPORARY` on `logistics_dwh` for `dwh_etl`, and the ETL stages data in temporary tables. Grant it once as the superuser, or recreate the volume:

```bash
docker compose exec postgres psql -U postgres -c 'GRANT TEMPORARY ON DATABASE logistics_dwh TO dwh_etl'
```

### Tests

```bash
pytest tests/test_etl_transform.py -q          # rules only, no database
pytest -q                                       # everything, needs the databases
docker compose --profile test run --rm tests    # migrations + whole suite in the container
```

Integration tests start with one committed full load, which is idempotent. The idempotency, backfill, and lock tests commit real batches. Every other test that writes rolls back. Run the suite against a non-production warehouse.

### Quality tooling

```bash
python -m etl.quality.reconcile --full --by-month   # independent source-to-core reconciliation, exit 1 on any unexplained deal
python scripts/benchmark_etl.py                     # cold load (rolled back) and warm rerun timings
python scripts/dq_audit.py                          # evidence behind docs/data_quality_report.md
```

Results are in [docs/data_quality_report.md](docs/data_quality_report.md).

## Market corridor mart

```bash
python -m etl.mart.refresh                  # rebuild mart.mart_market_corridor for AS_OF_DATE, one transaction
psql -f sql/mart/examples.sql               # six example queries
pytest tests/test_mart_market_corridor.py   # reference recomputation, edge cases, repeated refresh
```

The compose `pipeline` service refreshes the mart after the ETL, then builds the ML features. Definitions, indexes, and the results on the real warehouse are in [docs/mart_market_corridor.md](docs/mart_market_corridor.md).

## ML features

```bash
python -m ml.features.dataset                 # X/y/ids Parquet + metadata.json + profile.json in artifacts/features
pytest tests/test_ml_features.py              # rules and leakage guards, no database
pytest tests/test_ml_feature_dataset.py       # real warehouse: counts, boundaries, reproducibility
```

`ML_FEATURE_DIR` overrides the output directory. In compose, the files land in the `ml_artifacts` volume. The split is fixed: train `[2026-07-26, 2026-10-05)`, test `[2026-10-05, 2026-10-27)` on `loading_date`. The target `price_rub` is written only to `y_*.parquet`. Definitions, filtering, distributions, and the leakage audit are in [docs/feature_engineering_report.md](docs/feature_engineering_report.md).

## Reset

Initialization runs only on an empty data volume.

```bash
docker compose down -v
docker compose up --build
```

`down -v` deletes the generated snapshot.
