# Smart Logistics Data & ML Platform

A local, repeatable batch platform on PostgreSQL 17. It builds the supplied operational snapshot, loads validated deals into a core warehouse, refreshes a market-corridor mart, trains a CatBoost price model, and scores both the historical test window and planned future deals. One Python runner executes the whole chain and writes an execution report.

The source files `requirements/reference/sql/schema.sql` and `requirements/reference/sql/synthetic.sql` are mounted into the database container and applied in that order. Their business logic is unchanged. The generator keeps `SELECT setseed(0.20261026)` and receives `n_prices` from `N_PRICES` (default `300000`).

## Prerequisites

Docker Engine and Docker Compose v2.

## Start

```bash
docker compose up --build
```

The first start builds the Python image and creates the data volume. It applies the source schema, generates 300,000 deals, and creates the warehouse. Later starts reuse the volume and do not generate the snapshot again.

`postgres` stays up. `pipeline` runs `python -m scripts.run_pipeline --mode full` once, prints the execution summary, and exits. A pipeline container that exited with status 0 is the expected end. See [End-to-end pipeline](#end-to-end-pipeline).

```bash
docker compose up --build --exit-code-from pipeline   # CI form: stops when the pipeline exits, returns its exit code
docker compose up                                     # second run: starts the exited pipeline container again
docker compose run --rm pipeline python -m scripts.run_pipeline --mode score
PIPELINE_MODE=etl docker compose up pipeline          # pick the compose pipeline's mode
```

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
| `PIPELINE_DB_WAIT_TIMEOUT_S`, `PIPELINE_DB_WAIT_INTERVAL_S` | How long and how often the runner polls the databases before step 1 (default 600 s, every 5 s) |
| `PIPELINE_RETRY_BACKOFF_S` | First retry delay for transient database errors, doubled per attempt (default 5 s) |
| `PIPELINE_REPORT_DIR`, `ML_FEATURE_DIR`, `ML_ARTIFACT_DIR` | Report, feature, and model directories (defaults `artifacts/reports`, `artifacts/features`, `ml/artifacts`) |

No application path is absolute. SQL files are mounted from the project directory.

## Logs

```bash
docker compose logs -f postgres
docker compose logs pipeline
```

Initialization lines are prefixed with `[init ...]`. Smoke-test lines are prefixed with `[smoke]`. PostgreSQL logs connections to stderr with a UTC timestamp, user, and database.

## Smoke test

The runner's `warehouse_ready` task runs these checks: the server version, both connections, source tables, the price count, the warehouse schemas, and `audit.bootstrap`. Any failed check stops the run.

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
python -m etl.mart.refresh --if-changed     # skip the rebuild when the input fingerprint matches the last refresh
psql -f sql/mart/examples.sql               # six example queries
pytest tests/test_mart_market_corridor.py   # reference recomputation, edge cases, repeated refresh
```

The pipeline runner refreshes the mart after the data-quality step, then builds the ML features. Definitions, indexes, and the results on the real warehouse are in [docs/mart_market_corridor.md](docs/mart_market_corridor.md).

## ML features

```bash
python -m ml.features.dataset                 # X/y/ids Parquet + metadata.json + profile.json in artifacts/features
pytest tests/test_ml_features.py              # rules and leakage guards, no database
pytest tests/test_ml_feature_dataset.py       # real warehouse: counts, boundaries, reproducibility
```

`ML_FEATURE_DIR` overrides the output directory. In compose, the files land in the `pipeline_artifacts` volume. The split is fixed: train `[2026-07-26, 2026-10-05)`, test `[2026-10-05, 2026-10-27)` on `loading_date`. The target `price_rub` is written only to `y_*.parquet`. Definitions, filtering, distributions, and the leakage audit are in [docs/feature_engineering_report.md](docs/feature_engineering_report.md).

## Model training and batch scoring

```bash
python -m ml.model.train                  # CatBoost + baselines, artifacts in ml/artifacts/<version>/, row in ml.ml_model_registry
python -m ml.scoring.score                # test + future deals -> ml.ml_price_predictions (transactional upsert)
python -m ml.scoring.score --role future  # one role per batch; rows of the other role are left alone
psql -f sql/ml/fact_vs_forecast.sql       # actual vs predicted, error distribution, future predictions
pytest tests/test_ml_model.py tests/test_ml_scoring.py
```

`ML_ARTIFACT_DIR` overrides `ml/artifacts`. In compose that directory is the `ml_models` volume. Future deals get a prediction and NULL actuals, whatever their source price says. Measured metrics and limitations are in [docs/ml_evaluation_report.md](docs/ml_evaluation_report.md).

## End-to-end pipeline

`python -m scripts.run_pipeline` is the single entry point. `orchestration/` holds a small dependency-graph runner. Each task declares the tasks it depends on, and the order is a topological sort that keeps declaration order on ties. There is no Airflow.

```bash
python -m scripts.run_pipeline                                    # --mode full
python -m scripts.run_pipeline --mode etl                         # optional --start-date/--end-date
python -m scripts.run_pipeline --mode mart
python -m scripts.run_pipeline --mode train
python -m scripts.run_pipeline --mode score
python -m scripts.run_pipeline --mode backfill --start-date 2026-09-01 --end-date 2026-09-30
python -m scripts.run_pipeline --mode full --dry-run              # print the plan, connect to nothing
python -m scripts.run_pipeline --force-mart                       # rebuild the mart even if its inputs are unchanged
```

| Step | Task | Depends on | full | etl | mart | train | score | backfill |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1-2 | `source_ready`: source answers and `public.prices` has `N_PRICES` rows | | x | x | | | | x |
| 3 | `warehouse_ready`: `audit.bootstrap` exists, all readiness checks pass | source_ready | x | x | x | x | x | x |
| 4 | `migrate`: `sql/core`, `sql/mart`, `sql/ml` | warehouse_ready | x | x | x | x | x | x |
| 5 | `etl`: full history, or the given window | source_ready, migrate | x | x | | | | x |
| 6 | `data_quality`: independent source-to-core reconciliation; fails on any unexplained deal | etl | x | x | | | | x |
| 7 | `mart`: refresh `mart.mart_market_corridor` if its inputs changed | migrate, data_quality | x | | x | | | x |
| 8 | `features`: feature dataset to `ML_FEATURE_DIR` | migrate, data_quality | x | | | x | | |
| 9 | `train`: CatBoost and baselines, artifacts, `ml.ml_model_registry` | features | x | | | x | | |
| 10 | `score_test`: historical test window | migrate, data_quality, train | x | | | | x | x |
| 11 | `score_future`: planned deals, actuals NULL | migrate, data_quality, train | x | | | | x | x |
| 12 | `verify`: keys, foreign keys, populated tables, future actuals | all of the above | x | x | x | x | x | x |
| 12 | report: always written, also after a failure | | x | x | x | x | x | x |

Steps 1 to 3 initialize the databases. In compose that work happens in the `postgres` container's init script. The runner waits for it and checks it, so the same plan works from compose and from a host. A dependency outside the selected mode counts as satisfied by what the warehouse already holds. For example, `score` uses the model that `ml/artifacts/latest.json` points to. A backfill rescores with the existing model and skips scoring when no model has been trained yet.

### Reliability

- **Health checks.** Before its first task, the runner polls both databases every `PIPELINE_DB_WAIT_INTERVAL_S` seconds. It waits until the generator has written every deal and the bootstrap row exists. This covers the server restart at the end of container initialization. A wrong password fails at once instead of waiting.
- **Retries.** Retries apply only to transient database errors: lost connections, server restarts, serialization failures, and deadlocks. Delay is `PIPELINE_RETRY_BACKOFF_S` × 2^(attempt−1), with 2 retries for most tasks and 1 for training. Data, constraint, and credential errors fail on the first attempt. Every task is transactional and idempotent, so a retry is safe.
- **Transactions.** The ETL batch, the mart refresh, and each scoring batch commit atomically with their audit row, under the shared advisory lock `logistics_dwh.core_etl`. The `verify` task and the report read one repeatable-read snapshot each.
- **Idempotency.** Core and predictions are upserted with `IS DISTINCT FROM`, so unchanged rows are not rewritten. The mart rebuild is skipped when `mart.market_corridor_input_fingerprint()` matches the one stored with the last successful refresh. That fingerprint hashes every core value the build reads and both mart definitions. Retraining on the same data produces the same model version and a byte-identical `model.cbm`.
- **Failure reporting.** A failed task records its exception type, message, attempts, and traceback tail. Tasks that depend on it are marked `not_run` with the reason, and independent tasks still run. The exit code is 0 on success, 1 when any task failed or did not run, and 2 for a usage or configuration error.
- **Structured logging.** Every stdout line is JSON. Events include `pipeline_started`, `task_started`, `task_succeeded`, `task_retry`, `task_failed`, `task_not_run`, `database_waiting`, `pipeline_completed`, and `pipeline_failed`. CatBoost's `verbose=100` progress arrives as `catboost_progress` events. A readable summary goes to stderr.

### Execution report

Each run writes `PIPELINE_REPORT_DIR/pipeline_run_<UTC start>_<run id>.json` and `latest.json`, and one row in `audit.pipeline_run`. The report holds:

- the run: mode, window, status, start and end, duration, failed tasks with error details, and tasks that did not run;
- per-task status, attempts, duration, and output;
- counts: source records, extracted, accepted, and quarantined deals, core deals, mart rows, ML training, validation, test, and future rows;
- the model version, test metrics, baseline metrics, and scoring statistics;
- the results of the integrity checks;
- logical fingerprints of core, raw, quarantine, mart, predictions, and the registry. Lineage columns such as batch ids and timestamps are excluded, so the report can say whether this run changed any logical result compared with the previous successful run.

```sql
SELECT run_id, mode, status, duration_s, failed_task, report -> 'previous_run' -> 'comparison'
FROM audit.pipeline_run ORDER BY started_at DESC LIMIT 5;
```

### Measured results: clean environment, run twice

Docker is not installed on the development host. There is no daemon, no root access, and no `newuidmap` for rootless mode, so `docker compose up --build` itself has not been run. Two things were checked without it:

- `docker compose config` (Compose v2.39.4) validates the compose file.
- `requirements.txt` resolves to prebuilt CPython 3.12 manylinux wheels for all 33 packages. Their versions are the ones that produced the results below, so the image build needs no compiler.

The run itself copied what the `postgres:17` container does on an empty volume:

1. A new PostgreSQL 17.11 cluster with `initdb`, scram host authentication, and the `en_US.utf8` locale, using the compose server settings.
2. `createdb deals_bigdata`.
3. `docker/postgres/01_init.sh`, started in the background with the `.env.example` values.
4. Three seconds later, the runner, connecting over TCP as the application roles without the superuser password, with empty feature, model, and report directories.

The same commands ran a second time with no other change.

| | Run 1, clean | Run 2, same volume |
| --- | --- | --- |
| Source init: schema and generator | 40.6 s (outside the runner) | not repeated |
| `source_ready` | 40.07 s, waited 9 polls for the generator | 0.04 s |
| `etl` (300,000 deals) | 52.51 s: 299,797 inserted, 203 quarantined | 14.64 s: 0 inserted, 0 updated, 0 deleted, 299,797 unchanged |
| `data_quality` | 4.62 s, 0 unexplained, 0 duplicate ids | 4.49 s, same |
| `mart` | 13.41 s, 203,644 rows inserted | 1.40 s, skipped: inputs unchanged |
| `features`, `train` | 0.56 s, 5.34 s | 0.54 s, 5.39 s |
| `score_test`, `score_future` | 1.18 s, 0.39 s: 12,808 and 4,891 inserted | 0.56 s, 0.15 s: 17,699 unchanged |
| `verify`, report | 2.11 s, 5.63 s | 1.80 s, 5.50 s |
| Total, exit code | 126.0 s, 0 | 34.9 s, 0 |
| Compared with the previous run | n/a | core, mart, predictions, registry: unchanged |

Both runs reported the same results:

| Count | Value |
| --- | --- |
| Source records | 300,000 |
| Extracted / accepted / quarantined | 300,000 / 299,797 / 203 (164 `PRICE_NON_POSITIVE`, 39 `FX_RATE_MISSING`) |
| Core DWH deals | 299,797 |
| Mart rows | 203,644 |
| ML training / validation / test / future | 24,226 / 5,523 / 12,808 / 4,891 |
| Model | `20261026-8f32ec6d6901`, 263 trees, `model.cbm` SHA-256 `e089fe64…68f49a8`, identical to the Stage 7 artifact |
| CatBoost test metrics | R² 0.9203, MAPE 15.72 %, RMSE 54,018.83 RUB, MAE 28,190.88 RUB |
| Baselines (R², MAPE) | decision tree 0.9067 / 15.87 %, Ridge 0.8405 / 49.43 %, median RUB/km 0.7486 / 41.90 % |

All 11 integrity checks passed in both runs:

- every required table populated;
- every constraint validated;
- 35 foreign keys with no orphans;
- 29 primary and unique keys with no duplicates;
- no deal both loaded and quarantined;
- the full load reconciles (300,000 = 299,797 + 203);
- all 4,891 future rows have NULL actual, absolute error, and APE, and all fall after as_of;
- all 12,808 test actuals equal `core.fact_deal.price_rub` and fall inside the test window;
- the current model scored both roles.

A separate SQL pass after run 2 found the same results. It also showed that run 2 rewrote nothing:

- every core and raw row still carries ETL batch 1;
- the mart still carries refresh 1;
- predictions still carry the scoring batches of run 1.

The other modes were run once each, afterwards, on the same warehouse:

| Mode | Duration | Result |
| --- | --- | --- |
| backfill, 2026-09-01 to 2026-09-30 | 12.7 s | 13,767 extracted, 13,756 accepted, 11 quarantined. Rescored. Every layer unchanged |
| etl | 26.8 s | full reload. Every layer unchanged |
| etl, 2026-10-20 to 2026-10-26 | 9.4 s | 6,862 extracted, 6,851 accepted, 11 quarantined. Every layer unchanged |
| mart | 8.7 s | refresh skipped |
| train | 13.3 s | same model version |
| score | 8.7 s | 17,699 unchanged |

Failure paths tested against the real cluster:

- **Unreachable port:** `source_ready` fails after its wait timeout, the 10 tasks that depend on it are marked `not_run`, the exit code is 1, and the report file is still written.
- **Wrong source password:** fails on the first attempt in 0.03 s and is recorded in `audit.pipeline_run`.
- **Missing `DWH_DB_PASSWORD`:** exit code 2.

On a warm warehouse, about 5.5 s of each run goes to the report's fingerprints. The raw and quarantine hashes from `etl.quality.fingerprint` take most of that.

### Tests

```bash
pytest tests/test_pipeline_runner.py         # plan order per mode, dependencies, retries, failure propagation, CLI, report fallback; no database
pytest tests/test_pipeline_integration.py    # train + score twice through the runner, injected failure, mart skip and change detection, integrity helpers
```

Against the clean warehouse above, the whole suite gives `254 passed in 267.82s`.

## Reset

Initialization runs only on an empty data volume.

```bash
docker compose down -v
docker compose up --build
```

`down -v` deletes the generated snapshot.
