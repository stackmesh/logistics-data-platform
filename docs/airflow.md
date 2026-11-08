# Airflow orchestration — `smart_logistics_data_pipeline`

The platform's batch pipeline runs as an Apache Airflow 3.4 DAG. Airflow schedules, retries, times out, and records the tasks. The business logic stays in the platform's `etl`, `ml`, and `orchestration` packages. The command-line runner (`python -m scripts.run_pipeline`, README section 22) still works and is unchanged.

| File | Role |
| --- | --- |
| `dags/smart_logistics_data_pipeline.py` | The DAG: tasks, dependencies, schedule, retries, timeouts, params |
| `dags/smart_logistics/pipeline_step.py` | Runs one step in the pipeline interpreter, maps its exit code to Airflow retry semantics, and collects task states for the run statistics. Imports nothing from the platform |
| `orchestration/airflow_steps.py` | Step command line in the platform: each step calls the existing task function |
| `sql/core/008_pipeline_run_airflow.sql` | Allows `mode = 'airflow'` in `audit.pipeline_run` |
| `docker/airflow/Dockerfile`, `docker/airflow/entrypoint.sh` | Airflow image with the platform in its own virtualenv |
| `docker-compose.yml`, profile `airflow` | Metadata database, init, API server, scheduler, DAG processor |
| `tests/test_airflow_dag.py`, `tests/test_airflow_steps.py` | DAG, window, retry, and step tests |

## 1. Design

**Two interpreters.** Airflow 3.4's constraint file pins Polars 2.1, pandas 3.1.0, psycopg 3.3.6, and SQLAlchemy 2.0.55. The platform is tested with Polars 1.44.2 and the rest of `constraints.txt`, and the model version is a hash over the training data and the CatBoost version. Installing the platform into Airflow's environment would change the tested stack. So every task runs

```text
$PIPELINE_PYTHON -m orchestration.airflow_steps STEP --run-key DAG_ID/RUN_ID --start-date D --end-date D --result-file F
```

in `PIPELINE_HOME`. In the image that is `/opt/pipeline/venv/bin/python` in `/opt/pipeline/app`. The step writes its JSON result to the file, which becomes the task's XCom. Its JSON log lines stream into the Airflow task log.

**No duplicated logic.** Each step calls the function `scripts.run_pipeline` uses:

| Airflow task | Calls | Timeout | Retries |
| --- | --- | --- | --- |
| `check_source_connectivity` | One connection to the source and to the warehouse; `SHOW server_version` | 2 min | 5 |
| `validate_source_data` | `orchestration.tasks.source_ready` (all `N_PRICES` deals generated), plus the source deal count in the window | 15 min | 3 |
| `prepare_warehouse` | `tasks.warehouse_ready` (bootstrap + readiness checks), `tasks.migrate` | 15 min | 3 |
| `run_etl` | `tasks.etl` → `etl.pipeline.runner.run_pipeline(window)` | 45 min | 3 |
| `run_data_quality` | `tasks.data_quality` → `etl.quality.reconcile.reconcile_window(window)` | 20 min | 3 |
| `refresh_marts` | `tasks.mart` → `etl.mart.refresh.refresh_if_changed` (or a forced rebuild) | 30 min | 3 |
| `generate_features` | `tasks.features` → `ml.features.dataset.build_dataset` / `write_dataset` | 20 min | 3 |
| `train_or_load_model` | `ml.model.train.model_version`; reuse the registered model, else `tasks.train` | 60 min | 1 |
| `run_batch_inference` | `tasks.score_test`, `tasks.score_future` → `ml.scoring.score.score` with the version from the previous task | 30 min | 3 |
| `validate_prediction_records` | `tasks.verify` → `orchestration.verify.run_integrity_checks` (keys, foreign keys, future actuals null, test actuals equal core, current model scored) | 20 min | 3 |
| `publish_pipeline_statistics` | `orchestration.report.build_report` / `persist` from the Airflow task states | 10 min | 2 |

`prepare_warehouse` is an extra task between source validation and ETL. The ETL needs the warehouse bootstrap and the migrations, and the brief's ten tasks have no place for them.

```mermaid
flowchart LR
  c[check_source_connectivity] --> v[validate_source_data] --> p[prepare_warehouse] --> e[run_etl] --> q[run_data_quality]
  q --> m[refresh_marts]
  q --> f[generate_features] --> t[train_or_load_model] --> i[run_batch_inference]
  m --> vp[validate_prediction_records]
  i --> vp
  vp --> s["publish_pipeline_statistics<br/>(trigger rule all_done)"]
```

The mart and the feature build both read core after data quality has passed, so they run in parallel. Prediction validation waits for both, because its integrity checks cover the mart too.

## 2. Dates, data intervals, and backfills

**Schedule.** `CronDataIntervalTimetable("0 0 * * *", UTC)`: the run at D+1 00:00 UTC has the data interval [D, D+1) and loads `loading_date` D. In Airflow 3, `schedule="@daily"` produces zero-length intervals, so the window would be the day that has just started. `catchup=False`: enabling the DAG does not create three years of runs; history is loaded by backfill. `start_date` is 2023-10-01, just before the first deal (2023-10-03).

**Window rule** (`resolve_window`):

1. If the params `start_date` and `end_date` are set, the window is that inclusive range. Use this for manual runs.
2. Otherwise the window is the data interval: from `data_interval_start` to `data_interval_end` minus one microsecond, in UTC, inclusive. A zero-length interval gives the start day.
3. A run with neither fails at once, with no retry. Nothing is recorded in `audit.pipeline_run`, because no step ran.

The window drives the ETL, the reconciliation, and the source count. Every step receives it, and the run statistics record it. The mart, the feature set, the model, and scoring describe the fixed snapshot (`AS_OF_DATE = 2026-10-26`, README section 17), not the interval. A daily run therefore rescores the same test and future deals and reports them unchanged. The system date is never used.

**Manual runs.** Airflow 3.4's CLI behaves in two ways that matter here:

- `airflow dags trigger` without `--logical-date` creates a run with no logical date and no data interval. The DAG fails it at once ("no data interval") instead of guessing a window from the clock.
- `--logical-date X` gives the data interval of the last complete day before the trigger time, not the day of X. The REST API does use the logical date.

Pass the window explicitly instead:

```bash
airflow dags trigger smart_logistics_data_pipeline --conf '{"start_date": "2026-09-01", "end_date": "2026-09-30"}'
```

Params: `start_date`, `end_date` (inclusive dates); `force_mart` rebuilds the mart even if its inputs are unchanged; `retrain` trains even if the model version is already registered.

**Backfill.** One run per day, in date order, one at a time (`max_active_runs=1`):

```bash
airflow backfill create --dag-id smart_logistics_data_pipeline --from-date 2026-09-01 --to-date 2026-09-30 --max-active-runs 1
airflow backfill create ... --reprocess-behavior completed     # also re-run days that already succeeded
```

The scheduler executes backfill runs. A month in one run is faster (the ML steps run once): trigger with `start_date`/`end_date` params as above.

## 3. Idempotency

Every task can run again for the same interval with the same result:

| Task | Why a rerun is safe |
| --- | --- |
| checks, validation, data quality | Read only |
| `prepare_warehouse` | Migrations are repeatable files |
| `run_etl` | Window upsert under the advisory lock `logistics_dwh.core_etl` in one transaction; unchanged rows are not rewritten. A killed attempt rolls back and leaves a `failed` batch row |
| `refresh_marts` | Skips when the inputs are unchanged; otherwise a full rebuild in one transaction |
| `generate_features` | Deterministic Parquet snapshot, overwritten |
| `train_or_load_model` | The version is a hash of the training data, features, split, config, and CatBoost version. If that version is registered and its artifacts match the registry checksum, the model is loaded; otherwise it is trained, deterministically, to the same version |
| `run_batch_inference` | Upsert keyed by deal and model version; `rows_unchanged` on a rerun |
| `publish_pipeline_statistics` | `run_id` is a UUID5 of `dag_id/run_id`. A retried or cleared run upserts the same `audit.pipeline_run` row and overwrites the same report file |

## 4. Retries, timeouts, and failures

The step's exit code decides the Airflow outcome:

| Exit code | Meaning | Airflow |
| --- | --- | --- |
| 0 | Success | Result in XCom |
| 75 | Transient: connection lost, server restart, serialization failure, deadlock, database not ready (`orchestration.dag.is_transient`) | `AirflowException`: retried after `PIPELINE_AIRFLOW_RETRY_DELAY_S` (default 60 s), doubling up to 15 min, at most `PIPELINE_AIRFLOW_RETRIES` times (default 3) |
| 1 | Permanent: data quality failure, integrity failure, bad credentials, missing privilege, a bug | `AirflowFailException`: fails at once, no retry |
| 2 | Usage or configuration error (bad window, `AS_OF_DATE` mismatch) | `AirflowFailException` |

A failed attempt also pushes a `failure` XCom with the error type, message, attempt, and exit code.

**Timeouts.** Each task has an `execution_timeout` (table above), and a run has `dagrun_timeout = 4 h`. When Airflow interrupts a task, the step's process group gets SIGTERM, then SIGKILL after 30 s. The step's open transaction rolls back.

**Failed runs are still recorded.** `publish_pipeline_statistics` has trigger rule `all_done`. It runs after a failure, writes the report with the failed and not-run tasks, and then fails itself. The DAG run therefore ends `failed`, not `success`.

## 5. Run statistics

Each DAG run produces:

- a row in `audit.pipeline_run` with `mode = 'airflow'`, the window, status, failed task, error message, per-task status and attempts, and the full report. The report holds counts, model metrics, integrity results, logical fingerprints of core, mart, predictions, and registry, a comparison with the previous successful run, and an `airflow` section with `dag_id`, `run_id`, `run_type`, logical date, data interval, and params;
- `artifacts/reports/pipeline_run_<start>_<id>.json` and `latest.json` in the `pipeline_artifacts` volume;
- the XCom of every task, visible in the Airflow UI.

```sql
SELECT report -> 'airflow' ->> 'run_id' AS airflow_run, start_date, end_date, status, failed_task,
       report -> 'counts' ->> 'core_dwh_deals' AS core_deals, report -> 'previous_run' -> 'comparison' AS vs_previous
FROM audit.pipeline_run WHERE mode = 'airflow' ORDER BY started_at DESC LIMIT 10;
```

## 6. Deployment with Docker Compose

Services in the `airflow` profile:

| Service | Image | Role |
| --- | --- | --- |
| `airflow-postgres` | `postgres:17` | Airflow metadata database (`AIRFLOW_DB_*`), separate from the warehouse server |
| `airflow-init` | `logistics-airflow:local` | `airflow db migrate`, then exits |
| `airflow-apiserver` | same | UI, REST API, and the execution API tasks report to; `127.0.0.1:18080` |
| `airflow-scheduler` | same | Schedules runs and executes tasks (LocalExecutor); mounts `pipeline_artifacts` and `ml_models` |
| `airflow-dag-processor` | same | Parses `dags/` |

There is no triggerer: the DAG has no deferrable tasks. The image is `apache/airflow:3.4.0-python3.12` plus `/opt/pipeline/venv`, built from `requirements.txt` and `constraints.txt`, plus the platform code and `dags/`. Airflow containers run as uid 1000 (`AIRFLOW_UID`), the same as the `pipeline` image, so both write the shared artifact volumes.

```bash
docker compose up -d postgres                          # warehouse; the first start generates the snapshot
docker compose --profile airflow up -d --build         # metadata DB, migrate, API server, scheduler, DAG processor
docker compose exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated   # admin password
# UI: http://127.0.0.1:18080  (user admin)

docker compose exec airflow-scheduler airflow dags unpause smart_logistics_data_pipeline
docker compose exec airflow-scheduler airflow dags trigger smart_logistics_data_pipeline \
  --conf '{"start_date": "2026-09-01", "end_date": "2026-09-30"}'
docker compose exec airflow-scheduler airflow backfill create --dag-id smart_logistics_data_pipeline \
  --from-date 2026-09-01 --to-date 2026-09-07 --max-active-runs 1
docker compose exec airflow-scheduler airflow dags list-runs smart_logistics_data_pipeline
docker compose logs -f airflow-scheduler

docker compose --profile airflow down                  # stop Airflow; volumes stay
```

A new DAG is paused (`DAGS_ARE_PAUSED_AT_CREATION`). Unpausing it creates the run for the latest complete day; after that, one run per day. After a code change, rebuild with `docker compose --profile airflow build`, then `up -d`.

On a fresh volume, run the full load once: `docker compose up --build --exit-code-from pipeline`, or a manual DAG run with `start_date` 2023-10-01 and `end_date` 2027-02-13. A daily window loads only its own day, and the integrity checks need populated tables.

**Configuration** (`.env.example`; override in `.env`):

| Variable | Meaning |
| --- | --- |
| `AIRFLOW_DB_NAME`, `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD` | Metadata database. The entrypoint builds `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` from them, URL-encoded |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Signs task tokens between the API server and the scheduler. Replace it outside local development |
| `AIRFLOW_PUBLISH_PORT` | Host port of the UI, bound to `127.0.0.1` (default 18080). Read at compose time from the shell or `.env` |
| `AIRFLOW_UID` | Container uid (default 1000). Read at compose time |
| `PIPELINE_AIRFLOW_RETRIES`, `PIPELINE_AIRFLOW_RETRY_DELAY_S` | Default retries and first retry delay per task |
| `SOURCE_DB_*`, `DWH_DB_*`, `AS_OF_DATE`, `PIPELINE_DB_WAIT_*`, ... | Passed through to the steps, as for the `pipeline` service |

**Credentials** stay in environment variables. The DAG files contain none. Before starting a step, `pipeline_step.child_environment` removes `POSTGRES_PASSWORD`, `AIRFLOW_DB_PASSWORD`, and every `AIRFLOW__*` variable from the step's environment. The Airflow entrypoint unsets the warehouse superuser password. Users come from the simple auth manager (`admin`, with a generated password); use a real auth manager for shared deployments.

**Do not run the `pipeline` container and the DAG at the same time.** The ETL and scoring serialize on an advisory lock, but two runs of the mart refresh or of training would still race.

## 7. Running Airflow without Docker

This is how the DAG was tested. Airflow goes in its own virtualenv with the official constraints; the platform keeps `.venv`.

```bash
python3.11 -m venv .venv-airflow
.venv-airflow/bin/pip install "apache-airflow[postgres]==3.4.0" \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt

set -a && source .env.example && set +a; unset POSTGRES_PASSWORD
export POSTGRES_HOST=localhost POSTGRES_PORT=15432          # the warehouse server
export AIRFLOW_HOME=$PWD/.airflow-home AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:...@localhost:5432/airflow
export AIRFLOW__API__PORT=18080 AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:18080/execution/
export PIPELINE_PYTHON=$PWD/.venv/bin/python PIPELINE_HOME=$PWD
.venv-airflow/bin/airflow db migrate
.venv-airflow/bin/airflow api-server &  .venv-airflow/bin/airflow scheduler &  .venv-airflow/bin/airflow dag-processor &
```

## 8. Tests

```bash
pytest -q tests/test_airflow_steps.py tests/test_airflow_dag.py             # platform venv: Airflow-only tests skip
.venv-airflow/bin/python -m pytest -q -o addopts="" tests/test_airflow_dag.py   # with Airflow: all DAG tests run
```

| Area | Tests |
| --- | --- |
| Dependency order | Exact upstream set of every task; topological order |
| Schedule and policy | `CronDataIntervalTimetable`, `catchup=False`, `max_active_runs=1`, `dagrun_timeout`, params; every task has retries, an `execution_timeout` of at most 1 h, and exponential backoff; only the statistics task is `all_done` |
| Windows | Daily and monthly intervals, zero-length intervals, non-UTC timestamps, param override, invalid params |
| Retry semantics | Exit 75 raises a retryable `AirflowException`; exit 1 and 2 raise `AirflowFailException`; the `failure` XCom; a task interrupted by a timeout kills the step's process |
| Steps | Exit code per error class; `AS_OF_DATE` mismatch; inverted window; model version required; UUID5 run identity; mapping of Airflow states to the report; publishing the same run twice keeps one row (integration) |

**End-to-end results** on Airflow 3.4.0 (API server, scheduler with LocalExecutor, DAG processor; metadata in PostgreSQL 17.11), against the clean 300,000-deal warehouse:

| Scenario | How | Result |
| --- | --- | --- |
| Normal execution | Manual run with a logical date: interval [2026-11-07, 2026-11-08) | All 11 tasks succeeded in 22.2 s. The window was 2026-11-07 (52 deals, all unchanged). The registered model `20261026-8f32ec6d6901` was loaded, not retrained; 10 of 10 integrity checks passed |
| Manual run without a window | `airflow dags trigger` with no logical date and no params | `check_source_connectivity` failed at once, with no retry ("no data interval: trigger it with a logical date, or set params start_date and end_date"). Nothing ran against the databases |
| Historical backfill | `backfill create` 2026-09-01..03 | 3 runs, one at a time, intervals [D, D+1). Windows of exactly one day: 423, 457, 450 deals extracted, matching the source counts per day; 1 quarantined on 2026-09-02 |
| Repeated execution | The same backfill with `--reprocess-behavior completed` | Same run ids, second try. Every deal `unchanged`; fingerprints of core, mart, predictions, and registry unchanged; still one `audit.pipeline_run` row per run |
| Transient failure and retry | `pg_terminate_backend` on the ETL's connections while `run_etl` ran (September window) | Attempt 1 exited 75 (`the connection is lost`), and its batch was marked `failed` with nothing committed. Attempt 2 started 19 s later and succeeded: 13,767 extracted, 13,756 accepted, 11 quarantined. The run succeeded |
| Permanent failure | Scheduler started with a wrong `SOURCE_DB_PASSWORD` | `check_source_connectivity` failed on attempt 1 with no retry (5 allowed). The other 9 tasks were `upstream_failed`. The statistics task recorded `failed` / `check_source_connectivity` / the password error in `audit.pipeline_run`, then failed the run |
| Dependency order | Metadata query over all runs | 77 dependency edges in 7 runs: no task started before every upstream task had ended |

The first runs used `schedule="@daily"`. They showed the zero-length interval problem in section 2, which the `CronDataIntervalTimetable` fixed. All the results above are from the final DAG, except the dependency-order query, which also covers the two earlier runs.

## 9. Limitations

- The Airflow image and the compose services were never started (no Docker on the development host). `docker compose --profile airflow config` passes, and the same DAG, steps, and Airflow version ran outside Docker as described above.
- LocalExecutor on one node. More workers would need the Celery or Kubernetes executor; the steps would not change.
- Every daily run rebuilds the feature set and rescores the fixed test and future sets (about 6 s), because the ML split is tied to the snapshot, not to the interval. A rolling as-of date would make these steps interval-specific.
- The window is on `loading_date`. A source change to a deal outside the interval is picked up only when its day is reprocessed (README section 25).
- Simple auth manager and no Fernet key. No Airflow connections or variables hold secrets, but a shared deployment needs a real auth manager.
