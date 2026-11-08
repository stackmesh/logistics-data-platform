# Smart Logistics Data & ML Platform

Stage 1 is a local PostgreSQL 17 environment. It loads the supplied operational snapshot and creates an empty analytical warehouse. ETL and ML are not in this stage.

The source files `requirements/reference/sql/schema.sql` and `requirements/reference/sql/synthetic.sql` are mounted into the database container and applied in that order. Their business logic is unchanged. The generator keeps `SELECT setseed(0.20261026)` and receives `n_prices` from `N_PRICES` (default `300000`).

## Prerequisites

Docker Engine and Docker Compose v2.

## Start

```bash
docker compose up --build
```

The first start builds the Python image, creates a data volume, applies the source schema, generates 300,000 deals, then creates the warehouse. Generation usually takes several minutes. Later starts reuse the volume and do not generate the snapshot again.

`postgres` stays up. `pipeline` runs the connectivity smoke test once and exits 0 when the checks pass. An exited pipeline container after a zero status is the expected end of stage 1.

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

The script is safe to run again. Schema checks:

```bash
pytest tests/test_core_schema.py -q
```

## Reset

Initialization runs only on an empty data volume.

```bash
docker compose down -v
docker compose up --build
```

`down -v` deletes the generated snapshot.
