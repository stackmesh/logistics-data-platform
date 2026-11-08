# Apache Superset — Smart Logistics — Market Rate Intelligence

One Superset 6.1 dashboard, **Smart Logistics — Market Rate Intelligence**, for logistics procurement managers.
It reads the PostgreSQL warehouse directly, through three read-only views in schema `mart`, with a login that can
read nothing else. No calculation is moved into Superset: the views rename mart and ML columns, and every metric
restates a definition the mart or the prediction mart already uses.

| File | Purpose |
|---|---|
| `sql/ml/003_bi_views.sql` | BI views `mart.v_market_observation`, `mart.v_route_pricing`, `mart.v_fact_vs_forecast`; grants to `bi_reader` |
| `sql/mart/001_market_corridor.sql` | `mart.market_corridor_observations(as_of)`: the mart's eligible observations, shared by the mart build and the BI view |
| `docker/postgres/03_bi_reader.sh` | Creates the `bi_reader` login (superuser, repeatable) |
| `superset/superset_config.py` | Superset settings; every secret from the environment |
| `superset/provision.py` | Creates or updates the database connection, datasets, metrics, charts, filters, and dashboard over the REST API |
| `superset/validate.py` | Compares every chart's data with direct PostgreSQL queries |
| `docker/superset/Dockerfile`, `docker/superset/init.sh` | Image (official image + psycopg2) and metadata initialization |
| `tests/test_superset_bi.py` | Views, role privileges, and dashboard specification tests |

## 1. Data access

```
Superset ──(bi_reader, read-only)──▶ mart.v_market_observation ──▶ mart.bi_market_observations() ──▶ mart.market_corridor_observations(as_of)
                                     mart.v_route_pricing      ──▶ mart.mart_market_corridor
                                     mart.v_fact_vs_forecast   ──▶ ml.v_fact_vs_forecast (+ core.fact_deal.weight_category)
```

- **`bi_reader`** has `CONNECT` on the warehouse, `USAGE` on schema `mart`, `SELECT` on its tables and views, and
  `EXECUTE` on `mart.bi_market_observations()`. Its sessions default to read-only transactions with a 120 s statement
  timeout. It cannot read `raw`, `core`, `ml`, or `audit`, and cannot write anything (`tests/test_superset_bi.py`).
- Views read other tables with their owner's privileges, but functions run with the caller's. Market-level
  KPIs need the deal-level observations behind the mart, which live in `core`. `mart.bi_market_observations()` is
  therefore `SECURITY DEFINER` with a fixed `search_path`, takes no arguments, and returns exactly the mart's
  population for the `as_of_date` of the current mart rows. `EXECUTE` is revoked from `PUBLIC`.
- `mart.market_corridor_observations(as_of)` is the first step of `mart.build_market_corridor`, moved into its own
  function so the mart and the BI view share one eligibility rule. The refactor does not change the mart: before
  and after, the mart has the same 203,644 rows and the same input fingerprint, and the BI view reproduces
  `trips_count`, `carriers_count`, `median_trip`, and `p25_km` of every cell (section 6).

### Datasets

| Dataset | Grain | Rows (snapshot 2026-10-26) | Used for |
|---|---|---|---|
| `mart.v_market_observation` | deal × price month × body group (the mart's observation) | 291,020 | Market Overview, Market Dynamics |
| `mart.v_route_pricing` | month × origin × destination × body group × weight category (the mart's cell) | 203,644 | Route Pricing |
| `mart.v_fact_vs_forecast` | scored deal | 17,699 (12,808 historical, 4,891 future) | Fact vs Forecast |

All three expose the filter columns `month` (`YYYY-MM`), `origin_city`, `destination_city`, `body_group`, and
`weight_category`; the two market views also have `reliability_level`. Superset datasets carry readable column
names and the saved metrics below.

## 2. Dashboard

Open `http://127.0.0.1:18088/superset/dashboard/market-rate-intelligence/`. Each tab starts with a short guide for
non-technical readers. All amounts are RUB; per-km prices are RUB per kilometre.

### Market Overview

| Chart | Definition |
|---|---|
| Total completed trips | `COUNT(DISTINCT price_id)` over the mart's observations (234,472 unfiltered) |
| Unique carriers | `COUNT(DISTINCT carrier_id)`; a carrier is the organization of a price of type 3 or 9, as in the mart's `carriers_count` (2,500) |
| Median market rate | `percentile_cont(0.5)` of `price_rub` over observations, the market-level counterpart of `median_trip` (112,086) |
| Average market rate | `AVG(price_rub)` over observations (167,806) |
| Average price per kilometer | `AVG(price_rub / distance_km)` over observations (83.2) |
| Market data reliability | share of observations whose mart cell is HIGH or MEDIUM (5.0%) |
| Monthly median market rate, Median rate by body group, Observations by reliability level | context for the KPIs |

**Trips and observations.** A deal priced on dates in two months, or valid for two body groups, is one trip but two
observations, exactly as in the mart, whose `SUM(trips_count)` is 291,020. Trip and carrier counts are distinct;
medians, averages, and the reliability share are over observations so that they agree with the mart cells.

**Reliability** is the mart's rule: HIGH with at least 30 trips, 5 carriers, and a price within 30 days; MEDIUM with
at least 10 trips; otherwise LOW_DATA. 99.5% of cells and 95% of observations are LOW_DATA in this snapshot, so the
dashboard states the share instead of hiding it.

### Route Pricing

`mart.mart_market_corridor` as a searchable, sortable, server-paginated table: month, origin city, destination city,
body group, weight category, P25/P50/P75 trip price, P25/P50/P75 per km, MoM %, YoY %, trips, carriers, reliability.
MoM % and YoY % are the mart's values (percent units: `+5.2` is +5.2%), coloured red when the route got more
expensive and green when it got cheaper. Default order: latest month first, then most trips.

Weight categories are the source's codes: L5T, 5T, 10T, FTL, G20T, OTH. When the source leaves them empty, ETL
imputes them from the weight in tonnes (G20T ≥ 30, FTL ≥ 10, 10T ≥ 5, 5T ≥ 2.5, L5T ≥ 0.5, else OTH); the source's
own boundaries are not documented, so the dashboard shows the codes with a short description, not tonne ranges.

### Market Dynamics

| Chart | Definition |
|---|---|
| Monthly median rate | median `price_rub` per price month over the filtered observations |
| Price corridor P25 / P50 / P75 | the three percentiles per month |
| Median rate, month over month | `median(m) / median(m − 1 month) − 1`, Superset time comparison on the monthly medians |
| Median rate, year over year | the same against 12 months earlier |
| Seasonality: median rate by month of year | median per year × calendar month heatmap |

These are market-level medians of whatever the filters select. The route table's MoM/YoY are per cell, from the
mart. Superset's time comparison needs a closed time range, so these charts carry the range
`2000-01-01 : 2100-01-01`, which covers all data without reading the system date.

### Fact vs Forecast

| Chart | Definition |
|---|---|
| Mean absolute error | `AVG(abs_error_rub)`, historical deals |
| Median absolute error % | median `ape_pct`, historical deals |
| Historical deals scored, Future deals predicted | counts by `record_type` |
| Model versions | deals per model, model version, and record type |
| Historical: actual vs predicted (daily average) | average actual and predicted price per loading date |
| Historical deals by absolute error band | `ape_band` of `ml.v_fact_vs_forecast` |
| Historical deals: actual vs predicted | per deal: actual, predicted, absolute error, signed error % (positive = predicted too high), model version |
| Future: predicted price (weekly average), Future deals: predicted price | future deals; no actual price column |

Historical and future records are separate charts. Future rows have no actual price or error; a price present on a
future source record is never shown as actual (`validate.py` checks the future rows carry none).

### Filters

Month, Origin city, Destination city (cascades from Origin city), Body group, Weight category, Reliability. Every
filter is multi-select with search. Option lists come from `mart.v_route_pricing`.

- **Reliability** does not apply to Fact vs Forecast; predictions have no market cell.
- **Month** does not apply to Market Dynamics, which always shows the full history: a single month would leave one
  point and no comparison. The filter bar shows it under "Filters out of scope".
- Month options are the mart's months (2023-10 to 2026-10). Future predictions reach 2027-02; on Fact vs Forecast
  they are always visible unless Month restricts them to a mart month.

## 3. Run with Docker Compose

Superset is the compose profile `superset`: a separate PostgreSQL server for Superset's metadata
(`superset-postgres`), a one-shot `superset-init` (metadata migrations, admin user, roles), the web server
`superset` on `127.0.0.1:${SUPERSET_PUBLISH_PORT:-18088}`, and a one-shot `superset-provision` that builds the
dashboard once the web server is healthy.

```bash
# 1. Warehouse and one pipeline run. The pipeline's migrate step creates the BI views and grants.
docker compose up -d postgres
docker compose run --rm pipeline

# 2. Superset
docker compose --profile superset up -d --build
docker compose --profile superset logs -f superset-provision   # ends with the dashboard URL

# 3. Open http://127.0.0.1:18088/superset/dashboard/market-rate-intelligence/
#    Sign in with SUPERSET_ADMIN_USERNAME / SUPERSET_ADMIN_PASSWORD from .env (template: .env.example).
```

`03_bi_reader.sh` runs as an init script only on a new `logistics_pgdata` volume. On an existing volume, create the
role and grant it once:

```bash
docker compose exec postgres bash /docker-entrypoint-initdb.d/03_bi_reader.sh
docker compose run --rm pipeline python -m etl.migrate
```

Re-run provisioning after changing `superset/provision.py`, or to restore the dashboard after manual edits:
`docker compose --profile superset run --rm --build superset-provision`. Charts are matched by name and updated;
the dashboard layout and filters are rewritten, so keep custom charts on a separate dashboard.

Check the dashboard against PostgreSQL from inside the compose network:

```bash
docker compose --profile superset run --rm \
  -e SUPERSET_URL=http://superset:8088 -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
  -w /app/smart_logistics superset-provision /app/.venv/bin/python validate.py
```

### Variables

All in `.env.example`; override them in `.env` for anything beyond a local machine.

| Variable | Meaning |
|---|---|
| `BI_DB_USER`, `BI_DB_PASSWORD` | warehouse login used by Superset; the role name is fixed to `bi_reader` |
| `SUPERSET_DB_NAME`, `SUPERSET_DB_USER`, `SUPERSET_DB_PASSWORD` | Superset metadata database |
| `SUPERSET_SECRET_KEY` | signs sessions and encrypts the stored warehouse password; changing it makes the stored connection unreadable |
| `SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD`, `SUPERSET_ADMIN_EMAIL` | admin user; `init.sh` resets the password to this value on every start |
| `SUPERSET_PUBLISH_PORT` | host port, bound to 127.0.0.1 |

The warehouse password reaches Superset only through the provisioning call and is stored encrypted with
`SUPERSET_SECRET_KEY`. Procurement users should get Superset accounts with the Gamma role plus access to the
"Smart Logistics DWH (mart)" database, not the admin account.

## 4. Run without Docker

This is how the dashboard was built and validated here (Docker was unavailable on the build machine).

```bash
python3.11 -m venv .venv-superset
.venv-superset/bin/pip install "apache-superset==6.1.0" psycopg2-binary "flask-caching==2.3.1" cachetools rich
```

The 6.1.0 wheel does not declare `cachetools` and `rich`, which it imports, and pip otherwise resolves
`flask-caching` 2.5.1, which breaks Superset's metastore cache. The official image ships with pinned
versions and is not affected.

```bash
export SUPERSET_CONFIG_PATH=$PWD/superset/superset_config.py SUPERSET_SECRET_KEY=<long random string>
export SUPERSET_METADATA_URI=sqlite:////tmp/superset-home/superset.db FLASK_APP=superset
.venv-superset/bin/superset db upgrade
.venv-superset/bin/superset fab create-admin --username admin --password <password> \
  --firstname Superset --lastname Admin --email admin@example.com
.venv-superset/bin/superset init
.venv-superset/bin/superset run -h 127.0.0.1 -p 18088 --with-threads

# Warehouse on the host: create bi_reader (as superuser) and grant it
PGHOST=127.0.0.1 PGPORT=<port> PGPASSWORD=<superuser password> \
  POSTGRES_USER=postgres DWH_DB_NAME=logistics_dwh BI_DB_PASSWORD=<password> bash docker/postgres/03_bi_reader.sh
python -m etl.migrate

SUPERSET_URL=http://127.0.0.1:18088 SUPERSET_ADMIN_USERNAME=admin SUPERSET_ADMIN_PASSWORD=<password> \
  BI_DB_HOST=127.0.0.1 BI_DB_PORT=<port> DWH_DB_NAME=logistics_dwh BI_DB_USER=bi_reader BI_DB_PASSWORD=<password> \
  .venv-superset/bin/python superset/provision.py
```

## 5. Validation against PostgreSQL

`superset/validate.py` loads each chart's saved query from Superset, adds the filters a dashboard filter
would add, runs it through Superset's chart data API, and compares the result with reference SQL. The reference
reads `mart.mart_market_corridor`, `mart.market_corridor_observations`, and `ml.ml_price_predictions` as the
warehouse owner, not the BI views. Numbers must agree to a relative 1e-6; row sets must match key for key.

Scenarios: no filters; month 2026-09; origin Москва with body group Тент (the largest origin and body group);
weight FTL with reliability HIGH or MEDIUM. Route pricing compares every column of every row for the three
filtered scenarios. Fact vs Forecast runs no filters, month 2026-10, Москва / Тент, and FTL.

Result on the clean warehouse (snapshot 2026-10-26, model `20261026-8f32ec6d6901`): **91 of 91 checks match**.

| Check | Superset = PostgreSQL |
|---|---|
| Observations reproduce mart cells | 203,644 of 203,644 cells, 0 mismatched |
| Overview KPIs, no filters | trips 234,472; carriers 2,500; median 112,085.955; average 167,806.0203; per km 83.2497; reliable share 0.0495 |
| Overview KPIs, month 2026-09 | trips 11,943; carriers 1,886; median 125,179.21 |
| Overview KPIs, Москва / Тент | trips 24,203; carriers 2,368; median 86,995 |
| Overview KPIs, FTL + HIGH/MEDIUM | trips 12,016; carriers 1,896; median 114,171.62; reliable share 1.0 |
| Monthly median, corridor, MoM, YoY, seasonality | 37 months each, 3 scenarios |
| Route pricing, all columns | 8,271 rows (2026-09); 6,731 (Москва / Тент); 854 (FTL + HIGH/MEDIUM) |
| Fact vs Forecast, no filters | MAE 28,190.88 RUB; median APE 12.04%; 12,808 historical; 4,891 future; 12,808 deal rows; 0 future rows with an actual |

The dashboard was also opened in a browser: all four tabs rendered every chart without errors, the filter bar
listed 37 months, 40 origin and 40 destination cities, 6 body groups, 6 weight categories, and 3 reliability
levels, and the out-of-scope filters were shown as such.

## 6. Tests

`tests/test_superset_bi.py`, part of the platform suite (17 tests):

- dashboard specification without a database: every chart uses metrics and columns that exist in its dataset;
  every filter column exists in every dataset it reaches; every chart is placed once and rows fit the 12-column
  grid; filter scopes exclude the intended tabs; historical and future forecast charts are separated; query
  contexts carry the time comparison and its closed range;
- views: `v_market_observation` reproduces trips, carriers, median, and P25 per km of every mart cell;
  `v_route_pricing` equals the mart; `v_fact_vs_forecast` has the rows of `ml.v_fact_vs_forecast` and no actuals on
  future rows; all views share the filter column names;
- `bi_reader`: reads the three views; cannot read `core` or `ml`; cannot create or delete. These skip when the role
  or `BI_DB_PASSWORD` is missing.

## 7. Limitations

- The Superset image and compose services were not started: Docker is unavailable on the build machine. The
  compose file passes `docker compose config`; the dashboard ran on the same Superset version from a virtualenv
  against the clean warehouse (section 4).
- Superset's chart cache is per process (`SimpleCache`, one hour). After a pipeline run, use "Refresh dashboard"
  or wait an hour. Use Redis for the cache when running several web workers in production.
- Overview and dynamics queries aggregate 291,020 observations through the definer function, about 1.5 s per
  query on the build machine; the cache makes repeat views instant.
- Most route cells are LOW_DATA, so many route medians rest on few trips. The table shows trips, carriers, and
  reliability next to every price; the Reliability filter narrows to HIGH and MEDIUM cells.
