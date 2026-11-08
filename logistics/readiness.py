"""Connectivity and readiness checks for the stage 1 databases."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from logistics.settings import DatabaseTarget, Settings, SettingsError, load_settings

SOURCE_TABLES = (
    "migrations",
    "cities",
    "body_types",
    "exchange_rates",
    "price_group_stats",
    "prices",
    "price_body_types",
    "price_dates",
    "price_participants",
    "selected_directions",
    "favorite_directions",
)

WAREHOUSE_SCHEMAS = ("raw", "core", "mart", "ml", "audit")
ML_TABLES = ("ml.ml_model_registry", "ml.ml_price_predictions")

COUNT_TABLES = (
    "cities",
    "body_types",
    "exchange_rates",
    "prices",
    "price_body_types",
    "price_dates",
    "price_group_stats",
    "price_participants",
    "selected_directions",
    "favorite_directions",
)

_DENIAL_MARKERS = (
    "permission denied",
    "not permitted",
    "pg_hba",
    "authentication failed",
    "password authentication",
)


@dataclass
class Report:
    lines: list[str] = field(default_factory=list)
    failed: bool = False

    def add(self, ok: bool, message: str) -> None:
        line = f"[smoke] {'PASS' if ok else 'FAIL'} {message}"
        print(line, flush=True)
        self.lines.append(line)
        self.failed = self.failed or not ok

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


def _engine(settings: Settings, target: DatabaseTarget) -> Engine:
    return create_engine(
        target.url(settings.host, settings.port),
        poolclass=NullPool,
        **settings.engine_kwargs(),
    )


def _safe_db_error(exc: OperationalError) -> str:
    orig = getattr(exc, "orig", None)
    message = " ".join(str(orig or exc.__class__.__name__).split())
    return message[:240]


def _version(conn: Connection) -> tuple[bool, str]:
    row = conn.execute(
        text(
            "SELECT current_setting('server_version') AS server_version, "
            "current_setting('server_version_num')::int AS server_version_num"
        )
    ).one()
    number = int(row.server_version_num)
    return 170000 <= number < 180000, str(row.server_version)


def _access_denied(settings: Settings, database: str, user: str, password: str) -> tuple[bool, str]:
    target = DatabaseTarget(name=database, user=user, password=password)
    engine = _engine(settings, target)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return False, f"{user} was allowed to connect to {database}"
    except OperationalError as exc:
        message = str(exc).lower()
        denied = any(marker in message for marker in _DENIAL_MARKERS)
        detail = "access denied" if denied else f"unexpected connection error: {exc.__class__.__name__}"
        return denied, f"{user} -> {database}: {detail}"
    finally:
        engine.dispose()


def run_checks(settings: Settings | None = None) -> Report:
    report = Report()
    try:
        resolved = settings if settings is not None else load_settings()
    except SettingsError as exc:
        report.add(False, str(exc))
        return report

    source_engine = _engine(resolved, resolved.source)
    warehouse_engine = _engine(resolved, resolved.warehouse)
    try:
        try:
            source_conn = source_engine.connect()
        except OperationalError as exc:
            report.add(
                False,
                f"source connectivity {resolved.source.user}@{resolved.source.name} "
                f"on {resolved.host}:{resolved.port}: {_safe_db_error(exc)}",
            )
            return report
        try:
            warehouse_conn = warehouse_engine.connect()
        except OperationalError as exc:
            source_conn.close()
            report.add(
                False,
                f"warehouse connectivity {resolved.warehouse.user}@{resolved.warehouse.name} "
                f"on {resolved.host}:{resolved.port}: {_safe_db_error(exc)}",
            )
            return report

        with source_conn, warehouse_conn:
            source_ok, source_version = _version(source_conn)
            warehouse_ok, warehouse_version = _version(warehouse_conn)
            report.add(source_ok, f"source PostgreSQL version {source_version}")
            report.add(warehouse_ok, f"warehouse PostgreSQL version {warehouse_version}")
            report.add(source_version == warehouse_version, f"server version matches ({source_version})")
            report.add(
                True,
                f"source connectivity {resolved.source.user}@{resolved.source.name} "
                f"host={resolved.host} port={resolved.port}",
            )
            report.add(
                True,
                f"warehouse connectivity {resolved.warehouse.user}@{resolved.warehouse.name} "
                f"host={resolved.host} port={resolved.port}",
            )

            source_super = bool(
                source_conn.execute(text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")).scalar_one()
            )
            warehouse_super = bool(
                warehouse_conn.execute(
                    text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
                ).scalar_one()
            )
            report.add(not source_super, f"source role {resolved.source.user} is not a superuser")
            report.add(not warehouse_super, f"warehouse role {resolved.warehouse.user} is not a superuser")

            present_tables = set(
                source_conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
                    )
                ).scalars()
            )
            missing_tables = [name for name in SOURCE_TABLES if name not in present_tables]
            report.add(
                not missing_tables,
                "source tables present: " + ", ".join(SOURCE_TABLES)
                if not missing_tables
                else "source tables missing: " + ", ".join(missing_tables),
            )

            counts: dict[str, int] = {}
            for table_name in COUNT_TABLES:
                if table_name not in present_tables:
                    continue
                counts[table_name] = int(
                    source_conn.execute(text(f"SELECT COUNT(*) FROM public.{table_name}")).scalar_one()
                )
                report.add(True, f"source {table_name} rows={counts[table_name]}")

            prices = counts.get("prices")
            report.add(prices == resolved.n_prices, f"source prices {prices} expected {resolved.n_prices}")
            report.add(
                counts.get("cities") == 42,
                f"source cities {counts.get('cities')} expected 42 from the reference generator",
            )
            report.add(
                counts.get("body_types") == 19,
                f"source body_types {counts.get('body_types')} expected 19 from the reference generator",
            )
            dates = counts.get("price_dates", 0)
            report.add(
                prices is not None and prices > 0 and dates >= prices,
                f"source price_dates {dates} covers {prices} prices",
            )

            kazan = None
            if "cities" in present_tables:
                kazan = source_conn.execute(
                    text("SELECT full_name_local FROM cities WHERE gc_id = 1003 ORDER BY id LIMIT 1")
                ).scalar_one_or_none()
            report.add(kazan == "Казань, Республика Татарстан", f"first Kazan row is {kazan!r}")

            present_schemas = set(
                warehouse_conn.execute(
                    text("SELECT schema_name FROM information_schema.schemata")
                ).scalars()
            )
            missing_schemas = [name for name in WAREHOUSE_SCHEMAS if name not in present_schemas]
            report.add(
                not missing_schemas,
                "warehouse schemas present: " + ", ".join(WAREHOUSE_SCHEMAS)
                if not missing_schemas
                else "warehouse schemas missing: " + ", ".join(missing_schemas),
            )
            for schema_name in WAREHOUSE_SCHEMAS:
                if schema_name not in present_schemas:
                    continue
                can_create = bool(
                    warehouse_conn.execute(
                        text("SELECT has_schema_privilege(current_user, :schema_name, 'CREATE')"),
                        {"schema_name": schema_name},
                    ).scalar_one()
                )
                report.add(can_create, f"warehouse role can create objects in {schema_name}")

            bootstrap = warehouse_conn.execute(
                text(
                    "SELECT source_database, source_price_count, n_prices_requested, server_version "
                    "FROM audit.bootstrap WHERE id = 1"
                )
            ).mappings().one_or_none()
            if bootstrap is None:
                report.add(False, "audit.bootstrap has no readiness row")
            else:
                report.add(
                    bootstrap["source_database"] == resolved.source.name
                    and int(bootstrap["source_price_count"]) == resolved.n_prices
                    and int(bootstrap["n_prices_requested"]) == resolved.n_prices
                    and int(bootstrap["source_price_count"]) == prices,
                    "warehouse bootstrap "
                    f"source={bootstrap['source_database']} prices={bootstrap['source_price_count']} "
                    f"requested={bootstrap['n_prices_requested']} server={bootstrap['server_version']}",
                )

            relations = warehouse_conn.execute(
                text(
                    "SELECT n.nspname AS schema_name, c.relname AS table_name "
                    "FROM pg_class AS c "
                    "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                    "WHERE n.nspname IN ('raw', 'core', 'mart', 'ml', 'audit') "
                    "AND c.relkind = 'r' "
                    "ORDER BY 1, 2"
                )
            ).mappings()
            relation_list = [f"{row['schema_name']}.{row['table_name']}" for row in relations]
            unexpected = [name for name in relation_list if name.startswith("ml.") and name not in ML_TABLES]
            report.add("audit.bootstrap" in relation_list, "audit.bootstrap exists")
            report.add(
                not unexpected,
                "ml schema holds only model tables"
                if not unexpected
                else "unexpected ml tables: " + ", ".join(unexpected),
            )

        source_denied, source_denied_detail = _access_denied(
            resolved,
            resolved.warehouse.name,
            resolved.source.user,
            resolved.source.password,
        )
        warehouse_denied, warehouse_denied_detail = _access_denied(
            resolved,
            resolved.source.name,
            resolved.warehouse.user,
            resolved.warehouse.password,
        )
        report.add(source_denied, source_denied_detail)
        report.add(warehouse_denied, warehouse_denied_detail)
    finally:
        source_engine.dispose()
        warehouse_engine.dispose()

    report.add(report.ok, "stage 1 readiness")
    return report
