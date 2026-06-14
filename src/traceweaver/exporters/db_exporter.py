"""Database exporter for ``--output db``.

Builds and owns its own schema via ``MetaData.create_all`` (idempotent,
create-if-not-exists) using SQLAlchemy Core, so the same code targets
PostgreSQL in production and SQLite in tests. The schema mirrors
``database/migrations/001_create_lineage_tables.sql`` — that file is the
equivalent hand-maintained DDL for applying with ``psql`` or a Postgres
``docker-entrypoint-initdb.d`` mount; keep the two in sync.

Semantics: the four structured tables (jobs, datasets, edges, task
dependencies, function calls) hold the *current* snapshot and are refreshed on
each scan, while ``raw_scan_results`` is append-only so prior payloads remain
available for debugging and reprocessing.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from traceweaver.models import ScanResult

try:  # pragma: no cover - import guard
    import sqlalchemy as sa
    from sqlalchemy.dialects import postgresql

    _HAS_SQLALCHEMY = True
except Exception:  # pragma: no cover - optional dependency
    sa = None
    postgresql = None
    _HAS_SQLALCHEMY = False


def sqlalchemy_available() -> bool:
    return _HAS_SQLALCHEMY


def normalize_database_url(url: str) -> str:
    """Pin bare PostgreSQL URLs to the installed psycopg (v3) driver."""
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    return url


def _utcnow() -> datetime:
    # Naive UTC, to match the migration's `TIMESTAMP` (without time zone) column.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _build_metadata():
    metadata = sa.MetaData()
    json_type = sa.JSON().with_variant(postgresql.JSONB, "postgresql")
    # BIGSERIAL on PostgreSQL; INTEGER PRIMARY KEY (rowid alias) on SQLite.
    pk_type = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

    def pk():
        return sa.Column("id", pk_type, primary_key=True, autoincrement=True)

    def created_at():
        # TIMESTAMP (without time zone) to match the SQL migration; value is
        # supplied client-side as naive UTC.
        return sa.Column("created_at", sa.DateTime, default=_utcnow)

    jobs = sa.Table(
        "lineage_jobs",
        metadata,
        pk(),
        sa.Column("dag_id", sa.Text, nullable=False),
        sa.Column("task_id", sa.Text, nullable=False),
        sa.Column("operator_class", sa.Text),
        sa.Column("callable_path", sa.Text),
        sa.Column("file_path", sa.Text),
        sa.Column("line_no", sa.Integer),
        created_at(),
    )
    datasets = sa.Table(
        "lineage_datasets",
        metadata,
        pk(),
        sa.Column("dataset_id", sa.Text, unique=True, nullable=False),
        sa.Column("namespace", sa.Text),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("dataset_type", sa.Text),
        sa.Column("uri", sa.Text),
        sa.Column("schema_name", sa.Text),
        sa.Column("table_name", sa.Text),
        created_at(),
    )
    edges = sa.Table(
        "lineage_edges",
        metadata,
        pk(),
        sa.Column("dag_id", sa.Text, nullable=False),
        sa.Column("task_id", sa.Text, nullable=False),
        sa.Column("source_dataset", sa.Text),
        sa.Column("target_dataset", sa.Text),
        sa.Column("extraction_method", sa.Text, nullable=False),
        sa.Column("confidence", sa.Text, nullable=False),
        created_at(),
    )
    task_deps = sa.Table(
        "task_dependencies",
        metadata,
        pk(),
        sa.Column("dag_id", sa.Text, nullable=False),
        sa.Column("upstream_task_id", sa.Text, nullable=False),
        sa.Column("downstream_task_id", sa.Text, nullable=False),
        sa.Column("extraction_method", sa.Text, nullable=False, default="static_ast"),
        sa.Column("confidence", sa.Text, nullable=False, default="high"),
        created_at(),
    )
    function_calls = sa.Table(
        "function_calls",
        metadata,
        pk(),
        sa.Column("dag_id", sa.Text),
        sa.Column("task_id", sa.Text),
        sa.Column("module", sa.Text),
        sa.Column("function_name", sa.Text, nullable=False),
        sa.Column("caller_function", sa.Text),
        sa.Column("file_path", sa.Text, nullable=False),
        sa.Column("line_no", sa.Integer),
        sa.Column("method", sa.Text, default="static_ast"),
        created_at(),
    )
    raw_results = sa.Table(
        "raw_scan_results",
        metadata,
        pk(),
        sa.Column("repo_path", sa.Text),
        sa.Column("payload_json", json_type),
        created_at(),
    )
    return metadata, {
        "jobs": jobs,
        "datasets": datasets,
        "edges": edges,
        "task_dependencies": task_deps,
        "function_calls": function_calls,
        "raw_scan_results": raw_results,
    }


class DbExporter:
    def __init__(self, database_url: str) -> None:
        if not _HAS_SQLALCHEMY:
            raise RuntimeError(
                "Database export requires the 'db' extra. Install with: "
                "pip install 'traceweaver[db]'"
            )
        self.database_url = normalize_database_url(database_url)

    def export(self, result: ScanResult, append: bool = False) -> None:
        engine = sa.create_engine(self.database_url)
        metadata, tables = _build_metadata()
        try:
            metadata.create_all(engine)
            with engine.begin() as conn:
                if not append:
                    for key in (
                        "jobs",
                        "datasets",
                        "edges",
                        "task_dependencies",
                        "function_calls",
                    ):
                        conn.execute(sa.delete(tables[key]))
                self._insert_rows(conn, tables, result, append=append)
        finally:
            engine.dispose()

    def _insert_rows(self, conn, tables, result: ScanResult, append: bool) -> None:
        if result.jobs:
            conn.execute(
                sa.insert(tables["jobs"]), [asdict(job) for job in result.jobs]
            )

        dataset_rows = []
        for dataset in result.datasets:
            row = asdict(dataset)
            row["dataset_id"] = dataset.dataset_id
            dataset_rows.append(row)
        if dataset_rows:
            self._insert_datasets(conn, tables["datasets"], dataset_rows, append=append)

        if result.edges:
            conn.execute(sa.insert(tables["edges"]), [asdict(e) for e in result.edges])
        if result.task_dependencies:
            conn.execute(
                sa.insert(tables["task_dependencies"]),
                [asdict(t) for t in result.task_dependencies],
            )
        if result.function_calls:
            conn.execute(
                sa.insert(tables["function_calls"]),
                [asdict(c) for c in result.function_calls],
            )

        conn.execute(
            sa.insert(tables["raw_scan_results"]),
            {"repo_path": result.repo_path, "payload_json": result.to_dict()},
        )

    def _insert_datasets(self, conn, table, rows, append: bool) -> None:
        if append:
            # Keep only dataset_ids not already present to respect the unique key.
            existing = set(conn.execute(sa.select(table.c.dataset_id)).scalars())
            rows = [r for r in rows if r["dataset_id"] not in existing]
        if rows:
            conn.execute(sa.insert(table), rows)
