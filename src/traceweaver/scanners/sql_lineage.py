"""SQL lineage extraction.

Prefers :mod:`sqlglot` for dialect-aware, parse-tree based table extraction and
falls back to the regex scanner in :mod:`traceweaver.scanners.sql_scanner` when
sqlglot is not installed or cannot parse a statement.
"""

from __future__ import annotations

from traceweaver.models import Dataset, LineageEdge
from traceweaver.scanners import sql_scanner

try:  # pragma: no cover - import guard
    import sqlglot
    from sqlglot import exp

    _HAS_SQLGLOT = True
except Exception:  # pragma: no cover - sqlglot is optional
    sqlglot = None
    exp = None
    _HAS_SQLGLOT = False


# Map an operator class (or hook) to a sqlglot dialect name.
_OPERATOR_DIALECTS: dict[str, str] = {
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "mysql": "mysql",
    "mssql": "tsql",
    "sqlserver": "tsql",
    "oracle": "oracle",
    "databricks": "databricks",
    "spark": "spark",
    "hive": "hive",
    "trino": "trino",
    "presto": "presto",
    "duckdb": "duckdb",
    "athena": "athena",
    "clickhouse": "clickhouse",
}


def sqlglot_available() -> bool:
    return _HAS_SQLGLOT


def dialect_for_operator(operator_class: str | None) -> str | None:
    """Best-effort sqlglot dialect from an operator/hook class name."""
    if not operator_class:
        return None
    lowered = operator_class.lower()
    for needle, dialect in _OPERATOR_DIALECTS.items():
        if needle in lowered:
            return dialect
    return None


def extract_sql_lineage(
    sql: str,
    dag_id: str,
    task_id: str,
    dialect: str | None = None,
) -> tuple[list[Dataset], list[LineageEdge], str]:
    """Return ``(datasets, edges, backend)`` for a single SQL statement string.

    ``backend`` is ``"sqlglot"`` when the parse-tree path succeeded, otherwise
    ``"sql_regex"``.
    """
    per_statement = _extract_tables_sqlglot(sql, dialect) if _HAS_SQLGLOT else None
    if per_statement is not None:
        datasets: list[Dataset] = []
        edges: list[LineageEdge] = []
        # Build lineage per statement so a multi-statement script does not
        # cross-product sources and targets that belong to different statements.
        for sources, targets in per_statement:
            ds, ed = sql_scanner.build_lineage(
                sources,
                targets,
                dag_id=dag_id,
                task_id=task_id,
                method="sqlglot",
                high_confidence=True,
            )
            datasets.extend(ds)
            edges.extend(ed)
        return datasets, edges, "sqlglot"

    datasets, edges = sql_scanner.scan_sql(sql, dag_id=dag_id, task_id=task_id)
    return datasets, edges, "sql_regex"


def _extract_tables_sqlglot(
    sql: str, dialect: str | None
) -> list[tuple[set[str], set[str]]] | None:
    """Parse with sqlglot. Returns per-statement ``(sources, targets)`` or ``None``."""
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except Exception:
        return None

    statements = [stmt for stmt in statements if stmt is not None]
    if not statements:
        return None

    per_statement: list[tuple[set[str], set[str]]] = []
    for statement in statements:
        # DROP / TRUNCATE / CREATE INDEX etc. have no read/write data lineage;
        # skip them so the affected table is not reported as a phantom source.
        if _is_non_lineage_statement(statement):
            continue
        cte_names = {cte.alias for cte in statement.find_all(exp.CTE) if cte.alias}
        targets = _statement_targets(statement)
        sources: set[str] = set()

        for table in statement.find_all(exp.Table):
            name = _format_table(table)
            if not name:
                continue
            # Skip references to CTE names (they are not real datasets).
            if table.name in cte_names and not table.db:
                continue
            if name in targets:
                continue
            sources.add(name)

        sources -= targets
        if sources or targets:
            per_statement.append((sources, targets))

    return per_statement or [(set(), set())]


def _is_non_lineage_statement(statement) -> bool:
    """True for DDL/maintenance statements that carry no data lineage."""
    if isinstance(statement, (exp.Drop, exp.TruncateTable)):
        return True
    if isinstance(statement, exp.Create):
        kind = (statement.args.get("kind") or "").upper()
        return kind not in ("TABLE", "VIEW", "")
    return False


def _statement_targets(statement) -> set[str]:
    """Extract write targets (INSERT/CREATE/MERGE/UPDATE/DELETE) from one statement."""
    targets: set[str] = set()

    # The mutated table is the write target; any USING/subquery tables remain
    # sources because they are not added here.
    for kind in (exp.Insert, exp.Merge, exp.Update, exp.Delete):
        if isinstance(statement, kind):
            name = _first_table_name(statement.this)
            if name:
                targets.add(name)

    # CREATE TABLE/VIEW ... AS — the created object is the write target.
    if isinstance(statement, exp.Create):
        kind = (statement.args.get("kind") or "").upper()
        if kind in ("TABLE", "VIEW", ""):
            name = _first_table_name(statement.this)
            if name:
                targets.add(name)

    return targets


def _first_table_name(node) -> str | None:
    if node is None:
        return None
    if isinstance(node, exp.Table):
        return _format_table(node)
    found = node.find(exp.Table)
    return _format_table(found) if found is not None else None


def _format_table(table) -> str:
    parts = [part for part in (table.catalog, table.db, table.name) if part]
    return ".".join(parts)
