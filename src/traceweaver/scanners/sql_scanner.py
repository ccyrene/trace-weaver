"""Regex-based SQL table extraction.

This is the dependency-free fallback used when ``sqlglot`` is not installed or
cannot parse a statement. It is intentionally conservative: it recognises the
most common ``FROM`` / ``JOIN`` sources and ``INSERT`` / ``CREATE`` / ``MERGE``
targets. For richer, dialect-aware parsing see
:mod:`traceweaver.scanners.sql_lineage`.
"""

from __future__ import annotations

import re

from traceweaver import confidence
from traceweaver.models import Dataset, LineageEdge

_TABLE = r"[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){0,2}"

SOURCE_PATTERNS = [
    re.compile(rf"\bfrom\s+({_TABLE})", re.IGNORECASE),
    re.compile(rf"\bjoin\s+({_TABLE})", re.IGNORECASE),
    re.compile(rf"\busing\s+({_TABLE})", re.IGNORECASE),
]

TARGET_PATTERNS = [
    re.compile(rf"\binsert\s+into\s+({_TABLE})", re.IGNORECASE),
    re.compile(rf"\binsert\s+overwrite\s+(?:table\s+)?({_TABLE})", re.IGNORECASE),
    re.compile(
        rf"\bcreate\s+(?:or\s+replace\s+)?(?:temporary\s+)?(?:table|view)\s+(?:if\s+not\s+exists\s+)?({_TABLE})",
        re.IGNORECASE,
    ),
    re.compile(rf"\bmerge\s+into\s+({_TABLE})", re.IGNORECASE),
    re.compile(rf"\bupdate\s+({_TABLE})", re.IGNORECASE),
    re.compile(rf"\bdelete\s+from\s+({_TABLE})", re.IGNORECASE),
]

# Keywords that should never be treated as table names if they slip through.
_SQL_NOISE = {"select", "values", "from", "where", "set", "table", "as", "on", "using"}

# Strip comments and string literals before matching so keywords inside them
# are not mistaken for table names.
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
# CTE aliases (``WITH a AS (...)``, ``, b AS (...)``) are not real datasets.
_CTE_RE = re.compile(r"(?:\bwith\b|,)\s+([A-Za-z_]\w*)\s+as\s*\(", re.IGNORECASE)


def extract_tables_regex(sql: str) -> tuple[set[str], set[str]]:
    """Return ``(sources, targets)`` table-name sets using regex heuristics."""
    cleaned = _STRING_LITERAL_RE.sub(
        " '' ", _LINE_COMMENT_RE.sub(" ", _BLOCK_COMMENT_RE.sub(" ", sql))
    )
    normalized = " ".join(cleaned.replace("\n", " ").split())
    cte_names = {m.group(1).lower() for m in _CTE_RE.finditer(normalized)}

    sources = {
        n
        for n in _extract(SOURCE_PATTERNS, normalized)
        if n.lower() not in _SQL_NOISE and n.lower() not in cte_names
    }
    targets = {
        n for n in _extract(TARGET_PATTERNS, normalized) if n.lower() not in _SQL_NOISE
    }
    # A table can be matched as both (e.g. ``MERGE INTO t USING t`` or
    # ``DELETE FROM t``); a write target wins so we do not invent a self-edge.
    sources -= targets
    return sources, targets


def scan_sql(
    sql: str, dag_id: str, task_id: str
) -> tuple[list[Dataset], list[LineageEdge]]:
    """Regex SQL lineage entrypoint (medium/low confidence)."""
    sources, targets = extract_tables_regex(sql)
    return build_lineage(
        sources, targets, dag_id=dag_id, task_id=task_id, method="sql_regex"
    )


def build_lineage(
    sources: set[str],
    targets: set[str],
    dag_id: str,
    task_id: str,
    method: str,
    high_confidence: bool = False,
) -> tuple[list[Dataset], list[LineageEdge]]:
    """Turn source/target table sets into datasets and lineage edges.

    When both sources and targets exist we emit ``source --> target`` edges.
    With only one side present we emit a half-edge so the dataset is still
    attributable to the task.
    """
    datasets = [dataset_from_table(name) for name in sorted(sources | targets)]

    paired = confidence.sql_confidence(high_confidence=high_confidence, paired=True)
    half = confidence.sql_confidence(high_confidence=high_confidence, paired=False)

    edges: list[LineageEdge] = []
    if sources and targets:
        for source in sorted(sources):
            for target in sorted(targets):
                edges.append(
                    LineageEdge(dag_id, task_id, source, target, method, paired)
                )
    elif sources:
        for source in sorted(sources):
            edges.append(LineageEdge(dag_id, task_id, source, None, method, half))
    elif targets:
        for target in sorted(targets):
            edges.append(LineageEdge(dag_id, task_id, None, target, method, half))

    return datasets, edges


def _extract(patterns, sql: str) -> set[str]:
    found: set[str] = set()
    for pattern in patterns:
        for match in pattern.finditer(sql):
            found.add(match.group(1))
    return found


def dataset_from_table(name: str) -> Dataset:
    parts = name.split(".")
    schema_name = parts[-2] if len(parts) >= 2 else None
    table_name = parts[-1]
    return Dataset(
        name=name,
        dataset_type="table",
        schema_name=schema_name,
        table_name=table_name,
    )
