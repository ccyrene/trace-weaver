"""Coarse, operation-level data-access detection.

When a DAG does I/O through library calls (pandas, boto3, SQLAlchemy, Airflow
hooks) instead of operator keyword arguments, the exact path/table is usually
built at runtime and cannot be resolved statically. We can still tell the KIND
of source/sink from the call itself — e.g. ``df.to_parquet("s3://...")`` writes
parquet to S3 — and emit a coarse dataset so the lineage graph shows the
direction of data flow even when the precise name is unknown.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Callable
from dataclasses import dataclass

READ = "read"
WRITE = "write"

# Final component of the dotted call name -> (default dataset_type, direction,
# format-or-None). Matched on the last attribute, so ``pd.read_csv`` and
# ``df.to_parquet`` both hit by their method name.
_IO_CALLS: dict[str, tuple[str, str, str | None]] = {
    # pandas (and similar) readers / writers
    "read_csv": ("file", READ, "csv"),
    "read_parquet": ("file", READ, "parquet"),
    "read_json": ("file", READ, "json"),
    "read_excel": ("file", READ, "excel"),
    "read_orc": ("file", READ, "orc"),
    "read_sql": ("table", READ, None),
    "read_sql_table": ("table", READ, None),
    "read_sql_query": ("table", READ, None),
    "to_csv": ("file", WRITE, "csv"),
    "to_parquet": ("file", WRITE, "parquet"),
    "to_json": ("file", WRITE, "json"),
    "to_excel": ("file", WRITE, "excel"),
    "to_orc": ("file", WRITE, "orc"),
    "to_sql": ("table", WRITE, None),
    # boto3 S3 client/resource
    "get_object": ("s3", READ, None),
    "download_file": ("s3", READ, None),
    "download_fileobj": ("s3", READ, None),
    "list_objects": ("s3", READ, None),
    "list_objects_v2": ("s3", READ, None),
    "put_object": ("s3", WRITE, None),
    "upload_file": ("s3", WRITE, None),
    "upload_fileobj": ("s3", WRITE, None),
    # connections / engines
    "get_connection": ("connection", READ, None),
    "create_engine": ("connection", READ, None),
}

# URI scheme found in a (possibly partial) static string -> dataset_type.
_SCHEME_TYPES = {
    "s3": "s3",
    "s3a": "s3",
    "s3n": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "wasb": "azure_blob",
    "wasbs": "azure_blob",
    "abfs": "azure_blob",
    "abfss": "azure_blob",
    "hdfs": "hdfs",
}
_SCHEME_RE = re.compile(r"^([a-z0-9+]+)://", re.IGNORECASE)
_PATH_KWARGS = ("path", "path_or_buf", "filepath_or_buffer", "fname")

Resolver = Callable[[ast.AST], "str | None"]


@dataclass
class IoAccess:
    """A coarse data read/write inferred from a library call."""

    caller_function: str
    dataset_type: str
    direction: str  # READ | WRITE
    name: str


def describe_io(call: ast.Call, resolve: Resolver) -> IoAccess | None:
    """Return a coarse :class:`IoAccess` for a known I/O call, else ``None``.

    ``resolve`` maps an AST node to its static string value when possible
    (literal, module-level constant, or the static fragments of an f-string),
    else ``None``. The caller fills in ``caller_function``.
    """
    callee = _callee_last_name(call.func)
    if callee not in _IO_CALLS:
        return None
    dtype, direction, fmt = _IO_CALLS[callee]

    # df.to_sql("table", con, schema="s") — table name is usually a literal even
    # when nothing else is, so capture the exact schema.table when we can.
    if callee == "to_sql":
        table = resolve(call.args[0]) if call.args else _kwarg(call, "name", resolve)
        schema = _kwarg(call, "schema", resolve)
        if schema and table:
            return IoAccess("", "table", direction, f"{schema}.{table}")
        return IoAccess("", "table", direction, table or "table:write")

    candidate = _first_path(call, resolve)

    if callee in ("create_engine", "get_connection"):
        scheme = _raw_scheme(candidate)
        if scheme:
            name = scheme
        elif candidate and "://" not in candidate:
            name = candidate  # e.g. a literal conn_id passed to get_connection
        else:
            name = "connection"
        return IoAccess("", "connection", direction, name)

    scheme_type = _SCHEME_TYPES.get(_raw_scheme(candidate) or "")
    if scheme_type:
        dtype = scheme_type
    label = scheme_type or dtype
    name = f"{label}:{fmt}" if fmt else f"{label}:{direction}"
    return IoAccess("", dtype, direction, name)


def _first_path(call: ast.Call, resolve: Resolver) -> str | None:
    if call.args:
        value = resolve(call.args[0])
        if value:
            return value
    for kwname in _PATH_KWARGS:
        value = _kwarg(call, kwname, resolve)
        if value:
            return value
    return None


def _kwarg(call: ast.Call, name: str, resolve: Resolver) -> str | None:
    for kw in call.keywords:
        if kw.arg == name:
            return resolve(kw.value)
    return None


def _raw_scheme(value: str | None) -> str | None:
    if not value:
        return None
    match = _SCHEME_RE.match(value.strip())
    if not match:
        return None
    return match.group(1).split("+")[0].lower()


def _callee_last_name(func: ast.AST) -> str | None:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None
