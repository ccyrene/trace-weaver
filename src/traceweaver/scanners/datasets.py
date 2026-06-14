"""Pure helpers for extracting dataset candidates from string literals.

Kept dependency-free and side-effect-free so they can be unit tested in
isolation and reused by the AST scanner.
"""

from __future__ import annotations

import re

from traceweaver.models import Dataset

# Object-store / filesystem style URIs. Mapped scheme -> dataset_type.
_URI_SCHEME_TYPES: dict[str, str] = {
    "s3": "s3",
    "s3a": "s3",
    "s3n": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "wasb": "azure_blob",
    "wasbs": "azure_blob",
    "abfs": "azure_blob",
    "abfss": "azure_blob",
    "adl": "azure_datalake",
    "hdfs": "hdfs",
    "file": "file",
    "ftp": "ftp",
    "sftp": "ftp",
}

# Build a single regex that only matches the schemes we know about so we do
# not accidentally grab things like ``https://example.com`` documentation URLs.
_SCHEMES_ALT = "|".join(sorted(_URI_SCHEME_TYPES, key=len, reverse=True))
DATASET_URI_RE = re.compile(
    rf"\b(?P<scheme>{_SCHEMES_ALT})://[^\s'\"<>]+", re.IGNORECASE
)

# Bare file paths with a recognised data extension, e.g. ``/data/orders.parquet``.
FILE_HINT_RE = re.compile(
    r"[^\s'\"<>]+\.(?:csv|json|jsonl|parquet|orc|avro|tsv|xlsx?)\b", re.IGNORECASE
)

_FILE_EXT_TYPES = {
    "csv": "file",
    "tsv": "file",
    "json": "file",
    "jsonl": "file",
    "parquet": "file",
    "orc": "file",
    "avro": "file",
    "xls": "file",
    "xlsx": "file",
}


def datasets_from_text(text: str) -> list[Dataset]:
    """Return dataset candidates found inside a single string literal.

    A URI like ``s3://bucket/orders.csv`` is reported once (as its scheme
    type) rather than double-counted as both an ``s3`` dataset and a bare
    ``file`` dataset.
    """
    datasets: list[Dataset] = []
    matched_spans: list[tuple[int, int]] = []

    for match in DATASET_URI_RE.finditer(text):
        uri = match.group(0).rstrip("/.,);")
        scheme = match.group("scheme").lower()
        dataset_type = _URI_SCHEME_TYPES.get(scheme, "uri")
        datasets.append(Dataset(name=uri, dataset_type=dataset_type, uri=uri))
        matched_spans.append((match.start(), match.end()))

    for match in FILE_HINT_RE.finditer(text):
        start = match.start()
        # Skip file hints that are part of an already-matched URI.
        if any(s <= start < e for s, e in matched_spans):
            continue
        name = match.group(0)
        # Avoid swallowing unknown-scheme URLs (e.g. https://host/file.csv).
        if "://" in name:
            continue
        ext = name.rsplit(".", 1)[-1].lower()
        datasets.append(
            Dataset(name=name, dataset_type=_FILE_EXT_TYPES.get(ext, "file"), uri=name)
        )

    return datasets


def connection_dataset(conn_id: str) -> Dataset:
    """Represent an Airflow connection id as a dataset candidate."""
    return Dataset(
        name=conn_id,
        dataset_type="connection",
        namespace="airflow_connection",
    )
