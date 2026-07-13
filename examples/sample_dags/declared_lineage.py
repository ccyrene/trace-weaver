"""Declarative dataset-level lineage with the ``@lineage`` decorator.

This DAG shows the **authoring** path: instead of leaving the compiler to infer
lineage from SQL / pandas code, the engineer *declares* the datasets each task
reads and writes with :func:`trace_weaver.lineage`. The decorator is a runtime
no-op — it returns the function unchanged and only attaches
``__traceweaver_lineage__`` — so the DAG runs normally under Airflow, and the
trace-weaver scanner reads the declarations statically (it never imports this
module).

    landing (S3)  ->  bronze (Iceberg)  ->  curated (Iceberg + Postgres mirror)

Scan it with::

    trace-weaver gate --repo-path examples/sample_dags --min-task-coverage 0.5
    trace-weaver scan examples/sample_dags -o out.weave.json
"""

from __future__ import annotations

from trace_weaver import lineage


@lineage(
    inputs=["s3://acme-raw/sales/{ds}/events.parquet"],
    outputs=["iceberg://warehouse.sales.bronze_events"],
    name="ingest_sales_events",
    description="Land raw S3 sales events into the bronze Iceberg table.",
)
def build_bronze():
    # Real Airflow/pandas/Spark body would go here; the scanner never runs it.
    ...


@lineage(
    inputs=["iceberg://warehouse.sales.bronze_events"],
    outputs=[
        "iceberg://warehouse.sales.curated_events",
        "postgresql://analytics/public.curated_events",
    ],
    description="Clean + dedupe bronze into the curated table (mirrored to Postgres).",
)
def build_curated():
    ...
