"""
Medallion lineage pipeline — the trace-weaver-convention version of the OpenMetadata POC.

This is the same linear medallion pipeline as ``ref/medallion_lineage_poc.py``
(landing -> bronze -> silver -> gold), but rewritten in the trace-weaver authoring
convention: instead of imperatively pushing lineage to OpenMetadata via the SDK,
each data-producing task is annotated with the ``@tw.task(...)`` decorator.

The decorator is a runtime no-op (it returns the wrapped function unchanged, so
this DAG still runs normally in Airflow). Its real purpose is to be *statically*
parseable by the data-lineage compiler, which reads the literal arguments and
derives column-level lineage.

This file deliberately exercises ALL THREE provenance modes so you can see the
"(inferred ...)" tagging at work:

  * ``build_bronze`` — engine="sql", sql=BRONZE_SQL, NO column_map.
        => every bronze column's lineage is *inferred from SQL*.
  * ``build_silver`` — engine="pandas", copy=[...] for the same-name
        passthroughs + column_map for the derived columns.
        => every silver column's lineage is *declared* (authoritative).
  * ``build_gold``   — engine="sql", sql=GOLD_SQL, PARTIAL column_map
        (only event_date + total_revenue_usd declared).
        => a MIX: those two are declared, the remaining gold columns
           (total_transactions, unique_customers, avg_transaction_usd)
           are *inferred from SQL*.

FQNs follow the OpenMetadata "service.database.schema.table" style under the
service "Test Database", database "poc_db", schema "public".
"""

from datetime import datetime

# Airflow / pandas are optional at *parse* time — the compiler is static and
# never executes this module. Guard the imports so the file still imports
# cleanly in environments where they are not installed.
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - airflow not needed for static scanning
    DAG = None
    PythonOperator = None

# The trace-weaver SDK. Both import styles are supported by the scanner; this file uses
# the ``import trace_weaver as tw`` form.
import trace_weaver as tw

# Tier 1: set per-file defaults ONCE. After this, tasks reference tables by their
# bare name (e.g. "bronze_sales") and the scanner expands them to the full FQN
# "Test Database.poc_db.public.bronze_sales". The default ``dag`` is taken from
# the ``with DAG(dag_id="medallion_lineage")`` block below (Tier 2), so individual
# tasks don't repeat service/database/schema/dag.
tw.configure(service="Test Database", database="poc_db", schema="public")

# ── SQL transformations (one per hop, copied verbatim from the POC) ──────────────

BRONZE_SQL = """
INSERT INTO bronze_sales (event_id, customer_name, amount, currency, event_ts)
SELECT raw_event_id::bigint, customer, amount::numeric(12,2), currency, event_ts::timestamp
FROM (
  SELECT *, row_number() OVER (PARTITION BY raw_event_id ORDER BY ingested_at) AS rn
  FROM landing_sales
) deduped
WHERE rn = 1
ON CONFLICT (event_id) DO NOTHING;
"""

SILVER_SQL = """
INSERT INTO silver_sales (event_id, customer_name, amount, currency, amount_usd, event_date, is_valid)
SELECT event_id, customer_name, amount, currency,
       ROUND(amount * CASE currency WHEN 'USD' THEN 1.00 WHEN 'EUR' THEN 1.08
                                    WHEN 'GBP' THEN 1.27 WHEN 'THB' THEN 0.028 ELSE 1.00 END, 2),
       event_ts::date,
       (amount > 0)
FROM bronze_sales
ON CONFLICT (event_id) DO NOTHING;
"""

GOLD_SQL = """
INSERT INTO gold_sales_daily (event_date, total_transactions, unique_customers, total_revenue_usd, avg_transaction_usd)
SELECT event_date, COUNT(*), COUNT(DISTINCT customer_name), SUM(amount_usd), ROUND(AVG(amount_usd), 2)
FROM silver_sales
WHERE is_valid
GROUP BY event_date
ON CONFLICT (event_date) DO NOTHING;
"""

# ── Rich edge descriptions (markdown) ────────────────────────────────────────────

BRONZE_DESC = """\
### landing_sales → bronze_sales
**CAST / PARSE / DEDUPE** — turn the raw text landing layer into a typed,
deduplicated bronze table. Column lineage is *inferred from the SQL* below.
"""

SILVER_DESC = """\
### bronze_sales → silver_sales
**ENRICH / STANDARDISE** — normalise currency to USD, derive `event_date`, and
flag data quality. All column lineage here is *declared* in `column_map`.
"""

GOLD_DESC = """\
### silver_sales → gold_sales_daily
**AGGREGATE (daily)** — collapse valid silver rows into one row per day. Only
`event_date` and `total_revenue_usd` are declared; the remaining measures are
*inferred from the SQL*.
"""

# ── trace-weaver-annotated tasks ─────────────────────────────────────────────────────────


# Tier 1: `@tw.sql(...)` shortcut — engine is implicitly "sql" and column
# lineage is auto-extracted from the query, so no column_map is needed. Tables
# are bare names (expanded via configure() above); dag comes from `with DAG(...)`.
@tw.sql(
    BRONZE_SQL,
    inputs=["landing_sales"],
    outputs=["bronze_sales"],
    description=BRONZE_DESC,
    transform="CAST / PARSE / DEDUPE",
)
def build_bronze():
    """Run the landing -> bronze SQL (CAST/PARSE + dedupe by event_id)."""
    import os

    from airflow.hooks.postgres_hook import PostgresHook

    hook = PostgresHook(postgres_conn_id=os.environ.get("POC_DB_CONN", "poc_db"))
    hook.run(BRONZE_SQL)
    print("bronze_sales populated from landing_sales (deduped).")


# pandas/Spark/python tasks: the compiler can't trace dataflow, so declare a
# column_map. engine is omitted (inferred); tables are bare names.
@tw.task(
    inputs=["bronze_sales"],
    outputs=["silver_sales"],
    description=SILVER_DESC,
    transform="ENRICH to USD + validity flag",
    # Same-name passthroughs: one `copy=[...]` line instead of four
    # ("x" -> "x", "direct copy") rows. These become DECLARED identity lineage.
    copy=["event_id", "customer_name", "amount", "currency"],
    # Only the columns that actually change need a column_map entry. A single
    # source can be written as a bare string (no surrounding list).
    column_map=[
        (["amount", "currency"], "amount_usd", "ROUND(amount * fx[currency], 2)"),
        ("event_ts", "event_date", "CAST timestamp -> date"),
        ("amount", "is_valid", "amount > 0"),
    ],
)
def build_silver():
    """Build silver_sales from bronze_sales with a plausible pandas transform.

    This body is illustrative — the compiler never executes it; it reads the
    declared ``column_map`` above. Imports are local so the module still parses
    without pandas installed.
    """
    import pandas as pd

    # Per-currency FX rates -> USD (mirrors the CASE expression in SILVER_SQL).
    fx = {"USD": 1.00, "EUR": 1.08, "GBP": 1.27, "THB": 0.028}

    bronze = pd.read_sql("SELECT * FROM bronze_sales", con="postgresql://poc_db")

    silver = pd.DataFrame()
    silver["event_id"] = bronze["event_id"]
    silver["customer_name"] = bronze["customer_name"]
    silver["amount"] = bronze["amount"]
    silver["currency"] = bronze["currency"]
    silver["amount_usd"] = (
        bronze["amount"] * bronze["currency"].map(fx).fillna(1.00)
    ).round(2)
    silver["event_date"] = pd.to_datetime(bronze["event_ts"]).dt.date
    silver["is_valid"] = bronze["amount"] > 0

    silver.to_sql("silver_sales", con="postgresql://poc_db", if_exists="append", index=False)
    print(f"silver_sales populated: {len(silver)} rows.")


# `@tw.sql` again, but with a PARTIAL column_map: the two declared columns win,
# the rest (total_transactions, unique_customers, avg_transaction_usd) are still
# inferred from GOLD_SQL — demonstrating declared + inferred on one hop.
@tw.sql(
    GOLD_SQL,
    inputs=["silver_sales"],
    outputs=["gold_sales_daily"],
    description=GOLD_DESC,
    transform="AGGREGATE daily",
    column_map=[
        (["event_date"], "event_date", "GROUP BY key"),
        (["amount_usd"], "total_revenue_usd", "SUM(amount_usd)"),
    ],
)
def build_gold():
    """Run the silver -> gold daily aggregation SQL (filter is_valid)."""
    import os

    from airflow.hooks.postgres_hook import PostgresHook

    hook = PostgresHook(postgres_conn_id=os.environ.get("POC_DB_CONN", "poc_db"))
    hook.run(GOLD_SQL)
    print("gold_sales_daily populated from silver_sales (daily aggregate).")


# ── DAG definition ─────────────────────────────────────────────────────────────

if DAG is not None:  # pragma: no cover - only constructed inside a live Airflow env
    with DAG(
        dag_id="medallion_lineage",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        is_paused_upon_creation=True,
        tags=["trace-weaver", "lineage", "medallion"],
    ) as dag:
        dag.doc_md = __doc__

        bronze = PythonOperator(task_id="build_bronze", python_callable=build_bronze)
        silver = PythonOperator(task_id="build_silver", python_callable=build_silver)
        gold = PythonOperator(task_id="build_gold", python_callable=build_gold)

        bronze >> silver >> gold
