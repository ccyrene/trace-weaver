"""
Medallion lineage pipeline — a PLAIN Apache Airflow DAG (no trace-weaver annotation).

landing_sales → bronze_sales → silver_sales → gold_sales_daily

This DAG carries **zero** `@tw` decorators. trace-weaver scans it statically and
recovers full column-level lineage on its own:

  * ``build_bronze`` — a SQL operator running ``BRONZE_SQL``.  trace-weaver parses
        the query → table + column lineage (*inferred from SQL*).
  * ``build_silver`` — a ``PythonOperator`` whose pandas callable reads bronze and
        writes silver.  trace-weaver reads the function body → column lineage
        (*inferred from code*): identity copies, the USD conversion fan-in, the
        date cast, and the validity comparison.
  * ``build_gold``   — a SQL operator running ``GOLD_SQL`` (daily aggregates).
        → *inferred from SQL*.

Run::

    trace-weaver scan examples/dags \\
      --service "Test Database" --database poc_db --schema public

The ``--service/--database/--schema`` flags expand the bare table names the code
uses (``bronze_sales``) into OpenMetadata FQNs; no ``tw.configure(...)`` needed.
"""

from datetime import datetime

# Airflow / pandas are optional at *parse* time — trace-weaver never executes this
# module. Guard the imports so the file still imports where they aren't installed.
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.operators.postgres import PostgresOperator
except ImportError:  # pragma: no cover - not needed for static scanning
    DAG = None
    PythonOperator = None
    PostgresOperator = None

# ── SQL transformations (the bronze + gold hops run as SQL operators) ─────────────

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

GOLD_SQL = """
INSERT INTO gold_sales_daily (event_date, total_transactions, unique_customers, total_revenue_usd, avg_transaction_usd)
SELECT event_date, COUNT(*), COUNT(DISTINCT customer_name), SUM(amount_usd), ROUND(AVG(amount_usd), 2)
FROM silver_sales
WHERE is_valid
GROUP BY event_date
ON CONFLICT (event_date) DO NOTHING;
"""


# ── The silver hop is plain pandas — trace-weaver reads the body for lineage ──────


def build_silver():
    """bronze_sales → silver_sales: convert to USD, derive event_date, flag validity.

    Every column below is recovered by trace-weaver's static dataflow analysis —
    no column_map, no decorator. The body never runs during a scan.
    """
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    fx = {"USD": 1.00, "EUR": 1.08, "GBP": 1.27, "THB": 0.028}
    engine = PostgresHook(postgres_conn_id="poc_db").get_sqlalchemy_engine()

    bronze = pd.read_sql("SELECT * FROM bronze_sales", con=engine)
    silver = pd.DataFrame()
    silver["event_id"] = bronze["event_id"]  # identity copy
    silver["customer_name"] = bronze["customer_name"]  # identity copy
    silver["amount"] = bronze["amount"]  # identity copy
    silver["currency"] = bronze["currency"]  # identity copy
    silver["amount_usd"] = bronze["amount"] * bronze["currency"].map(fx).fillna(1.0)  # fan-in
    silver["event_date"] = bronze["event_ts"]  # rename / cast
    silver["is_valid"] = bronze["amount"] > 0  # comparison
    silver.to_sql("silver_sales", con=engine, if_exists="append", index=False)


# ── DAG definition (plain operators; trace-weaver discovers tasks from these) ─────

if DAG is not None:  # pragma: no cover - only constructed inside a live Airflow env
    with DAG(
        dag_id="medallion_lineage",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        is_paused_upon_creation=True,
        tags=["lineage", "medallion"],
    ) as dag:
        build_bronze = PostgresOperator(
            task_id="build_bronze",
            postgres_conn_id="poc_db",
            sql=BRONZE_SQL,
        )
        build_silver_task = PythonOperator(
            task_id="build_silver",
            python_callable=build_silver,
        )
        build_gold = PostgresOperator(
            task_id="build_gold",
            postgres_conn_id="poc_db",
            sql=GOLD_SQL,
        )

        build_bronze >> build_silver_task >> build_gold
