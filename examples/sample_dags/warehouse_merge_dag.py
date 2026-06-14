"""Snowflake MERGE inside a ``with DAG(...)`` block.

Exercises dialect-aware SQL parsing (MERGE target + USING source) and a SQL
statement stored in a module-level constant that is passed by reference.
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

MERGE_SQL = """
MERGE INTO analytics.dim_customer AS target
USING staging.customer_updates AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.name = source.name
WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)
"""

with DAG(dag_id="customer_warehouse") as dag:
    merge_customers = SnowflakeOperator(
        task_id="merge_customers",
        snowflake_conn_id="snowflake_default",
        sql=MERGE_SQL,
    )
