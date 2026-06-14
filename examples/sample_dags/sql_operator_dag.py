from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

SQL = """
INSERT INTO analytics.orders_clean
SELECT o.id, c.name
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
"""

with DAG(dag_id="daily_sql_sales") as dag:
    transform = PostgresOperator(
        task_id="transform_orders",
        postgres_conn_id="warehouse",
        sql=SQL,
    )
