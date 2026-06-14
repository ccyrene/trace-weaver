from airflow import DAG
from airflow.operators.python import PythonOperator


def read_orders():
    return "postgres.sales.orders"


def write_orders_to_s3():
    path = "s3://lake/raw/orders/orders.csv"
    return path


def extract_orders():
    rows = read_orders()
    output = write_orders_to_s3()
    return {"rows": rows, "output": output}


with DAG("daily_sales") as dag:
    extract = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )
