from airflow.decorators import dag, task


@dag(dag_id="taskflow_sales")
def taskflow_sales():
    @task
    def extract():
        source = "gs://landing/orders/orders.parquet"
        return source

    @task
    def transform():
        read_raw_orders()
        write_clean_orders()

    def read_raw_orders():
        return "raw.orders"

    def write_clean_orders():
        return "analytics.orders"

    extract()
    transform()


taskflow_sales()
