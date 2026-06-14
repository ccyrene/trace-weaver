"""Classic ``dag = DAG(...)`` style with explicit task dependencies.

Exercises: ``dag=`` keyword resolution, BashOperator file datasets, and the
``>>`` / ``chain`` / ``set_upstream`` dependency forms.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain


def load_data():
    return "loaded"


dag = DAG(dag_id="etl_pipeline")

download = BashOperator(
    task_id="download",
    bash_command="curl -o /data/raw/input.csv https://example.com/input.csv",
    dag=dag,
)

load = PythonOperator(
    task_id="load",
    python_callable=load_data,
    dag=dag,
)

validate = BashOperator(
    task_id="validate",
    bash_command="great_expectations checkpoint run /data/raw/input.csv",
    dag=dag,
)

notify = BashOperator(task_id="notify", bash_command="echo done", dag=dag)

download >> load >> validate
chain(validate, notify)
notify.set_upstream(load)
