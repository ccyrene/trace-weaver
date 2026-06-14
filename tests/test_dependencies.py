import unittest

from support import dep_pairs, scan_files

CHAIN_DAG = """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

dag = DAG(dag_id="pipeline")
a = BashOperator(task_id="a", dag=dag)
b = BashOperator(task_id="b", dag=dag)
c = BashOperator(task_id="c", dag=dag)
d = BashOperator(task_id="d", dag=dag)

a >> b >> c
chain(c, d)
d.set_upstream(a)
"""

FANOUT_DAG = """
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="fan") as dag:
    start = EmptyOperator(task_id="start")
    left = EmptyOperator(task_id="left")
    right = EmptyOperator(task_id="right")
    end = EmptyOperator(task_id="end")

    start >> [left, right] >> end
    end << start
"""


class TestDependencies(unittest.TestCase):
    def test_shift_chain_and_set_upstream(self):
        result = scan_files(pipeline=CHAIN_DAG)
        pairs = dep_pairs(result)
        self.assertIn(("a", "b"), pairs)
        self.assertIn(("b", "c"), pairs)
        self.assertIn(("c", "d"), pairs)  # chain()
        self.assertIn(("a", "d"), pairs)  # set_upstream
        # dag id resolved from `dag = DAG(...)` + dag= kwarg
        self.assertTrue(all(t.dag_id == "pipeline" for t in result.task_dependencies))
        # no self-dependencies
        self.assertTrue(all(u != d for u, d in pairs))

    def test_list_fanout_and_lshift(self):
        result = scan_files(fan=FANOUT_DAG)
        pairs = dep_pairs(result)
        self.assertIn(("start", "left"), pairs)
        self.assertIn(("start", "right"), pairs)
        self.assertIn(("left", "end"), pairs)
        self.assertIn(("right", "end"), pairs)
        self.assertIn(("start", "end"), pairs)  # end << start

    def test_taskflow_data_dependencies(self):
        # Dependencies expressed by passing one task's output to another
        # (XCom data deps), with an explicit @task(task_id=...).
        dag = """
from airflow.decorators import dag, task

@dag(dag_id="tf")
def tf():
    @task
    def a():
        return 1

    @task(task_id="b_task")
    def b(x):
        return x

    @task
    def c(x, y):
        return x

    av = a()
    bv = b(av)
    c(av, bv)

tf()
"""
        result = scan_files(tf=dag)
        # @task(task_id="b_task") must use the explicit id, not the fn name.
        self.assertIn(("tf", "b_task"), {(j.dag_id, j.task_id) for j in result.jobs})
        pairs = dep_pairs(result)
        self.assertIn(("a", "b_task"), pairs)  # b(av)
        self.assertIn(("a", "c"), pairs)  # c(av, ...)
        self.assertIn(("b_task", "c"), pairs)  # c(..., bv)
        self.assertTrue(
            all(
                t.extraction_method == "taskflow_data" for t in result.task_dependencies
            )
        )

    def test_same_edge_from_two_syntaxes_collapses_to_one(self):
        # `a >> b` and `b.set_upstream(a)` both express (a -> b); the result
        # must contain exactly one such dependency, not two.
        dag = """
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="dup") as dag:
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    a >> b
    b.set_upstream(a)
"""
        result = scan_files(dup=dag)
        a_to_b = [
            t
            for t in result.task_dependencies
            if t.upstream_task_id == "a" and t.downstream_task_id == "b"
        ]
        self.assertEqual(len(a_to_b), 1)


if __name__ == "__main__":
    unittest.main()
