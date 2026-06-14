"""Operation-level (library-call) data-access detection + task attribution."""

import ast
import tempfile
import unittest
from pathlib import Path

from traceweaver.scanners.operations import describe_io
from traceweaver.scanners.repo_scanner import RepoScanner


def _call(src: str) -> ast.Call:
    return ast.parse(src, mode="eval").body


def _resolve(node: ast.AST) -> str | None:
    """Minimal static resolver: literal str or an f-string's static fragments."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts = [
            v.value
            for v in node.values
            if isinstance(v, ast.Constant) and isinstance(v.value, str)
        ]
        return "".join(parts) if parts else None
    return None


class TestDescribeIo(unittest.TestCase):
    def test_to_sql_exact_table(self):
        acc = describe_io(
            _call('df.to_sql("orders", con, schema="analytics")'), _resolve
        )
        self.assertEqual(
            (acc.dataset_type, acc.direction, acc.name),
            ("table", "write", "analytics.orders"),
        )

    def test_read_csv_from_s3(self):
        acc = describe_io(_call('pd.read_csv("s3://bucket/in.csv")'), _resolve)
        self.assertEqual(
            (acc.dataset_type, acc.direction, acc.name), ("s3", "read", "s3:csv")
        )

    def test_to_parquet_s3_via_path_kwarg(self):
        acc = describe_io(_call('df.to_parquet(path="s3://bucket/out")'), _resolve)
        self.assertEqual(
            (acc.dataset_type, acc.direction, acc.name), ("s3", "write", "s3:parquet")
        )

    def test_read_csv_local_file(self):
        acc = describe_io(_call('pd.read_csv("/data/in.csv")'), _resolve)
        self.assertEqual(
            (acc.dataset_type, acc.direction, acc.name), ("file", "read", "file:csv")
        )

    def test_fstring_scheme_recovered_from_static_part(self):
        # Bucket is interpolated, but the "s3://" prefix is a static fragment.
        acc = describe_io(_call('pd.read_csv(f"s3://{bucket}/{key}")'), _resolve)
        self.assertEqual((acc.dataset_type, acc.direction), ("s3", "read"))

    def test_create_engine_scheme(self):
        acc = describe_io(_call('create_engine("postgresql://u:p@h/db")'), _resolve)
        self.assertEqual(
            (acc.dataset_type, acc.direction, acc.name),
            ("connection", "read", "postgresql"),
        )

    def test_unknown_call_is_ignored(self):
        self.assertIsNone(describe_io(_call("foo.bar(1, 2)"), _resolve))
        self.assertIsNone(describe_io(_call("datetime.now()"), _resolve))


class TestOperationLineageAttribution(unittest.TestCase):
    def test_io_in_cross_file_helper_attributes_to_task(self):
        # A @task that calls helper functions in another module, which do the
        # actual pandas/SQLAlchemy I/O (the hard cross-file attribution case).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "pkg").mkdir()
            (root / "pkg" / "__init__.py").write_text("")
            (root / "pkg" / "etl.py").write_text(
                "import pandas as pd\n"
                "def load_csv():\n"
                '    return pd.read_csv(f"s3://{bucket}/raw/in.csv")\n'
                "def save_table(df, con):\n"
                '    df.to_sql("orders", con, schema="analytics")\n'
            )
            (root / "pkg" / "flow.py").write_text(
                "from airflow.decorators import dag, task\n"
                "from pkg.etl import load_csv, save_table\n"
                '@dag(dag_id="sales")\n'
                "def sales():\n"
                "    @task\n"
                "    def etl():\n"
                "        load_csv()\n"
                "        save_table(None, None)\n"
                "    etl()\n"
                "sales()\n"
            )
            result = RepoScanner(root).scan()

        op_edges = {
            (e.source_dataset, e.target_dataset)
            for e in result.edges
            if e.extraction_method == "operation"
            and e.dag_id == "sales"
            and e.task_id == "etl"
        }
        self.assertIn(("s3:csv", None), op_edges)  # read
        self.assertIn((None, "analytics.orders"), op_edges)  # write

    def test_ambiguous_function_name_is_not_misattributed(self):
        # Two modules define `run`; only b.run does the I/O, and the task calls
        # a.run (no I/O). A bare-name call graph would wrongly link the S3 read
        # to the task — instead it must be skipped (ambiguous) with a warning.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "mod_a.py").write_text("def run():\n    return 1\n")
            (root / "mod_b.py").write_text(
                "import pandas as pd\n"
                "def run():\n"
                "    return pd.read_csv('s3://bucket/x.csv')\n"
            )
            (root / "flow.py").write_text(
                "from airflow.decorators import dag, task\n"
                "from mod_a import run\n"
                '@dag(dag_id="d")\n'
                "def d():\n"
                "    @task\n"
                "    def task_a():\n"
                "        run()\n"
                "    task_a()\n"
                "d()\n"
            )
            result = RepoScanner(root).scan()
        op_edges = [e for e in result.edges if e.extraction_method == "operation"]
        self.assertEqual(op_edges, [])  # no wrong attribution
        self.assertTrue(
            any("defined in multiple modules" in w for w in result.warnings),
            result.warnings,
        )

    def test_owner_dotted_string_is_not_a_table(self):
        # An Airflow `owner` like "first.last" must not be read as a table.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "dag.py").write_text(
                "from airflow import DAG\n"
                "from airflow.operators.python import PythonOperator\n"
                'default_args = {"owner": "first.last"}\n'
                'with DAG(dag_id="d", default_args=default_args) as dag:\n'
                '    PythonOperator(task_id="t", python_callable=lambda: None)\n'
            )
            result = RepoScanner(root).scan()
        table_names = {d.name for d in result.datasets if d.dataset_type == "table"}
        self.assertNotIn("first.last", table_names)


if __name__ == "__main__":
    unittest.main()
