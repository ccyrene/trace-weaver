"""SQL discovered from external .sql files and nested operator config blocks."""

import tempfile
import unittest
from pathlib import Path

from traceweaver.scanners.repo_scanner import RepoScanner


def _pairs(result):
    return {(e.source_dataset, e.target_dataset) for e in result.edges}


def _scan(files: dict[str, str]):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for rel, content in files.items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        return RepoScanner(root).scan()


class TestExternalSqlFile(unittest.TestCase):
    def test_sql_file_reference_is_resolved_and_parsed(self):
        result = _scan(
            {
                "sql/load.sql": (
                    "INSERT INTO analytics.orders_clean SELECT * FROM raw.orders\n"
                ),
                "dag.py": (
                    "from airflow import DAG\n"
                    "from airflow.providers.postgres.operators.postgres "
                    "import PostgresOperator\n"
                    'with DAG(dag_id="d") as dag:\n'
                    '    PostgresOperator(task_id="t", sql="sql/load.sql")\n'
                ),
            }
        )
        self.assertIn(("raw.orders", "analytics.orders_clean"), _pairs(result))

    def test_unresolved_path_uses_unambiguous_basename_fallback(self):
        # The sql= path does not resolve directly, but exactly one file with
        # that basename exists in the repo, so it is found.
        result = _scan(
            {
                "etl/queries/load.sql": (
                    "INSERT INTO analytics.orders_clean SELECT * FROM raw.orders\n"
                ),
                "dag.py": (
                    "from airflow import DAG\n"
                    "from airflow.providers.postgres.operators.postgres "
                    "import PostgresOperator\n"
                    'with DAG(dag_id="d") as dag:\n'
                    '    PostgresOperator(task_id="t", sql="load.sql")\n'
                ),
            }
        )
        self.assertIn(("raw.orders", "analytics.orders_clean"), _pairs(result))

    def test_ambiguous_basename_is_skipped_with_warning(self):
        # Two files share the basename and neither resolves directly: refuse to
        # guess, emit a warning, and produce no (possibly wrong) lineage.
        result = _scan(
            {
                "a/load.sql": "INSERT INTO a.t SELECT * FROM a.src\n",
                "b/load.sql": "INSERT INTO b.t SELECT * FROM b.src\n",
                "dag.py": (
                    "from airflow import DAG\n"
                    "from airflow.providers.postgres.operators.postgres "
                    "import PostgresOperator\n"
                    'with DAG(dag_id="d") as dag:\n'
                    '    PostgresOperator(task_id="t", sql="load.sql")\n'
                ),
            }
        )
        sql_methods = {"sqlglot", "sql_regex"}
        self.assertEqual(
            [e for e in result.edges if e.extraction_method in sql_methods], []
        )
        self.assertTrue(
            any("Ambiguous .sql reference" in w for w in result.warnings),
            result.warnings,
        )

    def test_missing_sql_file_is_ignored_gracefully(self):
        result = _scan(
            {
                "dag.py": (
                    "from airflow import DAG\n"
                    "from airflow.providers.postgres.operators.postgres "
                    "import PostgresOperator\n"
                    'with DAG(dag_id="d") as dag:\n'
                    '    PostgresOperator(task_id="t", sql="nope/missing.sql")\n'
                ),
            }
        )
        sql_methods = {"sqlglot", "sql_regex"}
        self.assertEqual(
            [e for e in result.edges if e.extraction_method in sql_methods], []
        )


class TestNestedConfigSql(unittest.TestCase):
    def test_bigquery_configuration_query(self):
        result = _scan(
            {
                "bq.py": (
                    "from airflow import DAG\n"
                    "from airflow.providers.google.cloud.operators.bigquery "
                    "import BigQueryInsertJobOperator\n"
                    'with DAG(dag_id="d") as dag:\n'
                    '    BigQueryInsertJobOperator(task_id="t",\n'
                    '        configuration={"query": '
                    '{"query": "INSERT INTO sink.t SELECT * FROM src.t"}})\n'
                ),
            }
        )
        self.assertIn(("src.t", "sink.t"), _pairs(result))

    def test_op_kwargs_sql(self):
        result = _scan(
            {
                "p.py": (
                    "from airflow import DAG\n"
                    "from airflow.operators.python import PythonOperator\n"
                    'with DAG(dag_id="d") as dag:\n'
                    '    PythonOperator(task_id="t", '
                    'op_kwargs={"sql": "INSERT INTO out.a SELECT * FROM in.b"})\n'
                ),
            }
        )
        self.assertIn(("in.b", "out.a"), _pairs(result))


if __name__ == "__main__":
    unittest.main()
