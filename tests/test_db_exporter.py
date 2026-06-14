import tempfile
import unittest
from pathlib import Path

from support import scan_files
from traceweaver.exporters.db_exporter import (
    normalize_database_url,
    sqlalchemy_available,
)

SAMPLE = """
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id="d") as dag:
    t = PostgresOperator(
        task_id="t",
        postgres_conn_id="wh",
        sql="INSERT INTO out.t SELECT * FROM in.t",
    )
"""


class TestNormalizeUrl(unittest.TestCase):
    def test_postgres_urls_pinned_to_psycopg(self):
        self.assertEqual(
            normalize_database_url("postgresql://u:p@h:5432/db"),
            "postgresql+psycopg://u:p@h:5432/db",
        )
        self.assertEqual(
            normalize_database_url("postgres://u@h/db"),
            "postgresql+psycopg://u@h/db",
        )

    def test_other_urls_untouched(self):
        self.assertEqual(normalize_database_url("sqlite:///x.db"), "sqlite:///x.db")


@unittest.skipUnless(sqlalchemy_available(), "sqlalchemy not installed")
class TestDbExporter(unittest.TestCase):
    def _counts(self, url):
        import sqlalchemy as sa

        engine = sa.create_engine(url)
        out = {}
        with engine.connect() as conn:
            for table in (
                "lineage_jobs",
                "lineage_datasets",
                "lineage_edges",
                "task_dependencies",
                "function_calls",
                "raw_scan_results",
            ):
                # `table` is one of the hardcoded schema names above, never user
                # input; a SQL identifier cannot be passed as a bound parameter.
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                count_sql = sa.text(f"SELECT COUNT(*) FROM {table}")
                out[table] = conn.execute(count_sql).scalar()
        engine.dispose()
        return out

    def test_export_and_refresh(self):
        from traceweaver.exporters.db_exporter import DbExporter

        result = scan_files(d=SAMPLE)
        with tempfile.TemporaryDirectory() as tmp:
            url = f"sqlite:///{Path(tmp) / 'lineage.db'}"
            exporter = DbExporter(url)
            exporter.export(result)
            first = self._counts(url)
            self.assertGreaterEqual(first["lineage_jobs"], 1)
            self.assertGreaterEqual(first["lineage_edges"], 1)
            self.assertEqual(first["raw_scan_results"], 1)

            # Re-running refreshes structured tables but appends raw history.
            exporter.export(result)
            second = self._counts(url)
            self.assertEqual(second["lineage_jobs"], first["lineage_jobs"])
            self.assertEqual(second["lineage_datasets"], first["lineage_datasets"])
            self.assertEqual(second["raw_scan_results"], 2)

    def test_append_accumulates_jobs_but_not_duplicate_datasets(self):
        from traceweaver.exporters.db_exporter import DbExporter

        result = scan_files(d=SAMPLE)
        with tempfile.TemporaryDirectory() as tmp:
            url = f"sqlite:///{Path(tmp) / 'append.db'}"
            exporter = DbExporter(url)
            exporter.export(result, append=True)
            first = self._counts(url)
            # Second append must not raise a UNIQUE violation on dataset_id.
            exporter.export(result, append=True)
            second = self._counts(url)
            # Structured rows accumulate...
            self.assertEqual(second["lineage_jobs"], 2 * first["lineage_jobs"])
            # ...but datasets are de-duped against existing dataset_ids.
            self.assertEqual(second["lineage_datasets"], first["lineage_datasets"])


if __name__ == "__main__":
    unittest.main()
