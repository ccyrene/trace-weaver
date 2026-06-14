from pathlib import Path
import tempfile
import unittest

from traceweaver.exporters.csv_exporter import CsvExporter
from traceweaver.scanners.repo_scanner import RepoScanner


class TestSampleDags(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo = Path(__file__).resolve().parents[1] / "examples" / "sample_dags"
        cls.result = RepoScanner(repo).scan()

    def test_scan_sample_dags(self):
        result = self.result
        self.assertGreaterEqual(len(result.jobs), 9)
        self.assertTrue(
            any(
                j.dag_id == "daily_sales" and j.task_id == "extract_orders"
                for j in result.jobs
            )
        )
        self.assertTrue(any(e.source_dataset == "raw.orders" for e in result.edges))
        self.assertTrue(
            any(c.function_name == "read_orders" for c in result.function_calls)
        )

    def test_taskflow_jobs_extracted_once(self):
        taskflow = [
            (j.task_id, j.line_no)
            for j in self.result.jobs
            if j.dag_id == "taskflow_sales" and j.operator_class == "TaskFlowTask"
        ]
        task_ids = sorted(t for t, _ in taskflow)
        self.assertEqual(
            task_ids, ["extract", "transform"]
        )  # exactly one each, no dupes

    def test_task_dependencies_extracted(self):
        pairs = {
            (t.dag_id, t.upstream_task_id, t.downstream_task_id)
            for t in self.result.task_dependencies
        }
        self.assertIn(("etl_pipeline", "download", "load"), pairs)
        self.assertIn(("etl_pipeline", "load", "validate"), pairs)

    def test_connection_dataset_extracted(self):
        conns = {d.name for d in self.result.datasets if d.dataset_type == "connection"}
        self.assertIn("warehouse", conns)

    def test_function_calls_attributed_to_task(self):
        attributed = [
            c for c in self.result.function_calls if c.function_name == "read_orders"
        ]
        self.assertTrue(attributed)
        self.assertEqual(attributed[0].dag_id, "daily_sales")
        self.assertEqual(attributed[0].task_id, "extract_orders")

    def test_csv_exporter(self):
        with tempfile.TemporaryDirectory() as tmp:
            CsvExporter(Path(tmp)).export(self.result)
            for name in (
                "lineage_jobs.csv",
                "lineage_edges.csv",
                "task_dependencies.csv",
                "function_calls.csv",
                "lineage_datasets.csv",
            ):
                self.assertTrue((Path(tmp) / name).exists(), name)


if __name__ == "__main__":
    unittest.main()
