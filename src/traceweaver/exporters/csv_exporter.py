from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path

from traceweaver.models import ScanResult


class CsvExporter:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)

    def export(self, result: ScanResult) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._write_jobs(result)
        self._write_datasets(result)
        self._write_edges(result)
        self._write_task_dependencies(result)
        self._write_function_calls(result)

    def _write_jobs(self, result: ScanResult) -> None:
        rows = [asdict(row) for row in result.jobs]
        self._write(
            "lineage_jobs.csv",
            [
                "dag_id",
                "task_id",
                "operator_class",
                "callable_path",
                "file_path",
                "line_no",
            ],
            rows,
        )

    def _write_datasets(self, result: ScanResult) -> None:
        rows = []
        for row in result.datasets:
            item = asdict(row)
            item["dataset_id"] = row.dataset_id
            rows.append(item)
        self._write(
            "lineage_datasets.csv",
            [
                "dataset_id",
                "namespace",
                "name",
                "dataset_type",
                "uri",
                "schema_name",
                "table_name",
            ],
            rows,
        )

    def _write_edges(self, result: ScanResult) -> None:
        rows = [asdict(row) for row in result.edges]
        self._write(
            "lineage_edges.csv",
            [
                "dag_id",
                "task_id",
                "source_dataset",
                "target_dataset",
                "extraction_method",
                "confidence",
            ],
            rows,
        )

    def _write_task_dependencies(self, result: ScanResult) -> None:
        rows = [asdict(row) for row in result.task_dependencies]
        self._write(
            "task_dependencies.csv",
            [
                "dag_id",
                "upstream_task_id",
                "downstream_task_id",
                "extraction_method",
                "confidence",
            ],
            rows,
        )

    def _write_function_calls(self, result: ScanResult) -> None:
        rows = [asdict(row) for row in result.function_calls]
        self._write(
            "function_calls.csv",
            [
                "dag_id",
                "task_id",
                "module",
                "function_name",
                "caller_function",
                "file_path",
                "line_no",
                "method",
            ],
            rows,
        )

    def _write(self, filename: str, fieldnames: list[str], rows: list[dict]) -> None:
        path = self.output_dir / filename
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=fieldnames, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)
