from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
import hashlib

from traceweaver import confidence as _confidence


@dataclass(frozen=True)
class LineageJob:
    dag_id: str
    task_id: str
    operator_class: str | None
    callable_path: str | None
    file_path: str
    line_no: int | None


@dataclass(frozen=True)
class Dataset:
    name: str
    dataset_type: str = "unknown"
    namespace: str | None = None
    uri: str | None = None
    schema_name: str | None = None
    table_name: str | None = None

    @property
    def dataset_id(self) -> str:
        raw = "|".join(
            [
                self.namespace or "",
                self.name or "",
                self.dataset_type or "",
                self.uri or "",
            ]
        )
        return hashlib.sha1(raw.encode("utf-8"), usedforsecurity=False).hexdigest()[:16]


@dataclass(frozen=True)
class LineageEdge:
    dag_id: str
    task_id: str
    source_dataset: str | None
    target_dataset: str | None
    extraction_method: str
    confidence: str


@dataclass(frozen=True)
class TaskDependency:
    dag_id: str
    upstream_task_id: str
    downstream_task_id: str
    extraction_method: str = "static_ast"
    confidence: str = _confidence.TASK_DEPENDENCY


@dataclass(frozen=True)
class FunctionCall:
    function_name: str
    file_path: str
    line_no: int | None
    dag_id: str | None = None
    task_id: str | None = None
    module: str | None = None
    caller_function: str | None = None
    method: str = "static_ast"


@dataclass
class ScanResult:
    repo_path: str
    jobs: list[LineageJob] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)
    task_dependencies: list[TaskDependency] = field(default_factory=list)
    function_calls: list[FunctionCall] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    # Transient: raw operation-level accesses awaiting task attribution by
    # RepoScanner. Converted into datasets/edges there, never serialized.
    io_accesses: list = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_path": self.repo_path,
            "jobs": [asdict(item) for item in self.jobs],
            "datasets": [
                asdict(item) | {"dataset_id": item.dataset_id} for item in self.datasets
            ],
            "edges": [asdict(item) for item in self.edges],
            "task_dependencies": [asdict(item) for item in self.task_dependencies],
            "function_calls": [asdict(item) for item in self.function_calls],
            "warnings": list(self.warnings),
        }

    def extend(self, other: "ScanResult") -> None:
        self.jobs.extend(other.jobs)
        self.datasets.extend(other.datasets)
        self.edges.extend(other.edges)
        self.task_dependencies.extend(other.task_dependencies)
        self.function_calls.extend(other.function_calls)
        self.warnings.extend(other.warnings)
        self.io_accesses.extend(other.io_accesses)

    def dedupe(self) -> None:
        self.jobs = list(dict.fromkeys(self.jobs))
        self.datasets = list(dict.fromkeys(self.datasets))
        self.edges = list(dict.fromkeys(self.edges))
        self.task_dependencies = list(dict.fromkeys(self.task_dependencies))
        self.function_calls = list(dict.fromkeys(self.function_calls))
