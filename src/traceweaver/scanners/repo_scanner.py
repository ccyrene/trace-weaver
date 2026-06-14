from __future__ import annotations

from pathlib import Path

from traceweaver import confidence
from traceweaver.models import Dataset, LineageEdge, ScanResult
from traceweaver.scanners.python_ast_scanner import PythonAstScanner


class RepoScanner:
    def __init__(self, repo_path: Path, include_tests: bool = False) -> None:
        self.repo_path = Path(repo_path)
        self.include_tests = include_tests
        self.python_scanner = PythonAstScanner(repo_root=self.repo_path)

    def scan(self) -> ScanResult:
        result = ScanResult(repo_path=str(self.repo_path))
        for file_path in self._iter_python_files():
            try:
                partial = self.python_scanner.scan_file(file_path)
            except SyntaxError as exc:
                result.warnings.append(f"Syntax error in {file_path}: {exc}")
                continue
            except OSError as exc:
                result.warnings.append(f"Could not read {file_path}: {exc}")
                continue

            result.extend(partial)

        self._emit_operation_lineage(result)
        result.dedupe()
        return result

    def _emit_operation_lineage(self, result: ScanResult) -> None:
        """Attribute coarse I/O accesses to tasks via the whole-repo call graph.

        An access recorded inside a helper function is credited to every task
        whose callable reaches that helper (directly or transitively). Emits a
        half-edge (source for reads, target for writes) per (task, dataset).
        """
        accesses = result.io_accesses
        if not accesses:
            return

        # Entry-point function name -> the task(s) it belongs to.
        owners: dict[str, set[tuple[str, str]]] = {}
        for job in result.jobs:
            entry_names = {job.task_id}
            if job.callable_path:
                entry_names.add(job.callable_path.rsplit(".", 1)[-1])
            for name in entry_names:
                owners.setdefault(name, set()).add((job.dag_id, job.task_id))

        # Reverse call graph: callee function name -> functions that call it.
        callers: dict[str, set[str]] = {}
        for call in result.function_calls:
            if not call.caller_function:
                continue
            callee = (call.function_name or "").rsplit(".", 1)[-1]
            callers.setdefault(callee, set()).add(call.caller_function)

        for access in accesses:
            for dag_id, task_id in self._tasks_for_function(
                access.caller_function, owners, callers
            ):
                result.datasets.append(
                    Dataset(name=access.name, dataset_type=access.dataset_type)
                )
                if access.direction == "read":
                    source, target = access.name, None
                else:
                    source, target = None, access.name
                result.edges.append(
                    LineageEdge(
                        dag_id=dag_id,
                        task_id=task_id,
                        source_dataset=source,
                        target_dataset=target,
                        extraction_method="operation",
                        confidence=confidence.OPERATION,
                    )
                )
        result.io_accesses = []

    @staticmethod
    def _tasks_for_function(
        func: str,
        owners: dict[str, set[tuple[str, str]]],
        callers: dict[str, set[str]],
    ) -> set[tuple[str, str]]:
        """Tasks reachable upward from ``func`` through the call graph."""
        found: set[tuple[str, str]] = set()
        seen: set[str] = set()
        stack = [func]
        while stack:
            current = stack.pop()
            if not current or current in seen:
                continue
            seen.add(current)
            found |= owners.get(current, set())
            stack.extend(callers.get(current, ()))
        return found

    def _iter_python_files(self):
        for path in sorted(self.repo_path.rglob("*.py")):
            rel_parts = path.relative_to(self.repo_path).parts
            if not self.include_tests and any(
                part in {"tests", "test", "__pycache__"} for part in rel_parts
            ):
                continue
            yield path
