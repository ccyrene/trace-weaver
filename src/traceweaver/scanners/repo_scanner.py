from __future__ import annotations

from pathlib import Path

from traceweaver.models import ScanResult
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

        result.dedupe()
        return result

    def _iter_python_files(self):
        for path in sorted(self.repo_path.rglob("*.py")):
            rel_parts = path.relative_to(self.repo_path).parts
            if not self.include_tests and any(
                part in {"tests", "test", "__pycache__"} for part in rel_parts
            ):
                continue
            yield path
