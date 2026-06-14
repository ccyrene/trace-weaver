"""Shared test helpers."""

from __future__ import annotations

from pathlib import Path
import tempfile

from traceweaver.scanners.repo_scanner import RepoScanner


def scan_files(**files: str):
    """Write ``name=source`` pairs as ``name.py`` into a temp repo and scan it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name, src in files.items():
            (root / f"{name}.py").write_text(src)
        result = RepoScanner(root).scan()
        result.dedupe()
        return result


def dep_pairs(result):
    return {
        (t.upstream_task_id, t.downstream_task_id) for t in result.task_dependencies
    }


def edge_pairs(result):
    return {(e.source_dataset, e.target_dataset) for e in result.edges}
