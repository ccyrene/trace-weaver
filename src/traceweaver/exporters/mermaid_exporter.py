"""Mermaid exporter for ``--output mermaid``.

Renders the scan as a left-to-right Mermaid ``flowchart`` that shows, per DAG:

- the DAG name (subgraph title) and its tasks,
- task dependencies (``task ==> task``, thick),
- data lineage (``dataset --> task --> dataset``; dotted when low confidence),
- the business functions each task calls (``task -.-> function``).

Writes ``lineage.mmd`` (raw Mermaid). With ``image_formats`` set and
``mermaid-cli`` (mmdc) available, it also renders ``lineage.svg`` / ``.png``.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import json
import os
import shutil
import subprocess
import tempfile

from traceweaver.models import ScanResult

_URI_TYPES = {"s3", "gcs", "azure_blob", "azure_datalake", "hdfs", "ftp", "file", "uri"}

_INIT = "%%{init: {'flowchart': {'curve': 'basis', 'nodeSpacing': 45, 'rankSpacing': 70}}}%%"

_CLASSDEFS = [
    "classDef task fill:#dae8fc,stroke:#4575b4,stroke-width:1px,color:#16335b;",
    "classDef dataset fill:#d5e8d4,stroke:#5a9e54,color:#173d12;",
    "classDef conn fill:#ffe6cc,stroke:#d79b00,color:#5a3d00;",
    "classDef fn fill:#f5f5f5,stroke:#9aa0a6,color:#3c4043;",
]


def _escape(label: str) -> str:
    return label.replace('"', "'").replace("\n", " ")


def _card_label(task: str, functions: list[str]) -> str:
    """A task card: bold title, then a numbered list of the functions it calls."""
    rows = [f"<b>{_escape(task)}</b>"]
    if functions:
        rows.append("<i>calls:</i>")
        rows += [f"{i}. {_escape(fn)}" for i, fn in enumerate(functions, 1)]
    return "<br/>".join(rows)


def _shape(name: str, dataset_type: str) -> str:
    """Return the Mermaid node-shape suffix for a dataset, by type."""
    label = _escape(name)
    if dataset_type == "table":
        return '[("' + label + '")]'  # cylinder / database
    if dataset_type == "connection":
        return '{{"' + label + '"}}'  # hexagon
    if dataset_type in _URI_TYPES:
        return '[/"' + label + '"/]'  # parallelogram (object store / file)
    return '["' + label + '"]'  # rectangle (fallback)


class MermaidExporter:
    def __init__(self, output_dir: Path, image_formats: tuple[str, ...] = ()) -> None:
        self.output_dir = Path(output_dir)
        # e.g. ("svg",), ("png",) or ("svg", "png"); empty = no image rendering.
        self.image_formats = tuple(image_formats)

    def export(self, result: ScanResult) -> list[str]:
        """Write lineage.mmd (+ optional rendered images). Returns any warnings."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        graph = self.render(result)
        (self.output_dir / "lineage.mmd").write_text(graph + "\n", encoding="utf-8")
        return self._render_images()

    def _render_images(self) -> list[str]:
        if not self.image_formats:
            return []
        mmdc = shutil.which("mmdc")
        if not mmdc:
            return [
                "mermaid-cli (mmdc) not found; wrote lineage.mmd only. "
                "Install with: npm install -g @mermaid-js/mermaid-cli"
            ]

        source = self.output_dir / "lineage.mmd"
        warnings: list[str] = []
        # Headless Chromium needs --no-sandbox inside containers / as non-root.
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as cfg:
            json.dump({"args": ["--no-sandbox", "--disable-setuid-sandbox"]}, cfg)
            puppeteer_cfg = cfg.name
        try:
            for fmt in self.image_formats:
                target = self.output_dir / f"lineage.{fmt}"
                try:
                    subprocess.run(
                        [
                            mmdc,
                            "-i",
                            str(source),
                            "-o",
                            str(target),
                            "-p",
                            puppeteer_cfg,
                            "-b",
                            "white",
                        ],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                except subprocess.CalledProcessError as exc:
                    detail = (exc.stderr or exc.stdout or str(exc)).strip().splitlines()
                    warnings.append(
                        f"mermaid {fmt} render failed: {detail[-1] if detail else exc}"
                    )
        finally:
            os.unlink(puppeteer_cfg)
        return warnings

    def render(self, result: ScanResult) -> str:
        ids: dict[tuple, str] = {}

        def nid(key: tuple) -> str:
            if key not in ids:
                ids[key] = f"n{len(ids)}"
            return ids[key]

        dataset_types = {d.name: d.dataset_type for d in result.datasets}

        # Tasks grouped by DAG.
        dag_tasks: dict[str, set[str]] = defaultdict(set)
        for job in result.jobs:
            dag_tasks[job.dag_id].add(job.task_id)
        for dep in result.task_dependencies:
            dag_tasks[dep.dag_id].update([dep.upstream_task_id, dep.downstream_task_id])
        for edge in result.edges:
            dag_tasks[edge.dag_id].add(edge.task_id)

        # Ordered, de-duplicated list of functions each task calls (by line no).
        calls: dict[tuple, list[str]] = defaultdict(list)
        for call in result.function_calls:
            if call.dag_id and call.task_id:
                calls[(call.dag_id, call.task_id)].append(
                    (
                        call.line_no if call.line_no is not None else 1 << 30,
                        call.function_name,
                    )
                )

        def task_functions(dag: str, task: str) -> list[str]:
            seen: set[str] = set()
            ordered: list[str] = []
            for _, fn in sorted(calls.get((dag, task), [])):
                if fn not in seen:
                    seen.add(fn)
                    ordered.append(fn)
            return ordered

        lines = [_INIT, "flowchart LR"]
        lines += ["  " + d for d in _CLASSDEFS]

        # One subgraph per DAG; each task is a card listing its calls in order.
        for dag in sorted(dag_tasks):
            lines.append(f'  subgraph {nid(("dag", dag))}["DAG: {_escape(dag)}"]')
            lines.append("    direction LR")
            for task in sorted(dag_tasks[dag]):
                label = _card_label(task, task_functions(dag, task))
                lines.append(f'    {nid(("task", dag, task))}["{label}"]:::task')
            lines.append("  end")

        # Dataset nodes that appear in an edge.
        used: set[str] = set()
        for edge in result.edges:
            if edge.source_dataset:
                used.add(edge.source_dataset)
            if edge.target_dataset:
                used.add(edge.target_dataset)
        for name in sorted(used):
            dtype = dataset_types.get(name, "unknown")
            cls = "conn" if dtype == "connection" else "dataset"
            lines.append(f'  {nid(("ds", name))}{_shape(name, dtype)}:::{cls}')

        # Edges (deduped, order preserved).
        seen: set[str] = set()
        edge_lines: list[str] = []

        def add(line: str) -> None:
            if line not in seen:
                seen.add(line)
                edge_lines.append("  " + line)

        # Task dependencies — simple thick left-to-right arrows between cards.
        for dep in result.task_dependencies:
            up = nid(("task", dep.dag_id, dep.upstream_task_id))
            down = nid(("task", dep.dag_id, dep.downstream_task_id))
            add(f"{up} ==> {down}")

        # Data lineage — thin arrows, dotted when low confidence.
        for edge in result.edges:
            task = nid(("task", edge.dag_id, edge.task_id))
            arrow = "-.->" if edge.confidence == "low" else "-->"
            if edge.source_dataset:
                add(f'{nid(("ds", edge.source_dataset))} {arrow} {task}')
            if edge.target_dataset:
                add(f'{task} {arrow} {nid(("ds", edge.target_dataset))}')

        return "\n".join(lines + edge_lines)
