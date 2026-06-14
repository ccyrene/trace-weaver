from __future__ import annotations

import json
from pathlib import Path

from traceweaver.models import ScanResult


class JsonExporter:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)

    def export(self, result: ScanResult) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / "raw_scan_result.json"
        path.write_text(
            json.dumps(result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
        )
