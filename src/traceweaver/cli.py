from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from traceweaver.exporters.csv_exporter import CsvExporter
from traceweaver.exporters.json_exporter import JsonExporter
from traceweaver.exporters.mermaid_exporter import MermaidExporter
from traceweaver.scanners.git_source import resolve_repo
from traceweaver.scanners.repo_scanner import RepoScanner

_OUTPUT_CHOICES = ["csv", "json", "mermaid", "summary", "db", "all"]
_IMAGE_FORMATS = {"none": (), "svg": ("svg",), "png": ("png",), "both": ("svg", "png")}


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="traceweaver",
        description="Static lineage scanner for Airflow DAG repositories",
    )
    subparsers = parser.add_subparsers(dest="command")

    scan = subparsers.add_parser("scan", help="Scan a DAG repository")
    scan.add_argument(
        "--repo-path",
        default=_env("TRACEWEAVER_REPO_PATH"),
        help="Path to a local DAG repository or a git URL "
        "(env: TRACEWEAVER_REPO_PATH; default '/dags' inside the Docker image)",
    )
    scan.add_argument(
        "--output",
        choices=_OUTPUT_CHOICES,
        default=_env("TRACEWEAVER_OUTPUT_FORMAT", "csv"),
        help="Output format (env: TRACEWEAVER_OUTPUT_FORMAT)",
    )
    scan.add_argument(
        "--output-dir",
        default=_env("TRACEWEAVER_OUTPUT_DIR", "outputs"),
        help="Directory for CSV/JSON output (env: TRACEWEAVER_OUTPUT_DIR)",
    )
    scan.add_argument(
        "--database-url",
        default=_env("TRACEWEAVER_DATABASE_URL"),
        help="SQLAlchemy database URL for --output db/all (env: TRACEWEAVER_DATABASE_URL)",
    )
    scan.add_argument(
        "--db-append",
        action="store_true",
        help="Append to DB tables instead of refreshing the current snapshot",
    )
    scan.add_argument(
        "--image-format",
        choices=list(_IMAGE_FORMATS),
        default=_env("TRACEWEAVER_IMAGE_FORMAT", "none"),
        help="Also render the Mermaid graph to image(s) via mermaid-cli "
        "(env: TRACEWEAVER_IMAGE_FORMAT; the Docker image bundles mermaid-cli)",
    )
    scan.add_argument(
        "--git-ref",
        default=_env("TRACEWEAVER_GIT_REF"),
        help="Branch/tag to check out when --repo-path is a git URL",
    )
    scan.add_argument(
        "--include-tests",
        action="store_true",
        help="Include files under test directories",
    )
    return parser


def run_scan(args: argparse.Namespace) -> int:
    if not args.repo_path:
        print(
            "error: --repo-path is required (or set TRACEWEAVER_REPO_PATH)",
            file=sys.stderr,
        )
        return 2

    if args.output == "db" and not args.database_url:
        print(
            "error: --output db requires --database-url (or TRACEWEAVER_DATABASE_URL)",
            file=sys.stderr,
        )
        return 2

    try:
        with resolve_repo(args.repo_path, ref=args.git_ref) as repo_path:
            repo_path = repo_path.resolve()
            if not repo_path.exists():
                print(f"error: repo path does not exist: {repo_path}", file=sys.stderr)
                return 2

            scanner = RepoScanner(repo_path=repo_path, include_tests=args.include_tests)
            result = scanner.scan()
            result.dedupe()

            _write_outputs(args, result)
    except Exception as exc:
        # Surface any failure (git clone, DB connection/SQL, IO) as a clean
        # one-line error instead of dumping a traceback at the user.
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    return 0


def _write_outputs(args: argparse.Namespace, result) -> None:
    output = args.output
    wrote_files = False

    if output in ("csv", "all"):
        CsvExporter(Path(args.output_dir)).export(result)
        wrote_files = True
    if output in ("json", "all"):
        JsonExporter(Path(args.output_dir)).export(result)
        wrote_files = True
    if output in ("mermaid", "all"):
        image_formats = _IMAGE_FORMATS.get(args.image_format, ())
        notes = MermaidExporter(
            Path(args.output_dir), image_formats=image_formats
        ).export(result)
        for note in notes:
            print(f"note: {note}", file=sys.stderr)
        wrote_files = True
    if output == "db" or (output == "all" and args.database_url):
        from traceweaver.exporters.db_exporter import DbExporter

        DbExporter(args.database_url).export(result, append=args.db_append)

    print_summary(result)
    if wrote_files:
        print(f"output written to: {Path(args.output_dir).resolve()}")
    if output == "db" or (output == "all" and args.database_url):
        print(f"rows written to database: {args.database_url}")


def print_summary(result) -> None:
    print("TraceWeaver scan summary")
    print(f"  repo:               {result.repo_path}")
    print(f"  jobs:               {len(result.jobs)}")
    print(f"  datasets:           {len(result.datasets)}")
    print(f"  lineage edges:      {len(result.edges)}")
    print(f"  task dependencies:  {len(result.task_dependencies)}")
    print(f"  function calls:     {len(result.function_calls)}")
    if result.warnings:
        print(f"  warnings:           {len(result.warnings)}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "scan":
        return run_scan(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
