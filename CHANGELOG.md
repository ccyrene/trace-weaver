# Changelog

All notable changes to `trace-weaver` (the Rust CLI and the `trace_weaver`
Python authoring SDK, which share one version number) are documented here.

## 0.2.0

### Added

- **`@lineage` decorator** (`python/trace_weaver`) — a declarative,
  statically-parseable marker for **dataset-level** lineage on tasks the
  static analyzer can't see into (external APIs, opaque UDFs, shell-outs). It
  is a runtime no-op — returns the original function unchanged and attaches
  `__traceweaver_lineage__` — usable bare (`@lineage`) or with metadata
  (`@lineage(inputs=[...], outputs=[...], name=..., description=...)`). The
  scanner recognizes it under all four import forms (`from trace_weaver
  import lineage`, `... import lineage as X`, `import trace_weaver` +
  `@trace_weaver.lineage`, `import trace_weaver as tw` + `@tw.lineage`) and
  stacked with Airflow's `@task` in any order. Literal dataset strings are
  stamped **declared** (high confidence); non-literal entries (f-strings,
  names, calls) are kept as best-effort text and stamped **inferred**
  (medium confidence). See [`examples/sample_dags/declared_lineage.py`](examples/sample_dags/declared_lineage.py).
- **`trace-weaver gate` CLI subcommand** — turns a scan into a pass/fail CI
  check. Scans `--repo-path` (optionally as of `--git-ref`) and compares
  `task_coverage` and `high_confidence_fraction` against `--min-task-coverage`
  / `--min-high-confidence` (or the `TRACEWEAVER_MIN_TASK_COVERAGE` /
  `TRACEWEAVER_MIN_HIGH_CONFIDENCE` env vars, flag wins), in `text` or `json`
  `--format`. Exit codes: `0` pass, `1` threshold failed, `2` usage error.
- **`.github/workflows/publish.yml`** — builds the production Docker image
  and pushes it to Docker Hub (`docker.io/$DOCKERHUB_USERNAME/trace-weaver`,
  tags `{version}` and `latest`) on a pushed `v*` tag or manual dispatch.
  Requires the `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` repository secrets.

### Changed

- Version bumped to `0.2.0` (`python/pyproject.toml`, `Cargo.toml`, the
  Dockerfile's `org.opencontainers.image.version` label).

## 0.1.x

Initial Rust rewrite of the compiler: static AST scanning of Airflow DAG code
(`rustpython-parser`) into the `weave` intermediate document, with column
lineage inferred from embedded SQL and from statically-traced pandas/Spark
dataflow, and exporters to OpenMetadata, OpenLineage, and Graphviz DOT.
Included the stdlib-only `@tw.task` / `@tw.sql` authoring SDK for declaring
lineage the analyzer can't read, and CI (lint, tests, Docker build + CVE
scan, secret/dependency/IaC scanning). No `gate` command and no `@lineage`
decorator yet.
