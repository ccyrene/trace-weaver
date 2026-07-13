# Changelog

All notable changes to `trace-weaver` (the Rust CLI and the `trace_weaver`
Python authoring SDK, which share one version number) are documented here.

## 0.5.0

### Added

- **f-string SQL folding.** The pandas/Spark dataflow analyzer now folds an
  `Expr::JoinedStr` argument to `spark.sql(f"…")` before parsing it. Literal
  segments and v0.4-resolvable interpolations (`{NAME}` where `NAME` is a
  module-level constant or a local string variable) fold to their value;
  genuinely runtime interpolations (function parameters like
  `{full_table_name}`) fold to a stable placeholder identifier so the
  fully-static `SELECT`/`CAST` column list still parses. This unlocks
  column-level lineage from the dominant real-world shape
  `spark.sql(f"INSERT INTO {full_table_name} SELECT CAST(\`X\` AS STRING), … FROM staging")`,
  including the `INSERT OVERWRITE` and `CREATE TABLE` variants. An
  `INSERT … SELECT` with **no** explicit column list now names each projection
  by its underlying column even through a `CAST(...)` wrapper, so those casts
  still map `X <- X`.
- **Undecorated transform discovery (Pass C) — column lineage, not tasks.** A
  new discovery pass reaches module-top-level functions that neither a
  trace-weaver decorator (Pass A) nor an Airflow operator (Pass B) claimed —
  notably the `def run(spark, src_path, …)` transform protocol dispatched via
  `importlib` at runtime. Their bodies are traced purely for **column
  mappings**: each yields datasets and column-carrying edges but **no job**, so
  the lineage gate's task denominator (`tasks_total` / `tasks_with_lineage` /
  `tasks_annotated` and both coverage metrics) is unchanged — this is column
  discovery, not task discovery.
- **`createOrReplaceTempView` is modeled as a frame binding.**
  `df.createOrReplaceTempView("v")` now binds the view name to the frame's
  upstream table, so a later `spark.sql("… FROM v")` chains column lineage back
  through `df` (or keeps the view name as the source dataset when the frame's
  origin is unknown).

### Changed

- **Dataflow analysis is no longer suppressed under `@lineage`.** A
  `@lineage`-decorated function's declared datasets stay **declared / HIGH**
  confidence, but its body is now also traced so inferable column mappings
  (e.g. a literal-dict `.rename()`, a `spark.sql` INSERT) attach beneath the
  declaration. Previously the whole dataflow pass was skipped for `@lineage`,
  silently discarding those mappings.

## 0.4.0

### Added

- **Module-level string constants resolve in lineage declarations.** Dataset
  URIs (and OM FQNs) in `@lineage` / `@tw.task` / `@tw.sql` `inputs=`/`outputs=`
  may now reference a module-level string constant instead of an inline literal,
  and still resolve to **declared / HIGH confidence** — so teams can centralize
  URIs in a shared `config/datasets.py` without dropping to inferred/MEDIUM.
  Before scanning any file, `scan_path` builds a repo-wide constant symbol table
  (`ConstTable`) keyed by the dotted module path a file would be imported under
  (`config/datasets.py` → `config.datasets`), collecting every module-level
  `NAME = "literal"` assignment. One-level `NAME = OTHER_NAME` aliasing is
  followed too (bounded depth, cycle-guarded). The supported reference forms are:
  - a bare `NAME` defined in the **same** module;
  - `from pkg.mod import NAME` (and `... import NAME as ALIAS`);
  - `import pkg.mod [as m]` followed by `m.NAME` / `pkg.mod.NAME` attribute access.

  Anything that is not a compile-time string — a missing/undefined name, a
  function call, an f-string with placeholders, a subscript, or a cyclic alias —
  keeps today's behavior: the source text is preserved and the endpoint is
  stamped **medium / inferred** (for `@lineage`) or dropped with a
  `W_NON_LITERAL` diagnostic (for `@tw.task`/`@tw.sql`). The scanner never
  guesses a value it cannot see statically.

## 0.3.0

### Added

- **`annotation_coverage` gate metric** — a second, complementary gate
  dimension alongside `task_coverage`. `tasks_annotated` counts tasks
  synthesized from an explicit trace-weaver decorator (`@tw.task` / `@tw.sql` /
  `@lineage`, **bare or called**); `annotation_coverage = tasks_annotated /
  tasks_total`. It measures **review** completeness (did a human mark the task
  at all) independent of whether the annotation produced a full input→output
  edge — so a bare `@lineage` marker or an inputs-only declaration counts as
  annotated even though it contributes no edge. `tasks_total` (the shared
  denominator) intentionally still includes Pass-B tasks discovered
  decorator-free from raw Airflow operators; those cannot carry a decorator, so
  they hold `annotation_coverage` below 1.0 and surface the un-reviewed surface
  a human still owes. New flag `--min-annotation-coverage FLOAT` (default
  `0.0`) with env fallback `TRACEWEAVER_MIN_ANNOTATION_COVERAGE` (flag wins);
  the JSON report gains `tasks_annotated`, `annotation_coverage`, the
  `thresholds.min_annotation_coverage` / `checks.annotation_coverage` fields
  (and the same two fields on every `per_dag` entry), and the text report gains
  a PASS/FAIL line.

### Changed

- **Declared self-loop edges are now emitted.** When a dataset pair comes from
  an explicit declaration (`Origin::Declared`) and `input == output`, the edge
  derivation now emits a self-edge (e.g. a `@lineage(inputs=["s3://x"],
  outputs=["s3://x"])` task that reads a prefix and deletes orphans in place)
  instead of silently skipping it. Self-loops on **inferred** (decorator-free,
  Pass-B discovered) pairs remain skipped to avoid noise. `task_coverage` keeps
  its exact edge-based semantics — a declared self-loop is a real edge and now
  legitimately counts toward it. All exporters (json / dot / openlineage /
  openmetadata) emit valid output for a self-edge.
- Version bumped to `0.3.0` (`Cargo.toml`, the Dockerfile's
  `org.opencontainers.image.version` label). The `trace_weaver` Python SDK is
  unchanged and stays at its previous version.

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
