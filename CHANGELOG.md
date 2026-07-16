# Changelog

All notable changes to `trace-weaver` (the Rust CLI and the `trace_weaver`
Python authoring SDK, which share one version number) are documented here.

## 0.6.1

### Fixed

- **Column-discovery schema pollution across unrelated files (the real "~1000
  cols/edge" cause).** `finalize()`'s `backfill_schemas` + `code_inference_pass`
  union columns onto a dataset's schema and gap-fill identity mappings back
  from it, keyed purely by dataset *name*. Pass C's synthetic leftover frame
  names — `tw_tmpl` (the hardcoded template placeholder) and common, coincidentally-
  reused temp-view names like `raw_staging` — are not unique per function, so
  this silently unioned every unrelated function's columns that happened to
  share one of these generic names into one shared schema, then handed that
  union back to every edge touching the name. Verified live against the DII
  pilot repo: a single, unbranched 12-column file's own edge carried 1088
  entries, including columns from completely unrelated ministries (confirmed
  by isolating the file: scanned alone it correctly reports 12; scanned with
  the rest of the corpus, 1088). `backfill_schemas` and `code_inference_pass`
  now skip `column_discovery` edges, so a real dataset's schema still gets
  backfilled from every task/declared edge that names it, but Pass C's
  synthetic frames never contribute to (or draw from) that corpus-wide pool.
  Repo-wide, this dropped `column_mappings` from 99,633 to 2,052 with zero
  change to `dataset`/`declared-edge`/`annotated-job`/`module-column-coverage`
  (verified via the central repo's ref-diff no-loss integration test) — the
  99,631 difference was 100% fabricated, none of it real coverage.
- **Multi-branch column duplication (the smaller, real half of the same
  symptom).** `Analyzer::finish()` deduped `outputs` but never `columns`, and
  `stmt()`'s `Stmt::If` arm walks both `body` and `orelse` unconditionally
  (`elif` chains nest inside `orelse`), so a mapping re-derived byte-for-byte
  in several mutually-exclusive branches (the real shape in
  `mot_ops/public_transport_fare_assistance.py`'s 5-branch dispatch) was
  pushed once per branch. `columns` is now deduped by full field equality
  (after `output_table` is stamped) — a true duplicate (same sources, same
  target, same producing expression) collapses to one; two branches that
  merely share a target column but differ in source or expression (a literal
  fill vs. a real rename) still both survive as distinct entries.

## 0.6.0

### Added

- **Column-edge attribution.** Pass C (column/dataflow discovery) traces
  undecorated, un-wired top-level functions purely for column lineage — but
  when such a function's synthetic frame names ("`raw_staging`", "`tw_tmpl`",
  a temp-view name) carry no job, no file/line, no relationship to the real
  dataset URIs a co-located `@lineage`/`@tw.task` declaration names, exporters
  and downstream tooling had nothing to join the two on and dropped the
  mapping. Two changes close this gap:
  - **Full attribution.** When a Pass-C function is called *directly* (a plain
    top-level `helper(df)` call — not `python_callable=`, which Pass B already
    claims) from the body of a declared task, its column mappings are now
    resolved onto that task's REAL declared edge instead of a synthetic frame:
    unambiguous when the caller declares exactly one input and one output
    (the synthetic frame names are simply irrelevant then), or when the
    analyzer named a table that matches exactly one of several declared
    endpoints by its final FQN segment. The synthetic frame dataset/edge is
    then never emitted for that flow. Column entries keep
    `origin.source = "inferred_code"` and their confidence — only their
    *placement* becomes declared-anchored; they gain an `origin.location`
    (file/line) pointing at the callee's actual code.
  - **Minimum viable provenance (fallback).** Whatever isn't attributed —
    because no static caller exists at all (the dominant real-world shape: a
    `SparkSubmitOperator` / `importlib`-dispatched transform module, invisible
    to static analysis by construction), or because the caller declares
    several inputs AND several outputs with no name match to pair against —
    now carries `origin.location` (`file`, `line`) on both the synthetic frame
    dataset and the column-discovery edge, plus `job` (the caller's task id)
    whenever a caller was found even without a resolvable pairing. Pass B's
    decorator-free task discovery gets the same `origin.location` stamp on its
    structural dataset/edge origins. All additive to `Origin` — deserializes
    fine against documents written before this field existed, and
    `weave_version` stays `"0.1"`.

  On the reference central-repo corpus every one of the 122 column-discovery
  edges keeps its current shape: none of them are reachable from a declared
  task via a static call (the dispatch is `SparkSubmitOperator` +
  `transform_master.py`'s runtime module resolution, per
  `dii/services/warehouse/spark.py`'s own review note) — so full attribution
  correctly declines to guess anywhere on that repo, while still adding
  file/line to all 122 edges and their synthetic frame datasets. The feature
  is exercised by dedicated fixtures instead: a single-input/single-output
  declared task calling a helper directly (full attribution), and a
  multi-input/multi-output declared task calling a helper whose frame name
  doesn't match any declared endpoint (ambiguous → fallback with `job` set).

## 0.5.1

### Fixed

- **Gate metric separation: column-discovery edges no longer poison the task
  confidence ratio.** v0.5's column-discovery pass (Pass C) folded every
  inferred, job-less column-lineage hop into the gate's `edges_total` /
  `high_confidence_edges`. On a real repo this added 121 inferred edges beside
  the 29 declared task edges, collapsing `high_confidence_fraction` from
  `1.0` (29/29) to `0.193` (29/150) and tripping a CI gate at
  `--min-high-confidence 0.30` — even though nothing about the *declared*
  lineage had changed. Column-discovery edges now carry an explicit
  `column_discovery` marker in the weave model and are measured in their own
  dimension: the gate reports them as **report-only** `column_edges` /
  `column_mappings` (with per-DAG equivalents) and excludes them from
  `edges_total`, `high_confidence_edges` and `high_confidence_fraction`. Those
  three remain pure task/declared-scope metrics. This is the same separation the
  v0.3 annotation split established — discovery in one dimension must never
  dilute another dimension's ratio. No new thresholds were added.

### Added

- **`MERGE INTO` column lineage (Spark / Iceberg / Delta upserts).** SQL column
  extraction previously handled only `INSERT` and bare `SELECT`. It now maps a
  `MERGE INTO target USING source ON … WHEN MATCHED THEN UPDATE SET c = expr …
  WHEN NOT MATCHED THEN INSERT (cols) VALUES (exprs)` statement, recovering both
  the `UPDATE SET` assignment flows and the positional `INSERT … VALUES` flows
  (deduplicated per target column, keeping the UPDATE mapping). Source-side
  references qualified with the source relation's alias (e.g. `source.c`) are
  de-qualified so single-source resolution attaches them to the real input.
  This recovers lineage from the common Iceberg composite-key upsert shape and
  fixes the one central-extract module (`moe_ops/student_information.py`) that
  previously yielded zero column mappings.

### Known limitations

- ~~**Multi-branch column over-generation (targeted for v0.6).**~~ **Fixed, see
  Unreleased above.** The actual mechanism turned out to be broader than this
  entry described: it was corpus-wide schema pollution across unrelated files
  sharing a generic synthetic frame name (`backfill_schemas`/
  `code_inference_pass`), not merely branch fan-out within one edge — a real
  intra-function branch-duplication bug existed too, but was the smaller
  contributor. Original text, for the record: "When a column-discovery
  function fans a single dataset→dataset edge out across many output
  branches, the analyzer can attach an inflated column-mapping set to that
  edge (observed on the order of ~1000 mappings per edge on some central
  modules). This does not affect the gate's task/declared metrics — such
  edges are counted only in the report-only `column_mappings` dimension —
  but it inflates that count and downstream column-lineage exports."

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
