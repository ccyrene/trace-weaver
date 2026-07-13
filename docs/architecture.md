# Architecture

trace-weaver is a **static** data-lineage compiler. It reads annotated Python
Airflow DAGs and produces the `weave` universal lineage document — it **never
imports or executes** the scanned code. Everything is recovered by parsing the
AST and any embedded SQL.

## Crates

| crate                   | responsibility                                                                 |
|-------------------------|--------------------------------------------------------------------------------|
| `trace-weaver-core`     | the `weave` model (`WeaveDocument`, `Dataset`, `Job`, `Edge`, `ColumnEdge`) and `Origin` provenance; graph + validation helpers |
| `trace-weaver-scan`     | AST scanning (`python.rs`), SQL column lineage (`sql.rs`), pandas/Spark dataflow (`dataflow.rs`), and assembly into a document (`lib.rs`) |
| `trace-weaver-export`   | exporters: OpenMetadata, OpenLineage, Graphviz DOT                             |
| `trace-weaver-cli`      | the `trace-weaver` binary: `scan`, `validate`, `export`, `graph`, `gate`       |

The `python/` directory is a separate, stdlib-only **authoring SDK**
(`trace_weaver`) providing the `@tw.task` / `@tw.sql` / `@lineage` decorators.
The SDK is *only* a declarative, statically-parseable convention: every
decorator is a runtime no-op that returns the wrapped function unchanged. The
scanner reads the decorators from source and does not depend on the SDK.

## Provenance (`Origin`)

Every dataset, job, edge and column mapping carries an `Origin`:

- **Declared** — hand-authored (`@tw.task`, `@lineage`, a literal dataset). This
  is the "high confidence" tier and is never overwritten by inference.
- **InferredSql** — parsed from an embedded SQL query.
- **InferredCode** — best-effort pandas/Spark dataflow, or a non-literal
  `@lineage` dataset kept as best-effort text ("medium confidence").

## The `@lineage` decorator

`@lineage(inputs=[...], outputs=[...], name=None, description=None)` declares
**dataset-level** lineage for tasks whose data movement the code analyzer cannot
see. It is purely declarative metadata: at runtime it attaches
`__traceweaver_lineage__` and returns the function unchanged.

The scanner (`python.rs`) recognises it under all import forms (bare name,
`import ... as`, and `module.lineage` attribute), builds a `TaskDecl` with the
declared inputs/outputs, and `lib.rs` turns it into datasets + a job + edges via
the same machinery used for every other task. Literal dataset entries are stamped
**declared**; non-literal entries (f-strings, names, calls) are kept as their
source text and stamped **inferred (medium)** so the gate and exporters can tell
declared truth from a reconstruction.

## The `gate` command

`trace-weaver gate` reuses `scan_path`, then computes CI metrics over the
resulting document — `task_coverage` (tasks carrying lineage) and
`high_confidence_fraction` (declared vs. inferred edges), with a per-DAG
breakdown — and compares them against thresholds (flags, or the
`TRACEWEAVER_MIN_*` env vars). It exits `0` (pass), `1` (a threshold failed) or
`2` (usage error). It adds no state and imports no code; it is a read-only view
over a scan.
