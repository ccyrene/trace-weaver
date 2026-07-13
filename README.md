# trace-weaver

**A static, column-level data-lineage compiler for Apache Airflow DAGs.**

trace-weaver reads your DAG code **without executing it**, recovers
column-level lineage, and compiles it into a single intermediate document
(`*.weave.json`) that it can export to **OpenMetadata**, **OpenLineage** (Marquez
& friends), or a **Graphviz DOT** graph.

```
DAG code (.py)  ──scan──▶  weave IR (.weave.json)  ──export──▶  OpenMetadata
                  │                                              OpenLineage
              (no exec)                                          Graphviz DOT
```

The compiler is written in Rust (a Cargo workspace); a tiny, dependency-free
**Python authoring SDK** (`trace_weaver`) is included for the cases where you
want to *declare* lineage by hand. The SDK is optional — see below.

---

## Highlights

- **Works on plain Airflow DAGs with zero annotation.** trace-weaver discovers
  tasks from raw operators — `PythonOperator(python_callable=…)` and SQL
  operators (`sql=`/`query=`) — then reads the embedded SQL and the pandas/Spark
  function body to derive column lineage on its own.
- **Auto-infer first, declare only the tail.** Column lineage is extracted from
  SQL (parsed) and from pandas/Spark dataflow (statically traced). You annotate
  only the spots the analyzer genuinely cannot read.
- **Provenance is first-class.** Every dataset, job, edge and column mapping
  records whether it was **declared** by a human or **inferred** by the compiler
  (and how). Exporters visibly tag inferred lineage so a guess is never mistaken
  for a hand-declared fact.
- **Nothing is silently dropped.** A pattern the analyzer can't trace becomes a
  precise `W_OPAQUE_COLUMN` diagnostic pointing at the line — not a missing edge.
- **Static & reproducible.** No DAG is ever run; no network call is made during a
  scan. The compiled document is deterministic.

---

## Quick start

```bash
# Build the compiler (produces target/release/trace-weaver).
cargo build --release

# Scan a plain Airflow DAG (no @tw needed). --service/--database/--schema expand
# bare table names into OpenMetadata FQNs (service.database.schema.table).
trace-weaver scan examples/dags \
  --service "Test Database" --database poc_db --schema public \
  -o build/lineage.weave.json --strict

# Inspect / validate the compiled document.
trace-weaver validate build/lineage.weave.json

# Export.
trace-weaver export --to openmetadata --dry-run build/lineage.weave.json
trace-weaver export --to openlineage  -o build/events.json build/lineage.weave.json
trace-weaver graph  build/lineage.weave.json -o build/lineage.dot   # → DOT
```

[`examples/dags/medallion.py`](examples/dags/medallion.py) is a fully
un-annotated `landing → bronze → silver → gold` DAG. Scanning it yields
**4 datasets · 3 edges · 17 column mappings · 0 diagnostics** — bronze & gold
inferred from SQL, silver inferred from the pandas body.

---

## How it works

A Cargo workspace, one concern per crate:

| Crate                  | Responsibility |
|------------------------|----------------|
| `trace-weaver-core`    | The weave IR (`WeaveDocument` / `Dataset` / `Job` / `Edge` / `ColumnEdge`), the `Origin` provenance type, graph helpers, and structural validation. Aligned with the OpenLineage spec. |
| `trace-weaver-scan`    | DAG code → IR. AST parsing (`python.rs`), SQL column lineage (`sql.rs`), pandas/Spark dataflow analysis (`dataflow.rs`), same-name gap-fill (`infer.rs`). |
| `trace-weaver-export`  | IR → catalogues: `openmetadata.rs`, `openlineage.rs`, `dot.rs`. One `export(target, doc, &cfg)` entry point. |
| `trace-weaver-cli`     | The `trace-weaver` binary: `scan` / `validate` / `export` / `graph`. |

**Scan pipeline** (per file, literals only — no code is executed):

1. Parse the source AST (`rustpython-parser`).
2. **Discover tasks.** (A) `@tw.task` / `@tw.sql` decorated functions, and
   (B) decorator-free raw Airflow operators. A `@tw` decorator on a function
   *overrides* its decorator-free discovery (deduped by name).
3. For SQL steps, parse the query for column lineage (`inferred from SQL`).
4. For pandas/Spark steps, statically trace the function body
   (`inferred from code`); fill remaining same-name gaps by identity.
5. Finalize: back-fill dataset schemas from observed columns, flag any
   data-producing edge that ended up with no column lineage
   (`W_NO_COLUMN_LINEAGE`), and run structural validation.

---

## Provenance: declared vs. inferred

The compiler treats your declarations as the source of truth and only *fills
gaps*. Every element carries an `Origin`:

| Tier            | Source | ~confidence | When |
|-----------------|--------|:-----------:|------|
| **Declared**    | a human, in a `@tw.task(...)` `column_map` | — (authoritative) | never overwritten |
| **Inferred from SQL**  | parsing an embedded SQL query  | `0.85` | `engine="sql"` + a query is present |
| **Inferred from code** | statically tracing a pandas/Spark body | `0.70` | `df["c"]=…`, `withColumn`, `select`, `groupby/agg`, `rename`, `expr("…")`, … |
| _(identity gap-fill)_  | conservative same-name passthrough     | `0.40` | a target column sharing a name with an undeclared input column |

Exporters append `(inferred from SQL)` / `(inferred from code)` to inferred
labels, so a human reading the lineage can always tell declared truth from a
machine guess.

On a **plain (un-annotated) DAG**, the lineage is inferred top to bottom: not
only the columns but the **datasets, jobs and edges themselves** are tagged
inferred — because their very existence was recovered by static analysis, not
hand-declared. A `@tw`-declared task, by contrast, keeps a `declared` origin and
only its undeclared columns are inferred.

---

## What the scanner reads automatically (and what it can't)

**Auto (no annotation):** SQL operators; `read_sql`/`to_sql`/`spark.read.table`/
`saveAsTable`; `df["c"]=expr`, arithmetic / cast / `.map` / comparisons; fan-in;
`rename` / `assign` / `df[[...]]`; `withColumn` / `withColumnRenamed` / `select`
/ `selectExpr` / `expr()`; `groupby`/`agg`; literal-list loops; inline
`apply(lambda …)`; and column names from a **local string constant**
(`col = "amount_usd"; out[col] = …`).

**Opaque → `W_OPAQUE_COLUMN` (declare or refactor):** column names from a
**runtime** value (`out[x]=…` where `x` isn't a compile-time constant;
`df.columns=[...]`; `pivot`/`melt`/`explode`); named UDFs / `.rdd` / `.pipe`;
join columns that aren't keys; SQL or loops built at runtime (f-strings /
`.format`).

The do/don't guide with per-case rewrites is in
[`TRACEABLE_PIPELINES.md`](TRACEABLE_PIPELINES.md).

---

## CLI reference

```text
trace-weaver scan <path> [-o out.weave.json] [--namespace NS]
                         [--service S --database D --schema SC]
                         [--no-sql-infer] [--no-code-infer] [--strict]
trace-weaver validate <doc.weave.json> [--strict]
trace-weaver export --to <openmetadata|openlineage|dot> <doc.weave.json>
                    [--dry-run] [-o out] [--om-host URL]
                    [--om-token T | --om-token-file PATH] [--om-service S]
                    [--ol-producer URI] [--timeout S] [--retries N]
                    [--fail-on-partial]
trace-weaver graph <doc.weave.json> [-o out.dot]      # shortcut for export --to dot
trace-weaver gate --repo-path P [--git-ref R]
                  [--min-task-coverage F] [--min-high-confidence F]
                  [--format text|json]
```

- **`scan`** walks a file or directory of `.py` DAGs and writes the weave IR.
  `--service/--database/--schema` supply OpenMetadata FQN parts for DAGs without
  a `tw.configure(...)`. `--no-sql-infer` / `--no-code-infer` disable a tier.
- **`validate`** runs structural checks (unknown datasets, duplicate names,
  off-endpoint columns, …).
- **`export`** sends the document to a catalogue. `--dry-run` builds the request
  bodies / artifact and performs **no** network I/O.
- **`gate`** scans a repo and fails CI when lineage coverage/confidence falls
  below a threshold (see [CI lineage gate](#ci-lineage-gate) below).
- **Exit codes:** `0` success; `1` on error or a tripped `--strict` /
  `--fail-on-partial` gate.
- **OpenMetadata auth:** the ingestion-bot JWT is read from `--om-token-file`,
  then `--om-token`, then the `OPENMETADATA_BOT_TOKEN` environment variable.
  Prefer the file or env var — a raw `--om-token` is visible in your shell
  history.

### Export targets

- **`openmetadata`** (`om`) — PUTs `add_lineage` edges (with per-column
  `columnsLineage` and `function` labels) to `{host}/v1/lineage`; prints the
  request bodies under `--dry-run`.
- **`openlineage`** (`ol`) — emits `COMPLETE` `RunEvent`s carrying the
  `columnLineage` dataset facet, as a JSON array (file-only).
- **`dot`** (`graphviz`) — a Graphviz DOT graph; edges with any inferred lineage
  are drawn dashed/orange.

### CI lineage gate

`trace-weaver gate` turns a scan into a pass/fail check so a PR can be blocked when
lineage regresses. It scans `--repo-path` (optionally as of `--git-ref`) and
compares two metrics against thresholds:

- **`task_coverage`** — fraction of tasks (jobs) that carry at least one lineage
  edge.
- **`high_confidence_fraction`** — fraction of edges that are **declared**
  (high confidence) rather than inferred from SQL/code or reconstructed from a
  non-literal `@lineage` dataset.

```bash
# Fail the build if fewer than 80% of tasks have lineage, or under 50% of
# edges are declared. The JSON format adds a per-DAG breakdown.
trace-weaver gate --repo-path dags --min-task-coverage 0.8 --min-high-confidence 0.5
trace-weaver gate --repo-path dags --format json
```

Thresholds may also come from the environment — `TRACEWEAVER_MIN_TASK_COVERAGE`
and `TRACEWEAVER_MIN_HIGH_CONFIDENCE` — and an explicit flag always wins over the
env var. **Exit codes:** `0` pass, `1` a threshold failed (the failing metric is
printed), `2` usage error (bad path, unparseable threshold, invalid `--format`).

---

## Lineage decorator (`@lineage`)

For **dataset-level** lineage that the code analyzer can't see (an external API,
an opaque UDF, a step that shells out), declare the datasets a task reads and
writes with the `@lineage` decorator. Like the rest of the SDK it is a **runtime
no-op** — it returns your function unchanged and only attaches
`__traceweaver_lineage__` — and the scanner reads it statically without importing
your module.

```python
from trace_weaver import lineage

@lineage(
    inputs=["s3://acme-raw/sales/{ds}/events.parquet"],   # {ds} template is fine
    outputs=["iceberg://warehouse.sales.bronze_events"],
    name="ingest_sales_events",          # optional: overrides the task name
    description="Land raw S3 events into bronze.",
)
def build_bronze():
    ...

@lineage            # bare form: marks the function, declares no datasets
def touch():
    ...
```

- `inputs` / `outputs` are lists of dataset **URI strings** (`s3://`, `iceberg://`,
  `postgresql://`, `mongodb://`, `file://`, or an Airflow conn-id ref). A string
  may contain `{placeholders}` — it is still treated as one declared dataset.
- The scanner recognises every import form: `from trace_weaver import lineage`,
  `... import lineage as X`, `import trace_weaver` + `@trace_weaver.lineage`, and
  `import trace_weaver as tw` + `@tw.lineage`. It also works **stacked with
  Airflow's `@task`** in any order.
- **Confidence:** a string-literal dataset is **declared / high confidence**; a
  non-literal entry (an f-string, a variable, a call) is kept as a best-effort
  textual representation and marked **medium confidence** (inferred) so the gate
  and exporters can tell it apart.

See [`examples/sample_dags/declared_lineage.py`](examples/sample_dags/declared_lineage.py)
for a full DAG.

---

## Optional: the Python authoring SDK (`@tw`)

You rarely need it — the compiler infers lineage from SQL and pandas/Spark code.
Reach for the SDK only to **declare** a column the analyzer flagged opaque, or to
attach a description/transform label. The decorator is a **runtime no-op**: it
returns your function unchanged, so your DAGs run exactly as before.

```bash
cd python && pip install -e .        # stdlib only, Python 3.9+
```

```python
import trace_weaver as tw

# Per-file defaults: FQN parts + dag, so you can use bare table names below.
tw.configure(service="Test Database", database="poc_db", schema="public",
             dag="medallion_lineage")

BRONZE_SQL = "SELECT CAST(raw_event_id AS BIGINT) AS event_id FROM landing_sales"

# SQL step — column lineage auto-derived from the query (no column_map needed).
@tw.sql(BRONZE_SQL, inputs=["landing_sales"], outputs=["bronze_sales"],
        transform="CAST / DEDUPE")
def build_bronze():
    ...

# pandas/Spark step — declare only the columns the analyzer can't trace.
@tw.task(
    inputs=["bronze_sales"],
    outputs=["silver_sales"],
    column_map=[
        # (sources, target, function); sources may be a bare string for one source
        (["amount", "currency"], "amount_usd", "ROUND(amount * fx[currency], 2)"),
    ],
    copy=["event_id", "customer_name"],   # same-name passthrough → declared identity
)
def build_silver():
    ...
```

**Decorator reference** — `@task(dag=None, inputs=None, outputs=None,
engine=None, sql=None, description=None, transform=None, column_map=None,
copy=None, **kwargs)`:

| arg           | type                              | notes |
|---------------|-----------------------------------|-------|
| `dag`         | `str`                             | DAG / pipeline id (falls back to `configure(dag=)` or a `with DAG(...)` block) |
| `inputs`      | `list[str \| Dataset]`            | source dataset FQNs (bare names expand via `configure`) |
| `outputs`     | `list[str \| Dataset]`            | output dataset FQNs |
| `engine`      | `"sql"\|"pandas"\|"spark"\|"python"\|"bash"` | optional; inferred (`sql` if a query is present, else `python`). Unknown values tolerated |
| `sql`         | `str` or module-level const name  | parsed when `engine="sql"` |
| `description` | `str` or const name               | rich (markdown) edge description |
| `transform`   | `str`                             | short transform-kind label |
| `column_map`  | `list[(sources, target, function)]` | declared, authoritative lineage; `sources` may be a list **or a bare string** |
| `copy`        | `list[str]`                       | same-name passthrough columns — each a declared identity; `column_map` wins on conflict |
| `**kwargs`    | anything                          | tolerated & recorded for forward-compat |

`@tw.sql(QUERY, ...)` is sugar for `@task(engine="sql", sql=QUERY, ...)`.
`inputs` + `outputs` are the meaningful minimum; everything else is optional.
Datasets use OpenMetadata style `service.database.schema.table` (the *service*
segment may contain spaces). Every declaration is also appended to `tw.registry`
(a `list[trace_weaver.Declaration]`) for optional runtime introspection — the
decorator's only side effect.

---

## Build, test & lint

```bash
cargo build --release                                            # → target/release/trace-weaver
cargo test --workspace                                           # Rust test suite
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
ruff check python examples                                       # Python SDK + examples
```

Rust 2021 edition, MSRV 1.80.

## Publishing (Docker Hub)

`.github/workflows/publish.yml` builds the production image and pushes it to
Docker Hub as `docker.io/$DOCKERHUB_USERNAME/trace-weaver` (tags `{version}` and
`latest`). It runs on a pushed `v*` tag and via manual `workflow_dispatch`. It
requires two **repository secrets**:

| secret               | purpose                                            |
|----------------------|----------------------------------------------------|
| `DOCKERHUB_USERNAME` | Docker Hub account/namespace to push under         |
| `DOCKERHUB_TOKEN`    | Docker Hub access token (used by `docker/login-action`) |

The existing `ci.yml` gates are unchanged; publishing is a separate workflow.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
