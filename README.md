# trace-weaver

A tiny, **dependency-free** Python authoring SDK for declaring column-level data
lineage on your Airflow tasks — the **trace-weaver** convention.

You annotate each data-producing task with `@tw.sql(...)` (SQL steps) or
`@tw.task(...)` (pandas/Spark/python steps). The decorator is a **runtime
no-op** (it returns your function unchanged, so your DAGs run exactly as before),
but the arguments you pass are **statically parseable** literals that the trace-weaver
compiler reads straight from your source to build column-level lineage.

- Pure Python, **stdlib only**, Python **3.9+**.
- Importable as `from trace_weaver import task, sql, configure` or `import trace_weaver as tw`
  (`@tw.sql(...)`, `@tw.task(...)`).
- Adds **zero** runtime behaviour to your tasks.

## Install

```bash
cd python
pip install -e .
```

Or just put the `trace_weaver/` package on your `PYTHONPATH` — it has no dependencies.

## 30-second usage

```python
import trace_weaver as tw

# Once per file — sets FQN + dag defaults so you can use bare table names below.
tw.configure(service="Test Database", database="poc_db", schema="public",
               dag="medallion_lineage")

# Module-level SQL constant — the scanner resolves the `sql` ref to this text.
BRONZE_SQL = """
    SELECT
        CAST(raw_event_id AS BIGINT) AS event_id,
        ROUND(amount * fx_rate, 2)   AS amount_usd
    FROM landing_sales
"""

# SQL step — column lineage is auto-derived from the query, no column_map needed.
@tw.sql(
    BRONZE_SQL,                         # str literal OR a name of a module-level str const
    inputs=["landing_sales"],           # bare names → expanded to the configured FQN
    outputs=["bronze_sales"],
    description="### Bronze build\nCleans + casts landing rows.",
    transform="CAST / DEDUPE",
)
def build_bronze():
    ...  # your normal Airflow task body; runs unchanged
```

`engine=` is optional (inferred as `sql` from the query); `dag=` comes from
`configure(...)`. For a pandas/Spark/python step, use `@tw.task(...)` with a
`column_map` — the compiler can't trace that dataflow:

```python
@tw.task(
    inputs=["bronze_sales"],
    outputs=["silver_sales"],
    column_map=[
        # (sources, target, function)  — bare column names
        (["amount", "currency"], "amount_usd", "ROUND(amount * fx[currency], 2)"),
    ],
)
def build_silver():
    ...
```

## Dataset FQNs

Datasets use OpenMetadata style `service.database.schema.table`. The **service**
segment may contain spaces, e.g. `"Test Database.poc_db.public.bronze_sales"`.

With `tw.configure(service=…, database=…, schema=…)` set you write **bare table
names** (`"landing_sales"`) and the scanner expands them to the full FQN. Names
that already contain a `.` are passed through unchanged, so you can mix bare and
fully-qualified names — handy for multi-source pipelines (e.g. an S3 input and a
Redshift output in the same task). `inputs=` / `outputs=` also accept the
`Dataset` helper (sugar):

```python
import trace_weaver as tw
from trace_weaver import task, Dataset

tw.configure(service="Test Database", database="poc_db", schema="public")

@tw.sql(
    "SELECT raw_event_id AS event_id FROM landing_sales",
    inputs=[Dataset("landing_sales")],          # bare or full FQN; both work
    outputs=[Dataset("bronze_sales")],
)
def build_bronze():
    ...
```

Keep these literal (one string per `Dataset(...)`) so the static scanner can read
them without executing your code.

## Provenance: declared vs. inferred

The compiler treats your declarations as the source of truth and only *fills gaps*:

1. **`column_map` entries are DECLARED** — authoritative, never overwritten.
2. If `engine="sql"` and `sql=...` is present, the compiler **parses the SQL** to
   auto-derive lineage for any target column *not* already in `column_map`
   (tagged *inferred from SQL*).
3. For `pandas`/`spark`/`python` tasks the compiler **statically reads the function
   body** and traces column flow (`df["c"] = df["a"] * 2`, `withColumn`, `select`,
   `groupby/agg`, `rename`, `expr("…")`, …), tagged *inferred from code* (≈`0.7`).
   A conservative same-name identity gap-fill (≈`0.4`) fills anything still missing.

Exporters append `(inferred from SQL)` / `(inferred from code)` to inferred
labels, so a human can always tell declared truth from a machine guess.

> **You usually don't need a `column_map` anymore** — the compiler derives column
> lineage from the SQL and the pandas/Spark body. Patterns it can't read statically
> (dynamic column names, named UDFs / `.rdd`, joins, pivots) raise `W_OPAQUE_COLUMN` with
> the exact line; refactor to a traceable form or declare just that column. See
> [`TRACEABLE_PIPELINES.md`](TRACEABLE_PIPELINES.md) for the do/don't guide.

## Decorator reference

`@task(dag=None, inputs=None, outputs=None, engine=None, sql=None,
description=None, transform=None, column_map=None, copy=None, **kwargs)`

| arg           | type                              | notes |
|---------------|-----------------------------------|-------|
| `dag`         | `str`                             | DAG / pipeline id |
| `inputs`      | `list[str | Dataset]`             | source dataset FQNs |
| `outputs`     | `list[str | Dataset]`             | output dataset FQNs |
| `engine`      | `"sql"\|"pandas"\|"spark"\|"python"\|"bash"` | optional; inferred (`sql` if a query is present, else `python`). Unknown values tolerated |
| `sql`         | `str` or module-level const name  | parsed when `engine="sql"` |
| `description` | `str` or const name               | rich (markdown) edge description |
| `transform`   | `str`                             | short transform-kind label |
| `column_map`  | `list[(sources, target, function)]` | declared, authoritative lineage; `sources` may be a list **or a bare string** for a single source |
| `copy`        | `list[str]`                       | same-name passthrough columns — each a declared identity (`"direct copy"`); `column_map` wins on conflict |
| `**kwargs`    | anything                          | tolerated & recorded for forward-compat |

`inputs` + `outputs` are the meaningful minimum; everything else is optional.
`engine` is **inferred** (`sql` when a `sql=`/query is present, else `python`),
and `dag` falls back to `tw.configure(dag=…)` or a surrounding `with DAG(...)`
block — so you rarely pass either. A bare `@task` (no parentheses) is also
accepted and is likewise a no-op.

`@tw.sql(QUERY, ...)` is sugar for `@task(engine="sql", sql=QUERY, ...)`; on a
SQL step the compiler auto-derives column lineage from the query, so `column_map`
is optional there (any entries you do declare win, the rest are inferred from the
SQL). `tw.configure(service=…, database=…, schema=…, dag=…)` sets per-file
defaults used to expand bare table names and supply the `dag`.

### Runtime introspection (optional)

Every declaration is appended to `tw.registry` (a `list[trace_weaver.Declaration]`).
This is the decorator's only side effect and never changes behaviour — handy if
you want to inspect declarations in a running process.

## License

Apache-2.0.
