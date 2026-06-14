# TraceWeaver

TraceWeaver is a static lineage scanner for Apache Airflow DAG repositories.

It reads Python DAG code **without importing or executing Airflow**, extracts
DAG / task / dependency / function / SQL metadata, and exports lineage
candidates as CSV, JSON, or directly into PostgreSQL.

## What it extracts

- **Jobs** — `dag_id`, `task_id`, operator class, python callable, file, line.
  Handles `with DAG(...) as dag:`, `dag = DAG(...)`, the `@dag` / `@task`
  TaskFlow API, and resolves the owning DAG via the `dag=` keyword or an
  ambient single-DAG file.
- **Task dependencies** — `>>`, `<<`, list fan-out, `chain(...)`,
  `set_upstream` / `set_downstream`.
- **SQL lineage** — source/target tables via [`sqlglot`](https://github.com/tobymao/sqlglot)
  (dialect-aware, high confidence) with a regex fallback when sqlglot is absent.
- **Datasets** — object-store URIs (`s3://`, `gs://`, `wasb://`, `hdfs://`, …),
  local data files (`.csv`, `.parquet`, …), DB tables, and Airflow connection ids.
- **Function call graph** — static caller→callee edges, attributed to the
  owning dag/task where resolvable.

Outputs: `lineage_jobs.csv`, `lineage_datasets.csv`, `lineage_edges.csv`,
`task_dependencies.csv`, `function_calls.csv`, `raw_scan_result.json`, and a
ready-to-view **Mermaid** graph (`lineage.mmd`).

## Visualize the lineage (Mermaid)

`--output mermaid` (also included in `--output all`, the Docker default) writes
`lineage.mmd`: a single left-to-right Mermaid `flowchart`, one labelled
`DAG: <id>` subgraph per DAG.

```bash
traceweaver scan --repo-path ./dags --output mermaid --output-dir ./out
```

Each task is a **card** whose title is the task name and whose body lists, in
call order, the business functions that task invokes. Cards are linked left to
right by their dependencies (thick `==>`). Data lineage is shown as datasets
flowing in/out of the cards — tables are cylinders, object-store/file URIs are
parallelograms, connections are hexagons; low-confidence edges are dotted
(`-.->`).

View `lineage.mmd` at <https://mermaid.live>, in a Mermaid-aware editor, or
render it to an image with `mmdc -i lineage.mmd -o lineage.svg`.

### Optional: render to PNG/SVG in the image

The default image is lean and writes only `lineage.mmd`. To get an image that
renders `lineage.svg` + `lineage.png` itself (bundles Node.js + Chromium +
mermaid-cli), build `Dockerfile.render` and use `--image-format`:

```bash
docker build -f Dockerfile.render -t traceweaver:render .
docker run --rm -v "$PWD/dags:/dags:ro" -v "$PWD/out:/out" traceweaver:render
```

| image | size (uncompressed / pull) | renders PNG/SVG |
|---|---|---|
| `traceweaver:latest` (default, `Dockerfile`) | **93 MB / 29 MB** | no — writes `lineage.mmd` |
| `traceweaver:render` (`Dockerfile.render`) | ~2.4 GB | yes (mermaid-cli + Chromium) |

For programmatic graphs, the `raw_scan_result.json` / CSVs also load directly
into Neo4j, Marquez/OpenLineage, or any graph tool.

## Quick start with Docker (recommended)

Build the image, then mount a DAGs folder and scan it — no local Python needed:

```bash
docker build -t traceweaver:latest .

docker run --rm \
  -v "/path/to/your/dags:/dags:ro" \
  -v "$PWD/out:/out" \
  traceweaver:latest
```

The image scans `/dags` and writes CSV + JSON + the Mermaid graph
(`lineage.mmd`) to `/out` by default. To get host-owned output files, add
`--user "$(id -u):$(id -g)"`.

Override the defaults with normal CLI args, e.g. CSV only:

```bash
docker run --rm -v "$PWD/dags:/dags:ro" -v "$PWD/out:/out" \
  traceweaver:latest scan --output csv
```

### docker compose

```bash
mkdir -p dags out
cp /path/to/your/dags/*.py dags/

# CSV + JSON into ./out
docker compose run --rm traceweaver

# Or scan straight into a throwaway Postgres (schema auto-created)
docker compose --profile db up --build
```

## Quick start with Python

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[sql]"        # add ,db for PostgreSQL export

traceweaver scan --repo-path examples/sample_dags --output all --output-dir outputs
```

Run without installing:

```bash
python -m traceweaver scan --repo-path examples/sample_dags --output json --output-dir outputs/json
```

## CLI

```bash
traceweaver scan --repo-path ./dags --output csv  --output-dir ./outputs
traceweaver scan --repo-path ./dags --output json --output-dir ./outputs
traceweaver scan --repo-path ./dags --output mermaid --output-dir ./outputs  # graph
traceweaver scan --repo-path ./dags --output all  --output-dir ./outputs   # csv + json + mermaid
traceweaver scan --repo-path ./dags --output summary                       # print only

# Scan a remote repository (shallow clone, needs git)
traceweaver scan --repo-path https://github.com/org/dags.git --git-ref main --output all

# Export into PostgreSQL
traceweaver scan --repo-path ./dags --output db \
  --database-url postgresql://lineage:lineage@localhost:5432/lineage
```

Every flag has an environment-variable default, which is what makes the Docker
image work with no arguments:

| flag | env var | default |
|---|---|---|
| `--repo-path` | `TRACEWEAVER_REPO_PATH` | _(required)_ / `/dags` in image |
| `--output` | `TRACEWEAVER_OUTPUT_FORMAT` | `csv` / `all` in image |
| `--output-dir` | `TRACEWEAVER_OUTPUT_DIR` | `outputs` / `/out` in image |
| `--database-url` | `TRACEWEAVER_DATABASE_URL` | _(none)_ |
| `--git-ref` | `TRACEWEAVER_GIT_REF` | _(none)_ |

## Optional dependencies

- `pip install ".[sql]"` — `sqlglot`, for dialect-aware SQL lineage.
- `pip install ".[db]"` — `sqlalchemy` + `psycopg`, for `--output db`.
- `pip install ".[all]"` — both. (The default Docker image bundles only `sql`;
  use `Dockerfile.render` or `.[all]` locally for DB export.)

Without these extras the scanner still runs on the Python standard library and
falls back to regex-based SQL extraction.

## Limitations

TraceWeaver is a **static** scanner. It does not execute DAG files, so it cannot
fully resolve dynamically generated DAGs, runtime-built SQL, XCom-driven paths,
or values from secret managers. Results are lineage *candidates* with confidence
levels (`high` for sqlglot-parsed SQL, `medium`/`low` for heuristics). Pair with
OpenLineage for runtime-confirmed lineage. See [docs/limitations.md](docs/limitations.md).

## Development

```bash
pip install -e ".[dev,all]"
make test           # unittest discover
pytest -q           # or pytest
```

See [docs/output-schema.md](docs/output-schema.md) and
[docs/architecture.md](docs/architecture.md) for details.
