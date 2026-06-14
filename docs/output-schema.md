# Output Schema

## lineage_jobs.csv

| column | description |
|---|---|
| dag_id | Airflow DAG ID |
| task_id | Airflow task ID |
| operator_class | Operator class name |
| callable_path | Python callable name/path if detected |
| file_path | DAG source file |
| line_no | Line number |

## lineage_datasets.csv

| column | description |
|---|---|
| dataset_id | Generated stable ID |
| namespace | Optional namespace (e.g. `airflow_connection`) |
| name | Dataset name |
| dataset_type | table, s3, gcs, azure_blob, azure_datalake, hdfs, ftp, file, connection, unknown |
| uri | URI if detected |
| schema_name | DB schema if detected |
| table_name | DB table if detected |

## lineage_edges.csv

| column | description |
|---|---|
| dag_id | DAG ID |
| task_id | Task ID |
| source_dataset | Source dataset candidate |
| target_dataset | Target dataset candidate |
| extraction_method | sqlglot, sql_regex, dataset_pattern, conn_id |
| confidence | high, medium, low |

Confidence guide: `high` = sqlglot-parsed SQL; `medium` = regex-parsed SQL or a
fully paired source→target; `low` = single-sided heuristic (URI/conn/file hint).

## task_dependencies.csv

| column | description |
|---|---|
| dag_id | DAG ID |
| upstream_task_id | Task that runs first |
| downstream_task_id | Task that depends on the upstream |
| extraction_method | static_ast, taskflow_data |
| confidence | high |

Extracted from explicit ordering (`>>`, `<<`, list fan-out, `chain(...)`,
`set_upstream` / `set_downstream`) → `static_ast`, and from TaskFlow XCom
argument passing (a task receiving another task's output as an argument) →
`taskflow_data`.

## lineage.mmd (Mermaid)

A single Mermaid `flowchart LR` rendering of the scan, written by
`--output mermaid` (and `--output all`):

- Tasks are grouped into one `subgraph` per DAG, titled `DAG: <id>`.
- Each task is a **card**: a bold title (the task name) followed by a numbered
  list of the business functions it calls, in call order.
- Cards are linked left-to-right by task dependencies — thick arrows (`==>`).
- Data lineage flows through the cards as datasets: tables → cylinders
  `[(name)]`, object-store/file URIs → parallelograms `[/name/]`, Airflow
  connections → hexagons `{{name}}`; edges are thin (`-->`), dotted (`-.->`)
  when confidence is `low`.

`lineage.mmd` is the raw graph for mermaid-cli / mermaid.live (paste it at
<https://mermaid.live> or render it locally). With `--image-format svg|png|both`
(the `Dockerfile.render` image bundles mermaid-cli and defaults to `both`)
TraceWeaver also writes rendered `lineage.svg` / `lineage.png` alongside it.

## function_calls.csv

| column | description |
|---|---|
| dag_id | DAG ID if resolved |
| task_id | Task ID if resolved |
| module | Dotted module path relative to the repo root (e.g. `pkg.sub.mod`); the file stem for top-level files |
| function_name | Called function name |
| caller_function | Calling function |
| file_path | Source file |
| line_no | Line number |
| method | static_ast |
