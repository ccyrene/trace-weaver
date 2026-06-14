# TraceWeaver Architecture

## MVP architecture

```text
DAG repository  (local path or git URL)
    ↓
git_source.resolve_repo   (shallow clone if a URL)
    ↓
RepoScanner
    ↓
PythonAstScanner
    ├── DAG/task extraction (with/assign/@dag/@task, dag= resolution)
    ├── task dependency extraction (>> << chain set_upstream/downstream)
    ├── function call extraction (attributed to dag/task)
    ├── dataset + connection-id extraction
    └── SQL string collection (with dialect from operator class)
    ↓
sql_lineage   (sqlglot → regex fallback)
    ↓
ScanResult  (jobs, datasets, edges, task_dependencies, function_calls)
    ↓
CSV / JSON / PostgreSQL exporters
```

## Future production architecture

```text
TraceWeaver Static Scanner
    ↓
TraceWeaver Metadata DB
    ↑
OpenLineage Collector
    ↑
Airflow OpenLineage Provider
```

Optional future runtime layer:

```text
Airflow Worker + Python Runtime Tracer
    ↓
TraceWeaver Collector
```

Optional system evidence layer:

```text
eBPF Observer
    ↓
TraceWeaver Collector
```
