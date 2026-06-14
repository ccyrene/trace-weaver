# TraceWeaver Handoff

## Summary

TraceWeaver is an MVP CLI project for static data lineage extraction from Airflow DAG repositories.

The current implementation is intentionally lightweight and uses Python stdlib only. It scans `.py` files, parses AST, extracts DAG/task/function/SQL metadata, and exports CSV or JSON.

## Main command

```bash
traceweaver scan --repo-path examples/sample_dags --output csv --output-dir outputs/csv
```

## Why this approach

- Airflow DAGs are Python, so Python AST is the fastest MVP path.
- Static scanning gives an immediate demo without changing DAG code.
- OpenLineage can be added later for runtime-confirmed lineage.
- eBPF should not be MVP because it sees OS-level evidence, not DAG/SQL semantics.

## Files to inspect first

1. `src/traceweaver/cli.py`
2. `src/traceweaver/scanners/repo_scanner.py`
3. `src/traceweaver/scanners/python_ast_scanner.py`
4. `src/traceweaver/scanners/sql_scanner.py`
5. `src/traceweaver/models.py`
6. `examples/sample_dags/simple_python_operator.py`
7. `examples/sample_dags/sql_operator_dag.py`

## Done in this version

- ✅ Git clone support (`--repo-path <git-url>` + `--git-ref`).
- ✅ Postgres exporter (`--output db --database-url ...`) via SQLAlchemy/psycopg.
- ✅ `sqlglot`-based SQL lineage (dialect-aware) with regex fallback.
- ✅ Task dependency extraction (`>>`, `<<`, list fan-out, `chain`, `set_*stream`).
- ✅ `dag = DAG(...)` / `dag=` keyword / ambient single-DAG resolution.
- ✅ Connection-id + richer dataset/URI extraction.
- ✅ Function calls attributed to the owning dag/task.
- ✅ Docker image (mount `/dags`, write `/out`) + compose with optional Postgres.

## Remaining gaps (still static-only)

- Does not resolve complex imports or dynamic DAG generation.
- Does not read SQL from external `.sql` files or nested operator config blocks.
- Does not execute DAG code or verify runtime lineage.

## Recommended next implementation sequence

1. OpenLineage event collector as a second service.
2. Runtime function tracer for function-level truth.
3. Optional eBPF agent only as system-level evidence.

## Acceptance criteria (met)

Given a DAG repo, TraceWeaver produces:

- `lineage_jobs.csv` with `dag_id`, `task_id`, `operator_class`, `callable_path`, `file_path`, `line_no`.
- `lineage_edges.csv` with source dataset, target dataset, DAG/task, extraction method, confidence.
- `task_dependencies.csv` with upstream/downstream task control flow.
- `function_calls.csv` with the function call graph from business functions.
- `raw_scan_result.json` for debugging and future reprocessing.
