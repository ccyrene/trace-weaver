# Limitations

TraceWeaver currently performs static analysis only.

It may miss or partially resolve:

- Datasets and table names that only exist at runtime — e.g. S3 paths built
  from `Variable.get(...)` / config, or SQL kept in external `.sql` files
  (only inline SQL and module-level string constants are parsed).
- Dynamic DAG factories and loops that build DAGs/tasks programmatically.
- Task IDs or table names built from variables, XCom, or config at runtime.
- SQL loaded from external `.sql` files (only inline SQL and module-level
  string constants are read).
- SQL nested in operator config blocks (e.g. `BigQueryInsertJobOperator`'s
  `configuration={"query": {"query": ...}}`).
- SQL generated dynamically with complex Python logic.
- Datasets passed through XCom or resolved from secret managers.
- Custom operators with hidden lineage behavior.

When `sqlglot` is installed, SQL lineage is parse-tree based and dialect-aware
(`high` confidence). Without it, TraceWeaver falls back to regex extraction
(`medium`/`low` confidence). Either way, results are **candidates** — use the
confidence levels and `raw_scan_result.json` for triage.

Recommended production pairing: OpenLineage runtime events for confirmed lineage.
