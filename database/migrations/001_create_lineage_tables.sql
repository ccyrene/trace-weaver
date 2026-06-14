CREATE TABLE IF NOT EXISTS lineage_jobs (
    id BIGSERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    operator_class TEXT,
    callable_path TEXT,
    file_path TEXT,
    line_no INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lineage_datasets (
    id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT UNIQUE NOT NULL,
    namespace TEXT,
    name TEXT NOT NULL,
    dataset_type TEXT,
    uri TEXT,
    schema_name TEXT,
    table_name TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    id BIGSERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    source_dataset TEXT,
    target_dataset TEXT,
    extraction_method TEXT NOT NULL,
    confidence TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    id BIGSERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    upstream_task_id TEXT NOT NULL,
    downstream_task_id TEXT NOT NULL,
    extraction_method TEXT NOT NULL DEFAULT 'static_ast',
    confidence TEXT NOT NULL DEFAULT 'high',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS function_calls (
    id BIGSERIAL PRIMARY KEY,
    dag_id TEXT,
    task_id TEXT,
    module TEXT,
    function_name TEXT NOT NULL,
    caller_function TEXT,
    file_path TEXT NOT NULL,
    line_no INT,
    method TEXT DEFAULT 'static_ast',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_scan_results (
    id BIGSERIAL PRIMARY KEY,
    repo_path TEXT,
    payload_json JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
