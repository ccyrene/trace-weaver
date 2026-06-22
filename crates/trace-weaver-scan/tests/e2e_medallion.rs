use std::path::Path;
use trace_weaver_scan::{scan_path, ScanOptions};

/// The example DAG is a PLAIN Airflow DAG with NO `@tw` annotation. trace-weaver
/// recovers full column lineage on its own: bronze + gold from SQL operators,
/// silver from the pandas PythonOperator body. The `--service/--database/--schema`
/// defaults expand the bare table names into 4-part FQNs.
#[test]
fn medallion_example_is_fully_traced_without_decorators() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/dags/medallion.py");
    let opts = ScanOptions {
        namespace: "example.dwh".into(),
        producer: "test".into(),
        enable_sql_inference: true,
        enable_code_inference: true,
        service: Some("Test Database".into()),
        database: Some("poc_db".into()),
        schema: Some("public".into()),
    };
    let doc = scan_path(root.parent().unwrap(), &opts).unwrap();

    assert_eq!(doc.jobs.len(), 3, "jobs");
    assert_eq!(doc.datasets.len(), 4, "datasets");
    assert_eq!(doc.edges.len(), 3, "edges");
    assert!(
        doc.diagnostics.is_empty(),
        "clean scan: {:?}",
        doc.diagnostics
    );

    // Bare table names expanded to 4-part OpenMetadata FQNs via the CLI defaults.
    assert!(doc
        .datasets
        .iter()
        .all(|d| d.name.starts_with("Test Database.poc_db.public.")));

    use trace_weaver_core::OriginSource::*;
    let find = |to: &str| doc.edges.iter().find(|e| e.to.ends_with(to)).unwrap();

    // bronze: SQL operator -> every column inferred from SQL.
    let bronze = find("bronze_sales");
    assert_eq!(bronze.column_lineage.len(), 5);
    assert!(bronze
        .column_lineage
        .iter()
        .all(|c| c.origin.source == InferredSql));

    // silver: PythonOperator pandas body -> every column inferred from CODE,
    // with NO column_map (identity copies, the USD fan-in, cast, comparison).
    let silver = find("silver_sales");
    assert_eq!(silver.column_lineage.len(), 7);
    assert!(silver
        .column_lineage
        .iter()
        .all(|c| c.origin.source == InferredCode));
    let usd = silver
        .column_lineage
        .iter()
        .find(|c| c.to_column.column == "amount_usd")
        .unwrap();
    assert_eq!(
        usd.from_columns.len(),
        2,
        "amount_usd is a fan-in of amount + currency"
    );

    // gold: SQL operator -> every column inferred from SQL.
    let gold = find("gold_sales_daily");
    assert_eq!(gold.column_lineage.len(), 5);
    assert!(gold
        .column_lineage
        .iter()
        .all(|c| c.origin.source == InferredSql));

    // ── Structural provenance: with ZERO @tw, the jobs, edges AND datasets are
    // all INFERRED — not just the columns. The IR must never claim a task that
    // was discovered decorator-free was hand-declared.
    assert!(
        doc.jobs.iter().all(|j| j.origin.is_inferred()),
        "all jobs inferred"
    );
    assert!(
        doc.edges.iter().all(|e| e.origin.is_inferred()),
        "all edges inferred"
    );
    assert!(
        doc.datasets.iter().all(|d| d.origin.is_inferred()),
        "all datasets inferred"
    );
    // Source matches the analyzer that recovered it: SQL operators -> inferred_sql,
    // the pandas PythonOperator -> inferred_code.
    assert_eq!(bronze.origin.source, InferredSql);
    assert_eq!(silver.origin.source, InferredCode);
    assert_eq!(gold.origin.source, InferredSql);
}
