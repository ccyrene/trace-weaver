use std::path::Path;
use trace_weaver_scan::{scan_path, ScanOptions};

#[test]
fn medallion_example_three_provenance_modes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/dags/medallion.py");
    let opts = ScanOptions {
        namespace: "example.dwh".into(),
        producer: "test".into(),
        enable_sql_inference: true,
        enable_code_inference: true,
    };
    let doc = scan_path(root.parent().unwrap(), &opts).unwrap();
    // 3 jobs, 4 datasets, 3 edges
    assert_eq!(doc.jobs.len(), 3, "jobs");
    assert_eq!(doc.datasets.len(), 4, "datasets");
    assert_eq!(doc.edges.len(), 3, "edges");

    use trace_weaver_core::OriginSource::*;
    let find = |to: &str| doc.edges.iter().find(|e| e.to.ends_with(to)).unwrap();

    // bronze: all inferred from SQL
    let bronze = find("bronze_sales");
    assert_eq!(bronze.column_lineage.len(), 5);
    assert!(bronze
        .column_lineage
        .iter()
        .all(|c| c.origin.source == InferredSql));

    // silver: all declared
    let silver = find("silver_sales");
    assert_eq!(silver.column_lineage.len(), 7);
    assert!(silver
        .column_lineage
        .iter()
        .all(|c| c.origin.source == Declared));

    // gold: mix - 2 declared, 3 inferred sql
    let gold = find("gold_sales_daily");
    assert_eq!(gold.column_lineage.len(), 5);
    let declared = gold
        .column_lineage
        .iter()
        .filter(|c| c.origin.source == Declared)
        .count();
    let inferred = gold
        .column_lineage
        .iter()
        .filter(|c| c.origin.source == InferredSql)
        .count();
    assert_eq!((declared, inferred), (2, 3));
}
