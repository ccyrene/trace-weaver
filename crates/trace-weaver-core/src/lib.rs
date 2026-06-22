//! # trace-weaver-core
//!
//! The **U**niversal **M**etadata & lineage **A**ggregation **P**ipeline core
//! model — the intermediate representation produced by the scanner and consumed
//! by every exporter.
//!
//! The model is intentionally aligned with the [OpenLineage] spec so a single
//! [`WeaveDocument`] can be exported to OpenMetadata, Marquez, DataHub or Atlas
//! without remodelling:
//!
//! * datasets are identified by `(namespace, name)`,
//! * column lineage mirrors the OpenLineage `columnLineage` facet
//!   ([`ColumnEdge`]), and
//! * [`TransformType`] mirrors OpenLineage's `transformationType`.
//!
//! Every element carries an [`Origin`] recording whether it was **declared** by
//! an engineer or **inferred** by the compiler (from SQL or code) — exporters
//! use this to visibly tag inferred lineage.
//!
//! [OpenLineage]: https://openlineage.io/

pub mod graph;
pub mod model;
pub mod origin;
pub mod validate;

pub use origin::{Origin, OriginSource};

pub use model::{
    ColumnEdge, ColumnRef, Dataset, DatasetRef, DiagLevel, Diagnostic, Edge, Engine, Field,
    FqnParts, Job, SourceLoc, Transform, TransformType, WeaveDocument, WEAVE_VERSION,
};

pub use graph::merge_origin;
pub use validate::{has_errors, validate};

/// Serialize a document to pretty JSON (the canonical on-disk `.weave.json`).
pub fn to_json_pretty(doc: &WeaveDocument) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(doc)
}

/// Parse a document from JSON.
pub fn from_json(s: &str) -> Result<WeaveDocument, serde_json::Error> {
    serde_json::from_str(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_annotation_marks_inferred() {
        assert_eq!(Origin::declared().annotate("COUNT(*)"), "COUNT(*)");
        assert_eq!(
            Origin::inferred_sql(0.9).annotate("COUNT(*)"),
            "COUNT(*) (inferred from SQL)"
        );
        assert_eq!(
            Origin::inferred_code(0.5).annotate(""),
            "(inferred from code)"
        );
    }

    #[test]
    fn fqn_roundtrip_handles_spaces_in_service() {
        let f = FqnParts::parse("Test Database.poc_db.public.bronze_sales").unwrap();
        assert_eq!(f.service, "Test Database");
        assert_eq!(f.table, "bronze_sales");
        assert_eq!(
            f.column_fqn("event_id"),
            "Test Database.poc_db.public.bronze_sales.event_id"
        );
    }

    #[test]
    fn derive_edges_and_topo_order() {
        let mut doc = WeaveDocument::new("ns", "test");
        for n in ["a", "b", "c"] {
            doc.datasets.push(Dataset::new(n));
        }
        let mut j = Job::new("dag.j1", "j1", Engine::Sql);
        j.inputs = vec!["a".into()];
        j.outputs = vec!["b".into()];
        doc.jobs.push(j);
        let mut j2 = Job::new("dag.j2", "j2", Engine::Sql);
        j2.inputs = vec!["b".into()];
        j2.outputs = vec!["c".into()];
        doc.jobs.push(j2);

        assert_eq!(doc.derive_edges_from_jobs(), 2);
        let order = doc.edges_in_topo_order();
        assert_eq!(order[0].from, "a");
        assert_eq!(order[1].from, "b");
    }

    #[test]
    fn validation_flags_unknown_edge_dataset() {
        let mut doc = WeaveDocument::new("ns", "test");
        doc.datasets.push(Dataset::new("a"));
        doc.edges.push(Edge::new("a", "missing"));
        let diags = validate(&doc);
        assert!(has_errors(&diags));
    }

    #[test]
    fn display_function_appends_inferred_tag() {
        let mut ce = ColumnEdge::new(vec![ColumnRef::new("a", "x")], ColumnRef::new("b", "y"));
        ce.function = Some("SUM(x)".into());
        ce.origin = Origin::inferred_sql(0.8);
        assert_eq!(ce.display_function().unwrap(), "SUM(x) (inferred from SQL)");
    }
}
