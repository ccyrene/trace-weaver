//! Structural validation of a [`WeaveDocument`].
//!
//! Validation never mutates the document; it returns a fresh list of
//! [`Diagnostic`]s. The CLI merges these with scan-time diagnostics and decides
//! whether to fail (any [`DiagLevel::Error`]) based on `--strict`.

use std::collections::HashSet;

use crate::model::{DiagLevel, Diagnostic, WeaveDocument};

/// Run all structural checks and return the findings.
pub fn validate(doc: &WeaveDocument) -> Vec<Diagnostic> {
    let mut out = Vec::new();

    // 1. Dataset names must be unique (they are the linking key).
    let mut seen: HashSet<&str> = HashSet::new();
    for ds in &doc.datasets {
        if !seen.insert(ds.name.as_str()) {
            out.push(Diagnostic::error(
                "E_DUP_DATASET",
                format!("duplicate dataset name '{}'", ds.name),
            ));
        }
    }

    let known: HashSet<&str> = doc.datasets.iter().map(|d| d.name.as_str()).collect();

    // 2. Edges must reference known datasets.
    for e in &doc.edges {
        if !known.contains(e.from.as_str()) {
            out.push(Diagnostic::error(
                "E_UNKNOWN_DATASET",
                format!("edge source dataset '{}' is not declared", e.from),
            ));
        }
        if !known.contains(e.to.as_str()) {
            out.push(Diagnostic::error(
                "E_UNKNOWN_DATASET",
                format!("edge target dataset '{}' is not declared", e.to),
            ));
        }

        // 3. Column lineage must reference the edge's endpoints and, when a
        //    schema is known, real columns on those datasets.
        for ce in &e.column_lineage {
            check_column(
                doc,
                &ce.to_column.dataset,
                &ce.to_column.column,
                &e.to,
                &mut out,
            );
            for fc in &ce.from_columns {
                check_column(doc, &fc.dataset, &fc.column, &e.from, &mut out);
            }
        }
    }

    // 4. Job I/O must reference known datasets (warn — a job may legitimately
    //    read an external, un-catalogued source).
    for j in &doc.jobs {
        for d in j.inputs.iter().chain(j.outputs.iter()) {
            if !known.contains(d.as_str()) {
                out.push(Diagnostic::warn(
                    "W_JOB_DATASET",
                    format!("job '{}' references undeclared dataset '{}'", j.id, d),
                ));
            }
        }
    }

    out
}

fn check_column(
    doc: &WeaveDocument,
    ds_name: &str,
    col: &str,
    expected_endpoint: &str,
    out: &mut Vec<Diagnostic>,
) {
    if ds_name != expected_endpoint {
        out.push(Diagnostic::warn(
            "W_COLUMN_ENDPOINT",
            format!("column '{ds_name}.{col}' is not on the edge endpoint '{expected_endpoint}'"),
        ));
    }
    // W_UNKNOWN_COLUMN only fires when the dataset carries a NON-EMPTY,
    // authoritative schema. NOTE: on the normal scan→validate path the schema is
    // back-filled from the very column lineage being validated (see
    // `backfill_schemas` in trace-weaver-scan), so a typo'd column self-populates
    // the schema and this check stays silent there. It catches unknown columns
    // only when validating a document that carries an externally-supplied schema
    // (e.g. one seeded from the catalogue) — see the unit tests below.
    if let Some(ds) = doc.dataset(ds_name) {
        if !ds.schema.is_empty() && !ds.schema.iter().any(|f| f.name == col) {
            out.push(Diagnostic::warn(
                "W_UNKNOWN_COLUMN",
                format!("column '{col}' not found in schema of dataset '{ds_name}'"),
            ));
        }
    }
}

/// True if `diags` contains at least one error-level finding.
pub fn has_errors(diags: &[Diagnostic]) -> bool {
    diags.iter().any(|d| d.level == DiagLevel::Error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ColumnEdge, ColumnRef, Dataset, Edge, Field, WeaveDocument};

    /// A doc with a `src -> dst` edge whose source-column lineage points at
    /// `src_col`. `src` carries an explicit (authoritative) schema of `[a]`.
    fn doc_with_schema(src_col: &str) -> WeaveDocument {
        let mut doc = WeaveDocument::new("ns", "test");
        let mut src = Dataset::new("src");
        src.schema = vec![Field::new("a")]; // authoritative schema: only column `a`
        let mut dst = Dataset::new("dst");
        dst.schema = vec![Field::new("b")];
        doc.datasets.push(src);
        doc.datasets.push(dst);

        let mut edge = Edge::new("src", "dst");
        edge.column_lineage.push(ColumnEdge::new(
            vec![ColumnRef::new("src", src_col)],
            ColumnRef::new("dst", "b"),
        ));
        doc.edges.push(edge);
        doc
    }

    fn codes(doc: &WeaveDocument) -> Vec<String> {
        validate(doc).into_iter().map(|d| d.code).collect()
    }

    #[test]
    fn unknown_column_fires_against_an_authoritative_schema() {
        // `ghost` is not in src's declared schema [a] -> W_UNKNOWN_COLUMN.
        let cs = codes(&doc_with_schema("ghost"));
        assert!(
            cs.iter().any(|c| c == "W_UNKNOWN_COLUMN"),
            "expected W_UNKNOWN_COLUMN, got {cs:?}"
        );
        // The endpoint matches, so no W_COLUMN_ENDPOINT noise.
        assert!(!cs.iter().any(|c| c == "W_COLUMN_ENDPOINT"), "{cs:?}");
    }

    #[test]
    fn known_column_does_not_fire() {
        // `a` IS in src's schema -> no W_UNKNOWN_COLUMN.
        let cs = codes(&doc_with_schema("a"));
        assert!(
            !cs.iter().any(|c| c == "W_UNKNOWN_COLUMN"),
            "should not flag a known column, got {cs:?}"
        );
    }

    #[test]
    fn empty_schema_never_fires_unknown_column() {
        // With no schema at all, the check is inert (mirrors the scan path where
        // the schema is back-filled from the lineage itself).
        let mut doc = doc_with_schema("ghost");
        doc.dataset_mut("src").unwrap().schema.clear();
        let cs = codes(&doc);
        assert!(
            !cs.iter().any(|c| c == "W_UNKNOWN_COLUMN"),
            "empty schema must not fire W_UNKNOWN_COLUMN, got {cs:?}"
        );
    }
}
