//! Best-effort code-level inference to fill gaps the engineer left undeclared.
//!
//! This is the lowest-confidence source of lineage and runs *only* on columns
//! that neither a declared `column_map` nor SQL extraction covered. Everything
//! it emits is provenance [`trace_weaver_core::OriginSource::InferredCode`] and is
//! tagged `(inferred from code)` by exporters.
//!
//! The MVP heuristic is conservative: for a target dataset whose columns are
//! still unmapped, if a source dataset has an identically-named column, emit a
//! low-confidence identity mapping. Richer pandas/Spark dataflow analysis can
//! be layered in here later without changing callers.

use trace_weaver_core::{ColumnEdge, ColumnRef, Dataset, Origin, TransformType};

/// Confidence assigned to a code-inferred identity mapping.
const CODE_CONFIDENCE: f32 = 0.4;

/// Given the source and target datasets of an edge and the set of target
/// columns already mapped (declared or via SQL), return additional
/// low-confidence [`ColumnEdge`]s for the still-unmapped target columns.
///
/// Requires schemas to be known; returns empty when they aren't.
pub fn infer_identity_gap_fill(
    sources: &[&Dataset],
    target: &Dataset,
    already_mapped: &[String],
) -> Vec<ColumnEdge> {
    let mut out = Vec::new();
    if target.schema.is_empty() {
        return out;
    }

    for field in &target.schema {
        if already_mapped.iter().any(|m| m == &field.name) {
            continue;
        }
        // Find a source dataset with an identically-named column.
        let matching_source = sources
            .iter()
            .find(|src| !src.schema.is_empty() && src.schema.iter().any(|f| f.name == field.name));
        if let Some(src) = matching_source {
            let mut edge = ColumnEdge::new(
                vec![ColumnRef::new(src.name.clone(), field.name.clone())],
                ColumnRef::new(target.name.clone(), field.name.clone()),
            );
            edge.function = Some("direct copy".to_string());
            edge.transform_type = TransformType::Identity;
            edge.origin = Origin::inferred_code(CODE_CONFIDENCE);
            out.push(edge);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use trace_weaver_core::Field;

    fn ds(name: &str, cols: &[&str]) -> Dataset {
        let mut d = Dataset::new(name);
        d.schema = cols.iter().map(|c| Field::new(*c)).collect();
        d
    }

    #[test]
    fn fills_same_named_columns_only() {
        let src = ds("s", &["a", "b", "c"]);
        let tgt = ds("t", &["a", "b", "x"]);
        let edges = infer_identity_gap_fill(&[&src], &tgt, &["a".to_string()]);
        // a is already mapped; b matches source; x has no source match.
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].to_column.column, "b");
        assert!(edges[0].origin.is_inferred());
        assert_eq!(edges[0].transform_type, TransformType::Identity);
    }

    #[test]
    fn empty_when_target_schema_unknown() {
        let src = ds("s", &["a"]);
        let tgt = Dataset::new("t"); // no schema
        assert!(infer_identity_gap_fill(&[&src], &tgt, &[]).is_empty());
    }
}
