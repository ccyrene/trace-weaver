//! OpenLineage exporter — emits `RunEvent`s as a JSON array.
//!
//! Each [`trace_weaver_core::Job`] becomes a COMPLETE run event with its input/output
//! datasets; the `columnLineage` dataset facet carries the column mappings
//! (`inputFields` + `transformationType`/`transformationDescription`). This is
//! the interop path to Marquez and any OpenLineage-compatible backend.

use std::collections::BTreeMap;

use serde_json::{json, Value};
use trace_weaver_core::{TransformType, WeaveDocument};

use crate::{ExportConfig, ExportReport};

const COLUMN_LINEAGE_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet";
const RUN_EVENT_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
const DOC_JOB_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-0-1/DocumentationJobFacet.json#/$defs/DocumentationJobFacet";

/// Map a [`TransformType`] to a current-spec `(type, subtype)` pair for the
/// per-inputField `transformations` array (findings #12 / #18). The legacy
/// field-level `transformationType`/`transformationDescription` keys are no
/// longer emitted, as their enum did not include TRANSFORMATION/AGGREGATION.
fn transformation_type_subtype(t: TransformType) -> (&'static str, &'static str) {
    match t {
        TransformType::Identity => ("DIRECT", "IDENTITY"),
        TransformType::Transformation => ("DIRECT", "TRANSFORMATION"),
        TransformType::Aggregation => ("DIRECT", "AGGREGATION"),
        TransformType::Indirect => ("INDIRECT", "GROUP_BY"),
    }
}

/// Build a deterministic, UUID-shaped run id from a job id (no external deps).
/// Not a cryptographic UUID — just stable 8-4-4-4-12 hex derived from the id.
fn deterministic_run_id(job_id: &str) -> String {
    // Simple FNV-1a 64-bit hash, expanded to 128 bits via two rounds.
    fn fnv(seed: u64, bytes: &[u8]) -> u64 {
        let mut h = seed;
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
    let h1 = fnv(0xcbf29ce484222325, job_id.as_bytes());
    let h2 = fnv(h1, job_id.as_bytes());
    let hex = format!("{h1:016x}{h2:016x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

pub fn export(doc: &WeaveDocument, cfg: &ExportConfig) -> anyhow::Result<ExportReport> {
    let producer = cfg
        .ol_producer
        .clone()
        .unwrap_or_else(|| doc.producer.clone());
    let event_time = doc
        .generated_at
        .clone()
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    let ns_of = |name: &str| -> String {
        doc.dataset(name)
            .and_then(|d| d.namespace.clone())
            .unwrap_or_else(|| doc.namespace.clone())
    };

    let mut events: Vec<Value> = Vec::new();

    for job in &doc.jobs {
        // Edges produced by this job carry the column lineage we attach to the
        // output datasets.
        let job_edges: Vec<&trace_weaver_core::Edge> = doc
            .edges_in_topo_order()
            .into_iter()
            .filter(|e| e.job.as_deref() == Some(job.id.as_str()))
            .collect();

        // ── inputs ──
        let inputs: Vec<Value> = job
            .inputs
            .iter()
            .map(|name| {
                json!({
                    "namespace": ns_of(name),
                    "name": name,
                })
            })
            .collect();

        // ── outputs (with columnLineage facet when available) ──
        let outputs: Vec<Value> = job
            .outputs
            .iter()
            .map(|name| {
                // Gather column edges targeting this output dataset, grouped by
                // target column, across all edges of this job.
                let mut fields: BTreeMap<String, Value> = BTreeMap::new();
                for edge in &job_edges {
                    if edge.to != *name {
                        continue;
                    }
                    for ce in &edge.column_lineage {
                        if ce.to_column.dataset != *name {
                            continue;
                        }
                        let (ttype, tsubtype) = transformation_type_subtype(ce.transform_type);
                        // The transform (incl. any "(inferred …)" tag) lives in
                        // each inputField's `transformations` array per the
                        // current ColumnLineageDatasetFacet schema.
                        let transformation = {
                            let mut t = json!({
                                "type": ttype,
                                "subtype": tsubtype,
                                "masking": false,
                            });
                            if let Some(desc) = ce.display_function() {
                                t["description"] = json!(desc);
                            }
                            t
                        };
                        let input_fields: Vec<Value> = ce
                            .from_columns
                            .iter()
                            .map(|src| {
                                json!({
                                    "namespace": ns_of(&src.dataset),
                                    "name": src.dataset,
                                    "field": src.column,
                                    "transformations": [transformation.clone()],
                                })
                            })
                            .collect();
                        // A source-less mapping (e.g. COUNT(*)) has no inputFields
                        // to attach to; skip it at the column level.
                        if input_fields.is_empty() {
                            continue;
                        }
                        fields.insert(
                            ce.to_column.column.clone(),
                            json!({ "inputFields": input_fields }),
                        );
                    }
                }

                let mut ds = json!({
                    "namespace": ns_of(name),
                    "name": name,
                });
                if !fields.is_empty() {
                    let fields_obj: serde_json::Map<String, Value> = fields.into_iter().collect();
                    ds["facets"] = json!({
                        "columnLineage": {
                            "_producer": producer,
                            "_schemaURL": COLUMN_LINEAGE_SCHEMA_URL,
                            "fields": Value::Object(fields_obj),
                        }
                    });
                }
                ds
            })
            .collect();

        let job_ns = job
            .namespace
            .clone()
            .unwrap_or_else(|| doc.namespace.clone());

        // (#15) Reflect edge-level provenance even when no individual column is
        // inferred: tag the job with a documentation facet when any of its edges
        // carries inferred lineage, so a fully-inferred-but-uncolumned edge
        // doesn't read as declared.
        let mut job_obj = json!({ "namespace": job_ns, "name": job.id });
        if job_edges.iter().any(|e| e.has_inferred()) {
            job_obj["facets"] = json!({
                "documentation": {
                    "_producer": producer,
                    "_schemaURL": DOC_JOB_FACET_SCHEMA_URL,
                    "description": "This job contains lineage that was inferred by the trace-weaver \
                                    compiler (not hand-declared). See per-column transformation \
                                    descriptions tagged '(inferred …)'."
                }
            });
        }

        events.push(json!({
            "eventType": "COMPLETE",
            "eventTime": event_time,
            "schemaURL": RUN_EVENT_SCHEMA_URL,
            "producer": producer,
            "run": { "runId": deterministic_run_id(&job.id) },
            "job": job_obj,
            "inputs": inputs,
            "outputs": outputs,
        }));
    }

    let artifact = serde_json::to_string_pretty(&Value::Array(events.clone()))?;
    let count = events.len();

    Ok(ExportReport {
        target: "openlineage".to_string(),
        artifact,
        actions: vec![format!(
            "emitted {count} OpenLineage COMPLETE event(s) (file-only exporter; sent counts events)"
        )],
        sent: count,
        failed: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::tiny_doc;

    #[test]
    fn ol_contains_column_lineage() {
        let doc = tiny_doc();
        let report = export(&doc, &ExportConfig::default()).unwrap();
        assert!(report.artifact.contains("columnLineage"));
        assert!(report.artifact.contains("\"COMPLETE\""));
        assert!(report.sent >= 1);
    }
}
