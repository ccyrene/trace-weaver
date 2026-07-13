//! OpenMetadata exporter — mirrors the reference POC's `add_lineage` calls.
//!
//! For each [`trace_weaver_core::Edge`] it resolves the from/to tables by FQN, builds an
//! `AddLineageRequest` body (edge `description`, `sqlQuery`, and per-column
//! `columnsLineage` with `function` labels), and PUTs it to
//! `{host}/v1/lineage`. Under `--dry-run` it emits the request bodies as JSON
//! instead of sending them.
//!
//! Inferred column functions are suffixed with `(inferred …)` so they are
//! visibly distinguishable in the OpenMetadata lineage UI.

use std::time::Duration;

use serde_json::{json, Value};
use trace_weaver_core::{Dataset, Edge, FqnParts, WeaveDocument};

use crate::{ExportConfig, ExportReport};

/// Note prepended to an edge description when any of its lineage was inferred.
const INFERRED_NOTE: &str = "> ⚠ Some lineage below was inferred, not declared.";

/// Build a reusable HTTP agent with an optional global timeout.
fn build_agent(timeout_secs: u64) -> ureq::Agent {
    let mut builder = ureq::Agent::config_builder();
    if timeout_secs > 0 {
        builder = builder.timeout_global(Some(Duration::from_secs(timeout_secs)));
    }
    builder.build().into()
}

/// Whether an HTTP error is worth retrying. We retry only genuinely transient
/// failures — transport I/O, timeouts, dropped/stalled connections, and the
/// transient status codes (429 rate-limit, 500/502/503/504). Permanent errors
/// (DNS `HostNotFound`, malformed URI, TLS, redirects, oversized body, etc.) can
/// never succeed on retry, so they fail fast instead of burning the retry budget.
fn is_retryable(e: &ureq::Error) -> bool {
    match e {
        ureq::Error::StatusCode(code) => matches!(*code, 429 | 500 | 502 | 503 | 504),
        ureq::Error::Io(_)
        | ureq::Error::Timeout(_)
        | ureq::Error::ConnectionFailed
        | ureq::Error::BodyStalled => true,
        _ => false,
    }
}

/// Run `f` with up to `retries` extra attempts on transient failures, backing off
/// exponentially (≈0.4s, 0.8s, 1.6s, …). Non-retryable errors return immediately.
fn with_retries<T>(
    retries: u32,
    mut f: impl FnMut() -> Result<T, ureq::Error>,
) -> Result<T, ureq::Error> {
    let mut attempt = 0u32;
    loop {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt >= retries || !is_retryable(&e) {
                    return Err(e);
                }
                attempt += 1;
                let backoff = 200u64.saturating_mul(2u64.saturating_pow(attempt));
                std::thread::sleep(Duration::from_millis(backoff.min(8_000)));
            }
        }
    }
}

/// Percent-encode a string for use as a single URL path segment.
/// Encodes everything outside the RFC 3986 unreserved set.
fn percent_encode(s: &str) -> String {
    fn is_unreserved(b: u8) -> bool {
        b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
    }
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Resolve the parsed FQN for a dataset, falling back to parsing its `name`,
/// optionally overriding the service segment.
fn resolve_fqn(ds: &Dataset, service_override: Option<&str>) -> Option<FqnParts> {
    let mut parts = ds.fqn.clone().or_else(|| FqnParts::parse(&ds.name))?;
    if let Some(svc) = service_override {
        parts.service = svc.to_string();
    }
    Some(parts)
}

/// Build the edge description, prepending the inferred-lineage note when needed.
fn edge_description(edge: &Edge) -> Option<String> {
    let base = edge.transform.description.clone();
    if edge.has_inferred() {
        match base {
            Some(d) => Some(format!("{INFERRED_NOTE}\n\n{d}")),
            None => Some(INFERRED_NOTE.to_string()),
        }
    } else {
        base
    }
}

/// Build the per-column `columnsLineage` array for an edge, using table FQNs.
///
/// Column mappings with no source columns (e.g. `COUNT(*)`) are omitted: an
/// OpenMetadata `columnsLineage` entry requires a non-empty `fromColumns`, and
/// a source-less entry would malform the request body (finding #8). The
/// edge-level (table) lineage is unaffected, so the target table still receives
/// the lineage edge.
fn columns_lineage(edge: &Edge, from_fqn: &FqnParts, to_fqn: &FqnParts) -> Vec<Value> {
    edge.column_lineage
        .iter()
        .filter(|ce| !ce.from_columns.is_empty())
        .map(|ce| {
            let from_cols: Vec<Value> = ce
                .from_columns
                .iter()
                .map(|c| Value::String(from_fqn.column_fqn(&c.column)))
                .collect();
            let mut entry = json!({
                "fromColumns": from_cols,
                "toColumn": to_fqn.column_fqn(&ce.to_column.column),
            });
            if let Some(f) = ce.display_function() {
                entry["function"] = json!(f);
            }
            entry
        })
        .collect()
}

/// Build the `lineageDetails` object shared by dry-run and live bodies.
fn lineage_details(edge: &Edge, from_fqn: &FqnParts, to_fqn: &FqnParts) -> Value {
    let mut details = json!({
        "columnsLineage": columns_lineage(edge, from_fqn, to_fqn),
    });
    if let Some(desc) = edge_description(edge) {
        details["description"] = json!(desc);
    }
    if let Some(sql) = &edge.transform.sql {
        details["sqlQuery"] = json!(sql);
    }
    details
}

pub fn export(doc: &WeaveDocument, cfg: &ExportConfig) -> anyhow::Result<ExportReport> {
    let service_override = cfg.om_service_override.as_deref();

    let live = !cfg.dry_run && cfg.om_host.is_some() && cfg.om_token.is_some();

    if !live {
        return export_dry_run(doc, service_override);
    }

    export_live(
        doc,
        service_override,
        cfg.om_host.as_deref().expect("checked above"),
        cfg.om_token.as_deref().expect("checked above"),
        cfg.timeout_secs,
        cfg.retries,
    )
}

/// Dry-run: build all request bodies (by FQN), don't touch the network.
fn export_dry_run(
    doc: &WeaveDocument,
    service_override: Option<&str>,
) -> anyhow::Result<ExportReport> {
    let mut bodies: Vec<Value> = Vec::new();
    let mut actions: Vec<String> = Vec::new();

    for edge in doc.edges_in_topo_order() {
        let (from_ds, to_ds) = match (doc.dataset(&edge.from), doc.dataset(&edge.to)) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                actions.push(format!(
                    "skip {} -> {} : dataset not found in document",
                    edge.from, edge.to
                ));
                continue;
            }
        };
        let (from_fqn, to_fqn) = match (
            resolve_fqn(from_ds, service_override),
            resolve_fqn(to_ds, service_override),
        ) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                actions.push(format!(
                    "skip {} -> {} : could not resolve a 4-part FQN",
                    edge.from, edge.to
                ));
                continue;
            }
        };

        // Reference entities by `fullyQualifiedName` — a valid EntityReference
        // key that OpenMetadata's name-resolving path accepts. (Live mode below
        // resolves these to the UUIDs the lineage API ultimately stores.)
        let body = json!({
            "edge": {
                "fromEntity": { "type": "table", "fullyQualifiedName": from_fqn.table_fqn() },
                "toEntity": { "type": "table", "fullyQualifiedName": to_fqn.table_fqn() },
                "lineageDetails": lineage_details(edge, &from_fqn, &to_fqn),
            }
        });
        bodies.push(body);
        actions.push(format!(
            "would PUT .../v1/lineage : {} -> {}",
            from_fqn.table_fqn(),
            to_fqn.table_fqn()
        ));
    }

    let artifact = serde_json::to_string_pretty(&Value::Array(bodies))?;
    Ok(ExportReport {
        target: "openmetadata".to_string(),
        artifact,
        actions,
        sent: 0,
        failed: 0,
    })
}

/// Live: resolve table UUIDs by FQN, then PUT each edge body.
fn export_live(
    doc: &WeaveDocument,
    service_override: Option<&str>,
    host: &str,
    token: &str,
    timeout_secs: u64,
    retries: u32,
) -> anyhow::Result<ExportReport> {
    let host = host.trim_end_matches('/');
    let bearer = format!("Bearer {token}");
    let agent = build_agent(timeout_secs);
    let mut bodies_sent: Vec<Value> = Vec::new();
    let mut actions: Vec<String> = Vec::new();
    let mut sent = 0usize;
    let mut attempted = 0usize;

    for edge in doc.edges_in_topo_order() {
        attempted += 1;
        let (from_ds, to_ds) = match (doc.dataset(&edge.from), doc.dataset(&edge.to)) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                actions.push(format!(
                    "skip {} -> {} : dataset not found in document",
                    edge.from, edge.to
                ));
                continue;
            }
        };
        let (from_fqn, to_fqn) = match (
            resolve_fqn(from_ds, service_override),
            resolve_fqn(to_ds, service_override),
        ) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                actions.push(format!(
                    "skip {} -> {} : could not resolve a 4-part FQN",
                    edge.from, edge.to
                ));
                continue;
            }
        };

        let from_table = from_fqn.table_fqn();
        let to_table = to_fqn.table_fqn();

        // Resolve UUIDs. Any network/HTTP error is recorded, not fatal.
        let from_id = match resolve_table_id(&agent, host, &bearer, &from_table, retries) {
            Ok(id) => id,
            Err(e) => {
                actions.push(format!(
                    "error {from_table} -> {to_table} : resolving from-table id: {e}"
                ));
                continue;
            }
        };
        let to_id = match resolve_table_id(&agent, host, &bearer, &to_table, retries) {
            Ok(id) => id,
            Err(e) => {
                actions.push(format!(
                    "error {from_table} -> {to_table} : resolving to-table id: {e}"
                ));
                continue;
            }
        };

        let body = json!({
            "edge": {
                "fromEntity": { "id": from_id, "type": "table" },
                "toEntity": { "id": to_id, "type": "table" },
                "lineageDetails": lineage_details(edge, &from_fqn, &to_fqn),
            }
        });

        let url = format!("{host}/v1/lineage");
        match with_retries(retries, || {
            agent
                .put(&url)
                .header("Authorization", &bearer)
                .send_json(&body)
        }) {
            Ok(resp) => {
                let status = resp.status();
                sent += 1;
                // Store an FQN-annotated copy in the artifact so the live record
                // is comparable with the dry-run output (finding #16); the wire
                // body itself stays id-only as OpenMetadata requires.
                let mut artifact_body = body.clone();
                artifact_body["edge"]["fromEntity"]["fullyQualifiedName"] = json!(from_table);
                artifact_body["edge"]["toEntity"]["fullyQualifiedName"] = json!(to_table);
                bodies_sent.push(artifact_body);
                actions.push(format!("PUT {url} [{status}] : {from_table} -> {to_table}"));
            }
            Err(e) => {
                actions.push(format!(
                    "error PUT {url} : {from_table} -> {to_table} : {e}"
                ));
            }
        }
    }

    let artifact = serde_json::to_string_pretty(&Value::Array(bodies_sent))?;
    Ok(ExportReport {
        target: "openmetadata".to_string(),
        artifact,
        actions,
        sent,
        failed: attempted.saturating_sub(sent),
    })
}

/// GET `{host}/v1/tables/name/{encoded-fqn}?fields=id` and pull the `id`,
/// retrying transient failures.
fn resolve_table_id(
    agent: &ureq::Agent,
    host: &str,
    bearer: &str,
    fqn: &str,
    retries: u32,
) -> anyhow::Result<String> {
    let url = format!("{host}/v1/tables/name/{}?fields=id", percent_encode(fqn));
    let body = with_retries(retries, || {
        agent.get(&url).header("Authorization", bearer).call()
    })?
    .body_mut()
    .read_json::<Value>()?;
    body.get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("response for {fqn} had no string `id`"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::tiny_doc;

    #[test]
    fn om_dry_run_contains_fqns_and_inferred_suffix() {
        let doc = tiny_doc();
        let report = export(&doc, &ExportConfig::default()).unwrap();
        assert_eq!(report.sent, 0);
        // Table FQNs present.
        assert!(report
            .artifact
            .contains("Test Database.poc_db.public.landing_sales"));
        assert!(report
            .artifact
            .contains("Test Database.poc_db.public.bronze_sales"));
        // The inferred column's function carries the suffix.
        assert!(report.artifact.contains("(inferred from SQL)"));
        // A would-PUT action line per edge.
        assert!(report.actions.iter().any(|a| a.contains("would PUT")));
    }

    #[test]
    fn percent_encode_handles_spaces_and_dots() {
        assert_eq!(percent_encode("Test Database.poc"), "Test%20Database.poc");
    }

    #[test]
    fn om_dry_run_emits_valid_body_for_self_edge() {
        // A self-edge produces a well-formed request body whose from/to entities
        // are the same FQN — valid JSON, no crash, one edge.
        let doc = crate::test_support::self_loop_doc();
        let report = export(&doc, &ExportConfig::default()).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&report.artifact).expect("valid OpenMetadata JSON");
        assert_eq!(v.as_array().map(|a| a.len()), Some(1));
        let fqn = "Test Database.poc_db.public.orphans";
        assert_eq!(v[0]["edge"]["fromEntity"]["fullyQualifiedName"], fqn);
        assert_eq!(v[0]["edge"]["toEntity"]["fullyQualifiedName"], fqn);
        // The same-dataset column mapping is carried on the edge.
        let cols = &v[0]["edge"]["lineageDetails"]["columnsLineage"];
        assert!(cols.is_array() && !cols.as_array().unwrap().is_empty());
    }
}
