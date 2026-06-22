//! # trace-weaver-export
//!
//! Consumes a [`trace_weaver_core::WeaveDocument`] and emits it to a downstream lineage
//! catalogue. All exporters share one entry point, [`export`], so the CLI is
//! target-agnostic.
//!
//! Targets:
//! * [`Target::OpenMetadata`] — pushes `add_lineage` edges over the OM REST API
//!   (mirrors the reference POC), or prints the request bodies under `--dry-run`.
//! * [`Target::OpenLineage`] — emits OpenLineage `RunEvent`s (with the
//!   `columnLineage` facet) as JSON.
//! * [`Target::Dot`] — renders a Graphviz DOT graph for quick visualisation.
//!
//! Inferred lineage is rendered with an `(inferred …)` suffix via
//! [`trace_weaver_core::ColumnEdge::display_function`] / [`trace_weaver_core::Origin::annotate`].

use trace_weaver_core::WeaveDocument;

pub mod dot;
pub mod openlineage;
pub mod openmetadata;

/// Which downstream catalogue to export to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    OpenMetadata,
    OpenLineage,
    Dot,
}

impl Target {
    pub fn parse(s: &str) -> anyhow::Result<Target> {
        match s.trim().to_ascii_lowercase().as_str() {
            "openmetadata" | "om" => Ok(Target::OpenMetadata),
            "openlineage" | "ol" => Ok(Target::OpenLineage),
            "dot" | "graphviz" => Ok(Target::Dot),
            other => anyhow::bail!(
                "unknown export target '{other}' (expected: openmetadata|openlineage|dot)"
            ),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Target::OpenMetadata => "openmetadata",
            Target::OpenLineage => "openlineage",
            Target::Dot => "dot",
        }
    }
}

/// Configuration covering every target (only the relevant fields are read).
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// When true, build artifacts and log intended actions but perform no I/O.
    pub dry_run: bool,

    // ── OpenMetadata ──
    /// e.g. `"http://openmetadata-server:8585/api"`.
    pub om_host: Option<String>,
    /// ingestion-bot JWT.
    pub om_token: Option<String>,
    /// Override the service segment of every dataset FQN, if needed.
    pub om_service_override: Option<String>,

    // ── OpenLineage ──
    /// `producer` URI for emitted events; falls back to the document producer.
    pub ol_producer: Option<String>,

    // ── HTTP resilience (used by network-backed exporters) ──
    /// Per-request timeout in seconds. `0` means no explicit timeout.
    pub timeout_secs: u64,
    /// Number of automatic retries for transient failures (timeouts, 429, 5xx).
    pub retries: u32,
}

impl Default for ExportConfig {
    fn default() -> Self {
        ExportConfig {
            dry_run: false,
            om_host: None,
            om_token: None,
            om_service_override: None,
            ol_producer: None,
            timeout_secs: 30,
            retries: 2,
        }
    }
}

/// What an export produced.
#[derive(Debug, Clone)]
pub struct ExportReport {
    pub target: String,
    /// A textual artifact (the OL events JSON, DOT graph, or the OM request
    /// bodies under `--dry-run`) suitable for printing or writing to a file.
    pub artifact: String,
    /// Human-readable log of what was sent or would be sent.
    pub actions: Vec<String>,
    /// Count of lineage edges actually pushed (0 under `--dry-run`).
    pub sent: usize,
    /// Count of edges that reached the network but failed (after retries).
    /// Always 0 under `--dry-run`. Used by the CLI's `--fail-on-partial` gate.
    pub failed: usize,
}

/// Export `doc` to `target` using `cfg`.
pub fn export(
    target: Target,
    doc: &WeaveDocument,
    cfg: &ExportConfig,
) -> anyhow::Result<ExportReport> {
    match target {
        Target::OpenMetadata => openmetadata::export(doc, cfg),
        Target::OpenLineage => openlineage::export(doc, cfg),
        Target::Dot => dot::export(doc, cfg),
    }
}

/// Shared fixtures for the per-exporter unit tests: a tiny one-hop document
/// (`landing_sales → bronze_sales`) with one declared and one inferred-from-SQL
/// column mapping.
#[cfg(test)]
pub(crate) mod test_support {
    use trace_weaver_core::{
        ColumnEdge, ColumnRef, Dataset, Edge, Engine, FqnParts, Job, Origin, Transform,
        TransformType, WeaveDocument,
    };

    pub(crate) fn tiny_doc() -> WeaveDocument {
        let mut doc = WeaveDocument::new("example.dwh", "trace-weaver/test");

        let mut landing = Dataset::new("Test Database.poc_db.public.landing_sales");
        landing.platform = Some("postgres".into());
        landing.fqn = FqnParts::parse(&landing.name);

        let mut bronze = Dataset::new("Test Database.poc_db.public.bronze_sales");
        bronze.platform = Some("postgres".into());
        bronze.fqn = FqnParts::parse(&bronze.name);

        doc.upsert_dataset(landing);
        doc.upsert_dataset(bronze);

        let mut job = Job::new("medallion.build_bronze", "build_bronze", Engine::Sql);
        job.inputs = vec!["Test Database.poc_db.public.landing_sales".into()];
        job.outputs = vec!["Test Database.poc_db.public.bronze_sales".into()];
        job.sql = Some("INSERT INTO bronze_sales SELECT * FROM landing_sales".into());
        doc.jobs.push(job);

        let mut edge = Edge::new(
            "Test Database.poc_db.public.landing_sales",
            "Test Database.poc_db.public.bronze_sales",
        );
        edge.job = Some("medallion.build_bronze".into());
        edge.transform = Transform {
            kind: Some("CAST / DEDUPE".into()),
            description: Some("CAST raw strings to typed columns + dedupe.".into()),
            sql: Some("INSERT INTO bronze_sales SELECT * FROM landing_sales".into()),
        };

        // Declared mapping: customer -> customer_name.
        let mut declared = ColumnEdge::new(
            vec![ColumnRef::new(
                "Test Database.poc_db.public.landing_sales",
                "customer",
            )],
            ColumnRef::new("Test Database.poc_db.public.bronze_sales", "customer_name"),
        );
        declared.function = Some("rename (direct copy)".into());
        declared.transform_type = TransformType::Identity;
        declared.origin = Origin::declared();

        // Inferred-from-SQL mapping: raw_event_id -> event_id.
        let mut inferred = ColumnEdge::new(
            vec![ColumnRef::new(
                "Test Database.poc_db.public.landing_sales",
                "raw_event_id",
            )],
            ColumnRef::new("Test Database.poc_db.public.bronze_sales", "event_id"),
        );
        inferred.function = Some("CAST text -> bigint".into());
        inferred.transform_type = TransformType::Transformation;
        inferred.origin = Origin::inferred_sql(0.9);

        edge.column_lineage = vec![declared, inferred];
        doc.edges.push(edge);

        doc
    }
}
