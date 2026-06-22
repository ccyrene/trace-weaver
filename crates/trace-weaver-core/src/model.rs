//! The weave universal lineage model.
//!
//! A [`WeaveDocument`] is the intermediate representation the compiler produces
//! by scanning DAG code, and the input every exporter consumes. The model is
//! deliberately *aligned with the OpenLineage spec* — datasets are identified
//! by `(namespace, name)`, column lineage mirrors the OpenLineage
//! `columnLineage` facet, and `TransformType` matches OpenLineage's
//! `transformationType`/`subtype` — so it can fan out to OpenMetadata,
//! Marquez/OpenLineage, DataHub or Atlas without lossy remodelling.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::origin::Origin;

/// The schema version this build of the model understands.
pub const WEAVE_VERSION: &str = "0.1";

/// A reference to a dataset by its canonical `name` (unique within a document).
/// For OpenMetadata this is the FQN `service.database.schema.table`.
pub type DatasetRef = String;

/// A reference to a column within a dataset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Canonical name of the owning dataset (matches [`Dataset::name`]).
    pub dataset: DatasetRef,
    pub column: String,
}

impl ColumnRef {
    pub fn new(dataset: impl Into<String>, column: impl Into<String>) -> Self {
        ColumnRef {
            dataset: dataset.into(),
            column: column.into(),
        }
    }
}

// ───────────────────────────── document root ─────────────────────────────

/// Top-level lineage document — the universal format ("weave").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeaveDocument {
    /// Schema version, e.g. `"0.1"`.
    pub weave_version: String,
    /// Tool that produced this document, e.g. `"trace-weaver/0.1.0"`.
    pub producer: String,
    /// ISO-8601 timestamp, stamped by the CLI *after* compilation
    /// (kept out of the deterministic compile so output is reproducible).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
    /// Default logical namespace for entities that don't override it,
    /// e.g. `"example.dwh"` or a connection URI.
    pub namespace: String,
    #[serde(default)]
    pub datasets: Vec<Dataset>,
    #[serde(default)]
    pub jobs: Vec<Job>,
    #[serde(default)]
    pub edges: Vec<Edge>,
    /// Warnings/notes raised during scanning (e.g. incomplete declarations).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
}

impl WeaveDocument {
    pub fn new(namespace: impl Into<String>, producer: impl Into<String>) -> Self {
        WeaveDocument {
            weave_version: WEAVE_VERSION.to_string(),
            producer: producer.into(),
            generated_at: None,
            namespace: namespace.into(),
            datasets: Vec::new(),
            jobs: Vec::new(),
            edges: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    /// Look up a dataset by its canonical name.
    pub fn dataset(&self, name: &str) -> Option<&Dataset> {
        self.datasets.iter().find(|d| d.name == name)
    }

    pub fn dataset_mut(&mut self, name: &str) -> Option<&mut Dataset> {
        self.datasets.iter_mut().find(|d| d.name == name)
    }

    /// Insert a dataset if no dataset with the same name exists yet, otherwise
    /// merge schema/origin into the existing one. Returns the canonical name.
    pub fn upsert_dataset(&mut self, ds: Dataset) -> DatasetRef {
        if let Some(existing) = self.dataset_mut(&ds.name) {
            existing.merge_from(ds);
            existing.name.clone()
        } else {
            let name = ds.name.clone();
            self.datasets.push(ds);
            name
        }
    }
}

// ─────────────────────────────── datasets ────────────────────────────────

/// A table / file / topic — anything data flows into or out of.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    /// Canonical, document-unique name. For OpenMetadata: the table FQN
    /// `service.database.schema.table`.
    pub name: String,
    /// OpenLineage dataset namespace (a connection/source identifier),
    /// e.g. `"postgres://host:5432"` or `"s3://bucket"`. Falls back to the
    /// document namespace when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Source platform, e.g. `"postgres"`, `"delta"`, `"s3"`, `"kafka"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// Parsed FQN parts, used by the OpenMetadata exporter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fqn: Option<FqnParts>,
    /// Column schema (optional — column lineage works even without it).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema: Vec<Field>,
    /// Arbitrary extra facets (free-form, exporter-specific).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub facets: IndexMap<String, Json>,
    pub origin: Origin,
}

impl Dataset {
    pub fn new(name: impl Into<String>) -> Self {
        Dataset {
            name: name.into(),
            namespace: None,
            platform: None,
            fqn: None,
            schema: Vec::new(),
            facets: IndexMap::new(),
            origin: Origin::declared(),
        }
    }

    pub fn with_origin(mut self, origin: Origin) -> Self {
        self.origin = origin;
        self
    }

    /// Merge fields from `other` into `self`, preferring declared data and
    /// not dropping schema columns already known.
    pub fn merge_from(&mut self, other: Dataset) {
        if self.namespace.is_none() {
            self.namespace = other.namespace;
        }
        if self.platform.is_none() {
            self.platform = other.platform;
        }
        if self.fqn.is_none() {
            self.fqn = other.fqn;
        }
        for f in other.schema {
            if !self.schema.iter().any(|e| e.name == f.name) {
                self.schema.push(f);
            }
        }
        for (k, v) in other.facets {
            self.facets.entry(k).or_insert(v);
        }
        // A declared origin always wins over an inferred one.
        if self.origin.is_inferred() && !other.origin.is_inferred() {
            self.origin = other.origin;
        }
    }

    /// Effective namespace: dataset's own, else the document default.
    pub fn effective_namespace<'a>(&'a self, doc_ns: &'a str) -> &'a str {
        self.namespace.as_deref().unwrap_or(doc_ns)
    }
}

/// A single column definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Field {
    pub fn new(name: impl Into<String>) -> Self {
        Field {
            name: name.into(),
            data_type: None,
            description: None,
        }
    }
}

/// OpenMetadata-style fully-qualified-name parts (`service.database.schema.table`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FqnParts {
    pub service: String,
    pub database: String,
    pub schema: String,
    pub table: String,
}

impl FqnParts {
    /// Parse a dotted FQN. Requires exactly four dot-separated segments.
    /// (Service names may contain spaces, e.g. `"Test Database"`, which is fine.)
    pub fn parse(fqn: &str) -> Option<FqnParts> {
        let parts: Vec<&str> = fqn.split('.').collect();
        if parts.len() == 4 {
            Some(FqnParts {
                service: parts[0].to_string(),
                database: parts[1].to_string(),
                schema: parts[2].to_string(),
                table: parts[3].to_string(),
            })
        } else {
            None
        }
    }

    pub fn table_fqn(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.service, self.database, self.schema, self.table
        )
    }

    pub fn column_fqn(&self, column: &str) -> String {
        format!("{}.{column}", self.table_fqn())
    }
}

// ───────────────────────────────── jobs ──────────────────────────────────

/// The processing engine a job uses. Drives which analyzer the scanner picks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    Sql,
    Pandas,
    Spark,
    Python,
    Bash,
    /// Engine not specified / not recognised.
    Unknown,
}

impl Engine {
    pub fn from_str_loose(s: &str) -> Engine {
        match s.trim().to_ascii_lowercase().as_str() {
            "sql" => Engine::Sql,
            "pandas" | "pd" => Engine::Pandas,
            "spark" | "pyspark" => Engine::Spark,
            "python" | "py" => Engine::Python,
            "bash" | "shell" => Engine::Bash,
            _ => Engine::Unknown,
        }
    }
}

/// A unit of processing — typically one Airflow task / one `@tw.task`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Document-unique job id, e.g. `"medallion_lineage.build_bronze"`.
    pub id: String,
    /// Human task name, e.g. `"build_bronze"`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// The DAG / pipeline this job belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dag: Option<String>,
    pub engine: Engine,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLoc>,
    /// Markdown description (rendered on the lineage edge by exporters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The transform SQL, when the engine is SQL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<DatasetRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<DatasetRef>,
    pub origin: Origin,
}

impl Job {
    pub fn new(id: impl Into<String>, name: impl Into<String>, engine: Engine) -> Self {
        Job {
            id: id.into(),
            name: name.into(),
            namespace: None,
            dag: None,
            engine,
            location: None,
            description: None,
            sql: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
            origin: Origin::declared(),
        }
    }
}

/// Where in source a declaration lives — used in diagnostics and OM facets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceLoc {
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
}

impl SourceLoc {
    pub fn new(file: impl Into<String>, line: Option<u32>) -> Self {
        SourceLoc {
            file: file.into(),
            line,
        }
    }
}

// ───────────────────────────────── edges ─────────────────────────────────

/// OpenLineage-aligned column transformation classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformType {
    /// Value copied unchanged (rename / direct copy).
    Identity,
    /// Value transformed (cast, arithmetic, expression).
    #[default]
    Transformation,
    /// Aggregation (COUNT/SUM/AVG/...).
    Aggregation,
    /// Used only to filter/join, value does not flow into the target.
    Indirect,
}

/// Edge-level transform metadata (what the whole hop does).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Transform {
    /// Short label, e.g. `"CAST / DEDUPE"`, `"ENRICH"`, `"AGGREGATE"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Rich Markdown description shown on the lineage edge in the UI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
}

/// A column-level mapping: one (or more) source columns feed one target column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnEdge {
    /// Source columns (a list — supports fan-in, e.g. `amount_usd ← amount + currency`).
    pub from_columns: Vec<ColumnRef>,
    pub to_column: ColumnRef,
    /// Human-readable transform label, e.g. `"ROUND(amount * fx, 2)"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(default)]
    pub transform_type: TransformType,
    /// Provenance — declared, inferred-from-SQL, or inferred-from-code.
    pub origin: Origin,
}

impl ColumnEdge {
    pub fn new(from_columns: Vec<ColumnRef>, to_column: ColumnRef) -> Self {
        ColumnEdge {
            from_columns,
            to_column,
            function: None,
            transform_type: TransformType::default(),
            origin: Origin::declared(),
        }
    }

    /// The `function` label with an `(inferred …)` suffix appended when the
    /// mapping was not hand-declared. This is what exporters should display.
    pub fn display_function(&self) -> Option<String> {
        match &self.function {
            Some(f) => Some(self.origin.annotate(f)),
            None if self.origin.is_inferred() => Some(self.origin.annotate("")),
            None => None,
        }
    }
}

/// A dataset → dataset lineage edge (one OpenMetadata `add_lineage` call,
/// or one OpenLineage input→output pair).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: DatasetRef,
    pub to: DatasetRef,
    /// The job that produced this edge, if known (links to [`Job::id`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    #[serde(default)]
    pub transform: Transform,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_lineage: Vec<ColumnEdge>,
    pub origin: Origin,
}

impl Edge {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Edge {
            from: from.into(),
            to: to.into(),
            job: None,
            transform: Transform::default(),
            column_lineage: Vec::new(),
            origin: Origin::declared(),
        }
    }

    /// True if any part of this edge (the edge itself or any column mapping)
    /// was inferred rather than declared.
    pub fn has_inferred(&self) -> bool {
        self.origin.is_inferred() || self.column_lineage.iter().any(|c| c.origin.is_inferred())
    }
}

// ─────────────────────────────── diagnostics ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagLevel {
    Info,
    Warn,
    Error,
}

/// A scan-time finding the user should know about (incomplete declaration,
/// unresolvable reference, low-confidence inference, …).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: DiagLevel,
    /// Stable machine code, e.g. `"E001"`, `"W_INCOMPLETE_COLUMN_MAP"`.
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLoc>,
}

impl Diagnostic {
    pub fn warn(code: impl Into<String>, message: impl Into<String>) -> Self {
        Diagnostic {
            level: DiagLevel::Warn,
            code: code.into(),
            message: message.into(),
            location: None,
        }
    }
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Diagnostic {
            level: DiagLevel::Error,
            code: code.into(),
            message: message.into(),
            location: None,
        }
    }
    pub fn info(code: impl Into<String>, message: impl Into<String>) -> Self {
        Diagnostic {
            level: DiagLevel::Info,
            code: code.into(),
            message: message.into(),
            location: None,
        }
    }
    pub fn at(mut self, loc: SourceLoc) -> Self {
        self.location = Some(loc);
        self
    }
}
