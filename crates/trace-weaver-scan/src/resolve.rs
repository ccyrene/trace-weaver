//! Extension seam for resolving `W_OPAQUE_COLUMN` cases that static analysis
//! cannot recover (data-dependent column names, named UDFs, joins, pivots, …).
//!
//! **No resolver ships today, and nothing here calls a model or the network.**
//! This is only the *structure* a future resolver plugs into — for example one
//! that reads OpenLineage runtime events / warehouse query logs (observed fact),
//! or an LLM-assisted drafter whose output a human reviews before it is committed
//! as a declared `column_map`.
//!
//! Intended wiring (not active yet): collect the dataflow analyzer's
//! [`crate::dataflow::OpaqueNote`]s into [`ResolutionRequest`]s, run each
//! configured [`OpaqueResolver`] in priority order, and lower any returned
//! [`ResolvedColumn`] into a `ColumnEdge` whose provenance reflects its
//! [`ResolutionSource`]. Until [`resolvers`] returns a non-empty list, opaque
//! columns stay opaque and must be declared by hand — which is the safe default.

/// Where a resolved column came from. Kept separate from the static
/// `Declared` / `InferredSql` / `InferredCode` tiers so a runtime- or
/// LLM-recovered fact is never mistaken for a hand-declared one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionSource {
    /// Observed from a real run (OpenLineage events / warehouse query logs).
    Runtime,
    /// Drafted by an LLM — must be human-reviewed before it is trusted.
    Llm,
}

/// One opaque spot handed to a resolver, with enough context to recover lineage.
#[derive(Debug, Clone)]
pub struct ResolutionRequest {
    /// Task / job the opaque column belongs to.
    pub task: String,
    /// The output column whose lineage is missing, when the analyzer knew its name.
    pub target: Option<String>,
    /// The task's declared input / output dataset FQNs.
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    /// 1-based source line of the opaque code.
    pub line: Option<u32>,
    /// Why static analysis gave up (the `W_OPAQUE_COLUMN` detail text).
    pub reason: String,
    /// The offending source snippet, when available.
    pub code: Option<String>,
}

/// A column a resolver recovered. Mirrors the analyzer's column shape so it can
/// be lowered to a `ColumnEdge` once resolvers are wired into the pipeline.
#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    /// Source columns as `"table.col"` or bare `"col"`.
    pub sources: Vec<String>,
    pub target: String,
    pub function: Option<String>,
    pub source: ResolutionSource,
    pub confidence: f32,
    /// LLM drafts set this; such lineage must be reviewed before it is trusted.
    pub needs_review: bool,
}

/// Implemented by a future runtime/LLM resolver. Returns the columns it could
/// recover for a request (empty when it can't help).
pub trait OpaqueResolver {
    fn resolve(&self, request: &ResolutionRequest) -> Vec<ResolvedColumn>;
}

/// The configured resolvers, in priority order. **Empty today** — opaque columns
/// stay opaque until a runtime/LLM resolver is registered here. This is the one
/// place to plug a future resolver in.
pub fn resolvers() -> Vec<Box<dyn OpaqueResolver>> {
    Vec::new()
}
