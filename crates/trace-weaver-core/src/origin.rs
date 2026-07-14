//! Provenance tracking — *how* a piece of lineage came to exist.
//!
//! This is a first-class concept in trace-weaver. The compiler combines three sources
//! of lineage knowledge:
//!
//! 1. **Declared** — written by an engineer in a `@tw.task(...)` decorator.
//!    Authoritative; never overwritten by inference.
//! 2. **InferredSql** — derived by parsing an embedded SQL transform.
//!    High confidence, but still machine-derived.
//! 3. **InferredCode** — best-effort static analysis of pandas/Spark/other code
//!    used only to *fill gaps* the engineer left undeclared.
//!
//! Every dataset, job, edge and (crucially) every column-level mapping carries
//! an [`Origin`]. Exporters use it to visibly mark inferred facts — e.g. the
//! column `function` label `"COUNT(*)"` becomes `"COUNT(*) (inferred)"` so a
//! human reading the lineage graph can always tell declared truth from a guess.

use serde::{Deserialize, Serialize};

use crate::model::SourceLoc;

/// Where a piece of lineage knowledge originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginSource {
    /// Authored by a human in a `@tw.task(...)` declaration.
    Declared,
    /// Auto-extracted by parsing an embedded SQL query.
    InferredSql,
    /// Best-effort static analysis of code (pandas / Spark / Python).
    InferredCode,
}

impl OriginSource {
    /// True for anything not hand-declared by an engineer.
    pub fn is_inferred(self) -> bool {
        !matches!(self, OriginSource::Declared)
    }

    /// Short tag rendered inline next to inferred labels, e.g. `(inferred)`.
    /// Returns `None` for declared facts so labels stay clean.
    pub fn inline_tag(self) -> Option<&'static str> {
        match self {
            OriginSource::Declared => None,
            OriginSource::InferredSql => Some("inferred from SQL"),
            OriginSource::InferredCode => Some("inferred from code"),
        }
    }
}

/// Full provenance record attached to lineage elements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Origin {
    pub source: OriginSource,
    /// Heuristic confidence in `[0.0, 1.0]` for inferred facts. `None` when declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    /// Free-text explanation, e.g. which analyzer produced the inference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Where in source this fact was recovered from, e.g. the transform
    /// function's file/line for a machine-inferred dataset, edge, or
    /// column-discovery edge. `None` when unknown (declared facts don't need
    /// one — the decorator itself is the provenance). Additive field: absent
    /// from documents written before it existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLoc>,
}

impl Origin {
    /// A hand-declared fact (full confidence, no annotation).
    pub fn declared() -> Self {
        Origin {
            source: OriginSource::Declared,
            confidence: None,
            note: None,
            location: None,
        }
    }

    /// A fact extracted by parsing SQL.
    pub fn inferred_sql(confidence: f32) -> Self {
        Origin {
            source: OriginSource::InferredSql,
            confidence: Some(confidence),
            note: None,
            location: None,
        }
    }

    /// A fact produced by best-effort code analysis.
    pub fn inferred_code(confidence: f32) -> Self {
        Origin {
            source: OriginSource::InferredCode,
            confidence: Some(confidence),
            note: None,
            location: None,
        }
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Stamp the source file/line this fact was recovered from.
    pub fn with_location(mut self, loc: SourceLoc) -> Self {
        self.location = Some(loc);
        self
    }

    pub fn is_inferred(&self) -> bool {
        self.source.is_inferred()
    }

    /// Render a human label, appending an `(inferred …)` suffix when not declared.
    /// `base` is the underlying text (e.g. a column `function` label).
    pub fn annotate(&self, base: &str) -> String {
        match self.source.inline_tag() {
            None => base.to_string(),
            Some(tag) => {
                if base.is_empty() {
                    format!("({tag})")
                } else {
                    format!("{base} ({tag})")
                }
            }
        }
    }
}

impl Default for Origin {
    fn default() -> Self {
        Origin::declared()
    }
}
