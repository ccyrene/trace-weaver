//! Lineage-graph helpers over a [`WeaveDocument`]: deriving edges from jobs,
//! deterministic ordering, and up/downstream traversal.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{DatasetRef, Edge, Transform, WeaveDocument};
use crate::origin::Origin;

impl WeaveDocument {
    /// For every job, ensure a dataset→dataset edge exists for each
    /// (input, output) pair. Existing edges (declared or already present) are
    /// left untouched; only missing pairs are added, carrying the job's
    /// description/sql/origin. Returns the number of edges added.
    ///
    /// This lets engineers declare just `inputs`/`outputs` on a job and still
    /// get edges, while column-level detail is layered on separately.
    pub fn derive_edges_from_jobs(&mut self) -> usize {
        let existing: BTreeSet<(DatasetRef, DatasetRef)> = self
            .edges
            .iter()
            .map(|e| (e.from.clone(), e.to.clone()))
            .collect();

        let mut to_add: Vec<Edge> = Vec::new();
        for job in &self.jobs {
            for from in &job.inputs {
                for to in &job.outputs {
                    if from == to {
                        continue;
                    }
                    let key = (from.clone(), to.clone());
                    if existing.contains(&key)
                        || to_add.iter().any(|e| e.from == *from && e.to == *to)
                    {
                        continue;
                    }
                    let mut edge = Edge::new(from.clone(), to.clone());
                    edge.job = Some(job.id.clone());
                    edge.transform = Transform {
                        kind: None,
                        description: job.description.clone(),
                        sql: job.sql.clone(),
                    };
                    edge.origin = job.origin.clone();
                    to_add.push(edge);
                }
            }
        }
        let n = to_add.len();
        self.edges.append(&mut to_add);
        n
    }

    /// Datasets that feed `target` directly (one hop upstream).
    pub fn upstream_of<'a>(&'a self, target: &str) -> Vec<&'a str> {
        self.edges
            .iter()
            .filter(|e| e.to == target)
            .map(|e| e.from.as_str())
            .collect()
    }

    /// Datasets fed by `source` directly (one hop downstream).
    pub fn downstream_of<'a>(&'a self, source: &str) -> Vec<&'a str> {
        self.edges
            .iter()
            .filter(|e| e.from == source)
            .map(|e| e.to.as_str())
            .collect()
    }

    /// Edges sorted into a stable, dependency-respecting order: a topological
    /// sort of datasets (Kahn's algorithm, ties broken by name for
    /// determinism), with edges emitted in that order. Cyclic remnants are
    /// appended deterministically so the result is always a total order.
    pub fn edges_in_topo_order(&self) -> Vec<&Edge> {
        // Build adjacency over datasets that actually appear on edges.
        let mut indeg: BTreeMap<&str, usize> = BTreeMap::new();
        let mut adj: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for e in &self.edges {
            indeg.entry(e.from.as_str()).or_insert(0);
            *indeg.entry(e.to.as_str()).or_insert(0) += 1;
            adj.entry(e.from.as_str()).or_default().push(e.to.as_str());
        }

        // Kahn's algorithm with a sorted ready-set for determinism.
        let mut order: BTreeMap<&str, usize> = BTreeMap::new();
        let mut ready: BTreeSet<&str> = indeg
            .iter()
            .filter(|(_, &d)| d == 0)
            .map(|(&n, _)| n)
            .collect();
        let mut idx = 0usize;
        while let Some(&node) = ready.iter().next() {
            ready.remove(node);
            order.insert(node, idx);
            idx += 1;
            if let Some(next) = adj.get(node) {
                let mut next_sorted = next.clone();
                next_sorted.sort_unstable();
                for &m in &next_sorted {
                    let d = indeg.get_mut(m).expect("node present");
                    *d -= 1;
                    if *d == 0 {
                        ready.insert(m);
                    }
                }
            }
        }
        // Any dataset left out of `order` (part of a cycle) gets a high rank.
        let rank = |n: &str| order.get(n).copied().unwrap_or(usize::MAX);

        let mut edges: Vec<&Edge> = self.edges.iter().collect();
        edges.sort_by(|a, b| {
            rank(&a.from)
                .cmp(&rank(&b.from))
                .then_with(|| rank(&a.to).cmp(&rank(&b.to)))
                .then_with(|| a.from.cmp(&b.from))
                .then_with(|| a.to.cmp(&b.to))
        });
        edges
    }
}

/// Combine two origins for a merged element: a declared origin always wins;
/// otherwise keep the higher-confidence inference.
pub fn merge_origin(a: Origin, b: Origin) -> Origin {
    match (a.is_inferred(), b.is_inferred()) {
        (false, _) => a,
        (true, false) => b,
        (true, true) => {
            if b.confidence.unwrap_or(0.0) > a.confidence.unwrap_or(0.0) {
                b
            } else {
                a
            }
        }
    }
}
