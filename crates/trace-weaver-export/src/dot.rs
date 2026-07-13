//! Graphviz DOT exporter — a quick local visualisation of the lineage graph.
//!
//! Nodes are datasets (coloured by platform); edges show the transform `kind`.
//! Edges or columns containing inferred lineage are drawn dashed so guesses are
//! visually distinct from declared lineage.

use trace_weaver_core::WeaveDocument;

use crate::{ExportConfig, ExportReport};

/// Escape a string for use inside a double-quoted DOT label/attribute.
fn dot_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

/// A stable, DOT-safe node identifier derived from a dataset name.
fn node_id(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    out.push_str(&dot_escape(name));
    out.push('"');
    out
}

/// The label shown on a dataset node: the last FQN segment when present,
/// otherwise the whole name.
fn node_label(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// Pick a fill colour for a platform, or `None` for unstyled nodes.
fn platform_colour(platform: &str) -> &'static str {
    match platform.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => "#cfe2f3",
        "delta" | "deltalake" => "#d9ead3",
        "s3" => "#fff2cc",
        "kafka" => "#f4cccc",
        "snowflake" => "#d0e0e3",
        "mysql" => "#fce5cd",
        _ => "#eeeeee",
    }
}

pub fn export(doc: &WeaveDocument, cfg: &ExportConfig) -> anyhow::Result<ExportReport> {
    let _ = cfg;

    let mut s = String::new();
    s.push_str("digraph lineage {\n");
    s.push_str("  rankdir=LR;\n");
    s.push_str("  node [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\"];\n");
    s.push_str("  edge [fontname=\"Helvetica\"];\n");

    // ── nodes ──
    let mut node_count = 0usize;
    for ds in &doc.datasets {
        let id = node_id(&ds.name);
        let label = dot_escape(node_label(&ds.name));
        let tooltip = dot_escape(&ds.name);
        let fill = ds
            .platform
            .as_deref()
            .map(platform_colour)
            .unwrap_or("#ffffff");
        s.push_str(&format!(
            "  {id} [label=\"{label}\", tooltip=\"{tooltip}\", fillcolor=\"{fill}\"];\n"
        ));
        node_count += 1;
    }

    // ── edges (stable topological order) ──
    let mut edge_count = 0usize;
    for edge in doc.edges_in_topo_order() {
        let from = node_id(&edge.from);
        let to = node_id(&edge.to);
        let label = dot_escape(edge.transform.kind.as_deref().unwrap_or(""));
        let style = if edge.has_inferred() {
            "style=dashed, color=orange"
        } else {
            "style=solid, color=black"
        };
        s.push_str(&format!("  {from} -> {to} [label=\"{label}\", {style}];\n"));
        edge_count += 1;
    }

    s.push_str("}\n");

    Ok(ExportReport {
        target: "dot".to_string(),
        artifact: s,
        actions: vec![format!("wrote {node_count} nodes, {edge_count} edges")],
        sent: 0,
        failed: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::tiny_doc;

    #[test]
    fn dot_contains_digraph_and_nodes() {
        let doc = tiny_doc();
        let report = export(&doc, &ExportConfig::default()).unwrap();
        assert!(report.artifact.contains("digraph"));
        assert!(report.artifact.contains("bronze_sales"));
        assert!(report.actions[0].contains("nodes"));
    }

    #[test]
    fn dot_renders_self_edge() {
        // A self-edge must render as a valid Graphviz `"X" -> "X"` line (Graphviz
        // draws these as self-loops) — no panic, one edge.
        let doc = crate::test_support::self_loop_doc();
        let report = export(&doc, &ExportConfig::default()).unwrap();
        let node = "\"Test Database.poc_db.public.orphans\"";
        assert!(
            report.artifact.contains(&format!("{node} -> {node}")),
            "expected a self-edge line, got:\n{}",
            report.artifact
        );
        assert!(report.actions[0].contains("1 edges"));
    }
}
