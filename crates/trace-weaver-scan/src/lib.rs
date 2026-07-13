//! # trace-weaver-scan
//!
//! Turns annotated Python DAG code into a [`WeaveDocument`].
//!
//! Pipeline per file:
//! 1. [`python`] parses the source (rustpython-parser) and extracts every
//!    `@tw.task(...)` declaration into a [`python::TaskDecl`] (literals only —
//!    no code execution).
//! 2. The declaration's `inputs`/`outputs`/`column_map`/`sql` become datasets,
//!    a job, and one or more edges. Their origin is **declared** for a `@tw`
//!    task, or **inferred** when the task was discovered decorator-free from a
//!    raw operator (its very existence is a machine inference, not a human fact).
//! 3. When `engine="sql"` and a `sql=` query is present, [`sql`] auto-extracts
//!    column lineage to fill any columns the engineer didn't map by hand
//!    (origin = **inferred from SQL**).
//! 4. [`dataflow`] traces the task's function BODY (pandas/Spark) for column
//!    lineage, and [`infer`] fills any remaining same-name identity gaps
//!    (both origin = **inferred from code**). Untraceable spots are reported as
//!    `W_OPAQUE_COLUMN`, never silently dropped.
//!
//! Declared facts always win; inference only fills gaps and is tagged so
//! exporters can render `(inferred …)` next to it.

use std::path::Path;

use trace_weaver_core::{
    ColumnEdge, ColumnRef, Dataset, Diagnostic, Edge, Engine, FqnParts, Job, Origin, SourceLoc,
    Transform, TransformType, WeaveDocument,
};

use crate::python::{ColumnMapEntry, ModuleConfig, TaskDecl};
use crate::sql::{SqlColRef, SqlMapping};

pub mod dataflow;
pub mod infer;
pub mod python;
pub mod resolve;
pub mod sql;

/// Knobs controlling a scan.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// Default document namespace for datasets that don't specify one.
    pub namespace: String,
    /// `producer` string written into the document.
    pub producer: String,
    /// Auto-extract column lineage from embedded SQL.
    pub enable_sql_inference: bool,
    /// Best-effort code analysis (pandas/Spark) to fill undeclared gaps.
    pub enable_code_inference: bool,
    /// Default FQN parts for raw DAGs without `tw.configure(...)`. They fill any
    /// gap the file's own `configure()` left, so bare table names expand to
    /// `service.database.schema.table` and export to OpenMetadata.
    pub service: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        ScanOptions {
            namespace: "default".to_string(),
            producer: concat!("trace-weaver/", env!("CARGO_PKG_VERSION")).to_string(),
            enable_sql_inference: true,
            enable_code_inference: true,
            service: None,
            database: None,
            schema: None,
        }
    }
}

/// Confidence assigned to SQL-inferred column lineage.
const SQL_CONFIDENCE: f32 = 0.85;

/// Confidence assigned to column lineage traced from pandas/Spark code. Lower
/// than SQL (looser semantics), higher than the same-name identity gap-fill.
const CODE_DATAFLOW_CONFIDENCE: f32 = 0.7;

/// Confidence for a `@lineage` dataset that was declared as a non-literal (an
/// f-string / name / call): the engineer's intent is real but the exact URI is
/// only a best-effort textual reconstruction, so it is "medium".
const LINEAGE_MEDIUM_CONFIDENCE: f32 = 0.5;

/// Provenance note stamped on structural elements of a `@lineage` task. Contains
/// the marker substring `@lineage` so the finalize pass can recognise
/// dataset-level lineage and skip the "no column lineage" advisory for it.
const LINEAGE_NOTE: &str = "@lineage declared dataset-level lineage";

/// Provenance note for a `@lineage` endpoint declared as a non-literal.
const LINEAGE_NONLITERAL_NOTE: &str = "@lineage non-literal dataset (best-effort text)";

/// Recursively scan every `*.py` file under `root` and assemble one document.
///
/// After collecting all files this derives any missing job-level edges,
/// runs structural validation, and folds the findings into
/// `document.diagnostics`.
pub fn scan_path(root: &Path, opts: &ScanOptions) -> anyhow::Result<WeaveDocument> {
    let mut doc = WeaveDocument::new(opts.namespace.clone(), opts.producer.clone());

    // First walk: read every `*.py` file once, deriving the dotted module path it
    // would be imported under. Sources are cached so the constant-table pass and
    // the scan pass don't re-read (or re-fail) the disk.
    struct SourceFile {
        path_str: String,
        module_path: String,
        source: String,
    }
    let mut files: Vec<SourceFile> = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("py") {
            continue;
        }
        let path_str = path.display().to_string();
        match std::fs::read_to_string(path) {
            Ok(source) => files.push(SourceFile {
                module_path: module_path_of(root, path),
                path_str,
                source,
            }),
            Err(e) => doc.diagnostics.push(
                Diagnostic::error("E_READ", format!("could not read {path_str}: {e}"))
                    .at(SourceLoc::new(path_str.clone(), None)),
            ),
        }
    }

    // Build the repo-wide constant symbol table BEFORE scanning any file, so a
    // dataset declared as `from config.datasets import RAW_SALES` resolves even
    // when its defining file is scanned later.
    let mut table = python::ConstTable::default();
    for f in &files {
        table.insert_module(
            f.module_path.clone(),
            python::collect_module_constants(&f.source),
        );
    }

    // Second pass: scan each file with the table in scope. Per-file errors are
    // collected as diagnostics; one bad file must not abort the whole scan.
    for f in &files {
        if let Err(e) = scan_source_with_table(
            &f.path_str,
            &f.module_path,
            &f.source,
            &table,
            opts,
            &mut doc,
        ) {
            doc.diagnostics.push(
                Diagnostic::error("E_PARSE", format!("failed to scan {}: {e}", f.path_str))
                    .at(SourceLoc::new(f.path_str.clone(), None)),
            );
        }
    }

    doc.derive_edges_from_jobs();

    // Cross-job finalisation: back-fill schemas from observed columns, run code
    // inference to fill remaining gaps, and flag edges with no column lineage.
    finalize(&mut doc, opts);

    let validation = trace_weaver_core::validate(&doc);
    doc.diagnostics.extend(validation);

    Ok(doc)
}

/// Post-pass over the whole document (findings #3 / #11): once every job has been
/// scanned, datasets know their observed columns, code inference can fill
/// same-named gaps, and we can flag data-producing edges that ended up with no
/// column lineage at all.
fn finalize(doc: &mut WeaveDocument, opts: &ScanOptions) {
    backfill_schemas(doc);

    if opts.enable_code_inference {
        code_inference_pass(doc);
    }

    // An edge carrying zero column lineage is the real, detectable gap (e.g. a
    // pandas/Spark task with no `column_map`). This is the actionable warning —
    // engineers should declare a `column_map` for it.
    let mut warnings = Vec::new();
    for e in &doc.edges {
        // A `@lineage` edge is declarative dataset-level lineage by design — it
        // has no column map to extract, so the "declare a column_map" advisory
        // does not apply. Recognise it by the `@lineage` marker in its origin note.
        let is_lineage_edge = e
            .origin
            .note
            .as_deref()
            .is_some_and(|n| n.contains("@lineage"));
        if e.column_lineage.is_empty() && !is_lineage_edge {
            let loc = e
                .job
                .as_ref()
                .and_then(|jid| doc.jobs.iter().find(|j| &j.id == jid))
                .and_then(|j| j.location.clone());
            let from = e.from.rsplit('.').next().unwrap_or(&e.from);
            let to = e.to.rsplit('.').next().unwrap_or(&e.to);
            let mut d = Diagnostic::warn(
                "W_NO_COLUMN_LINEAGE",
                format!(
                    "edge {from} -> {to} has no column-level lineage; declare a column_map \
                     (only SQL transforms are auto-extracted)"
                ),
            );
            if let Some(l) = loc {
                d = d.at(l);
            }
            warnings.push(d);
        }
    }
    doc.diagnostics.extend(warnings);
}

/// Merge the columns observed on edges into each dataset's `schema`, so the
/// document records known columns and `validate()`'s column checks become live.
fn backfill_schemas(doc: &mut WeaveDocument) {
    use std::collections::BTreeMap;
    let mut cols: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let note = |ds: &str, col: &str, m: &mut BTreeMap<String, Vec<String>>| {
        let v = m.entry(ds.to_string()).or_default();
        if !v.iter().any(|c| c == col) {
            v.push(col.to_string());
        }
    };
    for e in &doc.edges {
        for ce in &e.column_lineage {
            note(&ce.to_column.dataset, &ce.to_column.column, &mut cols);
            for fc in &ce.from_columns {
                note(&fc.dataset, &fc.column, &mut cols);
            }
        }
    }
    for (ds_name, columns) in cols {
        if let Some(ds) = doc.dataset_mut(&ds_name) {
            for c in columns {
                if !ds.schema.iter().any(|f| f.name == c) {
                    ds.schema.push(trace_weaver_core::Field::new(c));
                }
            }
        }
    }
}

/// Best-effort identity gap-fill (origin = inferred_code) for output columns that
/// share a name with an input column but weren't mapped on that edge.
fn code_inference_pass(doc: &mut WeaveDocument) {
    let datasets: std::collections::HashMap<String, Dataset> = doc
        .datasets
        .iter()
        .map(|d| (d.name.clone(), d.clone()))
        .collect();
    for edge in &mut doc.edges {
        let already: Vec<String> = edge
            .column_lineage
            .iter()
            .map(|c| c.to_column.column.clone())
            .collect();
        let (src, tgt) = match (datasets.get(&edge.from), datasets.get(&edge.to)) {
            (Some(s), Some(t)) => (s, t),
            _ => continue,
        };
        for ce in infer::infer_identity_gap_fill(&[src], tgt, &already) {
            edge.column_lineage.push(ce);
        }
    }
}

/// Derive the dotted module path a file would be imported under, relative to the
/// scan `root` — mirroring how Python's import machinery sees a file when `root`
/// is on `sys.path`. `pkg/mod.py` → `pkg.mod`; a package's `__init__.py` →
/// `pkg`. Falls back to the bare file stem when the path is not under `root`.
fn module_path_of(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    let mut parts: Vec<String> = rel
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => s.to_str().map(|s| s.to_string()),
            _ => None,
        })
        .collect();
    if let Some(last) = parts.last_mut() {
        if let Some(stem) = last.strip_suffix(".py") {
            *last = stem.to_string();
        }
        // A package's __init__ IS the package; drop the trailing segment.
        if parts.last().map(|s| s == "__init__").unwrap_or(false) {
            parts.pop();
        }
    }
    if parts.is_empty() {
        // e.g. root itself is the file — use its stem.
        return path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
    }
    parts.join(".")
}

/// Scan a single Python source string into `doc` (used directly in tests). This
/// resolves only same-file constants; cross-module constant resolution requires
/// the repo-wide table built by [`scan_path`].
pub fn scan_source(
    path: &str,
    source: &str,
    opts: &ScanOptions,
    doc: &mut WeaveDocument,
) -> anyhow::Result<()> {
    scan_source_with_table(path, "", source, &python::ConstTable::default(), opts, doc)
}

/// Scan one source with a repo-wide [`ConstTable`](python::ConstTable) in scope,
/// so dataset references to constants in OTHER modules resolve. `module_path` is
/// this file's own dotted module path (anchors relative imports).
fn scan_source_with_table(
    path: &str,
    module_path: &str,
    source: &str,
    table: &python::ConstTable,
    opts: &ScanOptions,
    doc: &mut WeaveDocument,
) -> anyhow::Result<()> {
    let module = python::extract_task_decls_with(source, module_path, table)?;
    // CLI-supplied FQN defaults fill any part the file's own configure() left unset,
    // so raw DAGs (no tw.configure) still expand bare table names to full FQNs.
    let mut config = module.config;
    if config.service.is_none() {
        config.service = opts.service.clone();
    }
    if config.database.is_none() {
        config.database = opts.database.clone();
    }
    if config.schema.is_none() {
        config.schema = opts.schema.clone();
    }
    for decl in &module.tasks {
        scan_decl(path, decl, &config, opts, doc);
    }
    Ok(())
}

/// Expand a bare table name into a full FQN using the module's `configure(...)`
/// defaults. Names that already contain a `.` (assumed to be fuller paths/FQNs)
/// are left untouched; bare names are prefixed `service.database.schema.` only
/// when all three defaults are present.
fn expand_fqn(name: &str, config: &ModuleConfig) -> String {
    if name.contains('.') {
        return name.to_string();
    }
    match (&config.service, &config.database, &config.schema) {
        (Some(s), Some(d), Some(sc)) => format!("{s}.{d}.{sc}.{name}"),
        _ => name.to_string(),
    }
}

/// Turn one task declaration into datasets + a job + edges with column lineage.
fn scan_decl(
    path: &str,
    decl: &TaskDecl,
    config: &ModuleConfig,
    opts: &ScanOptions,
    doc: &mut WeaveDocument,
) {
    // Column/dataflow-discovery declarations (Pass C) produce datasets and
    // column-carrying edges but NO job, so they never enter the gate's task
    // denominator. Handled on a dedicated, side-effect-free path.
    if decl.column_only {
        scan_column_discovery(path, decl, config, opts, doc);
        return;
    }
    let loc = SourceLoc::new(path, decl.line);

    // Tier 1: infer the engine when omitted — `sql` if a query is present,
    // otherwise `python` (we no longer require an explicit engine=).
    let engine = match &decl.engine {
        Some(e) => Engine::from_str_loose(e),
        None if decl.sql.is_some() => Engine::Sql,
        None => Engine::Python,
    };

    // Tier 1: expand bare table names into full FQNs via configure() defaults.
    let inputs: Vec<String> = decl.inputs.iter().map(|n| expand_fqn(n, config)).collect();
    let outputs: Vec<String> = decl.outputs.iter().map(|n| expand_fqn(n, config)).collect();

    // Decorator-free discovery (Pass B) infers the WHOLE task — its job, edges and
    // datasets, not just its columns. Stamp those structural elements inferred so
    // the IR never claims a discovered task was hand-declared; a `@tw` task keeps
    // declared. (Column-level origin is set independently per mapping below.)
    let element_origin = if decl.discovered {
        discovered_origin(engine)
    } else if decl.lineage {
        // A @lineage task is hand-declared, but tagged so exporters/finalize can
        // tell it apart as declarative dataset-level lineage.
        Origin::declared().with_note(LINEAGE_NOTE)
    } else {
        Origin::declared()
    };

    // For a @lineage task, non-literal dataset entries carry medium (inferred)
    // confidence while literal entries stay declared/high. The recorded
    // non-literal texts are FQN-expanded so they match `inputs`/`outputs`.
    let nonliteral: std::collections::HashSet<String> = if decl.lineage {
        decl.nonliteral
            .iter()
            .map(|n| expand_fqn(n, config))
            .collect()
    } else {
        std::collections::HashSet::new()
    };
    let endpoint_origin = |name: &str| -> Origin {
        if nonliteral.contains(name) {
            Origin::inferred_code(LINEAGE_MEDIUM_CONFIDENCE).with_note(LINEAGE_NONLITERAL_NOTE)
        } else {
            element_origin.clone()
        }
    };

    // ── Hygiene diagnostics (findings #6, #10; I/O still required) ───────
    emit_decl_diagnostics(decl, engine, &inputs, &outputs, &loc, doc);

    // Spots the pandas/Spark dataflow analyzer could not trace — tell the engineer
    // exactly which column to declare by hand, instead of silently dropping it.
    // A note about a column the engineer ALREADY declared (column_map / copy) is
    // just noise — the declaration covers it — so suppress those. The two notes
    // that name a target embed it as `"col"`, which is how we match it here.
    if opts.enable_code_inference {
        let declared: std::collections::HashSet<&str> = decl
            .column_map
            .iter()
            .map(|e| e.target.as_str())
            .chain(decl.copy.iter().map(|s| s.as_str()))
            .collect();
        for note in &decl.opaque {
            if declared
                .iter()
                .any(|c| note.detail.contains(&format!("\"{c}\"")))
            {
                continue;
            }
            let mut d = Diagnostic::warn(
                "W_OPAQUE_COLUMN",
                format!("task '{}': {}", decl.task_name, note.detail),
            );
            if let Some(line) = note.line {
                d = d.at(SourceLoc::new(path, Some(line)));
            }
            doc.diagnostics.push(d);
        }
    }

    // 1. Upsert datasets for every input and output.
    for ds_name in inputs.iter().chain(outputs.iter()) {
        doc.upsert_dataset(build_dataset(ds_name, &endpoint_origin(ds_name)));
    }

    // 2. Build the job. Tier 2: default the DAG from configure()/`with DAG(...)`.
    let dag = decl
        .dag
        .clone()
        .or_else(|| config.dag.clone())
        .unwrap_or_else(|| "default".to_string());
    let job_id = format!("{dag}.{}", decl.task_name);
    let mut job = Job::new(job_id.clone(), decl.task_name.clone(), engine);
    job.dag = Some(dag);
    job.location = Some(loc.clone());
    job.description = decl.description.clone();
    job.sql = decl.sql.clone();
    job.inputs = inputs.clone();
    job.outputs = outputs.clone();
    job.origin = element_origin.clone();
    doc.jobs.push(job);

    // 3. Parse the SQL transform once (the INSERT targets a single table), and
    //    surface diagnostics for projections we refuse to map (findings #1/#2).
    let sql_lineage = if opts.enable_sql_inference && engine == Engine::Sql {
        decl.sql
            .as_deref()
            .and_then(|s| sql::column_lineage_from_sql(s).ok())
    } else {
        None
    };
    if let Some(l) = &sql_lineage {
        if l.wildcard_in_projection {
            doc.diagnostics.push(
                Diagnostic::warn(
                    "W_SQL_WILDCARD",
                    format!(
                        "task '{}': SQL uses SELECT * with an explicit INSERT column list; \
                         column lineage cannot be derived safely — declare a column_map",
                        decl.task_name
                    ),
                )
                .at(loc.clone()),
            );
        }
        if let Some((proj, tgt)) = l.arity_mismatch {
            doc.diagnostics.push(
                Diagnostic::warn(
                    "W_SQL_ARITY",
                    format!(
                        "task '{}': SQL projection has {proj} column(s) but the INSERT lists {tgt}; \
                         column lineage skipped — declare a column_map",
                        decl.task_name
                    ),
                )
                .at(loc.clone()),
            );
        }
    }

    // 4. Build column lineage once per output, then distribute each mapping to
    //    the (input, output) edges its sources actually belong to (finding #4).
    let default_input = inputs.first().cloned();
    let single_output = outputs.len() == 1;
    for output in &outputs {
        let col_edges = match &default_input {
            Some(di) => build_output_column_edges(
                decl,
                output,
                di,
                &inputs,
                sql_lineage.as_ref(),
                opts.enable_code_inference,
                single_output,
            ),
            None => Vec::new(),
        };

        let mut first_edge = true;
        for input in &inputs {
            // A self-loop (input == output) is normally noise, so it is skipped —
            // but a DECLARED self-loop is a legitimate hand-authored fact (e.g. a
            // `@lineage(inputs=["s3://x"], outputs=["s3://x"])` "read prefix X and
            // delete orphans in X"), so we emit it. The edge origin computed below
            // is inferred only when an endpoint was non-literal; skip the self-loop
            // in exactly that inferred case, mirroring the derive-edges guard.
            if input == output && (nonliteral.contains(input) || element_origin.is_inferred()) {
                continue;
            }
            let mut edge = Edge::new(input.clone(), output.clone());
            edge.job = Some(job_id.clone());
            edge.transform = Transform {
                kind: decl.transform.clone(),
                description: decl.description.clone(),
                sql: decl.sql.clone(),
            };
            edge.origin = if nonliteral.contains(input) || nonliteral.contains(output) {
                Origin::inferred_code(LINEAGE_MEDIUM_CONFIDENCE).with_note(LINEAGE_NONLITERAL_NOTE)
            } else {
                element_origin.clone()
            };

            for ce in &col_edges {
                let attach = if ce.from_columns.is_empty() {
                    // A source-less mapping (e.g. COUNT(*)) can't be attributed
                    // to a particular input; attach it to one edge only.
                    first_edge
                } else {
                    ce.from_columns.iter().any(|fc| &fc.dataset == input)
                };
                if attach {
                    edge.column_lineage.push(ce.clone());
                }
            }
            doc.edges.push(edge);
            first_edge = false;
        }
    }
}

/// Build datasets + column-carrying edges for a COLUMN/dataflow-discovery
/// declaration (Pass C) — an undecorated, un-wired top-level function whose body
/// was traced purely for column lineage (e.g. the `run(spark, …)` transform
/// protocol; a `spark.sql(f"INSERT … SELECT CAST(…) FROM view")`).
///
/// Deliberately side-effect-light: it emits **no job** (so the gate task
/// denominator is unchanged), no hygiene diagnostics, and no `W_OPAQUE_COLUMN`
/// advisories. Every element it does emit is inferred (never declared/HIGH), and
/// an edge is only pushed when it actually carries a column mapping.
fn scan_column_discovery(
    path: &str,
    decl: &TaskDecl,
    config: &ModuleConfig,
    opts: &ScanOptions,
    doc: &mut WeaveDocument,
) {
    if !opts.enable_code_inference {
        return; // column discovery IS code inference — respect the opt-out
    }
    let engine = match &decl.engine {
        Some(e) => Engine::from_str_loose(e),
        None => Engine::Python,
    };
    let inputs: Vec<String> = decl.inputs.iter().map(|n| expand_fqn(n, config)).collect();
    let outputs: Vec<String> = decl.outputs.iter().map(|n| expand_fqn(n, config)).collect();
    // Need at least one endpoint on each side to hang a dataset→dataset edge.
    if inputs.is_empty() || outputs.is_empty() {
        return;
    }
    let _loc = SourceLoc::new(path, decl.line); // kept for parity / future use
    let element_origin = discovered_origin(engine);

    for ds_name in inputs.iter().chain(outputs.iter()) {
        doc.upsert_dataset(build_dataset(ds_name, &element_origin));
    }

    let default_input = inputs.first().cloned();
    let single_output = outputs.len() == 1;
    for output in &outputs {
        let col_edges = match &default_input {
            Some(di) => build_output_column_edges(
                decl,
                output,
                di,
                &inputs,
                None,
                opts.enable_code_inference,
                single_output,
            ),
            None => Vec::new(),
        };
        if col_edges.is_empty() {
            continue;
        }
        for input in &inputs {
            if input == output {
                continue; // self-loops are noise for inferred discovery
            }
            let mut edge = Edge::new(input.clone(), output.clone());
            edge.transform = Transform {
                kind: decl.transform.clone(),
                description: decl.description.clone(),
                sql: None,
            };
            edge.origin = element_origin.clone();
            // Mark this as a column-discovery (Pass C) edge so the gate measures
            // it in the column dimension, not the task/declared edge ratio.
            edge.column_discovery = true;
            for ce in &col_edges {
                let attach = !ce.from_columns.is_empty()
                    && ce.from_columns.iter().any(|fc| &fc.dataset == input);
                if attach {
                    edge.column_lineage.push(ce.clone());
                }
            }
            // Only emit an edge that actually carries a column mapping — a bare
            // dataset-level edge here would be noise (and would trip the
            // no-column-lineage advisory).
            if !edge.column_lineage.is_empty() {
                doc.edges.push(edge);
            }
        }
    }
}

/// Emit hygiene diagnostics for one declaration. `inputs`/`outputs` are the
/// FQN-expanded dataset names. (Engine is inferred when omitted, so a missing
/// engine is no longer an error — Tier 1.)
fn emit_decl_diagnostics(
    decl: &TaskDecl,
    engine: Engine,
    inputs: &[String],
    outputs: &[String],
    loc: &SourceLoc,
    doc: &mut WeaveDocument,
) {
    // inputs and outputs are mandatory for a data-producing task — except a
    // `@lineage` marker, which is declarative and may legitimately declare only
    // one side (or, in its bare form, none).
    if !decl.lineage && (inputs.is_empty() || outputs.is_empty()) {
        doc.diagnostics.push(
            Diagnostic::error(
                "E_MISSING_IO",
                format!(
                    "task '{}' must declare both inputs= and outputs= (got {} input(s), {} output(s))",
                    decl.task_name,
                    inputs.len(),
                    outputs.len()
                ),
            )
            .at(loc.clone()),
        );
    }
    // (#5) sql= on a non-SQL engine is silently ignored — warn.
    if decl.sql.is_some() && engine != Engine::Sql {
        doc.diagnostics.push(
            Diagnostic::warn(
                "W_SQL_NOT_PARSED",
                format!(
                    "task '{}' provides sql= but engine is {:?}; SQL column lineage is only \
                     extracted for engine=\"sql\"",
                    decl.task_name, engine
                ),
            )
            .at(loc.clone()),
        );
    }
    // (#6) keyword arguments that weren't literals/constants were dropped.
    for kw in &decl.unresolved {
        doc.diagnostics.push(
            Diagnostic::warn(
                "W_NON_LITERAL",
                format!(
                    "task '{}': argument '{kw}' is not a string literal or module-level constant \
                     and was ignored (trace-weaver scans statically — use a literal or a top-level constant)",
                    decl.task_name
                ),
            )
            .at(loc.clone()),
        );
    }
    // (#10) dataset FQNs that aren't 4-part are accepted but won't export to OM.
    // After Tier 1 expansion this only fires when there's no configure() default
    // and the engineer wrote a bare name. `@lineage` datasets are intentionally
    // dataset URIs (s3://, iceberg://, conn refs), not OM FQNs, so the OM-export
    // advisory would be pure noise for them — skip it.
    for ds_name in inputs.iter().chain(outputs.iter()) {
        if decl.lineage {
            continue;
        }
        if FqnParts::parse(ds_name).is_none() {
            doc.diagnostics.push(
                Diagnostic::warn(
                    "W_BAD_FQN",
                    format!(
                        "dataset '{ds_name}' is not a 4-part FQN 'service.database.schema.table' \
                         (add tw.configure(service=, database=, schema=) or write the full FQN); \
                         the OpenMetadata exporter will skip it"
                    ),
                )
                .at(loc.clone()),
            );
        }
    }
}

/// Provenance for the structural elements (job, edges, datasets) of a task
/// recovered by decorator-FREE discovery (Pass B). Such a task was not authored
/// by a human — the scanner inferred its very existence by statically reading a
/// raw Airflow operator — so its job/edges/datasets are inferred too, mirroring
/// the analyzer that produced them: SQL parsing (`engine="sql"`) vs. pandas/Spark
/// code reading. A `@tw`-declared task keeps `Origin::declared()`.
fn discovered_origin(engine: Engine) -> Origin {
    let base = match engine {
        Engine::Sql => Origin::inferred_sql(SQL_CONFIDENCE),
        _ => Origin::inferred_code(CODE_DATAFLOW_CONFIDENCE),
    };
    base.with_note("discovered from a raw Airflow operator")
}

/// Build a dataset from an FQN, parsing the OpenMetadata parts when possible.
/// `origin` is the provenance to stamp on it — declared for a `@tw` task,
/// inferred for one recovered by decorator-free discovery.
fn build_dataset(fqn: &str, origin: &Origin) -> Dataset {
    let mut ds = Dataset::new(fqn).with_origin(origin.clone());
    if let Some(parts) = FqnParts::parse(fqn) {
        ds.fqn = Some(parts);
    }
    ds
}

/// Build the column lineage for one output: declared `column_map` entries first,
/// then SQL-inferred mappings for any target column not already declared. Bare
/// source columns resolve against `default_input`; qualified `tbl.col` against
/// the matching input. The SQL result is only applied when it targets this
/// output's table.
fn build_output_column_edges(
    decl: &TaskDecl,
    output: &str,
    default_input: &str,
    all_inputs: &[String],
    sql_lineage: Option<&sql::SqlColumnLineage>,
    enable_code_inference: bool,
    single_output: bool,
) -> Vec<ColumnEdge> {
    let mut edges: Vec<ColumnEdge> = Vec::new();
    let mut mapped: Vec<String> = Vec::new();

    // ── Declared ──
    for entry in &decl.column_map {
        edges.push(declared_column_edge(
            entry,
            default_input,
            output,
            all_inputs,
        ));
        mapped.push(entry.target.clone());
    }

    // ── Inferred from SQL (only for this output's table) ──
    if let Some(l) = sql_lineage {
        let applies = match &l.target_table {
            Some(t) => output.rsplit('.').next() == Some(t.as_str()) || output == t,
            None => true,
        };
        if applies {
            for m in &l.mappings {
                if mapped.iter().any(|c| c == &m.target) {
                    continue;
                }
                edges.push(sql_column_edge(m, default_input, output, all_inputs));
                mapped.push(m.target.clone());
            }
        }
    }

    // ── Inferred from code (pandas/Spark dataflow), gap-filling only ──
    if enable_code_inference {
        for c in &decl.inferred_columns {
            if mapped.iter().any(|t| t == &c.target) {
                continue;
            }
            // Attach to this output when the analyzer named its table, or when
            // the table is unknown and there is exactly one output.
            let applies = match &c.output_table {
                Some(t) => output.rsplit('.').next() == Some(t.as_str()) || output == t,
                None => single_output,
            };
            if !applies {
                continue;
            }
            edges.push(inferred_code_column_edge(
                c,
                default_input,
                output,
                all_inputs,
            ));
            mapped.push(c.target.clone());
        }
    }

    edges
}

/// Build a declared ColumnEdge from a `column_map` entry.
fn declared_column_edge(
    entry: &ColumnMapEntry,
    input: &str,
    output: &str,
    all_inputs: &[String],
) -> ColumnEdge {
    let from_columns: Vec<ColumnRef> = entry
        .sources
        .iter()
        .map(|s| resolve_source_ref(s, input, all_inputs))
        .collect();
    let mut ce = ColumnEdge::new(from_columns, ColumnRef::new(output, entry.target.clone()));
    ce.function = entry.function.clone();
    ce.transform_type = classify_function_label(entry.function.as_deref(), entry.sources.len());
    ce.origin = Origin::declared();
    ce
}

/// Build a ColumnEdge from a SQL-inferred mapping.
fn sql_column_edge(m: &SqlMapping, input: &str, output: &str, all_inputs: &[String]) -> ColumnEdge {
    let from_columns: Vec<ColumnRef> = m
        .sources
        .iter()
        .map(|s| resolve_sql_source_ref(s, input, all_inputs))
        .collect();
    let mut ce = ColumnEdge::new(from_columns, ColumnRef::new(output, m.target.clone()));
    ce.function = Some(m.function.clone());
    ce.transform_type = m.transform_type;
    ce.origin = Origin::inferred_sql(SQL_CONFIDENCE);
    ce
}

/// Build a ColumnEdge from a code-dataflow-inferred mapping (pandas/Spark).
fn inferred_code_column_edge(
    c: &crate::dataflow::InferredColumn,
    input: &str,
    output: &str,
    all_inputs: &[String],
) -> ColumnEdge {
    let from_columns: Vec<ColumnRef> = c
        .sources
        .iter()
        .map(|s| resolve_source_ref(s, input, all_inputs))
        .collect();
    let mut ce = ColumnEdge::new(from_columns, ColumnRef::new(output, c.target.clone()));
    ce.function = c.function.clone();
    ce.transform_type = c.transform_type;
    ce.origin = Origin::inferred_code(CODE_DATAFLOW_CONFIDENCE).with_note("pandas/Spark dataflow");
    ce
}

/// Resolve a bare/qualified declared source column to a [`ColumnRef`].
///
/// `"col"` resolves to the edge's single `input` dataset; `"tbl.col"` resolves
/// against whichever of `all_inputs` ends with `.tbl` (or whose final FQN
/// segment is `tbl`), falling back to `input`.
fn resolve_source_ref(src: &str, input: &str, all_inputs: &[String]) -> ColumnRef {
    if let Some((tbl, col)) = src.rsplit_once('.') {
        // Could be a qualified "tbl.col"; match against inputs.
        if let Some(ds) = match_input_by_table(tbl, all_inputs) {
            return ColumnRef::new(ds, col);
        }
        // Not a recognised table qualifier — treat the whole thing as a column
        // on the edge input only if it has no dot; otherwise use the tail.
        return ColumnRef::new(input, col);
    }
    ColumnRef::new(input, src)
}

/// Same resolution for SQL-derived refs, which carry an optional table.
fn resolve_sql_source_ref(src: &SqlColRef, input: &str, all_inputs: &[String]) -> ColumnRef {
    if let Some(tbl) = &src.table {
        if let Some(ds) = match_input_by_table(tbl, all_inputs) {
            return ColumnRef::new(ds, src.column.clone());
        }
    }
    ColumnRef::new(input, src.column.clone())
}

/// Find an input dataset whose final FQN segment matches `tbl`.
fn match_input_by_table(tbl: &str, all_inputs: &[String]) -> Option<String> {
    all_inputs
        .iter()
        .find(|fqn| fqn.rsplit('.').next() == Some(tbl) || fqn.as_str() == tbl)
        .cloned()
}

/// Classify a declared `function` label into a [`TransformType`].
fn classify_function_label(label: Option<&str>, n_sources: usize) -> TransformType {
    let label = match label {
        Some(l) => l.trim(),
        None => {
            return if n_sources <= 1 {
                TransformType::Identity
            } else {
                TransformType::Transformation
            }
        }
    };
    let upper = label.to_ascii_uppercase();
    if ["COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP BY"]
        .iter()
        .any(|kw| upper.contains(kw))
    {
        return TransformType::Aggregation;
    }
    let l = label.to_ascii_lowercase();
    if l == "direct copy" || l.starts_with("rename") || l == "identity" {
        return TransformType::Identity;
    }
    TransformType::Transformation
}

#[cfg(test)]
mod tests {
    use super::*;
    use trace_weaver_core::OriginSource;

    fn opts() -> ScanOptions {
        ScanOptions {
            namespace: "ns".into(),
            producer: "test".into(),
            enable_sql_inference: true,
            enable_code_inference: true,
            ..Default::default()
        }
    }

    #[test]
    fn declared_column_map_yields_declared_origin() {
        let src = r#"
import trace_weaver as tw
@tw.task(
    dag="d",
    inputs=["Test Database.poc_db.public.bronze_sales"],
    outputs=["Test Database.poc_db.public.silver_sales"],
    engine="pandas",
    transform="ENRICH",
    column_map=[
        (["amount", "currency"], "amount_usd", "ROUND(amount * fx[currency], 2)"),
        (["event_id"], "event_id", "direct copy"),
    ],
)
def build_silver():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("test.py", src, &opts(), &mut doc).unwrap();

        assert_eq!(doc.jobs.len(), 1);
        assert_eq!(doc.jobs[0].engine, Engine::Pandas);
        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        assert_eq!(edge.column_lineage.len(), 2);
        for ce in &edge.column_lineage {
            assert_eq!(ce.origin.source, OriginSource::Declared);
        }
        // Fan-in: amount_usd has two sources, both on the input dataset.
        let usd = edge
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "amount_usd")
            .unwrap();
        assert_eq!(usd.from_columns.len(), 2);
        assert_eq!(
            usd.from_columns[0].dataset,
            "Test Database.poc_db.public.bronze_sales"
        );
        assert_eq!(usd.transform_type, TransformType::Transformation);

        let ev = edge
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "event_id")
            .unwrap();
        assert_eq!(ev.transform_type, TransformType::Identity);
    }

    #[test]
    fn sql_only_hop_yields_inferred_sql_edges() {
        let src = r#"
import trace_weaver as tw
BRONZE_SQL = """
INSERT INTO bronze_sales (event_id, customer_name, amount, currency, event_ts)
SELECT raw_event_id::bigint, customer, amount::numeric(12,2), currency, event_ts::timestamp
FROM (
  SELECT *, row_number() OVER (PARTITION BY raw_event_id ORDER BY ingested_at) AS rn
  FROM landing_sales
) deduped
WHERE rn = 1;
"""
@tw.task(
    dag="medallion_lineage",
    inputs=["Test Database.poc_db.public.landing_sales"],
    outputs=["Test Database.poc_db.public.bronze_sales"],
    engine="sql",
    sql=BRONZE_SQL,
    transform="CAST / PARSE / DEDUPE",
)
def build_bronze():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("test.py", src, &opts(), &mut doc).unwrap();

        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        // 5 target columns, all inferred from SQL (no column_map declared).
        assert_eq!(edge.column_lineage.len(), 5);
        assert!(edge
            .column_lineage
            .iter()
            .all(|c| c.origin.source == OriginSource::InferredSql));

        // event_id <- raw_event_id, resolved to the input dataset.
        let ev = edge
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "event_id")
            .unwrap();
        assert_eq!(ev.from_columns[0].column, "raw_event_id");
        assert_eq!(
            ev.from_columns[0].dataset,
            "Test Database.poc_db.public.landing_sales"
        );
        assert_eq!(
            ev.to_column.dataset,
            "Test Database.poc_db.public.bronze_sales"
        );
        // Inferred display function carries the tag.
        assert!(ev
            .display_function()
            .unwrap()
            .contains("(inferred from SQL)"));
    }

    #[test]
    fn partial_map_mixes_declared_and_inferred() {
        let src = r#"
import trace_weaver as tw
GOLD_SQL = """
INSERT INTO gold_sales_daily (event_date, total_transactions, unique_customers, total_revenue_usd, avg_transaction_usd)
SELECT event_date, COUNT(*), COUNT(DISTINCT customer_name), SUM(amount_usd), ROUND(AVG(amount_usd), 2)
FROM silver_sales
WHERE is_valid
GROUP BY event_date;
"""
@tw.task(
    dag="medallion_lineage",
    inputs=["Test Database.poc_db.public.silver_sales"],
    outputs=["Test Database.poc_db.public.gold_sales_daily"],
    engine="sql",
    sql=GOLD_SQL,
    transform="AGGREGATE daily",
    column_map=[
        (["event_date"], "event_date", "GROUP BY key"),
        (["amount_usd"], "total_revenue_usd", "SUM(amount_usd)"),
    ],
)
def build_gold():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("test.py", src, &opts(), &mut doc).unwrap();

        let edge = &doc.edges[0];
        let by: std::collections::HashMap<_, _> = edge
            .column_lineage
            .iter()
            .map(|c| (c.to_column.column.clone(), c))
            .collect();

        // Declared.
        assert_eq!(by["event_date"].origin.source, OriginSource::Declared);
        assert_eq!(
            by["total_revenue_usd"].origin.source,
            OriginSource::Declared
        );
        // Inferred from SQL (not declared).
        assert_eq!(
            by["total_transactions"].origin.source,
            OriginSource::InferredSql
        );
        assert_eq!(
            by["unique_customers"].origin.source,
            OriginSource::InferredSql
        );
        assert_eq!(
            by["avg_transaction_usd"].origin.source,
            OriginSource::InferredSql
        );
        // All five present, none duplicated.
        assert_eq!(edge.column_lineage.len(), 5);
    }

    #[test]
    fn missing_io_is_an_error_but_missing_engine_is_inferred() {
        // Tier 1: engine is inferred (no error); only missing inputs/outputs errors.
        let src = r#"
import trace_weaver as tw
@tw.task(dag="d")
def t():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains(&"E_MISSING_IO"), "{codes:?}");
        assert!(
            !codes.contains(&"E_MISSING_ENGINE"),
            "engine should be inferred, not errored: {codes:?}"
        );
        assert!(trace_weaver_core::has_errors(&doc.diagnostics));
    }

    #[test]
    fn configure_expands_bare_names_and_sql_shortcut_infers_lineage() {
        // Tier 1+2 end-to-end via scan_source: bare names + @tw.sql + configure().
        let src = r#"
import trace_weaver as tw
tw.configure(service="Test Database", database="poc_db", schema="public")
B_SQL = "INSERT INTO bronze_sales (event_id) SELECT raw_event_id::bigint FROM landing_sales"
@tw.sql(B_SQL, inputs=["landing_sales"], outputs=["bronze_sales"])
def build_bronze():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        // Bare names expanded to full FQNs.
        assert!(doc
            .dataset("Test Database.poc_db.public.bronze_sales")
            .is_some());
        assert!(doc
            .dataset("Test Database.poc_db.public.landing_sales")
            .is_some());
        // No E_/W_BAD_FQN — everything resolved.
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(!codes.contains(&"W_BAD_FQN"), "{codes:?}");
        // @tw.sql inferred a column edge from the SQL.
        assert_eq!(doc.edges.len(), 1);
        assert_eq!(doc.edges[0].column_lineage.len(), 1);
        assert_eq!(doc.edges[0].column_lineage[0].to_column.column, "event_id");
        assert_eq!(doc.jobs[0].engine, Engine::Sql);
    }

    #[test]
    fn non_literal_sql_is_flagged_not_silently_dropped() {
        // f-string sql= can't be resolved statically (finding #6).
        let src = r#"
import trace_weaver as tw
TABLE = "x"
@tw.task(
    inputs=["s.d.p.a"],
    outputs=["s.d.p.b"],
    engine="sql",
    sql=f"INSERT INTO {TABLE} SELECT 1",
)
def t():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(
            codes.contains(&"W_NON_LITERAL"),
            "expected W_NON_LITERAL, got {codes:?}"
        );
    }

    #[test]
    fn multi_input_does_not_cross_contaminate() {
        // Two inputs, each contributing its own column; the column_map entry for
        // a column from input A must NOT be attached to the B->C edge (finding #4).
        let src = r#"
import trace_weaver as tw
@tw.task(
    inputs=["s.d.p.a", "s.d.p.b"],
    outputs=["s.d.p.c"],
    engine="python",
    column_map=[
        (["a.x"], "cx", "copy from a"),
        (["b.y"], "cy", "copy from b"),
    ],
)
def t():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        // Two edges: a->c and b->c.
        assert_eq!(doc.edges.len(), 2);
        let ac = doc.edges.iter().find(|e| e.from == "s.d.p.a").unwrap();
        let bc = doc.edges.iter().find(|e| e.from == "s.d.p.b").unwrap();
        // a->c carries only the 'cx' mapping; b->c only 'cy'.
        assert_eq!(ac.column_lineage.len(), 1);
        assert_eq!(ac.column_lineage[0].to_column.column, "cx");
        assert_eq!(bc.column_lineage.len(), 1);
        assert_eq!(bc.column_lineage[0].to_column.column, "cy");
    }

    #[test]
    fn short_fqn_is_warned_via_scan_path() {
        // A non-4-part dataset name with no configure() to expand it is accepted
        // but flagged W_BAD_FQN (it won't export to OpenMetadata). Positive lock
        // for probe p9 — complements configure_expands_... which asserts ABSENCE.
        let src = r#"
import trace_weaver as tw
@tw.task(inputs=["landing_sales"], outputs=["bronze.sales"], engine="sql",
         sql="INSERT INTO bronze_sales (a) SELECT x FROM landing_sales")
def f():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(
            codes.contains(&"W_BAD_FQN"),
            "expected W_BAD_FQN, got {codes:?}"
        );
    }

    /// Write `src` to a fresh temp dir and run the full document pipeline
    /// (`scan_path`), so document-level post-passes (W_NO_COLUMN_LINEAGE, schema
    /// back-fill, code inference) run — they do NOT run under bare `scan_source`.
    fn scan_one(dir_name: &str, src: &str) -> WeaveDocument {
        use std::io::Write;
        let dir = std::env::temp_dir().join(dir_name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut f = std::fs::File::create(dir.join("dag.py")).unwrap();
        write!(f, "{src}").unwrap();
        let doc = scan_path(&dir, &opts()).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        doc
    }

    #[test]
    fn bare_multi_input_column_map_binds_to_first_input_only() {
        // With >1 input, BARE (unqualified) column_map sources bind to the FIRST
        // input only; the other input edge gets no column lineage and is warned.
        // Probe p3 — contrast multi_input_does_not_cross_contaminate (qualified).
        let doc = scan_one(
            "trace_weaver_scan_test_p3",
            r#"
import trace_weaver as tw
@tw.task(
    inputs=["s.d.p.bronze", "s.d.p.fx"],
    outputs=["s.d.p.silver"],
    engine="pandas",
    column_map=[
        (["amount", "rate"], "amount_usd", "amount * rate"),
        (["event_id"], "event_id", "direct copy"),
    ],
)
def t():
    pass
"#,
        );
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains(&"W_NO_COLUMN_LINEAGE"), "{codes:?}");
        // Second input (fx) gets no column lineage; first input (bronze) carries it.
        let fx = doc.edges.iter().find(|e| e.from == "s.d.p.fx").unwrap();
        assert!(
            fx.column_lineage.is_empty(),
            "fx edge should carry no columns"
        );
        let bronze = doc.edges.iter().find(|e| e.from == "s.d.p.bronze").unwrap();
        assert!(
            !bronze.column_lineage.is_empty(),
            "bronze edge should carry columns"
        );
    }

    #[test]
    fn copy_shortcut_declares_identity_via_scan_path() {
        // copy=[...] produces DECLARED identity column lineage, so a pandas task of
        // only same-name passthroughs is fully covered — and the document-level
        // post-pass therefore does NOT raise W_NO_COLUMN_LINEAGE.
        let doc = scan_one(
            "trace_weaver_scan_test_copy",
            r#"
import trace_weaver as tw
@tw.task(inputs=["s.d.p.a"], outputs=["s.d.p.b"], engine="pandas",
         copy=["event_id", "amount"])
def t():
    pass
"#,
        );
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(
            !codes.contains(&"W_NO_COLUMN_LINEAGE"),
            "copy should declare lineage so the no-column-lineage warning must not fire: {codes:?}"
        );
        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        assert_eq!(edge.column_lineage.len(), 2);
        for ce in &edge.column_lineage {
            // same-name identity, declared (not inferred-from-code).
            assert_eq!(ce.function.as_deref(), Some("direct copy"));
            assert_eq!(ce.from_columns.len(), 1);
            assert_eq!(ce.from_columns[0].column, ce.to_column.column);
            assert!(
                !ce.origin.is_inferred(),
                "copy edges must be DECLARED, got {:?}",
                ce.origin
            );
        }
    }

    #[test]
    fn no_column_lineage_edge_is_warned_via_scan_path() {
        use std::io::Write;
        let dir = std::env::temp_dir().join("trace_weaver_scan_test_nocl");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("dag.py");
        let mut f = std::fs::File::create(&file).unwrap();
        write!(
            f,
            r#"
import trace_weaver as tw
@tw.task(inputs=["s.d.p.a"], outputs=["s.d.p.b"], engine="pandas")
def t():
    pass
"#
        )
        .unwrap();
        let doc = scan_path(&dir, &opts()).unwrap();
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains(&"W_NO_COLUMN_LINEAGE"), "{codes:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn code_inference_tier_is_live_via_scan_path() {
        // Regression for finding #3 (the inferred_code tier was structurally
        // dead). Across tasks, schemas are back-filled from observed columns,
        // then a same-named unmapped column is filled with an inferred_code
        // identity edge. Here A.x and B.x both become "observed" (A.x as a
        // target of z->A, B.x as a source of B->C), so the A->B edge — which
        // only declared `k` — gets x filled by code inference.
        use std::io::Write;
        use trace_weaver_core::OriginSource;
        let dir = std::env::temp_dir().join("trace_weaver_scan_test_codeinfer");
        std::fs::create_dir_all(&dir).unwrap();
        let mut f = std::fs::File::create(dir.join("dag.py")).unwrap();
        write!(
            f,
            r#"
import trace_weaver as tw
@tw.task(inputs=["s.d.p.z"], outputs=["s.d.p.a"], engine="pandas", column_map=[(["x"], "x", "copy")])
def t0(): ...
@tw.task(inputs=["s.d.p.a"], outputs=["s.d.p.b"], engine="pandas", column_map=[(["k"], "k", "copy")])
def t1(): ...
@tw.task(inputs=["s.d.p.b"], outputs=["s.d.p.c"], engine="pandas", column_map=[(["x"], "y", "copy")])
def t2(): ...
"#
        )
        .unwrap();
        let doc = scan_path(&dir, &opts()).unwrap();
        let ab = doc
            .edges
            .iter()
            .find(|e| e.from == "s.d.p.a" && e.to == "s.d.p.b")
            .unwrap();
        let inferred_x = ab
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "x" && c.origin.source == OriginSource::InferredCode);
        assert!(
            inferred_x.is_some(),
            "expected an inferred_code identity edge for A.x -> B.x; got {:?}",
            ab.column_lineage
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discovered_tasks_mark_structural_origin_inferred() {
        // A plain Airflow DAG (no @tw): a SQL operator + a pandas PythonOperator.
        // Its jobs, edges AND datasets — not just its columns — must be tagged
        // INFERRED, since the whole task was recovered by static analysis rather
        // than hand-declared. Regression for the handoff bug where edge/job/dataset
        // origin stayed "declared" even for decorator-free discovery.
        let src = r#"
from airflow import DAG
BRONZE_SQL = "INSERT INTO bronze_sales (event_id) SELECT raw_id FROM landing_sales"
def build_silver():
    bronze = pd.read_sql("SELECT * FROM bronze_sales", con=E)
    silver = pd.DataFrame()
    silver["amount_usd"] = bronze["amount"] * 1.08
    silver.to_sql("silver_sales", con=E)
with DAG("medallion") as dag:
    bronze = PostgresOperator(task_id="bronze", sql=BRONZE_SQL)
    silver = PythonOperator(task_id="silver", python_callable=build_silver)
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();

        // Jobs: SQL-discovered -> inferred_sql; pandas-discovered -> inferred_code.
        let bronze_job = doc.jobs.iter().find(|j| j.name == "bronze").unwrap();
        assert_eq!(bronze_job.origin.source, OriginSource::InferredSql);
        let silver_job = doc.jobs.iter().find(|j| j.name == "silver").unwrap();
        assert_eq!(silver_job.origin.source, OriginSource::InferredCode);

        // Every edge inherits its discovering task's inferred origin (none declared).
        assert!(!doc.edges.is_empty());
        assert!(
            doc.edges.iter().all(|e| e.origin.is_inferred()),
            "discovered edges must be inferred: {:?}",
            doc.edges
                .iter()
                .map(|e| (e.from.clone(), e.to.clone(), e.origin.source))
                .collect::<Vec<_>>()
        );

        // Datasets discovered from the operators are inferred, never stuck declared.
        assert!(
            doc.datasets.iter().all(|d| d.origin.is_inferred()),
            "discovered datasets must be inferred: {:?}",
            doc.datasets
                .iter()
                .map(|d| (d.name.clone(), d.origin.source))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn declared_task_keeps_declared_structural_origin() {
        // Contrast: a hand-authored @tw task keeps DECLARED job/edge/dataset origin
        // (only undeclared columns are inferred). Locks the discovered-vs-declared
        // split so the fix above never bleeds into authored tasks.
        let src = r#"
import trace_weaver as tw
@tw.task(
    inputs=["s.d.p.a"],
    outputs=["s.d.p.b"],
    engine="sql",
    sql="INSERT INTO b (x) SELECT x FROM a",
)
def t():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        assert!(
            doc.jobs.iter().all(|j| !j.origin.is_inferred()),
            "job declared"
        );
        assert!(
            doc.edges.iter().all(|e| !e.origin.is_inferred()),
            "edge declared"
        );
        assert!(
            doc.datasets.iter().all(|d| !d.origin.is_inferred()),
            "datasets declared"
        );
        // The undeclared column, however, IS inferred from SQL — edge.has_inferred()
        // must still be true so exporters tag the column.
        assert!(doc.edges.iter().all(|e| e.has_inferred()));
    }

    #[test]
    fn opaque_note_suppressed_for_declared_column() {
        // A column the engineer DECLARED must not raise W_OPAQUE_COLUMN even when
        // its body is untraceable (here a named-callable apply): the declaration
        // already covers it, so the warning would be noise.
        let src = r#"
import trace_weaver as tw
@tw.task(inputs=["s.d.p.a"], outputs=["s.d.p.b"], engine="pandas",
         column_map=[(["x"], "y", "row udf")])
def t():
    src = pd.read_sql("SELECT * FROM a", con=E)
    out = pd.DataFrame()
    out["y"] = src.apply(weird, axis=1)
    out.to_sql("b", con=E)
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("t.py", src, &opts(), &mut doc).unwrap();
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(
            !codes.contains(&"W_OPAQUE_COLUMN"),
            "declared column 'y' must not raise W_OPAQUE_COLUMN: {codes:?}"
        );
    }

    #[test]
    fn lineage_literal_datasets_are_declared_high_confidence() {
        // A @lineage task with literal URIs: datasets + edge are DECLARED (high),
        // and the OM-FQN / missing-column advisories do NOT fire (URIs, not FQNs).
        let src = r#"
from traceweaver import lineage
@lineage(
    inputs=["s3://raw-bucket/sales/{date}.parquet"],
    outputs=["iceberg://warehouse.sales.bronze"],
    description="daily ingest",
)
def build_bronze():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("dag.py", src, &opts(), &mut doc).unwrap();

        assert_eq!(doc.jobs.len(), 1);
        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        assert_eq!(edge.from, "s3://raw-bucket/sales/{date}.parquet");
        assert_eq!(edge.to, "iceberg://warehouse.sales.bronze");
        assert!(
            !edge.origin.is_inferred(),
            "literal @lineage edge is declared"
        );
        assert!(doc.datasets.iter().all(|d| !d.origin.is_inferred()));
        // No hygiene noise for a URI-based dataset-level declaration.
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(!codes.contains(&"W_BAD_FQN"), "{codes:?}");
        assert!(!codes.contains(&"E_MISSING_IO"), "{codes:?}");
    }

    #[test]
    fn lineage_non_literal_dataset_is_medium_confidence() {
        // A non-literal input (f-string) => the input dataset and the edge it
        // touches are INFERRED (medium), while the literal output stays declared.
        let src = r#"
from traceweaver import lineage
DAY = "2024-01-01"
@lineage(
    inputs=[f"s3://raw/{DAY}.parquet"],
    outputs=["iceberg://w.db.bronze"],
)
def build():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("dag.py", src, &opts(), &mut doc).unwrap();
        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        assert!(
            edge.origin.is_inferred(),
            "an edge touching a non-literal endpoint is medium/inferred"
        );
        assert_eq!(
            edge.origin.confidence,
            Some(LINEAGE_MEDIUM_CONFIDENCE),
            "non-literal @lineage confidence is medium"
        );
        // The literal output dataset is still declared/high.
        let out = doc.dataset("iceberg://w.db.bronze").unwrap();
        assert!(!out.origin.is_inferred());
    }

    #[test]
    fn lineage_dataset_level_edge_does_not_warn_no_column_lineage() {
        // @lineage is intentionally dataset-level, so the document post-pass must
        // NOT nag it to "declare a column_map".
        let doc = scan_one(
            "trace_weaver_scan_test_lineage_nocl",
            r#"
from traceweaver import lineage
@lineage(inputs=["s3://b/in"], outputs=["s3://b/out"])
def f():
    pass
"#,
        );
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(
            !codes.contains(&"W_NO_COLUMN_LINEAGE"),
            "dataset-level @lineage must not raise W_NO_COLUMN_LINEAGE: {codes:?}"
        );
        assert_eq!(doc.edges.len(), 1);
    }

    #[test]
    fn declared_lineage_self_loop_emits_a_self_edge() {
        // A declared @lineage whose input == output ("read prefix X and delete
        // orphans in X") is a legitimate self-loop: it must yield exactly one
        // declared self-edge X -> X, both via scan_source (scan_decl builds it)
        // and after the full scan_path pipeline (derive_edges must not dup it).
        let src = r#"
from traceweaver import lineage
@lineage(inputs=["s3://bucket/prefix"], outputs=["s3://bucket/prefix"],
         description="delete orphaned objects in place")
def dedupe_prefix():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("dag.py", src, &opts(), &mut doc).unwrap();
        assert_eq!(doc.edges.len(), 1, "self-edge built during scan_decl");
        let e = &doc.edges[0];
        assert_eq!(e.from, "s3://bucket/prefix");
        assert_eq!(e.to, "s3://bucket/prefix");
        assert!(!e.origin.is_inferred(), "declared self-edge is declared");

        // Full pipeline (scan_path runs derive_edges_from_jobs): still exactly one.
        let doc2 = scan_one("trace_weaver_scan_test_selfloop", src);
        assert_eq!(
            doc2.edges.len(),
            1,
            "derive_edges must not duplicate the declared self-edge"
        );
        assert_eq!(doc2.edges[0].from, doc2.edges[0].to);
    }

    #[test]
    fn nonliteral_lineage_self_loop_is_skipped_as_inferred() {
        // A @lineage self-loop where the (identical) endpoint is a NON-literal
        // (f-string) is medium/inferred, so — like any inferred self-loop — it is
        // skipped rather than emitted, avoiding noise.
        let src = r#"
from traceweaver import lineage
DAY = "2024-01-01"
@lineage(inputs=[f"s3://bucket/{DAY}"], outputs=[f"s3://bucket/{DAY}"])
def f():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("dag.py", src, &opts(), &mut doc).unwrap();
        assert!(
            doc.edges.is_empty(),
            "inferred (non-literal) self-loop must be skipped: {:?}",
            doc.edges
                .iter()
                .map(|e| (e.from.clone(), e.to.clone()))
                .collect::<Vec<_>>()
        );
        // The job still exists (it is annotated), just with no self-edge.
        assert_eq!(doc.jobs.len(), 1);
    }

    #[test]
    fn cross_file_constant_resolves_to_declared_high_confidence() {
        // The real target layout: URIs centralized in config/datasets.py and
        // consumed by services/x.py via `from config.datasets import ...`. The
        // constant must resolve to its literal so the edge + datasets stay
        // DECLARED / high confidence — NOT drop to inferred/medium.
        use std::io::Write;
        let dir = std::env::temp_dir().join("trace_weaver_scan_test_xfile");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("config")).unwrap();
        std::fs::create_dir_all(dir.join("services")).unwrap();
        let mut cfg = std::fs::File::create(dir.join("config/datasets.py")).unwrap();
        write!(
            cfg,
            r#"
RAW_SALES = "s3://acme-raw/sales/events.parquet"
BRONZE_SALES = "iceberg://warehouse.sales.bronze"
"#
        )
        .unwrap();
        let mut svc = std::fs::File::create(dir.join("services/ingest.py")).unwrap();
        write!(
            svc,
            r#"
from trace_weaver import lineage
from config.datasets import RAW_SALES, BRONZE_SALES

@lineage(inputs=[RAW_SALES], outputs=[BRONZE_SALES])
def ingest_sales():
    ...
"#
        )
        .unwrap();
        let doc = scan_path(&dir, &opts()).unwrap();
        let _ = std::fs::remove_dir_all(&dir);

        // The literals from the OTHER file became the dataset names.
        assert!(doc.dataset("s3://acme-raw/sales/events.parquet").is_some());
        assert!(doc.dataset("iceberg://warehouse.sales.bronze").is_some());
        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        assert_eq!(edge.from, "s3://acme-raw/sales/events.parquet");
        assert_eq!(edge.to, "iceberg://warehouse.sales.bronze");
        assert!(
            !edge.origin.is_inferred(),
            "cross-file constant edge must be DECLARED, got {:?}",
            edge.origin
        );
        assert!(
            doc.datasets.iter().all(|d| !d.origin.is_inferred()),
            "cross-file constant datasets must be declared/high"
        );
        // No medium/non-literal fallback fired.
        let codes: Vec<&str> = doc.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(!codes.contains(&"W_NON_LITERAL"), "{codes:?}");
    }

    #[test]
    fn cross_file_self_loop_from_constant_emits_declared_self_edge() {
        // A declared @lineage self-loop whose single endpoint is a cross-file
        // constant resolves to one DECLARED self-edge (regression: constants must
        // behave exactly like inline literals, including for self-loops).
        use std::io::Write;
        let dir = std::env::temp_dir().join("trace_weaver_scan_test_xfile_selfloop");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = std::fs::File::create(dir.join("datasets.py")).unwrap();
        writeln!(cfg, "PREFIX = \"s3://bucket/prefix\"").unwrap();
        let mut svc = std::fs::File::create(dir.join("dag.py")).unwrap();
        write!(
            svc,
            r#"
from trace_weaver import lineage
from datasets import PREFIX

@lineage(inputs=[PREFIX], outputs=[PREFIX], description="delete orphans in place")
def dedupe():
    ...
"#
        )
        .unwrap();
        let doc = scan_path(&dir, &opts()).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(doc.edges.len(), 1, "exactly one self-edge");
        let e = &doc.edges[0];
        assert_eq!(e.from, "s3://bucket/prefix");
        assert_eq!(e.to, "s3://bucket/prefix");
        assert!(!e.origin.is_inferred(), "declared self-edge from constant");
    }

    #[test]
    fn module_path_of_derives_dotted_paths() {
        use std::path::Path;
        let root = Path::new("/repo/dags");
        assert_eq!(
            module_path_of(root, Path::new("/repo/dags/config/datasets.py")),
            "config.datasets"
        );
        assert_eq!(module_path_of(root, Path::new("/repo/dags/dag.py")), "dag");
        assert_eq!(
            module_path_of(root, Path::new("/repo/dags/pkg/__init__.py")),
            "pkg"
        );
    }

    #[test]
    fn dii_transform_run_yields_column_mappings_without_a_task() {
        // The exact DII shape: an undecorated, un-wired top-level `run(spark, …)`
        // with expected_columns validation, a temp view, and an f-string
        // spark.sql INSERT whose SELECT/CAST list is fully static. Pass C must
        // recover column mappings — but emit NO job (so the gate task denominator
        // is untouched).
        let src = r#"
from pyspark.sql import functions as F
def run(spark, src_path, database, table, first_run, job_id, load_date_str, target_catalogs="both"):
    df = spark.read.format("parquet").load(src_path)
    expected_columns = ["HOSPCODE", "PID", "SEQ", "DATE_SERV", "WEIGHT"]
    actual_columns = df.columns
    if len(actual_columns) != len(expected_columns):
        raise ValueError("Column count mismatch.")
    if set(actual_columns) != set(expected_columns):
        raise ValueError("Column mismatch.")
    full_table_name = f"glue_cat.{database}.{table}"
    df.createOrReplaceTempView("raw_staging")
    spark.sql(f"""
        INSERT INTO {full_table_name}
        SELECT
            CAST(`HOSPCODE` AS STRING),
            CAST(`PID` AS STRING),
            CAST(`SEQ` AS INT),
            CAST(`DATE_SERV` AS STRING),
            `WEIGHT`
        FROM raw_staging
    """)
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("zone_anc_mom.py", src, &opts(), &mut doc).unwrap();

        // COLUMN discovery, not TASK discovery: no job enters the denominator.
        assert!(
            doc.jobs.is_empty(),
            "Pass C must not create a job: {:?}",
            doc.jobs.iter().map(|j| &j.id).collect::<Vec<_>>()
        );
        // But it DID recover per-column lineage.
        let total_cols: usize = doc.edges.iter().map(|e| e.column_lineage.len()).sum();
        assert!(total_cols > 0, "expected column mappings, got 0");
        // The staging INSERT maps raw_staging.<col> -> <target>.<col>, CAST=xform.
        let edge = doc
            .edges
            .iter()
            .find(|e| e.from == "raw_staging")
            .expect("raw_staging edge");
        assert!(edge.origin.is_inferred(), "discovery edge is inferred");
        let h = edge
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "HOSPCODE")
            .expect("HOSPCODE mapping");
        assert_eq!(h.from_columns[0].column, "HOSPCODE");
        assert_eq!(h.from_columns[0].dataset, "raw_staging");
        assert_eq!(h.transform_type, TransformType::Transformation);
        // A bare (uncast) column is an identity passthrough.
        let w = edge
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "WEIGHT")
            .expect("WEIGHT mapping");
        assert_eq!(w.transform_type, TransformType::Identity);
    }

    #[test]
    fn undecorated_helper_without_dataframe_ops_is_not_discovered() {
        // Pass C only fires when the body actually yields column lineage. A plain
        // helper with no DataFrame column flow contributes nothing (no job, no
        // edge, no dataset) — so the scanner stays quiet on ordinary code.
        let src = r#"
def helper(x, y):
    z = x + y
    return z * 2
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("helper.py", src, &opts(), &mut doc).unwrap();
        assert!(doc.jobs.is_empty());
        assert!(doc.edges.is_empty());
        assert!(doc.datasets.is_empty());
    }

    #[test]
    fn dataflow_under_lineage_attaches_inferred_columns_but_keeps_declared_edge() {
        // Regression for the un-suppress fix: a @lineage function whose body has a
        // literal-dict .rename() must keep its DECLARED / HIGH-confidence dataset
        // edge AND gain the inferred column mappings the analyzer finds.
        use trace_weaver_core::OriginSource;
        let src = r#"
from traceweaver import lineage
import pandas as pd
@lineage(
    inputs=["postgresql://conn/aoc.raw_mule"],
    outputs=["postgresql://conn/aoc.temp_mule_trend"],
)
def load_data_from_parquet_to_temp_table():
    df = pd.read_sql("SELECT * FROM raw_mule", con=E)
    df = df.rename(columns={"case_code": "case_id", "amount": "amount_thb"})
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("etl.py", src, &opts(), &mut doc).unwrap();

        // Exactly one @lineage task (the rename did NOT become a second task).
        assert_eq!(doc.jobs.len(), 1);
        assert!(
            !doc.jobs[0].origin.is_inferred(),
            "@lineage job is declared"
        );
        assert_eq!(doc.edges.len(), 1);
        let edge = &doc.edges[0];
        // Declared / HIGH confidence dataset edge preserved.
        assert!(
            !edge.origin.is_inferred(),
            "declared @lineage edge must stay declared/HIGH"
        );
        // …with inferred column mappings merged beneath it.
        let case_id = edge
            .column_lineage
            .iter()
            .find(|c| c.to_column.column == "case_id")
            .expect("inferred case_id mapping should attach under @lineage");
        assert_eq!(case_id.origin.source, OriginSource::InferredCode);
        assert_eq!(case_id.from_columns[0].column, "case_code");
        assert_eq!(
            case_id.from_columns[0].dataset,
            "postgresql://conn/aoc.raw_mule"
        );
        assert_eq!(
            case_id.to_column.dataset,
            "postgresql://conn/aoc.temp_mule_trend"
        );
    }

    #[test]
    fn bare_lineage_yields_a_job_with_no_edges_and_no_error() {
        // Bare @lineage: the function is a task with no declared datasets. It must
        // not error (E_MISSING_IO is suppressed) and contributes to task counts
        // without any lineage edges — the exact "task without lineage" case the
        // gate coverage metric measures.
        let src = r#"
from traceweaver import lineage
@lineage
def f():
    pass
"#;
        let mut doc = WeaveDocument::new("ns", "test");
        scan_source("dag.py", src, &opts(), &mut doc).unwrap();
        assert_eq!(doc.jobs.len(), 1);
        assert!(doc.edges.is_empty());
        assert!(!trace_weaver_core::has_errors(&doc.diagnostics));
    }
}
