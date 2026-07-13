//! `trace-weaver` — the command-line data-lineage compiler.
//!
//! ```text
//! trace-weaver scan <path> [-o out.weave.json] [--namespace NS] [--no-sql-infer] [--no-code-infer] [--strict]
//! trace-weaver validate <doc.weave.json> [--strict]
//! trace-weaver export --to <openmetadata|openlineage|dot> <doc.weave.json> [--dry-run] [-o out] ...
//! trace-weaver graph <doc.weave.json> [-o out.dot]
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};

use trace_weaver_core::{has_errors, validate, DiagLevel, Job, WeaveDocument};
use trace_weaver_export::{export, ExportConfig, Target};
use trace_weaver_scan::{scan_path, ScanOptions};

#[derive(Parser)]
#[command(
    name = "trace-weaver",
    version,
    about = "Universal data-lineage compiler: DAG code → weave → catalogue",
    long_about = "Universal data-lineage compiler: scan annotated DAG code into the weave \
universal format, validate it, and export it to OpenMetadata / OpenLineage / DOT.\n\n\
AUTH: the OpenMetadata JWT is read from --om-token-file, then --om-token, then the \
OPENMETADATA_BOT_TOKEN environment variable (the env var / file is preferred — a raw \
--om-token is visible in your shell history).\n\n\
EXIT CODES: 0 success; 1 error (bad args, I/O, scan/export failure, or --strict / \
--fail-on-partial gate tripped)."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan annotated Python DAG code into a weave document.
    Scan(ScanArgs),
    /// Validate an existing weave document.
    Validate(ValidateArgs),
    /// Export a weave document to a downstream catalogue.
    Export(ExportArgs),
    /// Shortcut for `export --to dot`.
    Graph(GraphArgs),
    /// Gate CI on lineage coverage/confidence thresholds computed from a scan.
    Gate(GateArgs),
}

#[derive(Args)]
struct ScanArgs {
    /// File or directory of DAG code to scan.
    path: PathBuf,
    /// Write the weave document here (default: stdout).
    #[arg(short, long)]
    out: Option<PathBuf>,
    /// Default namespace for datasets without their own.
    #[arg(long, default_value = "default")]
    namespace: String,
    /// Default OpenMetadata FQN parts for raw DAGs without `tw.configure(...)` —
    /// they expand bare table names to `service.database.schema.table`.
    #[arg(long)]
    service: Option<String>,
    #[arg(long)]
    database: Option<String>,
    #[arg(long)]
    schema: Option<String>,
    /// Disable column-lineage inference from SQL.
    #[arg(long)]
    no_sql_infer: bool,
    /// Disable best-effort code inference.
    #[arg(long)]
    no_code_infer: bool,
    /// Exit non-zero if any error-level diagnostics are produced.
    #[arg(long)]
    strict: bool,
}

#[derive(Args)]
struct ValidateArgs {
    /// The `.weave.json` document to validate.
    doc: PathBuf,
    #[arg(long)]
    strict: bool,
}

#[derive(Args)]
struct ExportArgs {
    /// Target catalogue: openmetadata | openlineage | dot.
    #[arg(long = "to")]
    to: String,
    /// The `.weave.json` document to export.
    doc: PathBuf,
    /// Build artifacts and log actions, but perform no network I/O.
    #[arg(long)]
    dry_run: bool,
    /// Write the export artifact here (default: stdout).
    #[arg(short, long)]
    out: Option<PathBuf>,
    /// OpenMetadata API host, e.g. http://openmetadata-server:8585/api.
    #[arg(long)]
    om_host: Option<String>,
    /// OpenMetadata ingestion-bot JWT. Prefer --om-token-file or the
    /// OPENMETADATA_BOT_TOKEN env var; a token passed here is visible in your
    /// shell history and process list.
    #[arg(long)]
    om_token: Option<String>,
    /// Read the OpenMetadata JWT from this file (recommended over --om-token).
    #[arg(long, value_name = "PATH")]
    om_token_file: Option<PathBuf>,
    /// Override the service segment of every dataset FQN.
    #[arg(long)]
    om_service: Option<String>,
    /// OpenLineage producer URI.
    #[arg(long)]
    ol_producer: Option<String>,
    /// Per-request HTTP timeout in seconds (0 = no explicit timeout).
    #[arg(long, default_value_t = 30)]
    timeout: u64,
    /// Automatic retries for transient HTTP failures (timeouts, 429, 5xx).
    #[arg(long, default_value_t = 2)]
    retries: u32,
    /// Exit non-zero if any lineage edge failed to push (for CI gating).
    #[arg(long)]
    fail_on_partial: bool,
}

#[derive(Args)]
struct GraphArgs {
    doc: PathBuf,
    #[arg(short, long)]
    out: Option<PathBuf>,
}

/// Output format for the `gate` report.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum GateFormat {
    /// A human-readable metric table with PASS/FAIL lines.
    Text,
    /// A machine-readable JSON object (includes the per-DAG breakdown).
    Json,
}

#[derive(Args)]
struct GateArgs {
    /// Directory (or file) of DAG code to scan and gate.
    #[arg(long)]
    repo_path: PathBuf,
    /// Scan the repo as of this git ref instead of the working tree.
    #[arg(long)]
    git_ref: Option<String>,
    /// Minimum acceptable fraction of tasks that carry lineage (0.0–1.0).
    /// Falls back to $TRACEWEAVER_MIN_TASK_COVERAGE, then 0.0. Flag wins.
    #[arg(long)]
    min_task_coverage: Option<f64>,
    /// Minimum acceptable fraction of edges that are high-confidence (declared).
    /// Falls back to $TRACEWEAVER_MIN_HIGH_CONFIDENCE, then 0.0. Flag wins.
    #[arg(long)]
    min_high_confidence: Option<f64>,
    /// Minimum acceptable fraction of tasks that carry an explicit trace-weaver
    /// decorator (annotation_coverage). Falls back to
    /// $TRACEWEAVER_MIN_ANNOTATION_COVERAGE, then 0.0. Flag wins.
    #[arg(long)]
    min_annotation_coverage: Option<f64>,
    /// Report format.
    #[arg(long, value_enum, default_value_t = GateFormat::Text)]
    format: GateFormat,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Scan(a) => cmd_scan(a),
        Command::Validate(a) => cmd_validate(a),
        Command::Export(a) => cmd_export(a),
        Command::Graph(a) => cmd_graph(a),
        // `gate` owns its exit codes (0 pass / 1 threshold fail / 2 usage error),
        // so it bypasses the anyhow `Result` path and exits explicitly.
        Command::Gate(a) => std::process::exit(cmd_gate(a)),
    }
}

fn cmd_scan(a: ScanArgs) -> Result<()> {
    let opts = ScanOptions {
        namespace: a.namespace,
        enable_sql_inference: !a.no_sql_infer,
        enable_code_inference: !a.no_code_infer,
        service: a.service,
        database: a.database,
        schema: a.schema,
        ..Default::default()
    };
    let mut doc = scan_path(&a.path, &opts).context("scan failed")?;
    doc.generated_at = Some(now_rfc3339());

    print_diagnostics(&doc);
    let json = trace_weaver_core::to_json_pretty(&doc)?;
    write_out(a.out.as_ref(), &json)?;

    if a.strict && has_errors(&doc.diagnostics) {
        anyhow::bail!("scan produced error-level diagnostics (--strict)");
    }
    Ok(())
}

fn cmd_validate(a: ValidateArgs) -> Result<()> {
    let doc = load_doc(&a.doc)?;
    let diags = validate(&doc);
    let merged: Vec<_> = doc
        .diagnostics
        .iter()
        .chain(diags.iter())
        .cloned()
        .collect();
    for d in &merged {
        eprintln!("[{:?}] {}: {}", d.level, d.code, d.message);
    }
    let errors = merged
        .iter()
        .filter(|d| d.level == DiagLevel::Error)
        .count();
    let warns = merged.iter().filter(|d| d.level == DiagLevel::Warn).count();
    eprintln!("validation: {errors} error(s), {warns} warning(s)");
    if a.strict && errors > 0 {
        anyhow::bail!("validation failed (--strict)");
    }
    Ok(())
}

fn cmd_export(a: ExportArgs) -> Result<()> {
    let target = Target::parse(&a.to)?;
    let doc = load_doc(&a.doc)?;
    let cfg = ExportConfig {
        dry_run: a.dry_run,
        om_host: a.om_host,
        om_token: resolve_om_token(a.om_token.as_deref(), a.om_token_file.as_deref())?,
        om_service_override: a.om_service,
        ol_producer: a.ol_producer,
        timeout_secs: a.timeout,
        retries: a.retries,
    };
    let report = export(target, &doc, &cfg)?;
    for line in &report.actions {
        eprintln!("{line}");
    }
    eprintln!(
        "export[{}]: {} edge(s) {}{}",
        report.target,
        report.sent,
        if cfg.dry_run {
            "(dry-run, nothing sent)"
        } else {
            "pushed"
        },
        if report.failed > 0 {
            format!(", {} failed", report.failed)
        } else {
            String::new()
        }
    );
    write_out(a.out.as_ref(), &report.artifact)?;

    if a.fail_on_partial && report.failed > 0 {
        anyhow::bail!(
            "{} edge(s) failed to push (--fail-on-partial)",
            report.failed
        );
    }
    Ok(())
}

/// Resolve the OpenMetadata token: `--om-token-file` > `--om-token` > the
/// `OPENMETADATA_BOT_TOKEN` env var. Warns when a raw token is passed on the CLI.
fn resolve_om_token(
    token: Option<&str>,
    token_file: Option<&std::path::Path>,
) -> Result<Option<String>> {
    if let Some(path) = token_file {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("reading token file {}", path.display()))?;
        return Ok(Some(raw.trim().to_string()));
    }
    if let Some(t) = token {
        eprintln!(
            "warning: --om-token exposes the JWT in your shell history and process list; \
             prefer --om-token-file or the OPENMETADATA_BOT_TOKEN env var."
        );
        return Ok(Some(t.to_string()));
    }
    Ok(std::env::var("OPENMETADATA_BOT_TOKEN").ok())
}

fn cmd_graph(a: GraphArgs) -> Result<()> {
    let doc = load_doc(&a.doc)?;
    let report = export(
        Target::Dot,
        &doc,
        &ExportConfig {
            dry_run: true,
            ..Default::default()
        },
    )?;
    write_out(a.out.as_ref(), &report.artifact)?;
    Ok(())
}

// ── gate ──

/// Lineage metrics for one DAG (or the whole document).
#[derive(Debug, Clone, PartialEq)]
struct DagMetrics {
    dag: String,
    tasks_total: usize,
    tasks_with_lineage: usize,
    task_coverage: f64,
    tasks_annotated: usize,
    annotation_coverage: f64,
    edges_total: usize,
    high_confidence_edges: usize,
    high_confidence_fraction: f64,
}

/// The full metric set a gate run evaluates: document totals + per-DAG breakdown.
#[derive(Debug, Clone, PartialEq)]
struct GateMetrics {
    tasks_total: usize,
    tasks_with_lineage: usize,
    task_coverage: f64,
    tasks_annotated: usize,
    annotation_coverage: f64,
    edges_total: usize,
    high_confidence_edges: usize,
    high_confidence_fraction: f64,
    per_dag: Vec<DagMetrics>,
}

/// `n / d`, guarded: returns `empty` when the denominator is zero.
fn frac(n: usize, d: usize, empty: f64) -> f64 {
    if d == 0 {
        empty
    } else {
        n as f64 / d as f64
    }
}

/// Compute lineage gate metrics from a scanned document.
///
/// * A task "has lineage" when at least one edge is attributed to its job
///   (`task_coverage` = data-flow completeness). A declared self-loop counts,
///   since it is now a real edge.
/// * A task is "annotated" when it was synthesized from an explicit trace-weaver
///   decorator (`@tw.task` / `@tw.sql` / `@lineage`, bare or called) — i.e. its
///   job carries a **declared** (not inferred) origin. `annotation_coverage`
///   = tasks_annotated / tasks_total measures **review** completeness (did a
///   human mark this task at all), independent of whether the annotation yielded
///   a full input→output edge.
///
///   `tasks_total` is the denominator for BOTH coverage metrics and counts
///   **every** job, including Pass-B tasks discovered decorator-free from raw
///   Airflow operators. Those raw-operator tasks *cannot* carry a decorator, so
///   they can never be "annotated" and intentionally hold `annotation_coverage`
///   below 1.0 — surfacing exactly the un-reviewed surface a human still owes an
///   annotation. (A bare `@lineage` marker, by contrast, IS annotated even
///   though it declares no datasets and so contributes no edge.)
/// * "high confidence" == a **declared** (not inferred) edge — literal `@lineage`
///   / `@tw` declarations are declared; SQL/code-inferred and non-literal
///   `@lineage` datasets are not.
/// * `task_coverage`/`annotation_coverage` are 0.0 for a document with no tasks;
///   `high_confidence_fraction` is 1.0 for a document with no edges (nothing
///   low-confidence to count).
fn compute_gate_metrics(doc: &WeaveDocument) -> GateMetrics {
    use std::collections::{BTreeMap, HashMap, HashSet};

    let jobs_with_edges: HashSet<&str> =
        doc.edges.iter().filter_map(|e| e.job.as_deref()).collect();
    let job_by_id: HashMap<&str, &Job> = doc.jobs.iter().map(|j| (j.id.as_str(), j)).collect();
    let dag_of = |j: &Job| j.dag.clone().unwrap_or_else(|| "default".to_string());

    // (tasks_total, tasks_with_lineage, tasks_annotated, edges_total, high_confidence_edges)
    let mut by_dag: BTreeMap<String, [usize; 5]> = BTreeMap::new();
    for j in &doc.jobs {
        let e = by_dag.entry(dag_of(j)).or_default();
        e[0] += 1;
        if jobs_with_edges.contains(j.id.as_str()) {
            e[1] += 1;
        }
        // A declared job origin means the task was authored via an explicit
        // decorator; an inferred origin means it was discovered decorator-free.
        if !j.origin.is_inferred() {
            e[2] += 1;
        }
    }
    for edge in &doc.edges {
        let dag = edge
            .job
            .as_deref()
            .and_then(|id| job_by_id.get(id))
            .map(|j| dag_of(j))
            .unwrap_or_else(|| "default".to_string());
        let e = by_dag.entry(dag).or_default();
        e[3] += 1;
        if !edge.origin.is_inferred() {
            e[4] += 1;
        }
    }

    let per_dag: Vec<DagMetrics> = by_dag
        .into_iter()
        .map(|(dag, c)| DagMetrics {
            dag,
            tasks_total: c[0],
            tasks_with_lineage: c[1],
            task_coverage: frac(c[1], c[0], 0.0),
            tasks_annotated: c[2],
            annotation_coverage: frac(c[2], c[0], 0.0),
            edges_total: c[3],
            high_confidence_edges: c[4],
            high_confidence_fraction: frac(c[4], c[3], 1.0),
        })
        .collect();

    let tasks_total = doc.jobs.len();
    let tasks_with_lineage = doc
        .jobs
        .iter()
        .filter(|j| jobs_with_edges.contains(j.id.as_str()))
        .count();
    let tasks_annotated = doc.jobs.iter().filter(|j| !j.origin.is_inferred()).count();
    let edges_total = doc.edges.len();
    let high_confidence_edges = doc.edges.iter().filter(|e| !e.origin.is_inferred()).count();

    GateMetrics {
        tasks_total,
        tasks_with_lineage,
        task_coverage: frac(tasks_with_lineage, tasks_total, 0.0),
        tasks_annotated,
        annotation_coverage: frac(tasks_annotated, tasks_total, 0.0),
        edges_total,
        high_confidence_edges,
        high_confidence_fraction: frac(high_confidence_edges, edges_total, 1.0),
        per_dag,
    }
}

/// Resolve a threshold: explicit `--flag` wins; otherwise the env var; otherwise
/// `0.0` (no gate). A present-but-unparseable env var is a usage error.
fn resolve_threshold(flag: Option<f64>, env: &str) -> Result<f64, String> {
    if let Some(v) = flag {
        return Ok(v);
    }
    match std::env::var(env) {
        Ok(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("environment variable {env}={s:?} is not a valid number")),
        Err(_) => Ok(0.0),
    }
}

/// Materialize `repo` at `git_ref` into a temp dir (via `git archive | tar`) and
/// return the extracted root. Any failure is a usage error (bad ref / no git).
fn materialize_git_ref(repo: &Path, git_ref: &str) -> Result<PathBuf, String> {
    let repo_str = repo
        .to_str()
        .ok_or_else(|| "repo path is not valid UTF-8".to_string())?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp =
        std::env::temp_dir().join(format!("trace-weaver-gate-{}-{stamp}", std::process::id()));
    let src = tmp.join("src");
    let tar = tmp.join("repo.tar");
    fs::create_dir_all(&src).map_err(|e| format!("creating temp dir: {e}"))?;

    let tar_str = tar.to_str().ok_or("temp path not UTF-8")?;
    let src_str = src.to_str().ok_or("temp path not UTF-8")?;

    let archive = std::process::Command::new("git")
        .args([
            "-C",
            repo_str,
            "archive",
            "--format=tar",
            "-o",
            tar_str,
            git_ref,
        ])
        .output()
        .map_err(|e| format!("running git: {e}"))?;
    if !archive.status.success() {
        let _ = fs::remove_dir_all(&tmp);
        return Err(format!(
            "git archive {git_ref} failed: {}",
            String::from_utf8_lossy(&archive.stderr).trim()
        ));
    }
    let untar = std::process::Command::new("tar")
        .args(["-xf", tar_str, "-C", src_str])
        .output()
        .map_err(|e| format!("running tar: {e}"))?;
    if !untar.status.success() {
        let _ = fs::remove_dir_all(&tmp);
        return Err(format!(
            "tar extract failed: {}",
            String::from_utf8_lossy(&untar.stderr).trim()
        ));
    }
    Ok(src)
}

fn cmd_gate(a: GateArgs) -> i32 {
    let min_cov = match resolve_threshold(a.min_task_coverage, "TRACEWEAVER_MIN_TASK_COVERAGE") {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let min_hc = match resolve_threshold(a.min_high_confidence, "TRACEWEAVER_MIN_HIGH_CONFIDENCE") {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let min_ann = match resolve_threshold(
        a.min_annotation_coverage,
        "TRACEWEAVER_MIN_ANNOTATION_COVERAGE",
    ) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };

    // Resolve the scan root: the working tree, or a temp checkout of --git-ref.
    let git_tmp: Option<PathBuf>;
    let root: PathBuf = match a.git_ref.as_deref() {
        Some(git_ref) => match materialize_git_ref(&a.repo_path, git_ref) {
            Ok(p) => {
                git_tmp = Some(p.clone());
                p
            }
            Err(e) => {
                eprintln!("error: {e}");
                return 2;
            }
        },
        None => {
            git_tmp = None;
            a.repo_path.clone()
        }
    };

    if !root.exists() {
        eprintln!("error: repo path does not exist: {}", root.display());
        cleanup_git_tmp(&git_tmp);
        return 2;
    }

    let opts = ScanOptions::default();
    let doc = match scan_path(&root, &opts) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: scan failed: {e}");
            cleanup_git_tmp(&git_tmp);
            return 2;
        }
    };
    cleanup_git_tmp(&git_tmp);

    let m = compute_gate_metrics(&doc);
    let cov_pass = m.task_coverage >= min_cov;
    let hc_pass = m.high_confidence_fraction >= min_hc;
    let ann_pass = m.annotation_coverage >= min_ann;
    let passed = cov_pass && hc_pass && ann_pass;

    let t = GateThresholds {
        min_cov,
        min_hc,
        min_ann,
    };
    let c = GateChecks {
        cov_pass,
        hc_pass,
        ann_pass,
        passed,
    };
    match a.format {
        GateFormat::Json => print_gate_json(&m, &t, &c),
        GateFormat::Text => print_gate_text(&m, &t, &c),
    }

    if passed {
        0
    } else {
        1
    }
}

fn cleanup_git_tmp(git_tmp: &Option<PathBuf>) {
    if let Some(t) = git_tmp {
        // Remove the whole temp root (parent of the extracted `src`).
        if let Some(parent) = t.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }
}

/// Resolved gate thresholds (`--min-*` flag / env / default), grouped so the
/// print helpers take one argument instead of a long positional list.
struct GateThresholds {
    min_cov: f64,
    min_hc: f64,
    min_ann: f64,
}

/// Per-metric PASS/FAIL outcomes plus the overall verdict.
struct GateChecks {
    cov_pass: bool,
    hc_pass: bool,
    ann_pass: bool,
    passed: bool,
}

fn print_gate_text(m: &GateMetrics, t: &GateThresholds, c: &GateChecks) {
    let mark = |ok: bool| if ok { "PASS" } else { "FAIL" };
    println!("trace-weaver lineage gate");
    println!("  tasks_total               {}", m.tasks_total);
    println!("  tasks_with_lineage        {}", m.tasks_with_lineage);
    println!("  task_coverage             {:.3}", m.task_coverage);
    println!("  tasks_annotated           {}", m.tasks_annotated);
    println!("  annotation_coverage       {:.3}", m.annotation_coverage);
    println!("  edges_total               {}", m.edges_total);
    println!("  high_confidence_edges     {}", m.high_confidence_edges);
    println!(
        "  high_confidence_fraction  {:.3}",
        m.high_confidence_fraction
    );
    if !m.per_dag.is_empty() {
        println!("  per-DAG:");
        for d in &m.per_dag {
            println!(
                "    {:<24} coverage {:.3} ({}/{})  annotated {:.3} ({}/{})  high-conf {:.3} ({}/{})",
                d.dag,
                d.task_coverage,
                d.tasks_with_lineage,
                d.tasks_total,
                d.annotation_coverage,
                d.tasks_annotated,
                d.tasks_total,
                d.high_confidence_fraction,
                d.high_confidence_edges,
                d.edges_total,
            );
        }
    }
    println!();
    println!(
        "  [{}] task_coverage {:.3} >= {:.3} (min)",
        mark(c.cov_pass),
        m.task_coverage,
        t.min_cov
    );
    println!(
        "  [{}] annotation_coverage {:.3} >= {:.3} (min)",
        mark(c.ann_pass),
        m.annotation_coverage,
        t.min_ann
    );
    println!(
        "  [{}] high_confidence_fraction {:.3} >= {:.3} (min)",
        mark(c.hc_pass),
        m.high_confidence_fraction,
        t.min_hc
    );
    println!();
    println!("gate: {}", if c.passed { "PASS" } else { "FAIL" });
    if !c.passed {
        if !c.cov_pass {
            eprintln!(
                "gate failed: task_coverage {:.3} < min {:.3}",
                m.task_coverage, t.min_cov
            );
        }
        if !c.ann_pass {
            eprintln!(
                "gate failed: annotation_coverage {:.3} < min {:.3}",
                m.annotation_coverage, t.min_ann
            );
        }
        if !c.hc_pass {
            eprintln!(
                "gate failed: high_confidence_fraction {:.3} < min {:.3}",
                m.high_confidence_fraction, t.min_hc
            );
        }
    }
}

fn print_gate_json(m: &GateMetrics, t: &GateThresholds, c: &GateChecks) {
    let per_dag: Vec<serde_json::Value> = m
        .per_dag
        .iter()
        .map(|d| {
            serde_json::json!({
                "dag": d.dag,
                "tasks_total": d.tasks_total,
                "tasks_with_lineage": d.tasks_with_lineage,
                "task_coverage": d.task_coverage,
                "tasks_annotated": d.tasks_annotated,
                "annotation_coverage": d.annotation_coverage,
                "edges_total": d.edges_total,
                "high_confidence_edges": d.high_confidence_edges,
                "high_confidence_fraction": d.high_confidence_fraction,
            })
        })
        .collect();
    let obj = serde_json::json!({
        "tasks_total": m.tasks_total,
        "tasks_with_lineage": m.tasks_with_lineage,
        "task_coverage": m.task_coverage,
        "tasks_annotated": m.tasks_annotated,
        "annotation_coverage": m.annotation_coverage,
        "edges_total": m.edges_total,
        "high_confidence_edges": m.high_confidence_edges,
        "high_confidence_fraction": m.high_confidence_fraction,
        "thresholds": {
            "min_task_coverage": t.min_cov,
            "min_high_confidence": t.min_hc,
            "min_annotation_coverage": t.min_ann,
        },
        "checks": {
            "task_coverage": c.cov_pass,
            "high_confidence_fraction": c.hc_pass,
            "annotation_coverage": c.ann_pass,
        },
        "passed": c.passed,
        "per_dag": per_dag,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&obj).unwrap_or_else(|_| "{}".to_string())
    );
}

// ── helpers ──

fn load_doc(path: &PathBuf) -> Result<WeaveDocument> {
    let s = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    trace_weaver_core::from_json(&s).with_context(|| format!("parsing {}", path.display()))
}

fn write_out(out: Option<&PathBuf>, content: &str) -> Result<()> {
    match out {
        Some(p) => {
            if let Some(parent) = p.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).ok();
                }
            }
            fs::write(p, content).with_context(|| format!("writing {}", p.display()))?;
            eprintln!("wrote {}", p.display());
        }
        None => println!("{content}"),
    }
    Ok(())
}

fn print_diagnostics(doc: &WeaveDocument) {
    for d in &doc.diagnostics {
        eprintln!("[{:?}] {}: {}", d.level, d.code, d.message);
    }
    let n = doc.diagnostics.len();
    eprintln!(
        "scanned: {} dataset(s), {} job(s), {} edge(s), {} diagnostic(s)",
        doc.datasets.len(),
        doc.jobs.len(),
        doc.edges.len(),
        n
    );
}

/// Minimal UTC RFC-3339 timestamp without pulling in a date crate.
fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (days, rem) = ((secs / 86400) as i64, (secs % 86400) as i64);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Howard Hinnant's days→(y,m,d) civil-date algorithm (UTC, proleptic Gregorian).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use trace_weaver_core::{Dataset, Edge, Engine, Origin};

    fn job(dag: &str, name: &str, inputs: &[&str], outputs: &[&str]) -> Job {
        let mut j = Job::new(format!("{dag}.{name}"), name, Engine::Python);
        j.dag = Some(dag.to_string());
        j.inputs = inputs.iter().map(|s| s.to_string()).collect();
        j.outputs = outputs.iter().map(|s| s.to_string()).collect();
        j
    }

    fn edge(from: &str, to: &str, job_id: &str, inferred: bool) -> Edge {
        let mut e = Edge::new(from, to);
        e.job = Some(job_id.to_string());
        e.origin = if inferred {
            Origin::inferred_code(0.5)
        } else {
            Origin::declared()
        };
        e
    }

    #[test]
    fn metrics_count_coverage_and_confidence() {
        let mut doc = WeaveDocument::new("ns", "test");
        for d in ["a", "b", "c"] {
            doc.datasets.push(Dataset::new(d));
        }
        // Two DAGs. dag1: one task with a declared edge. dag2: one task with an
        // inferred edge + one task with NO lineage (bare marker).
        doc.jobs.push(job("dag1", "t1", &["a"], &["b"]));
        doc.jobs.push(job("dag2", "t2", &["b"], &["c"]));
        doc.jobs.push(job("dag2", "t3", &[], &[]));
        doc.edges.push(edge("a", "b", "dag1.t1", false));
        doc.edges.push(edge("b", "c", "dag2.t2", true));

        let m = compute_gate_metrics(&doc);
        assert_eq!(m.tasks_total, 3);
        assert_eq!(m.tasks_with_lineage, 2); // t1, t2 (t3 has no edge)
        assert!((m.task_coverage - 2.0 / 3.0).abs() < 1e-9);
        assert_eq!(m.edges_total, 2);
        assert_eq!(m.high_confidence_edges, 1); // only the declared a->b
        assert!((m.high_confidence_fraction - 0.5).abs() < 1e-9);

        // Per-DAG breakdown.
        let d1 = m.per_dag.iter().find(|d| d.dag == "dag1").unwrap();
        assert_eq!((d1.tasks_total, d1.tasks_with_lineage), (1, 1));
        assert_eq!(d1.high_confidence_fraction, 1.0);
        let d2 = m.per_dag.iter().find(|d| d.dag == "dag2").unwrap();
        assert_eq!((d2.tasks_total, d2.tasks_with_lineage), (2, 1));
        assert_eq!(d2.task_coverage, 0.5);
        assert_eq!(d2.high_confidence_fraction, 0.0); // its one edge is inferred
    }

    #[test]
    fn empty_document_is_zero_coverage_but_vacuous_high_confidence() {
        let doc = WeaveDocument::new("ns", "test");
        let m = compute_gate_metrics(&doc);
        assert_eq!(m.tasks_total, 0);
        assert_eq!(m.task_coverage, 0.0);
        assert_eq!(m.tasks_annotated, 0);
        assert_eq!(m.annotation_coverage, 0.0);
        assert_eq!(m.edges_total, 0);
        assert_eq!(m.high_confidence_fraction, 1.0);
    }

    #[test]
    fn annotation_coverage_counts_decorated_tasks_only() {
        // Mix of task provenances in one DAG:
        //  * t_full   — declared @lineage with both inputs & outputs (edge).
        //  * t_inonly — declared @lineage, inputs only (no edge, but annotated).
        //  * t_bare   — declared bare @lineage marker (no datasets, no edge).
        //  * t_raw    — Pass-B raw operator discovered decorator-free (inferred
        //               job origin) — cannot carry a decorator, so NOT annotated.
        // annotation_coverage = 3/4; task_coverage counts only the edge-bearing
        // tasks (t_full and t_raw) = 2/4.
        let mut doc = WeaveDocument::new("ns", "test");
        for d in ["a", "b", "raw_in", "raw_out"] {
            doc.datasets.push(Dataset::new(d));
        }

        let mut t_full = job("dag", "t_full", &["a"], &["b"]);
        t_full.origin = Origin::declared();
        doc.jobs.push(t_full);

        let mut t_inonly = job("dag", "t_inonly", &["a"], &[]);
        t_inonly.origin = Origin::declared();
        doc.jobs.push(t_inonly);

        let mut t_bare = job("dag", "t_bare", &[], &[]);
        t_bare.origin = Origin::declared();
        doc.jobs.push(t_bare);

        let mut t_raw = job("dag", "t_raw", &["raw_in"], &["raw_out"]);
        t_raw.origin = Origin::inferred_code(0.7); // discovered decorator-free
        doc.jobs.push(t_raw);

        doc.edges.push(edge("a", "b", "dag.t_full", false));
        doc.edges.push(edge("raw_in", "raw_out", "dag.t_raw", true));

        let m = compute_gate_metrics(&doc);
        assert_eq!(m.tasks_total, 4);
        assert_eq!(
            m.tasks_annotated, 3,
            "the raw-operator task is not annotated"
        );
        assert!((m.annotation_coverage - 3.0 / 4.0).abs() < 1e-9);
        // task_coverage is edge-based and unchanged in semantics: t_full + t_raw.
        assert_eq!(m.tasks_with_lineage, 2);
        assert!((m.task_coverage - 2.0 / 4.0).abs() < 1e-9);

        // Per-DAG carries the same annotation numbers.
        let d = &m.per_dag[0];
        assert_eq!((d.tasks_annotated, d.tasks_total), (3, 4));
        assert!((d.annotation_coverage - 3.0 / 4.0).abs() < 1e-9);
    }

    #[test]
    fn threshold_flag_beats_env_and_bad_env_is_usage_error() {
        // Flag wins over env.
        std::env::set_var("TW_TEST_MIN", "0.4");
        assert_eq!(resolve_threshold(Some(0.9), "TW_TEST_MIN").unwrap(), 0.9);
        // Env used when no flag.
        assert_eq!(resolve_threshold(None, "TW_TEST_MIN").unwrap(), 0.4);
        // Unset env -> default 0.0.
        std::env::remove_var("TW_TEST_MIN");
        assert_eq!(resolve_threshold(None, "TW_TEST_MIN").unwrap(), 0.0);
        // Present-but-garbage env -> usage error.
        std::env::set_var("TW_TEST_MIN", "not-a-number");
        assert!(resolve_threshold(None, "TW_TEST_MIN").is_err());
        std::env::remove_var("TW_TEST_MIN");
    }
}
