//! `trace-weaver` — the command-line data-lineage compiler.
//!
//! ```text
//! trace-weaver scan <path> [-o out.weave.json] [--namespace NS] [--no-sql-infer] [--no-code-infer] [--strict]
//! trace-weaver validate <doc.weave.json> [--strict]
//! trace-weaver export --to <openmetadata|openlineage|dot> <doc.weave.json> [--dry-run] [-o out] ...
//! trace-weaver graph <doc.weave.json> [-o out.dot]
//! ```

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

use trace_weaver_core::{has_errors, validate, DiagLevel, WeaveDocument};
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Scan(a) => cmd_scan(a),
        Command::Validate(a) => cmd_validate(a),
        Command::Export(a) => cmd_export(a),
        Command::Graph(a) => cmd_graph(a),
    }
}

fn cmd_scan(a: ScanArgs) -> Result<()> {
    let opts = ScanOptions {
        namespace: a.namespace,
        enable_sql_inference: !a.no_sql_infer,
        enable_code_inference: !a.no_code_infer,
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
