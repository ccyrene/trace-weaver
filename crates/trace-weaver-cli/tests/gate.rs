//! End-to-end tests for the `trace-weaver gate` subcommand: exit codes
//! (0 pass / 1 threshold fail / 2 usage error), env-var fallbacks, and JSON.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Path to the compiled `trace-weaver` binary under test (set by Cargo).
fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_trace-weaver")
}

/// Write a small repo of two fully-declared `@lineage` tasks into a fresh temp
/// dir and return its path. Coverage = 1.0, all edges declared (high conf).
fn fixture_repo(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tw-gate-{}-{}", std::process::id(), tag));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("dag.py"),
        r#"
from traceweaver import lineage

@lineage(inputs=["s3://raw/sales.parquet"], outputs=["iceberg://w.sales.bronze"])
def build_bronze():
    pass

@lineage(inputs=["iceberg://w.sales.bronze"], outputs=["iceberg://w.sales.silver"])
def build_silver():
    pass
"#,
    )
    .unwrap();
    dir
}

/// A repo that reproduces the production "honest annotation" finding: four tasks
/// where every human-authored task carries an explicit `@lineage`, yet only two
/// yield a full input→output edge. So annotation_coverage (3/4) is high while
/// task_coverage (2/4) is lower — and one task (a raw operator) is undecoratable.
///
///  * `bare_marker`        — bare `@lineage`               → annotated, no edge
///  * `probe_inputs_only`  — `@lineage(inputs=[...])`      → annotated, no edge
///  * `full_task`          — `@lineage(inputs, outputs)`   → annotated, one edge
///  * `raw_op`             — plain SQL Airflow operator     → NOT annotated, edge
fn mixed_repo(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tw-gate-mix-{}-{}", std::process::id(), tag));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("dag.py"),
        r#"
from traceweaver import lineage
from airflow import DAG

@lineage
def bare_marker():
    pass

@lineage(inputs=["s3://raw/probe.parquet"])
def probe_inputs_only():
    pass

@lineage(inputs=["s3://raw/in.parquet"], outputs=["iceberg://w.db.out"])
def full_task():
    pass

RAW_SQL = "INSERT INTO raw_out (id) SELECT id FROM raw_in"
with DAG("d") as dag:
    raw = PostgresOperator(task_id="raw_op", sql=RAW_SQL)
"#,
    )
    .unwrap();
    dir
}

fn run(args: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.args(args);
    // Clear the two gate env vars unless explicitly set, so the ambient
    // environment can't perturb the test.
    cmd.env_remove("TRACEWEAVER_MIN_TASK_COVERAGE");
    cmd.env_remove("TRACEWEAVER_MIN_HIGH_CONFIDENCE");
    cmd.env_remove("TRACEWEAVER_MIN_ANNOTATION_COVERAGE");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().expect("failed to run trace-weaver")
}

fn code(o: &std::process::Output) -> i32 {
    o.status.code().expect("process exited via signal")
}

#[test]
fn gate_passes_with_satisfiable_threshold() {
    let repo = fixture_repo("pass");
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-task-coverage",
            "0.1",
            "--format",
            "text",
        ],
        &[],
    );
    assert_eq!(
        code(&o),
        0,
        "stderr: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(String::from_utf8_lossy(&o.stdout).contains("gate: PASS"));
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_fails_with_impossible_threshold() {
    let repo = fixture_repo("fail");
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-task-coverage",
            "1.1",
        ],
        &[],
    );
    assert_eq!(code(&o), 1);
    assert!(String::from_utf8_lossy(&o.stdout).contains("gate: FAIL"));
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_usage_error_on_missing_repo() {
    let o = run(&["gate", "--repo-path", "/no/such/path/xyzzy"], &[]);
    assert_eq!(code(&o), 2);
    let _ = o;
}

#[test]
fn gate_usage_error_on_bad_env_threshold() {
    let repo = fixture_repo("badenv");
    let o = run(
        &["gate", "--repo-path", repo.to_str().unwrap()],
        &[("TRACEWEAVER_MIN_TASK_COVERAGE", "not-a-number")],
    );
    assert_eq!(code(&o), 2);
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_env_fallback_can_fail_and_flag_overrides_env() {
    let repo = fixture_repo("env");

    // Env alone sets an impossible coverage -> fail (exit 1).
    let o = run(
        &["gate", "--repo-path", repo.to_str().unwrap()],
        &[("TRACEWEAVER_MIN_TASK_COVERAGE", "1.1")],
    );
    assert_eq!(code(&o), 1, "env threshold should gate");

    // A permissive flag overrides the impossible env -> pass (exit 0).
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-task-coverage",
            "0.0",
        ],
        &[("TRACEWEAVER_MIN_TASK_COVERAGE", "1.1")],
    );
    assert_eq!(code(&o), 0, "flag must win over env");
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_json_has_metrics_and_per_dag() {
    let repo = fixture_repo("json");
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-task-coverage",
            "0.5",
            "--format",
            "json",
        ],
        &[],
    );
    assert_eq!(code(&o), 0);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("valid JSON");
    assert_eq!(v["tasks_total"], 2);
    assert_eq!(v["task_coverage"], 1.0);
    assert_eq!(v["high_confidence_fraction"], 1.0);
    assert_eq!(v["passed"], true);
    assert!(v["per_dag"].is_array());
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_json_reports_annotation_coverage() {
    // The mixed repo: annotation_coverage = 3/4, task_coverage = 2/4, and the
    // raw-operator task is counted in tasks_total but is not annotated.
    let repo = mixed_repo("json-ann");
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--format",
            "json",
        ],
        &[],
    );
    assert_eq!(
        code(&o),
        0,
        "stderr: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("valid JSON");
    assert_eq!(v["tasks_total"], 4);
    assert_eq!(v["tasks_annotated"], 3);
    assert_eq!(v["annotation_coverage"], 0.75);
    assert_eq!(v["task_coverage"], 0.5);
    assert_eq!(v["thresholds"]["min_annotation_coverage"], 0.0);
    assert_eq!(v["checks"]["annotation_coverage"], true);
    // Per-DAG breakdown carries the annotation fields too.
    assert!(v["per_dag"][0]["annotation_coverage"].is_number());
    assert_eq!(v["per_dag"][0]["tasks_annotated"], 3);
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_annotation_flag_gates_and_text_shows_line() {
    let repo = mixed_repo("ann-flag");

    // annotation_coverage is 0.75, so a 0.8 minimum FAILS (exit 1)...
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-annotation-coverage",
            "0.8",
        ],
        &[],
    );
    assert_eq!(code(&o), 1, "0.8 annotation min should fail on 0.75");
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(
        out.contains("annotation_coverage"),
        "text metric line: {out}"
    );
    assert!(out.contains("gate: FAIL"));

    // ...while a 0.75 minimum PASSES (exit 0).
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-annotation-coverage",
            "0.75",
        ],
        &[],
    );
    assert_eq!(
        code(&o),
        0,
        "0.75 annotation min should pass; stderr: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_annotation_env_fallback_and_flag_overrides() {
    let repo = mixed_repo("ann-env");

    // Env alone sets an impossible annotation coverage -> fail (exit 1).
    let o = run(
        &["gate", "--repo-path", repo.to_str().unwrap()],
        &[("TRACEWEAVER_MIN_ANNOTATION_COVERAGE", "0.9")],
    );
    assert_eq!(code(&o), 1, "env annotation threshold should gate");

    // A permissive flag overrides the impossible env -> pass (exit 0).
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--min-annotation-coverage",
            "0.0",
        ],
        &[("TRACEWEAVER_MIN_ANNOTATION_COVERAGE", "0.9")],
    );
    assert_eq!(code(&o), 0, "flag must win over env");

    // A present-but-unparseable env is a usage error (exit 2).
    let o = run(
        &["gate", "--repo-path", repo.to_str().unwrap()],
        &[("TRACEWEAVER_MIN_ANNOTATION_COVERAGE", "not-a-number")],
    );
    assert_eq!(code(&o), 2);
    let _ = fs::remove_dir_all(&repo);
}

#[test]
fn gate_invalid_format_is_clap_usage_error() {
    let repo = fixture_repo("badfmt");
    let o = run(
        &[
            "gate",
            "--repo-path",
            repo.to_str().unwrap(),
            "--format",
            "yaml",
        ],
        &[],
    );
    // clap rejects an invalid --format value with its standard usage exit code 2.
    assert_eq!(code(&o), 2);
    let _ = fs::remove_dir_all(&repo);
}
