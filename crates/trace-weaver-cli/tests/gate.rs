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

fn run(args: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.args(args);
    // Clear the two gate env vars unless explicitly set, so the ambient
    // environment can't perturb the test.
    cmd.env_remove("TRACEWEAVER_MIN_TASK_COVERAGE");
    cmd.env_remove("TRACEWEAVER_MIN_HIGH_CONFIDENCE");
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
