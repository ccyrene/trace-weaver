#!/usr/bin/env python3
"""Claude-powered repository scanner for the CI pipeline.

Scans either the change set (diff mode) or the whole repository (full mode)
for anything undesirable: leaked organizational lineage/DAG data, secrets,
PII, vulnerabilities, malicious code, and risky misconfiguration.

The step is fully toggleable from GitHub *Actions variables* — no code change
or workflow edit needed:

    AI_SCAN_ENABLED        "true" to enable the scan (default: disabled)
    ANTHROPIC_API_KEY      secret (required when enabled)
    AI_SCAN_MODEL          Claude model id    (default: claude-opus-4-8)
    AI_SCAN_MODE           "diff" (default) or "full"
    AI_SCAN_FAIL_ON        "critical" | "high" (default) | "medium" | "never"
    AI_SCAN_FAIL_ON_ERROR  "true" to fail the build when the scanner itself
                           errors (default "false": availability first)
    AI_SCAN_REQUIRED       "true" makes a missing API key a hard error
                           (set by runs that exist only to run this scan)

Writes reports/ai-scan-report.md and reports/ai-scan-findings.json.
Exit codes: 0 = clean/skipped, 1 = blocking findings, 2 = scanner error.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = REPO_ROOT / "reports"

DEFAULT_MODEL = "claude-opus-4-8"
MAX_CONTENT_CHARS = 400_000  # hard cap on scanned content sent to the model

SEVERITY_ORDER = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}

TEXT_SUFFIXES = {
    ".py",
    ".sql",
    ".yml",
    ".yaml",
    ".toml",
    ".cfg",
    ".ini",
    ".json",
    ".md",
    ".mmd",
    ".txt",
    ".sh",
    ".csv",
    ".template",
    "",  # extensionless files: Dockerfile, .gitignore, .env, ...
}
EXCLUDED_DIRS = {
    ".git",
    "outputs",
    "reports",
    "logs",
    "__pycache__",
    ".venv",
    ".ruff_cache",
    ".mypy_cache",
    ".pytest_cache",
}
EXCLUDED_FILES = {"package-lock.json", "poetry.lock"}

SYSTEM_PROMPT = """\
You are a senior security engineer reviewing a CI change set for TraceWeaver,
an open-source CLI that statically scans Apache Airflow DAG repositories and
extracts data-lineage candidates (DAG/task/SQL/dataset/function metadata) to
CSV, JSON, Postgres, or a Mermaid graph.

TraceWeaver is a public repository, but it is RUN against private,
organizational DAG repositories. Its `outputs/` artifacts and any fixtures
derived from a real scan therefore expose internal pipeline structure. The
single most important thing to catch is ORGANIZATIONAL DATA LEAKING into the
public repo. Treat that as critical.

Review the provided content and report EVERYTHING undesirable, including
findings you are uncertain about — a downstream gate filters by severity.
Look specifically for:

1. org_data_leak     — real (non-synthetic) lineage from a scanned private
                       repo committed into the project: real DAG ids, task ids,
                       internal file paths, schema.table names, business
                       function names, connection ids, or scan outputs under
                       outputs/ or in tests/fixtures/docs. The intended sample
                       DAGs under examples/sample_dags/ are deliberately
                       synthetic (e.g. "daily_sales", s3://lake/raw/orders,
                       analytics.orders) — those are fine; flag anything that
                       looks like it came from a real company's Airflow repo.
2. secret            — hardcoded credentials, API keys, tokens, connection
                       strings with real passwords, private keys. (The demo
                       docker-compose Postgres credential "lineage:lineage" is
                       a known throwaway — not a finding.)
3. pii               — personal data committed to the repo: national id
                       numbers, bank/account numbers, phone numbers, real
                       names paired with accounts, real sample data.
4. vulnerability     — code injection, unsafe deserialization, SSRF, path
                       traversal, arbitrary file read/write, weak crypto. Note
                       TraceWeaver parses untrusted DAG source with ast.parse
                       (it must NEVER exec/eval/import scanned code).
5. malicious_code    — obfuscated code, suspicious network calls, backdoors,
                       typosquatted dependencies.
6. misconfiguration  — overly permissive settings, disabled TLS verification,
                       debug modes, dangerous CI permissions, default passwords.
7. quality           — only when it has operational risk (e.g. silent
                       exception swallowing that hides a scan failure).

Severity guide: critical = real org data/credential exposed or exploitable now;
high = likely exposure or real secret; medium = weakness needing specific
conditions; low/info = hardening advice. Use exact file paths from the input.
Do not invent line numbers — omit the line (null) if unsure.
"""

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "overall_risk": {
            "type": "string",
            "enum": ["critical", "high", "medium", "low", "clean"],
        },
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "severity": {
                        "type": "string",
                        "enum": ["critical", "high", "medium", "low", "info"],
                    },
                    "category": {
                        "type": "string",
                        "enum": [
                            "org_data_leak",
                            "secret",
                            "pii",
                            "vulnerability",
                            "malicious_code",
                            "misconfiguration",
                            "quality",
                        ],
                    },
                    "file": {"type": "string"},
                    "line": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                    "recommendation": {"type": "string"},
                },
                "required": [
                    "severity",
                    "category",
                    "file",
                    "line",
                    "title",
                    "description",
                    "recommendation",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["summary", "overall_risk", "findings"],
    "additionalProperties": False,
}


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_flag(name: str) -> bool:
    return env(name).lower() in {"1", "true", "yes", "on"}


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def collect_diff() -> str:
    """Return the change set this build should be judged on."""
    # GITHUB_BASE_REF is set on pull_request events.
    destination = env("GITHUB_BASE_REF")
    if destination:
        # FETCH_HEAD works regardless of the clone's fetch refspec;
        # origin/<branch> may not exist in single-branch clones.
        run_git("fetch", "--quiet", "origin", destination)
        return run_git("diff", "--no-color", "FETCH_HEAD...HEAD")

    # Build of the mainline itself: HEAD *is* origin/main, so a merge-base
    # diff would always be empty. Judge what just landed instead (for a
    # merge commit this is the whole merged change set vs first parent).
    if env("GITHUB_REF_NAME") == "main":
        return run_git("diff", "--no-color", "HEAD~1..HEAD")

    # Other branch push: diff against the mainline merge-base when
    # possible, otherwise fall back to the last commit.
    try:
        run_git("fetch", "--quiet", "origin", "main")
        return run_git("diff", "--no-color", "FETCH_HEAD...HEAD")
    except RuntimeError:
        return run_git("diff", "--no-color", "HEAD~1..HEAD")


def collect_full_repo() -> str:
    chunks: list[str] = []
    # -z + quotepath=false so non-ASCII file names round-trip.
    raw = run_git("-c", "core.quotepath=false", "ls-files", "-z")
    tracked = [p for p in raw.split("\0") if p]
    for rel_path in tracked:
        path = REPO_ROOT / rel_path
        parts = set(Path(rel_path).parts)
        if parts & EXCLUDED_DIRS or path.name in EXCLUDED_FILES:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        chunks.append(f"===== FILE: {rel_path} =====\n{text}")
    return "\n\n".join(chunks)


def build_user_content(mode: str) -> str | None:
    if mode == "full":
        header = "Full repository scan. Repository contents follow.\n\n"
        body = collect_full_repo()
    else:
        header = (
            "Change-set scan. Judge ONLY the changes in this unified diff "
            "(deleted lines matter for secret leaks too).\n\n"
        )
        body = collect_diff()

    if not body.strip():
        return None
    if len(body) > MAX_CONTENT_CHARS:
        body = body[:MAX_CONTENT_CHARS] + (
            "\n\n[TRUNCATED: content exceeded scan size limit — flag this as "
            "an 'info' finding so reviewers know coverage was partial]"
        )
    return header + body


def call_claude(model: str, user_content: str) -> dict:
    import anthropic

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=16000,
        thinking={"type": "adaptive"},
        output_config={
            "effort": "high",
            "format": {"type": "json_schema", "schema": OUTPUT_SCHEMA},
        },
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    text = next(block.text for block in response.content if block.type == "text")
    return json.loads(text)


def render_markdown(result: dict, mode: str, model: str) -> str:
    lines = [
        "# AI Scan Report (Claude)",
        "",
        f"- **Mode**: {mode}",
        f"- **Model**: {model}",
        f"- **Overall risk**: {result['overall_risk'].upper()}",
        "",
        f"> {result['summary']}",
        "",
    ]
    findings = sorted(
        result["findings"],
        key=lambda f: SEVERITY_ORDER.get(f["severity"], 0),
        reverse=True,
    )
    if not findings:
        lines.append("No findings. ✅")
        return "\n".join(lines) + "\n"

    lines += [
        "| Severity | Category | File | Line | Finding |",
        "|---|---|---|---|---|",
    ]
    for f in findings:
        line_no = f["line"] if f["line"] is not None else "-"
        lines.append(
            f"| {f['severity'].upper()} | {f['category']} | `{f['file']}` "
            f"| {line_no} | {f['title']} |"
        )
    lines.append("")
    for f in findings:
        lines += [
            f"## [{f['severity'].upper()}] {f['title']}",
            "",
            f"- **File**: `{f['file']}`"
            + (f" (line {f['line']})" if f["line"] is not None else ""),
            f"- **Category**: {f['category']}",
            "",
            f["description"],
            "",
            f"**Recommendation**: {f['recommendation']}",
            "",
        ]
    return "\n".join(lines) + "\n"


def write_reports(result: dict, mode: str, model: str) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "ai-scan-findings.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (REPORT_DIR / "ai-scan-report.md").write_text(
        render_markdown(result, mode, model), encoding="utf-8"
    )


def main() -> int:
    if not env_flag("AI_SCAN_ENABLED"):
        print("AI scan disabled (set Actions variable AI_SCAN_ENABLED=true).")
        return 0
    if not env("ANTHROPIC_API_KEY"):
        if env_flag("AI_SCAN_REQUIRED"):
            # An explicitly requested scan (e.g. a workflow_dispatch full scan)
            # must never silently pass green without scanning.
            print(
                "ERROR: this run requires the AI scan but ANTHROPIC_API_KEY is "
                "not set. Add it as a repository secret.",
                file=sys.stderr,
            )
            return 2
        print(
            "WARNING: AI_SCAN_ENABLED=true but ANTHROPIC_API_KEY is not set — "
            "skipping. Add it as a repository secret."
        )
        return 0

    mode = env("AI_SCAN_MODE", "diff").lower()
    model = env("AI_SCAN_MODEL", DEFAULT_MODEL)
    fail_on = env("AI_SCAN_FAIL_ON", "high").lower()

    try:
        user_content = build_user_content(mode)
        if user_content is None:
            print("AI scan: nothing to scan (empty change set).")
            return 0
        print(f"AI scan: mode={mode} model={model} fail_on={fail_on}")
        result = call_claude(model, user_content)
        write_reports(result, mode, model)
    except Exception as exc:  # noqa: BLE001 — report any scanner failure
        print(f"ERROR: AI scan failed to run: {exc}", file=sys.stderr)
        if env_flag("AI_SCAN_FAIL_ON_ERROR"):
            return 2
        print("AI_SCAN_FAIL_ON_ERROR is not set — not blocking the build.")
        return 0

    findings = result["findings"]
    print(f"\nAI scan: overall_risk={result['overall_risk']} findings={len(findings)}")
    for f in sorted(
        findings, key=lambda f: SEVERITY_ORDER.get(f["severity"], 0), reverse=True
    ):
        print(
            f"  [{f['severity'].upper():8}] {f['category']:16} "
            f"{f['file']}: {f['title']}"
        )
    print("\nFull report: reports/ai-scan-report.md (build artifact)")

    if fail_on == "never":
        return 0
    threshold = SEVERITY_ORDER.get(fail_on, SEVERITY_ORDER["high"])
    blocking = [
        f for f in findings if SEVERITY_ORDER.get(f["severity"], 0) >= threshold
    ]
    if blocking:
        print(
            f"\nFAIL: {len(blocking)} finding(s) at or above '{fail_on}' severity.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
