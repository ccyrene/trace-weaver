import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from traceweaver.cli import build_parser, main
from traceweaver.scanners.git_source import is_git_url

EXAMPLES = str(Path(__file__).resolve().parents[1] / "examples" / "sample_dags")


def _env_without_traceweaver():
    return {k: v for k, v in os.environ.items() if not k.startswith("TRACEWEAVER_")}


def _run(argv):
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        return main(argv)


class TestCliParsing(unittest.TestCase):
    def test_env_defaults(self):
        env = _env_without_traceweaver()
        env.update(
            {
                "TRACEWEAVER_REPO_PATH": "/repo",
                "TRACEWEAVER_OUTPUT_FORMAT": "json",
                "TRACEWEAVER_OUTPUT_DIR": "/artifacts",
                "TRACEWEAVER_DATABASE_URL": "postgresql://u:p@h/db",
                "TRACEWEAVER_GIT_REF": "main",
            }
        )
        with patch.dict(os.environ, env, clear=True):
            args = build_parser().parse_args(["scan"])
        self.assertEqual(args.repo_path, "/repo")
        self.assertEqual(args.output, "json")
        self.assertEqual(args.output_dir, "/artifacts")
        self.assertEqual(args.database_url, "postgresql://u:p@h/db")
        self.assertEqual(args.git_ref, "main")

    def test_defaults_without_env(self):
        with patch.dict(os.environ, _env_without_traceweaver(), clear=True):
            args = build_parser().parse_args(["scan"])
        self.assertIsNone(args.repo_path)
        self.assertEqual(args.output, "csv")
        self.assertEqual(args.output_dir, "outputs")


class TestCliRun(unittest.TestCase):
    def test_summary_ok(self):
        with patch.dict(os.environ, _env_without_traceweaver(), clear=True):
            self.assertEqual(
                _run(["scan", "--repo-path", EXAMPLES, "--output", "summary"]), 0
            )

    def test_missing_repo_path(self):
        with patch.dict(os.environ, _env_without_traceweaver(), clear=True):
            self.assertEqual(_run(["scan", "--output", "summary"]), 2)

    def test_db_requires_url(self):
        with patch.dict(os.environ, _env_without_traceweaver(), clear=True):
            self.assertEqual(
                _run(["scan", "--repo-path", EXAMPLES, "--output", "db"]), 2
            )

    def test_nonexistent_repo(self):
        with patch.dict(os.environ, _env_without_traceweaver(), clear=True):
            self.assertEqual(
                _run(["scan", "--repo-path", "/no/such/path", "--output", "summary"]), 2
            )

    def test_output_all_writes_every_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, _env_without_traceweaver(), clear=True):
                code = _run(
                    [
                        "scan",
                        "--repo-path",
                        EXAMPLES,
                        "--output",
                        "all",
                        "--output-dir",
                        tmp,
                    ]
                )
            self.assertEqual(code, 0)
            for name in (
                "lineage_jobs.csv",
                "lineage_datasets.csv",
                "lineage_edges.csv",
                "task_dependencies.csv",
                "function_calls.csv",
                "raw_scan_result.json",
            ):
                self.assertTrue((Path(tmp) / name).exists(), name)


class TestGitDetection(unittest.TestCase):
    def test_local_paths_are_not_git_urls(self):
        for path in ("examples/sample_dags", "/abs/path", "./dags", "../x"):
            self.assertFalse(is_git_url(path), path)

    def test_git_urls(self):
        for url in (
            "https://github.com/apache/airflow.git",
            "https://github.com/apache/airflow",
            "git@github.com:apache/airflow.git",
            "git://host/repo",
        ):
            self.assertTrue(is_git_url(url), url)


if __name__ == "__main__":
    unittest.main()
