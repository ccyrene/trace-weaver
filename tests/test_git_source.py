import unittest
from unittest.mock import patch

from traceweaver.scanners import git_source
from traceweaver.scanners.git_source import is_commit_sha


class TestCommitShaDetection(unittest.TestCase):
    def test_sha_like_refs(self):
        self.assertTrue(is_commit_sha("a1b2c3d"))  # abbreviated
        self.assertTrue(is_commit_sha("0" * 40))  # full
        self.assertTrue(is_commit_sha("0f1e2d3c4b5a"))

    def test_non_sha_refs(self):
        for ref in (None, "", "main", "v1.2.3", "release/2024", "abc", "feature"):
            self.assertFalse(is_commit_sha(ref), ref)


class _FakeCompleted:
    returncode = 0
    stdout = ""
    stderr = ""


class TestCloneRouting(unittest.TestCase):
    def _patched(self):
        calls: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            return _FakeCompleted()

        return calls, fake_run

    def test_branch_ref_uses_clone_branch(self):
        calls, fake = self._patched()
        with patch.object(git_source.subprocess, "run", fake):
            git_source._clone("https://example.com/x.git", "/tmp/dest", "main")
        self.assertEqual(len(calls), 1)
        self.assertIn("--branch", calls[0])
        self.assertIn("main", calls[0])

    def test_sha_ref_uses_fetch_then_checkout(self):
        calls, fake = self._patched()
        with patch.object(git_source.subprocess, "run", fake):
            git_source._clone("https://example.com/x.git", "/tmp/dest", "a1b2c3d")
        flat = [" ".join(c) for c in calls]
        self.assertTrue(any("fetch --depth 1 origin a1b2c3d" in f for f in flat))
        self.assertTrue(any("checkout --quiet FETCH_HEAD" in f for f in flat))
        # A SHA must never go through `git clone --branch`.
        self.assertFalse(any("--branch" in c for c in calls))


if __name__ == "__main__":
    unittest.main()
