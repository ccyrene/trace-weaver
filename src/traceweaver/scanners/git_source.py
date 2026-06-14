"""Resolve a scan target that may be a local path or a remote git repository.

When the target looks like a git URL it is shallow-cloned into a temporary
directory for the duration of the scan and removed afterwards.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

_GIT_URL_RE = re.compile(
    r"^(?:"
    r"git@[^:]+:.+"  # scp-style: git@host:org/repo.git
    r"|ssh://.+"
    r"|git://.+"
    r"|file://.+\.git/?$"  # file:// pointing at a git repo
    r"|https?://.+\.git/?$"  # http(s) ending in .git
    r"|https?://(?:www\.)?(?:github|gitlab|bitbucket)\.[^/]+/.+"  # well-known hosts
    r")$"
)


def is_git_url(target: str) -> bool:
    return bool(_GIT_URL_RE.match(target.strip()))


@contextmanager
def resolve_repo(target: str, ref: str | None = None):
    """Yield a local ``Path`` to scan.

    For a local path this is a no-op. For a git URL the repo is cloned into a
    temporary directory that is cleaned up on exit.
    """
    if not is_git_url(target):
        yield Path(target)
        return

    tmp_dir = tempfile.mkdtemp(prefix="traceweaver-clone-")
    try:
        _clone(target, tmp_dir, ref)
        yield Path(tmp_dir)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _clone(url: str, dest: str, ref: str | None) -> None:
    cmd = ["git", "clone", "--depth", "1"]
    if ref:
        cmd += ["--branch", ref]
    cmd += [url, dest]
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:  # pragma: no cover - environment specific
        raise RuntimeError(
            "git is not installed; cannot clone remote repositories"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"git clone failed: {exc.stderr.strip() or exc}") from exc
