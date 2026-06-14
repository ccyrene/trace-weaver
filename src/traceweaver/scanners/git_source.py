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


_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


def is_commit_sha(ref: str | None) -> bool:
    """True when ``ref`` looks like a (full or abbreviated) commit SHA.

    ``git clone --branch`` only accepts branch/tag names, so a SHA needs the
    fetch-by-commit path instead.
    """
    return bool(ref) and bool(_SHA_RE.match(ref.strip()))


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
    if is_commit_sha(ref):
        _clone_commit(url, dest, ref.strip())
    else:
        _clone_ref(url, dest, ref)


def _clone_ref(url: str, dest: str, ref: str | None) -> None:
    """Shallow-clone a branch/tag (or the default branch when ``ref`` is None)."""
    cmd = ["git", "clone", "--depth", "1"]
    if ref:
        cmd += ["--branch", ref]
    cmd += [url, dest]
    _run_git(cmd)


def _clone_commit(url: str, dest: str, sha: str) -> None:
    """Fetch a single commit by SHA.

    ``git clone --branch`` rejects a raw SHA, so init an empty repo and fetch
    just that commit (works against servers that allow fetching reachable SHAs,
    e.g. GitHub). Fall back to a full clone + checkout when the server refuses
    a by-commit fetch.
    """
    try:
        _run_git(["git", "init", "--quiet", dest])
        _run_git(["git", "-C", dest, "remote", "add", "origin", url])
        _run_git(["git", "-C", dest, "fetch", "--depth", "1", "origin", sha])
        _run_git(["git", "-C", dest, "checkout", "--quiet", "FETCH_HEAD"])
    except RuntimeError:
        # Reset the dest dir and fall back to a full clone, then check out the SHA.
        shutil.rmtree(dest, ignore_errors=True)
        Path(dest).mkdir(parents=True, exist_ok=True)
        _run_git(["git", "clone", url, dest])
        _run_git(["git", "-C", dest, "checkout", "--quiet", sha])


def _run_git(cmd: list[str]) -> None:
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
        raise RuntimeError(
            f"git command failed ({' '.join(cmd)}): {exc.stderr.strip() or exc}"
        ) from exc
