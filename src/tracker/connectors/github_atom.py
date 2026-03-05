from __future__ import annotations

import re
from urllib.parse import quote, urlsplit

_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def _normalize_repo(repo: str) -> str:
    r = (repo or "").strip()
    if r.startswith(("http://", "https://")):
        parts = urlsplit(r)
        if parts.netloc.lower().endswith("github.com"):
            seg = [p for p in parts.path.strip("/").split("/") if p]
            if len(seg) >= 2:
                r = f"{seg[0]}/{seg[1]}"

    if not _REPO_RE.fullmatch(r):
        raise ValueError(f"invalid repo: {repo!r}")
    return r


def build_github_releases_atom_url(*, repo: str) -> str:
    repo = _normalize_repo(repo)
    return f"https://github.com/{repo}/releases.atom"


def build_github_issues_atom_url(*, repo: str) -> str:
    repo = _normalize_repo(repo)
    return f"https://github.com/{repo}/issues.atom"


def build_github_pulls_atom_url(*, repo: str) -> str:
    repo = _normalize_repo(repo)
    return f"https://github.com/{repo}/pulls.atom"


def build_github_commits_atom_url(*, repo: str, branch: str = "main") -> str:
    repo = _normalize_repo(repo)
    branch = (branch or "").strip()
    if not branch:
        raise ValueError("branch must be non-empty")
    branch_q = quote(branch, safe="/-_.")
    return f"https://github.com/{repo}/commits/{branch_q}.atom"

