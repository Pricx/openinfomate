from __future__ import annotations

import pytest

from tracker.connectors.github_atom import (
    build_github_commits_atom_url,
    build_github_issues_atom_url,
    build_github_pulls_atom_url,
    build_github_releases_atom_url,
)


def test_build_github_releases_atom_url():
    assert (
        build_github_releases_atom_url(repo="openai/openai-python")
        == "https://github.com/openai/openai-python/releases.atom"
    )


def test_build_github_issues_atom_url():
    assert (
        build_github_issues_atom_url(repo="openai/openai-python")
        == "https://github.com/openai/openai-python/issues.atom"
    )


def test_build_github_pulls_atom_url():
    assert (
        build_github_pulls_atom_url(repo="openai/openai-python")
        == "https://github.com/openai/openai-python/pulls.atom"
    )


def test_build_github_commits_atom_url():
    assert (
        build_github_commits_atom_url(repo="openai/openai-python", branch="main")
        == "https://github.com/openai/openai-python/commits/main.atom"
    )


def test_build_github_urls_accept_repo_url():
    assert (
        build_github_releases_atom_url(repo="https://github.com/openai/openai-python")
        == "https://github.com/openai/openai-python/releases.atom"
    )


def test_build_github_urls_invalid_repo():
    with pytest.raises(ValueError):
        build_github_releases_atom_url(repo="not a repo")

