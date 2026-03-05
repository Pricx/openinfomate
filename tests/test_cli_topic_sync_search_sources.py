from __future__ import annotations

import json
from urllib.parse import parse_qsl, urlsplit

from typer.testing import CliRunner

from tracker.cli import app


def _get_param(url: str, param: str) -> str:
    pairs = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    return dict(pairs).get(param, "")


def test_topic_sync_search_sources_updates_in_place_when_not_shared(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    r = runner.invoke(app, ["db", "init"])
    assert r.exit_code == 0

    r = runner.invoke(
        app,
        [
            "topic",
            "bootstrap",
            "--name",
            "T",
            "--query",
            "gpu,asic",
            "--searxng-base-url",
            "http://127.0.0.1:8888",
        ],
    )
    assert r.exit_code == 0

    r = runner.invoke(app, ["topic", "update", "T", "--query", "rust,compiler"])
    assert r.exit_code == 0

    r = runner.invoke(app, ["topic", "sync-search-sources", "--name", "T"])
    assert r.exit_code == 0

    cfg = json.loads(runner.invoke(app, ["config", "export"]).stdout)
    bindings = [b for b in cfg["bindings"] if b["topic"] == "T"]
    assert bindings

    hn = [b for b in bindings if b["source"]["type"] == "hn_search"]
    sx = [b for b in bindings if b["source"]["type"] == "searxng_search"]
    assert len(hn) == 1
    assert len(sx) == 1

    assert _get_param(hn[0]["source"]["url"], "query") == "rust compiler"
    assert _get_param(sx[0]["source"]["url"], "q") == "rust compiler"


def test_topic_sync_search_sources_rebinds_when_sources_are_shared(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    r = runner.invoke(app, ["db", "init"])
    assert r.exit_code == 0

    args = ["--searxng-base-url", "http://127.0.0.1:8888"]
    r = runner.invoke(app, ["topic", "bootstrap", "--name", "A", "--query", "rust,compiler", *args])
    assert r.exit_code == 0
    r = runner.invoke(app, ["topic", "bootstrap", "--name", "B", "--query", "rust,compiler", *args])
    assert r.exit_code == 0

    # Two shared sources: hn_search + searxng_search
    s = runner.invoke(app, ["stats"])
    assert s.exit_code == 0
    assert "- topics_total: 2" in s.stdout
    assert "- sources_total: 2" in s.stdout
    assert "- bindings_total: 4" in s.stdout

    r = runner.invoke(app, ["topic", "update", "B", "--query", "go,compiler"])
    assert r.exit_code == 0

    r = runner.invoke(app, ["topic", "sync-search-sources", "--name", "B"])
    assert r.exit_code == 0

    s2 = runner.invoke(app, ["stats"])
    assert s2.exit_code == 0
    assert "- topics_total: 2" in s2.stdout
    assert "- sources_total: 4" in s2.stdout
    assert "- bindings_total: 4" in s2.stdout

    cfg = json.loads(runner.invoke(app, ["config", "export"]).stdout)
    a_bindings = [b for b in cfg["bindings"] if b["topic"] == "A"]
    b_bindings = [b for b in cfg["bindings"] if b["topic"] == "B"]
    assert a_bindings and b_bindings

    a_hn = [b for b in a_bindings if b["source"]["type"] == "hn_search"][0]["source"]["url"]
    a_sx = [b for b in a_bindings if b["source"]["type"] == "searxng_search"][0]["source"]["url"]
    b_hn = [b for b in b_bindings if b["source"]["type"] == "hn_search"][0]["source"]["url"]
    b_sx = [b for b in b_bindings if b["source"]["type"] == "searxng_search"][0]["source"]["url"]

    assert _get_param(a_hn, "query") == "rust compiler"
    assert _get_param(a_sx, "q") == "rust compiler"
    assert _get_param(b_hn, "query") == "go compiler"
    assert _get_param(b_sx, "q") == "go compiler"

