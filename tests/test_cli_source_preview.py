from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from tracker.cli import app


def test_source_preview_prints_entries(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(app, ["db", "init"])
    assert init.exit_code == 0

    fixture = Path(__file__).with_name("fixtures").joinpath("html_list_sample.html").resolve()
    page_url = fixture.as_uri()

    add = runner.invoke(
        app,
        [
            "source",
            "add-html-list",
            "--page-url",
            page_url,
            "--item-selector",
            ".posts li",
            "--title-selector",
            "a.post-link",
            "--summary-selector",
            "p.summary",
        ],
    )
    assert add.exit_code == 0

    out = runner.invoke(app, ["source", "preview", "1"])
    assert out.exit_code == 0
    assert "First Post" in out.stdout
    assert "Second Post" in out.stdout
    assert "Third Post" in out.stdout

