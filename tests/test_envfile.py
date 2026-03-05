from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from tracker.cli import app
from tracker.envfile import parse_env_assignments, upsert_env_vars


def test_upsert_env_vars_updates_and_appends(tmp_path):
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("# header\nTRACKER_A=1\nTRACKER_B=\"old\"\n", encoding="utf-8")

    upsert_env_vars(path=env_path, updates={"TRACKER_B": "new", "TRACKER_C": "x y"})

    out = env_path.read_text(encoding="utf-8")
    assert "# header" in out
    assert "TRACKER_A=1" in out
    assert "TRACKER_B=\"new\"" in out
    assert "TRACKER_C=\"x y\"" in out


def test_upsert_env_vars_updates_last_duplicate_key(tmp_path):
    env_path = Path(tmp_path) / ".env"
    env_path.write_text(
        "TRACKER_A=1\nTRACKER_B=\"first\"\nTRACKER_B=\"second\"\n",
        encoding="utf-8",
    )

    upsert_env_vars(path=env_path, updates={"TRACKER_B": "new"})

    parsed = parse_env_assignments(env_path.read_text(encoding="utf-8"))
    assert parsed["TRACKER_B"] == "new"


def test_parse_env_assignments_basic():
    text = """
    # comment
    TRACKER_A=1
    TRACKER_B="x y"
    TRACKER_JSON="{\\"a\\":{\\"b\\":1}}"
    TRACKER_NL="a\\nb"
    BAD KEY=oops
    TRACKER_C='z'
    """
    parsed = parse_env_assignments(text)
    assert parsed["TRACKER_A"] == "1"
    assert parsed["TRACKER_B"] == "x y"
    assert parsed["TRACKER_JSON"] == '{"a":{"b":1}}'
    assert parsed["TRACKER_NL"] == "a\nb"
    assert parsed["TRACKER_C"] == "z"
    assert "BAD KEY" not in parsed


def test_cli_env_set_writes_to_custom_env_path(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_ENV_PATH", "./custom.env")

    r = runner.invoke(app, ["env", "set", "TRACKER_X", "v"])
    assert r.exit_code == 0

    text = Path(tmp_path).joinpath("custom.env").read_text(encoding="utf-8")
    assert "TRACKER_X=\"v\"" in text


def test_get_settings_uses_tracker_env_path_for_loading(tmp_path, monkeypatch):
    env_path = Path(tmp_path) / "custom.env"
    env_path.write_text("TRACKER_DB_URL=sqlite:///./from_custom.db\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_ENV_PATH", str(env_path))

    from tracker.settings import get_settings

    s = get_settings()
    assert s.db_url.endswith("from_custom.db")
