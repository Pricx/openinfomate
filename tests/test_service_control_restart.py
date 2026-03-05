from __future__ import annotations

from types import SimpleNamespace

import tracker.service_control as sc


def test_systemctl_user_preflight_strict_failed_to_connect(monkeypatch):
    monkeypatch.setattr(sc.shutil, "which", lambda _name: "/bin/systemctl")

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(returncode=1, stderr="Failed to connect to bus: No such file or directory\n")

    monkeypatch.setattr(sc.subprocess, "run", fake_run)

    ok, msg = sc._systemctl_user_preflight()
    assert ok is False
    assert "Failed to connect to bus" in msg
    assert sc.can_restart_systemd_user() is False


def test_queue_restart_returns_error_when_preflight_fails(monkeypatch):
    monkeypatch.setattr(sc.shutil, "which", lambda _name: "/bin/systemctl")

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(returncode=1, stderr="Failed to connect to bus: No such file or directory\n")

    popen_called = {"n": 0}

    def fake_popen(*_args, **_kwargs):
        popen_called["n"] += 1
        raise AssertionError("should not call Popen when preflight fails")

    monkeypatch.setattr(sc.subprocess, "run", fake_run)
    monkeypatch.setattr(sc.subprocess, "Popen", fake_popen)

    res = sc.queue_restart_systemd_user(units=["tracker"], delay_seconds=0)
    assert res.ok is False
    assert res.queued is False
    assert popen_called["n"] == 0
    assert "Failed to connect to bus" in res.message


def test_queue_restart_denies_unknown_units(monkeypatch):
    monkeypatch.setattr(sc.shutil, "which", lambda _name: "/bin/systemctl")

    # Even if systemctl works, unknown units should be blocked before preflight.
    monkeypatch.setattr(sc, "_systemctl_user_preflight", lambda: (True, ""))

    res = sc.queue_restart_systemd_user(units=["tracker", "nope"], delay_seconds=0)
    assert res.ok is False
    assert res.queued is False
    assert "denied units" in res.message
