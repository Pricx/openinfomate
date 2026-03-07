from __future__ import annotations

from tracker.job_lock import job_lock_path


def test_job_lock_path_is_stable_across_cwd_for_same_instance(tmp_path, monkeypatch):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    monkeypatch.setenv("OPENINFOMATE_INSTANCE", "shared-instance")
    monkeypatch.delenv("TRACKER_ENV_PATH", raising=False)
    monkeypatch.delenv("TRACKER_DB_URL", raising=False)

    monkeypatch.chdir(a)
    p1 = job_lock_path(name="jobs")
    monkeypatch.chdir(b)
    p2 = job_lock_path(name="jobs")

    assert p1 == p2


def test_job_lock_path_separates_instances(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENINFOMATE_INSTANCE", "instance-a")
    p1 = job_lock_path(name="jobs")
    monkeypatch.setenv("OPENINFOMATE_INSTANCE", "instance-b")
    p2 = job_lock_path(name="jobs")

    assert p1 != p2
