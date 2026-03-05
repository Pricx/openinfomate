from __future__ import annotations

from tracker.repo import Repo


def test_repo_app_config_roundtrip(db_session):
    repo = Repo(db_session)

    key = "unit_test_key"
    assert repo.get_app_config(key) is None

    repo.set_app_config(key, "P1")
    assert repo.get_app_config(key) == "P1"

    repo.set_app_config(key, "P2")
    assert repo.get_app_config(key) == "P2"

    repo.delete_app_config(key)
    assert repo.get_app_config(key) is None
