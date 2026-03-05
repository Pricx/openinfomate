from tracker.repo import Repo


def test_repo_set_source_enabled(db_session):
    repo = Repo(db_session)
    s = repo.add_source(type="rss", url="https://example.com/feed")
    assert s.enabled is True

    repo.set_source_enabled(s.id, False)
    s2 = repo.get_source_by_id(s.id)
    assert s2 is not None and s2.enabled is False


def test_repo_list_sources_with_health(db_session):
    repo = Repo(db_session)
    repo.add_source(type="rss", url="https://example.com/feed")
    rows = repo.list_sources_with_health()
    assert len(rows) == 1
    source, health = rows[0]
    assert source.type == "rss"
    assert health is None

