from tracker.repo import Repo


def test_repo_source_meta_crud(db_session):
    repo = Repo(db_session)
    source = repo.add_source(type="rss", url="https://example.com/feed")

    assert repo.get_source_meta(source_id=source.id) is None
    repo.update_source_meta(source_id=source.id, tags="a,b", notes="hello")

    meta = repo.get_source_meta(source_id=source.id)
    assert meta is not None
    assert meta.tags == "a,b"
    assert meta.notes == "hello"

    rows = repo.list_sources_with_health_and_meta()
    assert len(rows) == 1
    s, _h, m = rows[0]
    assert s.id == source.id
    assert m is not None and m.tags == "a,b"

