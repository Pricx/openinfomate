from tracker.repo import Repo


def test_repo_binding_crud(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai")
    source = repo.add_source(type="rss", url="https://example.com/feed")

    ts = repo.bind_topic_source(topic=topic, source=source)
    assert ts.include_keywords == ""
    assert ts.exclude_keywords == ""

    repo.update_topic_source_filters(topic=topic, source=source, include_keywords="a", exclude_keywords="b")
    ts2 = repo.get_topic_source(topic_id=topic.id, source_id=source.id)
    assert ts2 is not None
    assert ts2.include_keywords == "a"
    assert ts2.exclude_keywords == "b"

    rows = repo.list_topic_sources(topic=topic)
    assert len(rows) == 1
    t, s, ts3 = rows[0]
    assert t.name == "T"
    assert s.url == "https://example.com/feed"
    assert ts3.include_keywords == "a"

    assert repo.unbind_topic_source(topic=topic, source=source) is True
    assert repo.get_topic_source(topic_id=topic.id, source_id=source.id) is None
    assert repo.unbind_topic_source(topic=topic, source=source) is False

