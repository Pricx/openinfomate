from tracker.repo import Repo


def test_repo_get_stats_counts(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai")
    source = repo.add_source(type="rss", url="https://example.com/feed")
    repo.bind_topic_source(topic=topic, source=source)

    stats = repo.get_stats()
    assert stats["topics_total"] == 1
    assert stats["topics_enabled"] == 1
    assert stats["sources_total"] == 1
    assert stats["sources_enabled"] == 1
    assert stats["bindings_total"] == 1

