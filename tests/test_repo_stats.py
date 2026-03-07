import datetime as dt

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


def test_repo_get_activity_snapshot_includes_scheduler_heartbeats(db_session):
    repo = Repo(db_session)
    repo.set_app_config_many(
        {
            "service.scheduler.digest_sync.last_ok_at": "2026-03-07T01:02:03",
            "service.scheduler.curated_sync.last_ok_at": "2026-03-07T01:07:03",
            "service.scheduler.digest_sync.enabled_topics": "5",
            "service.scheduler.digest_sync.scheduled_topics": "4",
            "service.scheduler.curated_sync.job_present": "1",
        }
    )

    snapshot = repo.get_activity_snapshot()

    assert snapshot.last_digest_sync_at == dt.datetime(2026, 3, 7, 1, 2, 3)
    assert snapshot.last_curated_sync_at == dt.datetime(2026, 3, 7, 1, 7, 3)
    assert snapshot.digest_sync_enabled_topics == 5
    assert snapshot.digest_sync_scheduled_topics == 4
    assert snapshot.curated_sync_job_present is True
