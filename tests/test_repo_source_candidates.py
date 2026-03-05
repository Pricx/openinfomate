from __future__ import annotations

from tracker.repo import Repo


def test_repo_add_source_candidate_is_idempotent(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")

    c1, created1 = repo.add_source_candidate(
        topic_id=topic.id,
        source_type="rss",
        url="https://example.com/feed?utm_source=x",
        title="Feed",
        discovered_from_url="https://example.com/post",
    )
    assert created1 is True
    assert c1.status == "new"
    assert c1.seen_count == 1
    assert c1.url == "https://example.com/feed"

    c2, created2 = repo.add_source_candidate(
        topic_id=topic.id,
        source_type="rss",
        url="https://example.com/feed?utm_medium=y",
        title="",
        discovered_from_url="https://example.com/another",
    )
    assert created2 is False
    assert c2.id == c1.id
    assert c2.seen_count == 2
    assert c2.discovered_from_url == "https://example.com/another"


def test_repo_list_source_candidates(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    repo.add_source_candidate(topic_id=topic.id, source_type="rss", url="https://example.com/feed")

    rows = repo.list_source_candidates(limit=10)
    assert len(rows) == 1
    cand, t = rows[0]
    assert t.name == "T"
    assert cand.url == "https://example.com/feed"

