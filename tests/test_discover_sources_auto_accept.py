from __future__ import annotations

import asyncio

from tracker.connectors.base import FetchedEntry
from tracker.llm import LlmSourceCandidateDecision
from tracker.repo import Repo
from tracker.runner import run_discover_sources
from tracker.settings import Settings


def test_run_discover_sources_auto_accepts_new_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="keep signals")

    cand, _created = repo.add_source_candidate(
        topic_id=topic.id,
        source_type="rss",
        url="https://example.com/feed.xml",
        discovered_from_url="https://example.com/blog/",
    )

    async def fake_fetch(self, *, url: str):  # type: ignore[no-untyped-def]
        assert url == "https://example.com/feed.xml"
        return [
            FetchedEntry(url="https://example.com/p/1", title="Signal 1"),
            FetchedEntry(url="https://example.com/p/2", title="Signal 2"),
        ]

    async def fake_decide(*, settings, topic, policy_prompt, candidates, max_accept, usage_cb=None):  # type: ignore[no-untyped-def]
        assert topic.name == "T"
        assert "keep signals" in policy_prompt
        assert max_accept == 2
        assert candidates and candidates[0]["candidate_id"] == cand.id
        return [
            LlmSourceCandidateDecision(candidate_id=cand.id, decision="accept", score=85, quality_score=85, relevance_score=80, novelty_score=70, why="high signal"),
        ]

    monkeypatch.setattr("tracker.runner.RssConnector.fetch", fake_fetch)
    monkeypatch.setattr("tracker.runner.llm_decide_source_candidates", fake_decide)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="gpt-5.2",
        discover_sources_auto_accept_enabled=True,
        discover_sources_auto_accept_max_per_topic=2,
        discover_sources_auto_accept_preview_entries=2,
    )
    asyncio.run(run_discover_sources(session=db_session, settings=settings, topic_ids=[topic.id]))

    cand_row = repo.get_source_candidate_by_id(cand.id)
    assert cand_row is not None
    assert cand_row.status == "accepted"

    sources = repo.list_sources()
    assert any(s.type == "rss" and s.url == "https://example.com/feed.xml" for s in sources)

    # Auto-accept note is stored on the created Source meta.
    created_source = next(s for s in sources if s.url == "https://example.com/feed.xml")
    meta = repo.get_source_meta(source_id=created_source.id)
    assert meta is not None
    assert "auto-accept" in (meta.notes or "")
    assert "high signal" in (meta.notes or "")


def test_run_discover_sources_auto_ignores_new_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="keep signals")

    cand, _created = repo.add_source_candidate(
        topic_id=topic.id,
        source_type="rss",
        url="https://example.com/feed.xml",
        discovered_from_url="https://example.com/blog/",
    )

    async def fake_fetch(self, *, url: str):  # type: ignore[no-untyped-def]
        return [FetchedEntry(url="https://example.com/p/1", title="Noise")]

    async def fake_decide(*, settings, topic, policy_prompt, candidates, max_accept, usage_cb=None):  # type: ignore[no-untyped-def]
        return [
            LlmSourceCandidateDecision(candidate_id=cand.id, decision="ignore", score=10, quality_score=10, relevance_score=5, novelty_score=0, why="noise"),
        ]

    monkeypatch.setattr("tracker.runner.RssConnector.fetch", fake_fetch)
    monkeypatch.setattr("tracker.runner.llm_decide_source_candidates", fake_decide)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="gpt-5.2",
        discover_sources_auto_accept_enabled=True,
        discover_sources_auto_accept_max_per_topic=1,
        discover_sources_auto_accept_preview_entries=1,
    )
    asyncio.run(run_discover_sources(session=db_session, settings=settings, topic_ids=[topic.id]))

    cand_row = repo.get_source_candidate_by_id(cand.id)
    assert cand_row is not None
    assert cand_row.status == "ignored"

    sources = repo.list_sources()
    assert not any(s.url == "https://example.com/feed.xml" for s in sources)


def test_run_discover_sources_marks_unpreviewable_candidates_ignored(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="keep signals")

    cand, _created = repo.add_source_candidate(
        topic_id=topic.id,
        source_type="rss",
        url="https://example.com/feed.xml",
        discovered_from_url="https://example.com/blog/",
    )

    async def fake_fetch(self, *, url: str):  # type: ignore[no-untyped-def]
        return []

    async def fake_decide(*, settings, topic, policy_prompt, candidates, max_accept, usage_cb=None):  # type: ignore[no-untyped-def]
        raise AssertionError("llm_decide_source_candidates should not be called for empty-preview candidates")

    monkeypatch.setattr("tracker.runner.RssConnector.fetch", fake_fetch)
    monkeypatch.setattr("tracker.runner.llm_decide_source_candidates", fake_decide)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="gpt-5.2",
        discover_sources_auto_accept_enabled=True,
        discover_sources_auto_accept_max_per_topic=1,
        discover_sources_auto_accept_preview_entries=1,
    )
    asyncio.run(run_discover_sources(session=db_session, settings=settings, topic_ids=[topic.id]))

    cand_row = repo.get_source_candidate_by_id(cand.id)
    assert cand_row is not None
    assert cand_row.status == "ignored"

    ev = repo.get_source_candidate_eval(candidate_id=cand.id)
    assert ev is not None
    assert ev.decision == "ignore"
    assert "空内容候选" in (ev.why or "")
