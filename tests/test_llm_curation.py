from __future__ import annotations

import asyncio
import datetime as dt

from sqlalchemy import select

from tracker.connectors.base import FetchedEntry
from tracker.llm import LlmCurationDecision
from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_digest, run_tick
from tracker.settings import Settings
from tracker.simhash import int_to_signed64, simhash64


def test_llm_curation_digest_curates_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")

    now = dt.datetime.utcnow()
    items = [
        Item(
            source_id=source.id,
            url="https://example.com/a",
            canonical_url="https://example.com/a",
            title="Signal A",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/b",
            canonical_url="https://example.com/b",
            title="Urgent B",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/c",
            canonical_url="https://example.com/c",
            title="Noise C",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
    ]
    for it in items:
        db_session.add(it)
        db_session.flush()
        db_session.add(ItemTopic(item_id=it.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        out: list[LlmCurationDecision] = []
        for c in candidates:
            title = str(c.get("title") or "")
            if "Urgent" in title:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="alert", why="urgent", summary="s"))
            elif "Noise" in title:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="ignore", why="noise", summary=""))
            else:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="signal", summary="s"))
        return out

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    md = result.per_topic[0].markdown
    assert "Signal A" in md
    assert "Urgent B" in md
    assert "Noise C" not in md

    rows = list(db_session.scalars(select(ItemTopic).order_by(ItemTopic.item_id)))
    assert [r.decision for r in rows] == ["digest", "alert", "ignore"]


def test_llm_curation_digest_fails_open_with_fallback_digest_on_error(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")

    now = dt.datetime.utcnow()
    items = [
        Item(
            source_id=source.id,
            url="https://example.com/new",
            canonical_url="https://example.com/new",
            title="New A",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/mid",
            canonical_url="https://example.com/mid",
            title="Mid B",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now - dt.timedelta(minutes=10),
        ),
        Item(
            source_id=source.id,
            url="https://example.com/old",
            canonical_url="https://example.com/old",
            title="Old C",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now - dt.timedelta(minutes=20),
        ),
    ]
    for it in items:
        db_session.add(it)
        db_session.flush()
        db_session.add(ItemTopic(item_id=it.id, topic_id=topic.id, decision="candidate", reason="", created_at=it.created_at))
    db_session.commit()

    async def bad_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        raise RuntimeError("llm is down")

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", bad_curate)

    async def fake_triage(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_keep, usage_cb=None
    ):
        # AI-only fail-open: return a ranked list of item_ids to keep.
        return [items[0].id, items[1].id]

    monkeypatch.setattr("tracker.runner.llm_triage_topic_items", fake_triage)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        llm_curation_fail_open=True,
        llm_curation_fail_open_max_digest=2,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    md = result.per_topic[0].markdown
    assert "New A" in md
    assert "Mid B" in md
    assert "Old C" not in md

    rows = list(db_session.scalars(select(ItemTopic)))
    assert sum(1 for r in rows if r.decision == "digest") == 2


def test_llm_curation_digest_dedupes_near_duplicate_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")

    now = dt.datetime.utcnow()
    items = [
        Item(
            source_id=source.id,
            url="https://example.com/a",
            canonical_url="https://example.com/a",
            title="Dup A",
            content_text="alpha",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/b",
            canonical_url="https://example.com/b",
            title="Dup B",
            content_text="alpha",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/c",
            canonical_url="https://example.com/c",
            title="Unique C",
            content_text="charlie",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
    ]
    for it in items:
        db_session.add(it)
        db_session.flush()
        db_session.add(ItemTopic(item_id=it.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        # Ensure topic-level anti-dup removes near-identical candidates before LLM call.
        assert sum(1 for c in candidates if (c.get("snippet") or "").strip() == "alpha") == 1
        return [
            LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="ok", summary="s")
            for c in candidates
        ]

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        llm_curation_max_candidates=10,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    md = result.per_topic[0].markdown
    assert "Unique C" in md


def test_llm_curation_digest_dedupes_against_recent_history(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")

    now = dt.datetime.utcnow()
    old_when = now - dt.timedelta(days=2)
    sh = int_to_signed64(simhash64("alpha"))

    old_digest = Item(
        source_id=source.id,
        url="https://example.com/old",
        canonical_url="https://example.com/old",
        title="Old Digest",
        content_text="alpha",
        content_hash="",
        simhash64=sh,
        created_at=old_when,
    )
    new_dup = Item(
        source_id=source.id,
        url="https://example.com/new-dup",
        canonical_url="https://example.com/new-dup",
        title="New Dup",
        content_text="alpha",
        content_hash="",
        simhash64=sh,
        created_at=now,
    )
    new_unique = Item(
        source_id=source.id,
        url="https://example.com/unique",
        canonical_url="https://example.com/unique",
        title="Unique",
        content_text="charlie",
        content_hash="",
        simhash64=int_to_signed64(simhash64("charlie")),
        created_at=now,
    )
    for it in [old_digest, new_dup, new_unique]:
        db_session.add(it)
        db_session.flush()
    db_session.add(ItemTopic(item_id=old_digest.id, topic_id=topic.id, decision="digest", reason="", created_at=old_when))
    db_session.add(ItemTopic(item_id=new_dup.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.add(ItemTopic(item_id=new_unique.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        # Ensure history-based anti-dup filters out repeats before LLM call.
        assert recent_sent is not None
        assert any(str(r.get("url") or "") == "https://example.com/old" for r in (recent_sent or []))
        assert all((str(c.get("snippet") or "").strip() != "alpha") for c in candidates)
        return [
            LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="ok", summary="s")
            for c in candidates
        ]

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        llm_curation_max_candidates=10,
        llm_curation_history_dedupe_days=7,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    md = result.per_topic[0].markdown
    assert "Unique" in md


def test_llm_curation_digest_story_dedupes_by_notable_links(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")

    now = dt.datetime.utcnow()
    same_title = "New tool: foo/bar"
    a = ("alpha " * 200) + " https://github.com/foo/bar "
    b = ("bravo " * 200) + " https://github.com/foo/bar "

    items = [
        Item(
            source_id=source.id,
            url="https://example.com/a",
            canonical_url="https://example.com/a",
            title=same_title,
            content_text=a,
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/b",
            canonical_url="https://example.com/b",
            title=same_title,
            content_text=b,
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
    ]
    for it in items:
        db_session.add(it)
        db_session.flush()
        db_session.add(ItemTopic(item_id=it.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        # Story-level anti-dup: even if excerpts differ a lot, keep only one entry for the same repo/story.
        assert len(candidates) == 1
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="ok", summary="s")]

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        llm_curation_max_candidates=10,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    assert same_title in result.per_topic[0].markdown


def test_llm_curation_tick_curates_new_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick urgent only")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.upsert_source_score(source_id=source.id, score=90, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(url="https://example.com/a", title="Signal A", summary="alpha " * 30),
            FetchedEntry(url="https://example.com/b", title="Urgent B", summary="bravo " * 30),
            FetchedEntry(url="https://example.com/c", title="Noise C", summary="charlie " * 30),
        ]

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        out: list[LlmCurationDecision] = []
        for c in candidates:
            title = str(c.get("title") or "")
            if "Urgent" in title:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="alert", why="urgent", summary="s"))
            elif "Noise" in title:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="ignore", why="noise", summary=""))
            else:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="signal", summary="s"))
        return out

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 3
    assert result.total_pushed_alerts == 0

    rows = list(db_session.scalars(select(ItemTopic).order_by(ItemTopic.item_id)))
    # tick-time: digest decisions should remain as candidate (so daily digest can cap once/day)
    assert sorted([r.decision for r in rows]) == ["alert", "candidate", "ignore"]


def test_llm_curation_tick_does_not_slice_when_triage_fails(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick urgent only")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.upsert_source_score(source_id=source.id, score=90, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(url="https://example.com/a", title="Signal A", summary="alpha0 " * 20),
            FetchedEntry(url="https://example.com/b", title="Signal B", summary="alpha1 " * 20),
            FetchedEntry(url="https://example.com/c", title="Signal C", summary="alpha2 " * 20),
            FetchedEntry(url="https://example.com/d", title="Signal D", summary="alpha3 " * 20),
            FetchedEntry(url="https://example.com/e", title="Signal E", summary="alpha4 " * 20),
        ]

    async def bad_triage(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_keep, usage_cb=None
    ):
        raise RuntimeError("triage down")

    seen: dict[str, int] = {}

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        seen["n"] = len(candidates)
        return [LlmCurationDecision(item_id=int(c["item_id"]), decision="ignore", why="", summary="") for c in candidates]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_triage_topic_items", bad_triage)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_model_mini="mini",
        llm_curation_enabled=True,
        llm_curation_max_candidates=2,
        llm_curation_triage_enabled=True,
        llm_curation_triage_pool_max_candidates=5,
    )

    asyncio.run(run_tick(session=db_session, settings=settings, push=False))

    # Triaging failed; pass the full bounded pool to the reasoning model (no deterministic slice).
    assert seen.get("n") == 5


def test_llm_curation_tick_drains_uncurated_backlog(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick urgent only")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.upsert_source_score(source_id=source.id, score=90, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)

    # Backfilled / missed item: still "candidate" and never curated.
    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/backfill",
        canonical_url="https://example.com/backfill",
        title="Introducing GPT-5.3-Codex-Spark",
        content_text="",
        content_hash="",
        simhash64=0,
        created_at=now - dt.timedelta(days=2),
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(
        ItemTopic(
            item_id=item.id,
            topic_id=topic.id,
            decision="candidate",
            reason="backfill: filtered by include_keywords (prefilter disabled)",
            created_at=item.created_at,
        )
    )
    db_session.commit()

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, **kwargs):  # noqa: ARG001
        return []

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        # Promote the backlog item as an alert.
        return [LlmCurationDecision(item_id=int(item.id), decision="alert", why="urgent", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 0

    it_row = repo.get_item_topic(item_id=item.id, topic_id=topic.id)
    assert it_row is not None
    assert it_row.decision == "alert"
    assert "llm_summary:" in (it_row.reason or "")


def test_llm_curation_tick_drains_backlog_even_when_new_pool_is_full(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick urgent only")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.upsert_source_score(source_id=source.id, score=90, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)

    # Backfilled / missed item: still "candidate" and never curated.
    now = dt.datetime.utcnow()
    backlog_item = Item(
        source_id=source.id,
        url="https://example.com/backlog",
        canonical_url="https://example.com/backlog",
        title="Introducing GPT-5.3-Codex-Spark",
        content_text="",
        content_hash="",
        simhash64=0,
        created_at=now - dt.timedelta(days=2),
    )
    db_session.add(backlog_item)
    db_session.flush()
    db_session.add(
        ItemTopic(
            item_id=backlog_item.id,
            topic_id=topic.id,
            decision="candidate",
            reason="llm curation candidate",
            created_at=backlog_item.created_at,
        )
    )
    db_session.commit()

    # Plenty of new candidates (enough to fill llm_curation_max_candidates), so backlog would starve
    # unless tick reserves a small slot to drain it.
    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, **kwargs):  # noqa: ARG001
        return [
            FetchedEntry(url="https://example.com/a", title="Signal A", summary="alpha0 " * 20),
            FetchedEntry(url="https://example.com/b", title="Signal B", summary="alpha1 " * 20),
            FetchedEntry(url="https://example.com/c", title="Signal C", summary="alpha2 " * 20),
        ]

    seen_ids: list[int] = []

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        nonlocal seen_ids
        seen_ids = [int(c["item_id"]) for c in candidates]
        out = []
        for c in candidates:
            iid = int(c["item_id"])
            if iid == int(backlog_item.id):
                out.append(LlmCurationDecision(item_id=iid, decision="alert", why="urgent", summary="s"))
            else:
                out.append(LlmCurationDecision(item_id=iid, decision="ignore", why="", summary=""))
        return out

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        llm_curation_max_candidates=2,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 3

    it_row = repo.get_item_topic(item_id=backlog_item.id, topic_id=topic.id)
    assert it_row is not None
    assert it_row.decision == "alert"
    assert "llm_summary:" in (it_row.reason or "")
    assert int(backlog_item.id) in seen_ids


def test_llm_curation_digest_curates_existing_digest_decisions(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")

    now = dt.datetime.utcnow()
    items = [
        Item(
            source_id=source.id,
            url="https://example.com/a",
            canonical_url="https://example.com/a",
            title="Signal A",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/b",
            canonical_url="https://example.com/b",
            title="Urgent B",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
        Item(
            source_id=source.id,
            url="https://example.com/c",
            canonical_url="https://example.com/c",
            title="Noise C",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now,
        ),
    ]
    for it in items:
        db_session.add(it)
        db_session.flush()
        # Legacy heuristic decisions: already marked as digest.
        db_session.add(ItemTopic(item_id=it.id, topic_id=topic.id, decision="digest", reason="", created_at=now))
    db_session.commit()

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        out: list[LlmCurationDecision] = []
        for c in candidates:
            title = str(c.get("title") or "")
            if "Urgent" in title:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="alert", why="urgent", summary="s"))
            elif "Noise" in title:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="ignore", why="noise", summary=""))
            else:
                out.append(LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="signal", summary="s"))
        return out

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    md = result.per_topic[0].markdown
    assert "Signal A" in md
    assert "Urgent B" in md
    assert "Noise C" not in md

    rows = list(db_session.scalars(select(ItemTopic).order_by(ItemTopic.item_id)))
    assert [r.decision for r in rows] == ["digest", "alert", "ignore"]
