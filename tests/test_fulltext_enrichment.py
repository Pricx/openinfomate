from __future__ import annotations

import asyncio
import datetime as dt

from sqlalchemy import select

from tracker.connectors.base import FetchedEntry
from tracker.fulltext import _extract_text_from_arxiv_abs_html
from tracker.llm import LlmCurationDecision
from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_digest, run_tick
from tracker.settings import Settings


def test_run_digest_fulltext_enriches_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/a",
        canonical_url="https://example.com/a",
        title="Signal A",
        content_text="short snippet",
        content_hash="",
        simhash64=0,
        created_at=now,
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        assert url == "https://example.com/a"
        assert discourse_cookie is None or isinstance(discourse_cookie, str)
        assert cookie_header is None or isinstance(cookie_header, str)
        return "FULL TEXT " * 200

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        assert candidates and "FULL TEXT" in str(candidates[0].get("snippet") or "")
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="w", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=5,
    )

    asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    content = repo.get_item_content(item_id=item.id)
    assert content is not None
    assert "FULL TEXT" in content.content_text


def test_run_tick_fulltext_enriches_candidates_before_llm_curation(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [FetchedEntry(url="https://example.com/a", title="Signal A", summary="short snippet")]

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        assert url == "https://example.com/a"
        assert discourse_cookie is None or isinstance(discourse_cookie, str)
        assert cookie_header is None or isinstance(cookie_header, str)
        return "FULL TEXT " * 200

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        assert candidates and "FULL TEXT" in str(candidates[0].get("snippet") or "")
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="w", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=5,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 1

    item = db_session.scalar(select(Item).order_by(Item.id.desc()))
    assert item is not None
    content = repo.get_item_content(item_id=item.id)
    assert content is not None
    assert "FULL TEXT" in content.content_text


def test_run_tick_skips_fulltext_for_linux_do(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [FetchedEntry(url="https://linux.do/t/topic/1", title="Signal A", summary="short snippet")]

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        raise AssertionError("fulltext fetch should be skipped for linux.do")

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        assert candidates
        assert str(candidates[0].get("snippet") or "") == "short snippet"
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="w", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=5,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 1

    item = db_session.scalar(select(Item).order_by(Item.id.desc()))
    assert item is not None
    assert repo.get_item_content(item_id=item.id) is None


def test_run_tick_attempts_fulltext_for_nodeseek(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [FetchedEntry(url="https://www.nodeseek.com/post-1-1", title="Signal A", summary="short snippet")]

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        assert url == "https://www.nodeseek.com/post-1-1"
        return "FULL TEXT FROM NODESEEK"

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        assert candidates
        assert "FULL TEXT FROM NODESEEK" in str(candidates[0].get("snippet") or "")
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="w", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=5,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 1

    item = db_session.scalar(select(Item).order_by(Item.id.desc()))
    assert item is not None
    content = repo.get_item_content(item_id=item.id)
    assert content is not None
    assert content.content_text == "FULL TEXT FROM NODESEEK"


def test_extract_text_from_arxiv_abs_html_prefers_abstract():
    html = """
    <html>
      <head>
        <meta name="citation_title" content="Test-Time Reasoning for Agents" />
      </head>
      <body>
        <h1 class="title mathjax">Title: Test-Time Reasoning for Agents</h1>
        <blockquote class="abstract mathjax">
          Abstract: We study whether agent systems benefit from explicit abstract-aware filtering.
        </blockquote>
      </body>
    </html>
    """
    text = _extract_text_from_arxiv_abs_html(html=html)
    assert "Title: Test-Time Reasoning for Agents" in text
    assert "Abstract: We study whether agent systems benefit from explicit abstract-aware filtering." in text


def test_run_tick_force_fetches_fulltext_for_arxiv_even_when_budget_zero(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="agent")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=arxiv")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(
                url="https://arxiv.org/abs/2604.12345",
                title="Generic arXiv title",
                summary="short rss snippet",
            )
        ]

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        assert url == "https://arxiv.org/abs/2604.12345"
        return "Title: Generic arXiv title Abstract: This abstract proves the paper is actually useful."

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, repo=None, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        assert candidates
        assert "This abstract proves the paper is actually useful." in str(candidates[0].get("snippet") or "")
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="w", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=0,
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 1

    item = db_session.scalar(select(Item).order_by(Item.id.desc()))
    assert item is not None
    content = repo.get_item_content(item_id=item.id)
    assert content is not None
    assert "This abstract proves the paper is actually useful." in content.content_text


def test_run_digest_retries_fulltext_after_previous_failure(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/a",
        canonical_url="https://example.com/a",
        title="Signal A",
        content_text="short snippet",
        content_hash="",
        simhash64=0,
        created_at=now,
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()
    repo.upsert_item_content(item_id=item.id, url=item.url, content_text="", error="temporary block")

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        return "FULL TEXT AFTER RETRY"

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        assert candidates and "FULL TEXT AFTER RETRY" in str(candidates[0].get("snippet") or "")
        return [LlmCurationDecision(item_id=int(candidates[0]["item_id"]), decision="digest", why="w", summary="s")]

    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=5,
    )

    asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    content = repo.get_item_content(item_id=item.id)
    assert content is not None
    assert content.content_text == "FULL TEXT AFTER RETRY"


def test_run_digest_fulltext_budget_counts_failed_attempts(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    for index in range(3):
        item = Item(
            source_id=source.id,
            url=f"https://example.com/{index}",
            canonical_url=f"https://example.com/{index}",
            title=f"Signal {index}",
            content_text="short snippet",
            content_hash="",
            simhash64=index,
            created_at=now - dt.timedelta(minutes=index),
        )
        db_session.add(item)
        db_session.flush()
        db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="candidate", reason="", created_at=item.created_at))
    db_session.commit()

    calls = {"n": 0}

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        calls["n"] += 1
        raise RuntimeError("temporary block")

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        return [
            LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="w", summary="s")
            for c in candidates
        ]

    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=1,
    )

    asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    assert calls["n"] == 1


def test_run_tick_fulltext_budget_counts_failed_attempts(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(url="https://example.com/a", title="Signal A", summary="short snippet"),
            FetchedEntry(url="https://example.com/b", title="Signal B", summary="short snippet"),
            FetchedEntry(url="https://example.com/c", title="Signal C", summary="short snippet"),
        ]

    calls = {"n": 0}

    async def fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        calls["n"] += 1
        raise RuntimeError("temporary block")

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        return [
            LlmCurationDecision(item_id=int(c["item_id"]), decision="digest", why="w", summary="s")
            for c in candidates
        ]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.fetch_fulltext_for_url", fake_fetch_fulltext_for_url)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=1,
    )

    asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert calls["n"] == 1
