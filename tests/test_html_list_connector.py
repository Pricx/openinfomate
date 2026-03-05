from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.connectors.html_list import HtmlListConnector, build_html_list_url
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_html_list_connector_file_url_extracts_links():
    fixture = Path(__file__).with_name("fixtures").joinpath("html_list_sample.html").resolve()
    page_url = fixture.as_uri()

    source_url = build_html_list_url(
        page_url=page_url,
        item_selector=".posts li",
        title_selector="a.post-link",
        summary_selector="p.summary",
        max_items=10,
    )

    entries = asyncio.run(HtmlListConnector(timeout_seconds=5).fetch(url=source_url))
    assert [e.title for e in entries] == ["First Post", "Second Post", "Third Post"]
    assert entries[0].url == "https://example.com/a"
    # Relative URLs are resolved against the page URL.
    assert entries[1].url.startswith("file://")


def test_html_list_connector_prefers_title_selector_anchor_over_first_link():
    fixture = Path(__file__).with_name("fixtures").joinpath("html_list_multi_anchor.html").resolve()
    page_url = fixture.as_uri()

    source_url = build_html_list_url(
        page_url=page_url,
        item_selector=".posts li",
        title_selector="h2 a.post-link",
        summary_selector="p.summary",
        max_items=10,
    )

    entries = asyncio.run(HtmlListConnector(timeout_seconds=5).fetch(url=source_url))
    assert [e.title for e in entries] == ["First Post", "Second Post"]
    assert entries[0].url == "https://example.com/a"
    assert "sponsor" not in entries[0].url
    assert "sponsor" not in entries[1].url


def test_run_tick_ingests_html_list_source(db_session):
    fixture = Path(__file__).with_name("fixtures").joinpath("html_list_sample.html").resolve()
    page_url = fixture.as_uri()
    source_url = build_html_list_url(
        page_url=page_url,
        item_selector=".posts li",
        title_selector="a.post-link",
        summary_selector="p.summary",
        max_items=10,
    )

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="post")
    source = repo.add_source(type="html_list", url=source_url)
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings()
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 3
