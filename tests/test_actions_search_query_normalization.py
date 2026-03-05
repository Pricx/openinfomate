from __future__ import annotations

from tracker.actions import SourceBindingSpec, create_hn_search_source, create_searxng_search_source
from tracker.repo import Repo


def test_search_query_commas_are_normalized_for_search_sources(db_session):
    repo = Repo(db_session)
    repo.add_topic(name="T", query="x")

    hn = create_hn_search_source(
        session=db_session,
        query="forum.example.com,discourse,hackernews",
        bind=SourceBindingSpec(topic="T"),
    )
    assert "query=forum.example.com+discourse+hackernews" in hn.url
    assert "query=forum.example.com%2Cdiscourse%2Chackernews" not in hn.url

    sx = create_searxng_search_source(
        session=db_session,
        base_url="http://127.0.0.1:8888",
        query="a,b",
        bind=SourceBindingSpec(topic="T"),
    )
    assert "q=a+b" in sx.url
    assert "q=a%2Cb" not in sx.url
