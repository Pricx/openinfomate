from __future__ import annotations


def test_normalize_searxng_search_url_collapses_repeated_search_segments():
    from tracker.connectors.searxng import normalize_searxng_search_url

    u = "http://127.0.0.1:8888/search/search?q=ai+memory&format=json"
    assert normalize_searxng_search_url(u) == "http://127.0.0.1:8888/search?q=ai+memory&format=json"


def test_normalize_searxng_search_url_preserves_subpath():
    from tracker.connectors.searxng import normalize_searxng_search_url

    u = "https://example.com/searxng/search/search?q=test&format=json"
    assert normalize_searxng_search_url(u) == "https://example.com/searxng/search?q=test&format=json"

