from __future__ import annotations

from tracker.connectors.searxng import normalize_searxng_base_url


def test_normalize_searxng_base_url_strips_repeated_search_segments():
    assert normalize_searxng_base_url("http://127.0.0.1:8888/search") == "http://127.0.0.1:8888"
    assert normalize_searxng_base_url("http://127.0.0.1:8888/search/search") == "http://127.0.0.1:8888"
    assert (
        normalize_searxng_base_url("https://example.com/searxng/search/search")
        == "https://example.com/searxng"
    )

