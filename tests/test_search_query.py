from __future__ import annotations

from tracker.search_query import normalize_search_query, rewrite_query_param


def test_normalize_search_query_turns_commas_into_spaces():
    assert normalize_search_query("a,b,c") == "a b c"
    assert normalize_search_query(" a, b ,  c ") == "a b c"
    assert normalize_search_query("gpu asic") == "gpu asic"


def test_rewrite_query_param_rewrites_only_when_needed():
    url = "https://example.com/search?query=a%2Cb&x=1"
    out = rewrite_query_param(url=url, param="query")
    assert "query=a+b" in out
    assert "x=1" in out

    # Unrelated param: no changes
    same = rewrite_query_param(url=url, param="q")
    assert same == url

