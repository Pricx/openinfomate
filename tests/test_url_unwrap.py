from __future__ import annotations

from tracker.url_unwrap import unwrap_tracking_url, unwrap_urls_in_markdown


def test_unwrap_tracking_url_bing_ck():
    u = (
        "https://www.bing.com/ck/a?!&&p=0961e9d29a29e81aff6a70eafd4b84ab599169975be67ddc6aaf093421b3d493"
        "JmltdHM9MTc3MTIwMDAwMA&ptn=3&ver=2&hsh=4&fclid=3d730b8a-a820-6177-064e-1c88a99d6042"
        "&u=a1aHR0cHM6Ly93d3cuc29mdGJhbmsuanAv&ntb=1"
    )
    assert unwrap_tracking_url(u) == "https://www.softbank.jp/"


def test_unwrap_tracking_url_duckduckgo_l():
    u = "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fa%3Fx%3D1%26y%3D2"
    assert unwrap_tracking_url(u) == "https://example.com/a?x=1&y=2"


def test_unwrap_tracking_url_google_url():
    u = "https://www.google.com/url?q=https%3A%2F%2Fexample.com%2Fdoc%3Fa%3Db"
    assert unwrap_tracking_url(u) == "https://example.com/doc?a=b"


def test_unwrap_urls_in_markdown_rewrites_links():
    md = (
        "Sources:\n"
        "- https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fdoc\n"
        "- https://www.bing.com/ck/a?!&&u=a1aHR0cHM6Ly93d3cuc29mdGJhbmsuanAv&ntb=1\n"
    )
    out = unwrap_urls_in_markdown(md)
    assert "duckduckgo.com/l/?" not in out
    assert "bing.com/ck/" not in out
    assert "https://example.com/doc" in out
    assert "https://www.softbank.jp/" in out

