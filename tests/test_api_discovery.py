from __future__ import annotations

from tracker.api_discovery import discover_api_urls_from_html


def test_discover_api_urls_from_html_extracts_common_patterns():
    html = """
    <html>
      <head>
        <script>fetch("/api/posts.json");</script>
        <script>const u = "https://example.com/graphql";</script>
        <link rel="preload" href="/api/data.json" />
      </head>
      <body>
        <a href="/api/v1/items">items</a>
        <a href="/about">about</a>
      </body>
    </html>
    """

    urls = discover_api_urls_from_html(page_url="https://example.com/blog/", html=html)
    assert urls == [
        "https://example.com/api/data.json",
        "https://example.com/api/posts.json",
        "https://example.com/api/v1/items",
        "https://example.com/graphql",
    ]
