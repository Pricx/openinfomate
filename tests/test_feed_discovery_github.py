from tracker.feed_discovery import discover_feed_urls_from_html


def test_discover_feed_urls_ignores_github_commits_atom():
    html = """
    <html>
      <head>
        <link rel="alternate" type="application/atom+xml" title="Atom" href="/acme/widgets/commits/main.atom" />
      </head>
      <body>ok</body>
    </html>
    """
    urls = discover_feed_urls_from_html(page_url="https://github.com/acme/widgets", html=html)
    assert urls == []

