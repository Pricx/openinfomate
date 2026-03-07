from __future__ import annotations

from pathlib import Path
import warnings

from bs4 import XMLParsedAsHTMLWarning

from tracker.feed_discovery import discover_feed_urls_from_html


def test_discover_feed_urls_resolves_relative_urls():
    fixture = Path(__file__).with_name("fixtures").joinpath("feed_discovery_sample.html")
    html = fixture.read_text(encoding="utf-8")

    urls = discover_feed_urls_from_html(page_url="https://example.com/blog/", html=html)
    assert urls == ["https://example.com/atom.xml", "https://example.com/feed.xml"]


def test_discover_feed_urls_filters_comment_feeds_by_title():
    html = """
    <html>
      <head>
        <link rel="alternate" type="application/rss+xml" title="Site Feed" href="/feed.xml" />
        <link rel="alternate" type="application/rss+xml" title="Comments Feed" href="/some-post/feed/" />
      </head>
    </html>
    """
    urls = discover_feed_urls_from_html(page_url="https://example.com/blog/", html=html)
    assert urls == ["https://example.com/feed.xml"]



def test_discover_feed_urls_on_xml_feed_document_is_warning_free():
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>Example Feed</title>
      </channel>
    </rss>
    """

    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        urls = discover_feed_urls_from_html(page_url="https://example.com/feed.xml", html=xml)

    assert urls == []
    assert not [w for w in seen if issubclass(w.category, XMLParsedAsHTMLWarning)]
