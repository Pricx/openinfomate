from tracker.normalize import canonicalize_url, html_to_text, normalize_text, sha256_hex


def test_canonicalize_url_strips_tracking_and_fragment():
    url = "https://Example.com/path?a=1&utm_source=x&b=2#section"
    assert canonicalize_url(url) == "https://example.com/path?a=1&b=2"

def test_canonicalize_url_normalizes_www_default_ports_and_trailing_slash():
    url = "http://www.Example.com:80/path/?utm_medium=x"
    assert canonicalize_url(url) == "https://example.com/path"


def test_canonicalize_url_drops_https_default_port():
    url = "https://example.com:443/a?b=1"
    assert canonicalize_url(url) == "https://example.com/a?b=1"


def test_normalize_text_collapses_whitespace():
    assert normalize_text("  a\tb\nc  ") == "a b c"


def test_sha256_hex_is_stable():
    assert sha256_hex("x") == sha256_hex("x")


def test_html_to_text_strips_tags():
    assert html_to_text("<p>Hello <b>world</b></p>") == "Hello world"
