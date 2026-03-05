from __future__ import annotations

from tracker.http_auth import cookie_header_for_url, parse_cookie_jar_json


def test_cookie_jar_parses_and_matches_longest_suffix():
    jar = parse_cookie_jar_json(
        '{"example.com":"A=1; B=2","sub.example.com":"C=3","https://foo.bar/x":"D=4",".github.com":"E=5"}'
    )

    assert cookie_header_for_url(url="https://sub.example.com/path", cookie_jar=jar) == "C=3"
    assert cookie_header_for_url(url="https://www.example.com/path", cookie_jar=jar) == "A=1; B=2"
    assert cookie_header_for_url(url="https://foo.bar/zzz", cookie_jar=jar) == "D=4"
    assert cookie_header_for_url(url="https://raw.github.com/whatever", cookie_jar=jar) == "E=5"


def test_cookie_jar_invalid_json_is_ignored():
    assert parse_cookie_jar_json("not json") == {}

