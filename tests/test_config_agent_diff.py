from __future__ import annotations


from tracker.config_agent import diff_tracking_snapshots


def test_diff_tracking_snapshots_includes_binding_source_details():
    before = {
        "topics": [{"name": "Profile", "query": "", "enabled": True}],
        "sources": [
            {"type": "discourse", "url": "https://forum.example.com/latest.json", "enabled": True},
            {
                "type": "searxng_search",
                "url": "http://127.0.0.1:8888/search?q=site%3Alinux.do+codex+fast&format=json&time_range=week&results=10",
                "enabled": True,
            },
        ],
        "bindings": [],
    }
    after = {
        **before,
        "bindings": [
            {
                "topic": "Profile",
                "source": {"type": "discourse", "url": "https://forum.example.com/latest.json"},
                "include_keywords": "",
                "exclude_keywords": "",
            },
            {
                "topic": "Profile",
                "source": {
                    "type": "searxng_search",
                    "url": "http://127.0.0.1:8888/search?q=site%3Alinux.do+codex+fast&format=json&time_range=week&results=10",
                },
                "include_keywords": "",
                "exclude_keywords": "",
            },
        ],
    }

    diff = diff_tracking_snapshots(before=before, after=after)
    assert "Profile<=discourse https://forum.example.com/latest.json" in diff
    assert "Profile<=search: site:linux.do codex fast" in diff

