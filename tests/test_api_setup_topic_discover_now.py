from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


@dataclass(frozen=True)
class _Row:
    topic_name: str
    pages_checked: int
    candidates_created: int
    candidates_found: int
    errors: int


@dataclass(frozen=True)
class _Res:
    per_topic: list[_Row]


def test_setup_topic_can_run_discover_sources_now(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    async def fake_run_discover_sources(*, session, settings, topic_ids=None):  # type: ignore[no-untyped-def]
        assert topic_ids == [1]
        return _Res(per_topic=[_Row(topic_name="T", pages_checked=1, candidates_created=2, candidates_found=3, errors=0)])

    monkeypatch.setattr("tracker.api.run_discover_sources", fake_run_discover_sources)

    r = client.post(
        "/setup/topic/apply?token=secret",
        data={
            "name": "T",
            "query": "gpu",
            "digest_cron": "0 9 * * *",
            "add_hn": "true",
            "add_searxng": "true",
            "searxng_base_url": "http://127.0.0.1:8888",
            "run_discover_sources_now": "true",
            "ai_enabled": "true",
            "ai_prompt": "pick only signals",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303
    loc = r.headers.get("location") or ""
    assert "discovered+feeds" in loc
    assert "created%3D2" in loc

