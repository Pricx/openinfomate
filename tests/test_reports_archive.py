from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from tracker.api import create_app
from tracker.cli import app as cli_app
from tracker.db import session_factory
from tracker.models import Base, Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_digest, run_health_report
from tracker.settings import Settings


def test_runner_persists_digest_and_health_reports(tmp_path):
    db_path = Path(tmp_path) / "test.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai", digest_cron="0 9 * * *")
        source = repo.add_source(type="rss", url="file:///tmp/test.xml")

        now = dt.datetime.utcnow()
        item = Item(
            source_id=source.id,
            url="https://example.com/x",
            canonical_url="https://example.com/x",
            title="AI news",
            published_at=now,
            content_text="ai",
            content_hash="0" * 64,
            simhash64=0,
            created_at=now,
        )
        session.add(item)
        session.commit()

        it = ItemTopic(
            item_id=item.id,
            topic_id=topic.id,
            decision="digest",
            reason="matched",
            created_at=now,
        )
        session.add(it)
        session.commit()

        asyncio.run(run_digest(session=session, settings=settings, hours=24, push=False))
        asyncio.run(run_health_report(session=session, settings=settings, push=False))

        digests = repo.list_reports(kind="digest", limit=10)
        assert len(digests) >= 1
        assert digests[0][0].kind == "digest"

        health = repo.list_reports(kind="health", limit=10)
        assert len(health) >= 1
        assert health[0][0].kind == "health"


def test_api_reports_endpoints(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    app = create_app(settings)
    client = TestClient(app)
    headers = {"x-tracker-token": "secret"}

    # Create a topic so digest runs create a per-topic report.
    r = client.post("/topics", headers=headers, json={"name": "T", "query": "ai"})
    assert r.status_code == 200

    r = client.post("/run/digest", headers=headers, params={"push": "false"})
    assert r.status_code == 200

    r = client.post("/run/health", headers=headers, params={"push": "false"})
    assert r.status_code == 200

    r = client.get("/reports", headers=headers, params={"limit": "50"})
    assert r.status_code == 200
    data = r.json()
    assert any(row["kind"] == "digest" for row in data)
    assert any(row["kind"] == "health" for row in data)

    rid = data[0]["id"]
    r = client.get(f"/reports/{rid}", headers=headers)
    assert r.status_code == 200
    assert "markdown" in r.json()

    # Admin UI renders reports section.
    r = client.get("/admin?token=secret")
    assert r.status_code == 200
    assert "Reports" in r.text


def test_cli_report_list_and_show(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(cli_app, ["db", "init"])
    assert init.exit_code == 0

    r = runner.invoke(cli_app, ["topic", "add", "--name", "T", "--query", "ai"])
    assert r.exit_code == 0

    # Running a digest persists a report even if there are no items.
    r = runner.invoke(cli_app, ["run", "digest", "--hours", "24"])
    assert r.exit_code == 0

    r = runner.invoke(cli_app, ["report", "list", "--kind", "digest", "--json", "--with-markdown"])
    assert r.exit_code == 0
    reports = json.loads(r.stdout)
    assert len(reports) >= 1
    rid = reports[0]["id"]
    assert reports[0]["kind"] == "digest"
    assert "markdown" in reports[0]

    r = runner.invoke(cli_app, ["report", "show", str(rid)])
    assert r.exit_code == 0
    assert "参考消息" in r.stdout
