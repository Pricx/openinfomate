from __future__ import annotations

import json

from tracker.config_io import export_config, import_config
from tracker.repo import Repo


def test_export_import_roundtrip(db_session, tmp_path):
    repo = Repo(db_session)
    t1 = repo.add_topic(name="T1", query="x", digest_cron="0 9 * * *")
    t1.alert_keywords = "boom"
    db_session.commit()

    s1 = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.update_source_meta(source_id=s1.id, tags="a,b", notes="n1")
    repo.bind_topic_source(topic=t1, source=s1)

    repo.upsert_topic_policy(topic_id=t1.id, llm_curation_enabled=True, llm_curation_prompt="PROMPT")
    repo.set_app_config("profile_topic_name", "Profile")
    repo.set_app_config("profile_text", "BOOKMARKS:\n- x | https://example.com")
    repo.set_app_config("prompt_templates_custom_json", '{"version":1,"templates":{"t1":{"title":"T1","text":{"zh":"ZH","en":"EN"}}}}')
    repo.set_app_config("prompt_template_bindings_json", '{"version":1,"bindings":{"llm.curate_items.system":"t1"}}')

    cfg = export_config(session=db_session)
    assert cfg["version"] == 2
    assert cfg["topics"][0]["name"] == "T1"
    assert cfg["sources"][0]["url"] == "https://example.com/feed.xml"
    assert any(p.get("topic") == "T1" for p in (cfg.get("topic_policies") or []))
    assert (cfg.get("app_config") or {}).get("profile_topic_name") == "Profile"
    assert (cfg.get("app_config") or {}).get("prompt_templates_custom_json")
    assert (cfg.get("app_config") or {}).get("prompt_template_bindings_json")

    # Import into a fresh DB.
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from tracker.models import Base

    db_path = tmp_path / "import.db"
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as s2:
        result = import_config(session=s2, data=json.loads(json.dumps(cfg)), update_existing=False)
        assert result["topics_created"] == 1
        assert result["sources_created"] == 1
        assert result["bindings_created"] == 1
        assert result["policies_created"] == 1
        assert result["app_config_created"] >= 1

        repo2 = Repo(s2)
        assert repo2.get_topic_by_name("T1") is not None
        src = repo2.get_source(type="rss", url="https://example.com/feed.xml")
        assert src is not None
        meta = repo2.get_source_meta(source_id=src.id)
        assert meta and meta.tags == "a,b" and meta.notes == "n1"
        pol = repo2.get_topic_policy(topic_id=repo2.get_topic_by_name("T1").id)  # type: ignore[union-attr]
        assert pol and pol.llm_curation_enabled is True and pol.llm_curation_prompt == "PROMPT"
        assert repo2.get_app_config("profile_topic_name") == "Profile"
        assert repo2.get_app_config("prompt_templates_custom_json") is not None
        assert repo2.get_app_config("prompt_template_bindings_json") is not None


def test_import_is_idempotent(db_session):
    repo = Repo(db_session)
    repo.add_topic(name="T1", query="x", digest_cron="0 9 * * *")
    src = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.update_source_meta(source_id=src.id, tags="a", notes="")
    repo.bind_topic_source(topic=repo.get_topic_by_name("T1"), source=src)  # type: ignore[arg-type]

    cfg = export_config(session=db_session)
    r1 = import_config(session=db_session, data=cfg, update_existing=False)
    r2 = import_config(session=db_session, data=cfg, update_existing=False)

    assert r1["topics_created"] == 0
    assert r2["topics_created"] == 0
    assert r1["bindings_created"] == 0
    assert r2["bindings_created"] == 0
    assert r1["policies_created"] == 0
    assert r2["policies_created"] == 0
