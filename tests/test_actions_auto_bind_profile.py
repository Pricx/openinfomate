from __future__ import annotations

from tracker.actions import TopicSpec, create_discourse_source, create_topic
from tracker.repo import Repo


def test_create_discourse_source_linux_do_rebinds_profile_and_reenables_source(db_session):
    create_topic(session=db_session, spec=TopicSpec(name="Profile", query=""))
    repo = Repo(db_session)

    src = repo.add_source(type="discourse", url="https://linux.do/latest.json")
    src.enabled = False
    db_session.commit()

    out = create_discourse_source(session=db_session, base_url="https://linux.do", json_path="/latest.json")
    assert int(out.id) == int(src.id)
    assert out.enabled is True

    profile = repo.get_topic_by_name("Profile")
    assert profile is not None
    rows = repo.list_topic_sources(topic=profile)
    assert [(s.type, s.url) for _t, s, _ts in rows] == [("discourse", "https://linux.do/latest.json")]


def test_create_discourse_source_other_host_does_not_auto_bind_profile(db_session):
    create_topic(session=db_session, spec=TopicSpec(name="Profile", query=""))
    repo = Repo(db_session)

    create_discourse_source(session=db_session, base_url="https://forum.example.com", json_path="/latest.json")

    profile = repo.get_topic_by_name("Profile")
    assert profile is not None
    assert repo.list_topic_sources(topic=profile) == []
