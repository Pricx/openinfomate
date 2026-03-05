from __future__ import annotations

from tracker.actions import SourceBindingSpec, TopicSpec, create_rss_sources_bulk, create_topic
from tracker.repo import Repo
from tracker.source_packs import get_rss_pack


def test_create_rss_sources_bulk_idempotent(db_session):
    create_topic(session=db_session, spec=TopicSpec(name="Profile", query=""))
    repo = Repo(db_session)

    urls = ["https://example.com/rss.xml", "https://example.com/alt.xml", "https://example.com/rss.xml"]
    created, bound = create_rss_sources_bulk(
        session=db_session,
        urls=urls,
        bind=SourceBindingSpec(topic="Profile", include_keywords="a,b", exclude_keywords="c"),
        tags="pack,test",
        notes="bulk test",
    )
    assert created == 2
    assert bound == 2

    # Re-import should be a no-op.
    created2, bound2 = create_rss_sources_bulk(
        session=db_session,
        urls=urls,
        bind=SourceBindingSpec(topic="Profile", include_keywords="a,b", exclude_keywords="c"),
        tags="pack,test",
        notes="bulk test",
    )
    assert created2 == 0
    assert bound2 == 0

    profile = repo.get_topic_by_name("Profile")
    assert profile is not None
    rows = repo.list_topic_sources(topic=profile)
    rss_rows = [(t, s, ts) for t, s, ts in rows if s.type == "rss"]
    assert len(rss_rows) == 2
    for _t, src, ts in rss_rows:
        assert (ts.include_keywords or "") == "a,b"
        assert (ts.exclude_keywords or "") == "c"
        meta = repo.get_source_meta(source_id=int(src.id))
        assert meta is not None
        assert (meta.tags or "") == "pack,test"
        assert (meta.notes or "") == "bulk test"


def test_get_rss_pack_alias():
    # Sanity check: aliases resolve.
    pack = get_rss_pack("karpathy90")
    assert pack.id == "hn_popularity_karpathy"
    assert len(pack.urls) >= 90
