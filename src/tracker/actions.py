from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from tracker.connectors.discourse import build_discourse_json_url
from tracker.connectors.html_list import build_html_list_url
from tracker.connectors.hn_algolia import build_hn_search_url
from tracker.connectors.searxng import build_searxng_search_url
from tracker.models import Source, SourceCandidate, Topic, TopicPolicy, TopicSource
from tracker.normalize import canonicalize_url
from tracker.repo import Repo
from tracker.search_query import normalize_search_query, set_query_param


@dataclass(frozen=True)
class TopicSpec:
    name: str
    query: str = ""
    digest_cron: str = "0 9 * * *"
    alert_keywords: str = ""


@dataclass(frozen=True)
class SourceBindingSpec:
    topic: str
    include_keywords: str = ""
    exclude_keywords: str = ""


@dataclass(frozen=True)
class TopicAiPolicySpec:
    topic: str
    enabled: bool
    prompt: str | None = None


@dataclass(frozen=True)
class SyncSearchSourcesResult:
    updated: int
    created: int
    rebound: int


_AUTO_BIND_PROFILE_HOSTS = {"linux.do"}


def _looks_like_profile_autobind_source(url: str) -> bool:
    try:
        host = ((urlsplit((url or "").strip()).hostname or "").strip().lower()).rstrip(".")
    except Exception:
        host = ""
    if not host:
        return False
    return any(host == domain or host.endswith(f".{domain}") for domain in _AUTO_BIND_PROFILE_HOSTS)


def _get_profile_topic(repo: Repo) -> Topic | None:
    profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
    return repo.get_topic_by_name(profile_topic_name)


def _set_binding_filters(
    *,
    session: Session,
    repo: Repo,
    topic: Topic,
    source: Source,
    include_keywords: str = "",
    exclude_keywords: str = "",
) -> TopicSource:
    ts = repo.bind_topic_source(topic=topic, source=source)
    changed = False
    if ts.include_keywords != include_keywords:
        ts.include_keywords = include_keywords
        changed = True
    if ts.exclude_keywords != exclude_keywords:
        ts.exclude_keywords = exclude_keywords
        changed = True
    if changed:
        session.commit()
    return ts


def _finalize_source_creation(*, session: Session, repo: Repo, source: Source, bind: SourceBindingSpec | None = None) -> Source:
    explicit_topic: Topic | None = None
    if bind:
        explicit_topic = repo.get_topic_by_name(bind.topic)
        if not explicit_topic:
            raise ValueError(f"topic not found: {bind.topic}")
        _set_binding_filters(
            session=session,
            repo=repo,
            topic=explicit_topic,
            source=source,
            include_keywords=bind.include_keywords,
            exclude_keywords=bind.exclude_keywords,
        )

    if _looks_like_profile_autobind_source(source.url):
        profile_topic = _get_profile_topic(repo)
        if profile_topic and (explicit_topic is None or int(explicit_topic.id) != int(profile_topic.id)):
            _set_binding_filters(session=session, repo=repo, topic=profile_topic, source=source)
        if not bool(getattr(source, "enabled", True)):
            source.enabled = True
            session.commit()
    return source


def sync_topic_search_sources(*, session: Session, topic_name: str) -> SyncSearchSourcesResult:
    """
    Sync bound HN/SearxNG search source queries to match the topic query.

    This solves a common operator pitfall: topic.query gets edited, but existing search sources keep the old query.

    Safety: if a Source is bound to multiple topics, we will NOT mutate it in-place (would impact other topics).
    Instead we create (or reuse) a new Source with the updated query and rebind the current topic to it.
    """
    repo = Repo(session)
    topic = repo.get_topic_by_name(topic_name)
    if not topic:
        raise ValueError(f"topic not found: {topic_name}")

    desired_query = normalize_search_query((topic.query or "").strip() or topic.name)
    updated = 0
    created = 0
    rebound = 0

    rows = repo.list_topic_sources(topic=topic)
    for _t, src, ts in rows:
        param = ""
        if src.type == "hn_search":
            param = "query"
        elif src.type == "searxng_search":
            param = "q"
        else:
            continue

        new_url = set_query_param(url=src.url, param=param, query=desired_query)
        if new_url == src.url:
            continue

        bind_count = int(
            session.scalar(
                select(func.count())
                .select_from(TopicSource)
                .where(TopicSource.source_id == src.id)
            )
            or 0
        )

        if bind_count > 1:
            existed = repo.get_source(type=src.type, url=new_url)
            new_src = repo.add_source(type=src.type, url=new_url)
            if existed is None:
                created += 1

            new_ts = repo.bind_topic_source(topic=topic, source=new_src)
            new_ts.include_keywords = ts.include_keywords
            new_ts.exclude_keywords = ts.exclude_keywords
            session.commit()

            repo.unbind_topic_source(topic=topic, source=src)
            rebound += 1
            continue

        src.url = new_url
        session.commit()
        updated += 1

    return SyncSearchSourcesResult(updated=updated, created=created, rebound=rebound)


def create_topic(*, session: Session, spec: TopicSpec) -> Topic:
    repo = Repo(session)
    topic = repo.add_topic(name=spec.name, query=spec.query, digest_cron=spec.digest_cron)
    topic.alert_keywords = spec.alert_keywords
    session.commit()
    return topic


def set_topic_enabled(*, session: Session, name: str, enabled: bool) -> None:
    Repo(session).set_topic_enabled(name, enabled)


def upsert_topic_ai_policy(*, session: Session, spec: TopicAiPolicySpec) -> None:
    repo = Repo(session)
    topic = repo.get_topic_by_name(spec.topic)
    if not topic:
        raise ValueError(f"topic not found: {spec.topic}")
    repo.upsert_topic_policy(
        topic_id=topic.id,
        llm_curation_enabled=bool(spec.enabled),
        llm_curation_prompt=(spec.prompt if spec.prompt is not None else None),
    )


def create_rss_source(*, session: Session, url: str, bind: SourceBindingSpec | None = None) -> Source:
    repo = Repo(session)
    source = repo.add_source(type="rss", url=url)
    return _finalize_source_creation(session=session, repo=repo, source=source, bind=bind)


def create_rss_sources_bulk(
    *,
    session: Session,
    urls: list[str],
    bind: SourceBindingSpec | None = None,
    tags: str | None = None,
    notes: str | None = None,
) -> tuple[int, int]:
    """
    Bulk-create RSS sources and (optionally) bind them to a topic.

    This avoids doing 90+ commits when importing large "RSS packs".
    """
    repo = Repo(session)

    uniq: list[str] = []
    seen: set[str] = set()
    for u in urls:
        s = (u or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)

    if not uniq:
        return 0, 0

    existing = {
        s.url: s for s in session.scalars(select(Source).where(Source.type == "rss", Source.url.in_(uniq))).all()
    }

    sources: list[Source] = []
    created = 0
    for url in uniq:
        src = existing.get(url)
        if not src:
            src = Source(type="rss", url=url)
            session.add(src)
            created += 1
        sources.append(src)

    session.flush()

    bound = 0
    if bind:
        topic = repo.get_topic_by_name(bind.topic)
        if not topic:
            raise ValueError(f"topic not found: {bind.topic}")

        source_ids = [int(s.id) for s in sources if s.id is not None]
        existing_bindings: set[int] = set()
        if source_ids:
            existing_bindings = set(
                session.scalars(
                    select(TopicSource.source_id).where(
                        TopicSource.topic_id == int(topic.id),
                        TopicSource.source_id.in_(source_ids),
                    )
                ).all()
            )

        for s in sources:
            if s.id is None:
                continue
            if int(s.id) in existing_bindings:
                continue
            ts = TopicSource(topic_id=int(topic.id), source_id=int(s.id))
            ts.include_keywords = bind.include_keywords
            ts.exclude_keywords = bind.exclude_keywords
            session.add(ts)
            bound += 1

    profile_topic = _get_profile_topic(repo)
    auto_bound = False
    for s in sources:
        if not _looks_like_profile_autobind_source(s.url):
            continue
        if profile_topic is not None:
            _set_binding_filters(session=session, repo=repo, topic=profile_topic, source=s)
            auto_bound = True
        if not bool(getattr(s, "enabled", True)):
            s.enabled = True
            auto_bound = True

    if tags is not None or notes is not None:
        for s in sources:
            if s.id is None:
                continue
            meta = repo.get_or_create_source_meta(source_id=int(s.id))
            if tags is not None:
                meta.tags = tags
            if notes is not None:
                meta.notes = notes

    if auto_bound:
        session.flush()
    session.commit()
    return created, bound


def create_hn_search_source(
    *,
    session: Session,
    query: str,
    tags: str = "story",
    hits_per_page: int = 50,
    bind: SourceBindingSpec | None = None,
) -> Source:
    repo = Repo(session)
    q = normalize_search_query(query)
    url = build_hn_search_url(query=q, tags=tags, hits_per_page=hits_per_page)
    source = repo.add_source(type="hn_search", url=url)
    return _finalize_source_creation(session=session, repo=repo, source=source, bind=bind)


def create_searxng_search_source(
    *,
    session: Session,
    base_url: str,
    query: str,
    categories: str | None = None,
    time_range: str | None = "day",
    language: str | None = None,
    results: int | None = 20,
    bind: SourceBindingSpec | None = None,
) -> Source:
    repo = Repo(session)
    q = normalize_search_query(query)
    url = build_searxng_search_url(
        base_url=base_url,
        query=q,
        categories=categories or None,
        time_range=time_range or None,
        language=language or None,
        results=results,
    )
    source = repo.add_source(type="searxng_search", url=url)
    return _finalize_source_creation(session=session, repo=repo, source=source, bind=bind)


def create_discourse_source(
    *,
    session: Session,
    base_url: str,
    json_path: str = "/latest.json",
    bind: SourceBindingSpec | None = None,
) -> Source:
    repo = Repo(session)
    url = build_discourse_json_url(base_url=base_url, json_path=json_path)
    source = repo.add_source(type="discourse", url=url)
    return _finalize_source_creation(session=session, repo=repo, source=source, bind=bind)


def create_llm_models_source(
    *,
    session: Session,
    base_url: str,
    bind: SourceBindingSpec | None = None,
) -> Source:
    """
    Create an internal "LLM models list" source.

    This polls an OpenAI-compatible gateway's `/v1/models` to detect newly available models.
    """
    repo = Repo(session)
    u = (base_url or "").strip()
    if not u:
        raise ValueError("base_url is required")
    source = repo.add_source(type="llm_models", url=u)
    return _finalize_source_creation(session=session, repo=repo, source=source, bind=bind)


def create_html_list_source(
    *,
    session: Session,
    page_url: str,
    item_selector: str,
    title_selector: str | None = None,
    summary_selector: str | None = None,
    max_items: int = 30,
    bind: SourceBindingSpec | None = None,
) -> Source:
    repo = Repo(session)
    url = build_html_list_url(
        page_url=page_url,
        item_selector=item_selector,
        title_selector=title_selector,
        summary_selector=summary_selector,
        max_items=max_items,
    )
    source = repo.add_source(type="html_list", url=url)
    return _finalize_source_creation(session=session, repo=repo, source=source, bind=bind)


def create_binding(
    *,
    session: Session,
    topic_name: str,
    source_id: int,
    include_keywords: str = "",
    exclude_keywords: str = "",
) -> None:
    repo = Repo(session)
    topic = repo.get_topic_by_name(topic_name)
    if not topic:
        raise ValueError(f"topic not found: {topic_name}")
    source = repo.get_source_by_id(source_id)
    if not source:
        raise ValueError(f"source not found: {source_id}")
    ts = repo.bind_topic_source(topic=topic, source=source)
    ts.include_keywords = include_keywords
    ts.exclude_keywords = exclude_keywords
    session.commit()


def update_binding(
    *,
    session: Session,
    topic_name: str,
    source_id: int,
    include_keywords: str | None = None,
    exclude_keywords: str | None = None,
) -> None:
    repo = Repo(session)
    topic = repo.get_topic_by_name(topic_name)
    if not topic:
        raise ValueError(f"topic not found: {topic_name}")
    source = repo.get_source_by_id(source_id)
    if not source:
        raise ValueError(f"source not found: {source_id}")
    repo.update_topic_source_filters(
        topic=topic,
        source=source,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
    )


def remove_binding(*, session: Session, topic_name: str, source_id: int) -> None:
    repo = Repo(session)
    topic = repo.get_topic_by_name(topic_name)
    if not topic:
        raise ValueError(f"topic not found: {topic_name}")
    source = repo.get_source_by_id(source_id)
    if not source:
        raise ValueError(f"source not found: {source_id}")
    if not repo.unbind_topic_source(topic=topic, source=source):
        raise ValueError("binding not found")


def update_source_meta(
    *,
    session: Session,
    source_id: int,
    tags: str | None = None,
    notes: str | None = None,
) -> None:
    Repo(session).update_source_meta(source_id=source_id, tags=tags, notes=notes)


def accept_source_candidate(*, session: Session, candidate_id: int, enabled: bool = True) -> Source:
    repo = Repo(session)
    cand = repo.get_source_candidate_by_id(candidate_id)
    if not cand:
        raise ValueError(f"candidate not found: {candidate_id}")
    if cand.source_type != "rss":
        raise ValueError(f"unsupported candidate source_type: {cand.source_type}")

    topic = session.get(Topic, cand.topic_id)
    if not topic:
        raise ValueError("candidate topic not found")

    effective_url = str(cand.url or "").strip()
    try:
        if cand.discovered_from_url:
            src = urlsplit(str(cand.discovered_from_url or "").strip())
            dst = urlsplit(effective_url)
            src_host = (src.hostname or "").strip().lower()
            dst_host = (dst.hostname or "").strip().lower()
            if src_host.startswith("www.") and dst_host and dst_host == src_host[4:]:
                effective_url = urlunsplit((dst.scheme, src.netloc, dst.path, dst.query, dst.fragment))
    except Exception:
        pass

    source = repo.add_source(type=cand.source_type, url=effective_url)
    if source.enabled != enabled:
        source.enabled = enabled
        session.commit()
    repo.bind_topic_source(topic=topic, source=source)

    # If the candidate has an LLM eval, persist it as the Source score so push-gates and eviction work
    # consistently whether the operator accepts manually or via auto-accept.
    try:
        ev = repo.get_source_candidate_eval(candidate_id=int(candidate_id))
        if ev is not None:
            repo.upsert_source_score(
                source_id=int(source.id),
                score=int(getattr(ev, "score", 0) or 0),
                quality_score=int(getattr(ev, "quality_score", 0) or 0),
                relevance_score=int(getattr(ev, "relevance_score", 0) or 0),
                novelty_score=int(getattr(ev, "novelty_score", 0) or 0),
                origin="cand",
                note=f"accepted from candidate_id={candidate_id} decision={getattr(ev,'decision','')}"[:4000],
            )
    except Exception:
        pass

    # Default UX: accepting candidates implies enabling per-topic LLM curation (mode=llm).
    try:
        pol = repo.get_topic_policy(topic_id=int(topic.id))
        if not pol:
            pol = TopicPolicy(topic_id=int(topic.id))
            session.add(pol)
            session.flush()
        pol.llm_curation_enabled = True
    except Exception:
        pass

    cand.status = "accepted"
    if effective_url and effective_url != str(cand.url or "").strip():
        try:
            cand.url = canonicalize_url(effective_url, strip_www=False)
        except Exception:
            pass
    session.commit()

    # Dedupe UX: if the same candidate URL exists for other topics, accept them too.
    try:
        others = list(
            session.scalars(
                select(SourceCandidate).where(
                    and_(
                        SourceCandidate.source_type == cand.source_type,
                        SourceCandidate.url == cand.url,
                        SourceCandidate.status == "new",
                    )
                )
            )
        )
    except Exception:
        others = []
    for o in others:
        try:
            if int(getattr(o, "id", 0) or 0) == int(cand.id):
                continue
            t2 = session.get(Topic, int(o.topic_id))
            if not t2:
                continue
            repo.bind_topic_source(topic=t2, source=source)
            try:
                pol2 = repo.get_topic_policy(topic_id=int(t2.id))
                if not pol2:
                    pol2 = TopicPolicy(topic_id=int(t2.id))
                    session.add(pol2)
                    session.flush()
                pol2.llm_curation_enabled = True
            except Exception:
                pass
            o.status = "accepted"
        except Exception:
            continue
    try:
        session.commit()
    except Exception:
        pass
    return source


def ignore_source_candidate(*, session: Session, candidate_id: int) -> None:
    repo = Repo(session)
    cand = repo.get_source_candidate_by_id(candidate_id)
    if not cand:
        raise ValueError(f"candidate not found: {candidate_id}")
    cand.status = "ignored"

    # Global ignore: make sure ignored candidates don't reappear (even across topics).
    try:
        raw = (repo.get_app_config("source_candidate_ignore_urls") or "").strip()
        lines = [ln.strip() for ln in raw.splitlines() if (ln or "").strip() and not ln.strip().startswith("#")]
        if (cand.url or "").strip() and (cand.url or "").strip() not in lines:
            lines.append((cand.url or "").strip())
        # Keep stable, human-editable format (one URL per line).
        repo.set_app_config("source_candidate_ignore_urls", "\n".join(sorted(set(lines))))
    except Exception:
        pass

    # Propagate ignore to duplicates across topics (but never touch accepted).
    try:
        for o in session.scalars(
            select(SourceCandidate).where(
                and_(
                    SourceCandidate.source_type == cand.source_type,
                    SourceCandidate.url == cand.url,
                    SourceCandidate.status != "accepted",
                )
            )
        ):
            o.status = "ignored"
    except Exception:
        pass

    session.commit()
