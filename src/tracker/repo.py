from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import Select, and_, case, delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tracker.models import (
    AppConfig,
    ConfigAgentRun,
    FeedbackEvent,
    Item,
    ItemContent,
    ItemTopic,
    LlmUsage,
    MuteRule,
    ProfileRevision,
    PushLog,
    Report,
    SettingsChange,
    Source,
    SourceCandidate,
    SourceCandidateEval,
    SourceHealth,
    SourceMeta,
    SourceScore,
    TelegramMessage,
    TelegramTask,
    Topic,
    TopicPolicy,
    TopicSource,
)
from tracker.normalize import canonicalize_url


def _normalize_topic_name(name: str) -> str:
    return " ".join((name or "").strip().split())


@dataclass(frozen=True)
class ActivitySnapshot:
    last_tick_at: dt.datetime | None
    last_digest_report_at: dt.datetime | None
    last_health_report_at: dt.datetime | None
    last_push_attempt_at: dt.datetime | None
    last_push_sent_at: dt.datetime | None
    last_digest_sync_at: dt.datetime | None = None
    last_curated_sync_at: dt.datetime | None = None
    digest_sync_enabled_topics: int = 0
    digest_sync_scheduled_topics: int = 0
    curated_sync_job_present: bool = False


def _parse_app_config_datetime(value: str | None) -> dt.datetime | None:
    raw = (value or '').strip()
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except Exception:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_app_config_int(value: str | None) -> int:
    raw = (value or '').strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except Exception:
        return 0


class Repo:
    def __init__(self, session: Session):
        self.session = session

    # --- app config (global)
    def get_app_config_entry(self, key: str) -> AppConfig | None:
        k = (key or "").strip()
        if not k:
            return None
        return self.session.get(AppConfig, k)

    def get_app_config(self, key: str) -> str | None:
        row = self.session.get(AppConfig, key)
        if not row:
            return None
        return (row.value or "").strip()

    def set_app_config_many(self, updates: dict[str, str]) -> None:
        if not updates:
            return
        for key, value in updates.items():
            k = (key or "").strip()
            if not k:
                continue
            row = self.session.get(AppConfig, k)
            if not row:
                row = AppConfig(key=k)
                self.session.add(row)
                self.session.flush()
            row.value = str(value or "")
        self.session.commit()

    def set_app_config(self, key: str, value: str) -> None:
        k = (key or "").strip()
        if not k:
            raise ValueError("missing config key")
        row = self.session.get(AppConfig, k)
        if not row:
            row = AppConfig(key=k)
            self.session.add(row)
            self.session.flush()
        row.value = str(value or "")
        self.session.commit()

    def delete_app_config(self, key: str) -> bool:
        k = (key or "").strip()
        if not k:
            return False
        row = self.session.get(AppConfig, k)
        if not row:
            return False
        self.session.delete(row)
        self.session.commit()
        return True

    # --- topics
    def get_topic_by_name(self, name: str) -> Topic | None:
        raw = _normalize_topic_name(name)
        if not raw:
            return None
        hit = self.session.scalar(select(Topic).where(Topic.name == raw))
        if hit:
            return hit
        # Anti-dup: treat names as case-insensitive in operator flows.
        low = raw.casefold()
        return self.session.scalar(select(Topic).where(func.lower(Topic.name) == low))

    def list_topics(self) -> list[Topic]:
        return list(self.session.scalars(select(Topic).order_by(Topic.id)))

    def get_topic_policy(self, *, topic_id: int) -> TopicPolicy | None:
        return self.session.scalar(select(TopicPolicy).where(TopicPolicy.topic_id == topic_id))

    def list_topic_policies(self) -> list[TopicPolicy]:
        return list(self.session.scalars(select(TopicPolicy).order_by(TopicPolicy.topic_id)))

    def upsert_topic_policy(
        self,
        *,
        topic_id: int,
        llm_curation_enabled: bool | None = None,
        llm_curation_prompt: str | None = None,
    ) -> TopicPolicy:
        pol = self.get_topic_policy(topic_id=topic_id)
        if not pol:
            pol = TopicPolicy(topic_id=topic_id)
            self.session.add(pol)
            self.session.flush()
        if llm_curation_enabled is not None:
            pol.llm_curation_enabled = bool(llm_curation_enabled)
        if llm_curation_prompt is not None:
            pol.llm_curation_prompt = str(llm_curation_prompt or "")
        self.session.commit()
        return pol

    def add_topic(self, *, name: str, query: str, digest_cron: str = "0 9 * * *") -> Topic:
        n = _normalize_topic_name(name)
        existing = self.get_topic_by_name(n)
        if existing:
            raise ValueError(f"topic already exists: {existing.name}")
        topic = Topic(name=n, query=query, digest_cron=digest_cron)
        self.session.add(topic)
        self.session.commit()
        return topic

    def set_topic_enabled(self, name: str, enabled: bool) -> None:
        topic = self.get_topic_by_name(name)
        if not topic:
            raise ValueError(f"topic not found: {name}")
        topic.enabled = enabled
        self.session.commit()

    def get_activity_snapshot(self) -> ActivitySnapshot:
        last_tick_at = self.session.scalar(select(func.max(Source.last_checked_at)).where(Source.enabled.is_(True)))
        last_digest_report_at = self.session.scalar(select(func.max(Report.updated_at)).where(Report.kind == "digest"))
        last_health_report_at = self.session.scalar(select(func.max(Report.updated_at)).where(Report.kind == "health"))
        last_push_attempt_at = self.session.scalar(select(func.max(PushLog.created_at)))
        last_push_sent_at = self.session.scalar(select(func.max(PushLog.sent_at)).where(PushLog.sent_at.is_not(None)))
        last_digest_sync_at = _parse_app_config_datetime(self.get_app_config('service.scheduler.digest_sync.last_ok_at'))
        last_curated_sync_at = _parse_app_config_datetime(self.get_app_config('service.scheduler.curated_sync.last_ok_at'))
        digest_sync_enabled_topics = _parse_app_config_int(self.get_app_config('service.scheduler.digest_sync.enabled_topics'))
        digest_sync_scheduled_topics = _parse_app_config_int(self.get_app_config('service.scheduler.digest_sync.scheduled_topics'))
        curated_sync_job_present = _parse_app_config_int(self.get_app_config('service.scheduler.curated_sync.job_present')) > 0
        return ActivitySnapshot(
            last_tick_at=last_tick_at,
            last_digest_report_at=last_digest_report_at,
            last_health_report_at=last_health_report_at,
            last_push_attempt_at=last_push_attempt_at,
            last_push_sent_at=last_push_sent_at,
            last_digest_sync_at=last_digest_sync_at,
            last_curated_sync_at=last_curated_sync_at,
            digest_sync_enabled_topics=digest_sync_enabled_topics,
            digest_sync_scheduled_topics=digest_sync_scheduled_topics,
            curated_sync_job_present=curated_sync_job_present,
        )

    # --- sources
    def get_source(self, *, type: str, url: str) -> Source | None:
        return self.session.scalar(select(Source).where(and_(Source.type == type, Source.url == url)))

    def add_source(self, *, type: str, url: str) -> Source:
        existing = self.get_source(type=type, url=url)
        if existing:
            return existing
        source = Source(type=type, url=url)
        self.session.add(source)
        self.session.commit()
        return source

    def get_source_by_id(self, source_id: int) -> Source | None:
        return self.session.get(Source, source_id)

    def list_sources(self) -> list[Source]:
        return list(self.session.scalars(select(Source).order_by(Source.id)))

    def list_sources_with_health(self) -> list[tuple[Source, SourceHealth | None]]:
        stmt = (
            select(Source, SourceHealth)
            .outerjoin(SourceHealth, SourceHealth.source_id == Source.id)
            .order_by(Source.id)
        )
        return list(self.session.execute(stmt).all())

    def list_sources_with_auth_required(self) -> list[tuple[Source, SourceHealth]]:
        """
        Return sources whose last fetch failed due to authentication (login/cookie required).

        This is used to surface "re-login needed" reminders in regular pushes (alerts/digests),
        so operators don't miss the once-per-host auth-required alert.
        """
        stmt = (
            select(Source, SourceHealth)
            .join(SourceHealth, SourceHealth.source_id == Source.id)
            .where(and_(SourceHealth.last_error == "auth_required", SourceHealth.last_error_at.is_not(None)))
            .order_by(SourceHealth.last_error_at.desc(), Source.id.desc())
        )
        return list(self.session.execute(stmt).all())

    def list_sources_with_health_and_meta(
        self,
    ) -> list[tuple[Source, SourceHealth | None, SourceMeta | None]]:
        stmt = (
            select(Source, SourceHealth, SourceMeta)
            .outerjoin(SourceHealth, SourceHealth.source_id == Source.id)
            .outerjoin(SourceMeta, SourceMeta.source_id == Source.id)
            .order_by(Source.id)
        )
        return list(self.session.execute(stmt).all())

    def set_source_enabled(self, source_id: int, enabled: bool) -> None:
        source = self.get_source_by_id(source_id)
        if not source:
            raise ValueError(f"source not found: {source_id}")
        source.enabled = enabled
        self.session.commit()

    def get_or_create_source_meta(self, *, source_id: int) -> SourceMeta:
        meta = self.session.scalar(select(SourceMeta).where(SourceMeta.source_id == source_id))
        if meta:
            return meta
        meta = SourceMeta(source_id=source_id)
        self.session.add(meta)
        self.session.flush()
        return meta

    def get_source_meta(self, *, source_id: int) -> SourceMeta | None:
        return self.session.scalar(select(SourceMeta).where(SourceMeta.source_id == source_id))

    def update_source_meta(
        self,
        *,
        source_id: int,
        tags: str | None = None,
        notes: str | None = None,
    ) -> SourceMeta:
        if not self.get_source_by_id(source_id):
            raise ValueError(f"source not found: {source_id}")
        meta = self.get_or_create_source_meta(source_id=source_id)
        if tags is not None:
            meta.tags = tags
        if notes is not None:
            meta.notes = notes
        self.session.commit()
        return meta

    # --- source scores (LLM/manual/feedback)
    def get_source_score(self, *, source_id: int) -> SourceScore | None:
        return self.session.scalar(select(SourceScore).where(SourceScore.source_id == int(source_id)))

    def upsert_source_score(
        self,
        *,
        source_id: int,
        score: int | None = None,
        quality_score: int | None = None,
        relevance_score: int | None = None,
        novelty_score: int | None = None,
        origin: str | None = None,
        note: str | None = None,
        locked: bool | None = None,
        force: bool = False,
    ) -> SourceScore:
        row = self.get_source_score(source_id=int(source_id))
        if not row:
            row = SourceScore(source_id=int(source_id))
            self.session.add(row)
            self.session.flush()
        if (not force) and bool(getattr(row, "locked", False)):
            return row

        def _clamp(v: object | None) -> int | None:
            if v is None:
                return None
            try:
                x = int(v)  # type: ignore[arg-type]
            except Exception:
                x = 0
            return max(0, min(100, x))

        s = _clamp(score)
        q = _clamp(quality_score)
        r = _clamp(relevance_score)
        n = _clamp(novelty_score)
        if s is not None:
            row.score = int(s)
        if q is not None:
            row.quality_score = int(q)
        if r is not None:
            row.relevance_score = int(r)
        if n is not None:
            row.novelty_score = int(n)
        if origin is not None:
            row.origin = (str(origin or "").strip() or "")[:16]
        if note is not None:
            row.note = str(note or "")[:4000]
        if locked is not None:
            row.locked = bool(locked)
        self.session.commit()
        return row

    def list_source_scores(self, *, limit: int = 2000) -> list[SourceScore]:
        limit = max(1, min(10_000, int(limit or 2000)))
        return list(self.session.scalars(select(SourceScore).order_by(SourceScore.source_id).limit(limit)))

    # --- candidate evals
    def get_source_candidate_eval(self, *, candidate_id: int) -> SourceCandidateEval | None:
        return self.session.scalar(select(SourceCandidateEval).where(SourceCandidateEval.candidate_id == int(candidate_id)))

    def upsert_source_candidate_eval(
        self,
        *,
        candidate_id: int,
        decision: str = "",
        score: int | None = None,
        quality_score: int | None = None,
        relevance_score: int | None = None,
        novelty_score: int | None = None,
        why: str | None = None,
        model: str | None = None,
        explore_weight: int | None = None,
        exploit_weight: int | None = None,
    ) -> SourceCandidateEval:
        row = self.get_source_candidate_eval(candidate_id=int(candidate_id))
        if not row:
            row = SourceCandidateEval(candidate_id=int(candidate_id))
            self.session.add(row)
            self.session.flush()

        def _clamp(v: object | None) -> int | None:
            if v is None:
                return None
            try:
                x = int(v)  # type: ignore[arg-type]
            except Exception:
                x = 0
            return max(0, min(100, x))

        s = _clamp(score)
        q = _clamp(quality_score)
        r = _clamp(relevance_score)
        n = _clamp(novelty_score)
        if s is not None:
            row.score = int(s)
        if q is not None:
            row.quality_score = int(q)
        if r is not None:
            row.relevance_score = int(r)
        if n is not None:
            row.novelty_score = int(n)
        row.decision = (str(decision or "").strip() or "")[:16]
        if why is not None:
            row.why = str(why or "")[:4000]
        if model is not None:
            row.model = str(model or "")[:200]
        if explore_weight is not None:
            row.explore_weight = max(0, min(10, int(explore_weight or 0)))
        if exploit_weight is not None:
            row.exploit_weight = max(0, min(10, int(exploit_weight or 0)))
        self.session.commit()
        return row

    def bind_topic_source(self, *, topic: Topic, source: Source) -> TopicSource:
        existing = self.session.scalar(
            select(TopicSource).where(
                and_(TopicSource.topic_id == topic.id, TopicSource.source_id == source.id)
            )
        )
        if existing:
            return existing
        ts = TopicSource(topic_id=topic.id, source_id=source.id)
        self.session.add(ts)
        self.session.commit()
        return ts

    def get_topic_source(self, *, topic_id: int, source_id: int) -> TopicSource | None:
        return self.session.scalar(
            select(TopicSource).where(and_(TopicSource.topic_id == topic_id, TopicSource.source_id == source_id))
        )

    def list_topic_sources(
        self, *, topic: Topic | None = None
    ) -> list[tuple[Topic, Source, TopicSource]]:
        stmt: Select[tuple[Topic, Source, TopicSource]] = (
            select(Topic, Source, TopicSource)
            .join(TopicSource, TopicSource.topic_id == Topic.id)
            .join(Source, TopicSource.source_id == Source.id)
            .order_by(Topic.id, Source.id)
        )
        if topic is not None:
            stmt = stmt.where(Topic.id == topic.id)
        return list(self.session.execute(stmt).all())

    def unbind_topic_source(self, *, topic: Topic, source: Source) -> bool:
        ts = self.get_topic_source(topic_id=topic.id, source_id=source.id)
        if not ts:
            return False
        self.session.delete(ts)
        self.session.commit()
        return True

    def update_topic_source_filters(
        self,
        *,
        topic: Topic,
        source: Source,
        include_keywords: str | None = None,
        exclude_keywords: str | None = None,
    ) -> TopicSource:
        ts = self.get_topic_source(topic_id=topic.id, source_id=source.id)
        if not ts:
            raise ValueError("binding not found")
        if include_keywords is not None:
            ts.include_keywords = include_keywords
        if exclude_keywords is not None:
            ts.exclude_keywords = exclude_keywords
        self.session.commit()
        return ts

    def list_enabled_topic_sources(self) -> list[tuple[Topic, Source, TopicSource]]:
        stmt: Select[tuple[Topic, Source, TopicSource]] = (
            select(Topic, Source, TopicSource)
            .join(TopicSource, TopicSource.topic_id == Topic.id)
            .join(Source, TopicSource.source_id == Source.id)
            .where(and_(Topic.enabled.is_(True), Source.enabled.is_(True)))
            .order_by(Topic.id, Source.id)
        )
        return list(self.session.execute(stmt).all())

    # --- items
    def item_exists_by_canonical_url(self, canonical_url: str) -> bool:
        return self.session.scalar(select(func.count()).select_from(Item).where(Item.canonical_url == canonical_url)) > 0

    def get_item_by_canonical_url(self, canonical_url: str) -> Item | None:
        return self.session.scalar(select(Item).where(Item.canonical_url == canonical_url))

    def get_item_by_id(self, item_id: int) -> Item | None:
        return self.session.get(Item, item_id)

    def get_item_content(self, *, item_id: int) -> ItemContent | None:
        return self.session.scalar(select(ItemContent).where(ItemContent.item_id == item_id))

    def upsert_item_content(self, *, item_id: int, url: str, content_text: str, error: str = "") -> ItemContent:
        row = self.get_item_content(item_id=item_id)
        if not row:
            row = ItemContent(item_id=item_id)
            self.session.add(row)
            self.session.flush()
        row.url = url
        row.content_text = content_text
        row.error = error
        row.fetched_at = dt.datetime.utcnow()
        self.session.commit()
        return row

    def get_item_topic(self, *, item_id: int, topic_id: int) -> ItemTopic | None:
        return self.session.scalar(
            select(ItemTopic).where(and_(ItemTopic.item_id == item_id, ItemTopic.topic_id == topic_id))
        )

    def item_topic_exists(self, *, item_id: int, topic_id: int) -> bool:
        return (
            self.session.scalar(
                select(func.count())
                .select_from(ItemTopic)
                .where(and_(ItemTopic.item_id == item_id, ItemTopic.topic_id == topic_id))
            )
            > 0
        )

    def add_item(self, item: Item) -> Item:
        self.session.add(item)
        self.session.commit()
        return item

    def add_item_topic(self, it: ItemTopic) -> ItemTopic:
        self.session.add(it)
        self.session.commit()
        return it

    def list_item_topics_for_digest(
        self, *, topic: Topic, since: dt.datetime
    ) -> list[tuple[ItemTopic, Item]]:
        stmt = (
            select(ItemTopic, Item)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(["digest", "alert"]),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                )
            )
            .order_by(Item.created_at.desc())
        )
        return list(self.session.execute(stmt).all())

    def list_item_topics_for_curation(
        self,
        *,
        topic: Topic,
        since: dt.datetime,
        limit: int = 50,
        decisions: list[str] | None = None,
    ) -> list[tuple[ItemTopic, Item]]:
        """
        List "candidate" items to be curated by an LLM.

        Ordering is by recency: coalesce(published_at, created_at) desc.
        """
        limit = max(1, min(500, int(limit)))
        decs = [d for d in (decisions or ["candidate"]) if d]
        if not decs:
            decs = ["candidate"]

        stmt = (
            select(ItemTopic, Item)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(decs),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                )
            )
            .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).all())

    def list_uncurated_item_topics_for_topic(
        self,
        *,
        topic: Topic,
        since: dt.datetime,
        limit: int = 50,
        exclude_item_ids: set[int] | None = None,
        order: str = "desc",
    ) -> list[tuple[ItemTopic, Item]]:
        """
        List pending candidate items for a topic.

        Intentionally treat *any* `decision == "candidate"` row in-window as pending
        digest work. Older code paths and manual backfills have used a mix of reason
        strings (including blank reason), so tying recovery to specific reason text is
        brittle and can silently strand valid backlog items.
        """
        limit = max(1, min(500, int(limit)))
        exclude_ids = exclude_item_ids or set()
        ord_raw = (order or "desc").strip().lower()
        oldest_first = ord_raw in {"asc", "oldest", "oldest_first", "oldest-first", "fifo"}

        stmt = (
            select(ItemTopic, Item)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision == "candidate",
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                )
            )
            .order_by(
                func.coalesce(Item.published_at, Item.created_at).asc() if oldest_first else func.coalesce(Item.published_at, Item.created_at).desc(),
                ItemTopic.id.asc() if oldest_first else ItemTopic.id.desc(),
            )
            .limit(limit)
        )

        if exclude_ids:
            stmt = stmt.where(ItemTopic.item_id.notin_(sorted(exclude_ids)))

        return list(self.session.execute(stmt).all())

    def list_item_simhashes_for_topic_window(
        self,
        *,
        topic: Topic,
        since: dt.datetime,
        until: dt.datetime,
        decisions: list[str] | None = None,
        limit: int = 5000,
    ) -> list[int]:
        """
        List item simhash values for a topic within a time window.

        Used for history-based anti-dup (e.g., to avoid re-sending the same story day after day).
        """
        limit = max(1, min(20_000, int(limit)))
        decs = [d for d in (decisions or ["digest", "alert"]) if d]
        if not decs:
            decs = ["digest", "alert"]

        stmt = (
            select(Item.simhash64)
            .select_from(ItemTopic)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(decs),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                    func.coalesce(Item.published_at, Item.created_at) < until,
                )
            )
            .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
            .limit(limit)
        )
        vals = [v for v in self.session.scalars(stmt).all() if v is not None]
        return [int(v) for v in vals if int(v) != 0]

    def list_item_simhashes_window(
        self,
        *,
        since: dt.datetime,
        until: dt.datetime,
        decisions: list[str] | None = None,
        limit: int = 5000,
    ) -> list[int]:
        """
        List item simhash values across ALL topics within a time window.

        Used for global history-based anti-dup (e.g., avoid re-alerting the same story
        under different topics).
        """
        limit = max(1, min(20_000, int(limit)))
        decs = [d for d in (decisions or ["digest", "alert"]) if d]
        if not decs:
            decs = ["digest", "alert"]

        stmt = (
            select(Item.simhash64)
            .select_from(ItemTopic)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.decision.in_(decs),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                    func.coalesce(Item.published_at, Item.created_at) < until,
                )
            )
            .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
            .limit(limit)
        )
        vals = [v for v in self.session.scalars(stmt).all() if v is not None]
        return [int(v) for v in vals if int(v) != 0]

    def list_recent_sent_items_for_topic_window(
        self,
        *,
        topic: Topic,
        since: dt.datetime,
        until: dt.datetime,
        decisions: list[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, str]]:
        """
        List recently sent (digest/alert) items for a topic within a time window.

        Used to provide an explicit anti-repeat context to LLM curation prompts.
        """
        limit = max(1, min(500, int(limit)))
        decs = [d for d in (decisions or ["digest", "alert"]) if d]
        if not decs:
            decs = ["digest", "alert"]

        stmt = (
            select(
                Item.id,
                Item.title,
                Item.canonical_url,
                func.coalesce(Item.published_at, Item.created_at),
            )
            .select_from(ItemTopic)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(decs),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                    func.coalesce(Item.published_at, Item.created_at) < until,
                )
            )
            .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
            .limit(limit)
        )
        out: list[dict[str, str]] = []
        for item_id, title, url, when in self.session.execute(stmt).all():
            t = str(title or "").strip()
            u = str(url or "").strip()
            if not u:
                continue
            out.append(
                {
                    "item_id": str(int(item_id or 0)),
                    "title": t,
                    "url": u,
                    "published_at": when.isoformat() if when else "",
                }
            )
        return out

    def list_recent_sent_items_window(
        self,
        *,
        since: dt.datetime,
        until: dt.datetime,
        decisions: list[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, str]]:
        """
        List recently sent (digest/alert) items across ALL topics within a time window.

        Used as cross-topic anti-repeat context for scheduled batch composition.
        """
        limit = max(1, min(500, int(limit)))
        decs = [d for d in (decisions or ["digest", "alert"]) if d]
        if not decs:
            decs = ["digest", "alert"]

        # Pull a slightly larger window so we can de-dup URLs across topics in-process.
        pre_limit = min(2000, max(limit * 10, limit))
        stmt = (
            select(
                Item.id,
                Item.title,
                Item.canonical_url,
                func.coalesce(Item.published_at, Item.created_at),
            )
            .select_from(ItemTopic)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.decision.in_(decs),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                    func.coalesce(Item.published_at, Item.created_at) < until,
                )
            )
            .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
            .limit(pre_limit)
        )

        out: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        for item_id, title, url, when in self.session.execute(stmt).all():
            t = str(title or "").strip()
            u = str(url or "").strip()
            if not u or u in seen_urls:
                continue
            seen_urls.add(u)
            out.append(
                {
                    "item_id": str(int(item_id or 0)),
                    "title": t,
                    "url": u,
                    "published_at": when.isoformat() if when else "",
                }
            )
            if len(out) >= limit:
                break
        return out

    def list_item_urls_for_discovery(
        self,
        *,
        topic: Topic,
        since: dt.datetime,
        limit: int = 100,
        decisions: list[str] | None = None,
    ) -> list[str]:
        """
        List recent item URLs for a topic, used as seed pages for feed discovery.

        This enables dynamic source discovery even when a topic has no search sources
        (e.g., stream-only profile topics). Callers should still apply host/scheme filters.
        """
        limit = max(1, min(500, int(limit)))
        decs = [d for d in (decisions or ["candidate", "digest", "alert"]) if d]
        if not decs:
            decs = ["candidate", "digest", "alert"]

        stmt = (
            select(Item.canonical_url)
            .select_from(ItemTopic)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(decs),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                )
            )
            .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
            .limit(limit)
        )
        urls = [str(u or "").strip() for u in self.session.scalars(stmt).all()]
        return [u for u in urls if u]

    def list_item_topics_for_digest_window(
        self, *, topic: Topic, since: dt.datetime, until: dt.datetime
    ) -> list[tuple[ItemTopic, Item]]:
        stmt = (
            select(ItemTopic, Item)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(["digest", "alert"]),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                    func.coalesce(Item.published_at, Item.created_at) < until,
                )
            )
            .order_by(Item.created_at.desc())
        )
        return list(self.session.execute(stmt).all())

    def count_item_topics_for_digest_window(
        self, *, topic: Topic, since: dt.datetime, until: dt.datetime
    ) -> tuple[int, int]:
        stmt = (
            select(
                func.count(),
                func.sum(case((ItemTopic.decision == "alert", 1), else_=0)),
            )
            .select_from(ItemTopic)
            .join(Item, Item.id == ItemTopic.item_id)
            .where(
                and_(
                    ItemTopic.topic_id == topic.id,
                    ItemTopic.decision.in_(["digest", "alert"]),
                    func.coalesce(Item.published_at, Item.created_at) >= since,
                    func.coalesce(Item.published_at, Item.created_at) < until,
                )
            )
        )
        total, alerts = self.session.execute(stmt).one()
        return int(total or 0), int(alerts or 0)

    def list_recent_events(
        self,
        *,
        topic: Topic | None = None,
        decisions: list[str] | None = None,
        since: dt.datetime | None = None,
        limit: int = 100,
    ) -> list[tuple[ItemTopic, Item, Topic, Source]]:
        """
        List recent per-topic decisions ("events") joined with Item+Topic+Source.

        Ordering is by the item recency: coalesce(published_at, created_at) desc.
        """
        stmt = (
            select(ItemTopic, Item, Topic, Source)
            .join(Item, Item.id == ItemTopic.item_id)
            .join(Topic, Topic.id == ItemTopic.topic_id)
            .join(Source, Source.id == Item.source_id)
        )

        conds = []
        if topic is not None:
            conds.append(ItemTopic.topic_id == topic.id)
        if decisions:
            conds.append(ItemTopic.decision.in_(decisions))
        if since is not None:
            conds.append(func.coalesce(Item.published_at, Item.created_at) >= since)
        if conds:
            stmt = stmt.where(and_(*conds))

        stmt = stmt.order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
        if limit > 0:
            stmt = stmt.limit(int(limit))
        return list(self.session.execute(stmt).all())

    def prune_ignored(
        self,
        *,
        older_than: dt.datetime,
        delete_orphan_items: bool = True,
        dry_run: bool = True,
    ) -> dict[str, int]:
        """
        Prune old low-signal data to keep long-running SQLite DBs manageable.

        Current policy:
          - delete ItemTopic rows where decision='ignore' and created_at < older_than
          - optionally delete Items older than older_than that have no remaining ItemTopic rows

        Returns counts regardless of dry_run.
        """
        old_ignored = and_(ItemTopic.decision == "ignore", ItemTopic.created_at < older_than)

        item_topics_to_delete = int(
            self.session.scalar(
                select(func.count()).select_from(ItemTopic).where(old_ignored)
            )
            or 0
        )

        items_to_delete = 0
        if delete_orphan_items:
            # Orphan after prune means: no remaining ItemTopic that is NOT an old ignored row.
            remaining_exists = (
                select(ItemTopic.id)
                .where(and_(ItemTopic.item_id == Item.id, ~old_ignored))
                .exists()
            )
            items_to_delete = int(
                self.session.scalar(
                    select(func.count())
                    .select_from(Item)
                    .where(and_(Item.created_at < older_than, ~remaining_exists))
                )
                or 0
            )

        if dry_run:
            return {
                "item_topics_deleted": item_topics_to_delete,
                "items_deleted": items_to_delete,
            }

        self.session.execute(delete(ItemTopic).where(old_ignored))
        if delete_orphan_items:
            any_topic_exists = select(ItemTopic.id).where(ItemTopic.item_id == Item.id).exists()
            self.session.execute(delete(Item).where(and_(Item.created_at < older_than, ~any_topic_exists)))

        self.session.commit()
        return {
            "item_topics_deleted": item_topics_to_delete,
            "items_deleted": items_to_delete,
        }

    # --- pushes
    def was_pushed(self, *, channel: str, idempotency_key: str) -> bool:
        return (
            self.session.scalar(
                select(func.count())
                .select_from(PushLog)
                .where(and_(PushLog.channel == channel, PushLog.idempotency_key == idempotency_key))
            )
            > 0
        )

    def any_push_exists(self, *, idempotency_key: str) -> bool:
        return (
            self.session.scalar(
                select(func.count())
                .select_from(PushLog)
                .where(PushLog.idempotency_key == idempotency_key)
            )
            > 0
        )

    def any_push_sent(self, *, idempotency_key: str) -> bool:
        return (
            self.session.scalar(
                select(func.count())
                .select_from(PushLog)
                .where(and_(PushLog.idempotency_key == idempotency_key, PushLog.status == "sent"))
            )
            > 0
        )

    def any_push_sent_with_prefix(self, *, idempotency_prefix: str) -> bool:
        """
        Best-effort cross-topic dedupe helper.

        Example use: alerts are keyed as `alert:{item_id}:{topic_id}`.
        Checking `alert:{item_id}:` lets us avoid pushing the same item multiple
        times across different topics.
        """
        prefix = (idempotency_prefix or "").strip()
        if not prefix:
            return False
        return (
            self.session.scalar(
                select(func.count())
                .select_from(PushLog)
                .where(and_(PushLog.idempotency_key.like(f"{prefix}%"), PushLog.status == "sent"))
            )
            > 0
        )

    def any_push_exists_with_prefix(self, *, idempotency_prefix: str) -> bool:
        """
        Like `any_push_sent_with_prefix`, but counts ANY push attempt regardless of status.

        This is useful for cross-topic de-dupe in fast paths where two topics might try to
        send the same alert in close succession: reserving/creating a push row is enough
        to treat the item as "already attempted" and avoid duplicate notifications.
        """
        prefix = (idempotency_prefix or "").strip()
        if not prefix:
            return False
        return (
            self.session.scalar(
                select(func.count())
                .select_from(PushLog)
                .where(PushLog.idempotency_key.like(f"{prefix}%"))
            )
            > 0
        )

    def any_push_sent_with_prefix_excluding(
        self,
        *,
        idempotency_prefix: str,
        exclude_prefixes: list[str] | None = None,
    ) -> bool:
        """
        Like `any_push_sent_with_prefix`, but allows excluding sub-prefixes.

        Useful when multiple message types share a common prefix (prefix-based idempotency keys)
        and you only want to treat certain variants as "already pushed".
        """
        prefix = (idempotency_prefix or "").strip()
        if not prefix:
            return False
        stmt = (
            select(func.count())
            .select_from(PushLog)
            .where(and_(PushLog.idempotency_key.like(f"{prefix}%"), PushLog.status == "sent"))
        )
        for ex in exclude_prefixes or []:
            ex_prefix = (ex or "").strip()
            if not ex_prefix:
                continue
            stmt = stmt.where(~PushLog.idempotency_key.like(f"{ex_prefix}%"))
        return (self.session.scalar(stmt) or 0) > 0

    def reserve_push_attempt(
        self,
        *,
        channel: str,
        idempotency_key: str,
        max_attempts: int,
        allow_sent: bool = False,
    ) -> PushLog | None:
        existing = self.session.scalar(
            select(PushLog).where(
                and_(PushLog.channel == channel, PushLog.idempotency_key == idempotency_key)
            )
        )
        if existing:
            # `allow_sent=True` means the caller intends to replace/update a previously-sent payload
            # (e.g. Telegram edit-in-place for a canonical daily message). For these "update" semantics,
            # retries are non-spammy, so we do not enforce max_attempts.
            if allow_sent:
                existing.attempts += 1
                existing.status = "pending"
                existing.error = ""
                self.session.commit()
                return existing

            if existing.status == "sent":
                return None
            if existing.attempts >= max_attempts:
                return None
            existing.attempts += 1
            existing.status = "pending"
            existing.error = ""
            self.session.commit()
            return existing

        push = PushLog(
            channel=channel,
            idempotency_key=idempotency_key,
            status="pending",
            attempts=1,
        )
        self.session.add(push)
        try:
            self.session.commit()
            return push
        except IntegrityError:
            self.session.rollback()
            return None

    def list_telegram_message_ids_by_key(
        self,
        *,
        chat_id: str,
        idempotency_key: str,
        limit: int = 50,
    ) -> list[int]:
        """
        Return Telegram message ids for a specific pushed idempotency key.
        Ordered by message_id so multi-part messages are in send order.
        """
        cid = (chat_id or "").strip()
        key = (idempotency_key or "").strip()
        limit = max(1, min(500, int(limit)))
        if not (cid and key):
            return []
        stmt = (
            select(TelegramMessage.message_id)
            .where(and_(TelegramMessage.chat_id == cid, TelegramMessage.idempotency_key == key))
            .order_by(TelegramMessage.message_id.asc())
            .limit(limit)
        )
        return [int(m or 0) for m in self.session.scalars(stmt) if int(m or 0) > 0]

    def list_telegram_message_ids_by_prefix(
        self,
        *,
        chat_id: str,
        idempotency_prefix: str,
        limit: int = 200,
    ) -> list[int]:
        """
        Return Telegram message ids for pushed content whose idempotency key starts with a prefix.

        Useful to clean up legacy/forced variants (prefix-based).
        """
        cid = (chat_id or "").strip()
        prefix = (idempotency_prefix or "").strip()
        limit = max(1, min(2000, int(limit)))
        if not (cid and prefix):
            return []
        stmt = (
            select(TelegramMessage.message_id)
            .where(and_(TelegramMessage.chat_id == cid, TelegramMessage.idempotency_key.like(f"{prefix}%")))
            .order_by(TelegramMessage.message_id.asc())
            .limit(limit)
        )
        return [int(m or 0) for m in self.session.scalars(stmt) if int(m or 0) > 0]

    def mark_push_sent(self, push: PushLog) -> None:
        push.status = "sent"
        push.sent_at = dt.datetime.utcnow()
        self.session.commit()

    def mark_push_failed(self, push: PushLog, *, error: str) -> None:
        push.status = "failed"
        push.error = (error or "")[:4000]
        self.session.commit()

    def list_pushes(
        self,
        *,
        channel: str | None = None,
        status: str | None = None,
        idempotency_key: str | None = None,
        limit: int = 50,
    ) -> list[PushLog]:
        stmt = select(PushLog).order_by(PushLog.created_at.desc(), PushLog.id.desc())
        if channel:
            stmt = stmt.where(PushLog.channel == channel)
        if status:
            stmt = stmt.where(PushLog.status == status)
        if idempotency_key:
            stmt = stmt.where(PushLog.idempotency_key == idempotency_key)
        if limit > 0:
            stmt = stmt.limit(int(limit))
        return list(self.session.scalars(stmt))

    # --- reports (digest/health archives)
    def upsert_report(
        self,
        *,
        kind: str,
        idempotency_key: str,
        title: str,
        markdown: str,
        topic_id: int | None = None,
    ) -> Report:
        existing = self.session.scalar(
            select(Report).where(and_(Report.kind == kind, Report.idempotency_key == idempotency_key))
        )
        if existing:
            existing.title = title
            existing.markdown = markdown
            if topic_id is not None:
                existing.topic_id = topic_id
            self.session.commit()
            return existing

        report = Report(
            kind=kind,
            idempotency_key=idempotency_key,
            topic_id=topic_id,
            title=title,
            markdown=markdown,
        )
        self.session.add(report)
        self.session.commit()
        return report

    def get_report_by_key(self, *, kind: str, idempotency_key: str) -> Report | None:
        return self.session.scalar(
            select(Report).where(and_(Report.kind == kind, Report.idempotency_key == idempotency_key))
        )

    def get_report_by_id(self, report_id: int) -> Report | None:
        return self.session.get(Report, report_id)

    def list_reports(
        self,
        *,
        kind: str | None = None,
        topic: Topic | None = None,
        limit: int = 50,
    ) -> list[tuple[Report, Topic | None]]:
        stmt: Select[tuple[Report, Topic | None]] = (
            select(Report, Topic)
            .outerjoin(Topic, Topic.id == Report.topic_id)
            .order_by(Report.created_at.desc(), Report.id.desc())
        )
        if kind:
            stmt = stmt.where(Report.kind == kind)
        if topic is not None:
            stmt = stmt.where(Report.topic_id == topic.id)
        if limit > 0:
            stmt = stmt.limit(int(limit))
        return list(self.session.execute(stmt).all())

    # --- source health
    def get_or_create_source_health(self, *, source_id: int) -> SourceHealth:
        health = self.session.scalar(select(SourceHealth).where(SourceHealth.source_id == source_id))
        if health:
            return health
        health = SourceHealth(source_id=source_id)
        self.session.add(health)
        self.session.flush()
        return health

    def get_source_health(self, *, source_id: int) -> SourceHealth | None:
        return self.session.scalar(select(SourceHealth).where(SourceHealth.source_id == source_id))

    # --- source candidates (dynamic discovery queue)
    def get_source_candidate_by_id(self, candidate_id: int) -> SourceCandidate | None:
        return self.session.get(SourceCandidate, candidate_id)

    def get_source_candidate(
        self, *, topic_id: int, source_type: str, url: str
    ) -> SourceCandidate | None:
        return self.session.scalar(
            select(SourceCandidate).where(
                and_(
                    SourceCandidate.topic_id == topic_id,
                    SourceCandidate.source_type == source_type,
                    SourceCandidate.url == url,
                )
            )
        )

    def add_source_candidate(
        self,
        *,
        topic_id: int,
        source_type: str,
        url: str,
        title: str = "",
        discovered_from_url: str = "",
    ) -> tuple[SourceCandidate, bool]:
        # For source candidates, keep the host intact (do NOT strip "www."):
        # some sites are `www`-only and the apex domain does not resolve.
        u_keep = canonicalize_url(url, strip_www=False)
        u = u_keep
        # If the candidate was discovered from a `www.` page but the feed URL uses the apex,
        # prefer the discovered host for reachability.
        try:
            if discovered_from_url:
                src = urlsplit((discovered_from_url or "").strip())
                dst = urlsplit(u_keep)
                src_host = (src.hostname or "").strip().lower()
                dst_host = (dst.hostname or "").strip().lower()
                if src_host.startswith("www.") and dst_host and dst_host == src_host[4:]:
                    u = urlunsplit((dst.scheme, src.netloc, dst.path, dst.query, dst.fragment))
        except Exception:
            u = u_keep

        # A secondary key that strips `www.` for ignore matching + de-dupe migration.
        u_strip = canonicalize_url(u, strip_www=True)
        is_globally_ignored = False
        try:
            raw_ignore = (self.get_app_config("source_candidate_ignore_urls") or "").strip()
            if raw_ignore:
                ignored: set[str] = set()
                for line in raw_ignore.splitlines():
                    s = (line or "").strip()
                    if not s or s.startswith("#"):
                        continue
                    try:
                        ignored.add(canonicalize_url(s, strip_www=True))
                        ignored.add(canonicalize_url(s, strip_www=False))
                    except Exception:
                        ignored.add(s)
                is_globally_ignored = (u in ignored) or (u_strip in ignored)
        except Exception:
            is_globally_ignored = False

        existing = self.get_source_candidate(topic_id=topic_id, source_type=source_type, url=u)
        # Migration: if an older candidate exists under the `www`-stripped key, upgrade it in-place.
        if not existing and u_strip and u_strip != u:
            try:
                old = self.get_source_candidate(topic_id=topic_id, source_type=source_type, url=u_strip)
            except Exception:
                old = None
            if old is not None:
                try:
                    # Ensure we don't violate the unique constraint.
                    conflict = self.get_source_candidate(topic_id=topic_id, source_type=source_type, url=u)
                    if conflict is None or int(getattr(conflict, "id", 0) or 0) == int(getattr(old, "id", 0) or 0):
                        old.url = u
                        self.session.commit()
                        existing = old
                except Exception:
                    try:
                        self.session.rollback()
                    except Exception:
                        pass
        if existing:
            existing.last_seen_at = dt.datetime.utcnow()
            existing.seen_count += 1
            if title and not existing.title:
                existing.title = title
            if discovered_from_url:
                existing.discovered_from_url = discovered_from_url
            if is_globally_ignored and str(getattr(existing, "status", "") or "") != "accepted":
                existing.status = "ignored"
            self.session.commit()
            return existing, False

        cand = SourceCandidate(
            topic_id=topic_id,
            source_type=source_type,
            url=u,
            title=title or "",
            discovered_from_url=discovered_from_url or "",
            status=("ignored" if is_globally_ignored else "new"),
            seen_count=1,
            last_seen_at=dt.datetime.utcnow(),
        )
        self.session.add(cand)
        self.session.commit()
        return cand, True

    def list_source_candidates(
        self,
        *,
        topic: Topic | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[tuple[SourceCandidate, Topic]]:
        stmt: Select[tuple[SourceCandidate, Topic]] = (
            select(SourceCandidate, Topic)
            .join(Topic, Topic.id == SourceCandidate.topic_id)
            .order_by(SourceCandidate.last_seen_at.desc(), SourceCandidate.id.desc())
        )
        if topic is not None:
            stmt = stmt.where(SourceCandidate.topic_id == topic.id)
        if status:
            stmt = stmt.where(SourceCandidate.status == status)
        if limit > 0:
            stmt = stmt.limit(int(limit))
        return list(self.session.execute(stmt).all())

    # --- llm usage (optional; cost observability)
    def add_llm_usage(
        self,
        *,
        kind: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        topic: str = "",
    ) -> LlmUsage:
        """
        Add a single LLM usage row to the current session.

        NOTE: This does NOT commit. Callers should commit in their normal transaction flow.
        """
        row = LlmUsage(
            kind=(kind or "").strip()[:64],
            model=(model or "").strip()[:200],
            topic=(topic or "").strip()[:200],
            prompt_tokens=max(0, int(prompt_tokens or 0)),
            completion_tokens=max(0, int(completion_tokens or 0)),
            total_tokens=max(0, int(total_tokens or 0)),
        )
        self.session.add(row)
        self.session.flush()
        return row

    def summarize_llm_usage(
        self,
        *,
        since: dt.datetime,
    ) -> dict:
        """
        Summarize LLM usage since a given timestamp (UTC, naive).
        """
        s = since
        totals = self.session.execute(
            select(
                func.count(LlmUsage.id),
                func.coalesce(func.sum(LlmUsage.prompt_tokens), 0),
                func.coalesce(func.sum(LlmUsage.completion_tokens), 0),
                func.coalesce(func.sum(LlmUsage.total_tokens), 0),
            ).where(LlmUsage.created_at >= s)
        ).one()

        by_kind_rows = self.session.execute(
            select(
                LlmUsage.kind,
                func.count(LlmUsage.id),
                func.coalesce(func.sum(LlmUsage.total_tokens), 0),
            )
            .where(LlmUsage.created_at >= s)
            .group_by(LlmUsage.kind)
            .order_by(func.coalesce(func.sum(LlmUsage.total_tokens), 0).desc())
        ).all()

        by_model_rows = self.session.execute(
            select(
                LlmUsage.model,
                func.count(LlmUsage.id),
                func.coalesce(func.sum(LlmUsage.total_tokens), 0),
            )
            .where(LlmUsage.created_at >= s)
            .group_by(LlmUsage.model)
            .order_by(func.coalesce(func.sum(LlmUsage.total_tokens), 0).desc())
        ).all()

        return {
            "calls": int(totals[0] or 0),
            "prompt_tokens": int(totals[1] or 0),
            "completion_tokens": int(totals[2] or 0),
            "total_tokens": int(totals[3] or 0),
            "by_kind": [
                {"kind": str(k or ""), "calls": int(c or 0), "total_tokens": int(t or 0)}
                for k, c, t in by_kind_rows
            ],
            "by_model": [
                {"model": str(m or ""), "calls": int(c or 0), "total_tokens": int(t or 0)}
                for m, c, t in by_model_rows
            ],
        }

    # --- stats
    def get_stats(self) -> dict[str, int]:
        def _count(stmt) -> int:
            return int(self.session.scalar(stmt) or 0)

        return {
            "topics_total": _count(select(func.count()).select_from(Topic)),
            "topics_enabled": _count(
                select(func.count()).select_from(Topic).where(Topic.enabled.is_(True))
            ),
            "sources_total": _count(select(func.count()).select_from(Source)),
            "sources_enabled": _count(
                select(func.count()).select_from(Source).where(Source.enabled.is_(True))
            ),
            "bindings_total": _count(select(func.count()).select_from(TopicSource)),
            "items_total": _count(select(func.count()).select_from(Item)),
            "item_topics_total": _count(select(func.count()).select_from(ItemTopic)),
            "pushes_total": _count(select(func.count()).select_from(PushLog)),
            "pushes_failed": _count(
                select(func.count()).select_from(PushLog).where(PushLog.status == "failed")
            ),
            "sources_in_backoff": _count(
                select(func.count())
                .select_from(SourceHealth)
                .where(and_(SourceHealth.next_fetch_at.is_not(None), SourceHealth.next_fetch_at > dt.datetime.utcnow()))
            ),
            "sources_with_errors": _count(
                select(func.count()).select_from(SourceHealth).where(SourceHealth.error_count > 0)
            ),
            "source_candidates_total": _count(select(func.count()).select_from(SourceCandidate)),
            "source_candidates_new": _count(
                select(func.count()).select_from(SourceCandidate).where(SourceCandidate.status == "new")
            ),
        }

    # --- Telegram message map (for reactions/replies)
    def record_telegram_messages(
        self,
        *,
        chat_id: str,
        idempotency_key: str,
        message_ids: list[int],
        kind: str = "",
        item_id: int | None = None,
    ) -> int:
        cid = (chat_id or "").strip()
        key = (idempotency_key or "").strip()
        if not (cid and key):
            return 0

        mids = [int(m) for m in (message_ids or []) if int(m or 0) > 0]
        if not mids:
            return 0

        inserted = 0
        for mid in mids:
            existing = self.session.scalar(
                select(TelegramMessage).where(and_(TelegramMessage.chat_id == cid, TelegramMessage.message_id == mid))
            )
            if existing:
                continue
            row = TelegramMessage(
                chat_id=cid,
                message_id=int(mid),
                idempotency_key=key,
                kind=(kind or "").strip(),
                item_id=(int(item_id) if item_id is not None else None),
            )
            self.session.add(row)
            inserted += 1

        if inserted:
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
                inserted = 0
        return inserted

    def ensure_telegram_messages_recorded(
        self,
        *,
        chat_id: str,
        idempotency_key: str,
        message_ids: list[int],
        kind: str = "",
        item_id: int | None = None,
    ) -> list[int]:
        mids = [int(m) for m in (message_ids or []) if int(m or 0) > 0]
        if not mids:
            raise ValueError("no telegram message ids to record")
        self.record_telegram_messages(
            chat_id=chat_id,
            idempotency_key=idempotency_key,
            message_ids=mids,
            kind=kind,
            item_id=item_id,
        )
        missing = [mid for mid in mids if self.get_telegram_message(chat_id=chat_id, message_id=mid) is None]
        if missing:
            raise RuntimeError(f"telegram message mapping missing for ids={missing}")
        return mids

    def get_telegram_message(self, *, chat_id: str, message_id: int) -> TelegramMessage | None:
        cid = (chat_id or "").strip()
        mid = int(message_id or 0)
        if not (cid and mid > 0):
            return None
        return self.session.scalar(
            select(TelegramMessage).where(and_(TelegramMessage.chat_id == cid, TelegramMessage.message_id == mid))
        )

    def delete_telegram_message(self, *, chat_id: str, message_id: int) -> bool:
        """
        Delete a Telegram message mapping row.

        Useful when the Telegram message no longer exists (e.g. deleted manually) and edit-in-place
        would otherwise fail forever.
        """
        cid = (chat_id or "").strip()
        mid = int(message_id or 0)
        if not (cid and mid > 0):
            return False
        res = self.session.execute(
            delete(TelegramMessage).where(and_(TelegramMessage.chat_id == cid, TelegramMessage.message_id == mid))
        )
        self.session.commit()
        try:
            return int(getattr(res, "rowcount", 0) or 0) > 0
        except Exception:
            return False

    # --- Telegram tasks (interactive workflows)
    def list_telegram_tasks(
        self,
        *,
        chat_id: str,
        kind: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[TelegramTask]:
        cid = (chat_id or "").strip()
        if not cid:
            return []
        limit = max(1, min(500, int(limit)))
        stmt = select(TelegramTask).where(TelegramTask.chat_id == cid)
        if kind:
            stmt = stmt.where(TelegramTask.kind == (kind or "").strip())
        if status:
            stmt = stmt.where(TelegramTask.status == (status or "").strip())
        stmt = stmt.order_by(TelegramTask.created_at.desc(), TelegramTask.id.desc()).limit(limit)
        return list(self.session.scalars(stmt))

    def get_telegram_task_by_prompt_message(
        self,
        *,
        chat_id: str,
        prompt_message_id: int,
        kind: str | None = None,
        status: str | None = None,
    ) -> TelegramTask | None:
        cid = (chat_id or "").strip()
        mid = int(prompt_message_id or 0)
        if not (cid and mid > 0):
            return None
        stmt = select(TelegramTask).where(and_(TelegramTask.chat_id == cid, TelegramTask.prompt_message_id == mid))
        if kind:
            stmt = stmt.where(TelegramTask.kind == (kind or "").strip())
        if status:
            stmt = stmt.where(TelegramTask.status == (status or "").strip())
        return self.session.scalar(stmt)

    def get_telegram_task_by_request_message(
        self,
        *,
        chat_id: str,
        request_message_id: int,
        kind: str | None = None,
        status: str | None = None,
    ) -> TelegramTask | None:
        cid = (chat_id or "").strip()
        mid = int(request_message_id or 0)
        if not (cid and mid > 0):
            return None
        stmt = select(TelegramTask).where(and_(TelegramTask.chat_id == cid, TelegramTask.request_message_id == mid))
        if kind:
            stmt = stmt.where(TelegramTask.kind == (kind or "").strip())
        if status:
            stmt = stmt.where(TelegramTask.status == (status or "").strip())
        # request_message_id is not unique; pick the most recent match.
        stmt = stmt.order_by(TelegramTask.created_at.desc(), TelegramTask.id.desc()).limit(1)
        return self.session.scalar(stmt)

    def cancel_telegram_tasks(
        self,
        *,
        chat_id: str,
        kind: str,
        status: str,
        reason: str = "",
    ) -> int:
        cid = (chat_id or "").strip()
        k = (kind or "").strip()
        st = (status or "").strip()
        if not (cid and k and st):
            return 0
        rows = list(
            self.session.scalars(
                select(TelegramTask).where(and_(TelegramTask.chat_id == cid, TelegramTask.kind == k, TelegramTask.status == st))
            )
        )
        if not rows:
            return 0
        now = dt.datetime.utcnow()
        for r in rows:
            r.status = "canceled"
            r.error = (reason or "")[:4000]
            r.finished_at = now
        self.session.commit()
        return len(rows)

    def create_telegram_task(
        self,
        *,
        chat_id: str,
        user_id: str,
        kind: str,
        status: str,
        prompt_message_id: int,
        request_message_id: int,
        item_id: int | None = None,
        url: str = "",
        query: str,
    ) -> TelegramTask:
        cid = (chat_id or "").strip()
        prompt_mid = int(prompt_message_id or 0)
        # `prompt_message_id` is unique per chat. Many background workflows queue with a
        # temporary negative placeholder before the real Telegram message exists, so make
        # placeholder creation retry-safe instead of failing the whole workflow on collision.
        retryable_placeholder = prompt_mid <= 0
        if retryable_placeholder and prompt_mid == 0:
            prompt_mid = -int(dt.datetime.utcnow().timestamp() * 1_000_000)

        last_exc: IntegrityError | None = None
        for attempt in range(5):
            current_prompt_mid = prompt_mid - attempt if retryable_placeholder else prompt_mid
            row = TelegramTask(
                chat_id=cid,
                user_id=(user_id or "").strip(),
                kind=(kind or "").strip(),
                status=(status or "").strip(),
                prompt_message_id=int(current_prompt_mid),
                request_message_id=int(request_message_id or 0),
                item_id=(int(item_id) if item_id is not None else None),
                url=str(url or ""),
                query=str(query or ""),
            )
            self.session.add(row)
            try:
                self.session.commit()
                return row
            except IntegrityError as exc:
                self.session.rollback()
                last_exc = exc
                if not retryable_placeholder or attempt >= 4:
                    raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("telegram task creation failed")

    def mark_telegram_task_choice(
        self,
        task_id: int,
        *,
        option: int,
        intent: str,
        budget_seconds: int,
        provider: str = "",
    ) -> TelegramTask | None:
        row = self.session.get(TelegramTask, int(task_id or 0))
        if not row:
            return None
        row.option = int(option or 0)
        row.intent = (intent or "")[:4000]
        row.budget_seconds = int(budget_seconds or 0)
        if provider:
            row.provider = (provider or "").strip()
        row.status = "pending"
        row.result_key = ""
        row.error = ""
        row.started_at = None
        row.finished_at = None
        self.session.commit()
        return row

    def claim_next_pending_telegram_task(
        self,
        *,
        kind: str,
        status: str = "pending",
        mark_running: bool = True,
        stale_running_seconds: int = 0,
        provider: str | None = None,
        stale_provider: str | None = None,
    ) -> TelegramTask | None:
        k = (kind or "").strip()
        st = (status or "").strip() or "pending"
        if not k:
            return None

        now = dt.datetime.utcnow()
        pending_filters = [TelegramTask.kind == k, TelegramTask.status == st]
        if provider is not None:
            pending_filters.append(TelegramTask.provider == (provider or "").strip())
        stmt = select(TelegramTask).where(and_(*pending_filters)).order_by(TelegramTask.created_at.asc(), TelegramTask.id.asc()).limit(1)
        row = self.session.scalar(stmt)

        stale_seconds = max(0, int(stale_running_seconds or 0))
        if row is None and stale_seconds > 0:
            cutoff = now - dt.timedelta(seconds=stale_seconds)
            stale_filters = [
                TelegramTask.kind == k,
                TelegramTask.status == "running",
                or_(TelegramTask.started_at.is_(None), TelegramTask.started_at < cutoff),
            ]
            if stale_provider is not None:
                stale_filters.append(TelegramTask.provider == (stale_provider or "").strip())
            stale_stmt = select(TelegramTask).where(and_(*stale_filters)).order_by(TelegramTask.started_at.asc(), TelegramTask.id.asc()).limit(1)
            row = self.session.scalar(stale_stmt)

        if not row:
            return None
        if mark_running:
            row.status = "running"
            row.started_at = now
            row.finished_at = None
            self.session.commit()
        return row

    def mark_telegram_task_done(self, task_id: int, *, result_key: str) -> TelegramTask | None:
        row = self.session.get(TelegramTask, int(task_id or 0))
        if not row:
            return None
        row.status = "done"
        row.result_key = (result_key or "").strip()
        row.error = ""
        row.finished_at = dt.datetime.utcnow()
        self.session.commit()
        return row

    def set_telegram_task_prompt_message(self, task_id: int, *, prompt_message_id: int) -> TelegramTask | None:
        row = self.session.get(TelegramTask, int(task_id or 0))
        mid = int(prompt_message_id or 0)
        if not row or mid == 0:
            return row
        row.prompt_message_id = mid
        self.session.commit()
        return row

    def mark_telegram_task_canceled(self, task_id: int, *, reason: str = "") -> TelegramTask | None:
        row = self.session.get(TelegramTask, int(task_id or 0))
        if not row:
            return None
        row.status = "canceled"
        msg = (reason or "").strip()
        row.error = msg[:4000]
        row.finished_at = dt.datetime.utcnow()
        self.session.commit()
        return row

    def mark_telegram_task_failed(self, task_id: int, *, error: str) -> TelegramTask | None:
        row = self.session.get(TelegramTask, int(task_id or 0))
        if not row:
            return None
        row.status = "failed"
        msg = (error or "").strip()
        row.error = msg[:4000]
        row.finished_at = dt.datetime.utcnow()
        self.session.commit()
        return row

    # --- feedback events
    def add_feedback_event(
        self,
        *,
        channel: str,
        user_id: str,
        chat_id: str,
        message_id: int | None,
        kind: str,
        value_int: int = 0,
        item_id: int | None = None,
        url: str = "",
        domain: str = "",
        note: str = "",
        raw: str = "",
    ) -> FeedbackEvent:
        ev = FeedbackEvent(
            channel=(channel or "").strip() or "telegram",
            user_id=(user_id or "").strip(),
            chat_id=(chat_id or "").strip(),
            message_id=(int(message_id) if message_id is not None else None),
            kind=(kind or "").strip(),
            value_int=int(value_int or 0),
            item_id=(int(item_id) if item_id is not None else None),
            url=str(url or ""),
            domain=str(domain or ""),
            note=str(note or ""),
            raw=str(raw or ""),
        )
        self.session.add(ev)
        self.session.commit()
        return ev

    def list_pending_feedback_events(
        self,
        *,
        limit: int = 50,
        kinds: list[str] | None = None,
    ) -> list[FeedbackEvent]:
        limit = max(1, min(500, int(limit)))
        stmt = select(FeedbackEvent).where(FeedbackEvent.applied_at.is_(None))
        if kinds:
            allowed = sorted({str(k or "").strip() for k in kinds if str(k or "").strip()})
            if allowed:
                stmt = stmt.where(FeedbackEvent.kind.in_(allowed))
        stmt = stmt.order_by(FeedbackEvent.created_at.asc(), FeedbackEvent.id.asc()).limit(limit)
        return list(self.session.scalars(stmt))

    def mark_feedback_events_applied(self, *, ids: list[int]) -> int:
        now = dt.datetime.utcnow()
        unique_ids = sorted({int(i) for i in (ids or []) if int(i or 0) > 0})
        if not unique_ids:
            return 0
        stmt = select(FeedbackEvent).where(FeedbackEvent.id.in_(unique_ids))
        rows = list(self.session.scalars(stmt))
        for r in rows:
            r.applied_at = now
        self.session.commit()
        return len(rows)

    def summarize_feedback_by_domain(
        self,
        *,
        domains: list[str],
        since: dt.datetime | None = None,
        kinds: list[str] | None = None,
        limit: int = 200,
    ) -> dict[str, dict[str, int]]:
        """
        Aggregate feedback counts by domain.

        This is used to:
        - reduce repeat low-quality domains in LLM triage/curation, and
        - drive simple operator UX (e.g., "mute this domain?" suggestions).
        """
        doms = sorted({str(d or "").strip().lower() for d in (domains or []) if str(d or "").strip()})
        if not doms:
            return {}

        allowed_kinds = {str(k or "").strip() for k in (kinds or []) if str(k or "").strip()}
        stmt = (
            select(FeedbackEvent.domain, FeedbackEvent.kind, func.count())
            .where(FeedbackEvent.domain.in_(doms))
        )
        if since is not None:
            stmt = stmt.where(FeedbackEvent.created_at >= since)
        if allowed_kinds:
            stmt = stmt.where(FeedbackEvent.kind.in_(sorted(allowed_kinds)))
        stmt = stmt.group_by(FeedbackEvent.domain, FeedbackEvent.kind).limit(max(1, min(2000, int(limit or 2000))))

        out: dict[str, dict[str, int]] = {}
        for domain, kind, cnt in self.session.execute(stmt).all():
            d = str(domain or "").strip().lower()
            k = str(kind or "").strip()
            if not d or not k:
                continue
            bucket = out.setdefault(d, {})
            try:
                bucket[k] = int(cnt or 0)
            except Exception:
                bucket[k] = 0
        return out

    # --- mute rules
    def upsert_mute_rule(
        self,
        *,
        scope: str,
        key: str,
        topic_name: str = "",
        muted_until: dt.datetime,
        reason: str = "",
    ) -> MuteRule:
        sc = (scope or "").strip() or "domain"
        k = (key or "").strip().lower()
        tn = (topic_name or "").strip()
        existing = self.session.scalar(
            select(MuteRule).where(and_(MuteRule.scope == sc, MuteRule.key == k, MuteRule.topic_name == tn))
        )
        if existing:
            existing.muted_until = muted_until
            existing.reason = (reason or "")[:4000]
            self.session.commit()
            return existing
        row = MuteRule(scope=sc, key=k, topic_name=tn, muted_until=muted_until, reason=(reason or "")[:4000])
        self.session.add(row)
        self.session.commit()
        return row

    def is_muted(self, *, scope: str, key: str, topic_name: str = "", when: dt.datetime | None = None) -> bool:
        sc = (scope or "").strip() or "domain"
        k = (key or "").strip().lower()
        tn = (topic_name or "").strip()
        if not k:
            return False
        ref = when or dt.datetime.utcnow()
        return (
            (self.session.scalar(
                select(func.count())
                .select_from(MuteRule)
                .where(and_(MuteRule.scope == sc, MuteRule.key == k, MuteRule.topic_name == tn, MuteRule.muted_until > ref))
            ) or 0)
            > 0
        )

    def list_active_mute_rules(self, *, when: dt.datetime | None = None, limit: int = 200) -> list[MuteRule]:
        ref = when or dt.datetime.utcnow()
        limit = max(1, min(1000, int(limit)))
        stmt = (
            select(MuteRule)
            .where(MuteRule.muted_until > ref)
            .order_by(MuteRule.muted_until.desc(), MuteRule.id.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def delete_mute_rule(self, *, scope: str, key: str, topic_name: str = "") -> bool:
        sc = (scope or "").strip() or "domain"
        k = (key or "").strip().lower()
        tn = (topic_name or "").strip()
        if not k:
            return False
        row = self.session.scalar(
            select(MuteRule).where(and_(MuteRule.scope == sc, MuteRule.key == k, MuteRule.topic_name == tn))
        )
        if not row:
            return False
        self.session.delete(row)
        self.session.commit()
        return True

    # --- profile revisions (audit)
    def add_profile_revision(
        self,
        *,
        kind: str,
        core_prompt: str,
        delta_prompt: str,
        effective_prompt: str,
        note: str = "",
        applied_feedback_ids: list[int] | None = None,
    ) -> ProfileRevision:
        ids = sorted({int(i) for i in (applied_feedback_ids or []) if int(i or 0) > 0})
        row = ProfileRevision(
            kind=(kind or "").strip() or "delta",
            core_prompt=str(core_prompt or ""),
            delta_prompt=str(delta_prompt or ""),
            effective_prompt=str(effective_prompt or ""),
            note=str(note or ""),
            applied_feedback_ids=",".join(str(i) for i in ids),
        )
        self.session.add(row)
        self.session.commit()
        return row

    # --- settings changes (audit)
    def add_settings_change(
        self,
        *,
        source: str,
        fields: list[str] | None = None,
        env_keys: list[str] | None = None,
        restart_required: bool = False,
        actor: str = "",
        client_host: str = "",
    ) -> SettingsChange:
        fs = sorted({str(f or "").strip() for f in (fields or []) if str(f or "").strip()})
        ks = sorted({str(k or "").strip() for k in (env_keys or []) if str(k or "").strip()})
        row = SettingsChange(
            source=(source or "").strip()[:64],
            actor=(actor or "").strip()[:64],
            client_host=(client_host or "").strip()[:128],
            fields=",".join(fs),
            env_keys=",".join(ks),
            restart_required=bool(restart_required),
        )
        self.session.add(row)
        self.session.commit()
        return row

    def list_settings_changes(self, *, limit: int = 50) -> list[SettingsChange]:
        limit = max(1, min(500, int(limit)))
        stmt = select(SettingsChange).order_by(SettingsChange.id.desc()).limit(limit)
        return list(self.session.scalars(stmt))

    # --- config agent runs (audit; non-secret)
    def add_config_agent_run(
        self,
        *,
        kind: str = "tracking_ai_setup",
        status: str = "planned",
        actor: str = "",
        client_host: str = "",
        user_prompt: str = "",
        plan_json: str = "",
        preview_markdown: str = "",
        snapshot_before_json: str = "",
        snapshot_preview_json: str = "",
        snapshot_after_json: str = "",
        error: str = "",
    ) -> ConfigAgentRun:
        row = ConfigAgentRun(
            kind=(kind or "").strip()[:32] or "tracking_ai_setup",
            status=(status or "").strip()[:16] or "planned",
            actor=(actor or "").strip()[:64],
            client_host=(client_host or "").strip()[:128],
            user_prompt=str(user_prompt or ""),
            plan_json=str(plan_json or ""),
            preview_markdown=str(preview_markdown or ""),
            snapshot_before_json=str(snapshot_before_json or ""),
            snapshot_preview_json=str(snapshot_preview_json or ""),
            snapshot_after_json=str(snapshot_after_json or ""),
            error=str(error or ""),
        )
        self.session.add(row)
        self.session.commit()
        return row

    def update_config_agent_run(
        self,
        run_id: int,
        *,
        status: str | None = None,
        snapshot_after_json: str | None = None,
        error: str | None = None,
    ) -> ConfigAgentRun:
        row = self.get_config_agent_run(run_id)
        if not row:
            raise ValueError(f"config agent run not found: {run_id}")
        if status is not None:
            row.status = (status or "").strip()[:16]
        if snapshot_after_json is not None:
            row.snapshot_after_json = str(snapshot_after_json or "")
        if error is not None:
            row.error = str(error or "")
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        self.session.refresh(row)
        return row

    def get_config_agent_run(self, run_id: int) -> ConfigAgentRun | None:
        try:
            rid = int(run_id)
        except Exception:
            return None
        if rid <= 0:
            return None
        return self.session.get(ConfigAgentRun, rid)

    def list_config_agent_runs(self, *, kind: str = "tracking_ai_setup", limit: int = 30) -> list[ConfigAgentRun]:
        limit = max(1, min(200, int(limit)))
        k = (kind or "").strip()[:32] or "tracking_ai_setup"
        stmt = select(ConfigAgentRun).where(ConfigAgentRun.kind == k).order_by(ConfigAgentRun.id.desc()).limit(limit)
        return list(self.session.scalars(stmt))
