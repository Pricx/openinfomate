from __future__ import annotations

import datetime as dt

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    query: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    digest_cron: Mapped[str] = mapped_column(String(64), nullable=False, default="0 9 * * *")
    alert_keywords: Mapped[str] = mapped_column(Text, nullable=False, default="")
    alert_cooldown_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=120)
    alert_daily_cap: Mapped[int] = mapped_column(Integer, nullable=False, default=5)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    sources: Mapped[list["TopicSource"]] = relationship(back_populates="topic", cascade="all, delete-orphan")


class TopicPolicy(Base):
    __tablename__ = "topic_policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), nullable=False)

    # LLM curation: treat incoming items as candidates, then let LLM decide
    # ignore|digest|alert based on a prompt.
    llm_curation_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    llm_curation_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("topic_id", name="uq_topic_policies_topic_id"),)


class TopicGatePolicy(Base):
    __tablename__ = "topic_gate_policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), nullable=False)

    initial_min_score: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    candidate_convergence_mode: Mapped[str | None] = mapped_column(String(16), nullable=True, default=None)
    push_min_score: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    push_max_digest_items: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    push_max_alert_items: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    push_dedupe_strength: Mapped[str | None] = mapped_column(String(16), nullable=True, default=None)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("topic_id", name="uq_topic_gate_policies_topic_id"),)


class AppConfig(Base):
    __tablename__ = "app_config"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String(32), nullable=False)  # rss, scrape, hn, ...
    url: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # connector state (optional)
    etag: Mapped[str | None] = mapped_column(String(200), nullable=True, default=None)
    last_modified: Mapped[str | None] = mapped_column(String(200), nullable=True, default=None)
    last_checked_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)

    topics: Mapped[list["TopicSource"]] = relationship(back_populates="source", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("type", "url", name="uq_sources_type_url"),)


class SourceHealth(Base):
    __tablename__ = "source_health"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)

    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    last_error_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)
    last_success_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)
    next_fetch_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)

    __table_args__ = (UniqueConstraint("source_id", name="uq_source_health_source_id"),)


class SourceMeta(Base):
    __tablename__ = "source_meta"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)

    tags: Mapped[str] = mapped_column(Text, nullable=False, default="")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("source_id", name="uq_source_meta_source_id"),)


class SourceScore(Base):
    __tablename__ = "source_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)

    # 0..100 (LLM-derived; may be manually adjusted by operators).
    score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quality_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    relevance_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    novelty_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # auto|manual|feedback|import (best-effort; informational).
    origin: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")

    locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("source_id", name="uq_source_scores_source_id"),)


class SourceCandidateEval(Base):
    __tablename__ = "source_candidate_evals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("source_candidates.id"), nullable=False)

    score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quality_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    relevance_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    novelty_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    decision: Mapped[str] = mapped_column(String(16), nullable=False, default="")  # accept|ignore|skip
    why: Mapped[str] = mapped_column(Text, nullable=False, default="")
    model: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    explore_weight: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    exploit_weight: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("candidate_id", name="uq_source_candidate_evals_candidate_id"),)


class SourceCandidate(Base):
    __tablename__ = "source_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), nullable=False)

    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default="rss")
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    discovered_from_url: Mapped[str] = mapped_column(Text, nullable=False, default="")

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="new")  # new|accepted|ignored
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_seen_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("topic_id", "source_type", "url", name="uq_source_candidates_topic_type_url"),
    )


class TopicSource(Base):
    __tablename__ = "topic_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), nullable=False)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)

    # per-topic filter hints (v1 minimal)
    include_keywords: Mapped[str] = mapped_column(Text, nullable=False, default="")
    exclude_keywords: Mapped[str] = mapped_column(Text, nullable=False, default="")

    topic: Mapped["Topic"] = relationship(back_populates="sources")
    source: Mapped["Source"] = relationship(back_populates="topics")

    __table_args__ = (UniqueConstraint("topic_id", "source_id", name="uq_topic_sources_topic_source"),)


class Item(Base):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    published_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)

    content_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    simhash64: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (UniqueConstraint("canonical_url", name="uq_items_canonical_url"),)


class ItemContent(Base):
    __tablename__ = "item_contents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("items.id"), nullable=False)

    url: Mapped[str] = mapped_column(Text, nullable=False, default="")  # fetched url (after redirects)
    content_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")

    fetched_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("item_id", name="uq_item_contents_item_id"),)


class ItemTopic(Base):
    __tablename__ = "item_topics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("items.id"), nullable=False)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), nullable=False)

    decision: Mapped[str] = mapped_column(String(16), nullable=False)  # ignore|digest|alert
    relevance_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    novelty_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quality_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (UniqueConstraint("item_id", "topic_id", name="uq_item_topics_item_topic"),)


class PushLog(Base):
    __tablename__ = "pushes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)  # dingtalk|email
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")  # pending|sent|failed
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    sent_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)

    __table_args__ = (UniqueConstraint("channel", "idempotency_key", name="uq_pushes_channel_key"),)


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)  # digest|health
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)

    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topics.id"), nullable=True, default=None)
    title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    markdown: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("kind", "idempotency_key", name="uq_reports_kind_key"),)


class AlertBudget(Base):
    __tablename__ = "alert_budgets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), nullable=False)
    day: Mapped[str] = mapped_column(String(10), nullable=False)  # UTC date: YYYY-MM-DD

    sent_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sent_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)

    __table_args__ = (UniqueConstraint("topic_id", "day", name="uq_alert_budgets_topic_day"),)


class LlmUsage(Base):
    __tablename__ = "llm_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # High-level call type, e.g. curate_items|digest_summary|gate_alert.
    kind: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    model: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    topic: Mapped[str] = mapped_column(String(200), nullable=False, default="")

    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)


class TelegramMessage(Base):
    """
    Store Telegram message ids for pushed content so reactions/replies can be mapped back to items.

    Note: Telegram message ids are scoped to chat_id, so we unique on (chat_id, message_id).
    """

    __tablename__ = "telegram_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    message_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Push idempotency key that produced this message (e.g. alert:ITEM:TOPIC, digest:TOPIC:YYYY-MM-DD).
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="")  # alert|digest|misc

    # Optional: when the message corresponds to a single item (e.g. alert), record the item id.
    item_id: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (UniqueConstraint("chat_id", "message_id", name="uq_telegram_messages_chat_message_id"),)


class FeedbackEvent(Base):
    __tablename__ = "feedback_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    channel: Mapped[str] = mapped_column(String(32), nullable=False, default="telegram")
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    chat_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)

    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="")  # like|dislike|rate|mute|unmute
    value_int: Mapped[int] = mapped_column(Integer, nullable=False, default=0)  # rating, days, etc.

    item_id: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    domain: Mapped[str] = mapped_column(String(200), nullable=False, default="")

    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    raw: Mapped[str] = mapped_column(Text, nullable=False, default="")  # best-effort JSON

    applied_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)


class MuteRule(Base):
    __tablename__ = "mute_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    scope: Mapped[str] = mapped_column(String(32), nullable=False, default="domain")  # domain (v1)
    key: Mapped[str] = mapped_column(String(200), nullable=False, default="")  # e.g. forum.example.com
    topic_name: Mapped[str] = mapped_column(String(200), nullable=False, default="")  # v1: always ""

    muted_until: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("scope", "key", "topic_name", name="uq_mute_rules_scope_key_topic"),)


class ProfileRevision(Base):
    __tablename__ = "profile_revisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="delta")  # delta|manual
    core_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    delta_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    effective_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")

    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    applied_feedback_ids: Mapped[str] = mapped_column(Text, nullable=False, default="")  # comma-separated

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)


class SettingsChange(Base):
    """
    Audit log for operator-initiated Settings changes.

    Never store secret values here. We only record which fields/keys were changed,
    plus basic metadata for debugging.
    """

    __tablename__ = "settings_changes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source: Mapped[str] = mapped_column(String(64), nullable=False, default="")  # e.g. admin_patch, admin_apply_env, tg_wizard
    actor: Mapped[str] = mapped_column(String(64), nullable=False, default="")  # optional; e.g. username/user_id
    client_host: Mapped[str] = mapped_column(String(128), nullable=False, default="")

    fields: Mapped[str] = mapped_column(Text, nullable=False, default="")  # comma-separated Settings field names
    env_keys: Mapped[str] = mapped_column(Text, nullable=False, default="")  # comma-separated TRACKER_* env keys

    restart_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)


class ConfigAgentRun(Base):
    """
    Audit log for AI-assisted configuration changes.

    Never store secrets here. Payloads should be limited to tracking config (topics/sources/bindings)
    and other non-secret operator content.
    """

    __tablename__ = "config_agent_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # A stable logical "agent id" so we can reuse the same table for future agents.
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="tracking_ai_setup")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="planned")  # planned|applied|undone|restored|failed

    actor: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    client_host: Mapped[str] = mapped_column(String(128), nullable=False, default="")

    user_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    plan_json: Mapped[str] = mapped_column(Text, nullable=False, default="")
    preview_markdown: Mapped[str] = mapped_column(Text, nullable=False, default="")

    snapshot_before_json: Mapped[str] = mapped_column(Text, nullable=False, default="")
    snapshot_preview_json: Mapped[str] = mapped_column(Text, nullable=False, default="")
    snapshot_after_json: Mapped[str] = mapped_column(Text, nullable=False, default="")

    error: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )


class TelegramTask(Base):
    """
    Persist small interactive workflows driven by Telegram messages.

    Motivation: Telegram polling runs under the global `jobs` lock (to avoid SQLite contention),
    so long-running work must be queued and executed outside that lock.
    """

    __tablename__ = "telegram_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    chat_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")

    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")  # awaiting|pending|running|done|failed|canceled

    # For reply-based interaction: bot asks a question, user replies to that message.
    prompt_message_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    request_message_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Optional target (if the request is anchored to an item/message).
    item_id: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    url: Mapped[str] = mapped_column(Text, nullable=False, default="")

    query: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Human-selected intent/angle (may include short free-form text).
    intent: Mapped[str] = mapped_column(Text, nullable=False, default="")
    option: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    budget_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider: Mapped[str] = mapped_column(String(64), nullable=False, default="")  # optional override

    result_key: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")

    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True, default=None)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("chat_id", "prompt_message_id", name="uq_telegram_tasks_chat_prompt_mid"),
    )
