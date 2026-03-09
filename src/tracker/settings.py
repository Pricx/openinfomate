from __future__ import annotations

import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TRACKER_", env_file=".env", extra="ignore")

    db_url: str = Field(default="sqlite:///./tracker.db")
    env_path: str = ".env"

    # Bootstrap / safety
    # If enabled, OpenInfoMate will allow starting the API bound to a non-loopback host
    # without auth (admin password / api token). This is intended for first-run setup
    # behind localhost-only exposure (e.g. Docker port binding to 127.0.0.1).
    bootstrap_allow_no_auth: bool = False

    # Logging
    log_level: str = "INFO"

    # Admin auth (optional; recommended when binding to non-localhost)
    admin_username: str = "admin"
    admin_password: str | None = None
    admin_allow_remote_env_update: bool = False

    # Service control (systemd --user)
    # The Web Admin "Restart" button uses these unit names.
    #
    # Defaults keep backward compatibility with older installs that used `tracker*` units.
    # Operators running multiple instances can override these per instance via `.env`.
    systemd_user_unit_scheduler: str = "tracker"
    systemd_user_unit_api: str = "tracker-api"

    # Push: DingTalk
    push_dingtalk_enabled: bool = True
    dingtalk_webhook_url: str | None = None
    dingtalk_secret: str | None = None

    # Push: Generic webhook
    webhook_url: str | None = None

    # Push: Telegram
    push_telegram_enabled: bool = True
    telegram_bot_token: str | None = None
    telegram_bot_username: str | None = None  # e.g. TrackerHotBot
    telegram_chat_id: str | None = None  # optional fallback (preferred: connect via admin UI)
    # Optional "private bot" hardening: if set, only this Telegram user id may bind / issue commands.
    # If unset, the first successful connect will record an owner user id in app config.
    telegram_owner_user_id: str | None = None
    telegram_disable_preview: bool = True
    # Background polling for Telegram updates (commands, reactions, inline buttons).
    # 0 = disable.
    #
    # Default is intentionally low to keep inline keyboards responsive.
    telegram_connect_poll_seconds: int = 3
    # Telegram feedback controls (reactions + replies).
    #
    # These are non-secret operator preferences; they can be overridden via DB-backed dynamic config.
    telegram_feedback_reactions_enabled: bool = True
    telegram_feedback_replies_enabled: bool = True
    telegram_feedback_like_emojis: str = "👍,❤️,🔥,⭐,🌟"
    telegram_feedback_dislike_emojis: str = "👎,💩,😡,🤮,❌"
    telegram_feedback_mute_emojis: str = "🔕"
    # Use Telegram-native Reader UI (single message + inline buttons) for per-topic scheduled digests.
    telegram_digest_reader_enabled: bool = True
    # When using the Digest Reader, show per-item feedback buttons (👍/👎/🔕) inside the reader.
    telegram_digest_item_feedback_enabled: bool = True
    # Enable "prompt delta" proposals from reply feedback (auditable; requires Apply/Reject).
    telegram_prompt_delta_enabled: bool = True
    # Default prompt slot targeted by prompt-delta proposals (override via Web Admin / TG /env).
    telegram_prompt_delta_target_slot_id: str = "llm.curate_items.system"

    # Push: Email (SMTP)
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    smtp_starttls: bool = True
    smtp_use_ssl: bool = False
    email_from: str | None = None
    email_to: str | None = None  # comma-separated

    # Runtime
    alert_poll_seconds: int = 900  # 15 min
    cron_timezone: str = "+8"  # UTC offset (e.g. +8) or IANA name (e.g. UTC, Asia/Shanghai)
    # Output language for LLM-generated content and push/report formatting.
    # UI language is cookie-based; this is the server-side default used by background jobs.
    # Supported: en | zh
    output_language: str = "zh"
    # Web UI appearance.
    # When enabled, the admin/setup pages follow the browser/system light-dark scheme automatically.
    # Manual theme toggle from the top bar will disable follow-system until re-enabled in Config Center.
    ui_theme_follow_system: bool = True

    # Prompt templates (operator-configurable; DB-backed via dynamic config, optionally synced to `.env`).
    #
    # NOTE: This is NOT a secret. It is JSON stored as text.
    # - `prompt_templates_custom_json`: {"version":1,"templates":{...}}
    # - `prompt_template_bindings_json`: {"version":1,"bindings":{...}}
    prompt_templates_custom_json: str = ""
    prompt_template_bindings_json: str = ""
    # If a cron job is missed (e.g., service restart), run it on resume within this grace window.
    # Set to 0 to disable misfire catch-up.
    cron_misfire_grace_seconds: int = 21600  # 6h
    digest_scheduler_enabled: bool = True  # schedule per-topic digest jobs from Topic.digest_cron
    digest_push_enabled: bool = True  # when scheduled digests run, whether to push (still archived)
    # Curated Info (cross-topic batch) window.
    #
    # Curated Info runs on a fixed cadence derived from `digest_hours` (e.g. 2h => every 2 hours),
    # and looks back `digest_hours` when building each batch.
    digest_hours: int = 2
    health_report_cron: str = "0 8 * * *"  # UTC crontab (empty = disable)
    backup_cron: str = "0 3 * * *"  # UTC crontab (empty = disable)
    backup_dir: str = "./backups"
    backup_keep_days: int = 30
    http_timeout_seconds: int = 20
    push_max_attempts: int = 3
    push_retry_cron: str = ""  # empty = disable; cron schedule for retrying failed pushes
    push_retry_max_keys: int = 20
    max_concurrent_fetches: int = 10
    max_concurrent_fetches_per_host: int = 2
    max_concurrent_digests: int = 2
    host_min_interval_seconds: float = 0.0  # per-host min delay between requests (0 = disabled)

    # Full-text enrichment (optional)
    # If enabled, Tracker may fetch and extract full article text for top candidates before LLM curation.
    fulltext_enabled: bool = False
    fulltext_timeout_seconds: int = 20
    fulltext_max_chars: int = 60_000
    fulltext_max_fetches_per_topic: int = 8

    # Global domain filters (optional; comma-separated).
    # If include_domains is set, only matching hosts are considered (best-effort).
    # exclude_domains always wins (i.e., can block even if included).
    include_domains: str = ""
    # Default quality gate: filter out common low-signal content farms by domain.
    # Operators can clear/override in Config Center → Domain Filters.
    exclude_domains: str = "csdn.net"
    # Domain quality tiering (best-effort; used for push selection).
    # This does NOT filter content by "safety" categories; it only helps reduce low-quality sources.
    # Low-tier domains are soft down-ranked / reviewed more strictly, not hard-blocked by themselves.
    domain_quality_low_domains: str = "csdn.net"
    domain_quality_medium_domains: str = "cnblogs.com"
    domain_quality_high_domains: str = ""
    # Minimum tier for items to appear in pushed digests/alerts.
    # Allowed: low | medium | high
    domain_quality_min_tier_for_push: str = "low"

    # Source quality scoring gate (LLM-assisted).
    #
    # 0..100. Applied as a hard filter before pushing Curated Info and alerts.
    # Also used as the default acceptance threshold for auto-discovered sources.
    source_quality_min_score: int = 50

    # Legacy keyword prefilter (optional; not AI-native).
    # When disabled (default), bindings' `include_keywords` will not hard-filter ingestion in keywords mode.
    # LLM triage/curation (profile-driven) should decide relevance instead.
    include_keywords_prefilter_enabled: bool = False

    # Priority lane (optional; AI-native "must push" fast path).
    # Scans recent "candidate" items and promotes a few truly time-sensitive, high-impact signals to alerts,
    # so major model/tool releases don't get stuck waiting for scheduled batches.
    priority_lane_enabled: bool = True
    priority_lane_hours: int = 72
    priority_lane_pool_max_candidates: int = 200
    priority_lane_triage_keep_candidates: int = 20
    priority_lane_max_alert: int = 2

    # Dedupe policy
    simhash_lookback_days: int = 30  # 0 = scan all history (slow)
    # Prevent the same alert item from being pushed multiple times across different topics.
    alert_global_dedupe_enabled: bool = True

    # Source fetching policy (v1.2)
    rss_min_interval_seconds: int = 900
    hn_min_interval_seconds: int = 900
    searxng_min_interval_seconds: int = 3600
    discourse_min_interval_seconds: int = 900
    # Optional Discourse cookie header (for private categories / Cloudflare clearance).
    # Store as a raw `Cookie:` header value in `.env`. Never commit or export it.
    discourse_cookie: str = ""
    # Optional cookie jar (domain/url -> raw Cookie header value) for login-required sources.
    # Store in `.env` only; never commit or export.
    # Example: {"forum.example.com":"a=b; c=d", "github.com":"logged_in=yes; ..."}
    cookie_jar_json: str = ""
    # Discourse recall backstop: if a Discourse source was stale (service downtime), optionally
    # merge Top Daily RSS once on the next fetch to avoid missing older-but-important posts.
    # 0 disables the extra recall request.
    discourse_recall_top_rss_if_stale_seconds: int = 3600
    # Discourse RSS catch-up pages.
    # High-volume Discourse sites can move faster than one `latest.json` / `latest.rss` page, so we
    # keep a bounded multi-page recall window on latest feeds even when JSON works. This remains a
    # recall mechanism only; the LLM still decides relevance and final push decisions.
    discourse_rss_catchup_pages: int = 8
    source_disable_after_errors: int = 10
    source_backoff_base_seconds: int = 60
    source_backoff_max_seconds: int = 3600

    # API / Admin UI
    api_token: str | None = None
    api_host: str = "127.0.0.1"
    api_port: int = 8080

    # LLM (optional; OpenAI-compatible)
    # Enable by setting both `TRACKER_LLM_BASE_URL` and `TRACKER_LLM_MODEL`.
    llm_base_url: str | None = None
    llm_api_key: str | None = None
    # Optional HTTP proxy for the reasoning LLM provider (httpx `proxy=`). If empty, uses env/system proxy if any.
    llm_proxy: str = ""
    llm_model: str | None = None
    # Optional model routing:
    # - reasoning: for selection/curation (default: TRACKER_LLM_MODEL)
    # - mini: for pure compression/summarization tasks (default: TRACKER_LLM_MODEL)
    llm_model_reasoning: str | None = None
    llm_model_mini: str | None = None
    # Optional separate provider for mini/triage workloads (cheap, high-throughput).
    # If unset, mini tasks fall back to the main TRACKER_LLM_* provider.
    llm_mini_base_url: str | None = None
    llm_mini_api_key: str | None = None
    llm_mini_proxy: str = ""
    # Optional extra request body JSON merged into every OpenAI-compatible request.
    # Useful for providers that support knobs like "reasoning_effort".
    # Forbidden keys (ignored): model, messages, stream
    llm_extra_body_json: str = ""
    # Optional extra body override for mini provider/model requests (triage/compression).
    # If empty, mini requests fall back to `llm_extra_body_json`.
    llm_mini_extra_body_json: str = ""
    llm_timeout_seconds: int = 90
    llm_max_candidates_per_tick: int = 10
    llm_failure_alert_enabled: bool = True
    llm_failure_alert_threshold: int = 5
    llm_failure_alert_min_minutes: int = 10
    llm_failure_alert_cooldown_minutes: int = 180

    # Optional cost estimation (USD per 1M tokens).
    # If unset/0, Tracker will still record token counts (when the backend reports them),
    # but cost will be shown as "unknown" in CLI summaries.
    llm_price_input_per_million_usd: float = 0.0
    llm_price_output_per_million_usd: float = 0.0

    llm_digest_enabled: bool = False
    llm_digest_max_items: int = 20

    # LLM curation (optional; prompt-driven, not scoring-driven)
    # When enabled, topics with an enabled TopicPolicy will ingest items as "candidate",
    # then call the configured LLM once per topic to decide ignore|digest|alert.
    llm_curation_enabled: bool = True
    llm_curation_max_candidates: int = 30
    llm_curation_max_digest: int = 5
    llm_curation_max_alert: int = 2
    llm_curation_history_dedupe_days: int = 30
    # Reliability: when LLM curation is enabled but the backend is temporarily unavailable,
    # optionally "fail open" by selecting a small fallback digest so operators don't get silent days.
    llm_curation_fail_open: bool = False
    llm_curation_fail_open_max_digest: int = 3
    # Optional "triage" stage before full curation:
    # - Use a cheaper mini model to pre-filter a larger candidate pool down to a smaller set
    #   that is then passed to the main reasoning model for final ignore|digest|alert decisions.
    llm_curation_triage_enabled: bool = True
    llm_curation_triage_pool_max_candidates: int = 120
    # 0 = use llm_curation_max_candidates as the keep cap.
    llm_curation_triage_keep_candidates: int = 0

    # Retention / pruning (optional)
    # Empty cron disables scheduled pruning.
    prune_ignored_cron: str = ""
    prune_ignored_days: int = 180
    prune_keep_items: bool = False
    prune_vacuum: bool = False

    # Dynamic source discovery (optional)
    # Empty cron disables scheduled discovery.
    discover_sources_cron: str = "0 */2 * * *"
    discover_sources_enabled: bool = True
    discover_sources_max_results_per_topic: int = 50
    # Explore/exploit weights for SearxNG fallback queries in discover-sources.
    # Higher explore weight increases query diversity to avoid narrowing into an information bubble.
    # These weights are relative (not probabilities); defaults: explore 2 / exploit 8.
    discover_sources_explore_weight: int = 2
    discover_sources_exploit_weight: int = 8
    discover_sources_ai_enabled: bool = False
    discover_sources_ai_max_pages_per_topic: int = 2
    discover_sources_ai_max_html_chars: int = 50_000
    discover_sources_ai_max_feed_urls: int = 10

    # Optional self-hosted search backend for dynamic source discovery / Smart Config autofix.
    # If unset, web-search sources still work when their full URLs are provided.
    searxng_base_url: str = ""

    # Tracking AI Setup (Smart Config) budgets
    # Notes:
    # - `ai_setup_plan_max_tokens` controls LLM output budget for planning JSON.
    # - Input prompts can be arbitrarily long; Smart Config should transform/structure inputs
    #   instead of hard-truncating. These knobs bound worst-case work.
    # Keep this modest by default for reliability; Smart Config can still multi-pass and
    # auto-expand sources via discover-sources + candidates review.
    ai_setup_plan_max_tokens: int = 12_000
    ai_setup_transform_chunk_chars: int = 20_000
    ai_setup_transform_max_chunks: int = 60

    # Dynamic source discovery: optional auto-accept (prompt-driven)
    # If enabled, Tracker will use the configured LLM to decide which new RSS candidates
    # should be accepted and bound to each topic (bounded per topic per run).
    discover_sources_auto_accept_enabled: bool = True
    discover_sources_auto_accept_max_per_topic: int = 10
    discover_sources_auto_accept_preview_entries: int = 5
    # Total enabled content sources cap (auto-accept can evict low-scoring sources when full).
    discover_sources_max_sources_total: int = 500


def get_settings() -> Settings:
    env_path = os.getenv("TRACKER_ENV_PATH")
    if env_path:
        return Settings(_env_file=env_path)  # type: ignore[call-arg]
    return Settings()
