from __future__ import annotations

import datetime as dt

from tracker.repo import Repo


def export_config(*, session) -> dict:
    repo = Repo(session)

    # NOTE: Secrets live in `.env` and are intentionally NOT exported.
    # This export is meant for "tracking config" portability: topics/sources/bindings + AI prompts + profile text.

    topics = []
    for t in repo.list_topics():
        topics.append(
            {
                "name": t.name,
                "query": t.query,
                "enabled": bool(t.enabled),
                "digest_cron": t.digest_cron,
                "alert_keywords": t.alert_keywords,
                "alert_cooldown_minutes": t.alert_cooldown_minutes,
                "alert_daily_cap": t.alert_daily_cap,
            }
        )

    topic_name_by_id = {int(t.id): t.name for t in repo.list_topics() if t and t.id is not None}
    topic_policies = []
    for p in repo.list_topic_policies():
        name = topic_name_by_id.get(int(p.topic_id))
        if not name:
            continue
        topic_policies.append(
            {
                "topic": name,
                "llm_curation_enabled": bool(p.llm_curation_enabled),
                "llm_curation_prompt": p.llm_curation_prompt,
            }
        )

    meta_map = {s.id: m for s, _h, m in repo.list_sources_with_health_and_meta() if m}
    sources = []
    for s in repo.list_sources():
        m = meta_map.get(s.id)
        sources.append(
            {
                "type": s.type,
                "url": s.url,
                "enabled": bool(s.enabled),
                "tags": m.tags if m else "",
                "notes": m.notes if m else "",
            }
        )

    bindings = []
    for t, s, ts in repo.list_topic_sources():
        bindings.append(
            {
                "topic": t.name,
                "source": {"type": s.type, "url": s.url},
                "include_keywords": ts.include_keywords,
                "exclude_keywords": ts.exclude_keywords,
            }
        )

    # App-level config (safe allowlist).
    app_config_keys = [
        "profile_topic_name",
        "profile_text",
        "profile_understanding",
        "profile_interest_axes",
        "profile_interest_keywords",
        "profile_retrieval_queries",
        "profile_prompt_core",
        "profile_prompt_delta",
        "topic_policy_presets_custom_json",
        "prompt_templates_custom_json",
        "prompt_template_bindings_json",
        "output_language",
        "telegram_chat_id",
        "telegram_owner_user_id",
        "telegram_feedback_mute_days_default",
    ]
    app_config = {}
    for k in app_config_keys:
        v = repo.get_app_config(k)
        if v is None:
            continue
        app_config[k] = v

    return {
        "version": 2,
        "exported_at": dt.datetime.utcnow().isoformat() + "Z",
        "topics": topics,
        "topic_policies": topic_policies,
        "sources": sources,
        "bindings": bindings,
        "app_config": app_config,
    }


def import_config(*, session, data: dict, update_existing: bool = False) -> dict[str, int]:
    repo = Repo(session)

    version = int(data.get("version") or 0)
    if version not in {1, 2}:
        raise ValueError("unsupported config version")

    topics_created = 0
    topics_updated = 0
    sources_created = 0
    sources_updated = 0
    bindings_created = 0
    bindings_updated = 0
    policies_created = 0
    policies_updated = 0
    app_config_created = 0
    app_config_updated = 0

    for t in data.get("topics") or []:
        name = (t.get("name") or "").strip()
        if not name:
            continue
        existing = repo.get_topic_by_name(name)
        if existing:
            if update_existing:
                existing.query = (t.get("query") or "")
                existing.enabled = bool(t.get("enabled", True))
                existing.digest_cron = (t.get("digest_cron") or existing.digest_cron)
                existing.alert_keywords = (t.get("alert_keywords") or "")
                existing.alert_cooldown_minutes = int(
                    t.get("alert_cooldown_minutes") or existing.alert_cooldown_minutes
                )
                existing.alert_daily_cap = int(t.get("alert_daily_cap") or existing.alert_daily_cap)
                session.commit()
                topics_updated += 1
            continue

        topic = repo.add_topic(
            name=name,
            query=(t.get("query") or ""),
            digest_cron=(t.get("digest_cron") or "0 9 * * *"),
        )
        topic.enabled = bool(t.get("enabled", True))
        topic.alert_keywords = (t.get("alert_keywords") or "")
        topic.alert_cooldown_minutes = int(t.get("alert_cooldown_minutes") or topic.alert_cooldown_minutes)
        topic.alert_daily_cap = int(t.get("alert_daily_cap") or topic.alert_daily_cap)
        session.commit()
        topics_created += 1

    # Policies (v2)
    for p in (data.get("topic_policies") or []) if version >= 2 else []:
        topic_name = (p.get("topic") or "").strip()
        if not topic_name:
            continue
        topic = repo.get_topic_by_name(topic_name)
        if not topic:
            continue
        existing = repo.get_topic_policy(topic_id=int(topic.id))
        enabled = p.get("llm_curation_enabled")
        prompt = p.get("llm_curation_prompt")

        if existing:
            if update_existing:
                if enabled is not None:
                    existing.llm_curation_enabled = bool(enabled)
                if prompt is not None:
                    existing.llm_curation_prompt = str(prompt or "")
                session.commit()
                policies_updated += 1
            continue

        pol = repo.upsert_topic_policy(
            topic_id=int(topic.id),
            llm_curation_enabled=(bool(enabled) if enabled is not None else None),
            llm_curation_prompt=(str(prompt or "") if prompt is not None else None),
        )
        _ = pol
        policies_created += 1

    for s in data.get("sources") or []:
        stype = (s.get("type") or "").strip()
        url = (s.get("url") or "").strip()
        if not stype or not url:
            continue

        existed = repo.get_source(type=stype, url=url)
        src = repo.add_source(type=stype, url=url)
        if existed is None:
            sources_created += 1

        changed = False
        enabled = bool(s.get("enabled", True))
        if src.enabled != enabled:
            src.enabled = enabled
            changed = True

        tags = s.get("tags")
        notes = s.get("notes")
        if tags is not None or notes is not None:
            repo.update_source_meta(source_id=src.id, tags=tags, notes=notes)
            changed = True

        if existed is not None and update_existing and changed:
            sources_updated += 1

    for b in data.get("bindings") or []:
        topic_name = (b.get("topic") or "").strip()
        src_obj = b.get("source") or {}
        stype = (src_obj.get("type") or "").strip()
        url = (src_obj.get("url") or "").strip()
        if not topic_name or not stype or not url:
            continue

        topic = repo.get_topic_by_name(topic_name)
        source = repo.get_source(type=stype, url=url)
        if not topic or not source:
            continue

        existed = repo.get_topic_source(topic_id=topic.id, source_id=source.id)
        ts = repo.bind_topic_source(topic=topic, source=source)
        if existed is None:
            bindings_created += 1

        include_keywords = b.get("include_keywords")
        exclude_keywords = b.get("exclude_keywords")
        if update_existing and (include_keywords is not None or exclude_keywords is not None):
            if include_keywords is not None:
                ts.include_keywords = include_keywords
            if exclude_keywords is not None:
                ts.exclude_keywords = exclude_keywords
            session.commit()
            if existed is not None:
                bindings_updated += 1

    # App config (v2, safe allowlist only).
    app_cfg = (data.get("app_config") or {}) if version >= 2 else {}
    if isinstance(app_cfg, dict):
        allow = {
            "profile_topic_name",
            "profile_text",
            "profile_understanding",
            "profile_interest_axes",
            "profile_interest_keywords",
            "profile_retrieval_queries",
            "profile_prompt_core",
            "profile_prompt_delta",
            "topic_policy_presets_custom_json",
            "prompt_templates_custom_json",
            "prompt_template_bindings_json",
            "output_language",
            "telegram_chat_id",
            "telegram_owner_user_id",
            "telegram_feedback_mute_days_default",
        }
        for k, v in app_cfg.items():
            key = (str(k or "")).strip()
            if not key or key not in allow:
                continue
            value = str(v or "")
            existed = repo.get_app_config(key)
            if existed is None:
                repo.set_app_config(key, value)
                app_config_created += 1
            else:
                if update_existing:
                    repo.set_app_config(key, value)
                    app_config_updated += 1

    return {
        "topics_created": topics_created,
        "topics_updated": topics_updated,
        "sources_created": sources_created,
        "sources_updated": sources_updated,
        "bindings_created": bindings_created,
        "bindings_updated": bindings_updated,
        "policies_created": policies_created,
        "policies_updated": policies_updated,
        "app_config_created": app_config_created,
        "app_config_updated": app_config_updated,
    }
