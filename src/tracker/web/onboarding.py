from __future__ import annotations

from typing import Any

from tracker.repo import Repo
from tracker.settings import Settings

ADVANCED_TOUR_DISMISSED_KEY = "onboarding_advanced_tour_dismissed"


def _app_bool(repo: Repo, key: str) -> bool:
    try:
        value = str(repo.get_app_config(key) or "").strip().lower()
    except Exception:
        value = ""
    return value in {"1", "true", "yes", "y", "on"}


def _app_str(repo: Repo, key: str) -> str:
    try:
        return str(repo.get_app_config(key) or "").strip()
    except Exception:
        return ""


def _effective_settings(repo: Repo, settings: Settings) -> Settings:
    try:
        from tracker.dynamic_config import effective_settings

        return effective_settings(repo=repo, settings=settings)
    except Exception:
        return settings


def _llm_provider_ok(*, repo: Repo, base_url: str, model: str, slot: str) -> bool:
    fingerprint = f"{base_url}|{model}".strip("|")
    if not fingerprint:
        return False
    return _app_bool(repo, f"llm_test_{slot}_last_ok") and _app_str(repo, f"llm_test_{slot}_last_fingerprint") == fingerprint


def build_onboarding_state(*, repo: Repo, settings: Settings, token: str | None = None, page_id: str = "") -> dict[str, Any]:
    eff = _effective_settings(repo, settings)

    admin_user = str(getattr(eff, "admin_username", "") or "").strip()
    admin_pw = str(getattr(eff, "admin_password", "") or "").strip()
    access_ok = bool(admin_user and admin_pw)

    reasoning_base = str(getattr(eff, "llm_base_url", "") or "").strip()
    reasoning_model = str(getattr(eff, "llm_model_reasoning", "") or getattr(eff, "llm_model", "") or "").strip()
    reasoning_key = bool(str(getattr(eff, "llm_api_key", "") or "").strip())
    reasoning_ready = bool(reasoning_base and reasoning_model and reasoning_key)
    reasoning_ok = reasoning_ready and _llm_provider_ok(
        repo=repo,
        base_url=reasoning_base,
        model=reasoning_model,
        slot="reasoning",
    )

    mini_base = str(getattr(eff, "llm_mini_base_url", "") or reasoning_base).strip()
    mini_model = str(
        getattr(eff, "llm_model_mini", "") or getattr(eff, "llm_model_reasoning", "") or getattr(eff, "llm_model", "") or ""
    ).strip()
    mini_key = bool(str(getattr(eff, "llm_mini_api_key", "") or getattr(eff, "llm_api_key", "") or "").strip())
    mini_configured = bool(str(getattr(eff, "llm_model_mini", "") or "").strip())
    mini_ready = bool(mini_base and mini_model and mini_key)
    mini_ok = (not mini_configured) or (mini_ready and _llm_provider_ok(repo=repo, base_url=mini_base, model=mini_model, slot="mini"))
    llm_ok = bool(reasoning_ok and mini_ok)

    profile_text = _app_str(repo, "profile_text")
    profile_configured = bool(profile_text)
    ai_setup_applied = any((str(getattr(run, "status", "") or "").strip() == "applied") for run in repo.list_config_agent_runs(kind="tracking_ai_setup", limit=50))
    profile_ai_ok = bool(profile_configured and ai_setup_applied)

    telegram_token_set = bool(str(getattr(eff, "telegram_bot_token", "") or "").strip())
    telegram_chat_id = _app_str(repo, "telegram_chat_id") or str(getattr(eff, "telegram_chat_id", "") or "").strip()
    telegram_connected = bool(telegram_chat_id)
    telegram_ok = bool(telegram_token_set and telegram_connected)
    dingtalk_ok = bool(getattr(eff, "push_dingtalk_enabled", False) and str(getattr(eff, "dingtalk_webhook_url", "") or "").strip())
    webhook_ok = bool(str(getattr(eff, "webhook_url", "") or "").strip())
    email_ok = bool(
        str(getattr(eff, "smtp_host", "") or "").strip()
        and str(getattr(eff, "email_from", "") or "").strip()
        and str(getattr(eff, "email_to", "") or "").strip()
    )
    push_ok = bool(telegram_ok or dingtalk_ok or webhook_ok or email_ok)

    theme_follow_system = bool(getattr(eff, "ui_theme_follow_system", True))

    steps = [
        {"id": "access", "ok": access_ok},
        {"id": "llm", "ok": llm_ok},
        {"id": "profile_ai", "ok": profile_ai_ok},
        {"id": "push", "ok": push_ok},
    ]
    current = next((step for step in steps if not step["ok"]), None)
    install_complete = all(bool(step["ok"]) for step in steps)
    advanced_tour_dismissed = _app_bool(repo, ADVANCED_TOUR_DISMISSED_KEY)

    token_qs = f"?token={token}" if token else ""
    admin_qs_prefix = f"?token={token}&section=" if token else "?section="
    step_urls = {
        "access": f"/admin{admin_qs_prefix}config#cfg-access",
        "llm": f"/admin{admin_qs_prefix}config#cfg-llm",
        "profile_ai": f"/setup/profile{token_qs}",
        "push": f"/admin{admin_qs_prefix}push",
    }

    for step in steps:
        step["url"] = step_urls.get(step["id"], "/setup/wizard" + token_qs)

    return {
        "page_id": str(page_id or "").strip(),
        "theme_follow_system": theme_follow_system,
        "access_ok": access_ok,
        "llm_ok": llm_ok,
        "reasoning_ok": reasoning_ok,
        "mini_ok": mini_ok,
        "mini_configured": mini_configured,
        "profile_configured": profile_configured,
        "ai_setup_applied": ai_setup_applied,
        "profile_ai_ok": profile_ai_ok,
        "telegram_token_set": telegram_token_set,
        "telegram_connected": telegram_connected,
        "telegram_ok": telegram_ok,
        "push_ok": push_ok,
        "steps": steps,
        "current_step_id": str(current.get("id") if current else ""),
        "current_step_url": str(current.get("url") if current else ("/setup/wizard" + token_qs)),
        "install_complete": install_complete,
        "show_install_guide": not install_complete,
        "show_advanced_tour": bool(install_complete and not advanced_tour_dismissed),
        "advanced_tour_dismissed": advanced_tour_dismissed,
        "wizard_url": "/setup/wizard" + token_qs,
        "profile_url": "/setup/profile" + token_qs,
        "ai_setup_url": f"/admin{admin_qs_prefix}ai_setup",
        "push_url": f"/admin{admin_qs_prefix}push",
        "topics_url": f"/admin{admin_qs_prefix}topics",
        "sources_url": f"/admin{admin_qs_prefix}sources",
        "bindings_url": f"/admin{admin_qs_prefix}bindings",
        "config_access_url": f"/admin{admin_qs_prefix}config#cfg-access",
        "config_llm_url": f"/admin{admin_qs_prefix}config#cfg-llm",
        "signature": "|".join(
            [
                f"access={int(access_ok)}",
                f"llm={int(llm_ok)}",
                f"profile_ai={int(profile_ai_ok)}",
                f"push={int(push_ok)}",
                f"complete={int(install_complete)}",
                f"tour={int(not advanced_tour_dismissed)}",
            ]
        ),
    }


__all__ = ["ADVANCED_TOUR_DISMISSED_KEY", "build_onboarding_state"]
