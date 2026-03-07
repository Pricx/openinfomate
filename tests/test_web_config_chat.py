from __future__ import annotations

from tracker.web.config_chat import (
    build_config_chat_bootstrap,
    build_web_config_chat_history_text,
    build_web_config_chat_page_context,
    normalize_web_config_chat_messages,
)


def test_build_config_chat_bootstrap_blocks_until_access_and_llm_ready():
    state = {
        "access_ok": False,
        "llm_ok": False,
        "config_access_url": "/admin?section=config#cfg-access",
        "wizard_url": "/setup/wizard",
        "page_id": "admin:overview",
    }

    bootstrap = build_config_chat_bootstrap(onboarding=state, section="overview", lang="zh")

    assert bootstrap["enabled"] is False
    assert bootstrap["blocked_reason"] == "access"
    assert any(action["href"].endswith("#cfg-access") for action in bootstrap["manual_actions"])
    assert "手动" not in bootstrap["intro_message"]


def test_build_config_chat_bootstrap_guides_profile_then_ai_setup():
    state = {
        "access_ok": True,
        "llm_ok": True,
        "profile_configured": False,
        "ai_setup_applied": False,
        "install_complete": False,
        "profile_url": "/setup/profile",
        "page_id": "admin:overview",
    }
    bootstrap = build_config_chat_bootstrap(onboarding=state, section="overview", lang="zh")
    labels = [action.get("label") for action in bootstrap["starter_actions"]]

    assert bootstrap["enabled"] is True
    assert bootstrap["blocked_reason"] == ""
    assert "画像" in bootstrap["intro_message"]
    assert any("画像" in str(label) for label in labels)

    state2 = dict(state)
    state2.update({
        "profile_configured": True,
        "ai_setup_applied": False,
        "ai_setup_url": "/admin?section=ai_setup",
    })
    bootstrap2 = build_config_chat_bootstrap(onboarding=state2, section="overview", lang="zh")
    labels2 = [action.get("label") for action in bootstrap2["starter_actions"]]
    assert any("追踪" in str(label) for label in labels2)
    assert any(action.get("href") == "/admin?section=ai_setup" for action in bootstrap2["starter_actions"] if action.get("kind") == "link")


def test_config_chat_history_and_page_context_are_bounded():
    raw = [
        {"role": "user", "text": "A" * 1200},
        {"role": "assistant", "text": "ok"},
        {"role": "noise", "text": "skip"},
    ]
    rows = normalize_web_config_chat_messages(raw, max_turns=5)
    assert len(rows) == 2
    assert rows[0]["role"] == "user"
    assert rows[0]["text"].endswith("…")

    history = build_web_config_chat_history_text(raw)
    assert "RECENT_CONVERSATION_HISTORY" in history
    assert "USER:" in history
    assert len(history) < 2200

    ctx = build_web_config_chat_page_context(
        page_id="admin:sources",
        section="sources",
        onboarding={"current_step_id": "profile_ai", "install_complete": False, "profile_configured": True, "ai_setup_applied": False, "push_ok": False},
    )
    assert "WEB_ADMIN_CONTEXT" in ctx
    assert "section: sources" in ctx
    assert "onboarding_current_step: profile_ai" in ctx


def test_build_config_chat_bootstrap_uses_concrete_placeholder():
    bootstrap = build_config_chat_bootstrap(
        onboarding={
            "access_ok": True,
            "llm_ok": True,
            "profile_configured": True,
            "ai_setup_applied": True,
            "install_complete": True,
            "page_id": "admin:sources",
        },
        section="sources",
        lang="zh",
    )

    assert "加入xxx的rss" in bootstrap["input_placeholder"]

