from __future__ import annotations

import json
from typing import Any

WEB_CONFIG_CHAT_STORAGE_KEY = "tracker_web_config_chat_v1"
_MAX_HISTORY_TURNS = 10
_MAX_HISTORY_CHARS = 900


def _is_zh(lang: str) -> bool:
    raw = str(lang or "").strip().lower()
    return raw.startswith("zh") or raw in {"中文", "简体中文", "繁體中文", "繁体中文"}


def _norm_text(value: object) -> str:
    return str(value or "").strip()


def _truncate(text: str, limit: int) -> str:
    value = _norm_text(text)
    if len(value) <= limit:
        return value
    return value[: max(0, int(limit) - 1)].rstrip() + "…"


def _link(label: str, href: str) -> dict[str, str]:
    return {"kind": "link", "label": str(label or "").strip(), "href": str(href or "").strip()}


def _prompt(label: str, prompt: str) -> dict[str, str]:
    return {"kind": "prompt", "label": str(label or "").strip(), "prompt": str(prompt or "").strip()}


def _section_title(section: str, *, zh: bool) -> str:
    sec = _norm_text(section).lower()
    mapping_zh = {
        "overview": "概览",
        "push": "推送中心",
        "ai_setup": "智能配置",
        "topics": "话题",
        "sources": "来源",
        "bindings": "绑定",
        "config": "配置中心",
        "prompts": "提示词",
        "run": "运行",
        "all": "全部",
    }
    mapping_en = {
        "overview": "Overview",
        "push": "Push Center",
        "ai_setup": "AI Setup",
        "topics": "Topics",
        "sources": "Sources",
        "bindings": "Bindings",
        "config": "Config Center",
        "prompts": "Prompts",
        "run": "Run",
        "all": "All",
    }
    return (mapping_zh if zh else mapping_en).get(sec, sec or ("管理台" if zh else "Admin"))


def normalize_web_config_chat_messages(raw: object, *, max_turns: int = _MAX_HISTORY_TURNS) -> list[dict[str, str]]:
    obj = raw
    if isinstance(raw, str):
        try:
            obj = json.loads(raw)
        except Exception:
            return []
    if not isinstance(obj, list):
        return []

    out: list[dict[str, str]] = []
    for row in obj:
        if not isinstance(row, dict):
            continue
        role = _norm_text(row.get("role")).lower()
        if role not in {"user", "assistant", "system"}:
            continue
        text = _truncate(_norm_text(row.get("text") or row.get("content")), _MAX_HISTORY_CHARS)
        if not text:
            continue
        out.append({"role": role, "text": text})
    if max_turns > 0:
        out = out[-int(max_turns) :]
    return out


def build_web_config_chat_history_text(raw: object) -> str:
    rows = normalize_web_config_chat_messages(raw)
    if not rows:
        return ""
    lines = ["RECENT_CONVERSATION_HISTORY:"]
    for row in rows:
        role = str(row.get("role") or "assistant").upper()
        text = _truncate(str(row.get("text") or ""), _MAX_HISTORY_CHARS)
        if text:
            lines.append(f"{role}: {text}")
    return "\n".join(lines).strip()


def build_web_config_chat_page_context(*, page_id: str, section: str, onboarding: dict[str, Any] | None = None) -> str:
    state = onboarding if isinstance(onboarding, dict) else {}
    lines = ["WEB_ADMIN_CONTEXT:"]
    pid = _norm_text(page_id)
    sec = _norm_text(section)
    if pid:
        lines.append(f"- page_id: {pid}")
    if sec:
        lines.append(f"- section: {sec}")
    if state:
        current_step = _norm_text(state.get("current_step_id"))
        if current_step:
            lines.append(f"- onboarding_current_step: {current_step}")
        lines.append(f"- install_complete: {bool(state.get('install_complete'))}")
        lines.append(f"- profile_configured: {bool(state.get('profile_configured'))}")
        lines.append(f"- ai_setup_applied: {bool(state.get('ai_setup_applied'))}")
        lines.append(f"- push_ok: {bool(state.get('push_ok'))}")
    return "\n".join(lines).strip()


def build_config_chat_bootstrap(
    *,
    onboarding: dict[str, Any] | None,
    section: str,
    lang: str,
) -> dict[str, Any]:
    zh = _is_zh(lang)
    state = onboarding if isinstance(onboarding, dict) else {}
    access_ok = bool(state.get("access_ok"))
    llm_ok = bool(state.get("llm_ok"))
    profile_configured = bool(state.get("profile_configured"))
    ai_setup_applied = bool(state.get("ai_setup_applied"))
    install_complete = bool(state.get("install_complete"))
    current_step_url = _norm_text(state.get("current_step_url")) or "/setup/wizard"
    section_title = _section_title(section, zh=zh)

    starter_actions: list[dict[str, str]] = []
    manual_actions: list[dict[str, str]] = []
    blocked_reason = ""
    intro = ""

    if not access_ok:
        blocked_reason = "access"
        intro = (
            "先完成 Web Admin 的 Basic 用户名与密码设置，然后我才能继续接管其它配置。"
            if zh
            else "Set the Web Admin Basic username and password first, then I can handle the rest of the configuration."
        )
        manual_actions = [
            _link("打开访问控制" if zh else "Open Access Control", _norm_text(state.get("config_access_url")) or "/admin?section=config#cfg-access"),
            _link("返回安装向导" if zh else "Back to Setup Wizard", _norm_text(state.get("wizard_url")) or "/setup/wizard"),
        ]
    elif not llm_ok:
        blocked_reason = "llm"
        intro = (
            "先完成主力 LLM（以及可选 mini）连通性测试；通过后我就能直接帮你配置画像、追踪、来源和安全设置。"
            if zh
            else "Finish the reasoning LLM connectivity test first (and mini too if configured). Once it passes, I can configure profile, tracking, sources, and safe settings directly."
        )
        manual_actions = [
            _link("打开 LLM 配置" if zh else "Open LLM Settings", _norm_text(state.get("config_llm_url")) or "/admin?section=config#cfg-llm"),
            _link("返回安装向导" if zh else "Back to Setup Wizard", _norm_text(state.get("wizard_url")) or "/setup/wizard"),
        ]
    elif not profile_configured:
        intro = (
            "现在可以直接把收藏夹导出文本、关注方向、常逛的网站或一段任意描述贴给我；我会先帮你重建画像，再继续配置追踪。"
            if zh
            else "You can now paste bookmark exports, interests, favorite sites, or any free-form description. I’ll rebuild the profile first, then continue with tracking configuration."
        )
        starter_actions = [
            _prompt("重建我的画像" if zh else "Rebuild my profile", "根据我接下来发的文本重建 Profile，并压缩成可持续更新的默认画像" if zh else "Rebuild my Profile from the text I send next, and compress it into the default delta-updatable profile"),
            _prompt("按收藏夹生成画像" if zh else "Build from bookmarks", "我会粘贴浏览器收藏夹导出内容，请根据这些内容生成我的兴趣画像" if zh else "I will paste browser bookmark exports. Build my interest profile from them"),
            _link("打开画像页" if zh else "Open Profile Setup", _norm_text(state.get("profile_url")) or "/setup/profile"),
        ]
    elif not ai_setup_applied:
        intro = (
            "画像已经有了。接下来你可以直接让我生成并应用追踪计划，或者补充你想加的站点 / 搜索 / 主题。"
            if zh
            else "Your profile is ready. Next, ask me to generate and apply tracking, or add the sites / searches / topics you want."
        )
        starter_actions = [
            _prompt("根据当前画像生成追踪" if zh else "Generate tracking from profile", "根据当前 Profile 生成并应用一套追踪配置，宁可少也不要凑数" if zh else "Generate and apply a tracking configuration from the current Profile; prefer quality over quantity"),
            _prompt("新增来源与绑定" if zh else "Add source + binding", "加入 linux.do 的 RSS 和搜索，并自动绑定到最相关的现有 topic；如果重复就复用更新" if zh else "Add linux.do RSS and search, then bind them to the most relevant existing topic; reuse and update if already present"),
            _link("打开智能配置" if zh else "Open AI Setup", _norm_text(state.get("ai_setup_url")) or "/admin?section=ai_setup"),
        ]
    else:
        intro = (
            f"这里是 {section_title} 的智能配置窗。你既可以直接让我改配置，也可以让我基于已缓存的参考消息 / collect / 条目做总结与解释；涉及改配置时我会先给可审阅计划，再应用。"
            if zh
            else f"This is the smart config window for {section_title}. You can ask for configuration changes or cache-first summaries/explanations over recent digests, collect messages, and pushed items. When something needs config changes, I will draft a reviewable plan before applying it."
        )
        if section == "sources":
            starter_actions = [
                _prompt("加来源" if zh else "Add sources", "加入 linux.do 的 RSS 与站内搜索，并自动绑定到最相关 topic；重复则复用" if zh else "Add linux.do RSS and site search, and bind them to the most relevant topic; reuse if duplicated"),
                _prompt("清理低质量来源" if zh else "Disable weak sources", "禁用最近表现差且分数低的来源，但保留用户明确偏好的站点" if zh else "Disable low-scoring weak sources from recent runs, but keep explicitly preferred sites"),
                _prompt("总结最近 24h 参考消息" if zh else "Summarize last 24h digests", "基于缓存数据，总结最近 24 小时的参考消息重点，并按重要性排序" if zh else "Using cached data only, summarize the last 24 hours of digest pushes and rank the most important points"),
            ]
        elif section == "bindings":
            starter_actions = [
                _prompt("调整绑定" if zh else "Adjust bindings", "把新加的来源绑定到最相关 topic，并删除明显重复或失效的绑定" if zh else "Bind newly added sources to the most relevant topics and remove obviously duplicated or stale bindings"),
                _prompt("只改过滤条件" if zh else "Tune filters", "收紧噪音大的绑定过滤条件，但不要违背 Profile 偏好" if zh else "Tighten noisy binding filters without violating the Profile preferences"),
                _prompt("解释某条已推送内容" if zh else "Explain one pushed item", "基于缓存数据，解释最近一条我点名的已推送内容为什么重要" if zh else "Using cached data only, explain why one specific recently pushed item matters"),
            ]
        elif section == "topics":
            starter_actions = [
                _prompt("调消息量" if zh else "Tune volume", "把某个 topic 的参考消息收紧一点：先给我 Topic Gate 调整建议，确认后再应用" if zh else "Tighten one topic a bit: propose Topic Gate changes first, then apply after confirmation"),
                _prompt("调质量" if zh else "Tune quality", "如果某个 topic 噪音偏多，请按 Topic Gate 帮我提高进入候选和进入推送的门槛" if zh else "If one topic is noisy, raise its initial-screening and push thresholds through Topic Gate"),
                _prompt("总结 19:00 arXiv 专题" if zh else "Summarize 19:00 arXiv collect", "基于缓存数据，总结最近一条 19:00 arXiv 专题里最值得看的论文和核心观点" if zh else "Using cached data only, summarize the most worthwhile papers and core ideas from the latest 19:00 arXiv collect"),
            ]
        elif section == "config":
            starter_actions = [
                _prompt("调调度" if zh else "Tune schedules", "把参考消息窗口改成 2 小时，并把相关调度调整到更适合高频追踪" if zh else "Change the curated-info window to 2 hours and tune the related schedules for higher-frequency tracking"),
                _prompt("调阈值" if zh else "Tune thresholds", "如果消息太多或太杂，请先给我一份 Topic Gate + 调度的调整建议，确认后再应用" if zh else "If volume or quality is off, propose a Topic Gate + scheduling adjustment first, then apply after confirmation"),
            ]
        else:
            starter_actions = [
                _prompt("新增主题" if zh else "Add a topic", "新增一个关注开源 AI Agent 记忆系统与代码库检索的主题，并自动补来源与绑定" if zh else "Add a topic for open-source AI agent memory systems and codebase retrieval, with sources and bindings"),
                _prompt("补强来源" if zh else "Expand sources", "为当前配置补充高质量来源，但宁可少也不要凑数" if zh else "Expand the current setup with high-quality sources; prefer fewer over filler"),
                _prompt("改配置" if zh else "Adjust settings", "把参考消息和 digest 相关配置改得更适合我这种高密度信息追踪用户" if zh else "Adjust curated-info and digest settings to better fit a high-density information-tracking workflow"),
            ]
        if not bool(state.get("push_ok")):
            starter_actions.append(_link("打开推送中心" if zh else "Open Push Center", _norm_text(state.get("push_url")) or "/admin?section=push"))

    if install_complete:
        guide = (
            "基础安装已完成；你现在既可以像和配置助手聊天一样直接改 profile / tracking / sources / bindings / 安全设置，也可以让我基于缓存下来的参考消息、collect 与条目做总结和解释。"
            if zh
            else "Base installation is complete. You can now use this both as a config assistant and as a cache-first analyst for recent digests, collect messages, and pushed items."
        )
    else:
        guide = (
            "安装还没全部完成。我会优先引导你推进当前步骤，并尽量把可自动化的配置直接做好。"
            if zh
            else "Installation is not complete yet. I’ll guide you through the current step first and automate the rest whenever it is safe."
        )

    if not manual_actions and current_step_url:
        manual_actions.append(_link("继续当前步骤" if zh else "Continue current step", current_step_url))

    return {
        "enabled": bool(access_ok and llm_ok),
        "blocked_reason": blocked_reason,
        "lang": "zh" if zh else "en",
        "section": _norm_text(section) or "overview",
        "section_title": section_title,
        "page_id": _norm_text(state.get("page_id")) or f"admin:{_norm_text(section) or 'overview'}",
        "storage_key": WEB_CONFIG_CHAT_STORAGE_KEY,
        "launcher_label": "智能配置" if zh else "Smart Config",
        "panel_title": "智能配置助手" if zh else "Smart Config Assistant",
        "input_placeholder": (
            "例如：减少某个 topic 的噪音；总结最近 24h 参考消息；解释某条已推送内容；先给方案再应用"
            if zh
            else "For example: reduce noise for one topic; summarize the last 24h digests; explain one pushed item; propose first, then apply"
        ),
        "send_label": "发送" if zh else "Send",
        "apply_label": "应用" if zh else "Apply",
        "discard_label": "丢弃" if zh else "Discard",
        "thinking_label": "规划中…" if zh else "Planning…",
        "applying_label": "应用中…" if zh else "Applying…",
        "intro_message": intro,
        "guide_message": guide,
        "starter_actions": starter_actions[:6],
        "manual_actions": manual_actions[:3],
        "auto_open": bool(access_ok and llm_ok and not install_complete),
    }


__all__ = [
    "WEB_CONFIG_CHAT_STORAGE_KEY",
    "build_config_chat_bootstrap",
    "build_web_config_chat_history_text",
    "build_web_config_chat_page_context",
    "normalize_web_config_chat_messages",
]
