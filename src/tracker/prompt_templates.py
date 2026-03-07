from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Literal

from tracker.repo import Repo
from tracker.settings import Settings

logger = logging.getLogger(__name__)

PromptLanguage = Literal["zh", "en"]
PromptOutputFormat = Literal["text", "json"]

APP_CONFIG_PROMPT_TEMPLATES_KEY = "prompt_templates_custom_json"
APP_CONFIG_PROMPT_BINDINGS_KEY = "prompt_template_bindings_json"


def _normalize_lang(raw: str) -> PromptLanguage:
    v = (raw or "").strip()
    low = v.lower()
    if v in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语", "漢語"}:
        return "zh"
    if v in {"英文", "英语", "英語"}:
        return "en"
    if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
        return "zh"
    if low in {"en", "en-us", "en-gb", "english"} or low.startswith("en"):
        return "en"
    # Default to English to keep background jobs stable.
    return "en"


_PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_.:-]+)\s*\}\}")

_PROFILE_DEFAULT_MAX_CHARS = 4000


def _build_default_profile_text(*, repo: Repo) -> str:
    """
    Build the default `{{profile}}` prompt context (compressed, delta-aware).

    Preference order:
    - structured AI brief: understanding + axes + keywords + delta
    - fallback: raw `profile_text` (for early onboarding / back-compat)
    """
    parts: list[str] = []

    def _get(key: str) -> str:
        try:
            return (repo.get_app_config(key) or "").strip()
        except Exception:
            return ""

    understanding = _get("profile_understanding")
    axes = _get("profile_interest_axes")
    keywords = _get("profile_interest_keywords")
    delta = _get("profile_prompt_delta")

    if understanding:
        parts.append("understanding:\n" + understanding)
    if axes:
        parts.append("interest_axes:\n" + axes)
    if keywords:
        parts.append("keywords:\n" + keywords)
    if delta:
        parts.append("delta_prompt:\n" + delta)

    if not parts:
        raw = _get("profile_text")
        if raw:
            parts.append(raw)

    out = "\n\n".join([p for p in parts if (p or "").strip()]).strip()
    if not out:
        return ""
    if len(out) > _PROFILE_DEFAULT_MAX_CHARS:
        out = out[:_PROFILE_DEFAULT_MAX_CHARS].rstrip() + "…"
    return out


def _inject_default_context(*, repo: Repo, context: dict[str, Any] | None) -> dict[str, Any]:
    """
    Default prompt context injection.

    Goal: templates can always use `{{profile}}` without every call site passing it.
    """
    ctx: dict[str, Any] = dict(context or {})
    if "profile" not in ctx:
        prof = _build_default_profile_text(repo=repo)
        if prof:
            ctx["profile"] = prof
    return ctx


def _render_placeholders(text: str, context: dict[str, Any]) -> str:
    """
    Render templates using a conservative {{name}} placeholder syntax.

    Design notes:
    - Avoid Python .format() because many prompts include JSON examples with braces.
    - If a placeholder is missing, we leave it as-is to make the issue visible to operators.
    """
    if not (text or "").strip():
        return ""
    if not context:
        return text

    def _repl(m: re.Match[str]) -> str:
        key = (m.group(1) or "").strip()
        if not key:
            return m.group(0)
        if key not in context:
            return m.group(0)
        v = context.get(key)
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            try:
                return json.dumps(v, ensure_ascii=False)
            except Exception:
                return str(v)
        return str(v)

    try:
        return _PLACEHOLDER_RE.sub(_repl, text)
    except Exception:
        return text


@dataclass(frozen=True)
class PromptTemplate:
    id: str
    title: str
    description: str = ""
    text_zh: str = ""
    text_en: str = ""
    builtin: bool = True

    def text_for(self, lang: PromptLanguage) -> str:
        if lang == "zh" and (self.text_zh or "").strip():
            return self.text_zh
        if lang == "en" and (self.text_en or "").strip():
            return self.text_en
        # Fallback to any available text.
        return (self.text_zh or "").strip() or (self.text_en or "").strip()


@dataclass(frozen=True)
class PromptSlot:
    id: str
    title: str
    description: str = ""
    output_format: PromptOutputFormat = "text"
    default_template_id: str = ""
    placeholders: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ResolvedPrompt:
    slot_id: str
    template_id: str
    language: PromptLanguage
    text: str
    warnings: list[str] = field(default_factory=list)


def builtin_slots() -> list[PromptSlot]:
    # Keep this curated and stable; Web Admin / TG binds against slot ids.
    return [
        PromptSlot(
            id="llm.propose_topic_setup.system",
            title="Topic bootstrap: system",
            description="LLM proposes query keywords + AI curation prompt; must output strict JSON.",
            output_format="json",
            default_template_id="llm.propose_topic_setup.system",
        ),
        PromptSlot(
            id="llm.propose_topic_setup.user",
            title="Topic bootstrap: user",
            description="User payload for topic bootstrap.",
            output_format="text",
            default_template_id="llm.propose_topic_setup.user",
            placeholders=["topic_name", "brief"],
        ),
        PromptSlot(
            id="llm.propose_topic_setup.fallback_ai_prompt",
            title="Topic bootstrap: fallback ai_prompt",
            description="Fallback LLM curation policy prompt used when onboarding JSON omits ai_prompt.",
            output_format="text",
            default_template_id="llm.propose_topic_setup.fallback_ai_prompt",
            placeholders=["topic_name", "brief"],
        ),
        PromptSlot(
            id="llm.propose_profile_setup.system",
            title="Profile bootstrap: system",
            description="LLM proposes initial profile understanding + axes + retrieval hints + core prompt; strict JSON.",
            output_format="json",
            default_template_id="llm.propose_profile_setup.system",
        ),
        PromptSlot(
            id="llm.propose_profile_setup.user",
            title="Profile bootstrap: user",
            description="User payload for profile bootstrap.",
            output_format="text",
            default_template_id="llm.propose_profile_setup.user",
            placeholders=["profile_text"],
        ),
        PromptSlot(
            id="llm.prompt_template_translate.system",
            title="Prompt template translate: system",
            description="Translate prompt templates between zh/en. Output plain text only.",
            output_format="text",
            default_template_id="llm.prompt_template_translate.system",
        ),
        PromptSlot(
            id="llm.prompt_template_translate.user",
            title="Prompt template translate: user",
            description="Inputs for translating a prompt template (updated source + previous target).",
            output_format="text",
            default_template_id="llm.prompt_template_translate.user",
            placeholders=["source_lang", "target_lang", "updated_source_text", "previous_target_text"],
        ),
        PromptSlot(
            id="llm.localize_item_titles.system",
            title="Curated title localization: system",
            description="Translate/rewrite low-information Curated Info item titles into the configured output language.",
            output_format="json",
            default_template_id="llm.localize_item_titles.system",
        ),
        PromptSlot(
            id="llm.localize_item_titles.user",
            title="Curated title localization: user",
            description="Inputs for Curated Info title localization/rewrite.",
            output_format="text",
            default_template_id="llm.localize_item_titles.user",
            placeholders=["target_lang", "items_block"],
        ),
        PromptSlot(
            id="llm.propose_profile_setup.fallback_ai_prompt",
            title="Profile bootstrap: fallback ai_prompt",
            description="Fallback LLM curation policy prompt used when onboarding JSON omits ai_prompt.",
            output_format="text",
            default_template_id="llm.propose_profile_setup.fallback_ai_prompt",
            placeholders=["profile_text"],
        ),
        PromptSlot(
            id="llm.priority_lane.policy",
            title="Quick Messages: priority lane policy",
            description="Policy prompt for the Quick Messages priority lane (must-push alerts).",
            output_format="text",
            default_template_id="llm.priority_lane.policy",
        ),
        PromptSlot(
            id="llm.profile_delta_update.system",
            title="Profile delta update: system",
            description="Incrementally update profile delta from feedback events; strict JSON.",
            output_format="json",
            default_template_id="llm.profile_delta_update.system",
        ),
        PromptSlot(
            id="llm.profile_delta_update.user",
            title="Profile delta update: user",
            description="User payload for profile delta update.",
            output_format="text",
            default_template_id="llm.profile_delta_update.user",
            placeholders=["core_prompt", "current_delta_prompt", "feedback_events_json"],
        ),
        PromptSlot(
            id="llm.prompt_delta_update.system",
            title="Prompt delta update: system",
            description="Propose a small operator delta for a target prompt slot from feedback events; strict JSON.",
            output_format="json",
            default_template_id="llm.prompt_delta_update.system",
        ),
        PromptSlot(
            id="llm.prompt_delta_update.user",
            title="Prompt delta update: user",
            description="User payload for prompt delta update.",
            output_format="text",
            default_template_id="llm.prompt_delta_update.user",
            placeholders=["target_slot_id", "current_delta_prompt", "feedback_events_json"],
        ),
        PromptSlot(
            id="llm.gate_alert.system",
            title="Alert gate: system",
            description="Optional alert spam gate; strict JSON.",
            output_format="json",
            default_template_id="llm.gate_alert.system",
        ),
        PromptSlot(
            id="llm.gate_alert.user",
            title="Alert gate: user",
            description="User payload for alert gate.",
            output_format="text",
            default_template_id="llm.gate_alert.user",
            placeholders=[
                "profile",
                "topic_name",
                "topic_query_keywords",
                "topic_alert_keywords",
                "item_title",
                "item_url",
                "item_snippet",
            ],
        ),
        PromptSlot(
            id="llm.digest_summary.system",
            title="Digest summary: system",
            description="Bounded digest summary; strict JSON.",
            output_format="json",
            default_template_id="llm.digest_summary.system",
        ),
        PromptSlot(
            id="llm.digest_summary.user",
            title="Digest summary: user",
            description="User payload for digest summary.",
            output_format="text",
            default_template_id="llm.digest_summary.user",
            placeholders=[
                "topic_name",
                "topic_query_keywords",
                "topic_alert_keywords",
                "topic_policy_prompt_block",
                "since",
                "metrics_block",
                "items_block",
                "previous_items_block",
            ],
        ),
        PromptSlot(
            id="llm.triage_items.system",
            title="Triage items: system",
            description="Cheap pre-filter step; strict JSON.",
            output_format="json",
            default_template_id="llm.triage_items.system",
        ),
        PromptSlot(
            id="llm.triage_items.user",
            title="Triage items: user",
            description="User payload for triage items.",
            output_format="text",
            default_template_id="llm.triage_items.user",
            placeholders=[
                "profile",
                "topic_name",
                "topic_query_keywords",
                "topic_alert_keywords",
                "max_keep",
                "topic_policy_prompt_block",
                "recent_sent_block",
                "candidates_block",
            ],
        ),
        PromptSlot(
            id="llm.curate_items.system",
            title="Curate items: system",
            description="Main reasoning curation (ignore|digest|alert); strict JSON.",
            output_format="json",
            default_template_id="llm.curate_items.system",
        ),
        PromptSlot(
            id="llm.curate_items.user",
            title="Curate items: user",
            description="User payload for curate items.",
            output_format="text",
            default_template_id="llm.curate_items.user",
            placeholders=[
                "profile",
                "topic_name",
                "topic_query_keywords",
                "topic_alert_keywords",
                "max_digest",
                "max_alert",
                "topic_policy_prompt_block",
                "recent_sent_block",
                "candidates_block",
            ],
        ),
        PromptSlot(
            id="llm.guess_feed_urls.system",
            title="Discover feeds: system",
            description="Infer RSS/Atom feed URLs from HTML snippet; strict JSON.",
            output_format="json",
            default_template_id="llm.guess_feed_urls.system",
        ),
        PromptSlot(
            id="llm.guess_feed_urls.user",
            title="Discover feeds: user",
            description="User payload for feed discovery.",
            output_format="text",
            default_template_id="llm.guess_feed_urls.user",
            placeholders=["page_url", "html_snippet"],
        ),
        PromptSlot(
            id="llm.guess_api_endpoints.system",
            title="Discover API endpoints: system",
            description="Infer public API endpoints from HTML snippet; strict JSON.",
            output_format="json",
            default_template_id="llm.guess_api_endpoints.system",
        ),
        PromptSlot(
            id="llm.guess_api_endpoints.user",
            title="Discover API endpoints: user",
            description="User payload for API discovery.",
            output_format="text",
            default_template_id="llm.guess_api_endpoints.user",
            placeholders=["page_url", "html_snippet"],
        ),
        PromptSlot(
            id="llm.curate_sources.system",
            title="Curate source candidates: system",
            description="Decide which RSS candidates to accept; strict JSON.",
            output_format="json",
            default_template_id="llm.curate_sources.system",
        ),
        PromptSlot(
            id="llm.curate_sources.user",
            title="Curate source candidates: user",
            description="User payload for source candidate curation.",
            output_format="text",
            default_template_id="llm.curate_sources.user",
            placeholders=[
                "topic_name",
                "topic_query_keywords",
                "topic_alert_keywords",
                "max_accept",
                "topic_policy_prompt_block",
                "profile",
                "explore_weight",
                "exploit_weight",
                "candidates_block",
            ],
        ),
        # --- Admin / bench prompts (operator-visible)
        PromptSlot(
            id="admin.test_llm.system",
            title="Admin test LLM: system",
            description="Connectivity test system prompt (should be tiny and stable).",
            output_format="text",
            default_template_id="admin.test_llm.system",
        ),
        PromptSlot(
            id="admin.test_llm.user",
            title="Admin test LLM: user",
            description="Connectivity test user prompt (expects a tiny deterministic output).",
            output_format="text",
            default_template_id="admin.test_llm.user",
        ),
        PromptSlot(
            id="config_agent.tracking_ai_setup.plan.system",
            title="Tracking AI Setup: plan system",
            description="Generate a safe, bounded JSON plan for tracking config changes (topics/sources/bindings).",
            output_format="json",
            default_template_id="config_agent.tracking_ai_setup.plan.system",
        ),
        PromptSlot(
            id="config_agent.tracking_ai_setup.plan.user",
            title="Tracking AI Setup: plan user",
            description="User request + current tracking snapshot context for plan generation.",
            output_format="text",
            default_template_id="config_agent.tracking_ai_setup.plan.user",
            placeholders=["user_prompt", "tracking_snapshot_text", "web_context", "web_search_context"],
        ),
        PromptSlot(
            id="config_agent.core.plan.system",
            title="Config Agent Core: plan system",
            description="Generate a safe JSON plan for profile/settings/tracking changes.",
            output_format="json",
            default_template_id="config_agent.core.plan.system",
        ),
        PromptSlot(
            id="config_agent.core.plan.user",
            title="Config Agent Core: plan user",
            description="User request + current profile/settings/tracking state.",
            output_format="text",
            default_template_id="config_agent.core.plan.user",
            placeholders=["user_prompt", "tracking_snapshot_text", "profile_state_text", "settings_state_text"],
        ),
    ]


def builtin_templates() -> dict[str, PromptTemplate]:
    """
    Built-in default templates.

    IMPORTANT: These are defaults only. Operators can override via custom templates and/or bindings.
    """
    return {
        # --- Topic bootstrap
        "llm.propose_topic_setup.system": PromptTemplate(
            id="llm.propose_topic_setup.system",
            title="Topic bootstrap (system)",
            text_zh=(
                "你是一位为当前用户画像服务的高信号信息助手。\n"
                "你将收到一个 TOPIC（用户关心的主题）以及一个 BRIEF（关注点/上下文）。\n"
                "你的任务是：输出一份“可用于长期追踪”的配置提案。\n\n"
                "要求：\n"
                "- query_keywords: 用逗号分隔的关键词/短语（中英可混合），要尽量“窄而准”，避免大而全。\n"
                "- ai_prompt: 给 LLM curation 用的提示词，要求极度克制（宁缺毋滥），输出每条 1 句“新信息/变化点” + 1 句“影响/下一步”。\n"
                "- 不要编造外部事实；不要发明 RSS/网站链接。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  "topic_name": "...",\n'
                '  "query_keywords": "kw1,kw2,kw3",\n'
                '  "alert_keywords": "(optional; comma-separated)",\n'
                '  "ai_prompt": "...",\n'
                '  "source_hints": {\n'
                '    "add_hn": true,\n'
                '    "add_searxng": true,\n'
                '    "add_discourse": false,\n'
                '    "discourse_base_url": "(optional; e.g. https://forum.example.com)",\n'
                '    "discourse_json_path": "/latest.json",\n'
                '    "add_nodeseek": false\n'
                "  }\n"
                "}\n"
            ),
            text_en=(
                "You are a high-signal information assistant serving the current user's profile.\n"
                "You will receive a TOPIC and a BRIEF.\n"
                "Your task: propose a long-term tracking configuration.\n\n"
                "Requirements:\n"
                "- query_keywords: comma-separated keywords/phrases; keep it narrow and precise.\n"
                "- ai_prompt: a strict LLM curation prompt. Be selective; for each kept item output 1 sentence 'new info/change' + 1 sentence 'impact/next'.\n"
                "- Do not invent external facts; do not invent RSS/site URLs.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "topic_name": "...",\n'
                '  "query_keywords": "kw1,kw2,kw3",\n'
                '  "alert_keywords": "(optional; comma-separated)",\n'
                '  "ai_prompt": "...",\n'
                '  "source_hints": {\n'
                '    "add_hn": true,\n'
                '    "add_searxng": true,\n'
                '    "add_discourse": false,\n'
                '    "discourse_base_url": "(optional; e.g. https://forum.example.com)",\n'
                '    "discourse_json_path": "/latest.json",\n'
                '    "add_nodeseek": false\n'
                "  }\n"
                "}\n"
            ),
        ),
        "llm.propose_topic_setup.user": PromptTemplate(
            id="llm.propose_topic_setup.user",
            title="Topic bootstrap (user)",
            text_zh=("TOPIC:\n- topic_name: {{topic_name}}\n\nBRIEF:\n{{brief}}\n"),
            text_en=("TOPIC:\n- topic_name: {{topic_name}}\n\nBRIEF:\n{{brief}}\n"),
        ),
        "llm.propose_topic_setup.fallback_ai_prompt": PromptTemplate(
            id="llm.propose_topic_setup.fallback_ai_prompt",
            title="Topic bootstrap fallback ai_prompt",
            text_zh=(
                "你是我的高信号信息助手。\n"
                "主题：{{topic_name}}\n"
                "关注点：{{brief}}\n\n"
                "任务：从候选条目里挑出当天最值得我读的极少数内容（宁缺毋滥）。\n"
                "优先：高信息密度、可验证、与主题/画像强相关、能改变判断或提供可复用经验的内容。\n"
                "一手来源通常更好，但社区/论坛的一线经验、可复现排障、额度/价格/可用性变化、开放注册/邀请码/公共资源/入口汇总、工具实测，只要与主题/画像强相关且包含具体事实，也可以保留。\n"
                "忽略：营销/搬运/标题党/无新增信息/重复讨论。\n\n"
                "输出要求：\n"
                "- alert：只有强时效 + 高影响（会让我今天就改变决策/行动）的才用。\n"
                "- digest：最多 5 条；每条 1 句“新信息/变化点” + 1 句“影响/下一步”（避免两句重复）。\n"
                "- 不确定就写“需核实”，不要编造。\n"
            ),
            text_en=(
                "You are my high-signal information assistant.\n"
                "Topic: {{topic_name}}\n"
                "Focus: {{brief}}\n\n"
                "Task: pick only a tiny number of truly worth-reading items today (be selective).\n"
                "Prefer: high information density, verifiable facts, strong profile/topic relevance, and reusable lessons.\n"
                "Primary sources are usually better, but do NOT auto-drop community/forum field reports, reproducible debugging notes, pricing/quota/access changes, public resource openings, invite/registration threads, resource-directory roundups, or hands-on tool evaluations when they contain concrete facts and clearly match the profile/topic.\n"
                "Ignore: marketing, reposts, clickbait, no-new-info, repetitive discussions.\n\n"
                "Output rules:\n"
                "- alert: only for time-sensitive AND high-impact items (would change my actions today).\n"
                "- digest: at most 5 items; each item must be 1 sentence 'new info/change' + 1 sentence 'impact/next' (no repetition).\n"
                "- If unsure, say 'needs verification' and do not fabricate.\n"
            ),
        ),
        # --- Profile bootstrap
        "llm.propose_profile_setup.system": PromptTemplate(
            id="llm.propose_profile_setup.system",
            title="Profile bootstrap (system)",
            text_zh=(
                "你是一位“下一代兴趣画像 + 信息筛选”的信息秘书。\n"
                "你将收到一段 PROFILE_TEXT（可能是书签标题+URL列表、碎片化兴趣描述、项目清单等任意文本）。\n"
                "你的任务：输出一个非常简短的理解摘要（understanding），生成“兴趣轴”（interest_axes），以及一组用于检索/扩展的提示词（interest_keywords/retrieval_queries），并生成一个可直接用于长期筛选内容的 AI curation 提示词（ai_prompt）。\n\n"
                "要求：\n"
                "- understanding: 1~3 句，尽量描述“我在乎什么/偏好什么”，不要输出一串主题/关键词清单。\n"
                "- interest_axes: 按语义正交拆分，列出输入里所有明显不同的“兴趣轴/偏好/关注点”；不要强行凑数量，也不要遗漏。\n"
                "- interest_keywords: 尽可能提取“关键术语/同义词/实体/工具名/社区名”等短语（中英可混合），用于召回扩展；注意：这些不是关键词匹配过滤规则。\n"
                "- retrieval_queries: 尽可能生成“短、可检索、语义正交”的检索 query（中英可混合）；不要把所有词塞进一条超长 query；召回只是输入，最终筛选由 ai_prompt 决定。\n"
                "- ai_prompt: 必须“宁缺毋滥”；要求模型先读内容再判断；输出极短（每天最多 3~5 条 digest，alert 极少）。\n"
                "- 不要让系统做关键词匹配；要强调语义相关性、信息质量、增量价值、可验证性。\n"
                "- 不做“合规/安全审查式”的类目屏蔽：只按用户偏好 + 信息质量/增量价值筛选。\n"
                "  若内容涉及风险/攻击/越狱/漏洞，仅做客观摘要（不输出可直接武器化的步骤/代码），最终决策权在用户。\n"
                "- 不要编造外部事实。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  "understanding": "...",\n'
                '  "interest_axes": ["..."],\n'
                '  "interest_keywords": ["kw1", "kw2"],\n'
                '  "retrieval_queries": ["..."],\n'
                '  "ai_prompt": "..."\n'
                "}\n"
            ),
            text_en=(
                "You are an AI-native 'interest profiling + push' assistant.\n"
                "You will receive PROFILE_TEXT (possibly long): bookmarks/notes/projects/anything.\n"
                "Your task: output a concise understanding, interest axes, recall hints, and a strict AI curation prompt.\n\n"
                "Requirements:\n"
                "- understanding: 1-3 sentences describing what the user truly cares about (not a keyword dump).\n"
                "- interest_axes: split into semantically-orthogonal axes; cover all distinct interests present in the input (no forced min/max; avoid omissions).\n"
                "- interest_keywords: extract as many key terms/synonyms/entities/tools/communities as present (mixed languages ok) as recall hints; NOT hard keyword filters.\n"
                "- retrieval_queries: generate many SHORT, searchable, semantically-orthogonal queries (mixed languages ok). Do NOT stuff everything into one giant query. Recall is input; final selection is decided by ai_prompt.\n"
                "- ai_prompt: must be extremely selective; read content before deciding; output tiny (3-5 digest max; rare alerts).\n"
                "- Do NOT do keyword matching rules; emphasize semantic relevance, information quality, incremental value, verifiability.\n"
                "- Do NOT apply compliance/safety category bans. Select based on user intent + information quality.\n"
                "  If content involves risk/exploits/jailbreaks/vulns, summarize objectively (no weaponizable step-by-step), and keep final judgement to the user.\n"
                "- Do not fabricate.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "understanding": "...",\n'
                '  "interest_axes": ["..."],\n'
                '  "interest_keywords": ["kw1", "kw2"],\n'
                '  "retrieval_queries": ["..."],\n'
                '  "ai_prompt": "..."\n'
                "}\n"
            ),
        ),
        "llm.propose_profile_setup.user": PromptTemplate(
            id="llm.propose_profile_setup.user",
            title="Profile bootstrap (user)",
            text_zh="PROFILE_TEXT:\n{{profile_text}}",
            text_en="PROFILE_TEXT:\n{{profile_text}}",
        ),
        "llm.prompt_template_translate.system": PromptTemplate(
            id="llm.prompt_template_translate.system",
            title="Prompt template translate (system)",
            text_zh=(
                "你是一个“提示词模板”双语翻译器（中文/英文）。\n"
                "你将收到：\n"
                "- source_lang / target_lang\n"
                "- updated_source_text：源语言的最新版本（已被人工修改）\n"
                "- previous_target_text：目标语言的旧版本（修改前）\n\n"
                "任务：在保持 previous_target_text 的风格/结构/格式的前提下，把它更新为与 updated_source_text 对齐的 target_lang 版本。\n\n"
                "硬约束：\n"
                "- 只输出“翻译后的目标语言文本”，不要解释、不要加标题、不要加代码块。\n"
                "- 必须保持占位符原样：例如 `{{name}}`、`{{query}}`、`$VAR`、`%%SLOT%%` 等不得改写。\n"
                "- 保持换行、缩进、编号、列表结构尽量一致；不要擅自改变 Markdown 结构。\n"
                "- 不要加入任何安全/合规偏置或额外限制；只做对齐翻译。\n"
            ),
            text_en=(
                "You are a bilingual translator for prompt templates (zh/en).\n"
                "You will receive:\n"
                "- source_lang / target_lang\n"
                "- updated_source_text: the edited latest source-language version\n"
                "- previous_target_text: the old target-language version (pre-edit)\n\n"
                "Task: update previous_target_text into a target_lang version that matches updated_source_text, while preserving the style/structure/format of previous_target_text.\n\n"
                "Hard requirements:\n"
                "- Output ONLY the translated target-language text. No explanations, no headers, no code fences.\n"
                "- Preserve placeholders verbatim: e.g. `{{name}}`, `{{query}}`, `$VAR`, `%%SLOT%%`.\n"
                "- Keep line breaks/indentation/numbering/list structure as similar as possible.\n"
                "- Do not add safety/compliance bias or extra constraints; translation only.\n"
            ),
        ),
        "llm.prompt_template_translate.user": PromptTemplate(
            id="llm.prompt_template_translate.user",
            title="Prompt template translate (user)",
            text_zh=(
                "source_lang={{source_lang}}\n"
                "target_lang={{target_lang}}\n\n"
                "updated_source_text:\n"
                "<<<\n"
                "{{updated_source_text}}\n"
                ">>>\n\n"
                "previous_target_text:\n"
                "<<<\n"
                "{{previous_target_text}}\n"
                ">>>\n"
            ),
            text_en=(
                "source_lang={{source_lang}}\n"
                "target_lang={{target_lang}}\n\n"
                "updated_source_text:\n"
                "<<<\n"
                "{{updated_source_text}}\n"
                ">>>\n\n"
                "previous_target_text:\n"
                "<<<\n"
                "{{previous_target_text}}\n"
                ">>>\n"
            ),
        ),
        "llm.localize_item_titles.system": PromptTemplate(
            id="llm.localize_item_titles.system",
            title="Curated title localization (system)",
            text_zh=(
                "你是 OpenInfoMate 的标题本地化器。\n"
                "你将收到一批参考消息候选条目，每条包含：原始标题、URL、以及可能的 summary/snippet/fulltext。\n\n"
                "任务：为每条条目生成一个适合推送展示的目标语言标题。\n\n"
                "规则：\n"
                "- 若原始标题不是目标语言，翻译成目标语言。\n"
                "- 若原始标题信息量低（如 Ask HN/Show HN、仓库路径、文档路径、被截断的标题、只有文件名/栏目名），必须结合 summary/snippet/fulltext 改写成高信息量标题。\n"
                "- 标题必须忠于提供的证据，不要脑补未给出的事实。\n"
                "- 标题应简洁、可读、可独立理解；避免口号、标题党、营销腔。\n"
                "- 若已有高信息量且已是目标语言，可基本保持原意，仅做轻微润色。\n"
                "- 输出 STRICT JSON：{\"titles\":[{\"item_id\":123,\"title\":\"...\"}]}\n"
                "- 只输出 JSON，不要 markdown，不要解释。\n"
            ),
            text_en=(
                "You are OpenInfoMate's title localizer.\n"
                "You will receive Curated Info items with original title, URL, and optional summary/snippet/fulltext evidence.\n\n"
                "Task: produce a high-signal display title in the target language for each item.\n\n"
                "Rules:\n"
                "- If the original title is not in the target language, translate it.\n"
                "- If the original title is low-information (Ask HN/Show HN, repo path, docs path, truncated title, filename-only, category-like label), rewrite it using the provided evidence.\n"
                "- Stay faithful to the evidence; do not invent unsupported facts.\n"
                "- Keep titles concise, readable, and understandable on their own; no clickbait or marketing tone.\n"
                "- If the original title is already high-signal and already in the target language, keep the meaning and only lightly polish if needed.\n"
                "- Output STRICT JSON only: {\"titles\":[{\"item_id\":123,\"title\":\"...\"}]}\n"
                "- No markdown, no explanations, no code fences.\n"
            ),
        ),
        "llm.localize_item_titles.user": PromptTemplate(
            id="llm.localize_item_titles.user",
            title="Curated title localization (user)",
            text_zh=(
                "target_lang={{target_lang}}\n\n"
                "ITEMS:\n"
                "{{items_block}}\n"
            ),
            text_en=(
                "target_lang={{target_lang}}\n\n"
                "ITEMS:\n"
                "{{items_block}}\n"
            ),
        ),
        "llm.propose_profile_setup.fallback_ai_prompt": PromptTemplate(
            id="llm.propose_profile_setup.fallback_ai_prompt",
            title="Profile bootstrap fallback ai_prompt",
            text_zh=(
                "你是我的高信号信息助手。\n"
                "你将收到一批候选条目（标题/链接/摘要/可能的正文）。\n"
                "请基于我的 PROFILE_TEXT 所表达的真实兴趣，筛出当天最值得读的极少数内容（宁缺毋滥）。\n\n"
                "规则：\n"
                "- 先读内容再判断，不要只看标题；信息质量不够就忽略。\n"
                "- 优先：高信息密度、可验证、与 PROFILE_TEXT 强相关、能改变判断或提供可复用经验的内容。\n"
                "- 一手来源通常更好，但社区/论坛一线经验、可复现排障、额度/价格/可用性变化、开放注册/邀请码/公共资源/入口汇总、工具实测，只要与 PROFILE_TEXT 强相关且包含具体事实，也可以保留。\n"
                "- 忽略：营销/转载/标题党/无新增信息/重复讨论。\n"
                "- 不做合规/安全审查式过滤；只按信息质量 + 与我画像相关性筛选。\n"
                "  若涉及风险/攻击/漏洞，仅做客观摘要（不包含可直接武器化细节），最终决策权在我。\n"
                "- alert：只有强时效 + 高影响（会让我今天改变决策/行动）的才用。\n"
                "- digest：最多 5 条；每条 1 句“新信息/变化点” + 1 句“影响/下一步”（避免两句重复）。\n"
                "- 不确定就写“需核实”，不要编造。\n\n"
                "PROFILE_TEXT:\n"
                "{{profile_text}}\n"
            ),
            text_en=(
                "You are my high-signal information assistant.\n"
                "You will receive candidate items (title/link/snippet/possibly full text).\n"
                "Based on my PROFILE_TEXT, pick only a tiny number of truly worth-reading items today.\n\n"
                "Rules:\n"
                "- Read content before deciding; do not decide by title.\n"
                "- Prefer: high information density, verifiable facts, strong profile relevance, and reusable lessons.\n"
                "- Primary sources are usually better, but do NOT auto-drop community/forum field reports, reproducible debugging notes, pricing/quota/access changes, public resource openings, invite/registration threads, resource-directory roundups, or hands-on tool evaluations when they contain concrete facts and clearly match PROFILE_TEXT.\n"
                "- Ignore: marketing/reposts/clickbait/no-new-info/repetitive discussions.\n"
                "- Do NOT apply compliance/safety category bans. Select by information quality + relevance to my profile.\n"
                "  If it involves risk/exploits/vulns, summarize objectively (no weaponizable details). Final judgement is mine.\n"
                "- alert: only for time-sensitive AND high-impact items.\n"
                "- digest: at most 5 items; each item must be 1 sentence 'new info/change' + 1 sentence 'impact/next' (no repetition).\n"
                "- If unsure, say 'needs verification' and do not fabricate.\n\n"
                "PROFILE_TEXT:\n"
                "{{profile_text}}\n"
            ),
        ),
        # --- Quick Messages (priority lane)
        "llm.priority_lane.policy": PromptTemplate(
            id="llm.priority_lane.policy",
            title="Quick Messages (priority lane) policy prompt",
            text_zh=(
                "这是一个“快速消息/重大更新紧急通道”。你要基于用户画像，只挑选必须立即提醒的信号（宁缺毋滥）。\n\n"
                "USER_PROFILE:\n{{profile}}\n\n"
                "alert 必须满足：你很可能“今天就要采取行动/改配置/避免损失”（breaking/pricing/access/大范围影响/正在被大规模利用）。否则一律 ignore（不要把本该 digest 的内容升级为 alert）。\n"
                "不要做固定类目排除；是否 alert 只取决于 USER_PROFILE、时效性、影响范围、可信度与新增信息密度。\n"
                "不要按“安全/越狱/平台事故/灰色产业”等类别做默认屏蔽；只按信息质量、可验证性、时效性与影响筛选。\n"
                "若涉及风险/攻击/漏洞/越狱：允许提醒关键进展，但仅做客观摘要（影响/修复/出处），不输出可直接武器化的步骤/代码。\n"
                "低质量过滤：纯搬运/营销/标题党/无新增信息 → ignore。\n"
                "只要属于“今天你一定要知道”的新进展，就应该 alert；否则给 ignore。\n"
            ),
            text_en=(
                "This is the Quick Messages / 'Breaking Updates' priority lane. Only pick signals that MUST be alerted immediately.\n"
                "You MUST base decisions on the user's profile.\n\n"
                "USER_PROFILE:\n{{profile}}\n\n"
                "An alert MUST imply likely action today (breaking/pricing/access/widespread impact/in-the-wild exploitation). Otherwise ignore (do NOT promote digest-worthy items to alerts).\n"
                "Do NOT use fixed category bans. Whether something deserves alert depends on USER_PROFILE, timeliness, impact, credibility, and information density.\n"
                "Do NOT apply topic/category bans (security/jailbreak/outage/gray-market). Select by information quality, verifiability, timeliness, and impact.\n"
                "If it involves exploits/vulns/jailbreaks: alert key developments but summarize objectively (impact/patch/source) and avoid weaponizable step-by-step.\n"
                "Quality filter: reposts/marketing/clickbait/no-new-info -> ignore.\n"
                "If it's something the user should know TODAY, alert it; otherwise ignore.\n"
            ),
        ),
        # --- Profile delta update
        "llm.profile_delta_update.system": PromptTemplate(
            id="llm.profile_delta_update.system",
            title="Profile delta update (system)",
            text_zh=(
                "你是一个“个人兴趣画像”的增量更新器。\n"
                "你将收到：CORE_PROMPT（稳定不变）、CURRENT_DELTA_PROMPT（可更新）以及一组 FEEDBACK_EVENTS（like/dislike/rate/profile_note）。\n"
                "你的任务：输出一个新的 delta_prompt（更新后的完整 delta），用于追加到 CORE_PROMPT 后，影响后续内容筛选。\n"
                "注意：这是“增量更新”——你必须以 CURRENT_DELTA_PROMPT 为 baseline，只做最小必要修改，而不是推倒重来。\n\n"
                "强约束：\n"
                "- 不要修改 CORE_PROMPT。\n"
                "- delta_prompt 必须短小、可控：<= 12 行、<= 1200 字。\n"
                "- 必须保留 CURRENT_DELTA_PROMPT 中仍然有效的约束/偏好；除非用户新反馈明确相反，否则不要删除/反转已有偏好。\n"
                "- 不要引入任何与用户偏好相矛盾的“合规/安全/道德”类屏蔽或降权；系统是客观信息分析器，最终决策权在人类。\n"
                "- 不要擅自把“优惠/羊毛/免费额度/折扣”等内容当作低质量；若用户输入/反馈出现，视为合法偏好并允许纳入。\n"
                "- 对风险/攻击/漏洞/越狱类内容：允许纳入高信号信息，但避免输出可直接武器化的步骤/代码；以事实/影响/修复/出处为主。\n"
                "- 只提炼“稳定偏好/明确排斥/输出风格偏好”。不要因为一次反馈引入大方向漂移。\n"
                "- 不要引入关键词硬过滤规则（AI-native）。表达为语义偏好即可。\n"
                "- profile_note：把用户的自然语言反馈视为“风格/可理解性/偏好”的证据，不要过度泛化。\n"
                "- 输出使用中文；note 也必须是中文。\n"
                "- 禁止向用户提问/追问/澄清/索要“关键词”；只做增量更新提案。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  "delta_prompt": "...",\n'
                '  "note": "(optional; one sentence rationale)"\n'
                "}\n"
            ),
            text_en=(
                "You update the *delta* portion of a user's interest profile prompt.\n"
                "You will receive CORE_PROMPT (immutable), CURRENT_DELTA_PROMPT (mutable), and FEEDBACK_EVENTS (like/dislike/rate/profile_note).\n"
                "Your task: output a new delta_prompt (the updated full delta) that will be appended to CORE_PROMPT.\n"
                "Note: this is an incremental update — treat CURRENT_DELTA_PROMPT as the baseline and make minimal necessary edits.\n\n"
                "Hard constraints:\n"
                "- Do NOT modify CORE_PROMPT.\n"
                "- delta_prompt must be small and controllable: <= 12 lines, <= 1200 chars.\n"
                "- Preserve still-valid constraints/preferences from CURRENT_DELTA_PROMPT; do not delete/flip existing preferences unless the user explicitly contradicts them.\n"
                "- Do NOT introduce compliance/safety/moral category bans that contradict the user's stated preferences; this system is objective and final judgement stays with the human.\n"
                "- Do NOT treat deals/free credits/free tiers/discounts as inherently low-quality; if the user includes them, treat them as legitimate preferences.\n"
                "- For risk/exploit/vuln/jailbreak content: allow high-signal items, but avoid weaponizable step-by-step/code; focus on facts/impact/patch/source.\n"
                "- Only extract stable preferences / clear dislikes / output style preferences. Avoid major drift from a single event.\n"
                "- Do NOT introduce hard keyword filters (AI-native). Express semantic preferences instead.\n"
                "- profile_note: treat free-form user notes as evidence about style/understandability/preferences; do not over-generalize.\n"
                "- Output in English (note must also be English).\n"
                "- Do NOT ask the user questions or request keywords; only produce an incremental update proposal.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "delta_prompt": "...",\n'
                '  "note": "(optional; one sentence rationale)"\n'
                "}\n"
            ),
        ),
        "llm.profile_delta_update.user": PromptTemplate(
            id="llm.profile_delta_update.user",
            title="Profile delta update (user)",
            text_zh=(
                "CORE_PROMPT (immutable):\n{{core_prompt}}\n\n"
                "CURRENT_DELTA_PROMPT:\n{{current_delta_prompt}}\n\n"
                "FEEDBACK_EVENTS (newest last):\n{{feedback_events_json}}"
            ),
            text_en=(
                "CORE_PROMPT (immutable):\n{{core_prompt}}\n\n"
                "CURRENT_DELTA_PROMPT:\n{{current_delta_prompt}}\n\n"
                "FEEDBACK_EVENTS (newest last):\n{{feedback_events_json}}"
            ),
        ),
        # --- Prompt delta update (operator-tunable prompt corrections)
        "llm.prompt_delta_update.system": PromptTemplate(
            id="llm.prompt_delta_update.system",
            title="Prompt delta update (system)",
            text_zh=(
                "你是一个“提示词 delta”的增量更新器。\n"
                "你将收到：TARGET_SLOT_ID（要影响的提示词槽位）、CURRENT_DELTA_PROMPT（可更新）、以及一组 FEEDBACK_EVENTS（prompt_note）。\n"
                "你的任务：输出一个新的 delta_prompt（更新后的完整 delta），用于附加到 TARGET_SLOT_ID 对应的提示词末尾，修正文风/约束。\n"
                "注意：这是“增量更新”——你必须以 CURRENT_DELTA_PROMPT 为 baseline，只做最小必要修改，而不是推倒重来。\n\n"
                "强约束：\n"
                "- delta_prompt 必须短小、可控：<= 14 行、<= 1400 字。\n"
                "- 必须保留 CURRENT_DELTA_PROMPT 中仍然有效的约束；除非用户新反馈明确相反，否则不要删除/反转已有约束。\n"
                "- 不要引入任何与用户偏好相矛盾的“合规/安全/道德”类屏蔽或降权；系统是客观信息分析器，最终决策权在人类。\n"
                "- 不要加入任何预测；只允许基于证据的总结/对比。\n"
                "- delta_prompt 只能影响“输出格式/引用方式/去重/覆盖/语言/避免元旁白”等写作与信息呈现约束；不要引入新的业务逻辑。\n"
                "- 输出使用中文。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  \"delta_prompt\": \"...\",\n'
                '  \"note\": \"(optional; one sentence rationale)\"\n'
                "}\n"
            ),
            text_en=(
                "You update an operator-controlled *prompt delta* for a target slot.\n"
                "Inputs: TARGET_SLOT_ID, CURRENT_DELTA_PROMPT (mutable), and FEEDBACK_EVENTS (prompt_note).\n"
                "Task: output a new delta_prompt (full replacement) to be appended to the target slot prompt.\n"
                "This is an incremental update — treat CURRENT_DELTA_PROMPT as the baseline and make minimal necessary edits.\n\n"
                "Hard constraints:\n"
                "- delta_prompt must be small and controllable: <= 14 lines, <= 1400 chars.\n"
                "- Preserve still-valid constraints from CURRENT_DELTA_PROMPT unless the user explicitly contradicts them.\n"
                "- Do NOT introduce compliance/safety/moral category bans that contradict user preferences; final judgement stays with the human.\n"
                "- No predictions; only evidence-based summarization/comparisons.\n"
                "- delta_prompt may only affect output style/format/citations/dedup/coverage/language/avoiding meta narration; do not introduce new business logic.\n"
                "- Output in English.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  \"delta_prompt\": \"...\",\n'
                '  \"note\": \"(optional; one sentence rationale)\"\n'
                "}\n"
            ),
        ),
        "llm.prompt_delta_update.user": PromptTemplate(
            id="llm.prompt_delta_update.user",
            title="Prompt delta update (user)",
            text_zh=(
                "TARGET_SLOT_ID:\n{{target_slot_id}}\n\n"
                "CURRENT_DELTA_PROMPT:\n{{current_delta_prompt}}\n\n"
                "FEEDBACK_EVENTS (newest last):\n{{feedback_events_json}}"
            ),
            text_en=(
                "TARGET_SLOT_ID:\n{{target_slot_id}}\n\n"
                "CURRENT_DELTA_PROMPT:\n{{current_delta_prompt}}\n\n"
                "FEEDBACK_EVENTS (newest last):\n{{feedback_events_json}}"
            ),
        ),
        # --- Alert gate
        "llm.gate_alert.system": PromptTemplate(
            id="llm.gate_alert.system",
            title="Alert gate (system)",
            text_zh=(
                "你是一位为当前用户画像服务的信息秘书。\n"
                "给定 TOPIC 与一个候选 ALERT 条目，请判断是否需要立刻提醒。\n"
                "只输出 STRICT JSON（不要 markdown、不要代码块、不要额外文字）。\n\n"
                "Schema:\n"
                "{\n"
                '  "decision": "alert"|"digest",\n'
                '  "reason": <短句>\n'
                "}\n\n"
                "规则:\n"
                "- 除非强时效 + 高影响，否则优先选择 digest。\n"
                "- 必须结合 USER_PROFILE 判断：与画像弱相关/低信号的内容不能 alert。\n"
                "- alert 必须满足：很可能需要今天采取行动/改配置/避免损失（breaking/pricing/access/大范围影响/正在被大规模利用）。\n"
                "- 不要做固定类目排除；是否 alert 只取决于 USER_PROFILE、时效性、影响范围、可信度与新增信息密度。\n"
                "- 先读 snippet 再判断，不要只看标题。\n"
                "- 不要编造；不确定就选 digest，并在 reason 里写“需核实”。\n"
            ),
            text_en=(
                "You are an information secretary serving the current user's profile.\n"
                "Given a TOPIC and a CANDIDATE ALERT item, decide whether to alert immediately.\n"
                "Return STRICT JSON only, no markdown, no code fences.\n\n"
                "Schema:\n"
                "{\n"
                '  "decision": "alert"|"digest",\n'
                '  "reason": <short string>\n'
                "}\n\n"
                "Rules:\n"
                "- Prefer \"digest\" unless it is time-sensitive AND high impact.\n"
                "- Use USER_PROFILE: low-signal or weakly relevant items must NOT be \"alert\".\n"
                "- \"alert\" requires likely action today (breaking/pricing/access/widespread impact/in-the-wild exploitation).\n"
                "- Do NOT use fixed category bans. Whether something deserves alert depends on USER_PROFILE, timeliness, impact, credibility, and information density.\n"
                "- Read the snippet before deciding; don't decide by title alone.\n"
                "- Don't invent facts; if uncertain, choose \"digest\" and say so in reason.\n"
            ),
        ),
        "llm.gate_alert.user": PromptTemplate(
            id="llm.gate_alert.user",
            title="Alert gate (user)",
            text_zh=(
                "USER_PROFILE:\n{{profile}}\n\n"
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n\n"
                "ITEM:\n"
                "- title: {{item_title}}\n"
                "- url: {{item_url}}\n"
                "- snippet: {{item_snippet}}\n"
            ),
            text_en=(
                "USER_PROFILE:\n{{profile}}\n\n"
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n\n"
                "ITEM:\n"
                "- title: {{item_title}}\n"
                "- url: {{item_url}}\n"
                "- snippet: {{item_snippet}}\n"
            ),
        ),
        # --- Digest summary
        "llm.digest_summary.system": PromptTemplate(
            id="llm.digest_summary.system",
            title="Digest summary (system)",
            text_zh=(
                "你是一位为当前用户画像服务的信息秘书，擅长从多条链接里提炼高价值信号。\n"
                "请基于输入的 TOPIC + 最近窗口内的 ITEMS，生成面向决策的日报摘要。\n"
                "只输出 STRICT JSON（不要 markdown、不要代码块、不要额外文字）。\n\n"
                "Schema:\n"
                "{\n"
                '  "summary": <string>,\n'
                '  "highlights": <array of strings>,\n'
                '  "risks": <array of strings>,\n'
                '  "next_actions": <array of strings>\n'
                "}\n\n"
                "约束:\n"
                "- summary 1-2 句，中文，信息密度极高\n"
                "- highlights ≤ 2 条（可为空）\n"
                "- risks ≤ 1 条（可为空；不按“安全/运维”等类别做默认忽略；若涉及风险/攻击/漏洞，仅做客观摘要，避免可直接武器化细节）\n"
                "- next_actions 必须为空数组 []（禁止输出下一步/建议/行动）\n"
                "- 不要编造未提供的事实；不确定就省略\n"
            ),
            text_en=(
                "You are an information secretary serving the current user's profile. You extract high-value signals from many links.\n"
                "Given TOPIC + ITEMS from the latest window, write a decision-oriented daily summary.\n"
                "Return STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "summary": <string>,\n'
                '  "highlights": <array of strings>,\n'
                '  "risks": <array of strings>,\n'
                '  "next_actions": <array of strings>\n'
                "}\n\n"
                "Constraints:\n"
                "- summary: 1-2 sentences, English, very information-dense\n"
                "- highlights: <= 2 bullets (may be empty)\n"
                "- risks: <= 1 bullet (may be empty; do NOT ignore security/ops by category; if it involves exploits/vulns, keep it factual and avoid weaponizable details)\n"
                "- next_actions: MUST be an empty array [] (no action items / next steps)\n"
                "- Do not fabricate; if uncertain, omit.\n"
            ),
        ),
        "llm.digest_summary.user": PromptTemplate(
            id="llm.digest_summary.user",
            title="Digest summary (user)",
            text_zh=(
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "{{topic_policy_prompt_block}}\n"
                "METRICS:\n"
                "- since: {{since}} UTC\n"
                "{{metrics_block}}\n"
                "ITEMS (most recent first):\n"
                "{{items_block}}\n"
                "{{previous_items_block}}\n"
            ),
            text_en=(
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "{{topic_policy_prompt_block}}\n"
                "METRICS:\n"
                "- since: {{since}} UTC\n"
                "{{metrics_block}}\n"
                "ITEMS (most recent first):\n"
                "{{items_block}}\n"
                "{{previous_items_block}}\n"
            ),
        ),
        # --- Triage items
        "llm.triage_items.system": PromptTemplate(
            id="llm.triage_items.system",
            title="Triage items (system)",
            text_zh=(
                "你是一个“信息流 Triage”模块。\n"
                "你将收到 TOPIC + TOPIC_POLICY_PROMPT + RECENT_SENT，以及一组候选条目 CANDIDATES。\n"
                "你的任务：从候选中选出最多 MAX_KEEP 条“最可能高信号”的条目，供后续更强模型做最终筛选与写作。\n\n"
                "要求：\n"
                "- 这一步要便宜、克制：只做过滤，不做长篇总结。\n"
                "- 必须结合 USER_PROFILE：与画像弱相关/不符合偏好/明显跑题的内容，直接丢弃。\n"
                "- 严格去重：同一事件/同一 repo/同一发布（即使不同 URL）最多保留 1 条。\n"
                "- 反复出现：如果 RECENT_SENT 中已经出现同一事件，除非这条带来实质新增，否则不要保留。\n"
                "- 质量过滤：纯转载/营销软文/标题党/无新增信息 → 不保留。泛泛灌水讨论可丢弃；但社区/论坛一线经验、可复现排障、额度/价格/可用性变化、公共资源/开放注册/邀请码/入口汇总、工具实测，只要与 USER_PROFILE 强相关且包含具体事实，不应因为“不是官方源”就丢弃。\n"
                "- 域名质量：候选可能包含 domain_feedback（历史 👍/👎 计数）。对长期被 👎 的域名/明显 SEO 转载站，除非是唯一一手来源，否则倾向丢弃；优先官方/原始 repo/论文等一手来源，但不要把高质量社区一线报告一概当作低质。\n"
                "- 不对“安全/提示词注入/越狱/平台事故”等类别做默认屏蔽；只按信号强度 + 可信度 + 与 TOPIC/画像相关性过滤。\n"
                "- 若涉及风险/攻击/漏洞，仅保留客观事实/影响/修复线索；不要输出可直接武器化的步骤/代码。\n"
                "- 允许输出少于 MAX_KEEP；如果没有高信号内容，可输出空列表 []。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  "keep_item_ids": [123, 456]\n'
                "}\n"
            ),
            text_en=(
                "You are a cheap 'feed triage' module.\n"
                "You will receive TOPIC + TOPIC_POLICY_PROMPT + RECENT_SENT and CANDIDATES.\n"
                "Task: select up to MAX_KEEP items that are most likely high-signal, for a stronger model to curate later.\n\n"
                "Requirements:\n"
                "- Be cheap and strict: filtering only, no long summaries.\n"
                "- Use USER_PROFILE: weakly relevant / off-profile / off-topic items must be dropped.\n"
                "- Strict dedupe: same event/repo/release (even across URLs) keep at most 1.\n"
                "- Repeats: if RECENT_SENT already covered the same event, only keep if materially new.\n"
                "- Quality filter: reposts/marketing/clickbait/no-new-info -> drop. Generic chatter can be dropped, but community/forum field reports, reproducible debugging notes, pricing/quota/access changes, public-resource openings, invite/registration threads, resource-directory roundups, and hands-on evaluations should be kept when they contain concrete facts and strongly match USER_PROFILE.\n"
                "- Domain quality: candidates may include domain_feedback (historical 👍/👎 counts). Down-rank domains with repeated 👎 or obvious SEO repost sites; prefer primary sources (official/repo/paper) when available, but do NOT auto-treat strong first-hand community reports as low quality.\n"
                "- Do NOT apply category bans (security/prompt-injection/jailbreak/outage). Filter by signal strength + credibility + relevance.\n"
                "- If it involves exploits/vulns, keep it factual (impact/patch) and avoid weaponizable step-by-step.\n"
                "- It is OK to output fewer than MAX_KEEP; output an empty list [] if nothing meets the bar.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "keep_item_ids": [123, 456]\n'
                "}\n"
            ),
        ),
        "llm.triage_items.user": PromptTemplate(
            id="llm.triage_items.user",
            title="Triage items (user)",
            text_zh=(
                "USER_PROFILE:\n{{profile}}\n\n"
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "- MAX_KEEP: {{max_keep}}\n"
                "{{topic_policy_prompt_block}}"
                "{{recent_sent_block}}"
                "CANDIDATES (most recent first):\n"
                "{{candidates_block}}\n"
            ),
            text_en=(
                "USER_PROFILE:\n{{profile}}\n\n"
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "- MAX_KEEP: {{max_keep}}\n"
                "{{topic_policy_prompt_block}}"
                "{{recent_sent_block}}"
                "CANDIDATES (most recent first):\n"
                "{{candidates_block}}\n"
            ),
        ),
        # --- Curate items
        "llm.curate_items.system": PromptTemplate(
            id="llm.curate_items.system",
            title="Curate items (system)",
            text_zh=(
                "你是一位为当前用户画像服务的信息秘书。\n"
                "你将收到一个 TOPIC，以及一组候选条目 CANDIDATES（来自论坛/RSS/搜索结果）。\n"
                "你的任务是：为每个候选条目输出一个决策：ignore | digest | alert。\n\n"
                "要求：\n"
                "- 必须非常克制：digest <= MAX_DIGEST，alert <= MAX_ALERT，其余全部 ignore。\n"
                "- 允许输出远少于上限；如果没有达到高信号门槛，可全部 ignore（宁缺毋滥）。\n"
                "- 必须结合 USER_PROFILE：与画像弱相关/明显跑题/低信号内容，不能进 digest，更不能 alert。\n"
                "- alert 仅用于“强时效 + 高影响”的信息；否则放入 digest 或 ignore。\n"
                "- 如果 MAX_DIGEST==0：表示这是“快速消息/紧急通道”（priority lane）。此时不能输出 digest；但也绝不能把本该 digest 的内容升级成 alert。只有满足强时效+高影响（很可能今天要行动/改配置/避免损失）的才 alert，其余全部 ignore。\n"
                "- 不要做固定类目排除；是否 alert 只取决于 USER_PROFILE、时效性、影响范围、可信度与新增信息密度。\n"
                "- 你必须先读 snippet（如果提供）：它可能来自原文全文提取或 feed 摘要；不要只看标题做决定。\n"
                "- 如果 snippet 为空/信息不足：不要默认给 digest。只有当标题本身是强信号且与画像/主题强相关时，才给 digest/alert；否则 ignore。why 可为空，或仅说明证据状态（例如：仅标题/论坛贴未含一手链接）。\n"
                "- 质量过滤：纯转载/营销软文/标题党/无新增信息 → 直接 ignore（宁缺毋滥）。泛泛灌水讨论可忽略；但社区/论坛一线经验、可复现排障、额度/价格/可用性变化、开放注册/邀请码/公共资源/入口汇总、工具实测，只要与 USER_PROFILE 强相关且包含具体事实，不应因为“不是官方源”就直接 ignore。\n"
                "- 域名质量：候选可能包含 domain_feedback（历史 👍/👎 计数）。对长期被 👎 的域名/明显 SEO 转载站，若 snippet 未含一手链接/实质新事实，倾向 ignore；优先官方/原始 repo/论文等一手来源，但不要把高质量社区一线报告一概当作低质。\n"
                "- 不对“安全/提示词注入/越狱/平台事故”等类别做默认屏蔽；只按信号强度 + 可信度 + 与 TOPIC/画像相关性筛选。\n"
                "- 若涉及风险/攻击/漏洞，仅做客观摘要（不包含可直接武器化步骤/代码）；以事实/影响/修复/出处为主。\n"
                "- 重要：对“新模型发布/新开发工具/新框架/重大版本更新”，即使细节不足，也优先 digest/alert，而不是因为缺细节而忽略；但在 priority lane（MAX_DIGEST==0）中，只有满足“强时效+高影响”才允许 alert。\n"
                "- 去重（关键）：同一事件/同一发布/同一漏洞/同一 repo 更新（即使不同 URL/不同站点）最多出现 1 条；优先选择最一手/最权威来源。\n"
                "- 反复出现：如果 RECENT_SENT 已经出现同一事件，除非这条带来“实质新增”（新版本号/新数字/新修复/新决定/新出货/新漏洞细节），否则忽略。\n"
                "- 不要编造未提供的事实；如果仅凭标题/摘要无法判断，倾向于 digest 或 ignore；why 可为空或写清证据状态（不要写行动建议）。\n"
                "- 文案要求：summary=一句话“新信息/变化点”（不要复述标题，不讲背景，不喊口号；禁止建议/下一步/行动/价值判断）；why=0-1 句（可为空）：只写“证据/出处/可信度”（例如：官方 release notes / 论文 / repo / 维护者公告 / 一线媒体），不要写影响/建议/行动。\n"
                "- 输出语言：summary/why 必须使用中文；若标题/snippet 不是中文，请翻译其含义后再输出中文。\n"
                "- why 可以为空；不要因为写作困难把高信号降级为 ignore。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  "decisions": [\n'
                '    {"item_id": 123, "decision": "ignore|digest|alert", "why": "...", "summary": "..."}\n'
                "  ]\n"
                "}\n"
            ),
            text_en=(
                "You are an information secretary serving the current user's profile.\n"
                "You will receive a TOPIC and candidate items (from forums/RSS/search).\n"
                "Task: output a decision for each candidate: ignore | digest | alert.\n\n"
                "Rules:\n"
                "- Be strict: digest <= MAX_DIGEST, alert <= MAX_ALERT; everything else ignore.\n"
                "- It is OK to output far fewer than the caps; if nothing meets the bar, ignore everything.\n"
                "- Use USER_PROFILE: weakly relevant / off-profile / off-topic items must NOT enter digest and must NEVER be alert.\n"
                "- alert only for time-sensitive AND high-impact signals; otherwise digest or ignore.\n"
                "- If MAX_DIGEST==0: this is the priority lane (quick/breaking updates). Digest is disabled, but do NOT promote digest-worthy items to alerts. Only alert if it likely requires action today (breaking/pricing/access/widespread impact/in-the-wild exploitation); otherwise ignore.\n"
                "- Do NOT use fixed category bans. Whether something deserves alert depends on USER_PROFILE, timeliness, impact, credibility, and information density.\n"
                "- Read snippet if provided (may be fulltext extract); do not decide by title alone.\n"
                "- If snippet is empty/insufficient: do NOT default to digest. Only keep (digest/alert) if the TITLE itself is a strong signal AND it clearly matches the profile/topic; concrete access/pricing/quota/availability changes, first-hand operator reports, or reproducible community findings may still qualify even without a primary link. why may be empty or briefly describe evidence status.\n"
                "- Quality filter: reposts/marketing/clickbait/no-new-info -> ignore. Generic chatter can be ignored, but community/forum field reports, reproducible debugging notes, pricing/quota/access changes, public-resource openings, invite/registration threads, resource-directory roundups, and hands-on evaluations should be kept when they contain concrete facts and strongly match USER_PROFILE.\n"
                "- Domain quality: candidates may include domain_feedback (historical 👍/👎 counts). Down-rank domains with repeated 👎 or obvious SEO repost sites; if snippet lacks primary links/new facts, prefer ignore; prefer primary sources (official/repo/paper) when available, but do NOT auto-treat strong first-hand community reports as low quality.\n"
                "- Do NOT apply category bans (security/prompt-injection/jailbreak/outage). Select by signal strength + credibility + topic/profile relevance.\n"
                "- If it involves exploits/vulns, summarize objectively (impact/patch/evidence), avoid weaponizable step-by-step.\n"
                "- Important: for 'new model release / new dev tool / new framework / major version update', prefer digest/alert rather than ignoring due to missing details; but in the priority lane (MAX_DIGEST==0) only alert if it is also time-sensitive/high-impact.\n"
                "- Dedupe (critical): same event/release/vuln/repo update (even across URLs/sites) must appear at most once; pick the most primary/authoritative source.\n"
                "- Repeats: if RECENT_SENT already covered it, ignore unless materially new.\n"
                "- Do not fabricate; if you cannot judge from title/snippet, lean digest or ignore; why may be empty or briefly describe evidence status. Do not write action items.\n"
                "- Copy rules: summary = 1 sentence 'new info/change' (no title restate, no background; no recommendations/next steps). why = 0-1 sentence (may be empty) stating ONLY evidence/source/credibility (e.g. official release notes/paper/repo/maintainer post). NO impact/next steps.\n"
                "- Output language: English for summary/why. If the title/snippet is not English, translate its meaning.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "decisions": [\n'
                '    {"item_id": 123, "decision": "ignore|digest|alert", "why": "...", "summary": "..."}\n'
                "  ]\n"
                "}\n"
            ),
        ),
        "llm.curate_items.user": PromptTemplate(
            id="llm.curate_items.user",
            title="Curate items (user)",
            text_zh=(
                "USER_PROFILE:\n{{profile}}\n\n"
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "- MAX_DIGEST: {{max_digest}}\n"
                "- MAX_ALERT: {{max_alert}}\n"
                "{{topic_policy_prompt_block}}"
                "{{recent_sent_block}}"
                "CANDIDATES (most recent first):\n"
                "{{candidates_block}}\n"
            ),
            text_en=(
                "USER_PROFILE:\n{{profile}}\n\n"
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "- MAX_DIGEST: {{max_digest}}\n"
                "- MAX_ALERT: {{max_alert}}\n"
                "{{topic_policy_prompt_block}}"
                "{{recent_sent_block}}"
                "CANDIDATES (most recent first):\n"
                "{{candidates_block}}\n"
            ),
        ),
        # --- Feed discovery
        "llm.guess_feed_urls.system": PromptTemplate(
            id="llm.guess_feed_urls.system",
            title="Feed discovery (system)",
            text_en=(
                "You are a web research assistant.\n"
                "Given a webpage URL and a short HTML snippet, infer likely RSS/Atom feed URLs.\n"
                "Return STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "feed_urls": <array of strings>\n'
                "}\n\n"
                "Constraints:\n"
                "- Prefer true site feeds (not comment feeds).\n"
                "- Output at most 10 URLs.\n"
                "- URLs may be absolute or relative; relative URLs will be resolved against page_url.\n"
            ),
            text_zh=(
                "你是一个网页调研助手。\n"
                "给定一个网页 URL 和一段 HTML 片段，请推断可能的 RSS/Atom 订阅地址。\n"
                "只输出 STRICT JSON（不要 markdown、不要代码块、不要额外文字）。\n\n"
                "Schema:\n"
                "{\n"
                '  "feed_urls": <array of strings>\n'
                "}\n\n"
                "约束：\n"
                "- 优先输出站点真正的 feeds（避免评论 feeds）。\n"
                "- 最多输出 10 个 URL。\n"
                "- URL 可为绝对或相对路径；相对 URL 将以 page_url 为基准解析。\n"
            ),
        ),
        "llm.guess_feed_urls.user": PromptTemplate(
            id="llm.guess_feed_urls.user",
            title="Feed discovery (user)",
            text_en="page_url: {{page_url}}\n\nhtml_snippet:\n{{html_snippet}}\n",
            text_zh="page_url: {{page_url}}\n\nhtml_snippet:\n{{html_snippet}}\n",
        ),
        # --- API discovery
        "llm.guess_api_endpoints.system": PromptTemplate(
            id="llm.guess_api_endpoints.system",
            title="API discovery (system)",
            text_en=(
                "You are a web research assistant.\n"
                "Given a webpage URL and a short HTML snippet, infer likely public API endpoints used to load content.\n"
                "Return STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "api_endpoints": <array of strings>\n'
                "}\n\n"
                "Constraints:\n"
                "- Only include endpoints that appear public and safe to fetch.\n"
                "- Output at most 10 URLs.\n"
                "- URLs may be absolute or relative; relative URLs will be resolved against page_url.\n"
                "- Avoid private/internal endpoints.\n"
            ),
            text_zh=(
                "你是一个网页调研助手。\n"
                "给定一个网页 URL 和一段 HTML 片段，请推断页面可能使用的公开 API 接口地址（用于加载内容）。\n"
                "只输出 STRICT JSON（不要 markdown、不要代码块、不要额外文字）。\n\n"
                "Schema:\n"
                "{\n"
                '  "api_endpoints": <array of strings>\n'
                "}\n\n"
                "约束：\n"
                "- 只包含看起来公开且安全可抓取的 endpoints。\n"
                "- 最多输出 10 个 URL。\n"
                "- URL 可为绝对或相对路径；相对 URL 将以 page_url 为基准解析。\n"
                "- 避免输出明显私有/内部 endpoints。\n"
            ),
        ),
        "llm.guess_api_endpoints.user": PromptTemplate(
            id="llm.guess_api_endpoints.user",
            title="API discovery (user)",
            text_en="page_url: {{page_url}}\n\nhtml_snippet:\n{{html_snippet}}\n",
            text_zh="page_url: {{page_url}}\n\nhtml_snippet:\n{{html_snippet}}\n",
        ),
        # --- Source candidate curation
        "llm.curate_sources.system": PromptTemplate(
            id="llm.curate_sources.system",
            title="Curate source candidates (system)",
            text_zh=(
                "你是一位为当前用户画像服务的信息秘书。\n"
                "你将收到一个 TOPIC（含 Profile 摘要/Policy）以及一组 RSS/Atom 源候选（CANDIDATES）。\n"
                "每个候选包含：URL、发现来源、以及抓取后的 source_content（若干条近期条目：标题/摘要/链接）。\n"
                "你的任务是：对每个候选给出 0–100 的综合评分，并做 accept|ignore|skip 决策。\n\n"
                "要求：\n"
                "- 必须非常克制：accept <= MAX_ACCEPT；其余用 ignore 或 skip。\n"
                "- 综合评分 score 需考虑：\n"
                "  1) quality_score（来源质量）：是否一手/技术细节/可验证/持续产出，是否营销/搬运/洗稿/聚合。\n"
                "  2) relevance_score（相关性）：与 topic+profile 的可迁移价值，而非关键词表面匹配。\n"
                "  3) novelty_score（新颖性）：是否能带来新视角/新来源/新信号，避免重复同质内容。\n"
                "- explore/exploit 影响打分取向：\n"
                "  - exploit 高：更偏重 relevance。\n"
                "  - explore 高：允许 relevance 略低，但要求 quality+novelty 足够高，且不能完全不相关。\n"
                "- ignore：明显低质/广告/搬运/聚合站/完全不相关。\n"
                "- skip：信息不足或不确定，留给人工复核。\n"
                "- 不要编造未提供的事实；只根据 URL/source_content/上下文做判断。\n"
                "- 只输出 STRICT JSON，不要 markdown，不要代码块，不要额外文字。\n\n"
                "Schema:\n"
                "{\n"
                '  "decisions": [\n'
                '    {"candidate_id": 1, "decision": "accept|ignore|skip", "score": 0, "quality_score": 0, "relevance_score": 0, "novelty_score": 0, "why": "..."}\n'
                "  ]\n"
                "}\n"
            ),
            text_en=(
                "You are an information secretary serving the current user's profile.\n"
                "You will receive a TOPIC (including Profile/Policy) and RSS/Atom source candidates.\n"
                "Each candidate includes URL, discovered_from, and fetched source_content (recent entries: title/summary/link).\n"
                "Task: score each candidate (0-100) and decide accept|ignore|skip.\n\n"
                "Rules:\n"
                "- Be strict: accept <= MAX_ACCEPT; others are ignore or skip.\n"
                "- Overall score must consider:\n"
                "  1) quality_score: primary, technical, verifiable, consistent; penalize marketing/reposts/aggregators.\n"
                "  2) relevance_score: transferable value to topic+profile (not superficial keyword match).\n"
                "  3) novelty_score: adds new signals, avoids duplicates.\n"
                "- explore/exploit influences tradeoff:\n"
                "  - higher exploit => weight relevance more.\n"
                "  - higher explore => allow slightly lower relevance only if quality+novelty are high, never totally unrelated.\n"
                "- ignore: clearly low-quality/ads/reposts/aggregators/totally unrelated.\n"
                "- skip: uncertain / not enough info; leave for human review.\n"
                "- Do not fabricate; judge only by URL + source_content + context.\n"
                "- Output STRICT JSON only (no markdown, no code fences, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  "decisions": [\n'
                '    {"candidate_id": 1, "decision": "accept|ignore|skip", "score": 0, "quality_score": 0, "relevance_score": 0, "novelty_score": 0, "why": "..."}\n'
                "  ]\n"
                "}\n"
            ),
        ),
        "llm.curate_sources.user": PromptTemplate(
            id="llm.curate_sources.user",
            title="Curate source candidates (user)",
            text_zh=(
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "- MAX_ACCEPT: {{max_accept}}\n"
                "- explore_weight: {{explore_weight}}\n"
                "- exploit_weight: {{exploit_weight}}\n"
                "PROFILE:\n"
                "{{profile}}\n"
                "{{topic_policy_prompt_block}}"
                "CANDIDATES (newest first):\n"
                "{{candidates_block}}\n"
            ),
            text_en=(
                "TOPIC:\n"
                "- name: {{topic_name}}\n"
                "- query_keywords: {{topic_query_keywords}}\n"
                "- alert_keywords: {{topic_alert_keywords}}\n"
                "- MAX_ACCEPT: {{max_accept}}\n"
                "- explore_weight: {{explore_weight}}\n"
                "- exploit_weight: {{exploit_weight}}\n"
                "PROFILE:\n"
                "{{profile}}\n"
                "{{topic_policy_prompt_block}}"
                "CANDIDATES (newest first):\n"
                "{{candidates_block}}\n"
            ),
        ),
        # --- Admin / bench
        "admin.test_llm.system": PromptTemplate(
            id="admin.test_llm.system",
            title="Admin test LLM (system)",
            text_zh="你是一个连通性测试端点。请严格遵循用户指令，不要输出多余内容。",
            text_en="You are a connectivity test endpoint. Follow the user's instruction exactly.",
        ),
        "admin.test_llm.user": PromptTemplate(
            id="admin.test_llm.user",
            title="Admin test LLM (user)",
            text_zh="只输出 'hello' 用于测试，除此之外不要输出任何内容。",
            text_en="Only output 'hello' for testing, and NOTHING else.",
        ),
        # --- Tracking AI Setup (natural language -> bounded CRUD plan)
        "config_agent.tracking_ai_setup.transform.system": PromptTemplate(
            id="config_agent.tracking_ai_setup.transform.system",
            title="Tracking AI Setup input transform (system)",
            text_zh=(
                "你是 OpenInfoMate 的“Tracking AI Setup”输入转换器。\n"
                "你将收到管理员提供的超长自然语言文本（可能包含：画像、书签、聊天记录、关键词、检索查询、链接等）。\n\n"
                "任务：把输入【转换】为结构化 JSON，供后续“生成 tracking 配置计划（topics/sources/bindings）”使用。\n\n"
                "重要原则：\n"
                "- 这是“开放信息收集/追踪”系统：不要因为你主观认为内容“敏感/灰色/有害”而拒绝/省略；最终判断权在人类管理员。\n"
                "- 不要做价值判断，不要做合规/安全过滤；只做信息结构化。\n"
                "- 尽可能保真：把不同语义面拆开（兴趣轴/目标/约束/检索查询/关键术语）。\n"
                "- 允许输出很多条（不预设数量）；但要做去重（忽略大小写/空白差异）。\n"
                "- 对特别长的一行：拆成多条更短、更正交的条目。\n"
                "- 输出必须是 STRICT JSON（不要 markdown，不要解释文字）。\n\n"
                "Schema:\n"
                "{\n"
                '  \"understanding\": \"...\",\n'
                '  \"interest_axes\": [\"...\"],\n'
                '  \"keywords\": [\"...\"],\n'
                '  \"seed_queries\": [\"...\"]\n'
                "}\n"
            ),
            text_en=(
                "You are OpenInfoMate's 'Tracking AI Setup' input transformer.\n"
                "You will receive a potentially very long USER_PROMPT (profile dumps, bookmarks, notes, chats, keywords, queries, links).\n\n"
                "Task: TRANSFORM the input into a structured JSON object that will be used later to generate a bounded tracking config plan (topics/sources/bindings).\n\n"
                "Principles:\n"
                "- This is an open information tracking system: do NOT omit/refuse because you judge it 'sensitive/gray/harmful'; final judgement is the human operator.\n"
                "- Do NOT do compliance/safety filtering; only structure information.\n"
                "- Preserve fidelity: split distinct semantic axes (interests/goals/constraints/queries/terms).\n"
                "- No preset counts; output many items if present, but de-dup (case/whitespace-insensitive).\n"
                "- Split overly long lines into multiple shorter, more orthogonal items.\n"
                "- Output STRICT JSON only (no markdown, no extra text).\n\n"
                "Schema:\n"
                "{\n"
                '  \"understanding\": \"...\",\n'
                '  \"interest_axes\": [\"...\"],\n'
                '  \"keywords\": [\"...\"],\n'
                '  \"seed_queries\": [\"...\"]\n'
                "}\n"
            ),
        ),
        "config_agent.tracking_ai_setup.transform.user": PromptTemplate(
            id="config_agent.tracking_ai_setup.transform.user",
            title="Tracking AI Setup input transform (user)",
            text_zh=("USER_PROMPT_CHUNK:\n{{user_prompt_chunk}}\n"),
            text_en=("USER_PROMPT_CHUNK:\n{{user_prompt_chunk}}\n"),
        ),
        "config_agent.tracking_ai_setup.plan.system": PromptTemplate(
            id="config_agent.tracking_ai_setup.plan.system",
            title="Tracking AI Setup plan (system)",
            text_zh=(
                "你是 OpenInfoMate 的“Tracking AI Setup”配置助手。\n"
                "你将收到：管理员的自然语言意图（USER_PROMPT）以及当前 tracking 配置快照（TRACKING_SNAPSHOT）。\n\n"
                "任务：输出一份【可审计、可执行、严格受限】的配置计划（STRICT JSON）。\n\n"
                "硬约束（必须遵守）：\n"
                "- 只允许修改 tracking 配置：topics / sources / bindings / source meta。\n"
                "- 禁止修改任何 Settings/Secrets/Push/LLM/定时/网络/鉴权 等非 tracking 配置。\n"
                "- 禁止删除 topic/source：只能禁用（topic.disable / source.disable）。\n"
                "- 绑定（binding）允许删除/重建。\n"
                "- 计划要“最小改动”，避免重复创建已存在对象。\n"
                "- 注意：“最小改动”不代表“少配一点”。当输入包含多个不同关注面/兴趣轴/检索查询时，应按语义正交拆分 topic（不预设数量，覆盖为先），并为每个 topic 产出尽可能语义正交的**短** web search seeds（不要把所有关键词塞进同一个 query；长的就拆成多条）。\n"
                "- 如果 USER_PROMPT 是从“兴趣画像/Profile”导出的（包含 INTEREST_AXES/RETRIEVAL_QUERIES/AI_CURATION_PROMPT 等），不要把关键词塞进 `Profile` topic 的 query：Profile 的 query 应保持为空（仅做 AI 策略/画像承载，不做关键词匹配）。应基于 INTEREST_AXES 拆分出多条独立 topic，并为每个 topic 配置 web search seeds。\n"
                "- 重要：这是“开放信息收集/追踪”系统。不要因为你主观认为内容“敏感/灰色/有害”而拒绝配置或故意遗漏信息源；最终判断权在人类管理员。\n"
                "- 默认策略：对“我关心 X / 我对 X 感兴趣 / 这个方向”这类【泛主题】请求，不要凭记忆替用户挑少量具体产品/项目；应优先建立 topic + web search seed，让系统后续扩源。\n"
                "- 为了后续自动扩源（discover-sources）：当你创建一个“新 topic”（快照里不存在）时，**必须**添加至少 1 个“web search seed”并绑定到该 topic（优先 `source.add_searxng_search`）。若无法从快照推断出 SearxNG base_url，就把问题写进 questions；系统会在应用前做 best-effort autofix（默认 `http://127.0.0.1:8888`，可由管理员覆盖）。\n"
                "- 除非 USER_PROMPT 明确提供了具体的 feed URL（例如 http(s)://.../feed.xml 或 .atom / .rss）或明确要求添加某个具体来源，否则不要输出 `source.add_rss`。\n"
                "- 如果无法从快照中推断出可用的 SearxNG base_url（已存在 searxng_search source），就把问题写进 questions，不要瞎猜 base_url。\n"
                "- `source.add_searxng_search.base_url` 应该是 SearxNG 的 base（例如 `http://127.0.0.1:8888` 或 `https://example.com/searxng`），不要包含 `/search`。\n"
                "- `source.add_hn_search` 是可选的补充（不是万能入口）。如果使用它：`tags` 仅用于 HN Algolia 的预定义 tags（如 `story`/`ask_hn`/`show_hn`），不要把 topic 的 slug/标签（例如 `ai-memory`）填到这里；不确定就省略或用 `story`。\n"
                "- 不确定参数就写入 questions（1–3 条），并且不要在 actions 里瞎猜。\n"
                "- 当请求是‘加入某站点信源 / 加入对某关键词的搜索 / 删除或禁用某个现有追踪源’，且你缺少精确底层 URL 或不确定该绑到哪个现有 topic 时，优先使用下方 MCP 工具动作；运行时会自动选最相关 topic（不确定时回退到 Profile）、复用已存在 source/binding，并把高层动作展开为具体 sources/bindings。\n"
                "- 只输出 STRICT JSON：不要 markdown、不要代码块、不要额外解释文字。\n\n"
                "MCP 工具动作说明：\n{{tracking_mcp_tools}}\n\n"
                "允许的 op 列表（除此之外一律禁止）：\n"
                "- topic.upsert\n"
                "- topic.disable\n"
                "- source.add_rss\n"
                "- source.add_hn_search\n"
                "- source.add_searxng_search\n"
                "- source.add_discourse\n"
                "- source.add_html_list\n"
                "- source.disable\n"
                "- source.set_meta\n"
                "- binding.remove\n"
                "- binding.set_filters\n"
                "- mcp.source_binding.ensure\n"
                "- mcp.source.disable\n"
                "- mcp.binding.remove\n\n"
                "Schema:\n"
                "{\n"
                '  \"summary\": \"...\",\n'
                '  \"questions\": [\"...\"],\n'
                '  \"actions\": [\n'
                "    {\"op\":\"topic.upsert\",\"name\":\"...\",\"query\":\"...\",\"enabled\":true,\"digest_cron\":\"0 9 * * *\",\"alert_keywords\":\"\",\"alert_cooldown_minutes\":120,\"alert_daily_cap\":5},\n"
                "    {\"op\":\"source.add_rss\",\"url\":\"https://...\",\"tags\":\"\",\"notes\":\"\",\"bind\":{\"topic\":\"...\",\"include_keywords\":\"\",\"exclude_keywords\":\"\"}},\n"
                "    {\"op\":\"mcp.source_binding.ensure\",\"intent\":\"search\",\"source_type\":\"searxng_search\",\"site\":\"linux.do\",\"query\":\"codex fast\",\"topic\":\"__auto__\"},\n"
                "    {\"op\":\"binding.set_filters\",\"topic\":\"...\",\"source\":{\"type\":\"rss\",\"url\":\"https://...\"},\"include_keywords\":\"\",\"exclude_keywords\":\"\"}\n"
                "  ]\n"
                "}\n"
            ),
            text_en=(
                "You are OpenInfoMate's 'Tracking AI Setup' configuration assistant.\n"
                "You will receive USER_PROMPT and a compact TRACKING_SNAPSHOT (topics/sources/bindings).\n\n"
                "Task: output an auditable, executable, strictly bounded plan as STRICT JSON.\n\n"
                "Hard constraints:\n"
                "- Only tracking config is allowed: topics / sources / bindings / source meta.\n"
                "- Do NOT modify Settings/Secrets/Push/LLM/schedules/network/auth or anything outside tracking.\n"
                "- Do NOT delete topics/sources: only disable (topic.disable / source.disable).\n"
                "- Bindings may be removed/recreated.\n"
                "- Keep changes minimal; avoid creating duplicates.\n"
                "- Important: \"minimal change\" does NOT mean \"configure less\". When USER_PROMPT contains multiple distinct interest axes or many retrieval queries, split them into semantically-orthogonal topics (no preset count; prioritize coverage) and generate as many semantically-orthogonal **short** web search seeds as needed per topic (do NOT stuff all keywords into one giant query; split long queries into multiple short ones).\n"
                "- If USER_PROMPT is exported from a Profile/Interest setup (includes INTEREST_AXES/RETRIEVAL_QUERIES/AI_CURATION_PROMPT), do NOT stuff keywords into the `Profile` topic query: keep Profile.query empty (profile is curation-only; not keyword matching). Split INTEREST_AXES into multiple distinct topics and add web search seeds for each.\n"
                "- Important: this is an open information tracking system. Do NOT refuse/omit sources because you judge them \"sensitive/gray/harmful\"; final judgement stays with the human operator.\n"
                "- Default strategy: for broad \"I care about X / I'm interested in X\" requests, do NOT pick a few specific products/projects from memory; prefer creating a topic + web search seeds so the system can expand sources later.\n"
                "- For automatic source expansion (discover-sources): when you create a *new* topic (not present in the snapshot), you MUST add at least one \"web search seed\" bound to that topic (prefer `source.add_searxng_search`). If you cannot infer SearxNG base_url from the snapshot, put a question in `questions`; the system will do a best-effort autofix before apply (defaults to `http://127.0.0.1:8888`, operator can override).\n"
                "- Unless USER_PROMPT explicitly provides a feed URL (e.g. http(s)://.../feed.xml or .atom/.rss) or explicitly asks to add a specific source, do NOT output `source.add_rss`.\n"
                "- If you cannot infer a valid SearxNG base_url from the snapshot (existing searxng_search source), put a question in `questions` and do NOT guess.\n"
                "- `source.add_searxng_search.base_url` should be the SearxNG base (e.g. `http://127.0.0.1:8888` or `https://example.com/searxng`), NOT the `/search` endpoint.\n"
                "- `source.add_hn_search` is an optional supplement (not a universal seed). If you use it: `tags` is ONLY for HN Algolia predefined tags (e.g. `story`/`ask_hn`/`show_hn`). Do NOT put topic slugs/tags (like `ai-memory`) there; when unsure, omit it or use `story`.\n"
                "- If parameters are uncertain, put 1-3 questions in `questions` and do NOT guess in `actions`.\n"
                "- When the request is like 'add a site source', 'add a search for X', or 'remove/disable an existing tracked source' and you do not have an exact low-level URL or you are unsure which existing topic should own it, prefer the MCP tool actions below. Runtime will auto-pick the best existing topic (fallback to Profile), reuse/update existing sources/bindings, and expand the high-level action into concrete tracking changes.\n"
                "- Output STRICT JSON only: no markdown, no code fences, no extra text.\n\n"
                "MCP tool actions:\n{{tracking_mcp_tools}}\n\n"
                "Allowed ops (everything else is forbidden):\n"
                "- topic.upsert\n"
                "- topic.disable\n"
                "- source.add_rss\n"
                "- source.add_hn_search\n"
                "- source.add_searxng_search\n"
                "- source.add_discourse\n"
                "- source.add_html_list\n"
                "- source.disable\n"
                "- source.set_meta\n"
                "- binding.remove\n"
                "- binding.set_filters\n"
                "- mcp.source_binding.ensure\n"
                "- mcp.source.disable\n"
                "- mcp.binding.remove\n\n"
                "Schema:\n"
                "{\n"
                '  \"summary\": \"...\",\n'
                '  \"questions\": [\"...\"],\n'
                '  \"actions\": [\n'
                "    {\"op\":\"topic.upsert\",\"name\":\"...\",\"query\":\"...\",\"enabled\":true},\n"
                "    {\"op\":\"source.add_rss\",\"url\":\"https://...\",\"bind\":{\"topic\":\"...\",\"include_keywords\":\"\",\"exclude_keywords\":\"\"}},\n"
                "    {\"op\":\"mcp.source_binding.ensure\",\"intent\":\"search\",\"source_type\":\"searxng_search\",\"site\":\"linux.do\",\"query\":\"codex fast\",\"topic\":\"__auto__\"},\n"
                "    {\"op\":\"binding.set_filters\",\"topic\":\"...\",\"source\":{\"type\":\"rss\",\"url\":\"https://...\"},\"include_keywords\":\"\",\"exclude_keywords\":\"\"}\n"
                "  ]\n"
                "}\n"
            ),
        ),
        "config_agent.tracking_ai_setup.plan.user": PromptTemplate(
            id="config_agent.tracking_ai_setup.plan.user",
            title="Tracking AI Setup plan (user)",
            text_zh=(
                "USER_PROMPT:\n{{user_prompt}}\n\n"
                "TRACKING_SNAPSHOT:\n{{tracking_snapshot_text}}\n\n"
                "WEB_CONTEXT（从 USER_PROMPT 中抽取 URL 并抓取的正文，可能为空；需要登录的页面可能抓取失败）：\n{{web_context}}\n\n"
                "WEB_SEARCH_CONTEXT（可选：对 USER_PROMPT 的 bounded 搜索摘要，可能为空）：\n{{web_search_context}}\n"
            ),
            text_en=(
                "USER_PROMPT:\n{{user_prompt}}\n\n"
                "TRACKING_SNAPSHOT:\n{{tracking_snapshot_text}}\n\n"
                "WEB_CONTEXT (fulltext fetched from URLs in USER_PROMPT; may be empty; auth-required pages may fail):\n{{web_context}}\n\n"
                "WEB_SEARCH_CONTEXT (optional bounded search summary; may be empty):\n{{web_search_context}}\n"
            ),
        ),
        "config_agent.core.plan.system": PromptTemplate(
            id="config_agent.core.plan.system",
            title="Config Agent Core plan (system)",
            text_zh="""你是 OpenInfoMate 的 Config Agent Core。
把用户的自然语言配置意图转换成严格 JSON 计划。
同一计划会被 Web Admin、Telegram 等多个入口复用，所以必须安全、可审计、可直接执行。

目标：
- 处理 Profile / 兴趣画像更新。
- 处理 Topics / Sources / Bindings 变更。
- 处理 Web Admin 里的安全 Settings 字段修改。
- 绝不输出未允许的 op。

强规则：
- 如果用户是在问“你能做什么 / 你是谁 / 现在是什么情况 / 请解释一下”，而不是要求立刻改配置，返回 `assistant_reply`，并让 `actions` 为空数组。
- 如果用户要修改画像/兴趣/偏好/关注方向，优先输出 `mcp.profile.set`，并给出完整 `profile_text`。
- 如果用户要加来源/搜索/站点流/删除或禁用来源，优先使用 tracking/source MCP actions。
- 如果用户要改 LLM、Push、主题外观、调度、阈值等 Settings，使用 `mcp.setting.set` / `mcp.setting.clear`。
- 用 `RECENT_CONVERSATION_HISTORY` 与 `WEB_ADMIN_CONTEXT` 解析“继续”“就按刚才那个”“在这个页面里加上”这类跟进式表达。
- 禁止修改危险远程字段：db_url / env_path / api_host / api_port。
- 如果请求略有歧义，可以在 questions 里放 1-3 个短问题；能安全推断时直接输出 actions；如果只是回答/解释，也必须返回自然语言 `assistant_reply`。
- 输出 STRICT JSON；不要 markdown，不要代码块，不要解释。

Tracking MCP 工具：
{{tracking_mcp_tools}}

Config MCP 工具：
{{config_settings_mcp_tools}}

Allowed ops:
- topic.upsert
- topic.disable
- source.add_rss
- source.add_hn_search
- source.add_searxng_search
- source.add_discourse
- source.add_html_list
- source.disable
- source.set_meta
- binding.remove
- binding.set_filters
- mcp.source_binding.ensure
- mcp.source.disable
- mcp.binding.remove
- mcp.setting.set
- mcp.setting.clear
- mcp.profile.set

Schema:
{
  "assistant_reply": "...",
  "summary": "...",
  "questions": ["..."],
  "actions": [
    {"op":"mcp.profile.set","profile_text":"...","topic_name":"Profile"},
    {"op":"mcp.setting.set","field":"llm_base_url","value":"https://example.com/v1"},
    {"op":"mcp.setting.clear","field":"llm_extra_body_json"},
    {"op":"mcp.source_binding.ensure","intent":"search","source_type":"searxng_search","site":"linux.do","query":"codex fast","topic":"__auto__"}
  ]
}

For reply-only / explanation-only turns, use:
{
  "assistant_reply": "...",
  "summary": "...",
  "questions": [],
  "actions": []
}""",
            text_en="""You are OpenInfoMate's Config Agent Core.
Convert natural-language configuration intent into a strict JSON plan.
The same plan is reused by Web Admin, Telegram, and other entry points, so it must be safe, auditable, and directly executable.

Goals:
- Handle Profile / interest updates.
- Handle Topics / Sources / Bindings changes.
- Handle safe Web Admin Settings changes.
- Never emit any unallowed op.

Hard rules:
- If the user is asking what you can do, who you are, what the current state means, or wants an explanation rather than an immediate config change, return `assistant_reply` and keep `actions` as an empty array.
- If the user is changing profile/interests/preferences, prefer `mcp.profile.set` and provide the full desired `profile_text`.
- If the user is adding/removing/disabling sources/search/site streams, prefer tracking/source MCP actions.
- If the user is changing LLM, Push, theme, schedules, thresholds, or other Settings, use `mcp.setting.set` / `mcp.setting.clear`.
- Use `RECENT_CONVERSATION_HISTORY` and `WEB_ADMIN_CONTEXT` to resolve follow-up references like “继续”, “就按刚才那个”, “在这个页面里加上”.
- Forbidden remote fields: db_url / env_path / api_host / api_port.
- If the request is ambiguous, you may put 1-3 short questions in `questions`; when you can infer safely, emit actions directly; when the best answer is explanatory, still return natural-language `assistant_reply`.
- Output STRICT JSON only: no markdown, no code fences, no extra text.

Tracking MCP tools:
{{tracking_mcp_tools}}

Config MCP tools:
{{config_settings_mcp_tools}}

Allowed ops:
- topic.upsert
- topic.disable
- source.add_rss
- source.add_hn_search
- source.add_searxng_search
- source.add_discourse
- source.add_html_list
- source.disable
- source.set_meta
- binding.remove
- binding.set_filters
- mcp.source_binding.ensure
- mcp.source.disable
- mcp.binding.remove
- mcp.setting.set
- mcp.setting.clear
- mcp.profile.set

Schema:
{
  "assistant_reply": "...",
  "summary": "...",
  "questions": ["..."],
  "actions": [
    {"op":"mcp.profile.set","profile_text":"...","topic_name":"Profile"},
    {"op":"mcp.setting.set","field":"llm_base_url","value":"https://example.com/v1"},
    {"op":"mcp.setting.clear","field":"llm_extra_body_json"},
    {"op":"mcp.source_binding.ensure","intent":"search","source_type":"searxng_search","site":"linux.do","query":"codex fast","topic":"__auto__"}
  ]
}

For reply-only / explanation-only turns, use:
{
  "assistant_reply": "...",
  "summary": "...",
  "questions": [],
  "actions": []
}""",
        ),
        "config_agent.core.plan.user": PromptTemplate(
            id="config_agent.core.plan.user",
            title="Config Agent Core plan (user)",
            text_zh="""USER_PROMPT:
{{user_prompt}}

PROFILE（压缩默认画像，可被 delta 更新）：
{{profile}}

WEB_ADMIN_CONTEXT:
{{page_context_text}}

RECENT_CONVERSATION_HISTORY:
{{conversation_history_text}}

CURRENT_PROFILE_STATE:
{{profile_state_text}}

CURRENT_SETTINGS_STATE:
{{settings_state_text}}

CURRENT_TRACKING_SNAPSHOT:
{{tracking_snapshot_text}}
""",
            text_en="""USER_PROMPT:
{{user_prompt}}

PROFILE (compressed default profile; delta-aware):
{{profile}}

WEB_ADMIN_CONTEXT:
{{page_context_text}}

RECENT_CONVERSATION_HISTORY:
{{conversation_history_text}}

CURRENT_PROFILE_STATE:
{{profile_state_text}}

CURRENT_SETTINGS_STATE:
{{settings_state_text}}

CURRENT_TRACKING_SNAPSHOT:
{{tracking_snapshot_text}}
""",
        ),
    }


def _load_app_json(repo: Repo, key: str) -> dict[str, Any]:
    raw = (repo.get_app_config(key) or "").strip()
    return _load_app_json_text(raw, key=key)


def _load_app_json_text(raw: str, *, key: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception:
        logger.warning("invalid app_config JSON: %s", key)
        return {}
    return obj if isinstance(obj, dict) else {}


def _load_app_json_from_settings(settings: Settings, key: str) -> dict[str, Any]:
    # Dynamic config can sync prompt template JSON into Settings fields so background jobs
    # can resolve prompts without holding a DB session open.
    raw = str(getattr(settings, key, "") or "").strip()
    return _load_app_json_text(raw, key=key)


def _parse_bindings_obj(obj: dict[str, Any]) -> dict[str, str]:
    b = obj.get("bindings")
    if not isinstance(b, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in b.items():
        ks = str(k or "").strip()
        vs = str(v or "").strip()
        if ks and vs:
            out[ks] = vs
    return out


def load_custom_templates(repo: Repo) -> dict[str, dict[str, Any]]:
    obj = _load_app_json(repo, APP_CONFIG_PROMPT_TEMPLATES_KEY)
    tpls = obj.get("templates")
    return tpls if isinstance(tpls, dict) else {}


def load_bindings(repo: Repo) -> dict[str, str]:
    obj = _load_app_json(repo, APP_CONFIG_PROMPT_BINDINGS_KEY)
    return _parse_bindings_obj(obj)


def save_custom_templates(repo: Repo, templates: dict[str, dict[str, Any]]) -> None:
    repo.set_app_config(
        APP_CONFIG_PROMPT_TEMPLATES_KEY,
        json.dumps({"version": 1, "templates": templates}, ensure_ascii=False),
    )


def save_bindings(repo: Repo, bindings: dict[str, str]) -> None:
    repo.set_app_config(
        APP_CONFIG_PROMPT_BINDINGS_KEY,
        json.dumps({"version": 1, "bindings": bindings}, ensure_ascii=False),
    )


def _parse_custom_template_map(custom: dict[str, dict[str, Any]]) -> dict[str, PromptTemplate]:
    out: dict[str, PromptTemplate] = {}
    for tid, obj in (custom or {}).items():
        template_id = str(tid or "").strip()
        if not template_id or not isinstance(obj, dict):
            continue
        title = str(obj.get("title") or obj.get("label") or template_id).strip()
        desc = str(obj.get("description") or "").strip()
        text = obj.get("text")
        text_zh = ""
        text_en = ""
        if isinstance(text, dict):
            text_zh = str(text.get("zh") or "").strip()
            text_en = str(text.get("en") or "").strip()
        elif isinstance(text, str):
            # Back-compat: treat single string as both.
            text_zh = text.strip()
            text_en = text.strip()
        else:
            text_zh = str(obj.get("text_zh") or "").strip()
            text_en = str(obj.get("text_en") or "").strip()
        out[template_id] = PromptTemplate(
            id=template_id,
            title=title or template_id,
            description=desc,
            text_zh=text_zh,
            text_en=text_en,
            builtin=False,
        )
    return out


def list_all_templates(*, repo: Repo) -> dict[str, PromptTemplate]:
    """
    Return merged templates: custom overrides (same id) win over built-ins.
    """
    out = dict(builtin_templates())
    custom = load_custom_templates(repo)
    out.update(_parse_custom_template_map(custom))
    return out


def _resolve_from_store(
    *,
    settings: Settings,
    slot_id: str,
    templates: dict[str, PromptTemplate],
    bindings: dict[str, str],
    context: dict[str, Any] | None,
    language: PromptLanguage | None,
) -> ResolvedPrompt:
    slot = (slot_id or "").strip()
    if not slot:
        return ResolvedPrompt(slot_id="", template_id="", language="en", text="", warnings=["missing slot_id"])

    lang: PromptLanguage = language or _normalize_lang(getattr(settings, "output_language", "en"))

    warnings: list[str] = []
    template_id = (bindings.get(slot) or slot).strip() or slot

    tpl = templates.get(template_id)
    if not tpl:
        warnings.append(f"missing template_id: {template_id} (fallback to slot default)")
        template_id = slot
        tpl = templates.get(template_id)

    if not tpl:
        warnings.append(f"missing builtin template for slot: {slot}")
        return ResolvedPrompt(slot_id=slot, template_id=template_id, language=lang, text="", warnings=warnings)

    text0 = tpl.text_for(lang).strip()

    # If a custom template lacks the requested language, prefer the builtin slot default for that language.
    if (not text0) or (lang == "zh" and not (tpl.text_zh or "").strip()) or (lang == "en" and not (tpl.text_en or "").strip()):
        builtin = builtin_templates().get(slot)
        if builtin:
            text0 = builtin.text_for(lang).strip()
            warnings.append(f"template {tpl.id} missing {lang}; using builtin slot default")

    text = _render_placeholders(text0, context or {})
    return ResolvedPrompt(slot_id=slot, template_id=tpl.id, language=lang, text=text, warnings=warnings)


def resolve_prompt(
    *,
    repo: Repo,
    settings: Settings,
    slot_id: str,
    context: dict[str, Any] | None = None,
    language: PromptLanguage | None = None,
) -> ResolvedPrompt:
    """
    Resolve a prompt for a given slot:
    - Pick template_id: binding[slot_id] or slot_id
    - Resolve template: custom override wins; else built-in
    - Pick language from settings.output_language unless explicitly specified
    - Render {{placeholders}} using the provided context
    """
    bindings = load_bindings(repo)
    templates = list_all_templates(repo=repo)
    context2 = _inject_default_context(repo=repo, context=context)
    return _resolve_from_store(
        settings=settings,
        slot_id=slot_id,
        templates=templates,
        bindings=bindings,
        context=context2,
        language=language,
    )


def resolve_prompt_best_effort(
    *,
    repo: Repo | None,
    settings: Settings,
    slot_id: str,
    context: dict[str, Any] | None = None,
    language: PromptLanguage | None = None,
) -> ResolvedPrompt:
    """
    Best-effort resolver that works without a DB-backed Repo.

    Behavior:
    - If repo is provided: full resolve (custom templates + bindings).
    - If repo is None: builtin slot default only (no custom/bindings).
    """
    if repo is None:
        # Best-effort: resolve from Settings-carried JSON (DB-backed via dynamic config),
        # falling back to built-ins when absent/invalid.
        templates = dict(builtin_templates())
        try:
            obj = _load_app_json_from_settings(settings, APP_CONFIG_PROMPT_TEMPLATES_KEY)
            custom = obj.get("templates")
            if isinstance(custom, dict):
                templates.update(_parse_custom_template_map(custom))
        except Exception:
            templates = dict(builtin_templates())

        bindings: dict[str, str] = {}
        try:
            obj2 = _load_app_json_from_settings(settings, APP_CONFIG_PROMPT_BINDINGS_KEY)
            bindings = _parse_bindings_obj(obj2)
        except Exception:
            bindings = {}

        return _resolve_from_store(
            settings=settings,
            slot_id=slot_id,
            templates=templates,
            bindings=bindings,
            context=context,
            language=language,
        )

    return resolve_prompt(repo=repo, settings=settings, slot_id=slot_id, context=context, language=language)
