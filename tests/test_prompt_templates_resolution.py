from __future__ import annotations

import json

from tracker.prompt_templates import resolve_prompt_best_effort
from tracker.settings import Settings


def _settings_with_templates(*, templates: dict, bindings: dict, output_language: str = "en") -> Settings:
    return Settings(
        output_language=output_language,
        prompt_templates_custom_json=json.dumps({"version": 1, "templates": templates}, ensure_ascii=False),
        prompt_template_bindings_json=json.dumps({"version": 1, "bindings": bindings}, ensure_ascii=False),
    )


def test_resolve_best_effort_uses_settings_custom_template_and_binding():
    settings = _settings_with_templates(
        templates={"custom.task": {"title": "Custom task", "text": {"zh": "自定义ZH", "en": "CUSTOM_EN"}}},
        bindings={"admin.test_llm.user": "custom.task"},
        output_language="zh",
    )

    r1 = resolve_prompt_best_effort(
        repo=None,
        settings=settings,
        slot_id="admin.test_llm.user",
        language="zh",  # type: ignore[arg-type]
    )
    assert r1.template_id == "custom.task"
    assert r1.text == "自定义ZH"

    r2 = resolve_prompt_best_effort(
        repo=None,
        settings=settings,
        slot_id="admin.test_llm.user",
        language="en",  # type: ignore[arg-type]
    )
    assert r2.template_id == "custom.task"
    assert r2.text == "CUSTOM_EN"


def test_resolve_best_effort_renders_placeholders():
    settings = _settings_with_templates(
        templates={"custom.hello": {"title": "Hello", "text": {"zh": "你好，{{name}}", "en": "Hello, {{name}}"}}},
        bindings={"admin.test_llm.user": "custom.hello"},
        output_language="en",
    )

    r = resolve_prompt_best_effort(
        repo=None,
        settings=settings,
        slot_id="admin.test_llm.user",
        context={"name": "World"},
        language="en",  # type: ignore[arg-type]
    )
    assert r.text == "Hello, World"


def test_resolve_best_effort_missing_template_id_falls_back_to_slot_default():
    settings = _settings_with_templates(
        templates={},
        bindings={"admin.test_llm.user": "missing.template"},
        output_language="en",
    )

    r = resolve_prompt_best_effort(
        repo=None,
        settings=settings,
        slot_id="admin.test_llm.user",
        language="en",  # type: ignore[arg-type]
    )
    assert r.template_id == "admin.test_llm.user"
    assert "hello" in r.text.lower()
    assert any("missing template_id: missing.template" in w for w in (r.warnings or []))


def test_resolve_best_effort_custom_template_missing_lang_uses_builtin_default():
    settings = _settings_with_templates(
        templates={"custom.en_only": {"title": "EN only", "text": {"zh": "", "en": "EN_ONLY"}}},
        bindings={"admin.test_llm.user": "custom.en_only"},
        output_language="zh",
    )

    r = resolve_prompt_best_effort(
        repo=None,
        settings=settings,
        slot_id="admin.test_llm.user",
        language="zh",  # type: ignore[arg-type]
    )
    assert r.text == "只输出 'hello' 用于测试，除此之外不要输出任何内容。"
    assert any("missing zh" in w for w in (r.warnings or []))


def test_resolve_best_effort_invalid_json_falls_back_to_builtin():
    settings = Settings(
        output_language="en",
        prompt_templates_custom_json="{not json",
        prompt_template_bindings_json="{not json",
    )
    r = resolve_prompt_best_effort(repo=None, settings=settings, slot_id="admin.test_llm.user", language="en")  # type: ignore[arg-type]
    assert r.template_id == "admin.test_llm.user"
    assert "hello" in r.text.lower()
