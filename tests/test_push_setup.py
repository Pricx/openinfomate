from __future__ import annotations

import pytest

from tracker.push_setup import parse_push_setup_env_block


def test_parse_push_setup_env_block_filters_and_validates():
    text = """
    # comment
    TRACKER_CRON_TIMEZONE=Asia/Shanghai
    TRACKER_OUTPUT_LANGUAGE=中文
    TRACKER_CRON_MISFIRE_GRACE_SECONDS=3600
    TRACKER_LLM_BASE_URL=http://127.0.0.1:8317
    TRACKER_LLM_MODEL=gpt-5.2
    TRACKER_LLM_API_KEY=sk-placeholder
    TRACKER_LLM_MODEL_REASONING=gpt-5.2
    TRACKER_LLM_MODEL_MINI=gpt-5.2-mini
    TRACKER_LLM_EXTRA_BODY_JSON={"reasoning":{"effort":"xhigh"}}
    TRACKER_DIGEST_SCHEDULER_ENABLED=false
    TRACKER_DIGEST_PUSH_ENABLED=false
    TRACKER_DINGTALK_WEBHOOK_URL=https://oapi.dingtalk.com/robot/send?access_token=abc
    TRACKER_TELEGRAM_OWNER_USER_ID=123
    TRACKER_TELEGRAM_CONNECT_POLL_SECONDS=60
    TRACKER_SMTP_PORT=587
    TRACKER_SMTP_STARTTLS=true
    TRACKER_SMTP_USE_SSL=false
    TRACKER_EMAIL_TO=a@example.com,b@example.com
    OTHER_KEY=ignored
    TRACKER_SMTP_PORT=587
    """
    result = parse_push_setup_env_block(text)
    assert result.updates["TRACKER_CRON_TIMEZONE"] == "Asia/Shanghai"
    assert result.updates["TRACKER_OUTPUT_LANGUAGE"] == "zh"
    assert result.updates["TRACKER_CRON_MISFIRE_GRACE_SECONDS"] == "3600"
    assert result.updates["TRACKER_LLM_BASE_URL"].startswith("http://")
    assert result.updates["TRACKER_LLM_MODEL"] == "gpt-5.2"
    assert result.updates["TRACKER_LLM_API_KEY"] == "sk-placeholder"
    assert result.updates["TRACKER_LLM_MODEL_REASONING"] == "gpt-5.2"
    assert result.updates["TRACKER_LLM_MODEL_MINI"] == "gpt-5.2-mini"
    assert result.updates["TRACKER_LLM_EXTRA_BODY_JSON"] == '{"reasoning":{"effort":"xhigh"}}'
    assert result.updates["TRACKER_DIGEST_SCHEDULER_ENABLED"] == "false"
    assert result.updates["TRACKER_DIGEST_PUSH_ENABLED"] == "false"
    assert result.updates["TRACKER_DINGTALK_WEBHOOK_URL"].startswith("https://")
    assert result.updates["TRACKER_TELEGRAM_OWNER_USER_ID"] == "123"
    assert result.updates["TRACKER_TELEGRAM_CONNECT_POLL_SECONDS"] == "60"
    assert result.updates["TRACKER_SMTP_PORT"] == "587"
    assert result.updates["TRACKER_SMTP_STARTTLS"] == "true"
    assert result.updates["TRACKER_SMTP_USE_SSL"] == "false"
    assert result.updates["TRACKER_EMAIL_TO"] == "a@example.com,b@example.com"
    assert "OTHER_KEY" not in result.updates


@pytest.mark.parametrize(
    "text",
    [
        "TRACKER_SMTP_PORT=0\n",
        "TRACKER_SMTP_PORT=70000\n",
        "TRACKER_SMTP_PORT=oops\n",
    ],
)
def test_parse_push_setup_env_block_rejects_bad_port(text: str):
    with pytest.raises(ValueError):
        parse_push_setup_env_block(text)


@pytest.mark.parametrize(
    "text",
    [
        "TRACKER_SMTP_STARTTLS=yes\n",
        "TRACKER_SMTP_USE_SSL=maybe\n",
    ],
)
def test_parse_push_setup_env_block_rejects_bad_booleans(text: str):
    with pytest.raises(ValueError):
        parse_push_setup_env_block(text)


@pytest.mark.parametrize(
    "text",
    [
        "TRACKER_DIGEST_SCHEDULER_ENABLED=yes\n",
        "TRACKER_DIGEST_PUSH_ENABLED=maybe\n",
    ],
)
def test_parse_push_setup_env_block_rejects_bad_delivery_booleans(text: str):
    with pytest.raises(ValueError):
        parse_push_setup_env_block(text)


@pytest.mark.parametrize(
    "text",
    [
        "TRACKER_OUTPUT_LANGUAGE=oops\n",
        "TRACKER_CRON_MISFIRE_GRACE_SECONDS=-1\n",
        "TRACKER_TELEGRAM_OWNER_USER_ID=not-a-number\n",
        "TRACKER_TELEGRAM_CONNECT_POLL_SECONDS=-2\n",
    ],
)
def test_parse_push_setup_env_block_rejects_bad_core_config(text: str):
    with pytest.raises(ValueError):
        parse_push_setup_env_block(text)
