from __future__ import annotations

from dataclasses import dataclass

from tracker.envfile import parse_env_assignments


@dataclass(frozen=True)
class PushSetupParseResult:
    updates: dict[str, str]


_PUSH_SETUP_ALLOWED_KEYS: set[str] = {
    # Timezone
    "TRACKER_CRON_TIMEZONE",
    # Language / formatting
    "TRACKER_OUTPUT_LANGUAGE",
    # Cron reliability
    "TRACKER_CRON_MISFIRE_GRACE_SECONDS",
    "TRACKER_DIGEST_SCHEDULER_ENABLED",
    "TRACKER_DIGEST_PUSH_ENABLED",
    # AI-native selection (global toggle)
    "TRACKER_LLM_CURATION_ENABLED",
    # Full-text enrichment (optional; improves AI curation)
    "TRACKER_FULLTEXT_ENABLED",
    # LLM core config / routing (optional)
    "TRACKER_LLM_BASE_URL",
    "TRACKER_LLM_API_KEY",
    "TRACKER_LLM_PROXY",
    "TRACKER_LLM_MODEL",
    "TRACKER_LLM_MODEL_REASONING",
    "TRACKER_LLM_MODEL_MINI",
    "TRACKER_LLM_MINI_BASE_URL",
    "TRACKER_LLM_MINI_API_KEY",
    "TRACKER_LLM_MINI_PROXY",
    "TRACKER_LLM_EXTRA_BODY_JSON",
    # DingTalk
    "TRACKER_PUSH_DINGTALK_ENABLED",
    "TRACKER_DINGTALK_WEBHOOK_URL",
    "TRACKER_DINGTALK_SECRET",
    # Telegram
    "TRACKER_TELEGRAM_BOT_TOKEN",
    "TRACKER_TELEGRAM_BOT_USERNAME",
    "TRACKER_TELEGRAM_CHAT_ID",
    "TRACKER_TELEGRAM_OWNER_USER_ID",
    "TRACKER_TELEGRAM_DISABLE_PREVIEW",
    "TRACKER_TELEGRAM_CONNECT_POLL_SECONDS",
    # Generic webhook
    "TRACKER_WEBHOOK_URL",
    # SMTP
    "TRACKER_SMTP_HOST",
    "TRACKER_SMTP_PORT",
    "TRACKER_SMTP_USER",
    "TRACKER_SMTP_PASSWORD",
    "TRACKER_SMTP_STARTTLS",
    "TRACKER_SMTP_USE_SSL",
    # Email
    "TRACKER_EMAIL_FROM",
    "TRACKER_EMAIL_TO",
}


def parse_push_setup_env_block(text: str) -> PushSetupParseResult:
    """
    Parse a dotenv-ish env block and return safe updates for push setup.

    - Only whitelisted keys are allowed.
    - Blank values are treated as "no change".
    - Validates numeric/boolean fields.
    """
    raw = parse_env_assignments(text)

    updates: dict[str, str] = {}
    for key in _PUSH_SETUP_ALLOWED_KEYS:
        value = (raw.get(key) or "").strip()
        if not value:
            continue

        if key == "TRACKER_OUTPUT_LANGUAGE":
            low = value.strip().lower()
            if value in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语", "漢語"}:
                updates[key] = "zh"
                continue
            if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
                updates[key] = "zh"
                continue
            if value in {"英文", "英语", "英語"}:
                updates[key] = "en"
                continue
            if low in {"en", "en-us", "english"} or low.startswith("en"):
                updates[key] = "en"
                continue
            raise ValueError("invalid TRACKER_OUTPUT_LANGUAGE (expected zh|en)")

        if key == "TRACKER_SMTP_PORT":
            try:
                port = int(value)
            except ValueError as exc:
                raise ValueError("invalid TRACKER_SMTP_PORT") from exc
            if port < 1 or port > 65535:
                raise ValueError("invalid TRACKER_SMTP_PORT")
            updates[key] = str(port)
            continue

        if key in {"TRACKER_SMTP_STARTTLS", "TRACKER_SMTP_USE_SSL"}:
            v = value.lower()
            if v not in {"true", "false"}:
                raise ValueError(f"invalid {key} (expected true|false)")
            updates[key] = v
            continue

        if key in {"TRACKER_PUSH_DINGTALK_ENABLED"}:
            v = value.lower()
            if v not in {"true", "false"}:
                raise ValueError(f"invalid {key} (expected true|false)")
            updates[key] = v
            continue

        if key in {"TRACKER_TELEGRAM_DISABLE_PREVIEW"}:
            v = value.lower()
            if v not in {"true", "false"}:
                raise ValueError(f"invalid {key} (expected true|false)")
            updates[key] = v
            continue

        if key in {"TRACKER_DIGEST_SCHEDULER_ENABLED", "TRACKER_DIGEST_PUSH_ENABLED"}:
            v = value.lower()
            if v not in {"true", "false"}:
                raise ValueError(f"invalid {key} (expected true|false)")
            updates[key] = v
            continue

        if key in {"TRACKER_LLM_CURATION_ENABLED", "TRACKER_FULLTEXT_ENABLED"}:
            v = value.lower()
            if v not in {"true", "false"}:
                raise ValueError(f"invalid {key} (expected true|false)")
            updates[key] = v
            continue

        if key in {
            "TRACKER_CRON_MISFIRE_GRACE_SECONDS",
            "TRACKER_TELEGRAM_CONNECT_POLL_SECONDS",
        }:
            try:
                n = int(value)
            except ValueError as exc:
                raise ValueError(f"invalid {key} (expected int)") from exc
            if n < 0:
                raise ValueError(f"invalid {key} (expected >= 0)")
            updates[key] = str(n)
            continue

        if key == "TRACKER_TELEGRAM_OWNER_USER_ID":
            if not value.isdigit():
                raise ValueError("invalid TRACKER_TELEGRAM_OWNER_USER_ID (expected numeric user id)")
            updates[key] = value
            continue

        updates[key] = value

    return PushSetupParseResult(updates=updates)
