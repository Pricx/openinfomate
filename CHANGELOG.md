# Changelog

## 0.1.0

- Initial public release of OpenInfoMate (“信息秘书”).
- Web Admin UI:
  - Setup wizards (`/setup/push`, `/setup/profile`, `/setup/topic`).
  - Friendly Settings (human-readable names + env key hints) + env↔DB sync + config export/import.
- Telegram bot:
  - Push + website-free operator commands (`/setup`, `/status`, `/config`, `/llm`, `/profile`, `/t`, `/s`, `/bindings`, `/push`, `/api`, `/env`, `/why`, `/restart`).
  - Feedback loop: reactions/replies + domain mute/exclude + profile delta updates (reasoning model only).
- AI-native curation:
  - Prompt-driven LLM curation per topic (no keyword matching), optional mini triage, and an optional “Priority Lane” for fast alerts.
  - Built-in RSS pack import (90+ high-quality blogs) for broad recall (LLM filters hard).
