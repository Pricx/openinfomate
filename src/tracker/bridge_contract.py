from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


BRIDGE_CONTRACT_NAME = "openinfomate-internal-bridge"
BRIDGE_CONTRACT_MAJOR = 1
BRIDGE_CONTRACT_VERSION = "2026-03-22.1"


class BridgeLlmOverride(BaseModel):
    base_url: str = Field(min_length=1, max_length=2_000)
    api_key: str = Field(default="", max_length=8_000)
    model: str = Field(min_length=1, max_length=200)
    compat_mode: str = Field(default="auto", max_length=64)
    timeout_seconds: int | None = Field(default=None, ge=1, le=600)


class BridgeMetaResponse(BaseModel):
    contract_name: str = BRIDGE_CONTRACT_NAME
    contract_major: int = BRIDGE_CONTRACT_MAJOR
    contract_version: str = BRIDGE_CONTRACT_VERSION
    base_path: str = "/internal/bridge/v1"
    reverse_operations: list[str] = ["/api/internal/telegram/upstream-bind/consume"]


class BridgeProfileProposeRequest(BaseModel):
    text: str = Field(min_length=1, max_length=200_000)
    llm_override: BridgeLlmOverride | None = None


class BridgeProfileProposeResponse(BaseModel):
    normalized_profile_text: str
    understanding: str = ""
    interest_axes: list[str] = []
    interest_keywords: list[str] = []
    retrieval_queries: list[str] = []
    ai_prompt: str = ""


class BridgeTopicProposeRequest(BaseModel):
    name: str = Field(default="", max_length=200)
    brief: str = Field(min_length=1, max_length=20_000)
    llm_override: BridgeLlmOverride | None = None


class BridgeTopicProposeSourceHints(BaseModel):
    add_hn: bool = True
    add_searxng: bool = True
    add_discourse: bool = False
    discourse_base_url: str = ""
    discourse_json_path: str = "/latest.json"
    add_nodeseek: bool = False


class BridgeTopicProposeResponse(BaseModel):
    topic_name: str
    query: str
    alert_keywords: str = ""
    ai_prompt: str = ""
    source_hints: BridgeTopicProposeSourceHints | None = None


class BridgeTrackingPlanRequest(BaseModel):
    text: str = Field(min_length=1, max_length=200_000)
    profile_topic_name: str = Field(default="Profile", max_length=200)
    profile_understanding: str = Field(default="", max_length=8_000)
    profile_interest_axes: list[str] = []
    profile_interest_keywords: list[str] = []
    profile_retrieval_queries: list[str] = []
    tracking_snapshot: dict[str, Any] | None = None
    llm_override: BridgeLlmOverride | None = None


class BridgeTrackingPlanResponse(BridgeProfileProposeResponse):
    input_brief: str = ""
    warnings: list[str] = []
    actions: list[dict[str, Any]] = []


class BridgeConfigPlanRequest(BaseModel):
    user_prompt: str = Field(min_length=1, max_length=80_000)
    profile_text: str = Field(default="", max_length=200_000)
    profile_topic_name: str = Field(default="Profile", max_length=200)
    profile_understanding: str = Field(default="", max_length=8_000)
    profile_interest_axes: list[str] = []
    profile_interest_keywords: list[str] = []
    profile_retrieval_queries: list[str] = []
    tracking_snapshot: dict[str, Any] | None = None
    conversation_history_text: str = Field(default="", max_length=6_000)
    page_context_text: str = Field(default="", max_length=2_000)
    settings_state_text: str = Field(default="", max_length=16_000)
    settings_mcp_tools_text: str = Field(default="", max_length=16_000)
    allowed_setting_fields: list[str] = []
    llm_override: BridgeLlmOverride | None = None


class BridgeConfigPlanResponse(BaseModel):
    assistant_reply: str = ""
    summary: str = ""
    questions: list[str] = []
    warnings: list[str] = []
    actions: list[dict[str, Any]] = []
