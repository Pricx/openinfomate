from __future__ import annotations

import base64
import asyncio
import datetime as dt
import json
import logging
import os
import re
import secrets
import httpx
from dataclasses import asdict
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from jinja2 import pass_context
from pydantic import BaseModel, Field
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from tracker.db import session_factory
from tracker.job_lock import job_lock
from tracker.logging_config import configure_logging
from tracker.models import Base
from tracker.repo import Repo
from tracker.runner import (
    HealthResult,
    TickResult,
    run_curated_info,
    run_digest,
    run_health_report,
    run_tick,
    run_discover_sources,
)
from tracker.prompt_presets import (
    topic_policy_presets as get_topic_policy_presets,
)
from tracker.settings import Settings, get_settings
from tracker.actions import (
    SourceBindingSpec,
    TopicSpec,
    accept_source_candidate as accept_source_candidate_action,
    create_binding as create_binding_action,
    create_discourse_source as create_discourse_source_action,
    create_html_list_source as create_html_list_source_action,
    create_hn_search_source as create_hn_search_source_action,
    create_rss_source as create_rss_source_action,
    create_searxng_search_source as create_searxng_search_source_action,
    create_topic as create_topic_action,
    ignore_source_candidate as ignore_source_candidate_action,
    remove_binding as remove_binding_action,
    set_topic_enabled as set_topic_enabled_action,
    sync_topic_search_sources as sync_topic_search_sources_action,
    update_binding as update_binding_action,
    update_source_meta as update_source_meta_action,
)
from tracker.llm import llm_plan_tracking_ai_setup, llm_propose_profile_setup, llm_propose_topic_setup
from tracker.llm_usage import estimate_llm_cost_usd, make_llm_usage_recorder
from tracker.i18n import LANG_COOKIE_NAME, SUPPORTED_LANGS, get_request_lang, normalize_lang, t as translate_text
from tracker.envfile import parse_env_assignments

logger = logging.getLogger(__name__)


def _looks_like_loopback_host(host: str) -> bool:
    h = (host or "").strip().lower()
    return h in {"127.0.0.1", "::1", "localhost"}


def _try_basic_auth(request: Request, *, username: str, password: str) -> bool:
    raw = request.headers.get("authorization") or ""
    if not raw:
        return False
    scheme, _, value = raw.partition(" ")
    if scheme.strip().lower() != "basic":
        return False
    try:
        decoded = base64.b64decode(value.strip()).decode("utf-8")
    except Exception:
        return False
    if ":" not in decoded:
        return False
    u, p = decoded.split(":", 1)
    # Use bytes for compare_digest so non-ASCII credentials (e.g. 中文密码) work.
    try:
        u_b = str(u).encode("utf-8")
        p_b = str(p).encode("utf-8")
        user_b = str(username).encode("utf-8")
        pass_b = str(password).encode("utf-8")
    except Exception:
        return False
    return secrets.compare_digest(u_b, user_b) and secrets.compare_digest(p_b, pass_b)


_AUTH_TOKEN_COOKIE_NAME = "tracker_token"

_PLACEHOLDER_SECRETS = {"change-me", "changeme", "your-password", "password", "secret", "set-me"}

# Best-effort cache to avoid re-reading `.env` on every request.
_ENV_AUTH_CACHE: dict[str, Any] = {
    "path": "",
    "mtime": -1.0,
    "admin_username": None,
    "admin_password": None,
    "api_token": None,
}


def _is_placeholder_secret(v: str | None) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if not s:
        return True
    return s.lower() in _PLACEHOLDER_SECRETS


def _load_auth_from_envfile(settings: Settings) -> tuple[str | None, str | None, str | None]:
    """
    Load auth secrets from the env file (best-effort).

    Why:
    - Web Admin can update `.env` without restarting the process.
    - We want auth changes (username/password/token) to take effect immediately.
    """
    env_path = Path(str(getattr(settings, "env_path", "") or ".env"))
    try:
        st = env_path.stat()
    except Exception:
        return None, None, None

    mtime = float(getattr(st, "st_mtime", 0.0) or 0.0)
    if _ENV_AUTH_CACHE.get("path") == str(env_path) and float(_ENV_AUTH_CACHE.get("mtime") or -1.0) == mtime:
        return (
            _ENV_AUTH_CACHE.get("admin_username"),
            _ENV_AUTH_CACHE.get("admin_password"),
            _ENV_AUTH_CACHE.get("api_token"),
        )

    try:
        text = env_path.read_text(encoding="utf-8")
    except Exception:
        text = ""
    try:
        assignments = parse_env_assignments(text)
    except Exception:
        assignments = {}

    u = str(assignments.get("TRACKER_ADMIN_USERNAME") or "").strip() or None
    p = str(assignments.get("TRACKER_ADMIN_PASSWORD") or "").strip() or None
    t = str(assignments.get("TRACKER_API_TOKEN") or "").strip() or None

    if _is_placeholder_secret(p):
        p = None
    if _is_placeholder_secret(t):
        t = None

    _ENV_AUTH_CACHE["path"] = str(env_path)
    _ENV_AUTH_CACHE["mtime"] = mtime
    _ENV_AUTH_CACHE["admin_username"] = u
    _ENV_AUTH_CACHE["admin_password"] = p
    _ENV_AUTH_CACHE["api_token"] = t

    return u, p, t


def _token_auth_enabled(settings: Settings) -> bool:
    try:
        _u, _p, env_token = _load_auth_from_envfile(settings)
    except Exception:
        env_token = None
    return bool(env_token) or bool(getattr(settings, "api_token", None))


def _get_cookie_value(cookie_header: str, name: str) -> str:
    raw = (cookie_header or "").strip()
    if not raw or not name:
        return ""
    try:
        c = SimpleCookie()
        c.load(raw)
        morsel = c.get(name)
        if morsel is None:
            return ""
        return str(morsel.value or "")
    except Exception:
        return ""


def _require_auth(request: Request, settings: Settings) -> None:
    """
    Auth options:
    - `TRACKER_API_TOKEN`: header `x-tracker-token` or query `?token=...` (legacy / CLI-friendly)
    - `TRACKER_ADMIN_USERNAME` + `TRACKER_ADMIN_PASSWORD`: HTTP Basic auth (browser-friendly)
    """
    # Allow auth to be updated via `.env` without restart (Web Admin writes).
    env_user, env_pw, env_token = _load_auth_from_envfile(settings)

    api_token = env_token if env_token is not None else (settings.api_token or None)
    admin_password = env_pw if env_pw is not None else (settings.admin_password or None)
    admin_username = env_user if env_user is not None else (settings.admin_username or "admin")

    # If neither is configured, allow access (safe when binding to localhost / bootstrap).
    has_token = bool(api_token)
    has_admin = bool(admin_password)
    if not (has_token or has_admin):
        return

    if has_token:
        token = (
            request.headers.get("x-tracker-token")
            or request.query_params.get("token")
            or request.cookies.get(_AUTH_TOKEN_COOKIE_NAME)
        )
        if token == api_token:
            return

    if has_admin and _try_basic_auth(
        request,
        username=str(admin_username or "admin"),
        password=str(admin_password or ""),
    ):
        return

    raise HTTPException(
        status_code=401,
        detail="unauthorized",
        headers={"WWW-Authenticate": 'Basic realm="OpenInfoMate"'},
    )


class TopicCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    query: str = ""
    digest_cron: str = "0 9 * * *"
    alert_keywords: str = ""


class TopicUpdate(BaseModel):
    query: str | None = None
    digest_cron: str | None = None
    alert_keywords: str | None = None
    alert_cooldown_minutes: int | None = None
    alert_daily_cap: int | None = None
    enabled: bool | None = None


class TopicPolicyUpdate(BaseModel):
    llm_curation_enabled: bool | None = None
    llm_curation_prompt: str | None = None

class TopicProposeRequest(BaseModel):
    name: str = ""
    brief: str = Field(min_length=1, max_length=4000)


class TopicProposeSourceHints(BaseModel):
    add_hn: bool = True
    add_searxng: bool = True
    add_discourse: bool = False
    discourse_base_url: str = ""
    discourse_json_path: str = "/latest.json"
    add_nodeseek: bool = False


class TopicProposeResponse(BaseModel):
    topic_name: str
    query: str
    alert_keywords: str = ""
    ai_prompt: str
    source_hints: TopicProposeSourceHints | None = None


class ProfileProposeRequest(BaseModel):
    text: str = Field(min_length=1, max_length=200_000)


class ProfileProposeResponse(BaseModel):
    understanding: str = ""
    interest_axes: list[str] = []
    interest_keywords: list[str] = []
    retrieval_queries: list[str] = []
    ai_prompt: str


class ProfileDeltaProposeRequest(BaseModel):
    text: str = Field(min_length=1, max_length=20_000)


class ProfileDeltaProposeResponse(BaseModel):
    delta_prompt: str
    note: str = ""
    current_delta_prompt: str = ""


class ProfileDeltaApplyRequest(BaseModel):
    delta_prompt: str = Field(min_length=1, max_length=2000)
    note: str = Field(default="", max_length=800)


class ProfileDeltaApplyResponse(BaseModel):
    ok: bool
    rev_id: int


class SourceCreateRss(BaseModel):
    url: str = Field(min_length=1)
    topic: str | None = None
    include_keywords: str = ""
    exclude_keywords: str = ""


class SourceCreateHnSearch(BaseModel):
    query: str = Field(min_length=1)
    tags: str = "story"
    hits_per_page: int = 50
    topic: str | None = None
    include_keywords: str = ""
    exclude_keywords: str = ""


class SourceCreateSearxngSearch(BaseModel):
    base_url: str = Field(min_length=1)
    query: str = Field(min_length=1)
    categories: str | None = None
    time_range: str | None = "day"
    language: str | None = None
    results: int | None = 20
    topic: str | None = None
    include_keywords: str = ""
    exclude_keywords: str = ""


class CandidateBulkActionRequest(BaseModel):
    candidate_ids: list[int] = Field(default_factory=list)
    enabled: bool = True


class AiSetupCandidateNotifySettingsUpdate(BaseModel):
    telegram_enabled: bool | None = None
    batch_size: int | None = Field(default=None, ge=1, le=500)


class AiSetupDiscoverControlsUpdate(BaseModel):
    discovery_enabled: bool | None = None
    explore_weight: int | None = Field(default=None, ge=0, le=10)
    auto_accept_enabled: bool | None = None
    min_source_score: int | None = Field(default=None, ge=0, le=100)
    max_sources_total: int | None = Field(default=None, ge=50, le=5000)


class SourceCreateDiscourse(BaseModel):
    base_url: str = Field(min_length=1)
    json_path: str = "/latest.json"
    topic: str | None = None
    include_keywords: str = ""
    exclude_keywords: str = ""


class SourceCreateHtmlList(BaseModel):
    page_url: str = Field(min_length=1)
    item_selector: str = Field(min_length=1)
    title_selector: str | None = "a"
    summary_selector: str | None = None
    max_items: int = Field(30, ge=1, le=200)
    topic: str | None = None
    include_keywords: str = ""
    exclude_keywords: str = ""


class SourceMetaUpdate(BaseModel):
    tags: str | None = None
    notes: str | None = None


class BindingCreate(BaseModel):
    topic: str = Field(min_length=1)
    source_id: int = Field(ge=1)
    include_keywords: str = ""
    exclude_keywords: str = ""


class BindingUpdate(BaseModel):
    include_keywords: str | None = None
    exclude_keywords: str | None = None


class PushRetry(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=256)
    only: str | None = None  # dingtalk|telegram|email|webhook


class PushTest(BaseModel):
    only: str | None = None  # dingtalk|telegram|email|webhook


class TelegramLinkCreate(BaseModel):
    """
    Create a one-time Telegram /start link code.

    The bot token itself is read from TRACKER_TELEGRAM_BOT_TOKEN (env).
    """

    bot_username: str | None = None  # optional override; otherwise uses settings/known default


class TelegramPoll(BaseModel):
    code: str | None = None  # optional; default to stored code


def create_app(settings: Settings) -> FastAPI:
    configure_logging(level=settings.log_level)

    if not _looks_like_loopback_host(settings.api_host) and not (settings.api_token or settings.admin_password):
        if getattr(settings, "bootstrap_allow_no_auth", False):
            logger.warning(
                "Starting without auth while binding TRACKER_API_HOST=%s. "
                "This is intended only for first-run setup behind localhost-only exposure. "
                "Set TRACKER_ADMIN_PASSWORD or TRACKER_API_TOKEN before exposing publicly.",
                settings.api_host,
            )
        else:
            raise RuntimeError(
                "Refusing to bind TRACKER_API_HOST to a non-localhost address without auth. "
                "Set TRACKER_API_TOKEN or TRACKER_ADMIN_PASSWORD, or enable TRACKER_BOOTSTRAP_ALLOW_NO_AUTH "
                "for first-run setup behind localhost-only exposure."
            )

    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    template_dir = Path(__file__).with_name("templates")
    templates = Jinja2Templates(directory=str(template_dir))

    @pass_context
    def _t(ctx, text: str) -> str:
        lang = str(ctx.get("lang") or "en")
        return translate_text(lang, text)

    templates.env.globals["t"] = _t
    templates.env.globals["supported_langs"] = SUPPORTED_LANGS

    app = FastAPI(title="tracker", version="0.1.0")
    # Expose build/version info in templates so operators can quickly verify
    # whether a remote admin UI is running the expected release.
    templates.env.globals["app_version"] = str(app.version or "").strip() or "unknown"
    templates.env.globals["systemd_unit_scheduler"] = str(getattr(settings, "systemd_user_unit_scheduler", "") or "")
    templates.env.globals["systemd_unit_api"] = str(getattr(settings, "systemd_user_unit_api", "") or "")

    def get_db() -> Session:
        with make_session() as session:
            yield session

    def auth_dep(request: Request) -> None:
        return _require_auth(request, settings)

    def _parse_linux_default_gateway_ip(route_text: str) -> str:
        """
        Parse `/proc/net/route` and return the default gateway IP, or "".
        """
        raw = (route_text or "").splitlines()
        if not raw:
            return ""
        # Columns: Iface Destination Gateway Flags ...
        # Destination 00000000 indicates default route.
        for ln in raw[1:]:
            parts = ln.split()
            if len(parts) < 4:
                continue
            dest_hex = parts[1]
            gw_hex = parts[2]
            flags_hex = parts[3]
            if dest_hex != "00000000":
                continue
            try:
                flags = int(flags_hex, 16)
            except Exception:
                continue
            if (flags & 0x1) != 0x1:
                continue
            try:
                gw = int(gw_hex, 16)
            except Exception:
                continue
            if gw <= 0:
                continue
            # Gateway is little-endian.
            b0 = (gw) & 0xFF
            b1 = (gw >> 8) & 0xFF
            b2 = (gw >> 16) & 0xFF
            b3 = (gw >> 24) & 0xFF
            return f"{b0}.{b1}.{b2}.{b3}"
        return ""

    _trusted_local_client_hosts: set[str] = {"127.0.0.1", "::1", "localhost", "testclient"}

    def _refresh_trusted_hosts_once() -> None:
        """
        Best-effort support for Docker bridge mode.

        When the API runs inside a container with bridge networking, requests originating
        from the host's loopback may appear as the container's default gateway
        (e.g. 172.17.0.1). Treat that as local for admin writes/tests.
        """
        try:
            p = Path("/proc/net/route")
            if not p.exists():
                return
            gw = _parse_linux_default_gateway_ip(p.read_text(encoding="utf-8", errors="ignore"))
            if gw:
                _trusted_local_client_hosts.add(gw)
        except Exception:
            return

    _refresh_trusted_hosts_once()

    def _is_trusted_local_request(request: Request) -> bool:
        host = (request.client.host if request.client else "").strip()
        if host in _trusted_local_client_hosts:
            return True

        # Support SSH local port-forward (and other localhost-only host exposure).
        #
        # In some deployments, requests to a host-loopback bound port may still appear to the
        # container as coming from a non-loopback IP (or another internal hop). If the
        # operator explicitly configured the host exposure to be loopback-only via
        # OPENINFOMATE_API_BIND_HOST, trust requests whose Host header is loopback.
        bind_host = str(os.environ.get("OPENINFOMATE_API_BIND_HOST") or "").strip()
        if not _looks_like_loopback_host(bind_host):
            return False

        host_hdr = str(request.headers.get("host") or "").strip()
        if not host_hdr:
            return False
        # Strip port; tolerate IPv6 bracket form.
        if host_hdr.startswith("[") and "]" in host_hdr:
            host_only = host_hdr[1 : host_hdr.index("]")]
        else:
            host_only = host_hdr.split(":", 1)[0].strip()
        return _looks_like_loopback_host(host_only)

    def _require_localhost(request: Request) -> None:
        if _is_trusted_local_request(request):
            return
        if settings.admin_allow_remote_env_update:
            return
        raise HTTPException(status_code=403, detail="forbidden")

    def _audit_actor(request: Request) -> str:
        """
        Best-effort audit actor label (never includes secrets).
        """
        if settings.api_token:
            token = request.headers.get("x-tracker-token") or request.query_params.get("token")
            if token and token == settings.api_token:
                return "token"
        raw = request.headers.get("authorization") or ""
        if raw.lower().startswith("basic ") and settings.admin_password:
            try:
                decoded = base64.b64decode(raw.split(" ", 1)[1].strip()).decode("utf-8")
            except Exception:
                decoded = ""
            if ":" in decoded:
                u = decoded.split(":", 1)[0].strip()
                if u:
                    return f"basic:{u[:64]}"
            return "basic"
        return ""

    def _field_from_env_key(env_key: str) -> str:
        k = (env_key or "").strip()
        if not k.startswith("TRACKER_"):
            return ""
        raw = k[len("TRACKER_") :].strip()
        if not raw:
            return ""
        f = raw.lower()
        return f if f in Settings.model_fields else ""

    def _seed_locale_defaults(*, repo: Repo, request_lang: str) -> None:
        """
        UX: keep output_language aligned with UI language by default.

        Background jobs don't have request context (cookie / Accept-Language), so we persist
        the first-seen UI language into app_config unless the operator already set it.
        """
        try:
            updates: dict[str, str] = {}
            if repo.get_app_config_entry("output_language") is None:
                updates["output_language"] = normalize_lang(request_lang)
            # Prefer simple UTC offset input in OSS ("+8" / "-8") instead of IANA names.
            if repo.get_app_config_entry("cron_timezone") is None:
                updates["cron_timezone"] = "+8"
            if updates:
                repo.set_app_config_many(updates)
        except Exception:
            # Never block the UI on config persistence.
            pass

    def _load_custom_prompt_presets(repo: Repo, *, app_config_key: str) -> list[dict[str, str]]:
        """
        Load operator-defined prompt presets from app_config (JSON list).

        Contract:
        - No secrets.
        - Each preset: {id,label,description,prompt}.
        """
        raw = (repo.get_app_config(app_config_key) or "").strip()
        if not raw:
            return []
        try:
            obj = json.loads(raw)
        except Exception:
            return []
        if not isinstance(obj, list):
            return []
        out: list[dict[str, str]] = []
        for it in obj[:200]:
            if not isinstance(it, dict):
                continue
            pid = str(it.get("id") or "").strip()
            label = str(it.get("label") or "").strip()
            desc = str(it.get("description") or "").strip()
            prompt = str(it.get("prompt") or "").strip()
            if not pid or not label or not prompt:
                continue
            if len(pid) > 64 or len(label) > 120 or len(desc) > 400 or len(prompt) > 20_000:
                continue
            out.append({"id": pid, "label": label, "description": desc, "prompt": prompt})
        # Deduplicate by id, keeping the last occurrence (latest wins).
        merged: dict[str, dict[str, str]] = {}
        for p in out:
            merged[p["id"]] = p
        return list(merged.values())

    def _merge_prompt_presets(
        static_presets: list[dict[str, str]],
        custom_presets: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        merged: dict[str, dict[str, str]] = {}
        for p in static_presets or []:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("id") or "").strip()
            if not pid:
                continue
            merged[pid] = {
                "id": pid,
                "label": str(p.get("label") or "").strip() or pid,
                "description": str(p.get("description") or "").strip(),
                "prompt": str(p.get("prompt") or "").strip(),
            }
        for p in custom_presets or []:
            pid = str(p.get("id") or "").strip()
            if not pid:
                continue
            merged[pid] = p
        return list(merged.values())

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/", include_in_schema=False)
    def root(request: Request):
        """
        Convenience: redirect the bare host URL to the Admin UI.

        Many operators will open http://host:port/ by habit; the admin UI lives at `/admin`.
        """
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        url = "/admin" + (f"?token={token}" if token else "")
        return RedirectResponse(url=url, status_code=303)

    @app.get("/lang", include_in_schema=False)
    def set_lang(
        request: Request,
        lang: str = "en",
        next: str = "/admin",
        apply_output: bool = False,
        session: Session = Depends(get_db),
    ):
        """
        Set UI language cookie and redirect back.

        Note: this endpoint is intentionally unauthenticated (it does not reveal data).
        """
        target = (next or "/admin").strip() or "/admin"
        if not target.startswith("/") or target.startswith("//"):
            target = "/admin"
        resp = RedirectResponse(url=target, status_code=303)
        resp.set_cookie(
            key=LANG_COOKIE_NAME,
            value=normalize_lang(lang),
            max_age=365 * 24 * 3600,
            samesite="lax",
        )
        # Best-effort: when the operator is authenticated, also persist the chosen language
        # for server-side outputs (LLM writeups + push/report formatting) so background jobs
        # follow the same language as the UI.
        if apply_output:
            try:
                _require_auth(request, settings)
            except Exception:
                return resp
            try:
                repo = Repo(session)
                from tracker.dynamic_config import apply_env_block_updates

                apply_env_block_updates(
                    repo=repo,
                    settings=settings,
                    env_path=Path(settings.env_path or ".env"),
                    env_updates={"TRACKER_OUTPUT_LANGUAGE": normalize_lang(lang)},
                )
            except Exception:
                # Never break the UI language toggle on DB issues.
                pass
        return resp

    @app.get("/stats", dependencies=[Depends(auth_dep)])
    def stats(session: Session = Depends(get_db)):
        return Repo(session).get_stats()

    @app.get("/doctor", dependencies=[Depends(auth_dep)])
    def doctor(session: Session = Depends(get_db)):
        from tracker.doctor import build_doctor_report

        db_ok = True
        db_error: str | None = None
        stats: dict[str, int] = {}
        profile_configured = False
        telegram_chat_configured = False
        eff_settings = settings
        try:
            repo = Repo(session)
            stats = repo.get_stats()
            profile_configured = bool(repo.get_app_config("profile_text"))
            telegram_chat_configured = bool(repo.get_app_config("telegram_chat_id"))
            activity = repo.get_activity_snapshot()
            try:
                from tracker.dynamic_config import effective_settings

                eff_settings = effective_settings(repo=repo, settings=settings)
            except Exception:
                eff_settings = settings
        except OperationalError as exc:
            db_ok = False
            db_error = str(getattr(exc, "orig", exc))
            stats = {}
            activity = None

        report = build_doctor_report(
            settings=eff_settings,
            stats=stats,
            db_ok=db_ok,
            db_error=db_error,
            profile_configured=profile_configured,
            telegram_chat_configured=telegram_chat_configured,
            activity=activity,
        )
        return {
            "db_ok": report.db_ok,
            "db_error": report.db_error,
            "cron_timezone": report.cron_timezone,
            "cron_timezone_ok": report.cron_timezone_ok,
            "cron_now_iso": report.cron_now_iso,
            "activity": {
                "last_tick_at": report.last_tick_at,
                "last_digest_report_at": report.last_digest_report_at,
                "last_health_report_at": report.last_health_report_at,
                "last_push_attempt_at": report.last_push_attempt_at,
                "last_push_sent_at": report.last_push_sent_at,
            },
            "profile": {"configured": report.profile_configured},
            "push": {
                "dingtalk_configured": report.push_dingtalk_configured,
                "telegram_configured": report.push_telegram_configured,
                "email_configured": report.push_email_configured,
                "webhook_configured": report.push_webhook_configured,
                "missing_env": report.push_missing_env,
            },
            "stats": report.stats,
            "recommendations": report.recommendations,
        }

    # --- JSON API
    @app.get("/topics", dependencies=[Depends(auth_dep)])
    def list_topics(session: Session = Depends(get_db)):
        topics = Repo(session).list_topics()
        return [
            {
                "id": t.id,
                "name": t.name,
                "query": t.query,
                "enabled": t.enabled,
                "digest_cron": t.digest_cron,
                "alert_keywords": t.alert_keywords,
                "alert_cooldown_minutes": t.alert_cooldown_minutes,
                "alert_daily_cap": t.alert_daily_cap,
            }
            for t in topics
        ]

    @app.post("/topics", dependencies=[Depends(auth_dep)])
    def add_topic(payload: TopicCreate, session: Session = Depends(get_db)):
        topic = create_topic_action(
            session=session,
            spec=TopicSpec(
                name=payload.name,
                query=payload.query,
                digest_cron=payload.digest_cron,
                alert_keywords=payload.alert_keywords,
            ),
        )
        return {"id": topic.id, "name": topic.name}

    @app.patch("/topics/{name}", dependencies=[Depends(auth_dep)])
    def update_topic(name: str, payload: TopicUpdate, session: Session = Depends(get_db)):
        repo = Repo(session)
        topic = repo.get_topic_by_name(name)
        if not topic:
            raise HTTPException(status_code=404, detail="topic not found")
        if payload.query is not None:
            topic.query = payload.query
        if payload.digest_cron is not None:
            topic.digest_cron = payload.digest_cron
        if payload.alert_keywords is not None:
            topic.alert_keywords = payload.alert_keywords
        if payload.alert_cooldown_minutes is not None:
            topic.alert_cooldown_minutes = payload.alert_cooldown_minutes
        if payload.alert_daily_cap is not None:
            topic.alert_daily_cap = payload.alert_daily_cap
        if payload.enabled is not None:
            topic.enabled = payload.enabled
        session.commit()
        return {"ok": True}

    @app.get("/topics/{name}/policy", dependencies=[Depends(auth_dep)])
    def get_topic_policy(name: str, session: Session = Depends(get_db)):
        repo = Repo(session)
        topic = repo.get_topic_by_name(name)
        if not topic:
            raise HTTPException(status_code=404, detail="topic not found")
        pol = repo.get_topic_policy(topic_id=topic.id)
        return {
            "topic": topic.name,
            "topic_id": topic.id,
            "llm_curation_enabled": bool(pol.llm_curation_enabled) if pol else False,
            "llm_curation_prompt": (pol.llm_curation_prompt if pol else ""),
            "updated_at": (pol.updated_at.isoformat() if pol else None),
        }

    @app.put("/topics/{name}/policy", dependencies=[Depends(auth_dep)])
    def set_topic_policy(name: str, payload: TopicPolicyUpdate, session: Session = Depends(get_db)):
        repo = Repo(session)
        topic = repo.get_topic_by_name(name)
        if not topic:
            raise HTTPException(status_code=404, detail="topic not found")
        repo.upsert_topic_policy(
            topic_id=topic.id,
            llm_curation_enabled=payload.llm_curation_enabled,
            llm_curation_prompt=payload.llm_curation_prompt,
        )
        return {"ok": True}

    @app.post("/topics/propose", response_model=TopicProposeResponse, dependencies=[Depends(auth_dep)])
    async def propose_topic(payload: TopicProposeRequest, session: Session = Depends(get_db)):
        repo = Repo(session)
        try:
            from tracker.dynamic_config import effective_settings

            settings_eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            settings_eff = settings
        model_primary = str(
            getattr(settings_eff, "llm_model_reasoning", "") or getattr(settings_eff, "llm_model", "") or ""
        ).strip()
        if not (settings_eff.llm_base_url and model_primary):
            raise HTTPException(status_code=400, detail="LLM is not configured")
        name_hint = (payload.name or "").strip()
        brief = (payload.brief or "").strip()
        if not brief:
            raise HTTPException(status_code=400, detail="missing brief")

        usage_cb = make_llm_usage_recorder(session=session)
        try:
            proposal = await llm_propose_topic_setup(
                settings=settings_eff,
                topic_name=(name_hint or "New Topic"),
                brief=brief,
                usage_cb=usage_cb,
            )
        except httpx.HTTPStatusError as exc:
            body = ""
            try:
                body = (exc.response.text or "").strip()
            except Exception:
                body = ""
            if len(body) > 500:
                body = body[:500] + "…"
            raise HTTPException(status_code=502, detail=f"LLM HTTP {exc.response.status_code}: {body or exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"LLM error: {exc}") from exc
        if proposal is None:
            raise HTTPException(status_code=400, detail="LLM is not configured")

        hints = None
        if getattr(proposal, "source_hints", None):
            h = proposal.source_hints
            hints = TopicProposeSourceHints(
                add_hn=bool(getattr(h, "add_hn", True)),
                add_searxng=bool(getattr(h, "add_searxng", True)),
                add_discourse=bool(getattr(h, "add_discourse", False)),
                discourse_base_url=str(getattr(h, "discourse_base_url", "") or ""),
                discourse_json_path=str(getattr(h, "discourse_json_path", "/latest.json") or "/latest.json"),
                add_nodeseek=bool(getattr(h, "add_nodeseek", False)),
            )

        return TopicProposeResponse(
            topic_name=proposal.topic_name,
            query=proposal.query_keywords,
            alert_keywords=proposal.alert_keywords,
            ai_prompt=proposal.ai_prompt,
            source_hints=hints,
        )

    @app.post("/profile/propose", response_model=ProfileProposeResponse, dependencies=[Depends(auth_dep)])
    async def propose_profile(payload: ProfileProposeRequest, session: Session = Depends(get_db)):
        repo = Repo(session)
        try:
            from tracker.dynamic_config import effective_settings

            settings_eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            settings_eff = settings
        model_primary = str(
            getattr(settings_eff, "llm_model_reasoning", "") or getattr(settings_eff, "llm_model", "") or ""
        ).strip()
        if not (settings_eff.llm_base_url and model_primary):
            raise HTTPException(status_code=400, detail="LLM is not configured")
        raw = (payload.text or "").strip()
        if not raw:
            raise HTTPException(status_code=400, detail="missing text")

        from tracker.profile_input import normalize_profile_text

        text = normalize_profile_text(text=raw)

        usage_cb = make_llm_usage_recorder(session=session)
        try:
            proposal = await llm_propose_profile_setup(settings=settings_eff, profile_text=text, usage_cb=usage_cb)
        except httpx.HTTPStatusError as exc:
            body = ""
            try:
                body = (exc.response.text or "").strip()
            except Exception:
                body = ""
            if len(body) > 500:
                body = body[:500] + "…"
            raise HTTPException(status_code=502, detail=f"LLM HTTP {exc.response.status_code}: {body or exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"LLM error: {exc}") from exc
        if proposal is None:
            raise HTTPException(status_code=400, detail="LLM is not configured")

        return ProfileProposeResponse(
            understanding=str(getattr(proposal, "understanding", "") or ""),
            interest_axes=list(getattr(proposal, "interest_axes", []) or []),
            interest_keywords=list(getattr(proposal, "interest_keywords", []) or []),
            retrieval_queries=list(getattr(proposal, "retrieval_queries", []) or []),
            ai_prompt=str(getattr(proposal, "ai_prompt", "") or ""),
        )

    @app.post("/profile/delta/propose", response_model=ProfileDeltaProposeResponse, dependencies=[Depends(auth_dep)])
    async def propose_profile_delta(payload: ProfileDeltaProposeRequest, session: Session = Depends(get_db)):
        repo = Repo(session)
        raw = (payload.text or "").strip()
        if not raw:
            raise HTTPException(status_code=400, detail="missing text")

        profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
        topic = repo.get_topic_by_name(profile_topic_name)
        pol = repo.get_topic_policy(topic_id=int(topic.id)) if topic else None
        if not (topic and pol and (pol.llm_curation_prompt or "").strip()):
            raise HTTPException(status_code=400, detail="profile policy not configured")

        core = (repo.get_app_config("profile_prompt_core") or "").strip()
        if not core:
            core = (pol.llm_curation_prompt or "").strip()
            if core:
                repo.set_app_config("profile_prompt_core", core)
        cur_delta = (repo.get_app_config("profile_prompt_delta") or "").strip()

        from tracker.llm import llm_update_profile_delta_from_feedback

        # Keep raw language labels (e.g. "中文") so LLM-side normalization can do the right thing.
        out_lang = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
        try:
            s2 = settings.model_copy(update={"output_language": out_lang})  # type: ignore[attr-defined]
        except Exception:
            s2 = settings
        try:
            from tracker.dynamic_config import effective_settings

            s2 = effective_settings(repo=repo, settings=s2)
        except Exception:
            pass

        now_iso = dt.datetime.utcnow().isoformat() + "Z"
        events = [
            {
                "id": 0,
                "kind": "profile_note",
                "value_int": 0,
                "domain": "",
                "url": "",
                "note": "",
                "text": raw,
                "created_at": now_iso,
            }
        ]

        usage_cb = make_llm_usage_recorder(session=session)
        update = await llm_update_profile_delta_from_feedback(
            settings=s2,
            core_prompt=core,
            delta_prompt=cur_delta,
            feedback_events=events,
            usage_cb=usage_cb,
        )
        if not update or not str(getattr(update, "delta_prompt", "") or "").strip():
            raise HTTPException(status_code=400, detail="empty delta")

        note = str(getattr(update, "note", "") or "").strip()
        new_delta_raw = str(update.delta_prompt or "").strip()

        # Guardrail: profile delta updates should be incremental by default.
        #
        # We always preserve the existing delta lines and append new unique lines
        # (line-wise union) to avoid accidental preference loss. Operators can still
        # manually remove lines before clicking "Apply Delta".
        new_delta = new_delta_raw
        if cur_delta and new_delta_raw:
            old_lines = [ln.strip() for ln in str(cur_delta).splitlines() if ln.strip()]
            new_lines = [ln.strip() for ln in str(new_delta_raw).splitlines() if ln.strip()]
            if old_lines:
                merged: list[str] = []
                seen: set[str] = set()
                for ln in old_lines + new_lines:
                    if ln in seen:
                        continue
                    seen.add(ln)
                    merged.append(ln)
                new_delta = "\n".join(merged).strip()
                suffix = (
                    "baseline delta preserved (union, server-side)"
                    if (out_lang or "").lower().startswith("en")
                    else "已保留旧 delta（服务端合并）"
                )
                note = (note + " · " if note else "") + suffix

        if len(new_delta) > 2000:
            new_delta = new_delta[:2000] + "…"
        return ProfileDeltaProposeResponse(delta_prompt=new_delta, note=note, current_delta_prompt=cur_delta)

    @app.post("/profile/delta/apply", response_model=ProfileDeltaApplyResponse, dependencies=[Depends(auth_dep)])
    def apply_profile_delta(payload: ProfileDeltaApplyRequest, session: Session = Depends(get_db)):
        repo = Repo(session)
        new_delta = (payload.delta_prompt or "").strip()
        if not new_delta:
            raise HTTPException(status_code=400, detail="missing delta_prompt")

        profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
        topic = repo.get_topic_by_name(profile_topic_name)
        pol = repo.get_topic_policy(topic_id=int(topic.id)) if topic else None
        if not (topic and pol and (pol.llm_curation_prompt or "").strip()):
            raise HTTPException(status_code=400, detail="profile policy not configured")

        core = (repo.get_app_config("profile_prompt_core") or "").strip()
        if not core:
            core = (pol.llm_curation_prompt or "").strip()
            if core:
                repo.set_app_config("profile_prompt_core", core)

        effective = (core + "\n\n" + new_delta).strip()
        note = (payload.note or "").strip()
        now_iso = dt.datetime.utcnow().isoformat() + "Z"

        repo.set_app_config("profile_prompt_delta", new_delta)
        repo.set_app_config("profile_feedback_last_update_at_utc", now_iso)
        repo.upsert_topic_policy(topic_id=int(topic.id), llm_curation_prompt=effective)
        rev = repo.add_profile_revision(
            kind="delta",
            core_prompt=core,
            delta_prompt=new_delta,
            effective_prompt=effective,
            note=note,
            applied_feedback_ids=[],
        )
        return ProfileDeltaApplyResponse(ok=True, rev_id=int(rev.id))

    @app.get("/sources", dependencies=[Depends(auth_dep)])
    def list_sources(session: Session = Depends(get_db)):
        sources = Repo(session).list_sources()
        return [{"id": s.id, "type": s.type, "url": s.url, "enabled": s.enabled} for s in sources]

    @app.get("/sources/{source_id}/meta", dependencies=[Depends(auth_dep)])
    def get_source_meta(source_id: int, session: Session = Depends(get_db)):
        repo = Repo(session)
        meta = repo.get_source_meta(source_id=source_id)
        if not meta:
            return {"source_id": source_id, "tags": "", "notes": ""}
        return {"source_id": source_id, "tags": meta.tags, "notes": meta.notes}

    @app.put("/sources/{source_id}/meta", dependencies=[Depends(auth_dep)])
    def update_source_meta(source_id: int, payload: SourceMetaUpdate, session: Session = Depends(get_db)):
        try:
            update_source_meta_action(session=session, source_id=source_id, tags=payload.tags, notes=payload.notes)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        repo = Repo(session)
        meta = repo.get_source_meta(source_id=source_id)
        if not meta:
            return {"source_id": source_id, "tags": "", "notes": ""}
        return {"source_id": source_id, "tags": meta.tags, "notes": meta.notes}

    @app.post("/sources/rss", dependencies=[Depends(auth_dep)])
    def add_rss_source(payload: SourceCreateRss, session: Session = Depends(get_db)):
        try:
            source = create_rss_source_action(
                session=session,
                url=payload.url,
                bind=(
                    SourceBindingSpec(
                        topic=payload.topic,
                        include_keywords=payload.include_keywords,
                        exclude_keywords=payload.exclude_keywords,
                    )
                    if payload.topic
                    else None
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"id": source.id}

    @app.post("/sources/hn_search", dependencies=[Depends(auth_dep)])
    def add_hn_search_source(payload: SourceCreateHnSearch, session: Session = Depends(get_db)):
        try:
            source = create_hn_search_source_action(
                session=session,
                query=payload.query,
                tags=payload.tags,
                hits_per_page=payload.hits_per_page,
                bind=(
                    SourceBindingSpec(
                        topic=payload.topic,
                        include_keywords=payload.include_keywords,
                        exclude_keywords=payload.exclude_keywords,
                    )
                    if payload.topic
                    else None
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"id": source.id}

    @app.post("/sources/searxng_search", dependencies=[Depends(auth_dep)])
    def add_searxng_search_source(payload: SourceCreateSearxngSearch, session: Session = Depends(get_db)):
        try:
            source = create_searxng_search_source_action(
                session=session,
                base_url=payload.base_url,
                query=payload.query,
                categories=payload.categories,
                time_range=payload.time_range,
                language=payload.language,
                results=payload.results,
                bind=(
                    SourceBindingSpec(
                        topic=payload.topic,
                        include_keywords=payload.include_keywords,
                        exclude_keywords=payload.exclude_keywords,
                    )
                    if payload.topic
                    else None
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"id": source.id}

    @app.post("/sources/discourse", dependencies=[Depends(auth_dep)])
    def add_discourse_source(payload: SourceCreateDiscourse, session: Session = Depends(get_db)):
        try:
            source = create_discourse_source_action(
                session=session,
                base_url=payload.base_url,
                json_path=payload.json_path,
                bind=(
                    SourceBindingSpec(
                        topic=payload.topic,
                        include_keywords=payload.include_keywords,
                        exclude_keywords=payload.exclude_keywords,
                    )
                    if payload.topic
                    else None
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"id": source.id}

    @app.post("/sources/html_list", dependencies=[Depends(auth_dep)])
    def add_html_list_source(payload: SourceCreateHtmlList, session: Session = Depends(get_db)):
        try:
            source = create_html_list_source_action(
                session=session,
                page_url=payload.page_url,
                item_selector=payload.item_selector,
                title_selector=payload.title_selector,
                summary_selector=payload.summary_selector,
                max_items=payload.max_items,
                bind=(
                    SourceBindingSpec(
                        topic=payload.topic,
                        include_keywords=payload.include_keywords,
                        exclude_keywords=payload.exclude_keywords,
                    )
                    if payload.topic
                    else None
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"id": source.id}

    @app.get("/bindings", dependencies=[Depends(auth_dep)])
    def list_bindings(session: Session = Depends(get_db)):
        rows = Repo(session).list_topic_sources()
        return [
            {
                "topic": t.name,
                "source_id": s.id,
                "source_type": s.type,
                "source_url": s.url,
                "include_keywords": ts.include_keywords,
                "exclude_keywords": ts.exclude_keywords,
            }
            for t, s, ts in rows
        ]

    @app.post("/bindings", dependencies=[Depends(auth_dep)])
    def add_binding(payload: BindingCreate, session: Session = Depends(get_db)):
        try:
            create_binding_action(
                session=session,
                topic_name=payload.topic,
                source_id=payload.source_id,
                include_keywords=payload.include_keywords,
                exclude_keywords=payload.exclude_keywords,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True}

    @app.patch("/bindings/{topic}/{source_id}", dependencies=[Depends(auth_dep)])
    def update_binding(topic: str, source_id: int, payload: BindingUpdate, session: Session = Depends(get_db)):
        try:
            update_binding_action(
                session=session,
                topic_name=topic,
                source_id=source_id,
                include_keywords=payload.include_keywords,
                exclude_keywords=payload.exclude_keywords,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True}

    @app.delete("/bindings/{topic}/{source_id}", dependencies=[Depends(auth_dep)])
    def remove_binding(topic: str, source_id: int, session: Session = Depends(get_db)):
        try:
            remove_binding_action(session=session, topic_name=topic, source_id=source_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True}

    @app.post("/run/tick", dependencies=[Depends(auth_dep)])
    async def run_tick_endpoint(push: bool = False, session: Session = Depends(get_db)) -> dict:
        result: TickResult = await run_tick(session=session, settings=settings, push=push)
        return {
            "total_created": result.total_created,
            "total_pushed_alerts": result.total_pushed_alerts,
            "per_source": [r.__dict__ for r in result.per_source],
        }

    @app.post("/run/digest", dependencies=[Depends(auth_dep)])
    async def run_digest_endpoint(
        hours: int = 24,
        push: bool = False,
        force: bool = False,
        session: Session = Depends(get_db),
    ) -> dict:
        suffix = None
        if push and force:
            suffix = "manual-" + dt.datetime.utcnow().strftime("%H%M%S")
        result = await run_curated_info(session=session, settings=settings, hours=hours, push=push, key_suffix=suffix)
        return {
            "since": result.since.isoformat(),
            "pushed": getattr(result, "pushed", 0),
            "idempotency_key": getattr(result, "idempotency_key", ""),
            "markdown": result.markdown,
        }

    @app.post("/run/health", dependencies=[Depends(auth_dep)])
    async def run_health_endpoint(push: bool = False, session: Session = Depends(get_db)) -> dict:
        result: HealthResult = await run_health_report(session=session, settings=settings, push=push)
        return {"pushed": result.pushed, "markdown": result.markdown}

    @app.get("/events", dependencies=[Depends(auth_dep)])
    def list_events(
        topic: str | None = None,
        decision: str | None = None,
        hours: int = 24,
        limit: int = 100,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)

        t = None
        if topic:
            t = repo.get_topic_by_name(topic)
            if not t:
                raise HTTPException(status_code=404, detail="topic not found")

        decisions = None
        if decision:
            valid = {"ignore", "digest", "alert"}
            if decision not in valid:
                raise HTTPException(status_code=400, detail=f"invalid decision (expected one of: {', '.join(sorted(valid))})")
            decisions = [decision]

        since = None
        if hours and hours > 0:
            since = dt.datetime.utcnow() - dt.timedelta(hours=int(hours))

        limit = max(1, min(500, int(limit)))
        rows = repo.list_recent_events(topic=t, decisions=decisions, since=since, limit=limit)

        def _iso(value: dt.datetime | None) -> str | None:
            if value is None:
                return None
            return value.isoformat()

        return [
            {
                "id": it.id,
                "created_at": _iso(it.created_at),
                "topic_id": topic_row.id,
                "topic": topic_row.name,
                "decision": it.decision,
                "relevance_score": it.relevance_score,
                "novelty_score": it.novelty_score,
                "quality_score": it.quality_score,
                "reason": (it.reason or "")[:2000],
                "item_id": item.id,
                "item_title": item.title,
                "item_url": item.canonical_url,
                "item_published_at": _iso(item.published_at),
                "item_created_at": _iso(item.created_at),
                "source_id": source.id,
                "source_type": source.type,
                "source_url": source.url,
            }
            for it, item, topic_row, source in rows
        ]

    @app.get("/candidates/{candidate_id}/preview", dependencies=[Depends(auth_dep)])
    async def candidate_preview(
        candidate_id: int,
        limit: int = 10,
        session: Session = Depends(get_db),
    ):
        """
        Fetch a source candidate once and return extracted entries (no DB writes).
        """
        from tracker.connectors.rss import RssConnector
        from tracker.http_auth import AuthRequiredError, cookie_header_for_url, parse_cookie_jar_json

        repo = Repo(session)
        cand = repo.get_source_candidate_by_id(candidate_id)
        if not cand:
            raise HTTPException(status_code=404, detail="candidate not found")
        if (cand.source_type or "").strip().lower() != "rss":
            raise HTTPException(status_code=400, detail="unsupported candidate type (expected rss)")

        limit = max(1, min(50, int(limit)))
        try:
            cookie_jar = parse_cookie_jar_json(getattr(settings, "cookie_jar_json", "") or "")
            cookie_header = cookie_header_for_url(url=cand.url, cookie_jar=cookie_jar)
            entries = await RssConnector(timeout_seconds=settings.http_timeout_seconds).fetch_with_cookie(
                url=cand.url, cookie_header=cookie_header
            )
        except AuthRequiredError as exc:
            raise HTTPException(status_code=412, detail=f"auth required for {exc.host}") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"candidate preview fetch failed: {exc}") from exc

        payload = [
            {
                "url": e.url,
                "title": e.title,
                "published_at_iso": e.published_at_iso,
                "summary": e.summary,
            }
            for e in entries[:limit]
        ]
        return {
            "ok": True,
            "candidate": {
                "id": cand.id,
                "topic_id": cand.topic_id,
                "type": cand.source_type,
                "url": cand.url,
                "title": cand.title,
                "discovered_from_url": cand.discovered_from_url,
                "status": cand.status,
                "seen_count": cand.seen_count,
                "last_seen_at": cand.last_seen_at.isoformat(),
            },
            "entries": payload,
        }

    @app.get("/reports", dependencies=[Depends(auth_dep)])
    def list_reports(
        kind: str | None = None,
        topic: str | None = None,
        limit: int = 20,
        include_markdown: bool = False,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        t = None
        if topic:
            t = repo.get_topic_by_name(topic)
            if not t:
                raise HTTPException(status_code=404, detail="topic not found")

        limit = max(1, min(200, int(limit)))
        rows = repo.list_reports(kind=kind, topic=t, limit=limit)

        def _iso(value: dt.datetime | None) -> str | None:
            if value is None:
                return None
            return value.isoformat()

        out = []
        for r, topic_row in rows:
            row = {
                "id": r.id,
                "kind": r.kind,
                "idempotency_key": r.idempotency_key,
                "topic_id": r.topic_id,
                "topic": topic_row.name if topic_row else None,
                "title": r.title,
                "created_at": _iso(r.created_at),
                "updated_at": _iso(r.updated_at),
            }
            if include_markdown:
                row["markdown"] = r.markdown
            out.append(row)
        return out

    @app.get("/reports/{report_id}", dependencies=[Depends(auth_dep)])
    def get_report(report_id: int, session: Session = Depends(get_db)):
        repo = Repo(session)
        r = repo.get_report_by_id(report_id)
        if not r:
            raise HTTPException(status_code=404, detail="report not found")

        from tracker.models import Topic

        topic_name = None
        if r.topic_id:
            t = session.get(Topic, r.topic_id)
            topic_name = t.name if t else None

        return {
            "id": r.id,
            "kind": r.kind,
            "idempotency_key": r.idempotency_key,
            "topic_id": r.topic_id,
            "topic": topic_name,
            "title": r.title,
            "markdown": r.markdown,
            "created_at": r.created_at.isoformat(),
            "updated_at": r.updated_at.isoformat(),
        }

    @app.get("/pushes", dependencies=[Depends(auth_dep)])
    def list_pushes(
        channel: str | None = None,
        status: str | None = None,
        key: str | None = None,
        limit: int = 50,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        limit = max(1, min(200, int(limit)))
        rows = repo.list_pushes(channel=channel, status=status, idempotency_key=key, limit=limit)
        return [
            {
                "id": p.id,
                "channel": p.channel,
                "idempotency_key": p.idempotency_key,
                "status": p.status,
                "attempts": p.attempts,
                "error": p.error,
                "created_at": p.created_at.isoformat(),
                "sent_at": p.sent_at.isoformat() if p.sent_at else None,
            }
            for p in rows
        ]

    @app.post("/pushes/test", dependencies=[Depends(auth_dep)])
    async def push_test(payload: PushTest, session: Session = Depends(get_db)) -> dict:
        from tracker.push_ops import push_test as push_test_core

        try:
            results = await push_test_core(session=session, settings=settings, only=payload.only)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {"results": [{"channel": c, "status": s} for c, s in results]}

    @app.post("/pushes/retry", dependencies=[Depends(auth_dep)])
    async def retry_push(payload: PushRetry, session: Session = Depends(get_db)) -> dict:
        from tracker.push_ops import retry_push_key

        try:
            result = await retry_push_key(
                session=session,
                settings=settings,
                idempotency_key=payload.idempotency_key,
                only=payload.only,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "idempotency_key": result.idempotency_key,
            "results": [{"channel": c, "status": s} for c, s in result.results],
        }

    # --- Telegram connect (interactive)
    @app.get("/telegram/status", dependencies=[Depends(auth_dep)])
    def telegram_status(session: Session = Depends(get_db)) -> dict:
        repo = Repo(session)
        from tracker.telegram_connect import telegram_status as telegram_status_core

        return telegram_status_core(repo=repo, settings=settings)

    @app.post("/telegram/link", dependencies=[Depends(auth_dep)])
    def telegram_link(payload: TelegramLinkCreate, session: Session = Depends(get_db)) -> dict:
        repo = Repo(session)
        from tracker.telegram_connect import telegram_link as telegram_link_core

        try:
            return telegram_link_core(repo=repo, settings=settings, bot_username_override=payload.bot_username)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/telegram/poll", dependencies=[Depends(auth_dep)])
    async def telegram_poll(payload: TelegramPoll, session: Session = Depends(get_db)) -> dict:
        repo = Repo(session)
        from tracker.telegram_connect import telegram_poll as telegram_poll_core

        try:
            return await telegram_poll_core(repo=repo, settings=settings, code=payload.code)
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/telegram/disconnect", dependencies=[Depends(auth_dep)])
    def telegram_disconnect(session: Session = Depends(get_db)) -> dict:
        repo = Repo(session)
        from tracker.telegram_connect import telegram_disconnect as telegram_disconnect_core

        return telegram_disconnect_core(repo=repo, settings=settings)

    # --- Admin (HTML)
    @app.get("/admin", response_class=HTMLResponse, dependencies=[Depends(auth_dep)])
    def admin(request: Request, session: Session = Depends(get_db)):
        repo = Repo(session)
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        lang = get_request_lang(request)
        _seed_locale_defaults(repo=repo, request_lang=lang)
        # Apply DB-backed overrides for non-secret settings so the admin UI reflects
        # the runtime effective config (where applicable).
        try:
            from tracker.dynamic_config import effective_settings

            eff_settings = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff_settings = settings
        raw_section = (request.query_params.get("section") or "overview").strip().lower()

        # UX guard: "AI Setup" requires an LLM provider configured AND a passing connectivity test.
        # If not ready, redirect operators to Config Center → LLM.
        if raw_section == "ai_setup":
            try:
                base_url = str(getattr(eff_settings, "llm_base_url", "") or "").strip()
                model = str(getattr(eff_settings, "llm_model_reasoning", "") or getattr(eff_settings, "llm_model", "") or "").strip()
                llm_ready = bool(base_url and model and bool((getattr(eff_settings, "llm_api_key", "") or "").strip()))
            except Exception:
                base_url = ""
                model = ""
                llm_ready = False

            fingerprint = f"{base_url}|{model}"
            last_ok = (repo.get_app_config("llm_test_reasoning_last_ok") or "").strip().lower() == "true"
            last_fp = (repo.get_app_config("llm_test_reasoning_last_fingerprint") or "").strip()
            llm_test_ok = bool(last_ok and last_fp and (last_fp == fingerprint))

            if not (llm_ready and llm_test_ok):
                msg_txt = (
                    "智能配置需要先配置并测试 AI 供应商（LLM）。请在 配置中心 → LLM 点击 “Test Reasoning LLM”。"
                    if normalize_lang(lang) == "zh"
                    else "Smart config requires an LLM provider configured and tested. Go to Config Center → LLM and run “Test Reasoning LLM”."
                )
                params: dict[str, str] = {"section": "config", "msg": msg_txt}
                if token:
                    params["token"] = token
                return RedirectResponse(url="/admin?" + urlencode(params) + "#cfg-llm", status_code=303)

        section = (
            raw_section
            if raw_section
            in {"overview", "push", "topics", "sources", "bindings", "ai_setup", "config", "prompts", "run", "all"}
            else "overview"
        )
        msg = request.query_params.get("msg")
        base = str(request.base_url).rstrip("/")
        push_setup_url = f"{base}/setup/push" + (f"?token={token}" if token else "")
        profile_setup_url = f"{base}/setup/profile" + (f"?token={token}" if token else "")
        topic_setup_url = f"{base}/setup/topic" + (f"?token={token}" if token else "")
        try:
            stats = repo.get_stats()
            from tracker.doctor import build_doctor_report

            doctor_report = build_doctor_report(
                settings=eff_settings,
                stats=stats,
                db_ok=True,
                db_error=None,
                profile_configured=bool(repo.get_app_config("profile_text")),
                telegram_chat_configured=bool(repo.get_app_config("telegram_chat_id")),
                activity=repo.get_activity_snapshot(),
            )
        except Exception:
            doctor_report = None
            stats = {}
        topics = repo.list_topics()
        topic_name_by_id = {t.id: t.name for t in topics}
        topic_policies = {p.topic_id: p for p in repo.list_topic_policies()}
        meta_map = {s.id: m for s, _h, m in repo.list_sources_with_health_and_meta() if m}
        score_map = {int(s.source_id): s for s in repo.list_source_scores(limit=10_000)}
        candidates = repo.list_source_candidates(status="new", limit=50)
        recent_events = [
            {
                "id": it.id,
                "when": (item.published_at or item.created_at).isoformat(),
                "topic": topic.name,
                "decision": it.decision,
                "title": item.title,
                "url": item.canonical_url,
            }
            for it, item, topic, _source in repo.list_recent_events(
                decisions=["alert", "digest"],
                since=dt.datetime.utcnow() - dt.timedelta(hours=24),
                limit=50,
            )
        ]
        digest_reports = [
            {
                "id": r.id,
                "when": r.created_at.isoformat(),
                "topic": (topic_row.name if topic_row else ""),
                "title": r.title,
                "markdown": (r.markdown or "")[:8000],
            }
            for r, topic_row in repo.list_reports(kind="digest", limit=10)
        ]
        health_reports = [
            {
                "id": r.id,
                "when": r.created_at.isoformat(),
                "title": r.title,
                "markdown": (r.markdown or "")[:8000],
            }
            for r, _topic_row in repo.list_reports(kind="health", limit=5)
        ]
        failed_pushes = repo.list_pushes(status="failed", limit=20)
        mute_days_default = (repo.get_app_config("telegram_feedback_mute_days_default") or "").strip() or "7"
        active_mutes = [
            {
                "scope": (m.scope or "").strip(),
                "key": (m.key or "").strip(),
                "topic_name": (m.topic_name or "").strip(),
                "muted_until": m.muted_until.isoformat() if getattr(m, "muted_until", None) else "",
                "reason": (m.reason or "").strip(),
            }
            for m in repo.list_active_mute_rules(limit=100)
        ]

        prompt_slots: list[dict[str, Any]] = []
        prompt_templates: list[dict[str, Any]] = []
        prompt_bindings: dict[str, str] = {}
        prompt_custom_template_ids: list[str] = []
        prompt_builtin_template_ids: list[str] = []
        try:
            from tracker.prompt_templates import builtin_slots, builtin_templates, list_all_templates, load_bindings, load_custom_templates

            prompt_slots = [asdict(s) for s in builtin_slots()]
            prompt_bindings = load_bindings(repo)
            custom_raw = load_custom_templates(repo)
            prompt_custom_template_ids = sorted([str(k or "").strip() for k in custom_raw.keys() if str(k or "").strip()])
            prompt_builtin_template_ids = sorted(
                [str(k or "").strip() for k in builtin_templates().keys() if str(k or "").strip()]
            )

            merged = list_all_templates(repo=repo)
            tpls = sorted(merged.values(), key=lambda t: (str(getattr(t, "builtin", True)), str(getattr(t, "id", ""))))
            for tpl in tpls:
                prompt_templates.append(
                    {
                        "id": tpl.id,
                        "title": tpl.title,
                        "description": tpl.description,
                        "builtin": bool(tpl.builtin),
                        "text_zh": tpl.text_zh,
                        "text_en": tpl.text_en,
                    }
                )
        except Exception:
            prompt_slots = []
            prompt_templates = []
            prompt_bindings = {}
            prompt_custom_template_ids = []
            prompt_builtin_template_ids = []

        llm_usage_1h = None
        llm_usage_24h = None
        llm_cost_1h = None
        llm_cost_24h = None
        try:
            llm_usage_1h = repo.summarize_llm_usage(since=dt.datetime.utcnow() - dt.timedelta(hours=1))
            llm_usage_24h = repo.summarize_llm_usage(since=dt.datetime.utcnow() - dt.timedelta(hours=24))
            llm_cost_1h = estimate_llm_cost_usd(
                prompt_tokens=int(llm_usage_1h.get("prompt_tokens") or 0) if llm_usage_1h else 0,
                completion_tokens=int(llm_usage_1h.get("completion_tokens") or 0) if llm_usage_1h else 0,
                input_per_million_usd=float(settings.llm_price_input_per_million_usd or 0.0),
                output_per_million_usd=float(settings.llm_price_output_per_million_usd or 0.0),
            )
            llm_cost_24h = estimate_llm_cost_usd(
                prompt_tokens=int(llm_usage_24h.get("prompt_tokens") or 0) if llm_usage_24h else 0,
                completion_tokens=int(llm_usage_24h.get("completion_tokens") or 0) if llm_usage_24h else 0,
                input_per_million_usd=float(settings.llm_price_input_per_million_usd or 0.0),
                output_per_million_usd=float(settings.llm_price_output_per_million_usd or 0.0),
            )
        except Exception:
            llm_usage_1h = None
            llm_usage_24h = None
            llm_cost_1h = None
            llm_cost_24h = None

        def _cost_str(v: float | None) -> str:
            return f"${v:.4f}" if v is not None else "unknown"

        config_json = ""
        settings_env_export_block = ""
        settings_ui: dict[str, Any] | None = None
        settings_changes = []
        if section == "config":
            try:
                from tracker.config_io import export_config

                config_json = json.dumps(export_config(session=session), ensure_ascii=False, indent=2)
            except Exception:
                config_json = ""
            try:
                from tracker.dynamic_config import export_settings_env_block

                settings_env_export_block = export_settings_env_block(
                    repo=repo,
                    settings=settings,
                    env_path=Path(settings.env_path or ".env"),
                )
            except Exception:
                settings_env_export_block = ""
            try:
                from tracker.admin_settings import build_settings_view

                settings_ui = build_settings_view(repo=repo, settings=settings, env_path=Path(settings.env_path or ".env"))
            except Exception:
                settings_ui = None
            try:
                settings_changes = repo.list_settings_changes(limit=50)
            except Exception:
                settings_changes = []

        ai_setup_snapshot_text = ""
        ai_setup_runs = []
        ai_setup_baseline_present = False
        ai_setup_baseline_exported_at = ""
        ai_setup_llm_ready = False
        if section == "ai_setup":
            try:
                from tracker.config_agent import export_tracking_snapshot, load_baseline_snapshot, snapshot_compact_text

                snap = export_tracking_snapshot(session=session)
                ai_setup_snapshot_text = snapshot_compact_text(snap)
                base = load_baseline_snapshot(repo)
                ai_setup_baseline_present = bool(base)
                ai_setup_baseline_exported_at = str((base or {}).get("exported_at") or "")
            except Exception:
                ai_setup_snapshot_text = ""
                ai_setup_baseline_present = False
                ai_setup_baseline_exported_at = ""
            try:
                ai_setup_runs = repo.list_config_agent_runs(kind="tracking_ai_setup", limit=30)
            except Exception:
                ai_setup_runs = []
            try:
                ai_setup_llm_ready = bool(
                    getattr(eff_settings, "llm_base_url", None)
                    and (
                        getattr(eff_settings, "llm_model_reasoning", None)
                        or getattr(eff_settings, "llm_model", None)
                    )
                    and bool((getattr(eff_settings, "llm_api_key", "") or "").strip())
                )
            except Exception:
                ai_setup_llm_ready = False

        # Prompt presets (static + operator-defined).
        custom_topic_presets = _load_custom_prompt_presets(repo, app_config_key="topic_policy_presets_custom_json")
        topic_policy_presets = _merge_prompt_presets(
            [asdict(p) for p in get_topic_policy_presets()],
            custom_topic_presets,
        )

        # AI Setup candidate notification knobs (operator UX).
        # Default is OFF (to avoid noisy pushes). When auto-accept is enabled, notifications are always suppressed.
        try:
            raw_on = (repo.get_app_config("ai_setup_candidates_notify_telegram_enabled") or "").strip().lower()
            if not raw_on:
                ai_setup_notify_enabled = False
            else:
                ai_setup_notify_enabled = False if raw_on in {"0", "false", "off", "no"} else True
        except Exception:
            ai_setup_notify_enabled = False
        try:
            raw_bs = (repo.get_app_config("ai_setup_candidates_notify_batch_size") or "").strip()
            ai_setup_notify_batch_size = int(raw_bs or 10)
        except Exception:
            ai_setup_notify_batch_size = 10
        ai_setup_notify_batch_size = max(1, min(500, int(ai_setup_notify_batch_size or 10)))

        # AI Setup discovery controls (operator UX).
        try:
            ai_setup_discover_enabled = bool(getattr(eff_settings, "discover_sources_enabled", True))
        except Exception:
            ai_setup_discover_enabled = True
        try:
            ai_setup_explore_weight = int(getattr(eff_settings, "discover_sources_explore_weight", 2) or 2)
        except Exception:
            ai_setup_explore_weight = 2
        ai_setup_explore_weight = max(0, min(10, int(ai_setup_explore_weight or 2)))
        ai_setup_exploit_weight = max(0, 10 - int(ai_setup_explore_weight))
        try:
            ai_setup_auto_accept_enabled = bool(getattr(eff_settings, "discover_sources_auto_accept_enabled", True))
        except Exception:
            ai_setup_auto_accept_enabled = True
        if ai_setup_auto_accept_enabled:
            ai_setup_notify_enabled = False
        try:
            ai_setup_min_source_score = int(getattr(eff_settings, "source_quality_min_score", 50))
        except Exception:
            ai_setup_min_source_score = 50
        ai_setup_min_source_score = max(0, min(100, int(ai_setup_min_source_score or 50)))
        try:
            ai_setup_max_sources_total = int(getattr(eff_settings, "discover_sources_max_sources_total", 500) or 500)
        except Exception:
            ai_setup_max_sources_total = 500
        ai_setup_max_sources_total = max(50, min(5000, int(ai_setup_max_sources_total or 500)))

        def _app_bool(key: str) -> bool:
            try:
                v = str(repo.get_app_config(key) or "").strip().lower()
            except Exception:
                v = ""
            return v in {"1", "true", "yes", "y", "on"}

        def _app_str(key: str) -> str:
            try:
                return str(repo.get_app_config(key) or "").strip()
            except Exception:
                return ""

        llm_reasoning_base_url = str(getattr(eff_settings, "llm_base_url", "") or "").strip()
        llm_reasoning_model = str(
            (getattr(eff_settings, "llm_model_reasoning", "") or getattr(eff_settings, "llm_model", "") or "")
        ).strip()
        llm_test_reasoning_fingerprint_current = f"{llm_reasoning_base_url}|{llm_reasoning_model}".strip("|")
        llm_test_reasoning_last_ok = _app_bool("llm_test_reasoning_last_ok")
        llm_test_reasoning_last_fingerprint = _app_str("llm_test_reasoning_last_fingerprint")
        llm_test_reasoning_ok = bool(
            llm_test_reasoning_last_ok
            and llm_test_reasoning_fingerprint_current
            and llm_test_reasoning_last_fingerprint == llm_test_reasoning_fingerprint_current
        )

        llm_mini_base_url = str(getattr(eff_settings, "llm_mini_base_url", "") or "").strip() or llm_reasoning_base_url
        llm_mini_model = str(
            (
                getattr(eff_settings, "llm_model_mini", "")
                or getattr(eff_settings, "llm_model_reasoning", "")
                or getattr(eff_settings, "llm_model", "")
                or ""
            )
        ).strip()
        llm_test_mini_fingerprint_current = f"{llm_mini_base_url}|{llm_mini_model}".strip("|")
        llm_test_mini_last_ok = _app_bool("llm_test_mini_last_ok")
        llm_test_mini_last_fingerprint = _app_str("llm_test_mini_last_fingerprint")
        llm_test_mini_ok = bool(
            llm_test_mini_last_ok
            and llm_test_mini_fingerprint_current
            and llm_test_mini_last_fingerprint == llm_test_mini_fingerprint_current
        )

        return templates.TemplateResponse(
            request,
            "admin.html",
            {
                "token": token,
                "lang": lang,
                "section": section,
                "msg": msg,
                "push_setup_url": push_setup_url,
                "profile_setup_url": profile_setup_url,
                "topic_setup_url": topic_setup_url,
                "doctor_report": doctor_report,
                "stats": stats,
                "topics": topics,
                "topic_policies": topic_policies,
                "topic_name_by_id": topic_name_by_id,
                "sources": repo.list_sources(),
                "bindings": repo.list_topic_sources(),
                "config_json": config_json,
                "settings_env_export_block": settings_env_export_block,
                "settings_ui": settings_ui,
                "settings_changes": settings_changes,
                "ai_setup_snapshot_text": ai_setup_snapshot_text,
                "ai_setup_runs": ai_setup_runs,
                "ai_setup_baseline_present": bool(ai_setup_baseline_present),
                "ai_setup_baseline_exported_at": ai_setup_baseline_exported_at,
                "ai_setup_llm_ready": bool(ai_setup_llm_ready),
                "ai_setup_notify_enabled": bool(ai_setup_notify_enabled),
                "ai_setup_notify_batch_size": int(ai_setup_notify_batch_size),
                "ai_setup_discover_enabled": bool(ai_setup_discover_enabled),
                "ai_setup_explore_weight": int(ai_setup_explore_weight),
                "ai_setup_exploit_weight": int(ai_setup_exploit_weight),
                "ai_setup_auto_accept_enabled": bool(ai_setup_auto_accept_enabled),
                "ai_setup_min_source_score": int(ai_setup_min_source_score),
                "ai_setup_max_sources_total": int(ai_setup_max_sources_total),
                "topic_policy_presets": topic_policy_presets,
                "custom_topic_policy_presets": custom_topic_presets,
                "prompt_slots": prompt_slots,
                "prompt_templates": prompt_templates,
                "prompt_bindings": prompt_bindings,
                "prompt_custom_template_ids": prompt_custom_template_ids,
                "prompt_builtin_template_ids": prompt_builtin_template_ids,
                "llm_curation_global": bool(
                    getattr(eff_settings, "llm_curation_enabled", False)
                    and getattr(eff_settings, "llm_base_url", None)
                    and (
                        getattr(eff_settings, "llm_model_reasoning", None)
                        or getattr(eff_settings, "llm_model", None)
                    )
                ),
                "source_meta": meta_map,
                "source_scores": score_map,
                "candidates": candidates,
                "recent_events": recent_events,
                "digest_reports": digest_reports,
                "health_reports": health_reports,
                "failed_pushes": failed_pushes,
                "telegram_feedback_mute_days_default": mute_days_default,
                "active_mutes": active_mutes,
                "llm_usage_1h": llm_usage_1h,
                "llm_usage_24h": llm_usage_24h,
                "llm_cost_1h": _cost_str(llm_cost_1h),
                "llm_cost_24h": _cost_str(llm_cost_24h),
                "llm_price_in": float(settings.llm_price_input_per_million_usd or 0.0),
                "llm_price_out": float(settings.llm_price_output_per_million_usd or 0.0),
                "env_write_allowed": bool(
                    _is_trusted_local_request(request) or getattr(eff_settings, "admin_allow_remote_env_update", False)
                ),
                # A small, user-facing subset of Settings for the config UI.
                "settings_snapshot": {
                    "access_ok": bool(
                        str(getattr(eff_settings, "admin_username", "") or "").strip()
                        and str(getattr(eff_settings, "admin_password", "") or "").strip()
                    ),
                    "output_language": str(getattr(eff_settings, "output_language", "") or ""),
                    "cron_timezone": str(getattr(eff_settings, "cron_timezone", "") or ""),
                    "include_domains": str(getattr(eff_settings, "include_domains", "") or ""),
                    "exclude_domains": str(getattr(eff_settings, "exclude_domains", "") or ""),
                    "priority_lane_enabled": bool(getattr(eff_settings, "priority_lane_enabled", False)),
                    "priority_lane_hours": int(getattr(eff_settings, "priority_lane_hours", 0) or 0),
                    "priority_lane_pool_max_candidates": int(
                        getattr(eff_settings, "priority_lane_pool_max_candidates", 0) or 0
                    ),
                    "priority_lane_triage_keep_candidates": int(
                        getattr(eff_settings, "priority_lane_triage_keep_candidates", 0) or 0
                    ),
                    "priority_lane_max_alert": int(getattr(eff_settings, "priority_lane_max_alert", 0) or 0),
                    "digest_scheduler_enabled": bool(getattr(eff_settings, "digest_scheduler_enabled", False)),
                    "digest_push_enabled": bool(getattr(eff_settings, "digest_push_enabled", False)),
                    "llm_curation_enabled": bool(getattr(eff_settings, "llm_curation_enabled", False)),
                    "llm_curation_triage_enabled": bool(getattr(eff_settings, "llm_curation_triage_enabled", False)),
                    "llm_base_url": str(getattr(eff_settings, "llm_base_url", "") or ""),
                    "llm_model": str(getattr(eff_settings, "llm_model", "") or ""),
                    "llm_model_reasoning": str(getattr(eff_settings, "llm_model_reasoning", "") or ""),
                    "llm_model_mini": str(getattr(eff_settings, "llm_model_mini", "") or ""),
                    "llm_mini_base_url": str(getattr(eff_settings, "llm_mini_base_url", "") or ""),
                    "llm_api_key_set": bool(str(getattr(eff_settings, "llm_api_key", "") or "").strip()),
                    "llm_proxy_set": bool(str(getattr(eff_settings, "llm_proxy", "") or "").strip()),
                    "llm_mini_api_key_set": bool(str(getattr(eff_settings, "llm_mini_api_key", "") or "").strip()),
                    "llm_mini_proxy_set": bool(str(getattr(eff_settings, "llm_mini_proxy", "") or "").strip()),
                    "llm_test_reasoning_last_ok": bool(llm_test_reasoning_last_ok),
                    "llm_test_reasoning_last_at": _app_str("llm_test_reasoning_last_at"),
                    "llm_test_reasoning_last_message": _app_str("llm_test_reasoning_last_message"),
                    "llm_test_reasoning_last_fingerprint": llm_test_reasoning_last_fingerprint,
                    "llm_test_reasoning_fingerprint_current": llm_test_reasoning_fingerprint_current,
                    "llm_test_reasoning_ok": bool(llm_test_reasoning_ok),
                    "llm_test_mini_last_ok": bool(llm_test_mini_last_ok),
                    "llm_test_mini_last_at": _app_str("llm_test_mini_last_at"),
                    "llm_test_mini_last_message": _app_str("llm_test_mini_last_message"),
                    "llm_test_mini_last_fingerprint": llm_test_mini_last_fingerprint,
                    "llm_test_mini_fingerprint_current": llm_test_mini_fingerprint_current,
                    "llm_test_mini_ok": bool(llm_test_mini_ok),
                },
            },
            headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
        )

    @app.get(
        "/admin/openrouter/prices",
        response_class=JSONResponse,
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_openrouter_prices(
        request: Request,
        refresh: bool = False,
        session: Session = Depends(get_db),
    ) -> dict:
        repo = Repo(session)
        from tracker.openrouter_prices import get_openrouter_prices

        return get_openrouter_prices(repo, ttl_seconds=6 * 3600, force_refresh=bool(refresh))

    @app.get(
        "/management",
        response_class=HTMLResponse,
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    @app.get(
        "/management/",
        response_class=HTMLResponse,
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def management(request: Request, session: Session = Depends(get_db)):
        return admin(request=request, session=session)

    def _redir(request: Request, *, msg: str | None = None):
        params: dict[str, str] = {}
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        if token:
            params["token"] = token
        section = (request.query_params.get("section") or "").strip().lower()
        if section:
            params["section"] = section
        if msg:
            params["msg"] = msg
        qs = urlencode(params) if params else ""
        url = "/admin" + (f"?{qs}" if qs else "")
        return RedirectResponse(url=url, status_code=303)

    @app.post(
        "/admin/config/import",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_config_import(
        request: Request,
        config_json: str = Form(""),
        update_existing: bool = Form(False),
        session: Session = Depends(get_db),
    ):
        from tracker.config_io import import_config as import_config_action

        raw = (config_json or "").strip()
        if not raw:
            return _redir(request, msg="missing config_json")
        try:
            data = json.loads(raw)
        except Exception:
            return _redir(request, msg="invalid JSON")
        if not isinstance(data, dict):
            return _redir(request, msg="invalid config (expected JSON object)")
        try:
            result = import_config_action(session=session, data=data, update_existing=bool(update_existing))
        except Exception as exc:
            return _redir(request, msg=str(exc))

        msg = (
            "Config imported: "
            f"topics+{result.get('topics_created', 0)} "
            f"sources+{result.get('sources_created', 0)} "
            f"bindings+{result.get('bindings_created', 0)} "
            f"policies+{result.get('policies_created', 0)}"
        )
        return _redir(request, msg=msg)

    @app.get(
        "/admin/config/bundle/export",
        response_class=Response,
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_config_bundle_export(
        request: Request,
        include_secrets: bool = False,
        session: Session = Depends(get_db),
    ):
        """
        Export a single JSON bundle that includes:
        - tracking config (topics/sources/bindings/policies/app_config), and
        - a Settings env block (optionally including env-only secrets).

        Notes:
        - By default, secrets are excluded.
        - If include_secrets=true, we enforce localhost/remote-write-allowed guard.
        """
        if include_secrets:
            _require_localhost(request)

        repo = Repo(session)
        from tracker.config_io import export_config
        from tracker.dynamic_config import export_settings_env_block

        env_block = export_settings_env_block(
            repo=repo,
            settings=settings,
            env_path=Path(settings.env_path or ".env"),
            include_env_only=bool(include_secrets),
        )
        bundle = {
            "kind": "tracker_config_bundle",
            "version": 1,
            "exported_at": dt.datetime.utcnow().isoformat() + "Z",
            "include_secrets": bool(include_secrets),
            "settings_env_block": env_block,
            "tracking_config": export_config(session=session),
        }
        content = json.dumps(bundle, ensure_ascii=False, indent=2)
        ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        filename = f"tracker-config-bundle-{ts}Z.json"
        return Response(
            content=content,
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
            },
        )

    @app.post(
        "/admin/config/bundle/import",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_config_bundle_import(
        request: Request,
        bundle_file: UploadFile = File(...),
        update_existing: bool = Form(False),
        apply_settings: bool = Form(True),
        apply_tracking: bool = Form(True),
        allow_secrets: bool = Form(False),
        session: Session = Depends(get_db),
    ):
        """
        Import a config bundle JSON file.

        Safety:
        - Requires localhost/remote-write-allowed guard (writes DB and potentially .env).
        - Secrets are only applied when allow_secrets=true.
        """
        _require_localhost(request)

        try:
            raw = await bundle_file.read()
        except Exception:
            raw = b""
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            return _redir(request, msg="missing bundle_file")
        try:
            data = json.loads(text)
        except Exception:
            return _redir(request, msg="invalid JSON")
        if not isinstance(data, dict):
            return _redir(request, msg="invalid bundle (expected JSON object)")
        if str(data.get("kind") or "").strip() != "tracker_config_bundle":
            return _redir(request, msg="invalid bundle kind")
        ver = int(data.get("version") or 0)
        if ver != 1:
            return _redir(request, msg="unsupported bundle version")

        repo = Repo(session)
        settings_msg = ""
        tracking_msg = ""

        if bool(apply_settings):
            from tracker.dynamic_config import _ENV_ONLY_FIELDS, apply_env_block_updates, env_key_for_field, parse_settings_env_block

            block = str(data.get("settings_env_block") or "").strip()
            if block:
                try:
                    updates = parse_settings_env_block(block, allow_remote_updates=True, blank_values_mean_no_change=True)
                except ValueError as exc:
                    return _redir(request, msg=f"invalid settings_env_block: {exc}")
                if not bool(allow_secrets):
                    # Drop env-only keys unless operator explicitly opts in.
                    for f in _ENV_ONLY_FIELDS:
                        updates.pop(env_key_for_field(f), None)
                if updates:
                    res = apply_env_block_updates(
                        repo=repo,
                        settings=settings,
                        env_path=Path(settings.env_path or ".env"),
                        env_updates=updates,
                    )
                    try:
                        repo.add_settings_change(
                            source="admin_bundle_import",
                            fields=[_field_from_env_key(k) for k in res.updated_env_keys if _field_from_env_key(k)],
                            env_keys=res.updated_env_keys,
                            restart_required=res.restart_required,
                            actor=_audit_actor(request),
                            client_host=(request.client.host if request.client else ""),
                        )
                    except Exception:
                        pass
                    settings_msg = f"settings_keys={len(res.updated_env_keys)}"
                else:
                    settings_msg = "settings_keys=0"
            else:
                settings_msg = "settings_keys=0"

        if bool(apply_tracking):
            from tracker.config_io import import_config as import_config_action

            cfg = data.get("tracking_config")
            if isinstance(cfg, dict):
                try:
                    result = import_config_action(session=session, data=cfg, update_existing=bool(update_existing))
                except Exception as exc:
                    return _redir(request, msg=str(exc))
                tracking_msg = (
                    "tracking: "
                    f"topics+{result.get('topics_created', 0)} "
                    f"sources+{result.get('sources_created', 0)} "
                    f"bindings+{result.get('bindings_created', 0)} "
                    f"policies+{result.get('policies_created', 0)}"
                )
            else:
                tracking_msg = "tracking: 0"

        msg = "bundle imported"
        if settings_msg:
            msg += f" ({settings_msg})"
        if tracking_msg:
            msg += f" ({tracking_msg})"
        return _redir(request, msg=msg)

    @app.post(
        "/admin/settings/apply-env",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_settings_apply_env(
        request: Request,
        env_block: str = Form(""),
        blank_means_clear: bool = Form(False),
        session: Session = Depends(get_db),
    ):
        """
        Import an env block for Settings and keep env+DB in sync.

        Notes:
        - Non-secret fields are persisted in DB for dynamic overrides.
        - Secrets remain in `.env` only and are never echoed back.
        """
        _require_localhost(request)

        raw = (env_block or "").strip()
        if not raw:
            return _redir(request, msg="missing env_block")

        repo = Repo(session)
        from tracker.dynamic_config import apply_env_block_updates, parse_settings_env_block

        try:
            updates = parse_settings_env_block(
                raw,
                allow_remote_updates=True,
                blank_values_mean_no_change=(not bool(blank_means_clear)),
            )
        except ValueError as exc:
            return _redir(request, msg=f"invalid env block: {exc}")

        if not updates:
            return _redir(request, msg="settings env import: no changes")

        res = apply_env_block_updates(
            repo=repo,
            settings=settings,
            env_path=Path(settings.env_path or ".env"),
            env_updates=updates,
        )
        try:
            repo.add_settings_change(
                source="admin_apply_env",
                fields=[_field_from_env_key(k) for k in res.updated_env_keys if _field_from_env_key(k)],
                env_keys=res.updated_env_keys,
                restart_required=res.restart_required,
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
            )
        except Exception:
            pass
        keys = ", ".join(res.updated_env_keys)
        msg = f"settings updated: {keys}"
        if res.restart_required:
            msg += " (restart needed for cron scheduling)"
        return _redir(request, msg=msg)

    @app.post(
        "/admin/settings/patch",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_settings_patch(
        request: Request,
        session: Session = Depends(get_db),
    ):
        """
        Patch Settings via normal form fields (diff-only).

        This is the primary web-admin path. It avoids the operator needing to think
        in terms of TRACKER_* env blocks.

        Notes:
        - Non-secret fields are persisted in DB for dynamic overrides.
        - Secrets remain in `.env` only and are never echoed back.
        """
        _require_localhost(request)
        repo = Repo(session)
        form = await request.form()

        from tracker.admin_settings import parse_settings_patch_form

        want_json = "application/json" in (request.headers.get("accept") or "")

        updates, errors = parse_settings_patch_form(form=form, repo=repo, settings=settings)
        if errors:
            if want_json:
                msg = "invalid fields: " + ", ".join(errors)
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "invalid_fields", "fields": errors, "message": msg},
                )
            return _redir(request, msg=f"invalid fields: {', '.join(errors)}")
        if not updates:
            if want_json:
                return JSONResponse(
                    status_code=200,
                    content={"ok": True, "updated_env_keys": [], "restart_required": False},
                )
            return _redir(request, msg="settings patch: no changes")

        from tracker.dynamic_config import apply_env_block_updates

        res = apply_env_block_updates(
            repo=repo,
            settings=settings,
            env_path=Path(settings.env_path or ".env"),
            env_updates=updates,
        )
        try:
            repo.add_settings_change(
                source="admin_patch",
                fields=[_field_from_env_key(k) for k in res.updated_env_keys if _field_from_env_key(k)],
                env_keys=res.updated_env_keys,
                restart_required=res.restart_required,
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
            )
        except Exception:
            pass
        msg = f"settings updated: {', '.join(res.updated_env_keys)}"
        if res.restart_required:
            msg += " (restart needed)"
        if want_json:
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "updated_env_keys": res.updated_env_keys,
                    "updated_db_keys": res.updated_db_keys,
                    "restart_required": bool(res.restart_required),
                    "message": msg,
                },
            )
        return _redir(request, msg=msg)

    @app.post(
        "/admin/settings/clear-secret",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_settings_clear_secret(
        request: Request,
        field: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Explicitly clear an env-only secret field.

        UI contract:
        - This endpoint is only reachable after a client-side confirmation.
        - We do not echo the secret value, and we treat this as a destructive action.
        """
        _require_localhost(request)
        f = (field or "").strip()
        if not f:
            return _redir(request, msg="missing field")

        from tracker.dynamic_config import _ENV_ONLY_FIELDS, env_key_for_field, apply_env_block_updates

        if f not in _ENV_ONLY_FIELDS:
            return _redir(request, msg="not a secret field")

        repo = Repo(session)
        res = apply_env_block_updates(
            repo=repo,
            settings=settings,
            env_path=Path(settings.env_path or ".env"),
            env_updates={env_key_for_field(f): ""},
        )
        try:
            repo.add_settings_change(
                source="admin_clear_secret",
                fields=[f],
                env_keys=[env_key_for_field(f)],
                restart_required=res.restart_required,
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
            )
        except Exception:
            pass
        msg = f"secret cleared: {env_key_for_field(f)}"
        if res.restart_required:
            msg += " (restart needed)"
        return _redir(request, msg=msg)

    @app.post(
        "/admin/settings/sync",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_settings_sync(
        request: Request,
        session: Session = Depends(get_db),
    ):
        """
        Force a one-shot env↔DB sync for Settings (non-secret fields).
        """
        _require_localhost(request)
        repo = Repo(session)
        from tracker.dynamic_config import sync_env_and_db

        res = sync_env_and_db(repo=repo, settings=settings, env_path=Path(settings.env_path or ".env"))
        try:
            fields = list(res.updated_db_keys) + [
                _field_from_env_key(k) for k in res.updated_env_keys if _field_from_env_key(k)
            ]
            repo.add_settings_change(
                source="admin_sync",
                fields=fields,
                env_keys=res.updated_env_keys,
                restart_required=res.restart_required,
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
            )
        except Exception:
            pass
        msg = f"settings synced: env_keys={len(res.updated_env_keys)} db_keys={len(res.updated_db_keys)}"
        return _redir(request, msg=msg)

    @app.post(
        "/admin/settings/test-llm",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_settings_test_llm(
        request: Request,
        provider: str = Form("reasoning"),  # reasoning|mini
        session: Session = Depends(get_db),
    ):
        """
        Lightweight connectivity test for the configured LLM provider.

        Contract:
        - Sends a tiny Chat Completions request (expects a short response).
        - Never echoes secrets.
        """
        repo = Repo(session)
        accept = str(request.headers.get("accept") or "").lower()
        want_json = "application/json" in accept
        from tracker.dynamic_config import effective_settings
        from tracker.envfile import parse_env_assignments

        eff = effective_settings(repo=repo, settings=settings)

        env_path = Path(settings.env_path or ".env")
        env_assignments: dict[str, str] = {}
        try:
            if env_path.exists():
                env_assignments = parse_env_assignments(env_path.read_text(encoding="utf-8"))
        except Exception:
            env_assignments = {}

        def _env(key: str) -> str:
            return str(env_assignments.get(key) or "").strip()

        which = (provider or "").strip().lower()
        if which not in {"reasoning", "mini"}:
            which = "reasoning"

        if which == "mini":
            base_url = str(eff.llm_mini_base_url or eff.llm_base_url or "").strip()
            api_key = _env("TRACKER_LLM_MINI_API_KEY") or _env("TRACKER_LLM_API_KEY") or str(settings.llm_mini_api_key or settings.llm_api_key or "").strip()
            proxy = (
                _env("TRACKER_LLM_MINI_PROXY")
                or _env("TRACKER_LLM_PROXY")
                or str(settings.llm_mini_proxy or settings.llm_proxy or "").strip()
            )
            model = str(eff.llm_model_mini or eff.llm_model_reasoning or eff.llm_model or "").strip()
        else:
            base_url = str(eff.llm_base_url or "").strip()
            api_key = _env("TRACKER_LLM_API_KEY") or str(settings.llm_api_key or "").strip()
            proxy = _env("TRACKER_LLM_PROXY") or str(settings.llm_proxy or "").strip()
            model = str(eff.llm_model_reasoning or eff.llm_model or "").strip()

        fingerprint = f"{base_url}|{model}"

        def _record(ok: bool, message: str) -> None:
            try:
                now = dt.datetime.utcnow().isoformat() + "Z"
                repo.set_app_config_many(
                    {
                        f"llm_test_{which}_last_ok": ("true" if ok else "false"),
                        f"llm_test_{which}_last_at": now,
                        f"llm_test_{which}_last_fingerprint": fingerprint,
                        f"llm_test_{which}_last_message": (str(message or "")[:800]),
                    }
                )
            except Exception:
                pass

        if not base_url:
            msg = f"llm test ({which}): missing base_url"
            _record(False, msg)
            if want_json:
                return JSONResponse(status_code=400, content={"ok": False, "message": msg})
            return _redir(request, msg=msg)
        if not api_key:
            msg = f"llm test ({which}): missing api_key"
            _record(False, msg)
            if want_json:
                return JSONResponse(status_code=400, content={"ok": False, "message": msg})
            return _redir(request, msg=msg)
        if not model:
            msg = f"llm test ({which}): missing model"
            _record(False, msg)
            if want_json:
                return JSONResponse(status_code=400, content={"ok": False, "message": msg})
            return _redir(request, msg=msg)

        headers = {"Authorization": f"Bearer {api_key}", "User-Agent": "tracker/0.1"}
        timeout = max(8.0, float(getattr(eff, "llm_timeout_seconds", 30) or 30))

        # Best-effort extra request body JSON (same knobs used by runtime requests).
        extra_body: dict[str, object] = {}
        try:
            raw_extra = str(getattr(eff, "llm_extra_body_json", "") or "").strip()
            if which == "mini":
                raw_mini_extra = str(getattr(eff, "llm_mini_extra_body_json", "") or "").strip()
                if raw_mini_extra:
                    raw_extra = raw_mini_extra
            if raw_extra:
                obj = json.loads(raw_extra)
                if isinstance(obj, dict):
                    for k in ("model", "messages", "stream"):
                        if k in obj:
                            obj.pop(k, None)
                    extra_body = obj
        except Exception:
            extra_body = {}

        try:
            from tracker.prompt_templates import resolve_prompt_best_effort

            test_sys = resolve_prompt_best_effort(repo=repo, settings=eff, slot_id="admin.test_llm.system").text
            test_user = resolve_prompt_best_effort(repo=repo, settings=eff, slot_id="admin.test_llm.user").text
        except Exception as exc:
            msg = f"llm test ({which}): prompt template system error: {exc}"
            _record(False, msg)
            if want_json:
                return JSONResponse(status_code=500, content={"ok": False, "message": msg})
            return _redir(request, msg=msg)

        payload: dict[str, object] = {
            "model": model,
            "messages": [
                {"role": "system", "content": test_sys},
                {"role": "user", "content": test_user},
            ],
            "temperature": 0,
            "max_tokens": 16,
        }
        if extra_body:
            payload.update(extra_body)

        started = dt.datetime.utcnow()
        try:
            from tracker.openai_compat import extract_text_from_openai_compat_response, post_openai_compat_json

            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                proxy=(proxy or None),
            ) as client:
                data, mode = await post_openai_compat_json(
                    repo=repo,
                    client=client,
                    base_url=base_url,
                    headers=headers,
                    payload_chat=payload,  # type: ignore[arg-type]
                )
        except httpx.HTTPStatusError as exc:
            body = ""
            try:
                body = (exc.response.text or "").strip()
            except Exception:
                body = ""
            if len(body) > 500:
                body = body[:500] + "…"
            msg = f"llm test ({which}): HTTP {exc.response.status_code}: {body or exc}"
            _record(False, msg)
            if want_json:
                return JSONResponse(status_code=502, content={"ok": False, "message": msg})
            return _redir(request, msg=msg)
        except Exception as exc:
            msg = f"llm test ({which}): error: {exc}"
            _record(False, msg)
            if want_json:
                return JSONResponse(status_code=502, content={"ok": False, "message": msg})
            return _redir(request, msg=msg)

        elapsed_ms = int((dt.datetime.utcnow() - started).total_seconds() * 1000)
        out = extract_text_from_openai_compat_response(data)
        if not out:
            out = "(empty)"
        if len(out) > 200:
            out = out[:200] + "…"

        msg = f"llm test ({which}): ok in {elapsed_ms}ms · mode={mode} · {out}"
        _record(True, msg)
        if want_json:
            return JSONResponse(status_code=200, content={"ok": True, "message": msg})
        return _redir(request, msg=msg)

    _PROMPT_TEMPLATE_ID_RE = re.compile(r"^[a-zA-Z0-9_.:-]{3,120}$")

    @app.get(
        "/admin/prompts/state",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_prompts_state(
        session: Session = Depends(get_db),
    ):
        """
        Return prompt slots/templates/bindings for Web Admin/TG.

        Notes:
        - This endpoint returns prompts (not secrets).
        - Custom templates override built-ins by id.
        """
        repo = Repo(session)
        try:
            from tracker.prompt_templates import (
                builtin_slots,
                builtin_templates,
                list_all_templates,
                load_bindings,
                load_custom_templates,
            )
        except Exception:
            return JSONResponse(status_code=500, content={"ok": False, "message": "prompt template system unavailable"})

        slots = [asdict(s) for s in builtin_slots()]
        bindings = load_bindings(repo)
        custom_raw = load_custom_templates(repo)
        custom_ids = sorted([str(k or "").strip() for k in custom_raw.keys() if str(k or "").strip()])
        builtin_ids = sorted([str(k or "").strip() for k in builtin_templates().keys() if str(k or "").strip()])

        merged = list_all_templates(repo=repo)
        templates: list[dict[str, Any]] = []
        for tpl in sorted(merged.values(), key=lambda t: (0 if not bool(getattr(t, "builtin", True)) else 1, str(getattr(t, "id", "")))):
            templates.append(
                {
                    "id": tpl.id,
                    "title": tpl.title,
                    "description": tpl.description,
                    "builtin": bool(tpl.builtin),
                    "text_zh": tpl.text_zh,
                    "text_en": tpl.text_en,
                }
            )

        return {
            "ok": True,
            "slots": slots,
            "templates": templates,
            "bindings": bindings,
            "custom_template_ids": custom_ids,
            "builtin_template_ids": builtin_ids,
        }

    @app.post(
        "/admin/prompts/bindings/set",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_prompts_bindings_set(
        request: Request,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            obj = await request.json()
        except Exception:
            obj = {}
        slot_id = str((obj or {}).get("slot_id") or "").strip()
        template_id = str((obj or {}).get("template_id") or "").strip()
        if not slot_id:
            raise HTTPException(status_code=400, detail="missing slot_id")

        from tracker.prompt_templates import load_bindings, save_bindings, list_all_templates

        bindings = load_bindings(repo)
        if not template_id:
            bindings.pop(slot_id, None)
        else:
            # Only allow binding to an existing template id (builtin or custom), to avoid silent typos.
            if template_id not in list_all_templates(repo=repo):
                raise HTTPException(status_code=400, detail=f"unknown template_id: {template_id}")
            bindings[slot_id] = template_id
        save_bindings(repo, bindings)
        return {"ok": True, "bindings": bindings}

    @app.post(
        "/admin/prompts/templates/upsert",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_prompts_templates_upsert(
        request: Request,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            obj = await request.json()
        except Exception:
            obj = {}
        template_id = str((obj or {}).get("id") or "").strip()
        title = str((obj or {}).get("title") or "").strip()
        description = str((obj or {}).get("description") or "").strip()
        text_zh = str((obj or {}).get("text_zh") or "").rstrip()
        text_en = str((obj or {}).get("text_en") or "").rstrip()

        if not template_id:
            raise HTTPException(status_code=400, detail="missing template id")
        if not _PROMPT_TEMPLATE_ID_RE.match(template_id):
            raise HTTPException(
                status_code=400,
                detail="invalid template id (allowed: a-zA-Z0-9_.:-, length 3-120)",
            )
        if not (text_zh.strip() or text_en.strip()):
            raise HTTPException(status_code=400, detail="template text is empty (provide zh and/or en)")

        from tracker.prompt_templates import load_custom_templates, save_custom_templates

        templates = load_custom_templates(repo)
        templates[template_id] = {
            "title": title or template_id,
            "description": description,
            "text": {"zh": text_zh, "en": text_en},
        }
        save_custom_templates(repo, templates)
        return {"ok": True, "id": template_id}

    @app.post(
        "/admin/prompts/templates/delete",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_prompts_templates_delete(
        request: Request,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            obj = await request.json()
        except Exception:
            obj = {}
        template_id = str((obj or {}).get("id") or "").strip()
        if not template_id:
            raise HTTPException(status_code=400, detail="missing template id")

        from tracker.prompt_templates import builtin_templates, load_bindings, load_custom_templates, save_bindings, save_custom_templates

        templates = load_custom_templates(repo)
        existed = bool(template_id in templates)
        if existed:
            templates.pop(template_id, None)
            save_custom_templates(repo, templates)

        # If this template id no longer exists (not builtin, not custom), unbind affected slots.
        still_exists = bool(template_id in builtin_templates() or template_id in templates)
        if not still_exists:
            bindings = load_bindings(repo)
            changed = False
            for slot, tid in list(bindings.items()):
                if str(tid or "").strip() == template_id:
                    bindings.pop(slot, None)
                    changed = True
            if changed:
                save_bindings(repo, bindings)

        return {"ok": True, "deleted": existed}

    @app.post(
        "/admin/prompts/templates/restore",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_prompts_templates_restore(
        request: Request,
        session: Session = Depends(get_db),
    ):
        """
        Restore a built-in template by deleting its custom override (if any).

        This keeps operator intent explicit:
        - Delete = remove a custom template
        - Restore = revert an overridden built-in to its original version
        """
        repo = Repo(session)
        try:
            obj = await request.json()
        except Exception:
            obj = {}
        template_id = str((obj or {}).get("id") or "").strip()
        if not template_id:
            raise HTTPException(status_code=400, detail="missing template id")

        from tracker.prompt_templates import builtin_templates, load_custom_templates, save_custom_templates

        if template_id not in builtin_templates():
            raise HTTPException(status_code=400, detail=f"not a built-in template id: {template_id}")

        templates = load_custom_templates(repo)
        restored = bool(template_id in templates)
        if restored:
            templates.pop(template_id, None)
            save_custom_templates(repo, templates)

        return {"ok": True, "id": template_id, "restored": restored}

    @app.post(
        "/admin/prompts/templates/translate",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_prompts_templates_translate(
        request: Request,
        session: Session = Depends(get_db),
    ):
        """
        Translate prompt template text between zh/en using the mini LLM.

        This is intended for operator convenience (Template Editor "Auto translate").
        """
        repo = Repo(session)
        try:
            obj = await request.json()
        except Exception:
            obj = {}

        source_lang = str((obj or {}).get("source_lang") or "").strip().lower()
        target_lang = str((obj or {}).get("target_lang") or "").strip().lower()
        updated_source_text = str((obj or {}).get("updated_source_text") or "")
        previous_target_text = str((obj or {}).get("previous_target_text") or "")

        if source_lang not in {"zh", "en"} or target_lang not in {"zh", "en"} or source_lang == target_lang:
            raise HTTPException(status_code=400, detail="invalid source_lang/target_lang (expected zh/en)")
        if not updated_source_text.strip():
            raise HTTPException(status_code=400, detail="missing updated_source_text")

        # Use effective settings so DB overrides apply (base_url/model) without restart.
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        usage_cb = None
        try:
            usage_cb = make_llm_usage_recorder(session=session)
        except Exception:
            usage_cb = None

        from tracker.llm import llm_translate_prompt_template

        try:
            out = await llm_translate_prompt_template(
                repo=repo,
                settings=eff,
                source_lang=source_lang,
                target_lang=target_lang,
                updated_source_text=updated_source_text,
                previous_target_text=previous_target_text,
                usage_cb=usage_cb,
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"translate failed: {exc}") from exc
        if out is None:
            raise HTTPException(status_code=400, detail="LLM not configured")

        text = out.strip()
        if len(text) > 30_000:
            text = text[:30_000] + "…"
        return {"ok": True, "text": text}

    @app.post(
        "/admin/services/restart",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_services_restart(
        request: Request,
        units: str = Form("tracker,tracker-api"),
        session: Session = Depends(get_db),
    ):
        """
        Best-effort restart for systemd user services.

        This is intentionally async/queued: the endpoint returns first, then triggers
        the restart shortly after (so restarting tracker-api doesn't cut off the response).
        """
        lang = get_request_lang(request)
        want_json = "application/json" in (request.headers.get("accept") or "")

        from tracker.service_control import queue_restart_systemd_user, restart_hint_text

        # Delay restart long enough for debounced autosaves (≈700ms) to commit,
        # otherwise operators can lose the last config toggle right before restarting.
        res = queue_restart_systemd_user(units=units, delay_seconds=1.5)
        try:
            repo = Repo(session)
            repo.add_settings_change(
                source="admin_restart",
                fields=[],
                env_keys=list(res.units),
                restart_required=False,
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
            )
        except Exception:
            pass
        if want_json:
            hint = restart_hint_text(lang=lang, units=res.units)
            return JSONResponse(
                status_code=200,
                content={
                    "ok": bool(res.ok),
                    "queued": bool(res.queued),
                    "units": list(res.units),
                    "message": str(res.message or ""),
                    "command": str(hint or ""),
                    "severity": ("success" if res.ok else "warning"),
                },
            )

        if res.ok:
            return _redir(request, msg=f"restart queued: {', '.join(res.units)}")
        return _redir(request, msg=f"restart failed: {res.message}. {restart_hint_text(lang=lang, units=res.units)}")

    # --- Tracking → AI Setup (natural language config; auditable; bounded)

    @app.post(
        "/admin/ai-setup/plan",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    async def admin_ai_setup_plan(
        request: Request,
        user_prompt: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Generate a bounded tracking config plan (topics/sources/bindings) via LLM.

        Contract:
        - Never touches Settings/secrets.
        - Any actual tracking config changes require explicit Apply (separate endpoint).
        - Source expansion may be triggered to produce *reviewable candidates* (no auto-accept).
        """
        repo = Repo(session)
        want = (user_prompt or "").strip()
        if not want:
            return JSONResponse(status_code=400, content={"ok": False, "error": "missing_prompt", "message": "missing user_prompt"})

        def _looks_like_profile_brief(text: str) -> bool:
            """
            Heuristic: detect the structured profile brief format emitted by `/setup/profile`.

            When present, we can plan deterministically (topics + many SearxNG seeds) to
            avoid slow/flaky LLM planning calls for large prompts.
            """
            raw = (text or "").strip()
            if not raw:
                return False
            up = raw.upper()
            if "INTEREST_AXES:" not in up:
                return False
            return ("RETRIEVAL_QUERIES:" in up) or ("SEED_QUERIES:" in up)

        def _fallback_profile_seed_plan(*, profile_topic_name: str) -> dict[str, Any]:
            """
            Minimal plan that is safe and triggers `autofix_ai_setup_plan_for_source_expansion`.

            NOTE: `validate_ai_setup_plan(...)` requires a non-empty actions list.
            """
            name = (profile_topic_name or "").strip() or "Profile"
            return {"actions": [{"op": "topic.upsert", "name": name, "query": "", "enabled": True}]}

        # Use effective settings so operator DB overrides (base_url/model) take effect without restart.
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        if not (
            getattr(eff, "llm_base_url", None)
            and (getattr(eff, "llm_model_reasoning", None) or getattr(eff, "llm_model", None))
        ):
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": "llm_not_configured",
                    "message": "LLM not configured. Set TRACKER_LLM_BASE_URL + TRACKER_LLM_MODEL_REASONING (or TRACKER_LLM_MODEL) (+ TRACKER_LLM_API_KEY).",
                },
            )
        if not (getattr(eff, "llm_api_key", None) or ""):
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": "llm_api_key_missing",
                    "message": "LLM API key missing (TRACKER_LLM_API_KEY).",
                },
            )

        try:
            from tracker.config_agent import apply_plan_to_snapshot, diff_tracking_snapshots, export_tracking_snapshot, snapshot_compact_text

            snap_before = export_tracking_snapshot(session=session)
            snap_text = snapshot_compact_text(snap_before)
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "snapshot_failed", "message": f"failed to export snapshot: {exc}"},
            )

        # --- Optional: web access/search for smarter planning when the operator pastes URLs.
        #
        # This keeps the LLM "tool-less" while still letting it read the content the operator provided.
        web_context = ""
        web_search_context = ""
        try:
            import re as _re

            from tracker.fulltext import fetch_fulltext_for_url
            from tracker.http_auth import (
                AuthRequiredError,
                cookie_header_for_url,
                parse_cookie_jar_json,
            )

            _URL_RE = _re.compile(r"https?://[^\s<>()]+", _re.IGNORECASE)

            def _clean_url(u: str) -> str:
                s = (u or "").strip()
                # Common trailing punctuation from copy/paste.
                while s and s[-1] in ")]}>,.;":
                    s = s[:-1]
                return s.strip()

            raw_urls = [_clean_url(m.group(0)) for m in _URL_RE.finditer(want)]
            urls: list[str] = []
            seen: set[str] = set()
            for u in raw_urls:
                if not u.startswith(("http://", "https://")):
                    continue
                if u in seen:
                    continue
                seen.add(u)
                urls.append(u)
                if len(urls) >= 6:
                    break

            # Cookie source (best-effort): static cookie jar (env-only; secrets).
            cookie_jar = parse_cookie_jar_json(getattr(eff, "cookie_jar_json", "") or "")

            async def _cookie_for_url(url: str) -> str | None:
                return cookie_header_for_url(url=url, cookie_jar=cookie_jar)

            fetched_blocks: list[str] = []
            for u in urls:
                try:
                    cookie = await _cookie_for_url(u)
                    txt = await fetch_fulltext_for_url(
                        url=u,
                        timeout_seconds=int(getattr(eff, "http_timeout_seconds", 20) or 20),
                        max_chars=10_000,
                        discourse_cookie=((getattr(eff, "discourse_cookie", "") or "").strip() or cookie or None),
                        cookie_header=cookie,
                    )
                    txt2 = (txt or "").strip()
                    if len(txt2) > 6000:
                        txt2 = txt2[:6000] + "…"
                    fetched_blocks.append(f"URL: {u}\nTEXT: {txt2}".strip())
                except AuthRequiredError as exc:
                    fetched_blocks.append(f"URL: {u}\nAUTH_REQUIRED: host={exc.host} status={exc.status_code} final={exc.final_url}".strip())
                except Exception as exc:
                    fetched_blocks.append(f"URL: {u}\nFETCH_FAILED: {str(exc)[:300]}".strip())
                if len(fetched_blocks) >= 6:
                    break
            if fetched_blocks:
                web_context = "\n\n".join(fetched_blocks).strip()

            # Optional bounded search via SearxNG (if configured).
            searx_base = (getattr(eff, "searxng_base_url", "") or "").strip()
            if searx_base:
                try:
                    from tracker.connectors.searxng import SearxngConnector, build_searxng_search_url

                    q = _re.sub(_URL_RE, " ", want or "")
                    q = " ".join(q.split()).strip()
                    if len(q) > 160:
                        q = q[:160]
                    if q:
                        search_url = build_searxng_search_url(base_url=searx_base, query=q, results=6)
                        entries = await SearxngConnector(timeout_seconds=int(getattr(eff, "http_timeout_seconds", 20) or 20)).fetch(url=search_url)
                        out_lines: list[str] = [f"QUERY: {q}"]
                        for e in (entries or [])[:6]:
                            if not e or not e.url:
                                continue
                            title = (e.title or "").strip()
                            summ = (e.summary or "").strip()
                            if len(summ) > 240:
                                summ = summ[:240] + "…"
                            out_lines.append(f"- {title} — {e.url}".strip())
                            if summ:
                                out_lines.append(f"  {summ}".strip())
                        web_search_context = "\n".join([ln for ln in out_lines if ln is not None]).strip()
                except Exception:
                    web_search_context = ""
        except Exception:
            web_context = ""
            web_search_context = ""

        usage_cb = None
        try:
            usage_cb = make_llm_usage_recorder(session=session)
        except Exception:
            usage_cb = None

        # If the operator pastes a huge profile dump, transform it into a structured brief
        # instead of relying on hard truncation inside the LLM call.
        want_for_planner = want
        input_warnings: list[str] = []
        transform_info: dict[str, Any] | None = None
        try:
            chunk_chars = int(getattr(eff, "ai_setup_transform_chunk_chars", 10_000) or 10_000)
        except Exception:
            chunk_chars = 10_000
        try:
            max_chunks = int(getattr(eff, "ai_setup_transform_max_chunks", 20) or 20)
        except Exception:
            max_chunks = 20
        chunk_chars = max(2000, min(50_000, chunk_chars))
        max_chunks = max(1, min(200, max_chunks))

        # If the operator pasted a structured brief (e.g. from /setup/profile),
        # skip LLM transformation even when the text is long.
        if len(want) > chunk_chars and not _looks_like_profile_brief(want):
            try:
                from tracker.llm import llm_transform_tracking_ai_setup_input

                def _split_chunks(text: str, *, size: int, limit: int) -> list[str]:
                    raw = (text or "").strip()
                    if not raw:
                        return []
                    all_chunks: list[str] = []
                    i = 0
                    n = len(raw)
                    hard_cap = 5000  # safety bound
                    while i < n and len(all_chunks) < hard_cap:
                        j = min(n, i + size)
                        # Prefer cutting at a newline boundary when available.
                        cut = raw.rfind("\n", i, j)
                        if cut > i + int(size * 0.4):
                            j = cut
                        all_chunks.append(raw[i:j].strip())
                        i = j
                    all_chunks = [c for c in all_chunks if c]
                    if len(all_chunks) <= limit:
                        return all_chunks
                    if limit <= 1:
                        return [all_chunks[0]]
                    idxs: list[int] = []
                    last = len(all_chunks) - 1
                    for k in range(limit):
                        try:
                            idx = int(round(k * last / (limit - 1)))
                        except Exception:
                            idx = 0
                        if idx < 0:
                            idx = 0
                        if idx > last:
                            idx = last
                        if not idxs or idxs[-1] != idx:
                            idxs.append(idx)
                    return [all_chunks[ii] for ii in idxs if 0 <= ii < len(all_chunks)]

                chunks = _split_chunks(want, size=chunk_chars, limit=max_chunks)
                axes: list[str] = []
                axes_seen: set[str] = set()
                keywords: list[str] = []
                keywords_seen: set[str] = set()
                seed_queries: list[str] = []
                seed_queries_seen: set[str] = set()
                understanding = ""

                def _add_many(dst: list[str], seen: set[str], items: object, *, max_items: int) -> None:
                    if not isinstance(items, list):
                        return
                    for x in items:
                        s = str(x or "").strip()
                        if not s:
                            continue
                        key = " ".join(s.split()).strip().lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        dst.append(s)
                        if len(dst) >= max_items:
                            break

                for ch in chunks:
                    obj = await llm_transform_tracking_ai_setup_input(
                        repo=repo,
                        settings=eff,
                        user_prompt_chunk=ch,
                        usage_cb=usage_cb,
                    )
                    if not obj or not isinstance(obj, dict):
                        continue
                    if not understanding:
                        u0 = str(obj.get("understanding") or "").strip()
                        if u0:
                            understanding = u0
                    _add_many(axes, axes_seen, obj.get("interest_axes"), max_items=5000)
                    _add_many(keywords, keywords_seen, obj.get("keywords"), max_items=8000)
                    _add_many(seed_queries, seed_queries_seen, obj.get("seed_queries"), max_items=8000)

                lines: list[str] = []
                lines.append("SMART_CONFIG_INPUT (transformed from a large prompt; raw text is stored for audit):")
                if understanding:
                    lines.append("")
                    lines.append("UNDERSTANDING:")
                    lines.append(understanding)
                if axes:
                    lines.append("")
                    lines.append("INTEREST_AXES:")
                    for a in axes[:5000]:
                        lines.append(f"- {a}")
                if keywords:
                    lines.append("")
                    lines.append("KEYWORDS:")
                    for k in keywords[:8000]:
                        lines.append(f"- {k}")
                if seed_queries:
                    lines.append("")
                    lines.append("SEED_QUERIES:")
                    for q in seed_queries[:8000]:
                        lines.append(f"- {q}")
                lines.append("")
                lines.append("REQUIREMENTS:")
                lines.append("- Expand sources as much as possible; do not be conservative.")
                lines.append("- Split into semantically-orthogonal topics; no preset topic count.")
                lines.append("- Generate many short, semantically-orthogonal search seeds; do NOT stuff all keywords into one query.")
                lines.append("- Do NOT omit sources because you judge them sensitive/gray/harmful; final judgement is human.")
                want_for_planner = "\n".join(lines).strip()
                transform_info = {
                    "understanding": understanding,
                    "interest_axes": list(axes),
                    "keywords": list(keywords),
                    "seed_queries": list(seed_queries),
                }

                input_warnings.append(
                    f"input transformed: raw_chars={len(want)} chunks={len(chunks)} axes={len(axes)} "
                    f"keywords={len(keywords)} seed_queries={len(seed_queries)}"
                )
            except Exception as exc:
                input_warnings.append(f"input transform failed (fallback to raw prompt): {exc}")
                want_for_planner = want

        try:
            # Adaptive planner budget: scale output tokens with input complexity.
            planner_max_tokens: int | None = None
            try:
                base_tokens = int(getattr(eff, "ai_setup_plan_max_tokens", 50_000) or 50_000)
            except Exception:
                base_tokens = 50_000
            # Clamp to avoid pathological values (and preserve stability across LLM backends).
            base_tokens = max(1400, min(200_000, base_tokens))
            base_floor = max(1400, int(base_tokens * 0.35))
            try:
                if transform_info and isinstance(transform_info, dict):
                    axes_n = len(transform_info.get("interest_axes") or [])
                    kw_n = len(transform_info.get("keywords") or [])
                    q_n = len(transform_info.get("seed_queries") or [])
                    complexity = int(axes_n + q_n + (kw_n / 8.0))
                    mult = 1.0
                    if complexity >= 300:
                        mult = 3.0
                    elif complexity >= 150:
                        mult = 2.4
                    elif complexity >= 80:
                        mult = 1.9
                    elif complexity >= 40:
                        mult = 1.5
                    elif complexity >= 20:
                        mult = 1.25
                    planner_max_tokens = int(min(base_tokens, max(base_floor, int(base_floor * mult))))
                elif len(want_for_planner) > 30_000:
                    planner_max_tokens = int(min(base_tokens, max(base_floor, int(base_floor * 1.6))))
            except Exception:
                planner_max_tokens = None

            # If the transformed prompt is still too large, plan in multiple passes and merge.
            plan: dict[str, Any]
            warnings: list[str]
            axes2: list[str] = list((transform_info.get("interest_axes") or []) if isinstance(transform_info, dict) else [])
            kw2: list[str] = list((transform_info.get("keywords") or []) if isinstance(transform_info, dict) else [])
            q2: list[str] = list((transform_info.get("seed_queries") or []) if isinstance(transform_info, dict) else [])
            understanding2 = str((transform_info.get("understanding") or "") if isinstance(transform_info, dict) else "").strip()

            needs_multi_pass = bool(transform_info) and (
                len(want_for_planner) > 75_000
                or len(axes2) > 12
                or len(q2) > 40
                or len(kw2) > 200
            )

            profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"

            # Fast-path: if the prompt is already in structured profile-brief form, skip the LLM
            # planner and rely on deterministic expansion from INTEREST_AXES/RETRIEVAL_QUERIES.
            #
            # This avoids `httpx.ReadTimeout` on slow reasoning models / busy gateways.
            if _looks_like_profile_brief(want_for_planner):
                plan = _fallback_profile_seed_plan(profile_topic_name=profile_topic_name)
                warnings = ["planner: skipped LLM (profile brief detected); expanded deterministically"]
            elif needs_multi_pass:
                from tracker.config_agent import validate_ai_setup_plan

                def _build_pass_prompt(
                    *,
                    part: int,
                    understanding: str,
                    axes: list[str],
                    keywords: list[str],
                    seed_queries: list[str],
                    axis_start: int,
                    kw_start: int,
                    q_start: int,
                    budget_chars: int = 55_000,
                ) -> tuple[str, int, int, int]:
                    cur_len = 0
                    out_lines: list[str] = []

                    def _add(line: str) -> None:
                        nonlocal cur_len
                        out_lines.append(line)
                        cur_len += len(line) + 1

                    def _take(items: list[str], start: int, *, max_items: int) -> tuple[list[str], int]:
                        picked: list[str] = []
                        i = start
                        while i < len(items) and len(picked) < max_items:
                            s = str(items[i] or "").strip()
                            i += 1
                            if not s:
                                continue
                            line = f"- {s}"
                            if cur_len + len(line) + 1 > budget_chars:
                                break
                            picked.append(s)
                            _add(line)
                        return picked, i

                    _add(f"SMART_CONFIG_INPUT (transformed; PART {part}):")
                    if understanding:
                        _add("")
                        _add("UNDERSTANDING:")
                        for ln in str(understanding).splitlines()[:30]:
                            if cur_len + len(ln) + 1 > budget_chars:
                                break
                            _add(ln)

                    axis_next = axis_start
                    if axis_start < len(axes):
                        _add("")
                        _add("INTEREST_AXES:")
                        _picked, axis_next = _take(axes, axis_start, max_items=10_000)

                    q_next = q_start
                    if q_start < len(seed_queries):
                        _add("")
                        _add("SEED_QUERIES:")
                        _picked, q_next = _take(seed_queries, q_start, max_items=50_000)

                    # Keywords are optional hints; include only if there's still budget.
                    kw_next = kw_start
                    if kw_start < len(keywords) and cur_len < int(budget_chars * 0.9):
                        _add("")
                        _add("KEYWORDS:")
                        _picked, kw_next = _take(keywords, kw_start, max_items=50_000)

                    _add("")
                    _add("REQUIREMENTS:")
                    _add("- Expand sources as much as possible; do not be conservative.")
                    _add("- Split into semantically-orthogonal topics; no preset topic count.")
                    _add("- Generate many short, semantically-orthogonal search seeds; do NOT stuff all keywords into one query.")
                    _add("- Do NOT omit sources because you judge them sensitive/gray/harmful; final judgement is human.")

                    text = "\n".join(out_lines).strip()
                    return text, axis_next, kw_next, q_next

                axis_i = 0
                kw_i = 0
                q_i = 0
                merged_actions: list[dict[str, Any]] = []
                merged_warnings: list[str] = []
                passes_used = 0
                per_pass_tokens = int(min(planner_max_tokens, 12_000)) if planner_max_tokens else None
                budget_chars = 55_000 if len(want_for_planner) > 75_000 else 28_000

                # Keep the number of passes bounded, but try hard to cover all items.
                for part in range(1, 21):
                    if axis_i >= len(axes2) and q_i >= len(q2) and kw_i >= len(kw2):
                        break
                    pass_prompt, axis_next, kw_next, q_next = _build_pass_prompt(
                        part=part,
                        understanding=understanding2,
                        axes=axes2,
                        keywords=kw2,
                        seed_queries=q2,
                        axis_start=axis_i,
                        kw_start=kw_i,
                        q_start=q_i,
                        budget_chars=budget_chars,
                    )
                    planned_part = None
                    try:
                        planned_part = await llm_plan_tracking_ai_setup(
                            repo=repo,
                            settings=eff,
                            user_prompt=pass_prompt,
                            tracking_snapshot_text=snap_text,
                            web_context=web_context,
                            web_search_context=web_search_context,
                            max_tokens_override=per_pass_tokens,
                            usage_cb=usage_cb,
                        )
                    except Exception as exc:
                        bumped = None
                        try:
                            if per_pass_tokens:
                                max_pass = int(planner_max_tokens or base_tokens)
                                bumped = int(min(max_pass, max(per_pass_tokens + 1500, int(per_pass_tokens * 1.8))))
                        except Exception:
                            bumped = None
                        if bumped and per_pass_tokens and bumped != per_pass_tokens:
                            merged_warnings.append(
                                f"retry: increased planner budget for pass {part}: {per_pass_tokens} -> {bumped} (after error: {str(exc)[:160]})"
                            )
                            planned_part = await llm_plan_tracking_ai_setup(
                                repo=repo,
                                settings=eff,
                                user_prompt=pass_prompt,
                                tracking_snapshot_text=snap_text,
                                web_context=web_context,
                                web_search_context=web_search_context,
                                max_tokens_override=bumped,
                                usage_cb=usage_cb,
                            )
                        else:
                            raise
                    if planned_part is None:
                        raise RuntimeError("LLM is not configured")
                    part_plan, part_warnings = planned_part
                    merged_warnings.extend(list(part_warnings or []))
                    for a in (part_plan.get("actions") or [])[:2000]:
                        if isinstance(a, dict):
                            merged_actions.append(a)

                    passes_used += 1
                    if axis_next <= axis_i and q_next <= q_i and kw_next <= kw_i:
                        break
                    axis_i, kw_i, q_i = axis_next, kw_next, q_next

                # De-dup actions while preserving order; keep the last topic.upsert per name.
                final_actions: list[dict[str, Any]] = []
                seen_action_keys: set[str] = set()
                topic_upsert_index: dict[str, int] = {}
                for a in merged_actions:
                    op = str(a.get("op") or "").strip()
                    if op == "topic.upsert":
                        name = str(a.get("name") or "").strip()
                        if name:
                            if name in topic_upsert_index:
                                final_actions[topic_upsert_index[name]] = a
                            else:
                                topic_upsert_index[name] = len(final_actions)
                                final_actions.append(a)
                            continue
                    try:
                        k = json.dumps(a, ensure_ascii=False, sort_keys=True)
                    except Exception:
                        k = str(a)
                    if k in seen_action_keys:
                        continue
                    seen_action_keys.add(k)
                    final_actions.append(a)

                plan, plan_warnings = validate_ai_setup_plan({"actions": final_actions})
                warnings = list(merged_warnings or []) + list(plan_warnings or [])
                warnings.append(f"planned in multiple passes: {passes_used}")
            else:
                planned = None
                retry_note = ""
                try:
                    planned = await llm_plan_tracking_ai_setup(
                        repo=repo,
                        settings=eff,
                        user_prompt=want_for_planner,
                        tracking_snapshot_text=snap_text,
                        web_context=web_context,
                        web_search_context=web_search_context,
                        max_tokens_override=planner_max_tokens,
                        usage_cb=usage_cb,
                    )
                except Exception as exc:
                    # Smart Config must work for arbitrary operator input. When planning times out,
                    # fall back to a deterministic expansion from a transformed structured brief.
                    #
                    # This keeps the plan auditable (preview diff) and avoids hard 500s for non-profile input.
                    if isinstance(exc, httpx.TimeoutException):
                        try:
                            from tracker.llm import llm_transform_tracking_ai_setup_input

                            info = transform_info if isinstance(transform_info, dict) else None
                            if not info:
                                info = await llm_transform_tracking_ai_setup_input(
                                    repo=repo,
                                    settings=eff,
                                    user_prompt_chunk=want,
                                    usage_cb=usage_cb,
                                )
                            if not isinstance(info, dict):
                                info = {}

                            understanding3 = str(info.get("understanding") or "").strip()
                            axes3_raw = info.get("interest_axes") if isinstance(info.get("interest_axes"), list) else []
                            kw3_raw = info.get("keywords") if isinstance(info.get("keywords"), list) else []
                            q3_raw = info.get("seed_queries") if isinstance(info.get("seed_queries"), list) else []

                            def _clean_list(items: object, *, max_items: int, max_len: int) -> list[str]:
                                if not isinstance(items, list):
                                    return []
                                out2: list[str] = []
                                seen2: set[str] = set()
                                for x in items:
                                    s = " ".join(str(x or "").split()).strip()
                                    if not s:
                                        continue
                                    if len(s) > max_len:
                                        s = s[:max_len].rstrip()
                                    key = s.lower()
                                    if key in seen2:
                                        continue
                                    seen2.add(key)
                                    out2.append(s)
                                    if len(out2) >= max_items:
                                        break
                                return out2

                            axes3 = _clean_list(axes3_raw, max_items=120, max_len=220)
                            kw3 = _clean_list(kw3_raw, max_items=400, max_len=100)
                            q3 = _clean_list(q3_raw, max_items=200, max_len=260)

                            # Ensure the structured brief is non-empty so `autofix` can expand.
                            if not axes3:
                                w0 = (want or "").strip()
                                if w0:
                                    axes3 = [(w0[:160].rstrip() + "…") if len(w0) > 160 else w0]
                                else:
                                    axes3 = []
                            if not q3:
                                w0 = (want or "").strip()
                                if w0:
                                    q3 = [(w0[:180].rstrip() + "…") if len(w0) > 180 else w0]
                                else:
                                    q3 = []

                            lines2: list[str] = []
                            lines2.append("SMART_CONFIG_INPUT (transformed after planner timeout):")
                            if understanding3:
                                lines2.append("")
                                lines2.append("UNDERSTANDING:")
                                lines2.append(understanding3)
                            if axes3:
                                lines2.append("")
                                lines2.append("INTEREST_AXES:")
                                for a in axes3[:200]:
                                    lines2.append(f"- {a}")
                            if kw3:
                                lines2.append("")
                                lines2.append("KEYWORDS:")
                                for k in kw3[:400]:
                                    lines2.append(f"- {k}")
                            if q3:
                                lines2.append("")
                                lines2.append("SEED_QUERIES:")
                                for q in q3[:400]:
                                    lines2.append(f"- {q}")
                            lines2.append("")
                            lines2.append("REQUIREMENTS:")
                            lines2.append("- Expand sources as much as possible; do not be conservative.")
                            lines2.append("- Split into semantically-orthogonal topics; no preset topic count.")
                            lines2.append("- Generate many short, semantically-orthogonal search seeds; do NOT stuff all keywords into one query.")
                            lines2.append("- Do NOT omit sources because you judge them sensitive/gray/harmful; final judgement is human.")

                            want_for_planner = "\n".join(lines2).strip()
                            transform_info = {
                                "understanding": understanding3,
                                "interest_axes": list(axes3),
                                "keywords": list(kw3),
                                "seed_queries": list(q3),
                            }

                            plan = _fallback_profile_seed_plan(profile_topic_name=profile_topic_name)
                            warnings = [f"planner: {type(exc).__name__}; expanded deterministically (transformed brief)"]
                            planned = None
                        except Exception as exc2:
                            plan = _fallback_profile_seed_plan(profile_topic_name=profile_topic_name)
                            warnings = [f"planner: {type(exc).__name__}; deterministic fallback failed: {str(exc2)[:200]}"]
                            planned = None
                    else:
                        bumped = None
                        try:
                            if planner_max_tokens:
                                bumped = int(
                                    min(base_tokens, max(planner_max_tokens + 2000, int(planner_max_tokens * 1.8)))
                                )
                        except Exception:
                            bumped = None
                        if bumped and planner_max_tokens and bumped != planner_max_tokens:
                            retry_note = f"retry: increased planner budget: {planner_max_tokens} -> {bumped} (after error: {str(exc)[:160]})"
                            planned = await llm_plan_tracking_ai_setup(
                                repo=repo,
                                settings=eff,
                                user_prompt=want_for_planner,
                                tracking_snapshot_text=snap_text,
                                web_context=web_context,
                                web_search_context=web_search_context,
                                max_tokens_override=bumped,
                                usage_cb=usage_cb,
                            )
                        else:
                            raise
                if planned is not None:
                    plan, warnings = planned
                    if retry_note:
                        warnings = list(warnings or [])
                        warnings.append(retry_note)
            if input_warnings:
                warnings = list(warnings or [])
                warnings.extend([w for w in input_warnings if w])
            try:
                from tracker.config_agent import autofix_ai_setup_plan_for_source_expansion

                plan2, more = autofix_ai_setup_plan_for_source_expansion(
                    snapshot_before=snap_before,
                    plan=plan,
                    # Use the structured brief (when available) so autofix can expand topics/seeds
                    # deterministically from INTEREST_AXES/RETRIEVAL_QUERIES, without relying on LLM.
                    user_prompt=(want_for_planner if _looks_like_profile_brief(want_for_planner) else want),
                    searxng_base_url=str(getattr(eff, "searxng_base_url", "") or ""),
                    profile_topic_name=profile_topic_name,
                )
                plan = plan2
                if more:
                    warnings.extend([w for w in more if w])
            except Exception:
                pass
            snap_preview = apply_plan_to_snapshot(snapshot=snap_before, plan=plan)
            preview_md = diff_tracking_snapshots(before=snap_before, after=snap_preview)
        except Exception as exc:
            msg = str(exc) or f"{type(exc).__name__}"
            try:
                logger.exception("ai-setup plan failed: %s", msg)
            except Exception:
                pass
            try:
                repo.add_config_agent_run(
                    kind="tracking_ai_setup",
                    status="failed",
                    actor=_audit_actor(request),
                    client_host=(request.client.host if request.client else ""),
                    user_prompt=want,
                    error=str(msg)[:2000],
                )
            except Exception:
                pass
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "plan_failed", "message": msg},
            )

        try:
            run = repo.add_config_agent_run(
                kind="tracking_ai_setup",
                status="planned",
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
                user_prompt=want,
                plan_json=json.dumps(plan, ensure_ascii=False),
                preview_markdown=preview_md,
                snapshot_before_json=json.dumps(snap_before, ensure_ascii=False),
                snapshot_preview_json=json.dumps(snap_preview, ensure_ascii=False),
                snapshot_after_json="",
                error="",
            )
        except Exception:
            run = None

        # UX: start discovering candidate sources as soon as a plan is generated.
        #
        # This does NOT apply the plan (topics/sources/bindings). It only creates reviewable
        # SourceCandidate rows so operators can inspect/accept/ignore before applying.
        discover_queued = False
        try:
            if run and int(getattr(run, "id", 0) or 0) > 0:
                from tracker.ai_setup_discover_queue import enqueue_ai_setup_discover_job, record_ai_setup_discover_status
                from tracker.models import Topic

                # Ensure any new topics in the plan exist (disabled) so candidates can be attached.
                upsert_spec_by_name: dict[str, dict[str, Any]] = {}
                for a in (plan.get("actions") or [])[:400]:
                    if not isinstance(a, dict):
                        continue
                    if str(a.get("op") or "").strip() != "topic.upsert":
                        continue
                    name = str(a.get("name") or a.get("topic") or "").strip()
                    if not name:
                        continue
                    upsert_spec_by_name[name] = a

                topic_ids: list[int] = []
                for name in _ai_setup_extract_topic_names(plan):
                    trow = repo.get_topic_by_name(name)
                    if not trow:
                        spec = upsert_spec_by_name.get(name) or {}
                        query = str(spec.get("query") or "").strip() or name
                        digest_cron = str(spec.get("digest_cron") or "0 9 * * *").strip() or "0 9 * * *"
                        alert_keywords = str(spec.get("alert_keywords") or "").strip()
                        try:
                            tnew = Topic(name=name, query=query, enabled=False, digest_cron=digest_cron)
                            tnew.alert_keywords = alert_keywords
                            session.add(tnew)
                            session.commit()
                            trow = tnew
                        except Exception:
                            try:
                                session.rollback()
                            except Exception:
                                pass
                            trow = repo.get_topic_by_name(name)
                    if trow and getattr(trow, "id", None) is not None:
                        topic_ids.append(int(trow.id))

                topic_ids = list(dict.fromkeys([int(x) for x in topic_ids if int(x or 0) > 0]))[:200]
                if topic_ids:
                    discover_queued = bool(
                        enqueue_ai_setup_discover_job(repo=repo, run_id=int(run.id), topic_ids=topic_ids)
                    )
                    # Even if the queue entry already existed, record a "queued" status so the UI
                    # can show progress without manual refresh.
                    try:
                        record_ai_setup_discover_status(
                            repo=repo,
                            run_id=int(run.id),
                            ok=False,
                            queued=True,
                            running=False,
                            error="",
                            per_topic=[],
                        )
                    except Exception:
                        pass
        except Exception:
            discover_queued = False

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "run_id": int(run.id) if run else 0,
                "plan": plan,
                "warnings": warnings,
                "preview_markdown": preview_md,
                "discover_queued": bool(discover_queued),
            },
        )

    def _ai_setup_extract_topic_names(plan: dict[str, Any]) -> list[str]:
        """
        Extract topic names touched by an AI Setup plan.

        This is best-effort and intentionally conservative (bounded to the plan schema).
        """
        out: set[str] = set()
        for a in (plan.get("actions") or [])[:400]:
            if not isinstance(a, dict):
                continue
            op = str(a.get("op") or "").strip()
            if op in {"topic.upsert", "topic.disable"}:
                name = str(a.get("name") or a.get("topic") or "").strip()
                if name:
                    out.add(name)
                continue
            if op in {"binding.remove", "binding.set_filters"}:
                name = str(a.get("topic") or "").strip()
                if name:
                    out.add(name)
                continue
            bind = a.get("bind")
            if isinstance(bind, dict):
                name = str(bind.get("topic") or "").strip()
                if name:
                    out.add(name)
        return [s for s in sorted(out) if s]

    def _ai_setup_extract_topic_ids(repo: Repo, plan: dict[str, Any]) -> list[int]:
        ids: list[int] = []
        for name in _ai_setup_extract_topic_names(plan):
            t = repo.get_topic_by_name(name)
            if not t or t.id is None:
                continue
            if not bool(getattr(t, "enabled", True)):
                continue
            ids.append(int(t.id))
        # De-dup, preserve order.
        return list(dict.fromkeys(ids))

    @app.post(
        "/admin/ai-setup/apply",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_apply(
        request: Request,
        run_id: int = Form(...),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        row = repo.get_config_agent_run(int(run_id))
        if not row or (row.kind or "").strip() != "tracking_ai_setup":
            return JSONResponse(status_code=404, content={"ok": False, "error": "not_found", "message": "run not found"})
        if (row.status or "").strip() == "applied":
            return JSONResponse(status_code=200, content={"ok": True, "run_id": int(row.id), "notes": [], "message": "already applied"})
        if not (row.plan_json or "").strip():
            return JSONResponse(status_code=400, content={"ok": False, "error": "missing_plan", "message": "missing plan_json"})

        try:
            obj = json.loads(row.plan_json or "")
        except Exception as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": "bad_plan_json", "message": str(exc)})

        try:
            from tracker.config_agent import apply_plan_to_db, export_tracking_snapshot, validate_ai_setup_plan

            plan, warnings = validate_ai_setup_plan(obj)
            notes = apply_plan_to_db(session=session, plan=plan)
            snap_after = export_tracking_snapshot(session=session)
        except Exception as exc:
            try:
                row.status = "failed"
                row.error = str(exc)[:2000]
                session.commit()
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"ok": False, "error": "apply_failed", "message": str(exc)})

        try:
            row.status = "applied"
            row.snapshot_after_json = json.dumps(snap_after, ensure_ascii=False)
            session.commit()
        except Exception:
            pass

        # Default UX: Smart Config implies LLM curation for touched topics (mode=llm).
        try:
            from tracker.models import TopicPolicy

            for name in _ai_setup_extract_topic_names(plan):
                trow = repo.get_topic_by_name(name)
                if not trow or getattr(trow, "id", None) is None:
                    continue
                pol = repo.get_topic_policy(topic_id=int(trow.id))
                if not pol:
                    pol = TopicPolicy(topic_id=int(trow.id))
                    session.add(pol)
                    session.flush()
                pol.llm_curation_enabled = True
            session.commit()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass

        # UX: enqueue discover-sources (async) for topics touched by this plan.
        #
        # IMPORTANT: never run discovery synchronously in this request (it can take minutes and
        # makes the Web Admin look "stuck" on "Saving…"). The scheduler worker will drain the queue.
        discover = {"ok": False, "busy": False, "queued": False, "error": "", "per_topic": []}
        try:
            topic_ids = _ai_setup_extract_topic_ids(repo, plan)
            if topic_ids:
                from tracker.ai_setup_discover_queue import enqueue_ai_setup_discover_job, record_ai_setup_discover_status

                _ = enqueue_ai_setup_discover_job(repo=repo, run_id=int(row.id), topic_ids=topic_ids)
                try:
                    # Record "queued" even if it was a merge/no-op, so the UI can auto-poll.
                    record_ai_setup_discover_status(
                        repo=repo,
                        run_id=int(row.id),
                        ok=False,
                        queued=True,
                        running=False,
                        error="",
                        per_topic=[],
                    )
                except Exception:
                    pass
                discover = {"ok": False, "busy": False, "queued": True, "error": "", "per_topic": []}
        except Exception as exc:
            discover = {"ok": False, "busy": False, "queued": False, "error": str(exc), "per_topic": []}

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "run_id": int(row.id),
                "notes": notes,
                "warnings": warnings,
                "touched_topics": _ai_setup_extract_topic_names(plan),
                "discover": discover,
            },
        )

    @app.get(
        "/admin/ai-setup/run",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_run(
        request: Request,
        run_id: int,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        row = repo.get_config_agent_run(int(run_id))
        if not row or (row.kind or "").strip() != "tracking_ai_setup":
            return JSONResponse(status_code=404, content={"ok": False, "error": "not_found", "message": "run not found"})

        warnings: list[str] = []
        try:
            if (row.plan_json or "").strip():
                from tracker.config_agent import validate_ai_setup_plan

                obj = json.loads(row.plan_json or "")
                _plan, warnings = validate_ai_setup_plan(obj)
        except Exception:
            warnings = []

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "run_id": int(getattr(row, "id", 0) or 0),
                "status": str(getattr(row, "status", "") or ""),
                "user_prompt": str(getattr(row, "user_prompt", "") or ""),
                "preview_markdown": str(getattr(row, "preview_markdown", "") or ""),
                "error": str(getattr(row, "error", "") or ""),
                "warnings": warnings,
            },
        )

    @app.get(
        "/admin/ai-setup/candidates",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_candidates(
        request: Request,
        run_id: int,
        status: str = "new",
        limit: int = 200,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        row = repo.get_config_agent_run(int(run_id))
        if not row or (row.kind or "").strip() != "tracking_ai_setup":
            return JSONResponse(status_code=404, content={"ok": False, "error": "not_found", "message": "run not found"})
        try:
            obj = json.loads(row.plan_json or "")
        except Exception:
            obj = {}
        try:
            from tracker.config_agent import validate_ai_setup_plan

            plan, _warnings = validate_ai_setup_plan(obj)
        except Exception:
            plan = {}

        # Use effective settings so DB-backed domain policies are reflected.
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings
        try:
            from tracker.domain_quality import build_domain_quality_policy

            domain_policy = build_domain_quality_policy(settings=eff)
        except Exception:
            domain_policy = None

        topics_out: list[dict[str, Any]] = []
        total = 0
        for name in _ai_setup_extract_topic_names(plan):
            trow = repo.get_topic_by_name(name)
            if not trow:
                continue
            rows = repo.list_source_candidates(topic=trow, status=status, limit=max(1, min(500, int(limit or 200))))
            cands: list[dict[str, Any]] = []
            for cand, _topic in rows:
                cid = int(getattr(cand, "id", 0) or 0)
                url = str(getattr(cand, "url", "") or "")
                discovered_from_url = str(getattr(cand, "discovered_from_url", "") or "")
                host = ""
                try:
                    host = (urlsplit(url).netloc or "").strip()
                except Exception:
                    host = ""
                tier = "unknown"
                try:
                    if domain_policy:
                        tier = str(domain_policy.tier_for_url(url) or "unknown")
                except Exception:
                    tier = "unknown"
                ev = None
                try:
                    ev = repo.get_source_candidate_eval(candidate_id=cid)
                except Exception:
                    ev = None
                cands.append(
                    {
                        "candidate_id": cid,
                        "topic": str(getattr(trow, "name", "") or ""),
                        "url": url,
                        "host": host,
                        "tier": tier,
                        "score": int(getattr(ev, "score", 0) or 0) if ev else 0,
                        "quality_score": int(getattr(ev, "quality_score", 0) or 0) if ev else 0,
                        "relevance_score": int(getattr(ev, "relevance_score", 0) or 0) if ev else 0,
                        "novelty_score": int(getattr(ev, "novelty_score", 0) or 0) if ev else 0,
                        "eval_decision": str(getattr(ev, "decision", "") or "") if ev else "",
                        "eval_why": str(getattr(ev, "why", "") or "") if ev else "",
                        "title": str(getattr(cand, "title", "") or ""),
                        "discovered_from_url": discovered_from_url,
                        "status": str(getattr(cand, "status", "") or ""),
                        "seen_count": int(getattr(cand, "seen_count", 0) or 0),
                        "last_seen_at": (
                            getattr(cand, "last_seen_at").isoformat() if getattr(cand, "last_seen_at", None) else ""
                        ),
                    }
                )
            total += len(cands)
            topics_out.append({"topic": str(getattr(trow, "name", "") or ""), "candidates": cands})

        discover_info: dict[str, Any] = {"queue_len": 0, "queued_for_run": False, "last": {}}
        try:
            # Queue info.
            q_raw = (repo.get_app_config("tracking_ai_setup_discover_queue_json") or "").strip()
            q_obj: object = json.loads(q_raw) if q_raw else {}
            queue: list[object] = []
            if isinstance(q_obj, list):
                queue = q_obj
            elif isinstance(q_obj, dict):
                q0 = q_obj.get("queue")
                if isinstance(q0, list):
                    queue = q0
            discover_info["queue_len"] = len(queue)
            rid = int(row.id)
            for it in queue[:200]:
                if not isinstance(it, dict):
                    continue
                try:
                    if int(it.get("run_id") or 0) == rid:
                        discover_info["queued_for_run"] = True
                        break
                except Exception:
                    continue
        except Exception:
            pass
        try:
            st_raw = (repo.get_app_config("tracking_ai_setup_discover_last_json") or "").strip()
            st_obj: object = json.loads(st_raw) if st_raw else {}
            if isinstance(st_obj, dict):
                discover_info["last"] = st_obj
        except Exception:
            pass

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "run_id": int(row.id),
                "run_status": str(getattr(row, "status", "") or ""),
                "total": total,
                "topics": topics_out,
                "discover": discover_info,
            },
        )

    @app.post(
        "/admin/ai-setup/notify-settings",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_notify_settings_update(
        request: Request,
        body: AiSetupCandidateNotifySettingsUpdate,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        updates: dict[str, str] = {}
        if body.telegram_enabled is not None:
            updates["ai_setup_candidates_notify_telegram_enabled"] = "true" if bool(body.telegram_enabled) else "false"
        if body.batch_size is not None:
            v = int(body.batch_size or 0)
            v = max(1, min(500, v))
            updates["ai_setup_candidates_notify_batch_size"] = str(v)
        if updates:
            try:
                repo.set_app_config_many(updates)
            except Exception:
                pass

        try:
            raw_on = (repo.get_app_config("ai_setup_candidates_notify_telegram_enabled") or "").strip().lower()
            enabled = False if raw_on in {"0", "false", "off", "no"} else True
        except Exception:
            enabled = True
        # UX: when auto-accept is enabled, candidate notifications are suppressed and the toggle is disabled in UI.
        # Mirror that here so the UI can't show "on" while behavior is "off".
        try:
            raw_aa = (repo.get_app_config("discover_sources_auto_accept_enabled") or "").strip().lower()
            aa_on = False if raw_aa in {"0", "false", "off", "no"} else True
        except Exception:
            aa_on = bool(getattr(settings, "discover_sources_auto_accept_enabled", True))
        if aa_on:
            enabled = False
        try:
            raw_bs = (repo.get_app_config("ai_setup_candidates_notify_batch_size") or "").strip()
            batch = int(raw_bs or 10)
        except Exception:
            batch = 10
        batch = max(1, min(500, int(batch or 10)))

        return {"ok": True, "telegram_enabled": bool(enabled), "batch_size": int(batch)}

    @app.post(
        "/admin/ai-setup/discover-controls",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_discover_controls_update(
        request: Request,
        body: AiSetupDiscoverControlsUpdate,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        updates: dict[str, str] = {}
        if body.discovery_enabled is not None:
            updates["discover_sources_enabled"] = "true" if bool(body.discovery_enabled) else "false"
        if body.explore_weight is not None:
            ew = max(0, min(10, int(body.explore_weight or 0)))
            xw = max(0, 10 - int(ew))
            updates["discover_sources_explore_weight"] = str(int(ew))
            updates["discover_sources_exploit_weight"] = str(int(xw))
        if body.auto_accept_enabled is not None:
            aa_on = bool(body.auto_accept_enabled)
            updates["discover_sources_auto_accept_enabled"] = "true" if aa_on else "false"
            # UX: when auto-accept is enabled, suppress candidate batch notifications by default.
            if aa_on:
                updates["ai_setup_candidates_notify_telegram_enabled"] = "false"
        if body.min_source_score is not None:
            v = max(0, min(100, int(body.min_source_score or 0)))
            updates["source_quality_min_score"] = str(int(v))
        if body.max_sources_total is not None:
            v2 = int(body.max_sources_total or 0)
            v2 = max(50, min(5000, v2))
            updates["discover_sources_max_sources_total"] = str(int(v2))
        if updates:
            try:
                repo.set_app_config_many(updates)
            except Exception:
                pass

        # Read back effective values.
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings
        try:
            enabled = bool(getattr(eff, "discover_sources_enabled", True))
        except Exception:
            enabled = True
        try:
            ew2 = int(getattr(eff, "discover_sources_explore_weight", 2) or 2)
        except Exception:
            ew2 = 2
        ew2 = max(0, min(10, int(ew2 or 2)))
        xw2 = max(0, 10 - int(ew2))
        try:
            aa = bool(getattr(eff, "discover_sources_auto_accept_enabled", True))
        except Exception:
            aa = True
        try:
            ms = int(getattr(eff, "source_quality_min_score", 50))
        except Exception:
            ms = 50
        ms = max(0, min(100, int(ms)))
        try:
            mx = int(getattr(eff, "discover_sources_max_sources_total", 500) or 500)
        except Exception:
            mx = 500
        mx = max(50, min(5000, int(mx)))

        return {
            "ok": True,
            "discover_sources_enabled": bool(enabled),
            "explore_weight": int(ew2),
            "exploit_weight": int(xw2),
            "auto_accept_enabled": bool(aa),
            "min_source_score": int(ms),
            "max_sources_total": int(mx),
        }

    @app.post(
        "/admin/ai-setup/candidates/accept",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_candidates_accept(
        request: Request,
        body: CandidateBulkActionRequest,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        ids = [int(x) for x in (body.candidate_ids or []) if int(x or 0) > 0]
        ids = list(dict.fromkeys(ids))[:800]
        if not ids:
            return JSONResponse(status_code=400, content={"ok": False, "error": "missing_ids", "message": "missing candidate_ids"})

        # Group by topic for fewer commits.
        from tracker.actions import SourceBindingSpec, create_rss_sources_bulk as create_rss_sources_bulk_action
        from tracker.models import Topic, TopicPolicy

        by_topic: dict[int, list[object]] = {}
        skipped = 0
        for cid in ids:
            cand = repo.get_source_candidate_by_id(cid)
            if not cand:
                skipped += 1
                continue
            if str(getattr(cand, "source_type", "") or "").strip() != "rss":
                skipped += 1
                continue
            if str(getattr(cand, "status", "") or "").strip() == "ignored":
                skipped += 1
                continue
            tid = int(getattr(cand, "topic_id", 0) or 0)
            if tid <= 0:
                skipped += 1
                continue
            by_topic.setdefault(tid, []).append(cand)

        created_total = 0
        bound_total = 0
        accepted = 0
        for tid, cands in by_topic.items():
            topic = session.get(Topic, tid)
            if not topic:
                skipped += len(cands)
                continue
            urls = [str(getattr(c, "url", "") or "") for c in cands if str(getattr(c, "url", "") or "").strip()]
            if not urls:
                skipped += len(cands)
                continue
            created, bound = create_rss_sources_bulk_action(
                session=session,
                urls=urls,
                bind=SourceBindingSpec(topic=str(getattr(topic, "name", "") or "")),
            )
            created_total += int(created or 0)
            bound_total += int(bound or 0)

            # Default UX: accepting candidates implies "LLM mode" for this topic.
            #
            # Note: mode is per-topic (TopicPolicy), not per binding. Global TRACKER_LLM_CURATION_ENABLED
            # still gates whether LLM curation actually runs.
            try:
                pol = repo.get_topic_policy(topic_id=int(topic.id))
                if not pol:
                    pol = TopicPolicy(topic_id=int(topic.id))
                    session.add(pol)
                    session.flush()
                pol.llm_curation_enabled = True
            except Exception:
                pass

            for c in cands:
                if str(getattr(c, "status", "") or "").strip() == "accepted":
                    continue
                setattr(c, "status", "accepted")
                accepted += 1

        session.commit()

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "accepted": int(accepted),
                "skipped": int(skipped),
                "sources_created": int(created_total),
                "bindings_created": int(bound_total),
            },
        )

    @app.post(
        "/admin/ai-setup/candidates/ignore",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_candidates_ignore(
        request: Request,
        body: CandidateBulkActionRequest,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        ids = [int(x) for x in (body.candidate_ids or []) if int(x or 0) > 0]
        ids = list(dict.fromkeys(ids))[:1200]
        if not ids:
            return JSONResponse(status_code=400, content={"ok": False, "error": "missing_ids", "message": "missing candidate_ids"})

        ignored = 0
        skipped = 0
        for cid in ids:
            cand = repo.get_source_candidate_by_id(cid)
            if not cand:
                skipped += 1
                continue
            if str(getattr(cand, "status", "") or "").strip() == "ignored":
                skipped += 1
                continue
            setattr(cand, "status", "ignored")
            ignored += 1

        session.commit()
        return JSONResponse(status_code=200, content={"ok": True, "ignored": int(ignored), "skipped": int(skipped)})

    @app.post(
        "/admin/ai-setup/undo",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_undo(
        request: Request,
        run_id: int | None = Form(None),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        target = None
        if run_id:
            target = repo.get_config_agent_run(int(run_id))
        else:
            for r in repo.list_config_agent_runs(kind="tracking_ai_setup", limit=50):
                if (r.status or "").strip() == "applied":
                    target = r
                    break
        if not target or (target.kind or "").strip() != "tracking_ai_setup":
            return JSONResponse(status_code=404, content={"ok": False, "error": "not_found", "message": "no applied run found"})

        try:
            snap_restore = json.loads(target.snapshot_before_json or "")
            if not isinstance(snap_restore, dict):
                raise ValueError("snapshot_before_json is not an object")
        except Exception as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": "bad_snapshot", "message": str(exc)})

        try:
            from tracker.config_agent import diff_tracking_snapshots, export_tracking_snapshot, restore_tracking_snapshot_to_db

            snap_before = export_tracking_snapshot(session=session)
            notes = restore_tracking_snapshot_to_db(session=session, snapshot=snap_restore)
            snap_after = export_tracking_snapshot(session=session)
            preview_md = diff_tracking_snapshots(before=snap_before, after=snap_after)
        except Exception as exc:
            return JSONResponse(status_code=500, content={"ok": False, "error": "undo_failed", "message": str(exc)})

        try:
            repo.add_config_agent_run(
                kind="tracking_ai_setup",
                status="undone",
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
                user_prompt=f"UNDO run_id={int(target.id)}",
                plan_json="",
                preview_markdown=preview_md,
                snapshot_before_json=json.dumps(snap_before, ensure_ascii=False),
                snapshot_preview_json="",
                snapshot_after_json=json.dumps(snap_after, ensure_ascii=False),
                error="",
            )
        except Exception:
            pass

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "target_run_id": int(target.id),
                "notes": notes,
                "preview_markdown": preview_md,
            },
        )

    @app.post(
        "/admin/ai-setup/baseline/set",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_baseline_set(
        request: Request,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            from tracker.config_agent import export_tracking_snapshot, save_baseline_snapshot

            snap = export_tracking_snapshot(session=session)
            save_baseline_snapshot(repo, snap)
        except Exception as exc:
            return JSONResponse(status_code=500, content={"ok": False, "error": "baseline_set_failed", "message": str(exc)})

        try:
            repo.add_config_agent_run(
                kind="tracking_ai_setup",
                status="baseline_set",
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
                user_prompt="SET_BASELINE",
                plan_json="",
                preview_markdown="baseline snapshot captured",
                snapshot_before_json="",
                snapshot_preview_json="",
                snapshot_after_json=json.dumps(snap, ensure_ascii=False),
                error="",
            )
        except Exception:
            pass

        return JSONResponse(status_code=200, content={"ok": True})

    @app.post(
        "/admin/ai-setup/baseline/restore",
        dependencies=[Depends(auth_dep)],
        include_in_schema=False,
    )
    def admin_ai_setup_baseline_restore(
        request: Request,
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            from tracker.config_agent import load_baseline_snapshot

            base = load_baseline_snapshot(repo)
        except Exception:
            base = None
        if not base:
            return JSONResponse(status_code=400, content={"ok": False, "error": "baseline_missing", "message": "baseline snapshot not set"})

        try:
            from tracker.config_agent import diff_tracking_snapshots, export_tracking_snapshot, restore_tracking_snapshot_to_db

            snap_before = export_tracking_snapshot(session=session)
            notes = restore_tracking_snapshot_to_db(session=session, snapshot=base)
            snap_after = export_tracking_snapshot(session=session)
            preview_md = diff_tracking_snapshots(before=snap_before, after=snap_after)
        except Exception as exc:
            return JSONResponse(status_code=500, content={"ok": False, "error": "restore_failed", "message": str(exc)})

        try:
            repo.add_config_agent_run(
                kind="tracking_ai_setup",
                status="restored",
                actor=_audit_actor(request),
                client_host=(request.client.host if request.client else ""),
                user_prompt="RESTORE_BASELINE",
                plan_json="",
                preview_markdown=preview_md,
                snapshot_before_json=json.dumps(snap_before, ensure_ascii=False),
                snapshot_preview_json="",
                snapshot_after_json=json.dumps(snap_after, ensure_ascii=False),
                error="",
            )
        except Exception:
            pass

        return JSONResponse(status_code=200, content={"ok": True, "notes": notes, "preview_markdown": preview_md})

    def _redir_setup(token: str | None, *, msg: str | None = None):
        params: dict[str, str] = {}
        if token:
            params["token"] = token
        if msg:
            params["msg"] = msg
        qs = urlencode(params) if params else ""
        url = "/setup/push" + (f"?{qs}" if qs else "")
        return RedirectResponse(url=url, status_code=303)

    def _redir_setup_profile(token: str | None, *, msg: str | None = None):
        params: dict[str, str] = {}
        if token:
            params["token"] = token
        if msg:
            params["msg"] = msg
        qs = urlencode(params) if params else ""
        url = "/setup/profile" + (f"?{qs}" if qs else "")
        return RedirectResponse(url=url, status_code=303)

    def _redir_setup_topic(token: str | None, *, msg: str | None = None):
        params: dict[str, str] = {}
        if token:
            params["token"] = token
        if msg:
            params["msg"] = msg
        qs = urlencode(params) if params else ""
        url = "/setup/topic" + (f"?{qs}" if qs else "")
        return RedirectResponse(url=url, status_code=303)

    @app.get("/setup/wizard", response_class=HTMLResponse, dependencies=[Depends(auth_dep)])
    def setup_wizard(request: Request, session: Session = Depends(get_db)):
        repo = Repo(session)
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        lang = get_request_lang(request)
        _seed_locale_defaults(repo=repo, request_lang=lang)
        msg = request.query_params.get("msg")

        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        try:
            stats = repo.get_stats()
            from tracker.doctor import build_doctor_report

            doctor_report = build_doctor_report(
                settings=eff,
                stats=stats,
                db_ok=True,
                db_error=None,
                profile_configured=bool(repo.get_app_config("profile_text")),
                telegram_chat_configured=bool(repo.get_app_config("telegram_chat_id")),
                activity=repo.get_activity_snapshot(),
            )
        except Exception:
            doctor_report = None

        try:
            admin_user = str(getattr(eff, "admin_username", "") or "").strip()
            admin_pw_set = bool(str(getattr(eff, "admin_password", "") or "").strip())

            def _app_bool(key: str) -> bool:
                try:
                    v = str(repo.get_app_config(key) or "").strip().lower()
                except Exception:
                    v = ""
                return v in {"1", "true", "yes", "y", "on"}

            def _app_str(key: str) -> str:
                try:
                    return str(repo.get_app_config(key) or "").strip()
                except Exception:
                    return ""

            llm_reasoning_base_url = str(getattr(eff, "llm_base_url", "") or "").strip()
            llm_reasoning_model = str((getattr(eff, "llm_model_reasoning", "") or getattr(eff, "llm_model", "") or "")).strip()
            llm_test_reasoning_fingerprint_current = f"{llm_reasoning_base_url}|{llm_reasoning_model}".strip("|")
            llm_test_reasoning_ok = bool(
                _app_bool("llm_test_reasoning_last_ok")
                and llm_test_reasoning_fingerprint_current
                and _app_str("llm_test_reasoning_last_fingerprint") == llm_test_reasoning_fingerprint_current
            )

            llm_mini_base_url = str(getattr(eff, "llm_mini_base_url", "") or "").strip() or llm_reasoning_base_url
            llm_mini_model = str(
                (
                    getattr(eff, "llm_model_mini", "")
                    or getattr(eff, "llm_model_reasoning", "")
                    or getattr(eff, "llm_model", "")
                    or ""
                )
            ).strip()
            llm_test_mini_fingerprint_current = f"{llm_mini_base_url}|{llm_mini_model}".strip("|")
            llm_test_mini_ok = bool(
                _app_bool("llm_test_mini_last_ok")
                and llm_test_mini_fingerprint_current
                and _app_str("llm_test_mini_last_fingerprint") == llm_test_mini_fingerprint_current
            )

            settings_snapshot = {
                "output_language": str(getattr(eff, "output_language", "") or "").strip(),
                "cron_timezone": str(getattr(eff, "cron_timezone", "") or "").strip(),
                "llm_base_url": str(getattr(eff, "llm_base_url", "") or "").strip(),
                "llm_model": str(getattr(eff, "llm_model", "") or "").strip(),
                "llm_model_reasoning": str(getattr(eff, "llm_model_reasoning", "") or "").strip(),
                "llm_model_mini": str(getattr(eff, "llm_model_mini", "") or "").strip(),
                "llm_mini_base_url": str(getattr(eff, "llm_mini_base_url", "") or "").strip(),
                "llm_api_key_set": bool(str(getattr(eff, "llm_api_key", "") or "").strip()),
                "llm_mini_api_key_set": bool(str(getattr(eff, "llm_mini_api_key", "") or "").strip()),
                "llm_test_reasoning_ok": bool(llm_test_reasoning_ok),
                "llm_test_mini_ok": bool(llm_test_mini_ok),
                "health_report_cron": str(getattr(eff, "health_report_cron", "") or "").strip(),
                "priority_lane_enabled": bool(getattr(eff, "priority_lane_enabled", False)),
                "digest_scheduler_enabled": bool(getattr(eff, "digest_scheduler_enabled", False)),
                "digest_push_enabled": bool(getattr(eff, "digest_push_enabled", False)),
                "access_ok": bool(admin_user and admin_pw_set),
            }
        except Exception:
            settings_snapshot = {}

        return templates.TemplateResponse(
            request,
            "setup_wizard.html",
            {
                "token": token,
                "lang": lang,
                "msg": msg,
                "doctor_report": doctor_report,
                "settings_snapshot": settings_snapshot,
            },
        )

    @app.get("/setup/push", response_class=HTMLResponse, dependencies=[Depends(auth_dep)])
    def setup_push(request: Request, session: Session = Depends(get_db)):
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        msg = request.query_params.get("msg")
        qs = []
        if token:
            qs.append(f"token={token}")
        qs.append("section=push")
        if msg:
            from urllib.parse import quote_plus

            qs.append("msg=" + quote_plus(str(msg)))
        url = "/admin?" + "&".join(qs)
        return RedirectResponse(url=url, status_code=302)

    @app.post("/setup/push/apply", dependencies=[Depends(auth_dep)])
    def setup_push_apply(
        request: Request,
        env_block: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Interactive push setup: import a small `.env` block (DingTalk/Email/Webhook keys)
        and write it into TRACKER_ENV_PATH.
        """
        _require_localhost(request)

        from tracker.push_setup import parse_push_setup_env_block

        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        try:
            parsed = parse_push_setup_env_block(env_block)
        except ValueError as exc:
            return _redir_setup(token, msg=f"invalid env block: {exc}")

        if not parsed.updates:
            return _redir_setup(token, msg="env import: no changes")

        repo = Repo(session)
        from tracker.dynamic_config import apply_env_block_updates

        path = Path(settings.env_path or ".env")
        res = apply_env_block_updates(repo=repo, settings=settings, env_path=path, env_updates=parsed.updates)

        keys = ", ".join(sorted(res.updated_env_keys))
        msg = f"env updated: {keys}" + (" (restart services to apply)" if res.restart_required else "")
        return _redir_setup(token, msg=msg)

    @app.get("/setup/topic", response_class=HTMLResponse, dependencies=[Depends(auth_dep)])
    def setup_topic(request: Request, session: Session = Depends(get_db)):
        repo = Repo(session)
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        lang = get_request_lang(request)
        _seed_locale_defaults(repo=repo, request_lang=lang)
        msg = request.query_params.get("msg")

        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        try:
            stats = repo.get_stats()
            from tracker.doctor import build_doctor_report

            doctor_report = build_doctor_report(
                settings=eff,
                stats=stats,
                db_ok=True,
                db_error=None,
                profile_configured=bool(repo.get_app_config("profile_text")),
                telegram_chat_configured=bool(repo.get_app_config("telegram_chat_id")),
                activity=repo.get_activity_snapshot(),
            )
        except Exception:
            doctor_report = None

        return templates.TemplateResponse(
            request,
            "setup_topic.html",
            {
                "token": token,
                "lang": lang,
                "msg": msg,
                "doctor_report": doctor_report,
                # Prompt presets (static + operator-defined).
                "topic_policy_presets": _merge_prompt_presets(
                    [asdict(p) for p in get_topic_policy_presets()],
                    (custom_topic_presets := _load_custom_prompt_presets(repo, app_config_key="topic_policy_presets_custom_json")),
                ),
                "custom_topic_policy_presets": custom_topic_presets,
            },
        )

    @app.get("/setup/profile", response_class=HTMLResponse, dependencies=[Depends(auth_dep)])
    def setup_profile(request: Request, session: Session = Depends(get_db)):
        repo = Repo(session)
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        lang = get_request_lang(request)
        _seed_locale_defaults(repo=repo, request_lang=lang)
        msg = request.query_params.get("msg")

        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        try:
            stats = repo.get_stats()
            from tracker.doctor import build_doctor_report

            doctor_report = build_doctor_report(
                settings=eff,
                stats=stats,
                db_ok=True,
                db_error=None,
                profile_configured=bool(repo.get_app_config("profile_text")),
                telegram_chat_configured=bool(repo.get_app_config("telegram_chat_id")),
                activity=repo.get_activity_snapshot(),
            )
        except Exception:
            doctor_report = None

        profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
        profile_text = repo.get_app_config("profile_text") or ""
        profile_understanding = repo.get_app_config("profile_understanding") or ""
        profile_interest_axes = repo.get_app_config("profile_interest_axes") or ""
        profile_interest_keywords = repo.get_app_config("profile_interest_keywords") or ""
        profile_retrieval_queries = repo.get_app_config("profile_retrieval_queries") or ""
        profile_prompt_core = repo.get_app_config("profile_prompt_core") or ""
        profile_prompt_delta = repo.get_app_config("profile_prompt_delta") or ""
        profile_prompt = ""
        profile_digest_cron = "0 9 * * *"
        try:
            topic = repo.get_topic_by_name(profile_topic_name)
            if topic:
                pol = repo.get_topic_policy(topic_id=topic.id)
                profile_prompt = (pol.llm_curation_prompt if pol else "") or ""
                if (topic.digest_cron or "").strip():
                    profile_digest_cron = str(topic.digest_cron).strip()
        except Exception:
            profile_prompt = ""

        custom_topic_presets = _load_custom_prompt_presets(repo, app_config_key="topic_policy_presets_custom_json")
        merged_topic_presets = _merge_prompt_presets([asdict(p) for p in get_topic_policy_presets()], custom_topic_presets)

        return templates.TemplateResponse(
            request,
            "setup_profile.html",
            {
                "token": token,
                "lang": lang,
                "msg": msg,
                "doctor_report": doctor_report,
                "profile_topic_name": profile_topic_name,
                "profile_text": profile_text,
                "profile_understanding": profile_understanding,
                "profile_interest_axes": profile_interest_axes,
                "profile_interest_keywords": profile_interest_keywords,
                "profile_retrieval_queries": profile_retrieval_queries,
                "profile_prompt_core": profile_prompt_core,
                "profile_prompt_delta": profile_prompt_delta,
                "profile_prompt": profile_prompt,
                "profile_digest_cron": profile_digest_cron,
                "topic_policy_presets": merged_topic_presets,
                "custom_topic_policy_presets": custom_topic_presets,
            },
        )

    @app.post("/setup/profile/preset/save", dependencies=[Depends(auth_dep)])
    def setup_profile_preset_save(
        preset_id: str = Form(""),
        label: str = Form(""),
        description: str = Form(""),
        prompt: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Save a custom Topic AI policy prompt preset into app_config.

        Used by `/setup/profile` to let operators create/edit presets without visiting `/admin`.
        """
        repo = Repo(session)
        pid = (preset_id or "").strip()
        if not pid:
            raise HTTPException(status_code=400, detail="missing preset_id")
        if len(pid) > 64:
            raise HTTPException(status_code=400, detail="preset_id too long")
        for ch in pid:
            if not (ch.isalnum() or ch in {"_", "-"}):
                raise HTTPException(status_code=400, detail="invalid preset_id (allowed: a-zA-Z0-9_-)")

        lab = (label or "").strip()
        if not lab:
            raise HTTPException(status_code=400, detail="missing label")
        if len(lab) > 120:
            raise HTTPException(status_code=400, detail="label too long")

        desc = (description or "").strip()
        if len(desc) > 400:
            desc = desc[:400]

        pr = (prompt or "").strip()
        if not pr:
            raise HTTPException(status_code=400, detail="missing prompt")
        if len(pr) > 20_000:
            raise HTTPException(status_code=400, detail="prompt too long")

        key = "topic_policy_presets_custom_json"
        cur = (repo.get_app_config(key) or "").strip()
        try:
            obj = json.loads(cur) if cur else []
        except Exception:
            obj = []
        if not isinstance(obj, list):
            obj = []

        out = []
        replaced = False
        for it in obj[:200]:
            if not isinstance(it, dict):
                continue
            if str(it.get("id") or "").strip() == pid:
                out.append({"id": pid, "label": lab, "description": desc, "prompt": pr})
                replaced = True
            else:
                out.append(it)
        if not replaced:
            out.append({"id": pid, "label": lab, "description": desc, "prompt": pr})

        repo.set_app_config(key, json.dumps(out, ensure_ascii=False))
        return {"ok": True, "id": pid, "replaced": replaced}

    @app.post("/setup/profile/preset/delete", dependencies=[Depends(auth_dep)])
    def setup_profile_preset_delete(
        preset_id: str = Form(""),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        pid = (preset_id or "").strip()
        if not pid:
            raise HTTPException(status_code=400, detail="missing preset_id")

        key = "topic_policy_presets_custom_json"
        cur = (repo.get_app_config(key) or "").strip()
        try:
            obj = json.loads(cur) if cur else []
        except Exception:
            obj = []
        if not isinstance(obj, list):
            obj = []

        kept = []
        removed = 0
        for it in obj[:200]:
            if not isinstance(it, dict):
                continue
            if str(it.get("id") or "").strip() == pid:
                removed += 1
                continue
            kept.append(it)
        if removed <= 0:
            raise HTTPException(status_code=404, detail=f"preset not found: {pid}")
        repo.set_app_config(key, json.dumps(kept, ensure_ascii=False))
        return {"ok": True, "id": pid}

    @app.post("/setup/profile/apply", dependencies=[Depends(auth_dep)])
    def setup_profile_apply(
        request: Request,
        name: str = Form("Profile"),
        digest_cron: str = Form("0 9 * * *"),
        save_only: bool = Form(False),
        add_hn_rss: bool = Form(False),
        add_hn_popularity: bool = Form(False),
        add_github_trending_daily: bool = Form(False),
        add_github_trending_weekly: bool = Form(False),
        add_github_trending_monthly: bool = Form(False),
        github_languages: str = Form(""),
        add_arxiv: bool = Form(False),
        arxiv_categories: str = Form(""),
        add_searxng: bool = Form(False),
        searxng_base_url: str = Form("http://127.0.0.1:8888"),
        add_discourse: bool = Form(False),
        discourse_base_url: str = Form(""),
        discourse_json_path: str = Form("/latest.json"),
        add_nodeseek: bool = Form(False),
        rss_urls: str = Form(""),
        profile_text: str = Form(""),
        profile_understanding: str = Form(""),
        profile_interest_axes: str = Form(""),
        profile_interest_keywords: str = Form(""),
        profile_retrieval_queries: str = Form(""),
        ai_prompt: str = Form(""),
        kickoff_tick_now: bool = Form(False),
        kickoff_digest_now: bool = Form(False),
        kickoff_push_digest_now: bool = Form(False),
        kickoff_digest_hours: int = Form(24),
        session: Session = Depends(get_db),
    ):
        """
        Single-profile onboarding: store profile text, seed broad “streams” (+ optional web search, no keyword matching),
        and enable AI-native curation for long-term, high-signal pushes.
        """
        from tracker.actions import (
            SourceBindingSpec,
            TopicAiPolicySpec,
            TopicSpec,
            create_discourse_source as create_discourse_source_action,
            create_html_list_source as create_html_list_source_action,
            create_rss_source as create_rss_source_action,
            create_rss_sources_bulk as create_rss_sources_bulk_action,
            create_searxng_search_source as create_searxng_search_source_action,
            create_topic as create_topic_action,
            upsert_topic_ai_policy as upsert_topic_ai_policy_action,
        )

        repo = Repo(session)
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None
        want_json = "application/json" in (request.headers.get("accept") or "")

        # Use effective settings so Config Center updates apply without restart.
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        if not save_only and not (
            getattr(eff, "llm_curation_enabled", False)
            and getattr(eff, "llm_base_url", None)
            and (getattr(eff, "llm_model_reasoning", None) or getattr(eff, "llm_model", None))
        ):
            return _redir_setup_profile(
                token,
                msg="Profile requires TRACKER_LLM_CURATION_ENABLED=true + configured LLM (TRACKER_LLM_BASE_URL + TRACKER_LLM_MODEL_REASONING or TRACKER_LLM_MODEL).",
            )

        topic_name = (name or "").strip() or "Profile"
        prompt = (ai_prompt or "").strip()
        if not prompt:
            if want_json:
                return JSONResponse(status_code=400, content={"ok": False, "error": "missing_ai_prompt"})
            return _redir_setup_profile(token, msg="missing ai_prompt (use AI Suggest first)")

        # Create or update the profile topic. Query is intentionally left empty; we do not do keyword matching.
        topic = repo.get_topic_by_name(topic_name)
        if not topic:
            try:
                topic = create_topic_action(
                    session=session,
                    spec=TopicSpec(
                        name=topic_name,
                        query="",
                        digest_cron=(digest_cron or "0 9 * * *").strip() or "0 9 * * *",
                        alert_keywords="",
                    ),
                )
            except ValueError as exc:
                return _redir_setup_profile(token, msg=str(exc))
        else:
            topic.query = ""
            topic.digest_cron = (digest_cron or "0 9 * * *").strip() or "0 9 * * *"
            session.commit()

        try:
            def _set_or_delete(key: str, value: str) -> None:
                v = (value or "").strip()
                if v:
                    repo.set_app_config(key, v)
                else:
                    repo.delete_app_config(key)

            # Persist the raw profile text for operator reference (single profile).
            from tracker.profile_input import normalize_profile_text

            txt = normalize_profile_text(text=(profile_text or ""))
            if txt:
                repo.set_app_config("profile_text", txt)
            repo.set_app_config("profile_topic_name", topic_name)
            _set_or_delete("profile_understanding", profile_understanding)
            _set_or_delete("profile_interest_axes", profile_interest_axes)
            _set_or_delete("profile_interest_keywords", profile_interest_keywords)
            _set_or_delete("profile_retrieval_queries", profile_retrieval_queries)

            # Always store the prompt so Smart Config can reuse it even if the operator skips legacy bootstrap.
            _set_or_delete("profile_prompt_core", prompt)

            if save_only:
                # Persist the prompt as the Profile topic AI policy (when the topic exists).
                try:
                    upsert_topic_ai_policy_action(
                        session=session,
                        spec=TopicAiPolicySpec(
                            topic=topic_name,
                            enabled=bool(getattr(eff, "llm_curation_enabled", False)),
                            prompt=prompt,
                        ),
                    )
                except Exception:
                    pass
                if want_json:
                    return JSONResponse(status_code=200, content={"ok": True, "saved": True})
                return _redir_setup_profile(token, msg="profile saved")

            if add_hn_rss:
                create_rss_source_action(
                    session=session,
                    url="https://news.ycombinator.com/rss",
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

            if add_hn_popularity:
                from tracker.source_packs import get_rss_pack

                pack = get_rss_pack("hn_popularity_karpathy")
                create_rss_sources_bulk_action(
                    session=session,
                    urls=pack.urls,
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                    tags="hn-popularity,karpathy",
                )

            def _parse_csv_list(value: str, *, max_items: int) -> list[str]:
                raw = (value or "").strip()
                if not raw:
                    return []
                s = raw.replace("，", ",").replace("；", ",").replace(";", ",").replace("\n", ",")
                parts = [p.strip() for p in s.split(",") if p.strip()]
                out: list[str] = []
                seen: set[str] = set()
                for p in parts:
                    if p in seen:
                        continue
                    seen.add(p)
                    out.append(p)
                    if len(out) >= max_items:
                        break
                return out

            def _parse_query_block(value: str, *, max_items: int) -> list[str]:
                raw = (value or "").strip()
                if not raw:
                    return []
                lines = [ln.strip() for ln in raw.replace("\r", "").splitlines() if ln.strip()]
                if len(lines) == 1:
                    return _parse_csv_list(lines[0], max_items=max_items)
                out: list[str] = []
                seen: set[str] = set()
                for ln in lines:
                    q = " ".join(ln.split()).strip()
                    if not q:
                        continue
                    key = q.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(q)
                    if len(out) >= max_items:
                        break
                return out

            if add_github_trending_daily or add_github_trending_weekly or add_github_trending_monthly:
                langs = _parse_csv_list(github_languages, max_items=6)

                def _add_trending(*, since: str, language: str | None = None):
                    if language:
                        from urllib.parse import quote

                        page_url = f"https://github.com/trending/{quote(language)}?since={since}"
                    else:
                        page_url = f"https://github.com/trending?since={since}"
                    create_html_list_source_action(
                        session=session,
                        page_url=page_url,
                        item_selector="article.Box-row",
                        title_selector="h2 a",
                        summary_selector="p",
                        max_items=25,
                        bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                    )

                if add_github_trending_daily:
                    _add_trending(since="daily", language=None)
                    for lang in langs:
                        _add_trending(since="daily", language=lang)
                if add_github_trending_weekly:
                    _add_trending(since="weekly", language=None)
                    for lang in langs:
                        _add_trending(since="weekly", language=lang)
                if add_github_trending_monthly:
                    _add_trending(since="monthly", language=None)
                    for lang in langs:
                        _add_trending(since="monthly", language=lang)

            if add_searxng:
                base = (searxng_base_url or "").strip() or "http://127.0.0.1:8888"
                qs = _parse_query_block(profile_retrieval_queries, max_items=6)
                for q in qs:
                    create_searxng_search_source_action(
                        session=session,
                        base_url=base,
                        query=q,
                        time_range="day",
                        results=20,
                        bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                    )

            if add_arxiv:
                cats = _parse_csv_list(arxiv_categories, max_items=10)
                if not cats:
                    cats = ["cs.AI", "cs.LG", "cs.CL", "cs.CV", "stat.ML"]
                for cat in cats:
                    c = (cat or "").strip()
                    if not c:
                        continue
                    create_rss_source_action(
                        session=session,
                        url=f"https://export.arxiv.org/rss/{c}",
                        bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                    )

            if add_discourse:
                base_url = (discourse_base_url or "").strip()
                if not base_url:
                    raise ValueError("missing discourse_base_url")
                create_discourse_source_action(
                    session=session,
                    base_url=base_url,
                    json_path=(discourse_json_path or "/latest.json").strip() or "/latest.json",
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

            if add_nodeseek:
                create_rss_source_action(
                    session=session,
                    url="https://rss.nodeseek.com/",
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

            for raw in (rss_urls or "").splitlines():
                s = raw.strip()
                if not s or s.startswith("#"):
                    continue
                create_rss_source_action(
                    session=session,
                    url=s,
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

            upsert_topic_ai_policy_action(
                session=session,
                spec=TopicAiPolicySpec(
                    topic=topic_name,
                    enabled=True,
                    prompt=prompt,
                ),
            )
        except ValueError as exc:
            session.rollback()
            return _redir_setup_profile(token, msg=str(exc))

        kickoff_extra = ""
        if kickoff_tick_now or kickoff_digest_now:
            try:
                hrs = max(1, min(168, int(kickoff_digest_hours or 24)))
                suffix = None
                if kickoff_push_digest_now:
                    import hashlib

                    sig = hashlib.sha256((txt or "").encode("utf-8")).hexdigest()[:8] if txt else ""
                    suffix = f"profile-{sig}" if sig else "kickoff-" + dt.datetime.utcnow().strftime("%H%M%S")

                with job_lock(name="jobs", timeout_seconds=0.0):
                    if kickoff_tick_now:
                        asyncio.run(run_tick(session=session, settings=settings, push=False))
                    if kickoff_digest_now:
                        digest_result = asyncio.run(
                            run_digest(
                                session=session,
                                settings=settings,
                                hours=hrs,
                                push=bool(kickoff_push_digest_now),
                                key_suffix=suffix,
                                topic_ids=[int(topic.id)],
                            )
                        )
                        pushed = sum(int(t.pushed or 0) for t in digest_result.per_topic)
                    else:
                        pushed = 0

                kickoff_extra = (
                    f"; kickoff: tick={str(bool(kickoff_tick_now)).lower()} "
                    f"digest={str(bool(kickoff_digest_now)).lower()} pushed_digest={pushed}"
                )
            except TimeoutError:
                kickoff_extra = "; kickoff failed: busy: another job is running"
            except Exception as exc:
                kickoff_extra = f"; kickoff failed: {exc}"

        return _redir_setup_profile(
            token,
            msg=f"profile ready: {topic_name}{kickoff_extra} (see /admin → Overview → Reports)",
        )

    @app.post("/setup/topic/apply", dependencies=[Depends(auth_dep)])
    def setup_topic_apply(
        request: Request,
        name: str = Form(...),
        query: str = Form(""),
        digest_cron: str = Form("0 9 * * *"),
        alert_keywords: str = Form(""),
        add_hn: bool = Form(False),
        add_searxng: bool = Form(False),
        searxng_base_url: str = Form("http://127.0.0.1:8888"),
        add_discourse: bool = Form(False),
        discourse_base_url: str = Form(""),
        discourse_json_path: str = Form("/latest.json"),
        add_nodeseek: bool = Form(False),
        add_hn_popularity: bool = Form(False),
        rss_urls: str = Form(""),
        run_discover_sources_now: bool = Form(False),
        kickoff_tick_now: bool = Form(False),
        kickoff_digest_now: bool = Form(False),
        kickoff_push_digest_now: bool = Form(False),
        kickoff_digest_hours: int = Form(24),
        ai_enabled: bool = Form(False),
        ai_prompt: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Interactive topic setup: create/update a topic, seed some default sources, and (optionally)
        enable prompt-driven AI curation for high-signal daily digests.
        """
        from tracker.actions import (
            SourceBindingSpec,
            TopicAiPolicySpec,
            TopicSpec,
            create_discourse_source as create_discourse_source_action,
            create_hn_search_source as create_hn_search_source_action,
            create_rss_source as create_rss_source_action,
            create_rss_sources_bulk as create_rss_sources_bulk_action,
            create_searxng_search_source as create_searxng_search_source_action,
            create_topic as create_topic_action,
            upsert_topic_ai_policy as upsert_topic_ai_policy_action,
        )

        repo = Repo(session)
        token = request.query_params.get("token") if _token_auth_enabled(settings) else None

        topic_name = (name or "").strip()
        if not topic_name:
            return _redir_setup_topic(token, msg="missing topic name")

        q = (query or "").strip() or topic_name
        use_ai = bool(ai_enabled)

        topic = repo.get_topic_by_name(topic_name)
        if not topic:
            try:
                topic = create_topic_action(
                    session=session,
                    spec=TopicSpec(
                        name=topic_name,
                        query=q,
                        digest_cron=(digest_cron or "0 9 * * *").strip() or "0 9 * * *",
                        alert_keywords=(alert_keywords or "").strip(),
                    ),
                )
            except ValueError as exc:
                return _redir_setup_topic(token, msg=str(exc))
        else:
            topic.query = q
            topic.digest_cron = (digest_cron or "0 9 * * *").strip() or "0 9 * * *"
            topic.alert_keywords = (alert_keywords or "").strip()
            session.commit()

        try:
            if add_hn:
                create_hn_search_source_action(
                    session=session,
                    query=q,
                    bind=SourceBindingSpec(topic=topic_name),
                )

            if add_searxng:
                base_url = (searxng_base_url or "").strip()
                if not base_url:
                    raise ValueError("missing searxng_base_url")
                create_searxng_search_source_action(
                    session=session,
                    base_url=base_url,
                    query=q,
                    bind=SourceBindingSpec(topic=topic_name),
                )

            if add_discourse:
                base_url = (discourse_base_url or "").strip()
                if not base_url:
                    raise ValueError("missing discourse_base_url")
                create_discourse_source_action(
                    session=session,
                    base_url=base_url,
                    json_path=(discourse_json_path or "/latest.json").strip() or "/latest.json",
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=("" if use_ai else q)),
                )

            if add_nodeseek:
                create_rss_source_action(
                    session=session,
                    url="https://rss.nodeseek.com/",
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=("" if use_ai else q)),
                )

            if add_hn_popularity:
                from tracker.source_packs import get_rss_pack

                pack = get_rss_pack("hn_popularity_karpathy")
                create_rss_sources_bulk_action(
                    session=session,
                    urls=pack.urls,
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=("" if use_ai else q)),
                    tags="hn-popularity,karpathy",
                )

            for raw in (rss_urls or "").splitlines():
                s = raw.strip()
                if not s or s.startswith("#"):
                    continue
                create_rss_source_action(
                    session=session,
                    url=s,
                    bind=SourceBindingSpec(topic=topic_name),
                )

            # AI-native curation policy (optional; global enable is controlled by env).
            upsert_topic_ai_policy_action(
                session=session,
                spec=TopicAiPolicySpec(
                    topic=topic_name,
                    enabled=use_ai,
                    prompt=(ai_prompt or "").strip(),
                ),
            )
        except ValueError as exc:
            session.rollback()
            return _redir_setup_topic(token, msg=str(exc))

        extra = ""
        if run_discover_sources_now:
            try:
                with job_lock(name="jobs", timeout_seconds=0.0):
                    result = asyncio.run(
                        run_discover_sources(session=session, settings=settings, topic_ids=[int(topic.id)])
                    )
                created = 0
                found = 0
                errors = 0
                for row in result.per_topic:
                    created += int(getattr(row, "candidates_created", 0) or 0)
                    found += int(getattr(row, "candidates_found", 0) or 0)
                    errors += int(getattr(row, "errors", 0) or 0)
                extra = f"; discovered feeds: created={created} found={found} errors={errors}"
            except TimeoutError:
                extra = "; discover failed: busy: another job is running"
            except Exception as exc:
                extra = f"; discover failed: {exc}"

        kickoff_extra = ""
        if kickoff_tick_now or kickoff_digest_now:
            try:
                with job_lock(name="jobs", timeout_seconds=0.0):
                    if kickoff_tick_now:
                        asyncio.run(run_tick(session=session, settings=settings, push=False))
                    if kickoff_digest_now:
                        hrs = max(1, min(168, int(kickoff_digest_hours or 24)))
                        suffix = None
                        if kickoff_push_digest_now:
                            suffix = "kickoff-" + dt.datetime.utcnow().strftime("%H%M%S")
                        digest_result = asyncio.run(
                            run_digest(
                                session=session,
                                settings=settings,
                                hours=hrs,
                                push=bool(kickoff_push_digest_now),
                                key_suffix=suffix,
                                topic_ids=[int(topic.id)],
                            )
                        )
                        pushed = sum(int(t.pushed or 0) for t in digest_result.per_topic)
                        kickoff_extra = f"; kickoff: tick={str(bool(kickoff_tick_now)).lower()} digest={str(bool(kickoff_digest_now)).lower()} pushed={pushed}"
            except TimeoutError:
                kickoff_extra = "; kickoff failed: busy: another job is running"
            except Exception as exc:
                kickoff_extra = f"; kickoff failed: {exc}"

        return _redir_setup_topic(
            token,
            msg=f"topic ready: {topic_name}{extra}{kickoff_extra} (see /admin → Overview → Reports)",
        )

    @app.post("/admin/topic/add", dependencies=[Depends(auth_dep)])
    def admin_topic_add(
        request: Request,
        name: str = Form(...),
        query: str = Form(""),
        digest_cron: str = Form("0 9 * * *"),
        alert_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        create_topic_action(
            session=session,
            spec=TopicSpec(
                name=name,
                query=query,
                digest_cron=digest_cron,
                alert_keywords=alert_keywords,
            ),
        )
        return _redir(request)

    @app.post("/admin/topic/toggle", dependencies=[Depends(auth_dep)])
    def admin_topic_toggle(
        request: Request,
        name: str = Form(...),
        enabled: bool = Form(...),
        session: Session = Depends(get_db),
    ):
        try:
            set_topic_enabled_action(session=session, name=name, enabled=enabled)
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/topic/update", dependencies=[Depends(auth_dep)])
    def admin_topic_update(
        request: Request,
        name: str = Form(...),
        query: str = Form(""),
        digest_cron: str = Form("0 9 * * *"),
        alert_keywords: str = Form(""),
        alert_cooldown_minutes: int = Form(120),
        alert_daily_cap: int = Form(5),
        sync_search_sources: bool = Form(False),
        ai_enabled: bool = Form(False),
        ai_prompt: str = Form(""),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        topic = repo.get_topic_by_name(name)
        if not topic:
            return _redir(request, msg="topic not found")

        topic.query = query
        topic.digest_cron = digest_cron
        topic.alert_keywords = alert_keywords
        topic.alert_cooldown_minutes = alert_cooldown_minutes
        topic.alert_daily_cap = alert_daily_cap
        session.commit()

        # AI-native curation policy (optional; controlled by env + per-topic toggle).
        repo.upsert_topic_policy(
            topic_id=topic.id,
            llm_curation_enabled=ai_enabled,
            llm_curation_prompt=ai_prompt,
        )

        extra = ""
        if sync_search_sources:
            try:
                res = sync_topic_search_sources_action(session=session, topic_name=name)
                extra = f" (synced search sources: updated={res.updated} created={res.created} rebound={res.rebound})"
            except Exception:
                extra = " (sync search sources failed; see logs)"

        return _redir(request, msg=f"topic updated: {name}{extra}")

    @app.post("/admin/topic/delete", dependencies=[Depends(auth_dep)])
    def admin_topic_delete(
        request: Request,
        name: str = Form(...),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        topic = repo.get_topic_by_name(name)
        if not topic:
            return _redir(request, msg="topic not found")

        try:
            from sqlalchemy import delete
            from tracker.models import AlertBudget, ItemTopic, Report, SourceCandidate, Topic, TopicPolicy, TopicSource

            topic_id = int(topic.id)
            # Delete dependents first (avoid FK issues when enabled).
            session.execute(delete(ItemTopic).where(ItemTopic.topic_id == topic_id))
            session.execute(delete(TopicSource).where(TopicSource.topic_id == topic_id))
            session.execute(delete(TopicPolicy).where(TopicPolicy.topic_id == topic_id))
            session.execute(delete(SourceCandidate).where(SourceCandidate.topic_id == topic_id))
            session.execute(delete(AlertBudget).where(AlertBudget.topic_id == topic_id))
            session.execute(delete(Report).where(Report.topic_id == topic_id))
            session.execute(delete(Topic).where(Topic.id == topic_id))
            session.commit()
        except Exception as exc:
            session.rollback()
            return _redir(request, msg=f"delete failed: {exc}")

        return _redir(request, msg=f"topic deleted: {name}")

    @app.post("/admin/source/toggle", dependencies=[Depends(auth_dep)])
    def admin_source_toggle(
        request: Request,
        source_id: int = Form(...),
        enabled: bool = Form(...),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            repo.set_source_enabled(source_id=source_id, enabled=enabled)
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/source/delete", dependencies=[Depends(auth_dep)])
    def admin_source_delete(
        request: Request,
        source_id: int = Form(...),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        source = repo.get_source_by_id(int(source_id))
        if not source:
            return _redir(request, msg="source not found")

        try:
            from sqlalchemy import delete, func, select
            from tracker.models import Item, Source, SourceHealth, SourceMeta, TopicSource

            sid = int(source_id)
            # Always remove bindings first (safe "delete from tracking").
            session.execute(delete(TopicSource).where(TopicSource.source_id == sid))

            items_count = int(session.scalar(select(func.count()).select_from(Item).where(Item.source_id == sid)) or 0)
            if items_count > 0:
                # Keep history: we cannot hard-delete the source row without purging items.
                source.enabled = False
                session.commit()
                return _redir(request, msg=f"source disabled + unbound (kept {items_count} items)")

            # No items: safe to fully remove the source row.
            session.execute(delete(SourceHealth).where(SourceHealth.source_id == sid))
            session.execute(delete(SourceMeta).where(SourceMeta.source_id == sid))
            session.execute(delete(Source).where(Source.id == sid))
            session.commit()
        except Exception as exc:
            session.rollback()
            return _redir(request, msg=f"delete failed: {exc}")

        return _redir(request, msg="source deleted")

    @app.post("/admin/source/add-rss", dependencies=[Depends(auth_dep)])
    def admin_source_add_rss(
        request: Request,
        url: str = Form(...),
        topic: str = Form(""),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            create_rss_source_action(
                session=session,
                url=url,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/source/add-hn-search", dependencies=[Depends(auth_dep)])
    def admin_source_add_hn_search(
        request: Request,
        query: str = Form(...),
        tags: str = Form("story"),
        hits_per_page: int = Form(50),
        topic: str = Form(""),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            create_hn_search_source_action(
                session=session,
                query=query,
                tags=tags,
                hits_per_page=hits_per_page,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/source/add-searxng-search", dependencies=[Depends(auth_dep)])
    def admin_source_add_searxng_search(
        request: Request,
        base_url: str = Form(...),
        query: str = Form(...),
        categories: str = Form(""),
        time_range: str = Form("day"),
        language: str = Form(""),
        results: int = Form(20),
        topic: str = Form(""),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            create_searxng_search_source_action(
                session=session,
                base_url=base_url,
                query=query,
                categories=categories or None,
                time_range=time_range or None,
                language=language or None,
                results=results,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/source/add-discourse", dependencies=[Depends(auth_dep)])
    def admin_source_add_discourse(
        request: Request,
        base_url: str = Form(...),
        json_path: str = Form("/latest.json"),
        topic: str = Form(""),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            create_discourse_source_action(
                session=session,
                base_url=base_url,
                json_path=json_path,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/source/add-html-list", dependencies=[Depends(auth_dep)])
    def admin_source_add_html_list(
        request: Request,
        page_url: str = Form(...),
        item_selector: str = Form(...),
        title_selector: str = Form("a"),
        summary_selector: str = Form(""),
        max_items: int = Form(30),
        topic: str = Form(""),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            create_html_list_source_action(
                session=session,
                page_url=page_url,
                item_selector=item_selector,
                title_selector=title_selector or None,
                summary_selector=summary_selector or None,
                max_items=max_items,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/run/discover-sources", dependencies=[Depends(auth_dep)])
    def admin_run_discover_sources(
        request: Request,
        session: Session = Depends(get_db),
    ):
        try:
            with job_lock(name="jobs", timeout_seconds=0.0):
                asyncio.run(run_discover_sources(session=session, settings=settings))
        except TimeoutError:
            return _redir(request, msg="busy: another job is running")
        return _redir(request)

    @app.post("/admin/candidate/accept", dependencies=[Depends(auth_dep)])
    def admin_candidate_accept(
        request: Request,
        candidate_id: int = Form(...),
        enabled: bool = Form(True),
        session: Session = Depends(get_db),
    ):
        try:
            accept_source_candidate_action(session=session, candidate_id=candidate_id, enabled=enabled)
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/candidate/ignore", dependencies=[Depends(auth_dep)])
    def admin_candidate_ignore(
        request: Request,
        candidate_id: int = Form(...),
        session: Session = Depends(get_db),
    ):
        try:
            ignore_source_candidate_action(session=session, candidate_id=candidate_id)
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/run/tick", dependencies=[Depends(auth_dep)])
    def admin_run_tick(
        request: Request,
        push: bool = Form(False),
        session: Session = Depends(get_db),
    ):
        try:
            with job_lock(name="jobs", timeout_seconds=0.0):
                asyncio.run(run_tick(session=session, settings=settings, push=push))
        except TimeoutError:
            return _redir(request, msg="busy: another job is running")
        return _redir(request)

    @app.post("/admin/run/digest", dependencies=[Depends(auth_dep)])
    def admin_run_digest(
        request: Request,
        push: bool = Form(False),
        force: bool = Form(False),
        hours: int = Form(24),
        session: Session = Depends(get_db),
    ):
        suffix = None
        if push and force:
            suffix = "manual-" + dt.datetime.utcnow().strftime("%H%M%S")
        try:
            with job_lock(name="jobs", timeout_seconds=0.0):
                result = asyncio.run(run_curated_info(session=session, settings=settings, hours=hours, push=push, key_suffix=suffix))
        except TimeoutError:
            return _redir(request, msg="busy: another job is running")
        msg = None
        if push:
            msg = f"curated: pushed={getattr(result, 'pushed', 0)} key={getattr(result, 'idempotency_key', '')}"
        return _redir(request, msg=msg)

    @app.post("/admin/run/health", dependencies=[Depends(auth_dep)])
    def admin_run_health(
        request: Request,
        push: bool = Form(False),
        session: Session = Depends(get_db),
    ):
        try:
            with job_lock(name="jobs", timeout_seconds=0.0):
                asyncio.run(run_health_report(session=session, settings=settings, push=push))
        except TimeoutError:
            return _redir(request, msg="busy: another job is running")
        return _redir(request)

    @app.post("/admin/topic/policy/preset/add", dependencies=[Depends(auth_dep)], include_in_schema=False)
    def admin_topic_policy_preset_add(
        request: Request,
        preset_id: str = Form(""),
        label: str = Form(""),
        description: str = Form(""),
        prompt: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Save a custom Topic AI policy prompt preset into app_config.

        Notes:
        - Not a secret.
        - Used to extend the per-topic preset dropdown.
        """
        repo = Repo(session)
        pid = (preset_id or "").strip()
        if not pid:
            return _redir(request, msg="missing preset_id")
        if len(pid) > 64:
            return _redir(request, msg="preset_id too long")
        for ch in pid:
            if not (ch.isalnum() or ch in {"_", "-"}):
                return _redir(request, msg="invalid preset_id (allowed: a-zA-Z0-9_-)")

        lab = (label or "").strip()
        if not lab:
            return _redir(request, msg="missing label")
        if len(lab) > 120:
            return _redir(request, msg="label too long")
        desc = (description or "").strip()
        if len(desc) > 400:
            desc = desc[:400]
        pr = (prompt or "").strip()
        if not pr:
            return _redir(request, msg="missing prompt")
        if len(pr) > 20_000:
            return _redir(request, msg="prompt too long")

        key = "topic_policy_presets_custom_json"
        cur = (repo.get_app_config(key) or "").strip()
        try:
            obj = json.loads(cur) if cur else []
        except Exception:
            obj = []
        if not isinstance(obj, list):
            obj = []

        out = []
        replaced = False
        for it in obj[:200]:
            if not isinstance(it, dict):
                continue
            if str(it.get("id") or "").strip() == pid:
                out.append({"id": pid, "label": lab, "description": desc, "prompt": pr})
                replaced = True
            else:
                out.append(it)
        if not replaced:
            out.append({"id": pid, "label": lab, "description": desc, "prompt": pr})

        repo.set_app_config(key, json.dumps(out, ensure_ascii=False))
        return _redir(request, msg=f"topic preset saved: {pid}")

    @app.post("/admin/topic/policy/preset/delete", dependencies=[Depends(auth_dep)], include_in_schema=False)
    def admin_topic_policy_preset_delete(
        request: Request,
        preset_id: str = Form(""),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        pid = (preset_id or "").strip()
        if not pid:
            return _redir(request, msg="missing preset_id")

        key = "topic_policy_presets_custom_json"
        cur = (repo.get_app_config(key) or "").strip()
        try:
            obj = json.loads(cur) if cur else []
        except Exception:
            obj = []
        if not isinstance(obj, list):
            obj = []

        kept = []
        removed = 0
        for it in obj[:200]:
            if not isinstance(it, dict):
                continue
            if str(it.get("id") or "").strip() == pid:
                removed += 1
                continue
            kept.append(it)
        if removed <= 0:
            return _redir(request, msg=f"preset not found: {pid}")
        repo.set_app_config(key, json.dumps(kept, ensure_ascii=False))
        return _redir(request, msg=f"topic preset deleted: {pid}")

    @app.post("/admin/telegram/feedback", dependencies=[Depends(auth_dep)], include_in_schema=False)
    def admin_telegram_feedback(
        request: Request,
        mute_days_default: str = Form(""),
        reset: bool = Form(False),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        if reset:
            repo.delete_app_config("telegram_feedback_mute_days_default")
            return _redir(request, msg="telegram feedback settings reset")
        raw = (mute_days_default or "").strip()
        if not raw:
            return _redir(request, msg="missing mute_days_default")
        try:
            n = int(raw)
        except Exception:
            return _redir(request, msg="invalid mute_days_default")
        n = max(1, min(365, n))
        repo.set_app_config("telegram_feedback_mute_days_default", str(n))
        return _redir(request, msg="telegram feedback settings updated")

    @app.post("/admin/telegram/test-bot", dependencies=[Depends(auth_dep)], include_in_schema=False)
    async def admin_telegram_test_bot(
        request: Request,
        session: Session = Depends(get_db),
    ):
        """
        Connectivity test for Telegram bot token (does not require a connected chat).
        """
        _require_localhost(request)
        repo = Repo(session)
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        token = (getattr(eff, "telegram_bot_token", None) or "").strip()
        if not token:
            return JSONResponse(status_code=400, content={"ok": False, "message": "missing TRACKER_TELEGRAM_BOT_TOKEN"})

        url = f"https://api.telegram.org/bot{token}/getMe"
        try:
            timeout = max(5.0, float(getattr(eff, "http_timeout_seconds", 20) or 20))
        except Exception:
            timeout = 20.0

        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            return JSONResponse(status_code=502, content={"ok": False, "message": f"telegram token test error: {exc}"})

        if not isinstance(data, dict) or not data.get("ok"):
            desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
            return JSONResponse(status_code=502, content={"ok": False, "message": str(desc)})

        res = data.get("result") if isinstance(data, dict) else None
        username = ""
        bot_id = ""
        if isinstance(res, dict):
            username = str(res.get("username") or "").strip()
            bot_id = str(res.get("id") or "").strip()

        msg = "telegram token ok"
        if username:
            msg += f": @{username}"
        if bot_id:
            msg += f" id={bot_id}"
        return JSONResponse(status_code=200, content={"ok": True, "message": msg, "username": username, "id": bot_id})

    @app.post("/admin/auth/cookie-jar/delete", dependencies=[Depends(auth_dep)], include_in_schema=False)
    def admin_auth_cookie_jar_delete(
        request: Request,
        domain: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Delete a single domain entry from TRACKER_COOKIE_JAR_JSON (without echoing cookie values).
        """
        _require_localhost(request)
        d = (domain or "").strip()
        if not d:
            return _redir(request, msg="missing domain")

        from tracker.dynamic_config import apply_env_block_updates
        from tracker.envfile import parse_env_assignments
        from tracker.http_auth import parse_cookie_jar_json, parse_domains_csv

        env_path = Path(settings.env_path or ".env")
        env_assignments = parse_env_assignments(env_path.read_text(encoding="utf-8")) if env_path.exists() else {}
        raw = (env_assignments.get("TRACKER_COOKIE_JAR_JSON") or str(getattr(settings, "cookie_jar_json", "") or "")).strip()
        jar = parse_cookie_jar_json(raw)
        keys = parse_domains_csv(d)
        if not keys:
            return _redir(request, msg="invalid domain")
        key = keys[0]
        if key not in jar:
            return _redir(request, msg=f"not found: {key}")
        jar.pop(key, None)
        new_raw = "" if not jar else json.dumps(jar, ensure_ascii=False)

        repo = Repo(session)
        apply_env_block_updates(
            repo=repo,
            settings=settings,
            env_path=env_path,
            env_updates={"TRACKER_COOKIE_JAR_JSON": new_raw},
        )
        return _redir(request, msg=f"cookie jar updated: removed {key}")

    @app.post("/admin/mute/delete", dependencies=[Depends(auth_dep)], include_in_schema=False)
    def admin_mute_delete(
        request: Request,
        key: str = Form(...),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        repo.delete_mute_rule(scope="domain", key=key)
        return _redir(request, msg=f"unmuted: {key}")

    @app.post("/admin/push/retry", dependencies=[Depends(auth_dep)])
    def admin_push_retry(
        request: Request,
        key: str = Form(...),
        only: str = Form(""),
        session: Session = Depends(get_db),
    ):
        from tracker.push_ops import retry_push_key

        repo = Repo(session)
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        asyncio.run(
            retry_push_key(
                session=session,
                settings=eff,
                idempotency_key=key,
                only=(only or None),
            )
        )
        return _redir(request)

    @app.post("/admin/push/test", dependencies=[Depends(auth_dep)])
    def admin_push_test(
        request: Request,
        only: str = Form(""),
        session: Session = Depends(get_db),
    ):
        from tracker.push_ops import push_test as push_test_core

        repo = Repo(session)
        try:
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo, settings=settings)
        except Exception:
            eff = settings

        try:
            results = asyncio.run(push_test_core(session=session, settings=eff, only=(only or None)))
            short = ", ".join(f"{c}={s}" for c, s in results)
            msg = f"push test: {short}"
        except Exception as exc:
            msg = f"push test error: {exc}"

        return _redir(request, msg=msg)

    @app.post("/admin/env/update", dependencies=[Depends(auth_dep)])
    def admin_env_update(
        request: Request,
        cron_timezone: str = Form(""),
        dingtalk_webhook_url: str = Form(""),
        dingtalk_secret: str = Form(""),
        discourse_cookie: str = Form(""),
        cookie_jar_json: str = Form(""),
        smtp_host: str = Form(""),
        smtp_port: str = Form(""),
        smtp_user: str = Form(""),
        smtp_password: str = Form(""),
        smtp_starttls: str = Form(""),  # true|false|"" (leave)
        smtp_use_ssl: str = Form(""),  # true|false|"" (leave)
        email_from: str = Form(""),
        email_to: str = Form(""),
        session: Session = Depends(get_db),
    ):
        """
        Operator helper: write selected TRACKER_* keys into the `.env` file.

        Blank values are treated as "no change". After saving, restart services to apply.
        """
        _require_localhost(request)

        def _norm(s: str) -> str:
            return (s or "").strip()

        updates: dict[str, str] = {}

        tz = _norm(cron_timezone)
        if tz:
            updates["TRACKER_CRON_TIMEZONE"] = tz

        dt_url = _norm(dingtalk_webhook_url)
        if dt_url:
            updates["TRACKER_DINGTALK_WEBHOOK_URL"] = dt_url
        dt_sec = _norm(dingtalk_secret)
        if dt_sec:
            updates["TRACKER_DINGTALK_SECRET"] = dt_sec

        dc = _norm(discourse_cookie)
        if dc:
            updates["TRACKER_DISCOURSE_COOKIE"] = dc

        cj = _norm(cookie_jar_json)
        if cj:
            updates["TRACKER_COOKIE_JAR_JSON"] = cj

        sh = _norm(smtp_host)
        if sh:
            updates["TRACKER_SMTP_HOST"] = sh

        sp = _norm(smtp_port)
        if sp:
            try:
                p = int(sp)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid smtp_port") from exc
            if p < 1 or p > 65535:
                raise HTTPException(status_code=400, detail="invalid smtp_port")
            updates["TRACKER_SMTP_PORT"] = str(p)

        su = _norm(smtp_user)
        if su:
            updates["TRACKER_SMTP_USER"] = su
        pw = _norm(smtp_password)
        if pw:
            updates["TRACKER_SMTP_PASSWORD"] = pw

        st = _norm(smtp_starttls).lower()
        if st in {"true", "false"}:
            updates["TRACKER_SMTP_STARTTLS"] = st

        ssl = _norm(smtp_use_ssl).lower()
        if ssl in {"true", "false"}:
            updates["TRACKER_SMTP_USE_SSL"] = ssl

        ef = _norm(email_from)
        if ef:
            updates["TRACKER_EMAIL_FROM"] = ef
        eto = _norm(email_to)
        if eto:
            updates["TRACKER_EMAIL_TO"] = eto

        if not updates:
            return _redir(request, msg="env update: no changes")

        repo = Repo(session)
        from tracker.dynamic_config import apply_env_block_updates

        path = Path(settings.env_path or ".env")
        res = apply_env_block_updates(repo=repo, settings=settings, env_path=path, env_updates=updates)

        keys = ", ".join(sorted(res.updated_env_keys))
        msg = f"env updated: {keys} (restart services to apply)"
        return _redir(request, msg=msg)

    @app.post("/admin/source/meta", dependencies=[Depends(auth_dep)])
    def admin_source_meta(
        request: Request,
        source_id: int = Form(...),
        tags: str = Form(""),
        notes: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            update_source_meta_action(session=session, source_id=source_id, tags=tags, notes=notes)
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/source/score", dependencies=[Depends(auth_dep)])
    def admin_source_score(
        request: Request,
        source_id: int = Form(...),
        score: int = Form(0),
        locked: str | None = Form(None),
        session: Session = Depends(get_db),
    ):
        repo = Repo(session)
        try:
            v = max(0, min(100, int(score or 0)))
        except Exception:
            v = 0
        is_locked = bool(locked)
        try:
            repo.upsert_source_score(
                source_id=int(source_id),
                score=int(v),
                origin="manual",
                locked=bool(is_locked),
                force=True,
            )
        except Exception:
            pass
        return _redir(request)

    @app.post("/admin/bind/add", dependencies=[Depends(auth_dep)])
    def admin_bind_add(
        request: Request,
        topic: str = Form(...),
        source_id: int = Form(...),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            create_binding_action(
                session=session,
                topic_name=topic,
                source_id=source_id,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
            )
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/bind/remove", dependencies=[Depends(auth_dep)])
    def admin_bind_remove(
        request: Request,
        topic: str = Form(...),
        source_id: int = Form(...),
        session: Session = Depends(get_db),
    ):
        try:
            remove_binding_action(session=session, topic_name=topic, source_id=source_id)
        except ValueError:
            pass
        return _redir(request)

    @app.post("/admin/bind/update", dependencies=[Depends(auth_dep)])
    def admin_bind_update(
        request: Request,
        topic: str = Form(...),
        source_id: int = Form(...),
        include_keywords: str = Form(""),
        exclude_keywords: str = Form(""),
        session: Session = Depends(get_db),
    ):
        try:
            update_binding_action(
                session=session,
                topic_name=topic,
                source_id=source_id,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
            )
        except ValueError:
            pass
        return _redir(request)

    return app


app = create_app(get_settings())
