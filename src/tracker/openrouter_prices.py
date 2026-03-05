from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from tracker.repo import Repo

logger = logging.getLogger(__name__)

OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"

_CACHE_KEY_JSON = "openrouter_prices_cache_json"
_CACHE_KEY_TS = "openrouter_prices_cache_fetched_at"


@dataclass(frozen=True)
class VendorSpec:
    label: str
    prefix: str


VENDORS: list[VendorSpec] = [
    VendorSpec(label="OpenAI (GPT)", prefix="openai/"),
    VendorSpec(label="Anthropic (Claude)", prefix="anthropic/"),
    VendorSpec(label="Google (Gemini)", prefix="google/"),
    VendorSpec(label="DeepSeek", prefix="deepseek/"),
    VendorSpec(label="Zhipu (GLM)", prefix="zhipu/"),
    VendorSpec(label="MiniMax", prefix="minimax/"),
]


def _parse_price_per_token_usd(v: object) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return x if x >= 0 else None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            x = float(s)
        except Exception:
            return None
        return x if x >= 0 else None
    return None


def _usd_per_token_to_usd_per_m(v: float | None) -> float | None:
    if v is None:
        return None
    return v * 1_000_000.0


def _compute_prices_summary(models_payload: object) -> dict[str, Any]:
    """
    Return a compact summary suitable for UI display and DB caching.
    """
    data = models_payload.get("data") if isinstance(models_payload, dict) else None
    if not isinstance(data, list):
        return {"vendors": [], "models_count": 0}

    rows: list[dict[str, Any]] = []
    for m in data:
        if not isinstance(m, dict):
            continue
        mid = str(m.get("id") or "").strip()
        if not mid:
            continue
        pricing = m.get("pricing")
        if not isinstance(pricing, dict):
            continue
        prompt_usd_per_token = _parse_price_per_token_usd(pricing.get("prompt"))
        completion_usd_per_token = _parse_price_per_token_usd(pricing.get("completion"))
        in_m = _usd_per_token_to_usd_per_m(prompt_usd_per_token)
        out_m = _usd_per_token_to_usd_per_m(completion_usd_per_token)
        if in_m is None and out_m is None:
            continue
        rows.append(
            {
                "id": mid,
                "in_usd_per_m": in_m,
                "out_usd_per_m": out_m,
            }
        )

    def _score_row(r: dict[str, Any]) -> float:
        a = r.get("in_usd_per_m")
        b = r.get("out_usd_per_m")
        aa = float(a) if isinstance(a, (int, float)) else 1e18
        bb = float(b) if isinstance(b, (int, float)) else 1e18
        return aa + bb

    vendors_out: list[dict[str, Any]] = []
    for v in VENDORS:
        vs = [r for r in rows if str(r.get("id") or "").startswith(v.prefix)]
        if not vs:
            vendors_out.append({"label": v.label, "prefix": v.prefix, "top": [], "min_in": None, "min_out": None})
            continue
        min_in = min((r["in_usd_per_m"] for r in vs if isinstance(r.get("in_usd_per_m"), (int, float))), default=None)
        min_out = min((r["out_usd_per_m"] for r in vs if isinstance(r.get("out_usd_per_m"), (int, float))), default=None)
        top = sorted(vs, key=_score_row)[:3]
        vendors_out.append(
            {
                "label": v.label,
                "prefix": v.prefix,
                "top": top,
                "min_in": min_in,
                "min_out": min_out,
                "count": len(vs),
            }
        )

    return {"vendors": vendors_out, "models_count": len(rows)}


def get_openrouter_prices(repo: Repo, *, ttl_seconds: int, force_refresh: bool = False) -> dict[str, Any]:
    """
    Cached fetch of OpenRouter model prices summary.
    """
    now = dt.datetime.utcnow()
    cached_json = (repo.get_app_config(_CACHE_KEY_JSON) or "").strip()
    cached_ts = (repo.get_app_config(_CACHE_KEY_TS) or "").strip()
    if cached_json and cached_ts and not force_refresh:
        try:
            ts = dt.datetime.fromisoformat(cached_ts.replace("Z", "+00:00")).replace(tzinfo=None)
            age = (now - ts).total_seconds()
            if age >= 0 and age <= float(ttl_seconds):
                obj = json.loads(cached_json)
                if isinstance(obj, dict):
                    return {"ok": True, "cached": True, "fetched_at": cached_ts, **obj}
        except Exception:
            pass

    try:
        resp = httpx.get(
            OPENROUTER_MODELS_URL,
            headers={"User-Agent": "openinfomate/0.1", "Accept": "application/json"},
            timeout=8.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        payload = resp.json()
        summary = _compute_prices_summary(payload)
        fetched_at = now.isoformat() + "Z"
        repo.set_app_config(_CACHE_KEY_JSON, json.dumps(summary, ensure_ascii=False, sort_keys=True))
        repo.set_app_config(_CACHE_KEY_TS, fetched_at)
        return {"ok": True, "cached": False, "fetched_at": fetched_at, **summary}
    except Exception as exc:
        logger.info("openrouter price fetch failed: %s", exc)
        if cached_json:
            try:
                obj = json.loads(cached_json)
                if isinstance(obj, dict):
                    return {
                        "ok": True,
                        "cached": True,
                        "stale": True,
                        "fetched_at": cached_ts or "",
                        **obj,
                        "warning": str(exc),
                    }
            except Exception:
                pass
        return {"ok": False, "error": str(exc), "vendors": [], "models_count": 0}

