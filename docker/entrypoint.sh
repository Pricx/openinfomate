#!/bin/sh
set -eu

DATA_DIR="${OPENINFOMATE_DATA_DIR:-/data}"
ENV_PATH="${TRACKER_ENV_PATH:-${DATA_DIR}/.env}"
DB_URL="${TRACKER_DB_URL:-sqlite:////data/tracker.db}"

mkdir -p "${DATA_DIR}"

if [ ! -f "${ENV_PATH}" ]; then
  : > "${ENV_PATH}"
  chmod 600 "${ENV_PATH}" || true
  echo "[openinfomate] created ${ENV_PATH}"
fi

export TRACKER_ENV_PATH="${ENV_PATH}"
export TRACKER_DB_URL="${DB_URL}"

INSTANCE="${OPENINFOMATE_INSTANCE:-openinfomate}"
HOST_BIND_HOST="${OPENINFOMATE_API_BIND_HOST:-127.0.0.1}"
HOST_BIND_PORT="${OPENINFOMATE_API_PORT:-}"
LISTEN_HOST="${TRACKER_API_HOST:-0.0.0.0}"
LISTEN_PORT="${TRACKER_API_PORT:-8080}"
SEARX_BASE="${TRACKER_SEARXNG_BASE_URL:-}"
SEARX_PORT="${OPENINFOMATE_SEARXNG_PORT:-}"

if [ -n "${HOST_BIND_PORT}" ]; then
  echo "[openinfomate] instance=${INSTANCE} admin_url=http://${HOST_BIND_HOST}:${HOST_BIND_PORT}/admin"
else
  echo "[openinfomate] instance=${INSTANCE} api_listen=${LISTEN_HOST}:${LISTEN_PORT}"
fi
if [ -n "${SEARX_BASE}" ]; then
  echo "[openinfomate] searxng_base=${SEARX_BASE}"
fi
if [ -n "${SEARX_PORT}" ]; then
  echo "[openinfomate] searxng_host_port=${SEARX_PORT}"
fi

# Optional: generate admin/API secrets once (persisted to ENV_PATH) if missing/placeholder.
#
# Default UX for the docker-compose quickstart is "no preconfigured password";
# operators set auth in Web Admin. If you prefer an auto-generated first-run password,
# do NOT set TRACKER_BOOTSTRAP_ALLOW_NO_AUTH=true.
BOOTSTRAP_NO_AUTH="$(printf "%s" "${TRACKER_BOOTSTRAP_ALLOW_NO_AUTH:-}" | tr '[:upper:]' '[:lower:]')"
if [ "${BOOTSTRAP_NO_AUTH}" != "true" ] && [ "${BOOTSTRAP_NO_AUTH}" != "1" ]; then
  SECRETS_OUT="$(
    python - <<'PY'
from __future__ import annotations

import os
import secrets
from pathlib import Path

env_path = Path(os.environ.get("TRACKER_ENV_PATH") or "/data/.env")
text = env_path.read_text(encoding="utf-8") if env_path.exists() else ""

def _get(key: str) -> str | None:
    for line in text.splitlines():
        if not line or line.lstrip().startswith("#"):
            continue
        if line.startswith(key + "="):
            return line.split("=", 1)[1]
    return None

def _is_placeholder(v: str | None) -> bool:
    if v is None:
        return True
    s = v.strip()
    if not s:
        return True
    low = s.lower()
    return low in {"change-me", "changeme", "your-password", "password", "secret", "set-me"}

def _upsert(key: str, value: str) -> None:
    global text
    lines = text.splitlines()
    out: list[str] = []
    found = False
    for ln in lines:
        if ln.startswith(key + "="):
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(ln)
    if not found:
        if out and out[-1].strip():
            out.append("")
        out.append(f"{key}={value}")
    text = "\n".join(out).rstrip("\n") + "\n"

admin_pw = None
api_token = None

if _is_placeholder(os.environ.get("TRACKER_ADMIN_PASSWORD") or _get("TRACKER_ADMIN_PASSWORD")):
    admin_pw = secrets.token_urlsafe(18)
    _upsert("TRACKER_ADMIN_PASSWORD", admin_pw)

if _is_placeholder(os.environ.get("TRACKER_API_TOKEN") or _get("TRACKER_API_TOKEN")):
    api_token = secrets.token_urlsafe(24)
    _upsert("TRACKER_API_TOKEN", api_token)

if admin_pw or api_token:
    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text(text, encoding="utf-8")

if admin_pw:
    print(f"TRACKER_ADMIN_PASSWORD={admin_pw}")
if api_token:
    print(f"TRACKER_API_TOKEN={api_token}")
PY
  )"
  if [ -n "${SECRETS_OUT}" ]; then
    echo "[openinfomate] generated secrets (store these safely):"
    echo "${SECRETS_OUT}"
  fi
fi

# Ensure DB exists (idempotent).
tracker db init >/dev/null 2>&1 || tracker db init

exec "$@"
