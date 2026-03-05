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

# First-run bootstrap: seed a sensible default RSS pack (Karpathy 90+ feeds).
#
# This is intentionally best-effort and never blocks startup. It only runs when the DB
# is empty, and it uses a data-volume marker so api/scheduler don't race.
BOOTSTRAP_DONE="${DATA_DIR}/.openinfomate_bootstrap_done"
BOOTSTRAP_LOCK="${DATA_DIR}/.openinfomate_bootstrap_lock"
if [ ! -f "${BOOTSTRAP_DONE}" ]; then
  if mkdir "${BOOTSTRAP_LOCK}" 2>/dev/null; then
    trap 'rmdir "${BOOTSTRAP_LOCK}" 2>/dev/null || true' EXIT
    python - <<'PY' || true
from __future__ import annotations

from tracker.actions import SourceBindingSpec, TopicSpec, create_rss_sources_bulk, create_topic
from tracker.db import session_factory
from tracker.models import TopicPolicy
from tracker.repo import Repo
from tracker.settings import get_settings
from tracker.source_packs import get_rss_pack

settings = get_settings()
_engine, make_session = session_factory(settings)

topic_name = "HN Popularity"
pack_id = "hn_popularity_karpathy"

with make_session() as session:
    repo = Repo(session)
    if repo.list_sources():
        print(f"[openinfomate] bootstrap skipped: sources already exist")
    else:
        if not repo.get_topic_by_name(topic_name):
            create_topic(
                session=session,
                spec=TopicSpec(name=topic_name, query="", digest_cron="", alert_keywords=""),
            )
        trow = repo.get_topic_by_name(topic_name)
        if trow and getattr(trow, "id", None) is not None:
            pol = repo.get_topic_policy(topic_id=int(trow.id))
            if not pol:
                pol = TopicPolicy(topic_id=int(trow.id))
                session.add(pol)
                session.flush()
            pol.llm_curation_enabled = True
            session.commit()
        pack = get_rss_pack(pack_id)
        created, bound = create_rss_sources_bulk(
            session=session,
            urls=list(pack.urls),
            bind=SourceBindingSpec(topic=topic_name, include_keywords="", exclude_keywords=""),
            tags="hn-popularity,karpathy",
        )
        print(
            f"[openinfomate] bootstrap OK: pack={pack_id} feeds={len(pack.urls)} "
            f"created={created} bound={bound} topic='{topic_name}'"
        )
PY
    : > "${BOOTSTRAP_DONE}" || true
    rmdir "${BOOTSTRAP_LOCK}" 2>/dev/null || true
    trap - EXIT
  fi
fi

exec "$@"
