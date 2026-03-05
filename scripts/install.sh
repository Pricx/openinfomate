#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="python3"
ENABLE_SYSTEMD_USER="0"
NO_API="0"

usage() {
  cat <<'EOF'
Usage:
  scripts/install.sh [--python <python-bin>] [--systemd-user] [--no-api]

What it does:
  - Ensures .env exists (copies .env.example if present)
  - Generates admin/API secrets if missing (prints once)
  - Creates/updates local venv at ./.venv
  - Installs OpenInfoMate into the venv (editable)
  - Initializes DB (tracker db init)
  - (Optional) installs + starts systemd --user units

Examples:
  scripts/install.sh
  scripts/install.sh --systemd-user
  scripts/install.sh --systemd-user --no-api
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      PYTHON_BIN="$2"; shift 2;;
    --systemd-user)
      ENABLE_SYSTEMD_USER="1"; shift 1;;
    --no-api)
      NO_API="1"; shift 1;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2;;
  esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

if [[ ! -f ".env" ]]; then
  if [[ -f ".env.example" ]]; then
    cp ".env.example" ".env"
  else
    : > ".env"
  fi
  chmod 600 ".env" || true
  echo "[install] created .env"
fi

echo "[install] checking admin/API secrets"
SECRETS_OUT="$(
  ${PYTHON_BIN} - <<'PY'
from __future__ import annotations

from pathlib import Path
import secrets

path = Path(".env")
text = path.read_text(encoding="utf-8") if path.exists() else ""

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
    # Preserve trailing newline (nice for POSIX tools).
    text = "\n".join(out).rstrip("\n") + "\n"

admin_pw = None
api_token = None

if _is_placeholder(_get("TRACKER_ADMIN_PASSWORD")):
    admin_pw = secrets.token_urlsafe(18)
    _upsert("TRACKER_ADMIN_PASSWORD", admin_pw)

if _is_placeholder(_get("TRACKER_API_TOKEN")):
    api_token = secrets.token_urlsafe(24)
    _upsert("TRACKER_API_TOKEN", api_token)

if admin_pw or api_token:
    path.write_text(text, encoding="utf-8")

if admin_pw:
    print(f"TRACKER_ADMIN_PASSWORD={admin_pw}")
if api_token:
    print(f"TRACKER_API_TOKEN={api_token}")
PY
)"
if [[ -n "${SECRETS_OUT}" ]]; then
  echo "[install] generated secrets (store these safely):"
  echo "${SECRETS_OUT}"
  echo "[install] NOTE: restart services after changing secrets"
else
  echo "[install] secrets already set; nothing changed"
fi

echo "[install] creating venv + installing"
${PYTHON_BIN} --version
${PYTHON_BIN} -m venv .venv
./.venv/bin/python -m pip install -U pip
./.venv/bin/python -m pip install -e .
./.venv/bin/tracker db init

echo "[install] OK"

if [[ "${ENABLE_SYSTEMD_USER}" == "1" ]]; then
  if ! command -v systemctl >/dev/null 2>&1; then
    echo "[install] WARN: systemctl not found; skipping systemd --user install" >&2
    exit 0
  fi
  EXTRA=""
  if [[ "${NO_API}" == "1" ]]; then
    EXTRA="--no-api"
  fi
  ./scripts/install_systemd_user.sh --dir "${ROOT}" ${EXTRA}
fi
