#!/usr/bin/env bash
set -euo pipefail

WORKDIR="/opt/openinfomate"
ENABLE_API="1"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--dir <working-dir>] [--no-api]

Installs system-level systemd services:
  - tracker.service (scheduler)
  - tracker-api.service (admin/API) [optional]

Defaults:
  --dir "/opt/openinfomate"

Notes:
  - Requires root (will re-exec with sudo if available).
  - Expects a venv at <dir>/.venv and config at <dir>/.env.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      WORKDIR="$2"; shift 2;;
    --no-api)
      ENABLE_API="0"; shift 1;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2;;
  esac
done

WORKDIR="${WORKDIR/#\~/${HOME}}"

if [[ "${EUID}" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    exec sudo -E "$0" "$@"
  fi
  echo "[systemd] ERROR: must run as root (or with sudo)" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UNIT_SRC="${ROOT}/deploy/systemd/system"
UNIT_DST="/etc/systemd/system"

ensure_env_file() {
  if [[ -f "${WORKDIR}/.env" ]]; then
    return 0
  fi
  if [[ -f "${WORKDIR}/.env.example" ]]; then
    cp "${WORKDIR}/.env.example" "${WORKDIR}/.env"
  else
    : > "${WORKDIR}/.env"
  fi
  chmod 600 "${WORKDIR}/.env" || true
  echo "[systemd] created ${WORKDIR}/.env (edit it for push/API settings)"
}

install_unit() {
  local name="$1"
  sed "s|/opt/tracker|${WORKDIR}|g" "${UNIT_SRC}/${name}" > "${UNIT_DST}/${name}"
  echo "[systemd] installed ${UNIT_DST}/${name}"
}

ensure_env_file
install_unit "tracker.service"
if [[ "${ENABLE_API}" == "1" ]]; then
  install_unit "tracker-api.service"
fi

echo "[systemd] reloading + enabling (system)"
systemctl daemon-reload
systemctl enable --now tracker.service
if [[ "${ENABLE_API}" == "1" ]]; then
  systemctl enable --now tracker-api.service
fi
systemctl status tracker.service --no-pager || true
