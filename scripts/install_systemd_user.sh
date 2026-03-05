#!/usr/bin/env bash
set -euo pipefail

WORKDIR="${HOME}/openinfomate"
ENABLE_API="1"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--dir <working-dir>] [--no-api]

Installs user-level systemd services:
  - tracker.service (scheduler)
  - tracker-api.service (admin/API) [optional]

Defaults:
  --dir "\$HOME/openinfomate"
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

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UNIT_SRC="${ROOT}/deploy/systemd/user"
UNIT_DST="${HOME}/.config/systemd/user"

mkdir -p "${UNIT_DST}"

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
  sed "s|%h/tracker|${WORKDIR}|g" "${UNIT_SRC}/${name}" > "${UNIT_DST}/${name}"
  echo "[systemd] installed ${UNIT_DST}/${name}"
}

ensure_env_file
install_unit "tracker.service"
if [[ "${ENABLE_API}" == "1" ]]; then
  install_unit "tracker-api.service"
fi

echo "[systemd] reloading + enabling (user)"
if systemctl --user daemon-reload; then
  systemctl --user enable --now tracker.service
  if [[ "${ENABLE_API}" == "1" ]]; then
    systemctl --user enable --now tracker-api.service
  fi
  systemctl --user status tracker.service --no-pager || true
else
  echo "[systemd] WARN: systemctl --user failed. You may need: loginctl enable-linger \$(whoami)" >&2
fi
