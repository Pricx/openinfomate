#!/usr/bin/env sh
set -eu

# Deploy one OpenInfoMate instance with:
# - isolated Docker Compose project name (so volumes/networks don't collide)
# - automatic port selection (auto-increment if in use)
#
# Usage:
#   ./scripts/deploy_docker_instance.sh
#   ./scripts/deploy_docker_instance.sh --port 8901
#   ./scripts/deploy_docker_instance.sh --base-port 8899 --project-prefix openinfomate
#   ./scripts/deploy_docker_instance.sh --host   # opt-in host networking override (needs docker-compose.host.yml)

MODE="ghcr"
HOST_MODE="false"
BASE_PORT="8899"
PORT=""
SEARX_PORT=""
PROJECT_PREFIX="openinfomate"
INSTANCE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --host) HOST_MODE="true"; shift ;;
    --mode) MODE="${2:-}"; shift 2 ;;
    --base-port) BASE_PORT="${2:-}"; shift 2 ;;
    --port) PORT="${2:-}"; shift 2 ;;
    --searx-port) SEARX_PORT="${2:-}"; shift 2 ;;
    --project-prefix) PROJECT_PREFIX="${2:-}"; shift 2 ;;
    --instance) INSTANCE="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--port N] [--base-port N] [--project-prefix NAME] [--instance NAME] [--host]"
      exit 0
      ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

compose_file=""
if [ "${MODE}" = "ghcr" ]; then
  if [ -f "docker-compose.ghcr.yml" ]; then
    compose_file="docker-compose.ghcr.yml"
  elif [ -f "docker-compose.yml" ]; then
    compose_file="docker-compose.yml"
  else
    echo "Missing compose file (docker-compose.ghcr.yml or docker-compose.yml)" >&2
    exit 1
  fi
else
  compose_file="docker-compose.yml"
fi

need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1" >&2; exit 1; }; }
need_cmd docker
need_cmd ss || true

is_port_in_use() {
  p="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -E "(^|:)${p}\$" >/dev/null 2>&1
    return $?
  fi
  # Fallback: docker published ports
  docker ps --format '{{.Ports}}' | grep -E "[:.]${p}->" >/dev/null 2>&1
}

pick_port() {
  start="$1"
  p="$start"
  i=0
  while [ $i -lt 200 ]; do
    if ! is_port_in_use "$p"; then
      echo "$p"
      return 0
    fi
    p=$((p + 1))
    i=$((i + 1))
  done
  echo "No free port found starting at ${start}" >&2
  return 1
}

if [ -z "${PORT}" ]; then
  PORT="$(pick_port "${BASE_PORT}")"
fi

if [ -z "${INSTANCE}" ]; then
  INSTANCE="${PROJECT_PREFIX}-${PORT}"
fi

if [ "${HOST_MODE}" = "true" ] && [ -z "${SEARX_PORT}" ]; then
  # In host mode we may publish searxng to a host port; pick a nearby free one.
  guess=$((PORT - 10))
  if [ "${guess}" -lt 1024 ]; then
    guess=$((PORT + 10))
  fi
  SEARX_PORT="$(pick_port "${guess}")"
fi

export OPENINFOMATE_API_PORT="${PORT}"
export OPENINFOMATE_INSTANCE="${INSTANCE}"
if [ -n "${SEARX_PORT}" ]; then
  export OPENINFOMATE_SEARXNG_PORT="${SEARX_PORT}"
fi
export COMPOSE_PROJECT_NAME="${INSTANCE}"

echo "[deploy] project=${COMPOSE_PROJECT_NAME}"
echo "[deploy] api_port=${OPENINFOMATE_API_PORT}"
if [ -n "${SEARX_PORT}" ]; then
  echo "[deploy] searxng_port=${OPENINFOMATE_SEARXNG_PORT}"
fi
echo "[deploy] mode=${MODE} host_mode=${HOST_MODE}"

files="-f ${compose_file}"
if [ "${HOST_MODE}" = "true" ]; then
  if [ ! -f "docker-compose.host.yml" ]; then
    echo "Missing docker-compose.host.yml for --host" >&2
    exit 1
  fi
  files="${files} -f docker-compose.host.yml"
fi

# shellcheck disable=SC2086
docker compose ${files} pull
# shellcheck disable=SC2086
docker compose ${files} up -d --force-recreate
# shellcheck disable=SC2086
docker compose ${files} ps

echo "[deploy] admin_url=http://127.0.0.1:${OPENINFOMATE_API_PORT}/admin"
