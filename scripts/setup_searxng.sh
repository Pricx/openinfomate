#!/usr/bin/env bash
set -euo pipefail

DIR="${HOME}/searxng"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--dir <searxng-dir>]

Sets up a local SearxNG instance via Docker Compose:
  - binds to 127.0.0.1:8888
  - persists config+cache under <dir>/
  - enables JSON output (required by Tracker's searxng_search connector)

Defaults:
  --dir "\$HOME/searxng"
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      DIR="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2;;
  esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_SRC="${ROOT}/deploy/searxng/docker-compose.yml"

mkdir -p "${DIR}"
mkdir -p "${DIR}/searxng_etc" "${DIR}/searxng_cache"

cp "${COMPOSE_SRC}" "${DIR}/docker-compose.yml"

cd "${DIR}"
docker compose up -d

for _ in $(seq 1 30); do
  if [[ -f "searxng_etc/settings.yml" ]]; then
    break
  fi
  sleep 1
done

if [[ ! -f "searxng_etc/settings.yml" ]]; then
  echo "[searxng] ERROR: settings.yml not created; showing logs" >&2
  docker compose logs --tail 200 >&2 || true
  exit 1
fi

CID="$(docker compose ps -q searxng || true)"
if [[ -z "${CID}" ]]; then
  echo "[searxng] ERROR: couldn't find running container id" >&2
  docker compose ps >&2 || true
  exit 1
fi

docker exec -i -u 0 "${CID}" python3 - <<'PY'
from __future__ import annotations

from pathlib import Path

path = Path("/etc/searxng/settings.yml")
text = path.read_text(encoding="utf-8")

if "\n    - json\n" in text or "\n  - json\n" in text:
    print("[searxng] JSON already enabled")
    raise SystemExit(0)

needle_a = "formats:\n    - html\n"
needle_b = "formats:\n  - html\n"

if needle_a in text:
    text = text.replace(needle_a, "formats:\n    - html\n    - json\n", 1)
elif needle_b in text:
    text = text.replace(needle_b, "formats:\n  - html\n  - json\n", 1)
else:
    raise SystemExit("[searxng] ERROR: couldn't find search.formats block to patch")

path.write_text(text, encoding="utf-8")
print("[searxng] Enabled JSON output in settings.yml")
PY

docker compose restart
echo "[searxng] OK: http://127.0.0.1:8888"
