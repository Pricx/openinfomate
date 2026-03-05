# SearxNG (Web-Wide Search) Setup

Tracker can use SearxNG as a “daily web-wide discovery” source via `searxng_search`.

## 1) Self-host (recommended)

If your Linux host has Docker, the quickest path is to run SearxNG locally and only expose it on `127.0.0.1`.

```bash
mkdir -p ~/searxng
cd ~/searxng

# Copy the compose file from this repo:
cp /path/to/tracker/deploy/searxng/docker-compose.yml .

docker compose up -d
```

Important: Tracker’s `searxng_search` connector uses SearxNG’s **JSON** output, but SearxNG’s default config
often ships with `search.formats: [html]` only (JSON disabled). Enable JSON, then restart:

```bash
# After the first start, SearxNG writes config under ./searxng_etc/settings.yml
# Add "- json" under "search.formats", then restart:
grep -n "formats:" -n searxng_etc/settings.yml | head

docker compose restart
```

Shortcut: run the repo helper which sets up volumes and enables JSON automatically:

```bash
/path/to/tracker/scripts/setup_searxng.sh --dir ~/searxng
```

Then point Tracker at it:

```bash
tracker source add-searxng-search --base-url "http://127.0.0.1:8888" --query "your topic query" --topic "Your Topic"
```

## 2) Use an existing SearxNG instance

If you already run SearxNG elsewhere, set `--base-url` to that instance (prefer HTTPS, and keep it private).

## Notes
- Keep SearxNG private (LAN/VPN only) to reduce abuse risk.
- If your environment doesn’t have Docker, install SearxNG using your platform’s preferred method, then reuse the same `--base-url`.
  - If `docker pull` is timing out, configure a Docker registry mirror suitable for your region, then retry.
