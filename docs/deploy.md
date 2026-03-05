# Deployment (Linux systemd)

This project is designed to run 24/7 on a Linux host (your “nv” box). A Mac can run it as a fallback.

## 0) Optional: SearxNG (recommended for web-wide discovery)

If you want “daily web-wide search” sources, run a private SearxNG instance on the same host and point
`searxng_search` sources at it (default in docs: `http://127.0.0.1:8888`).

See `docs/searxng.md`.

## 1) Install

```bash
git clone <your-repo-url> openinfomate
cd openinfomate
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
```

Tip: if you have a custom SSH deploy script, keep it out of git (operator-specific).

On Debian/Ubuntu you may need:

```bash
apt-get update
apt-get install -y python3-venv
```

## 2) Configure

```bash
cp .env.example .env
vim .env
```

Initialize DB:

```bash
./.venv/bin/tracker db init
```

## 3) Run once (smoke test)

```bash
./.venv/bin/tracker topic list
./.venv/bin/tracker source list
./.venv/bin/tracker run tick
./.venv/bin/tracker run digest
```

## 4) systemd service

### Option A (recommended): user services (no root)

If you can deploy to `~/openinfomate`, the repo includes user-level unit templates plus an installer:

```bash
cd ~/openinfomate
./scripts/install_systemd_user.sh --dir "$HOME/openinfomate"
```

If `systemctl --user` fails, you may need to enable lingering (as root):

```bash
loginctl enable-linger <your-user>
```

### Option B: system services (root)

Create `/etc/systemd/system/tracker.service` (scheduler) (template: `deploy/systemd/system/tracker.service`):

```ini
[Unit]
Description=Tracker (Info Secretary)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/tracker
EnvironmentFile=/path/to/tracker/.env
ExecStart=/path/to/tracker/.venv/bin/tracker service run
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tracker
sudo systemctl status tracker
```

## 5) systemd service (API / Admin)

Create `/etc/systemd/system/tracker-api.service` (template: `deploy/systemd/system/tracker-api.service`):

```ini
[Unit]
Description=Tracker API (Admin UI)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/tracker
EnvironmentFile=/path/to/tracker/.env
ExecStart=/path/to/tracker/.venv/bin/tracker api serve
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Bind host/port via `.env`:

```bash
TRACKER_API_HOST=0.0.0.0
TRACKER_API_PORT=8899
# recommended when binding to 0.0.0.0:
TRACKER_ADMIN_PASSWORD=change-me
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tracker-api
sudo systemctl status tracker-api
```

## Notes
- Digest jobs are re-synced periodically; topic changes take effect automatically.
- Keep `.env` private; do not commit it.
