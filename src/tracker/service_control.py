from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


ALLOWED_SYSTEMD_USER_UNITS: set[str] = {
    # Core services.
    "tracker",
    "tracker-api",
    # Alternative unit names (useful for running multiple instances).
    "openinfomate",
    "openinfomate-api",
    # Optional forwards (often installed together).
    "tracker-api-compat-8080",
    "tracker-llm-compat-8400",
}


@dataclass(frozen=True)
class RestartResult:
    ok: bool
    units: list[str]
    queued: bool
    message: str
    command: str


def _normalize_units(raw: str | Sequence[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        # Accept: comma/space/newline separated.
        parts: list[str] = []
        for token in s.replace("\n", ",").replace("\r", ",").split(","):
            t = token.strip()
            if not t:
                continue
            for w in t.split():
                ww = w.strip()
                if ww:
                    parts.append(ww)
        raw_list = parts
    else:
        raw_list = [str(x or "").strip() for x in raw]
    out: list[str] = []
    seen = set()
    for u in raw_list:
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _filter_allowed(units: Sequence[str]) -> tuple[list[str], list[str]]:
    allowed: list[str] = []
    denied: list[str] = []
    for u in units:
        if u in ALLOWED_SYSTEMD_USER_UNITS:
            allowed.append(u)
        else:
            denied.append(u)
    return allowed, denied


def restart_command(units: Sequence[str]) -> str:
    uu = _normalize_units(units)
    if not uu:
        uu = ["tracker", "tracker-api"]
    return "systemctl --user restart " + " ".join(shlex.quote(u) for u in uu)


def can_restart_systemd_user() -> bool:
    if not shutil.which("systemctl"):
        return False
    ok, _ = _systemctl_user_preflight()
    return ok


def _running_in_docker() -> bool:
    try:
        return Path("/.dockerenv").exists()
    except Exception:
        return False


def _systemctl_user_preflight() -> tuple[bool, str]:
    """
    Return whether `systemctl --user` is *actually usable* (can connect to the user manager).

    Avoid "false queued" restarts caused by errors like:
    - Failed to connect to bus
    """
    if not shutil.which("systemctl"):
        if _running_in_docker():
            return False, "restart is not supported inside the container"
        return False, "systemctl not found"
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show-environment"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=3,
            text=True,
        )
    except Exception as exc:
        return False, f"systemctl --user check failed: {exc}"

    if proc.returncode == 0:
        return True, ""

    err = (proc.stderr or "").strip()
    if not err:
        err = f"systemctl --user failed with code {proc.returncode}"
    err = err.splitlines()[0].strip()
    return False, err


def queue_restart_systemd_user(
    *,
    units: Sequence[str],
    delay_seconds: float = 1.5,
) -> RestartResult:
    """
    Queue a best-effort `systemctl --user restart ...` in the background.

    Important:
    - We intentionally detach the restart command so the HTTP request can return
      before `tracker-api` is restarted (otherwise the response can be cut off).
    """
    wanted = _normalize_units(units)
    if not wanted:
        wanted = ["tracker", "tracker-api"]
    allowed, denied = _filter_allowed(wanted)
    cmd = restart_command(allowed or wanted)

    if denied:
        return RestartResult(
            ok=False,
            queued=False,
            units=allowed,
            message=f"denied units: {', '.join(denied)}",
            command=cmd,
        )

    if not allowed:
        return RestartResult(
            ok=False,
            queued=False,
            units=[],
            message="no units selected",
            command=cmd,
        )

    ok, msg = _systemctl_user_preflight()
    if not ok:
        return RestartResult(
            ok=False,
            queued=False,
            units=list(allowed),
            message=(msg or "systemctl --user is not available in this environment"),
            command=cmd,
        )

    # Detach: use a tiny shell wrapper so the restart continues even if the current
    # process is restarted.
    #
    # - `nohup` + `&` keeps it running
    # - redirect to /dev/null to avoid holding stdio
    #
    # NOTE: we avoid `timeout` here; systemctl should be fast for restart requests.
    # NOTE: Web Admin uses debounced autosave (≈700ms) for most non-secret settings.
    # Keep restart delay comfortably above that so "toggle → restart" doesn't drop changes.
    delay_s = max(0.0, float(delay_seconds or 0.0))
    shell_cmd = f"nohup sh -c {shlex.quote(f'sleep {delay_s:.2f}; {cmd}')} >/dev/null 2>&1 &"
    try:
        subprocess.Popen(
            ["bash", "-lc", shell_cmd],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            env=dict(os.environ),
        )
        return RestartResult(
            ok=True,
            queued=True,
            units=list(allowed),
            message="restart queued",
            command=cmd,
        )
    except Exception as exc:
        return RestartResult(
            ok=False,
            queued=False,
            units=list(allowed),
            message=f"failed to queue restart: {exc}",
            command=cmd,
        )


def restart_hint_text(*, lang: str, units: Sequence[str] | None = None) -> str:
    lang_norm = (lang or "").strip().lower()
    if not shutil.which("systemctl"):
        if lang_norm == "zh":
            return "手动重启：请在宿主机重启容器（例如 docker compose restart）。"
        return "Manual restart: restart from the host (e.g. docker compose restart)."
    cmd = restart_command(list(units or ["tracker", "tracker-api"]))
    if lang_norm == "zh":
        return f"手动重启命令：{cmd}"
    return f"Manual restart command: {cmd}"
