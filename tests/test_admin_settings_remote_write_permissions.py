from __future__ import annotations

from pathlib import Path

import asyncio
import httpx

from tracker.api import create_app
from tracker.settings import Settings


def test_admin_settings_patch_remote_write_denied(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        admin_allow_remote_env_update=False,
    )
    app = create_app(settings)

    async def _run() -> int:
        transport = httpx.ASGITransport(app=app, client=("8.8.8.8", 12345))
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r = await client.post(
                "/admin/settings/patch?token=secret&section=config",
                data={"output_language": "zh"},
                follow_redirects=False,
            )
        return int(r.status_code)

    assert asyncio.run(_run()) == 403


def test_admin_settings_patch_remote_write_allowed(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        admin_allow_remote_env_update=True,
    )
    app = create_app(settings)

    async def _run() -> int:
        transport = httpx.ASGITransport(app=app, client=("8.8.8.8", 12345))
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r = await client.post(
                "/admin/settings/patch?token=secret&section=config",
                data={"output_language": "en"},
                follow_redirects=False,
            )
        return int(r.status_code)

    assert asyncio.run(_run()) == 303
    assert 'TRACKER_OUTPUT_LANGUAGE="en"' in env_path.read_text(encoding="utf-8")


def test_admin_settings_patch_allows_docker_gateway_as_localhost(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        admin_allow_remote_env_update=False,
    )

    # Simulate the "host accesses published port" case in Docker bridge mode:
    # request.client.host becomes the container default gateway (e.g. 172.17.0.1).
    #
    # `create_app()` will read `/proc/net/route` on Linux to detect that gateway.
    # In unit tests, we monkeypatch that file content.
    from pathlib import Path as _Path

    real_exists = _Path.exists
    real_read_text = _Path.read_text

    def fake_exists(self):  # noqa: ANN001
        if str(self) == "/proc/net/route":
            return True
        return real_exists(self)

    def fake_read_text(self, *args, **kwargs):  # noqa: ANN001
        if str(self) == "/proc/net/route":
            # Gateway 010011AC -> 172.17.0.1 (little-endian)
            return (
                "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n"
                "eth0\t00000000\t010011AC\t0003\t0\t0\t0\t00000000\t0\t0\t0\n"
            )
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(_Path, "exists", fake_exists, raising=True)
    monkeypatch.setattr(_Path, "read_text", fake_read_text, raising=True)

    app = create_app(settings)

    async def _run() -> int:
        transport = httpx.ASGITransport(app=app, client=("172.17.0.1", 12345))
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r = await client.post(
                "/admin/settings/patch?token=secret&section=config",
                data={"output_language": "zh"},
                follow_redirects=False,
            )
        return int(r.status_code)

    # Without remote env update enabled, we still allow the Docker gateway address as "local".
    assert asyncio.run(_run()) == 303
