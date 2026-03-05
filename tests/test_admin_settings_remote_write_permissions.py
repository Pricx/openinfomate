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
