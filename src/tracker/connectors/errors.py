from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


def _norm_host(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    try:
        host = (urlsplit(raw).netloc or raw).strip().lower()
    except Exception:
        host = raw
    host = host.split("/", 1)[0].split(":", 1)[0].lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


@dataclass(frozen=True)
class TemporaryFetchBlockError(RuntimeError):
    """
    Raised when a public source is temporarily blocked/throttled by anti-bot or edge protection.
    """

    url: str
    status_code: int | None = None
    final_url: str | None = None
    retry_after_seconds: int | None = None
    reason: str | None = None

    @property
    def host(self) -> str:
        return _norm_host((self.final_url or self.url or "").strip())

    def __str__(self) -> str:
        return str(self.reason or "temporary fetch blocked")

    def meta(self) -> dict[str, str]:
        out: dict[str, str] = {"error_type": "temporary_block"}
        host = self.host
        if host:
            out["host"] = host
        if self.url:
            out["url"] = self.url
        if self.final_url:
            out["final_url"] = self.final_url
        if self.status_code is not None:
            out["status_code"] = str(int(self.status_code))
        if self.retry_after_seconds is not None:
            out["retry_after_seconds"] = str(max(0, int(self.retry_after_seconds)))
        if self.reason:
            out["reason"] = str(self.reason)
        return out
