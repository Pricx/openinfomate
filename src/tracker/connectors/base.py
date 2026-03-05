from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FetchedEntry:
    url: str
    title: str
    published_at_iso: str | None = None
    summary: str | None = None


class Connector:
    type: str

    async def fetch(self, *, url: str) -> list[FetchedEntry]:
        raise NotImplementedError

