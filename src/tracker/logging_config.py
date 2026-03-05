from __future__ import annotations

import logging


def configure_logging(*, level: str = "INFO") -> None:
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Avoid leaking secrets in HTTP client logs (e.g., webhook access tokens in URLs).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
