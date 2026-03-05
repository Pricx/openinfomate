from __future__ import annotations

import logging
from collections.abc import Callable

from sqlalchemy.orm import Session, sessionmaker

from tracker.repo import Repo

logger = logging.getLogger(__name__)

UsagePayload = dict
UsageCallback = Callable[[UsagePayload], None]


def make_llm_usage_recorder(*, session: Session) -> UsageCallback:
    """
    Return a best-effort callback that persists OpenAI-compatible token usage.

    We intentionally write usage rows in a *separate* DB session to avoid interfering
    with the caller's transaction/rollback flow.
    """
    bind = session.get_bind()
    engine = getattr(bind, "engine", bind)  # Connection -> Engine; Engine stays Engine.
    make_session = sessionmaker(bind=engine)

    def _cb(payload: UsagePayload) -> None:
        if not isinstance(payload, dict):
            return
        try:
            kind = str(payload.get("kind") or "")
            model = str(payload.get("model") or "")
            topic = str(payload.get("topic") or "")
            prompt_tokens = int(payload.get("prompt_tokens") or 0)
            completion_tokens = int(payload.get("completion_tokens") or 0)
            total_tokens = int(payload.get("total_tokens") or 0)
        except Exception:
            logger.debug("invalid usage payload: %r", payload, exc_info=True)
            return

        try:
            with make_session() as s:
                Repo(s).add_llm_usage(
                    kind=kind,
                    model=model,
                    topic=topic,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                )
                s.commit()
        except Exception:
            # Observability must never break the main pipeline.
            logger.debug("failed to record llm usage", exc_info=True)

    return _cb


def estimate_llm_cost_usd(
    *,
    prompt_tokens: int,
    completion_tokens: int,
    input_per_million_usd: float,
    output_per_million_usd: float,
) -> float | None:
    """
    Estimate USD cost using simple per-1M token pricing.

    Returns None when no pricing is configured.
    """
    in_price = float(input_per_million_usd or 0.0)
    out_price = float(output_per_million_usd or 0.0)
    if in_price <= 0.0 and out_price <= 0.0:
        return None
    pt = max(0, int(prompt_tokens or 0))
    ct = max(0, int(completion_tokens or 0))
    return (pt / 1_000_000.0) * in_price + (ct / 1_000_000.0) * out_price

