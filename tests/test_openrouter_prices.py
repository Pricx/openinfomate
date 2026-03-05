from __future__ import annotations

from tracker.openrouter_prices import _compute_prices_summary


def test_compute_prices_summary_groups_vendors_and_picks_top() -> None:
    payload = {
        "data": [
            {"id": "openai/gpt-x", "pricing": {"prompt": "0.000001", "completion": "0.000002"}},
            {"id": "openai/gpt-y", "pricing": {"prompt": "0.000010", "completion": "0.000020"}},
            {"id": "anthropic/claude-a", "pricing": {"prompt": "0.000003", "completion": "0.000015"}},
            {"id": "google/gemini-a", "pricing": {"prompt": 0.0000005, "completion": 0.000001}},
            {"id": "deepseek/deepseek-a", "pricing": {"prompt": "0.0000002", "completion": "0.0000002"}},
            {"id": "zhipu/glm-a", "pricing": {"prompt": "0.0000001", "completion": "0.0000002"}},
            {"id": "minimax/abab-a", "pricing": {"prompt": "0.0000009", "completion": "0.0000011"}},
            # invalid / ignored
            {"id": "openai/no-pricing"},
            {"id": "", "pricing": {"prompt": "0.1", "completion": "0.1"}},
            {"id": "openai/bad", "pricing": {"prompt": "x", "completion": "y"}},
        ]
    }

    out = _compute_prices_summary(payload)
    assert isinstance(out, dict)
    vendors = out.get("vendors")
    assert isinstance(vendors, list)
    # We always output all configured vendors.
    assert len(vendors) >= 6

    openai = next(v for v in vendors if v.get("prefix") == "openai/")
    assert openai.get("count") == 2
    assert openai.get("min_in") == 1.0  # 0.000001 * 1e6
    assert openai.get("min_out") == 2.0
    top = openai.get("top")
    assert isinstance(top, list) and top
    assert top[0]["id"] == "openai/gpt-x"

