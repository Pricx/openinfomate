from __future__ import annotations

from pathlib import Path
from typing import Mapping


def _quote_env_value(value: str) -> str:
    """
    Return a dotenv-safe double-quoted value.

    We avoid logging secrets and we keep this intentionally simple:
    - always double-quote
    - escape backslashes and quotes
    - replace newlines with literal "\n"
    """
    s = str(value)
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    s = s.replace("\r\n", "\n").replace("\n", "\\n")
    return f'"{s}"'


def parse_env_assignments(text: str) -> dict[str, str]:
    """
    Parse KEY=VALUE assignments (dotenv-ish) from text.

    - Skips blank lines and comments.
    - Supports optional single/double quotes around the whole value.
    - Best-effort unescape for double-quoted values written by `upsert_env_vars()`.
      This keeps env↔DB sync stable for JSON-ish settings (e.g. TRACKER_*_EXTRA_BODY_JSON).
    - Does not attempt full shell/dotenv expansion; this is an operator helper.
    """
    def _unescape_double_quoted(raw: str) -> str:
        # Minimal dotenv-style unescape: keep it conservative and symmetric with `_quote_env_value()`.
        # - \" -> "
        # - \\ -> \
        # - \n -> newline
        # - \r -> carriage return
        # - \t -> tab
        # Unknown escapes preserve the backslash (e.g. \u1234 stays \u1234).
        out: list[str] = []
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch != "\\":
                out.append(ch)
                i += 1
                continue
            i += 1
            if i >= len(raw):
                out.append("\\")
                break
            esc = raw[i]
            if esc == "n":
                out.append("\n")
            elif esc == "r":
                out.append("\r")
            elif esc == "t":
                out.append("\t")
            elif esc == '"':
                out.append('"')
            elif esc == "\\":
                out.append("\\")
            else:
                out.append("\\" + esc)
            i += 1
        return "".join(out)

    out: dict[str, str] = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not (key[0].isalpha() or key[0] == "_") or not all(ch.isalnum() or ch == "_" for ch in key):
            continue
        if len(value) >= 2 and (value[0] == value[-1] == '"'):
            value = _unescape_double_quoted(value[1:-1])
        elif len(value) >= 2 and (value[0] == value[-1] == "'"):
            value = value[1:-1]
        out[key] = value
    return out


def upsert_env_vars(*, path: Path, updates: Mapping[str, str]) -> None:
    """
    Update or append KEY=VALUE lines in a .env file (preserving unrelated lines).

    Notes:
    - Only the last occurrence of a key is updated; duplicates are left as-is.
    - Values are always written as double-quoted strings for safety.
    """
    if not updates:
        return

    raw_lines: list[str] = []
    if path.exists():
        raw_lines = path.read_text(encoding="utf-8").splitlines(keepends=True)

    # NOTE: `.env` semantics are "last assignment wins" in most loaders.
    # Our `parse_env_assignments()` also keeps the last seen value.
    # Therefore when a key appears multiple times, we must update the *last* occurrence
    # so the effective value actually changes. (Updating the first occurrence would
    # leave the later duplicate overriding it, causing config sync to revert changes.)
    key_to_index: dict[str, int] = {}
    for idx, line in enumerate(raw_lines):
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        # Basic KEY=... matcher (dotenv-compatible key charset).
        if "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if not key or not key[0].isalpha() and key[0] != "_":
            continue
        if not all(ch.isalnum() or ch == "_" for ch in key):
            continue
        key_to_index[key] = idx

    lines = list(raw_lines)
    for key, value in updates.items():
        new_line = f"{key}={_quote_env_value(value)}\n"
        if key in key_to_index:
            lines[key_to_index[key]] = new_line
        else:
            if lines and not lines[-1].endswith("\n"):
                lines[-1] = lines[-1] + "\n"
            lines.append(new_line)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("".join(lines), encoding="utf-8")
    tmp.replace(path)
