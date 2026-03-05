from __future__ import annotations

import hashlib
import re


_TOKEN_RE = re.compile(r"[A-Za-z0-9_+#./-]{2,}")
_MASK_64 = (1 << 64) - 1
_SIGN_BIT = 1 << 63


def _tokenize(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text)]


def _hash64(token: str) -> int:
    # stable 64-bit hash via sha1 (first 8 bytes)
    digest = hashlib.sha1(token.encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, "big", signed=False)


def simhash64(text: str) -> int:
    tokens = _tokenize(text)
    if not tokens:
        return 0

    weights = [0] * 64
    for token in tokens:
        h = _hash64(token)
        for bit in range(64):
            if (h >> bit) & 1:
                weights[bit] += 1
            else:
                weights[bit] -= 1

    out = 0
    for bit, w in enumerate(weights):
        if w > 0:
            out |= 1 << bit
    return out


def hamming_distance64(a: int, b: int) -> int:
    return ((a ^ b) & _MASK_64).bit_count()


def int_to_signed64(value: int) -> int:
    value &= _MASK_64
    if value & _SIGN_BIT:
        return value - (1 << 64)
    return value


def signed64_to_int(value: int) -> int:
    return value & _MASK_64
