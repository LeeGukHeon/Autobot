"""HTTP status classification policy for retries and error mapping."""

from __future__ import annotations


def classify_status(status_code: int) -> tuple[str, bool]:
    if status_code in (429, 418):
        return ("rate_limit", True)
    if status_code >= 500:
        return ("server", True)
    if status_code == 401:
        return ("auth", False)
    if status_code in (400, 404, 422):
        return ("validation", False)
    if 400 <= status_code < 500:
        return ("client", False)
    return ("ok", False)
