from __future__ import annotations

from typing import Any


DEFAULT_SELECTION_POLICY_MODE = "rank_effective_quantile"
DEFAULT_SELECTION_POLICY_VERSION = 1
_DEFAULT_THRESHOLD_KEY = "top_5pct"
_THRESHOLD_KEY_TO_FRACTION: dict[str, float] = {
    "top_1pct": 0.01,
    "top_5pct": 0.05,
    "top_10pct": 0.10,
}


def build_selection_policy_from_recommendations(
    *,
    selection_recommendations: dict[str, Any] | None,
    fallback_threshold_key: str = _DEFAULT_THRESHOLD_KEY,
    forced_threshold_key: str | None = None,
    score_source: str = "score_mean",
) -> dict[str, Any]:
    recommendations = dict(selection_recommendations or {})
    by_key = recommendations.get("by_threshold_key")
    if not isinstance(by_key, dict) or not by_key:
        return _fallback_policy(threshold_key=fallback_threshold_key, source="manual_fallback")

    forced_key = str(forced_threshold_key or "").strip()
    requested_key = forced_key or str(recommendations.get("recommended_threshold_key", "")).strip()
    threshold_key = (
        requested_key
        if requested_key and isinstance(by_key.get(requested_key), dict)
        else str(fallback_threshold_key).strip() or _DEFAULT_THRESHOLD_KEY
    )
    entry = by_key.get(threshold_key)
    if not isinstance(entry, dict):
        fallback_entry = by_key.get(_DEFAULT_THRESHOLD_KEY)
        if isinstance(fallback_entry, dict):
            threshold_key = _DEFAULT_THRESHOLD_KEY
            entry = fallback_entry
        else:
            first_key = next(iter(sorted(by_key.keys())), "")
            first_entry = by_key.get(first_key)
            if isinstance(first_entry, dict):
                threshold_key = str(first_key).strip() or _DEFAULT_THRESHOLD_KEY
                entry = first_entry
            else:
                return _fallback_policy(threshold_key=fallback_threshold_key, source="manual_fallback")

    recommended_top_pct = _clamp_fraction(entry.get("recommended_top_pct"), default=1.0)
    eligible_ratio = _clamp_fraction(
        entry.get("eligible_ratio"),
        default=_THRESHOLD_KEY_TO_FRACTION.get(threshold_key, 0.05),
    )
    selected_fraction = _clamp_fraction(float(eligible_ratio) * float(recommended_top_pct), default=0.05)
    min_candidates = max(_coerce_int(entry.get("recommended_min_candidates_per_ts"), default=1), 1)
    if selected_fraction <= 0.0:
        selected_fraction = _THRESHOLD_KEY_TO_FRACTION.get(threshold_key, 0.05)
    return {
        "version": DEFAULT_SELECTION_POLICY_VERSION,
        "mode": DEFAULT_SELECTION_POLICY_MODE,
        "selection_fraction": float(selected_fraction),
        "min_candidates_per_ts": int(min_candidates),
        "threshold_key": threshold_key,
        "threshold_value": _safe_optional_float(entry.get("threshold")),
        "recommended_top_pct": float(recommended_top_pct),
        "eligible_ratio": float(eligible_ratio),
        "selection_fraction_source": "eligible_ratio_x_recommended_top_pct",
        "selection_recommendation_source": str(
            (f"forced_threshold_key:{threshold_key}" if forced_key and threshold_key == forced_key else "")
            or entry.get("recommendation_source")
            or recommendations.get("recommended_threshold_key_source")
            or "manual_fallback"
        ),
        "objective": str(entry.get("objective") or ""),
        "objective_score": _safe_optional_float(entry.get("objective_score")),
        "constraint_reasons": list(entry.get("constraint_reasons") or []),
        "windows_covered": _coerce_int(entry.get("windows_covered"), default=0),
        "window_count": _coerce_int(entry.get("window_count"), default=0),
        "selected_rows_mean": _safe_optional_float(entry.get("selected_rows_mean")),
        "fallback_used": bool(entry.get("fallback_used", False)),
        "forced_threshold_key": forced_key if forced_key and threshold_key == forced_key else "",
        "score_source": str(score_source).strip() or "score_mean",
    }


def normalize_selection_policy(
    policy: dict[str, Any] | None,
    *,
    fallback_threshold_key: str = _DEFAULT_THRESHOLD_KEY,
) -> dict[str, Any]:
    payload = dict(policy or {})
    mode = str(payload.get("mode", "")).strip().lower()
    if mode == "raw_threshold":
        payload["version"] = int(payload.get("version") or DEFAULT_SELECTION_POLICY_VERSION)
        payload["mode"] = "raw_threshold"
        payload["threshold_key"] = str(payload.get("threshold_key", "")).strip() or (
            str(fallback_threshold_key).strip() or _DEFAULT_THRESHOLD_KEY
        )
        payload["tradeable_prob_min"] = _safe_optional_float(payload.get("tradeable_prob_min"))
        payload["expected_net_edge_bps_min"] = _safe_optional_float(payload.get("expected_net_edge_bps_min"))
        payload["recommended_top_pct"] = _clamp_fraction(payload.get("recommended_top_pct"), default=0.0)
        payload["min_candidates_per_ts"] = max(_coerce_int(payload.get("min_candidates_per_ts"), default=1), 1)
        payload["constraint_reasons"] = list(payload.get("constraint_reasons") or [])
        payload["score_source"] = str(payload.get("score_source", "score_mean")).strip() or "score_mean"
        payload["selection_recommendation_source"] = str(
            payload.get("selection_recommendation_source", "manual_threshold")
        ).strip() or "manual_threshold"
        payload["fallback_used"] = bool(payload.get("fallback_used", False))
        return payload
    if mode == DEFAULT_SELECTION_POLICY_MODE:
        selected_fraction = _clamp_fraction(payload.get("selection_fraction"), default=0.05)
        min_candidates = max(_coerce_int(payload.get("min_candidates_per_ts"), default=1), 1)
        payload["version"] = int(payload.get("version") or DEFAULT_SELECTION_POLICY_VERSION)
        payload["mode"] = DEFAULT_SELECTION_POLICY_MODE
        payload["selection_fraction"] = float(selected_fraction)
        payload["min_candidates_per_ts"] = int(min_candidates)
        payload["threshold_key"] = str(payload.get("threshold_key", "")).strip() or (
            str(fallback_threshold_key).strip() or _DEFAULT_THRESHOLD_KEY
        )
        payload["recommended_top_pct"] = _clamp_fraction(payload.get("recommended_top_pct"), default=1.0)
        payload["eligible_ratio"] = _clamp_fraction(
            payload.get("eligible_ratio"),
            default=payload["selection_fraction"],
        )
        payload["constraint_reasons"] = list(payload.get("constraint_reasons") or [])
        payload["score_source"] = str(payload.get("score_source", "score_mean")).strip() or "score_mean"
        return payload
    return _fallback_policy(threshold_key=fallback_threshold_key, source="manual_fallback")


def _fallback_policy(*, threshold_key: str, source: str) -> dict[str, Any]:
    resolved_key = str(threshold_key).strip() or _DEFAULT_THRESHOLD_KEY
    fallback_fraction = _THRESHOLD_KEY_TO_FRACTION.get(resolved_key, 0.05)
    return {
        "version": DEFAULT_SELECTION_POLICY_VERSION,
        "mode": DEFAULT_SELECTION_POLICY_MODE,
        "selection_fraction": float(fallback_fraction),
        "min_candidates_per_ts": 1,
        "threshold_key": resolved_key,
        "threshold_value": None,
        "recommended_top_pct": 1.0,
        "eligible_ratio": float(fallback_fraction),
        "selection_fraction_source": "threshold_key_default_fraction",
        "selection_recommendation_source": str(source),
        "objective": "",
        "objective_score": None,
        "constraint_reasons": ["NO_SELECTION_POLICY_ARTIFACT"],
        "windows_covered": 0,
        "window_count": 0,
        "selected_rows_mean": None,
        "fallback_used": True,
        "score_source": "score_mean",
    }


def _clamp_fraction(value: Any, *, default: float) -> float:
    parsed = _safe_optional_float(value)
    if parsed is None:
        return float(default)
    return max(min(float(parsed), 1.0), 0.0)


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(value)
    except (TypeError, ValueError):
        return int(default)
