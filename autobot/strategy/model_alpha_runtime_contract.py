"""Runtime contract resolution helpers for model_alpha_v1."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from autobot.models.predictor import ModelPredictor
from autobot.models.runtime_recommendation_contract import normalize_runtime_recommendations_payload
from autobot.models.selection_policy import DEFAULT_SELECTION_POLICY_MODE, normalize_selection_policy
from autobot.models.trade_action_policy import normalize_trade_action_policy


def resolve_model_alpha_runtime_row_columns(*, predictor: ModelPredictor) -> tuple[str, ...]:
    runtime_recommendations = getattr(predictor, "runtime_recommendations", {}) or {}
    trade_action_policy = normalize_trade_action_policy(
        runtime_recommendations.get("trade_action") if isinstance(runtime_recommendations, dict) else {}
    )
    feature_names = {str(name).strip() for name in getattr(predictor, "feature_columns", ()) if str(name).strip()}
    ordered: list[str] = ["close"]
    for name in (
        "m_trade_events",
        "m_book_events",
        "m_trade_coverage_ms",
        "m_book_coverage_ms",
        "m_trade_max_ts_ms",
        "m_book_max_ts_ms",
        "m_trade_imbalance",
        "m_spread_proxy",
        "m_depth_bid_top5_mean",
        "m_depth_ask_top5_mean",
        "m_micro_available",
        "m_micro_book_available",
    ):
        if name not in ordered and name not in feature_names:
            ordered.append(name)
    state_feature_names = trade_action_policy.get("state_feature_names") or []
    for raw_name in state_feature_names:
        name = str(raw_name).strip()
        if not name or name == "selection_score":
            continue
        if name not in ordered:
            ordered.append(name)
        if name == "atr_pct_14" and "atr_14" not in ordered:
            ordered.append("atr_14")
    risk_feature_name = str(trade_action_policy.get("risk_feature_name", "")).strip()
    if risk_feature_name and risk_feature_name != "selection_score" and risk_feature_name not in ordered:
        ordered.append(risk_feature_name)
        if risk_feature_name == "atr_pct_14" and "atr_14" not in ordered:
            ordered.append("atr_14")
    return tuple(ordered)


def resolve_selection_min_prob(
    *,
    predictor: ModelPredictor,
    settings: Any,
) -> tuple[float, str]:
    manual_min_prob = _safe_optional_float(getattr(settings, "min_prob", None))
    if manual_min_prob is not None:
        return _clamp_prob(manual_min_prob), "manual"

    threshold_key, _ = _resolve_runtime_threshold_key(
        predictor=predictor,
        settings=settings,
    )
    thresholds = predictor.thresholds if isinstance(predictor.thresholds, dict) else {}
    registry_value = _safe_optional_float(thresholds.get(threshold_key))
    if registry_value is not None:
        return _clamp_prob(registry_value), f"registry:{threshold_key}"

    if threshold_key != "top_5pct":
        fallback_value = _safe_optional_float(thresholds.get("top_5pct"))
        if fallback_value is not None:
            return _clamp_prob(fallback_value), "registry:top_5pct_fallback"

    return 0.0, "fallback_zero"


def resolve_selection_top_pct(
    *,
    predictor: ModelPredictor,
    settings: Any,
    selection_policy: dict[str, Any] | None = None,
    selection_policy_source: str = "manual",
) -> tuple[float, str]:
    normalized_policy = dict(selection_policy or {})
    if str(normalized_policy.get("mode", "")).strip().lower() == DEFAULT_SELECTION_POLICY_MODE:
        return max(min(float(normalized_policy.get("selection_fraction", 0.0)), 1.0), 0.0), selection_policy_source
    manual_value = max(min(float(getattr(settings, "top_pct", 0.0)), 1.0), 0.0)
    if not bool(getattr(settings, "use_learned_recommendations", False)):
        return manual_value, "manual"

    recommendation, source = _resolve_selection_recommendation_entry(
        predictor=predictor,
        settings=settings,
    )
    recommended_value = _safe_optional_float(recommendation.get("recommended_top_pct")) if recommendation else None
    if recommended_value is not None:
        return _clamp_prob(recommended_value), source
    return manual_value, "manual_fallback"


def resolve_selection_min_candidates(
    *,
    predictor: ModelPredictor,
    settings: Any,
    selection_policy: dict[str, Any] | None = None,
    selection_policy_source: str = "manual",
) -> tuple[int, str]:
    normalized_policy = dict(selection_policy or {})
    if str(normalized_policy.get("mode", "")).strip().lower() == DEFAULT_SELECTION_POLICY_MODE:
        return max(int(normalized_policy.get("min_candidates_per_ts", 1) or 1), 1), selection_policy_source
    manual_value = max(int(getattr(settings, "min_candidates_per_ts", 0)), 0)
    if not bool(getattr(settings, "use_learned_recommendations", False)):
        return manual_value, "manual"

    recommendation, source = _resolve_selection_recommendation_entry(
        predictor=predictor,
        settings=settings,
    )
    try:
        recommended_value = recommendation.get("recommended_min_candidates_per_ts") if recommendation else None
        if recommended_value is not None:
            return max(int(recommended_value), 0), source
    except (TypeError, ValueError):
        pass
    return manual_value, "manual_fallback"


def resolve_selection_policy(
    *,
    predictor: ModelPredictor,
    settings: Any,
) -> tuple[dict[str, Any], str]:
    selection_policy_payload = getattr(predictor, "selection_policy", {})
    mode = str(getattr(settings, "selection_policy_mode", "auto")).strip().lower() or "auto"
    fallback_threshold_key = str(getattr(settings, "registry_threshold_key", "top_5pct")).strip() or "top_5pct"
    if mode == "raw_threshold":
        return {"mode": "raw_threshold"}, "settings"
    if mode == DEFAULT_SELECTION_POLICY_MODE:
        policy = normalize_selection_policy(
            selection_policy_payload if isinstance(selection_policy_payload, dict) else {},
            fallback_threshold_key=fallback_threshold_key,
        )
        return policy, "registry_selection_policy"
    if _safe_optional_float(getattr(settings, "min_prob", None)) is not None:
        return {"mode": "raw_threshold"}, "manual_min_prob"
    policy_payload = selection_policy_payload if isinstance(selection_policy_payload, dict) else {}
    if policy_payload:
        policy = normalize_selection_policy(
            policy_payload,
            fallback_threshold_key=fallback_threshold_key,
        )
        return policy, "registry_selection_policy"
    return {"mode": "raw_threshold"}, "manual_fallback"


def resolve_runtime_model_alpha_settings(
    *,
    predictor: ModelPredictor,
    settings: Any,
) -> tuple[Any, dict[str, Any]]:
    runtime_recommendations_payload = getattr(predictor, "runtime_recommendations", {})
    runtime_recommendations = normalize_runtime_recommendations_payload(
        runtime_recommendations_payload if isinstance(runtime_recommendations_payload, dict) else {}
    )
    resolved = settings
    state: dict[str, Any] = {
        "runtime_recommendations_available": bool(runtime_recommendations),
        "exit_mode_source": "manual",
        "exit_hold_bars_source": "manual",
        "execution_source": "manual",
    }
    if not runtime_recommendations:
        return resolved, state
    runtime_contract_status = str(runtime_recommendations.get("contract_status", "")).strip()
    runtime_contract_issues = [
        str(item).strip() for item in (runtime_recommendations.get("contract_issues") or []) if str(item).strip()
    ]
    if runtime_contract_status:
        state["runtime_recommendations_contract_status"] = runtime_contract_status
    if runtime_contract_issues:
        state["runtime_recommendations_contract_issues"] = runtime_contract_issues
        if any(issue == "RUNTIME_RECOMMENDATIONS_VERSION_UNSUPPORTED" for issue in runtime_contract_issues):
            return resolved, state

    exit_doc = runtime_recommendations.get("exit")
    exit_contract_valid = isinstance(exit_doc, dict)
    exit_family_compare_supported = True
    if isinstance(exit_doc, dict):
        contract_status = str(exit_doc.get("contract_status") or "").strip()
        contract_issues = [str(item).strip() for item in (exit_doc.get("contract_issues") or []) if str(item).strip()]
        backfilled_fields = [
            str(item).strip() for item in (exit_doc.get("contract_backfilled_fields") or []) if str(item).strip()
        ]
        state["exit_recommendation"] = {
            "recommended_exit_mode": str(exit_doc.get("recommended_exit_mode", "")).strip().lower(),
            "recommended_exit_mode_source": str(exit_doc.get("recommended_exit_mode_source", "")).strip(),
            "recommended_exit_mode_reason_code": str(exit_doc.get("recommended_exit_mode_reason_code", "")).strip(),
            "recommended_hold_bars": (
                int(exit_doc.get("recommended_hold_bars"))
                if exit_doc.get("recommended_hold_bars") not in (None, "")
                else None
            ),
            "chosen_family": str(exit_doc.get("chosen_family", "")).strip(),
            "chosen_rule_id": str(exit_doc.get("chosen_rule_id", "")).strip(),
            "hold_family_status": str(exit_doc.get("hold_family_status", "")).strip(),
            "risk_family_status": str(exit_doc.get("risk_family_status", "")).strip(),
            "family_compare_status": str(exit_doc.get("family_compare_status", "")).strip(),
            "family_compare_reason_codes": [
                str(item).strip()
                for item in ((exit_doc.get("family_compare") or {}).get("reason_codes") or [])
                if str(item).strip()
            ],
            "path_risk": dict(exit_doc.get("path_risk") or {}) if isinstance(exit_doc.get("path_risk"), dict) else {},
        }
        family_compare_status = str(exit_doc.get("family_compare_status") or "").strip()
        state["exit_family_compare_status"] = family_compare_status or "missing"
        exit_family_compare_supported = family_compare_status.lower() in {"", "supported", "legacy_backfilled"}
        if not exit_family_compare_supported:
            state["exit_family_compare_reason_codes"] = [
                str(item).strip()
                for item in ((exit_doc.get("family_compare") or {}).get("reason_codes") or [])
                if str(item).strip()
            ]
        if contract_status:
            state["exit_contract_status"] = contract_status
        if contract_issues:
            state["exit_contract_issues"] = contract_issues
            exit_contract_valid = False
        if backfilled_fields:
            state["exit_contract_backfilled_fields"] = backfilled_fields

    if (
        isinstance(exit_doc, dict)
        and exit_contract_valid
        and exit_family_compare_supported
        and bool(getattr(settings.exit, "use_learned_exit_mode", False))
    ):
        recommended_exit_mode = str(exit_doc.get("recommended_exit_mode", "")).strip().lower()
        if recommended_exit_mode in {"hold", "risk"}:
            resolved = replace(
                resolved,
                exit=replace(
                    resolved.exit,
                    mode=recommended_exit_mode,
                ),
            )
            state["exit_mode_source"] = str(
                exit_doc.get("recommended_exit_mode_source", "runtime_recommendation")
            )
            state["exit_mode_recommendation"] = {
                "recommended_exit_mode": recommended_exit_mode,
                "recommended_exit_mode_source": state["exit_mode_source"],
                "recommended_exit_mode_reason_code": str(
                    exit_doc.get("recommended_exit_mode_reason_code", "")
                ).strip(),
                "exit_mode_compare": dict(exit_doc.get("exit_mode_compare", {}))
                if isinstance(exit_doc.get("exit_mode_compare"), dict)
                else {},
            }
    if (
        isinstance(exit_doc, dict)
        and exit_contract_valid
        and exit_family_compare_supported
        and bool(getattr(settings.exit, "use_learned_hold_bars", False))
    ):
        recommended_hold_bars = exit_doc.get("recommended_hold_bars")
        try:
            if recommended_hold_bars is not None and int(recommended_hold_bars) > 0:
                resolved = replace(
                    resolved,
                    exit=replace(
                        resolved.exit,
                        hold_bars=max(int(recommended_hold_bars), 1),
                    ),
                )
                state["exit_hold_bars_source"] = str(
                    exit_doc.get("recommendation_source", "runtime_recommendation")
                )
                state["exit_recommendation"] = dict(exit_doc)
        except (TypeError, ValueError):
            pass

    if (
        isinstance(exit_doc, dict)
        and exit_contract_valid
        and exit_family_compare_supported
        and bool(getattr(settings.exit, "use_learned_risk_recommendations", False))
    ):
        try:
            recommended_scaling_mode = str(
                exit_doc.get("recommended_risk_scaling_mode", resolved.exit.risk_scaling_mode)
            ).strip().lower() or str(resolved.exit.risk_scaling_mode)
            recommended_vol_feature = str(
                exit_doc.get("recommended_risk_vol_feature", resolved.exit.risk_vol_feature)
            ).strip() or str(resolved.exit.risk_vol_feature)
            recommended_tp_mult = _safe_optional_float(
                exit_doc.get("recommended_tp_vol_multiplier", resolved.exit.tp_vol_multiplier)
            )
            recommended_sl_mult = _safe_optional_float(
                exit_doc.get("recommended_sl_vol_multiplier", resolved.exit.sl_vol_multiplier)
            )
            recommended_trailing_mult = _safe_optional_float(
                exit_doc.get("recommended_trailing_vol_multiplier", resolved.exit.trailing_vol_multiplier)
            )
            resolved = replace(
                resolved,
                exit=replace(
                    resolved.exit,
                    risk_scaling_mode=recommended_scaling_mode,
                    risk_vol_feature=recommended_vol_feature,
                    tp_vol_multiplier=recommended_tp_mult,
                    sl_vol_multiplier=recommended_sl_mult,
                    trailing_vol_multiplier=recommended_trailing_mult,
                ),
            )
            state["exit_risk_source"] = str(
                exit_doc.get("recommendation_source", "runtime_recommendation")
            )
            state["exit_risk_recommendation"] = {
                "recommended_risk_scaling_mode": recommended_scaling_mode,
                "recommended_risk_vol_feature": recommended_vol_feature,
                "recommended_tp_vol_multiplier": recommended_tp_mult,
                "recommended_sl_vol_multiplier": recommended_sl_mult,
                "recommended_trailing_vol_multiplier": recommended_trailing_mult,
            }
        except (TypeError, ValueError):
            pass

    execution_doc = runtime_recommendations.get("execution")
    if isinstance(execution_doc, dict) and bool(getattr(settings.execution, "use_learned_recommendations", False)):
        try:
            recommended_price_mode = str(
                execution_doc.get("recommended_price_mode", resolved.execution.price_mode)
            ).strip() or str(resolved.execution.price_mode)
            recommended_timeout_bars = max(
                int(execution_doc.get("recommended_timeout_bars", resolved.execution.timeout_bars)),
                1,
            )
            recommended_replace_max = max(
                int(execution_doc.get("recommended_replace_max", resolved.execution.replace_max)),
                0,
            )
            resolved = replace(
                resolved,
                execution=replace(
                    resolved.execution,
                    price_mode=recommended_price_mode,
                    timeout_bars=recommended_timeout_bars,
                    replace_max=recommended_replace_max,
                ),
            )
            state["execution_source"] = str(
                execution_doc.get("recommendation_source", "runtime_recommendation")
            )
            state["execution_recommendation"] = dict(execution_doc)
        except (TypeError, ValueError):
            pass

    risk_control_doc = runtime_recommendations.get("risk_control")
    if isinstance(risk_control_doc, dict):
        contract_status = str(risk_control_doc.get("contract_status", "")).strip()
        contract_issues = [
            str(item).strip() for item in (risk_control_doc.get("contract_issues") or []) if str(item).strip()
        ]
        live_gate = dict(risk_control_doc.get("live_gate") or {})
        if contract_status:
            state["risk_control_contract_status"] = contract_status
        if contract_issues:
            state["risk_control_contract_issues"] = contract_issues
        state["risk_control"] = {
            "status": str(risk_control_doc.get("status", "")).strip(),
            "operating_mode": str(risk_control_doc.get("operating_mode", "")).strip(),
            "decision_metric_name": str(risk_control_doc.get("decision_metric_name", "")).strip(),
            "selected_threshold": _safe_optional_float(risk_control_doc.get("selected_threshold")),
            "selected_coverage": _safe_optional_int(risk_control_doc.get("selected_coverage")),
            "selected_nonpositive_rate_ucb": _safe_optional_float(
                risk_control_doc.get("selected_nonpositive_rate_ucb")
            ),
            "selected_severe_loss_rate_ucb": _safe_optional_float(
                risk_control_doc.get("selected_severe_loss_rate_ucb")
            ),
            "live_gate_enabled": bool(live_gate.get("enabled", False)),
            "live_gate_mode": str(live_gate.get("mode", "")).strip(),
            "live_gate_metric_name": str(live_gate.get("metric_name", "")).strip(),
            "live_gate_skip_reason_code": str(live_gate.get("skip_reason_code", "")).strip(),
            "subgroup_feature_name": str(((risk_control_doc.get("subgroup_family") or {}).get("feature_name")) or "").strip(),
            "subgroup_bucket_count_effective": _safe_optional_int(
                ((risk_control_doc.get("subgroup_family") or {}).get("bucket_count_effective"))
            ),
            "subgroup_min_coverage": _safe_optional_int(
                ((risk_control_doc.get("subgroup_family") or {}).get("min_coverage"))
            ),
            "size_ladder_status": str(((risk_control_doc.get("size_ladder") or {}).get("status")) or "").strip(),
            "size_ladder_global_max_multiplier": _safe_optional_float(
                ((risk_control_doc.get("size_ladder") or {}).get("global_max_multiplier"))
            ),
            "size_ladder_feature_name": str(((risk_control_doc.get("size_ladder") or {}).get("feature_name")) or "").strip(),
            "weighting_mode": str(((risk_control_doc.get("weighting") or {}).get("mode")) or "").strip(),
            "weighting_half_life_windows": _safe_optional_float(
                ((risk_control_doc.get("weighting") or {}).get("half_life_windows"))
            ),
            "weighting_covariate_similarity_mode": str(
                (((risk_control_doc.get("weighting") or {}).get("covariate_similarity") or {}).get("mode")) or ""
            ).strip(),
            "weighting_density_ratio_mode": str(
                (((risk_control_doc.get("weighting") or {}).get("density_ratio") or {}).get("mode")) or ""
            ).strip(),
            "weighting_density_ratio_classifier_status": str(
                (((risk_control_doc.get("weighting") or {}).get("density_ratio") or {}).get("classifier_status")) or ""
            ).strip(),
            "weighting_density_ratio_clip_fraction": _safe_optional_float(
                (((risk_control_doc.get("weighting") or {}).get("density_ratio") or {}).get("clip_fraction"))
            ),
            "online_adaptation_mode": str(((risk_control_doc.get("online_adaptation") or {}).get("mode")) or "").strip(),
            "online_adaptation_lookback_trades": _safe_optional_int(
                ((risk_control_doc.get("online_adaptation") or {}).get("lookback_trades"))
            ),
            "online_adaptation_max_step_up": _safe_optional_int(
                ((risk_control_doc.get("online_adaptation") or {}).get("max_step_up"))
            ),
            "online_adaptation_martingale_halt_threshold": _safe_optional_float(
                ((risk_control_doc.get("online_adaptation") or {}).get("martingale_halt_threshold"))
            ),
            "online_adaptation_martingale_escalation_threshold": _safe_optional_float(
                ((risk_control_doc.get("online_adaptation") or {}).get("martingale_escalation_threshold"))
            ),
            "online_adaptation_martingale_clear_threshold": _safe_optional_float(
                ((risk_control_doc.get("online_adaptation") or {}).get("martingale_clear_threshold"))
            ),
            "online_adaptation_martingale_halt_reason_code": str(
                ((risk_control_doc.get("online_adaptation") or {}).get("martingale_halt_reason_code")) or ""
            ).strip(),
            "online_adaptation_martingale_critical_reason_code": str(
                ((risk_control_doc.get("online_adaptation") or {}).get("martingale_critical_reason_code")) or ""
            ).strip(),
        }

    return resolved, state


def build_runtime_exit_recommendation_meta(runtime_state: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict((runtime_state or {}).get("exit_recommendation") or {})
    if not payload:
        return {}
    return {
        "recommended_exit_mode": str(payload.get("recommended_exit_mode", "")).strip(),
        "recommended_exit_mode_source": str(payload.get("recommended_exit_mode_source", "")).strip(),
        "recommended_exit_mode_reason_code": str(payload.get("recommended_exit_mode_reason_code", "")).strip(),
        "recommended_hold_bars": payload.get("recommended_hold_bars"),
        "chosen_family": str(payload.get("chosen_family", "")).strip(),
        "chosen_rule_id": str(payload.get("chosen_rule_id", "")).strip(),
        "hold_family_status": str(payload.get("hold_family_status", "")).strip(),
        "risk_family_status": str(payload.get("risk_family_status", "")).strip(),
        "family_compare_status": str(payload.get("family_compare_status", "")).strip(),
        "family_compare_reason_codes": [
            str(item).strip() for item in (payload.get("family_compare_reason_codes") or []) if str(item).strip()
        ],
        "path_risk": dict(payload.get("path_risk") or {}) if isinstance(payload.get("path_risk"), dict) else {},
    }


def _resolve_selection_recommendation_entry(
    *,
    predictor: ModelPredictor,
    settings: Any,
) -> tuple[dict[str, Any], str]:
    recommendations = predictor.selection_recommendations if isinstance(predictor.selection_recommendations, dict) else {}
    by_key = recommendations.get("by_threshold_key")
    if not isinstance(by_key, dict):
        return {}, "manual_fallback"

    threshold_key, threshold_key_source = _resolve_runtime_threshold_key(
        predictor=predictor,
        settings=settings,
    )
    entry = by_key.get(threshold_key)
    if isinstance(entry, dict):
        source = f"registry_recommendation:{threshold_key}"
        if threshold_key_source == "registry_recommendation":
            source += ":learned_threshold_key"
        return entry, source

    if threshold_key != "top_5pct":
        fallback_entry = by_key.get("top_5pct")
        if isinstance(fallback_entry, dict):
            return fallback_entry, "registry_recommendation:top_5pct_fallback"
    return {}, "manual_fallback"


def _resolve_runtime_threshold_key(
    *,
    predictor: ModelPredictor,
    settings: Any,
) -> tuple[str, str]:
    default_key = str(getattr(settings, "registry_threshold_key", "top_5pct")).strip() or "top_5pct"
    if not bool(getattr(settings, "use_learned_recommendations", False)):
        return default_key, "settings"

    recommendations = predictor.selection_recommendations if isinstance(predictor.selection_recommendations, dict) else {}
    recommended_key = str(recommendations.get("recommended_threshold_key", "")).strip()
    by_key = recommendations.get("by_threshold_key") if isinstance(recommendations.get("by_threshold_key"), dict) else {}
    if recommended_key and isinstance(by_key, dict) and isinstance(by_key.get(recommended_key), dict):
        return recommended_key, "registry_recommendation"
    return default_key, "settings"


def _clamp_prob(value: float) -> float:
    return max(min(float(value), 1.0), 0.0)


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
