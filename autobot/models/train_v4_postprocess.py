"""Post-processing helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any, Callable

import numpy as np


def build_lane_governance_v4(
    *,
    task: str,
    run_scope: str,
    economic_objective_profile: dict[str, Any],
    normalize_run_scope_fn: Callable[[str], str],
) -> dict[str, Any]:
    task_name = str(task).strip().lower() or "cls"
    normalized_run_scope = normalize_run_scope_fn(run_scope)
    if task_name == "rank":
        if "rank_governed" in normalized_run_scope or "rank_promotable" in normalized_run_scope:
            lane_id = "rank_governed_primary"
            lane_role = "production_candidate"
            shadow_only = False
            promotion_allowed = True
            live_replacement_allowed = False
            governance_reasons = ["AUTO_GOVERNED_FROM_RANK_SHADOW_PASS"]
        else:
            lane_id = "rank_shadow"
            lane_role = "shadow"
            shadow_only = True
            promotion_allowed = False
            live_replacement_allowed = False
            governance_reasons = ["RANK_LANE_SHADOW_EVALUATION_ONLY", "EXPLICIT_GOVERNANCE_DECISION_REQUIRED"]
    elif task_name == "cls":
        lane_id = "cls_primary"
        lane_role = "primary"
        shadow_only = False
        promotion_allowed = True
        live_replacement_allowed = True
        governance_reasons = ["PRIMARY_LANE_ELIGIBLE"]
    else:
        lane_id = f"{task_name}_research"
        lane_role = "research"
        shadow_only = False
        promotion_allowed = False
        live_replacement_allowed = False
        governance_reasons = ["NON_PRIMARY_LANE_REQUIRES_EXPLICIT_GOVERNANCE"]
    return {
        "version": 1,
        "policy": "v4_lane_governance_v1",
        "lane_id": lane_id,
        "task": task_name,
        "run_scope": normalized_run_scope,
        "lane_role": lane_role,
        "shadow_only": bool(shadow_only),
        "production_lane_id": "cls_primary",
        "production_task": "cls",
        "comparison_lane_id": "cls_primary" if task_name == "rank" else "",
        "promotion_allowed": bool(promotion_allowed),
        "live_replacement_allowed": bool(live_replacement_allowed),
        "certification_contract_frozen": True,
        "frozen_contract_family": "t21_11_to_t21_16",
        "economic_objective_profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
        or "v4_shared_economic_objective_v3",
        "governance_reasons": list(governance_reasons),
    }


def attach_ranking_metrics(
    *,
    metrics: dict[str, Any],
    y_rank: np.ndarray,
    ts_ms: np.ndarray,
    scores: np.ndarray,
) -> dict[str, Any]:
    payload = dict(metrics) if isinstance(metrics, dict) else {}
    payload["ranking"] = evaluate_ranking_metrics(
        y_rank=np.asarray(y_rank, dtype=np.float64),
        ts_ms=np.asarray(ts_ms, dtype=np.int64),
        scores=np.asarray(scores, dtype=np.float64),
    )
    return payload


def evaluate_ranking_metrics(
    *,
    y_rank: np.ndarray,
    ts_ms: np.ndarray,
    scores: np.ndarray,
) -> dict[str, Any]:
    rank_values = np.nan_to_num(np.asarray(y_rank, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    score_values = np.asarray(scores, dtype=np.float64)
    if rank_values.size <= 0 or ts_values.size <= 0 or score_values.size <= 0:
        return {
            "ts_group_count": 0,
            "eligible_group_count": 0,
            "mean_group_size": 0.0,
            "min_group_size": 0,
            "max_group_size": 0,
            "ndcg_at_5_mean": 0.0,
            "ndcg_full_mean": 0.0,
            "top1_match_rate": 0.0,
        }

    ndcg_at5_values: list[float] = []
    ndcg_full_values: list[float] = []
    top1_matches = 0
    group_sizes: list[int] = []
    eligible_groups = 0
    for _, indices in group_indices_by_ts(ts_values):
        if indices.size <= 0:
            continue
        group_sizes.append(int(indices.size))
        true_values = np.maximum(rank_values[indices], 0.0)
        pred_values = score_values[indices]
        if indices.size == 1:
            eligible_groups += 1
            ndcg_at5_values.append(1.0)
            ndcg_full_values.append(1.0)
            top1_matches += 1
            continue
        eligible_groups += 1
        ndcg_at5_values.append(ndcg_at_k(true_values, pred_values, k=min(5, int(indices.size))))
        ndcg_full_values.append(ndcg_at_k(true_values, pred_values, k=int(indices.size)))
        top1_matches += int(int(np.argmax(pred_values)) == int(np.argmax(true_values)))
    if not group_sizes:
        return {
            "ts_group_count": 0,
            "eligible_group_count": 0,
            "mean_group_size": 0.0,
            "min_group_size": 0,
            "max_group_size": 0,
            "ndcg_at_5_mean": 0.0,
            "ndcg_full_mean": 0.0,
            "top1_match_rate": 0.0,
        }
    return {
        "ts_group_count": len(group_sizes),
        "eligible_group_count": int(eligible_groups),
        "mean_group_size": float(np.mean(np.asarray(group_sizes, dtype=np.float64))),
        "min_group_size": int(min(group_sizes)),
        "max_group_size": int(max(group_sizes)),
        "ndcg_at_5_mean": float(np.mean(np.asarray(ndcg_at5_values, dtype=np.float64))) if ndcg_at5_values else 0.0,
        "ndcg_full_mean": float(np.mean(np.asarray(ndcg_full_values, dtype=np.float64))) if ndcg_full_values else 0.0,
        "top1_match_rate": float(top1_matches) / float(max(eligible_groups, 1)),
    }


def group_indices_by_ts(ts_ms: np.ndarray) -> list[tuple[int, np.ndarray]]:
    values = np.asarray(ts_ms, dtype=np.int64)
    if values.size <= 0:
        return []
    unique, inverse = np.unique(values, return_inverse=True)
    grouped: list[tuple[int, np.ndarray]] = []
    for group_idx, ts_value in enumerate(unique):
        grouped.append((int(ts_value), np.flatnonzero(inverse == group_idx).astype(np.int64, copy=False)))
    return grouped


def ndcg_at_k(relevance: np.ndarray, scores: np.ndarray, *, k: int) -> float:
    rel = np.asarray(relevance, dtype=np.float64)
    pred = np.asarray(scores, dtype=np.float64)
    if rel.size <= 0 or pred.size <= 0:
        return 0.0
    top_k = max(min(int(k), int(rel.size)), 1)
    order = np.argsort(-pred, kind="mergesort")[:top_k]
    ideal = np.argsort(-rel, kind="mergesort")[:top_k]
    dcg = _dcg(rel[order])
    idcg = _dcg(rel[ideal])
    if idcg <= 1e-12:
        return 0.0
    return float(dcg / idcg)


def build_leaderboard_row_v4(
    *,
    run_id: str,
    options: Any,
    task: str,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    cls = test_metrics.get("classification", {}) if isinstance(test_metrics, dict) else {}
    ranking = test_metrics.get("ranking", {}) if isinstance(test_metrics, dict) else {}
    top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
    backend = "xgboost_ranker" if task == "rank" else "xgboost_regressor" if task == "reg" else "xgboost"
    return {
        "run_id": run_id,
        "created_at_utc": utc_now(),
        "model_family": options.model_family,
        "trainer": "v4_crypto_cs",
        "task": task,
        "champion": "booster",
        "champion_backend": backend,
        "test_roc_auc": safe_float(cls.get("roc_auc")),
        "test_pr_auc": safe_float(cls.get("pr_auc")),
        "test_log_loss": safe_float(cls.get("log_loss")),
        "test_brier_score": safe_float(cls.get("brier_score")),
        "test_ndcg_at5": safe_float(ranking.get("ndcg_at_5_mean")),
        "test_top1_match_rate": safe_float(ranking.get("top1_match_rate")),
        "test_precision_top5": safe_float(top5.get("precision")),
        "test_ev_net_top5": safe_float(top5.get("ev_net")),
        "rows_train": int(rows.get("train", 0)),
        "rows_valid": int(rows.get("valid", 0)),
        "rows_test": int(rows.get("test", 0)),
    }


def load_champion_walk_forward_report(*, options: Any, load_json_fn: Callable[[Path], dict[str, Any]]) -> dict[str, Any] | None:
    champion_doc = load_json_fn(options.registry_root / options.model_family / "champion.json")
    champion_run_id = str(champion_doc.get("run_id", "")).strip()
    if not champion_run_id:
        return None
    run_dir = options.registry_root / options.model_family / champion_run_id
    walk_forward = load_json_fn(run_dir / "walk_forward_report.json")
    if isinstance(walk_forward.get("summary"), dict) and walk_forward.get("summary"):
        return dict(walk_forward)
    metrics = load_json_fn(run_dir / "metrics.json")
    summary = metrics.get("walk_forward") if isinstance(metrics, dict) else None
    if isinstance(summary, dict) and summary:
        return {
            "summary": dict(summary),
            "windows": [],
            "compare_to_champion": {},
            "spa_like_window_test": {},
        }
    return None


def detect_duplicate_candidate_artifacts(
    *,
    options: Any,
    run_id: str,
    run_dir: Path,
    load_json_fn: Callable[[Path], dict[str, Any]],
) -> dict[str, Any]:
    champion_doc = load_json_fn(options.registry_root / options.model_family / "champion.json")
    champion_run_id = str(champion_doc.get("run_id", "")).strip()
    candidate_model_path = run_dir / "model.bin"
    candidate_thresholds_path = run_dir / "thresholds.json"
    champion_run_dir = options.registry_root / options.model_family / champion_run_id if champion_run_id else Path("")
    champion_model_path = champion_run_dir / "model.bin" if champion_run_id else Path("")
    champion_thresholds_path = champion_run_dir / "thresholds.json" if champion_run_id else Path("")

    duplicate = False
    if champion_run_id and candidate_model_path.is_file() and candidate_thresholds_path.is_file():
        duplicate = (
            champion_model_path.is_file()
            and champion_thresholds_path.is_file()
            and sha256_file(candidate_model_path) == sha256_file(champion_model_path)
            and sha256_file(candidate_thresholds_path) == sha256_file(champion_thresholds_path)
        )

    reasons: list[str] = []
    if not champion_run_id:
        reasons.append("NO_EXISTING_CHAMPION")
    elif duplicate:
        reasons.append("ARTIFACT_HASH_MATCH")
    return {
        "evaluated": bool(champion_run_id),
        "duplicate": duplicate,
        "basis": "model_bin_and_thresholds_sha256",
        "candidate_ref": run_id,
        "champion_ref": champion_run_id,
        "candidate": {
            "run_dir": str(run_dir),
            "model_bin_path": str(candidate_model_path),
            "model_bin_sha256": sha256_file(candidate_model_path),
            "thresholds_path": str(candidate_thresholds_path),
            "thresholds_sha256": sha256_file(candidate_thresholds_path),
        },
        "champion": {
            "run_dir": str(champion_run_dir) if champion_run_id else "",
            "model_bin_path": str(champion_model_path) if champion_run_id else "",
            "model_bin_sha256": sha256_file(champion_model_path) if champion_run_id else "",
            "thresholds_path": str(champion_thresholds_path) if champion_run_id else "",
            "thresholds_sha256": sha256_file(champion_thresholds_path) if champion_run_id else "",
        },
        "reasons": reasons,
    }


def finalize_walk_forward_report(
    *,
    walk_forward: dict[str, Any],
    selection_recommendations: dict[str, Any],
    options: Any,
    summarize_walk_forward_trial_panel_fn: Callable[..., list[dict[str, Any]]],
    build_selection_search_trial_panel_fn: Callable[..., list[dict[str, Any]]],
    summarize_walk_forward_windows_fn: Callable[..., dict[str, Any]],
    load_champion_walk_forward_report_fn: Callable[..., dict[str, Any] | None],
    resolve_walk_forward_report_threshold_key_fn: Callable[..., str],
    compare_balanced_pareto_fn: Callable[..., dict[str, Any]],
    compare_spa_like_window_test_fn: Callable[..., dict[str, Any]],
    build_trial_window_differential_diagnostics_fn: Callable[..., dict[str, Any]],
    build_trial_window_differential_matrix_fn: Callable[..., Any],
    run_white_reality_check_fn: Callable[..., dict[str, Any]],
    run_hansen_spa_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    report = dict(walk_forward)
    selected_threshold_key, threshold_key_source = resolve_selection_recommendation_threshold_key(
        selection_recommendations=selection_recommendations,
    )
    report["selected_threshold_key"] = selected_threshold_key
    report["selected_threshold_key_source"] = threshold_key_source
    windows = report.get("windows", []) if isinstance(report.get("windows"), list) else []
    base_trial_panel = summarize_walk_forward_trial_panel_fn(windows, threshold_key=selected_threshold_key)
    selection_trial_panel = build_selection_search_trial_panel_fn(
        windows=windows,
        start_trial_id=max([int(row.get("trial", -1)) for row in base_trial_panel] + [-1]) + 1,
    )
    report["trial_panel"] = base_trial_panel + selection_trial_panel
    report["selection_search_trial_count"] = len(selection_trial_panel)
    report["summary"] = summarize_walk_forward_windows_fn(windows, threshold_key=selected_threshold_key)

    champion_report = load_champion_walk_forward_report_fn(options=options)
    champion_summary = champion_report.get("summary", {}) if isinstance(champion_report, dict) else {}
    champion_threshold_key = resolve_walk_forward_report_threshold_key_fn(
        champion_report,
        fallback_threshold_key=selected_threshold_key,
    )
    report["compare_to_champion"] = compare_balanced_pareto_fn(report["summary"], champion_summary or {})
    report["spa_like_window_test"] = compare_spa_like_window_test_fn(
        report.get("windows", []),
        champion_report.get("windows", []) if isinstance(champion_report, dict) else [],
        candidate_threshold_key=selected_threshold_key,
        champion_threshold_key=champion_threshold_key,
    )
    multiple_testing_panel_diagnostics = build_trial_window_differential_diagnostics_fn(
        report.get("trial_panel", []),
        champion_report.get("windows", []) if isinstance(champion_report, dict) else [],
        champion_threshold_key=champion_threshold_key,
    )
    report["multiple_testing_panel_diagnostics"] = multiple_testing_panel_diagnostics
    rc_matrix = build_trial_window_differential_matrix_fn(
        report.get("trial_panel", []),
        champion_report.get("windows", []) if isinstance(champion_report, dict) else [],
        champion_threshold_key=champion_threshold_key,
    )
    report["white_reality_check"] = run_white_reality_check_fn(
        rc_matrix,
        alpha=float(options.multiple_testing_alpha),
        bootstrap_iters=max(int(options.multiple_testing_bootstrap_iters), 100),
        seed=int(options.seed),
        average_block_length=(int(options.multiple_testing_block_length) if int(options.multiple_testing_block_length) > 0 else None),
        diagnostics=multiple_testing_panel_diagnostics,
    )
    report["hansen_spa"] = run_hansen_spa_fn(
        rc_matrix,
        alpha=float(options.multiple_testing_alpha),
        bootstrap_iters=max(int(options.multiple_testing_bootstrap_iters), 100),
        seed=int(options.seed),
        average_block_length=(int(options.multiple_testing_block_length) if int(options.multiple_testing_block_length) > 0 else None),
        diagnostics=multiple_testing_panel_diagnostics,
    )
    if champion_summary:
        report["champion_summary"] = champion_summary
        report["champion_selected_threshold_key"] = champion_threshold_key
    return report


def resolve_selection_recommendation_threshold_key(
    *,
    selection_recommendations: dict[str, Any],
) -> tuple[str, str]:
    threshold_key = str(selection_recommendations.get("recommended_threshold_key", "")).strip()
    if threshold_key:
        return threshold_key, str(selection_recommendations.get("recommended_threshold_key_source", "walk_forward_objective_optimizer"))
    return "top_5pct", "manual_fallback"


def resolve_walk_forward_report_threshold_key(
    walk_forward_report: dict[str, Any] | None,
    *,
    fallback_threshold_key: str = "top_5pct",
) -> str:
    report = dict(walk_forward_report or {})
    threshold_key = str(report.get("selected_threshold_key", "")).strip()
    if threshold_key:
        return threshold_key
    summary = report.get("summary")
    if isinstance(summary, dict):
        threshold_key = str(summary.get("selected_threshold_key", "")).strip()
        if threshold_key:
            return threshold_key
    return str(fallback_threshold_key).strip() or "top_5pct"


def sha256_file(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dcg(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=np.float64)
    if arr.size <= 0:
        return 0.0
    positions = np.arange(2, arr.size + 2, dtype=np.float64)
    gains = np.power(2.0, arr) - 1.0
    discounts = np.log2(positions)
    return float(np.sum(gains / discounts))
