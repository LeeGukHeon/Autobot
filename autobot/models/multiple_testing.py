from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

import numpy as np


@dataclass(frozen=True)
class TrialWindowMatrix:
    trial_ids: list[int]
    window_indices: list[int]
    panel_keys: list[str]
    differential_matrix: np.ndarray


@dataclass(frozen=True)
class BlockLengthSelection:
    average_block_length: int
    source: str
    threshold: float | None = None
    cutoff_lag: int | None = None
    dependence_strength: float | None = None


def build_trial_window_differential_matrix(
    candidate_trial_panel: list[dict[str, Any]] | None,
    champion_windows: list[dict[str, Any]] | None,
) -> TrialWindowMatrix | None:
    candidate_rows = list(candidate_trial_panel or [])
    champion_slice_map = _extract_champion_slice_map(champion_windows or [])
    if not candidate_rows or not champion_slice_map:
        return None

    trial_panel_maps: list[tuple[int, dict[str, float]]] = []
    common_keys: set[str] | None = None
    for record in candidate_rows:
        trial_id = int(record.get("trial", -1))
        if trial_id < 0:
            continue
        slices = _extract_trial_slice_map(record)
        shared = sorted(set(slices).intersection(champion_slice_map))
        if not shared:
            continue
        if common_keys is None:
            common_keys = set(shared)
        else:
            common_keys &= set(shared)
        trial_panel_maps.append((trial_id, slices))

    if not trial_panel_maps or not common_keys:
        return None

    panel_keys = sorted(common_keys)
    if len(panel_keys) < 2:
        return None

    matrix_rows: list[list[float]] = []
    trial_ids: list[int] = []
    for trial_id, record_map in trial_panel_maps:
        if not all(key in record_map for key in panel_keys):
            continue
        trial_ids.append(trial_id)
        matrix_rows.append([record_map[key] - champion_slice_map[key] for key in panel_keys])

    if len(trial_ids) < 2 or not matrix_rows:
        return None
    return TrialWindowMatrix(
        trial_ids=trial_ids,
        window_indices=[_parse_window_index_from_panel_key(key) for key in panel_keys],
        panel_keys=panel_keys,
        differential_matrix=np.asarray(matrix_rows, dtype=np.float64),
    )


def run_white_reality_check(
    matrix: TrialWindowMatrix | None,
    *,
    bootstrap_iters: int = 500,
    alpha: float = 0.20,
    seed: int = 42,
    average_block_length: int | str | None = None,
) -> dict[str, Any]:
    if matrix is None:
        return _insufficient("white_reality_check")
    diffs = matrix.differential_matrix
    trial_count, window_count = diffs.shape
    if trial_count < 2 or window_count < 2:
        return _insufficient("white_reality_check")

    means = diffs.mean(axis=1)
    observed = math.sqrt(window_count) * max(float(np.max(means)), 0.0)
    rng = np.random.default_rng(int(seed))
    centered = diffs - means[:, None]
    bootstrap_iters_eff = max(int(bootstrap_iters), 100)
    block_length = _resolve_average_block_length(matrix, average_block_length)
    exceed = 0
    for _ in range(bootstrap_iters_eff):
        sample = _stationary_bootstrap_indices(window_count, block_length.average_block_length, rng)
        boot = centered[:, sample]
        stat = math.sqrt(window_count) * max(float(np.max(boot.mean(axis=1))), 0.0)
        if stat >= observed - 1e-12:
            exceed += 1
    p_value = float(exceed + 1) / float(bootstrap_iters_eff + 1)
    best_idx = int(np.argmax(means))
    decision = "candidate_edge" if means[best_idx] > 0.0 and p_value <= float(alpha) else "indeterminate"
    return {
        "policy": "white_reality_check",
        "comparable": True,
        "decision": decision,
        "candidate_edge": decision == "candidate_edge",
        "alpha": float(alpha),
        "bootstrap_iters": int(bootstrap_iters_eff),
        "bootstrap_method": "stationary",
        "average_block_length": int(block_length.average_block_length),
        "block_length_source": str(block_length.source),
        "block_length_threshold": _float_or_none(block_length.threshold),
        "block_length_cutoff_lag": _int_or_none(block_length.cutoff_lag),
        "block_length_dependence_strength": _float_or_none(block_length.dependence_strength),
        "trial_count": int(trial_count),
        "window_count": int(window_count),
        "best_trial": int(matrix.trial_ids[best_idx]),
        "best_mean_diff_ev_net": float(means[best_idx]),
        "p_value": p_value,
        "reasons": ["WHITE_RC_PASS" if decision == "candidate_edge" else "WHITE_RC_HOLD"],
    }


def run_hansen_spa(
    matrix: TrialWindowMatrix | None,
    *,
    bootstrap_iters: int = 500,
    alpha: float = 0.20,
    seed: int = 42,
    average_block_length: int | str | None = None,
) -> dict[str, Any]:
    if matrix is None:
        return _insufficient("hansen_spa")
    diffs = matrix.differential_matrix
    trial_count, window_count = diffs.shape
    if trial_count < 2 or window_count < 2:
        return _insufficient("hansen_spa")

    means = diffs.mean(axis=1)
    stds = diffs.std(axis=1, ddof=1)
    stds = np.where(stds <= 1e-12, 1e-12, stds)
    observed_stats = np.sqrt(window_count) * means / stds
    observed = max(float(np.max(observed_stats)), 0.0)
    rng = np.random.default_rng(int(seed))
    bootstrap_iters_eff = max(int(bootstrap_iters), 100)
    block_length = _resolve_average_block_length(matrix, average_block_length)
    exceed = 0
    mu_c = _hansen_sample_dependent_null(means=means, stds=stds, observations=window_count)
    for _ in range(bootstrap_iters_eff):
        sample = _stationary_bootstrap_indices(window_count, block_length.average_block_length, rng)
        boot = diffs[:, sample] - mu_c[:, None]
        stat = max(float(np.max(np.sqrt(window_count) * boot.mean(axis=1) / stds)), 0.0)
        if stat >= observed - 1e-12:
            exceed += 1
    p_value = float(exceed + 1) / float(bootstrap_iters_eff + 1)
    best_idx = int(np.argmax(observed_stats))
    decision = "candidate_edge" if means[best_idx] > 0.0 and p_value <= float(alpha) else "indeterminate"
    return {
        "policy": "hansen_spa",
        "comparable": True,
        "decision": decision,
        "candidate_edge": decision == "candidate_edge",
        "alpha": float(alpha),
        "bootstrap_iters": int(bootstrap_iters_eff),
        "bootstrap_method": "stationary",
        "average_block_length": int(block_length.average_block_length),
        "block_length_source": str(block_length.source),
        "block_length_threshold": _float_or_none(block_length.threshold),
        "block_length_cutoff_lag": _int_or_none(block_length.cutoff_lag),
        "block_length_dependence_strength": _float_or_none(block_length.dependence_strength),
        "trial_count": int(trial_count),
        "window_count": int(window_count),
        "best_trial": int(matrix.trial_ids[best_idx]),
        "best_mean_diff_ev_net": float(means[best_idx]),
        "p_value": p_value,
        "reasons": ["HANSEN_SPA_PASS" if decision == "candidate_edge" else "HANSEN_SPA_HOLD"],
    }


def _insufficient(policy: str) -> dict[str, Any]:
    return {
        "policy": policy,
        "comparable": False,
        "decision": "insufficient_evidence",
        "candidate_edge": False,
        "reasons": ["INSUFFICIENT_COMMON_TRIAL_WINDOWS"],
    }


def _extract_trial_slice_map(record: dict[str, Any]) -> dict[str, float]:
    windows = record.get("windows") or []
    candidate_slice_map: dict[str, float] = {}
    for window in windows:
        if not isinstance(window, dict):
            continue
        window_index = int(window.get("window_index", -1))
        oos_slices = window.get("oos_slices") or []
        if oos_slices:
            for slice_doc in oos_slices:
                if not isinstance(slice_doc, dict):
                    continue
                slice_index = int(slice_doc.get("slice_index", -1))
                if window_index < 0 or slice_index < 0:
                    continue
                slice_key = _compose_panel_key(window_index, slice_index)
                candidate_slice_map[slice_key] = _safe_float(
                    (((slice_doc.get("metrics") or {}).get("trading") or {}).get("top_5pct") or {}).get("ev_net")
                )
        elif window_index >= 0:
            candidate_slice_map[_compose_panel_key(window_index, 0)] = _safe_float(
                (((window.get("metrics") or {}).get("trading") or {}).get("top_5pct") or {}).get("ev_net")
            )
    return candidate_slice_map


def _extract_champion_slice_map(windows: list[dict[str, Any]]) -> dict[str, float]:
    champion_slice_map: dict[str, float] = {}
    for window in windows:
        if not isinstance(window, dict):
            continue
        window_index = int(window.get("window_index", -1))
        if window_index < 0:
            continue
        oos_slices = window.get("oos_slices") or []
        if oos_slices:
            for slice_doc in oos_slices:
                if not isinstance(slice_doc, dict):
                    continue
                slice_index = int(slice_doc.get("slice_index", -1))
                if slice_index < 0:
                    continue
                champion_slice_map[_compose_panel_key(window_index, slice_index)] = _safe_float(
                    (((slice_doc.get("metrics") or {}).get("trading") or {}).get("top_5pct") or {}).get("ev_net")
                )
        else:
            champion_slice_map[_compose_panel_key(window_index, 0)] = _safe_float(
                (((window.get("metrics") or {}).get("trading") or {}).get("top_5pct") or {}).get("ev_net")
            )
    return champion_slice_map


def _compose_panel_key(window_index: int, slice_index: int) -> str:
    return f"{int(window_index)}:{int(slice_index)}"


def _parse_window_index_from_panel_key(panel_key: str) -> int:
    try:
        return int(str(panel_key).split(":", 1)[0])
    except Exception:
        return -1


def _resolve_average_block_length(
    matrix: TrialWindowMatrix,
    requested: int | str | None,
) -> BlockLengthSelection:
    count = max(int(matrix.differential_matrix.shape[1]), 1)
    if requested is not None and str(requested).strip().lower() != "auto":
        try:
            manual = int(requested)
        except Exception:
            manual = 0
        if manual > 0:
            return BlockLengthSelection(
                average_block_length=max(1, min(manual, count)),
                source="manual_override",
            )
    return _auto_select_average_block_length(matrix)


def _auto_select_average_block_length(matrix: TrialWindowMatrix) -> BlockLengthSelection:
    diffs = np.asarray(matrix.differential_matrix, dtype=np.float64)
    trial_count, window_count = diffs.shape
    if trial_count < 1 or window_count < 2:
        return BlockLengthSelection(
            average_block_length=max(1, min(window_count, 2)),
            source="auto_fallback_short_panel",
        )

    max_lag = min(window_count - 1, max(3, int(round(math.sqrt(window_count) * 2.0))))
    lag_count = min(window_count - 1, max_lag)
    if lag_count < 1:
        return BlockLengthSelection(
            average_block_length=max(1, min(window_count, 2)),
            source="auto_fallback_short_panel",
        )

    _, avg_autocorr = _estimate_average_autocovariance_profile(diffs, lag_count)
    if avg_autocorr.size == 0:
        return BlockLengthSelection(
            average_block_length=max(1, min(window_count, 2)),
            source="auto_fallback_no_dependence_signal",
        )

    threshold = 2.0 * math.sqrt(max(math.log10(float(max(window_count, 10))), 1e-12) / float(window_count))
    consecutive_lags = max(2, int(math.ceil(math.sqrt(max(math.log(float(window_count)), 1.0)))))
    cutoff_lag = _find_dependence_cutoff_lag(avg_autocorr, threshold, consecutive_lags)
    dependence_strength = (
        float(np.mean(np.clip(avg_autocorr[:cutoff_lag], a_min=0.0, a_max=None))) if cutoff_lag > 0 else 0.0
    )

    taper_lag = max(1, min(cutoff_lag, lag_count))
    positive_corr_mass = 0.0
    for lag in range(1, taper_lag + 1):
        weight = _flat_top_weight(lag, taper_lag)
        positive_corr_mass += float(weight) * max(float(avg_autocorr[lag - 1]), 0.0)

    if not np.isfinite(positive_corr_mass) or positive_corr_mass <= 1e-12:
        auto_length = max(2, min(window_count, 2))
        source = "auto_fallback_weak_dependence"
    else:
        auto_length = int(round(1.0 + 2.0 * positive_corr_mass))
        auto_length = max(2, min(window_count, auto_length))
        source = "auto_dependence_selector"

    return BlockLengthSelection(
        average_block_length=auto_length,
        source=source,
        threshold=threshold,
        cutoff_lag=cutoff_lag,
        dependence_strength=dependence_strength,
    )


def _estimate_average_autocovariance_profile(
    diffs: np.ndarray,
    max_lag: int,
) -> tuple[np.ndarray, np.ndarray]:
    rows, observations = diffs.shape
    lag_limit = min(max_lag, observations - 1)
    if rows < 1 or lag_limit < 1:
        return np.asarray([], dtype=np.float64), np.asarray([], dtype=np.float64)

    autocovariances: list[float] = []
    autocorrelations: list[float] = []
    for lag in range(1, lag_limit + 1):
        covs: list[float] = []
        corrs: list[float] = []
        for row in diffs:
            series = np.asarray(row, dtype=np.float64)
            if series.size <= lag:
                continue
            centered = series - float(np.mean(series))
            variance = float(np.var(centered, ddof=1))
            if not np.isfinite(variance) or variance <= 1e-12:
                continue
            left = centered[:-lag]
            right = centered[lag:]
            if left.size < 2 or right.size < 2:
                continue
            cov = float(np.dot(left, right) / float(left.size))
            covs.append(cov)
            corrs.append(cov / variance)
        if covs:
            autocovariances.append(float(np.mean(covs)))
            autocorrelations.append(float(np.mean(corrs)))
        else:
            autocovariances.append(0.0)
            autocorrelations.append(0.0)
    return np.asarray(autocovariances, dtype=np.float64), np.asarray(autocorrelations, dtype=np.float64)


def _find_dependence_cutoff_lag(
    avg_autocorr: np.ndarray,
    threshold: float,
    consecutive_lags: int,
) -> int:
    lag_count = int(avg_autocorr.size)
    if lag_count < 1:
        return 0
    consec = max(1, min(int(consecutive_lags), lag_count))
    for start in range(0, lag_count - consec + 1):
        window = np.abs(avg_autocorr[start : start + consec])
        if np.all(window < float(threshold)):
            return max(1, start)
    return lag_count


def _flat_top_weight(lag: int, bandwidth: int) -> float:
    if bandwidth <= 1:
        return 1.0
    scaled = float(lag) / float(max(bandwidth, 1))
    if scaled <= 0.5:
        return 1.0
    if scaled <= 1.0:
        return 2.0 * (1.0 - scaled)
    return 0.0


def _stationary_bootstrap_indices(
    observations: int,
    average_block_length: int,
    rng: np.random.Generator,
) -> np.ndarray:
    n = max(int(observations), 1)
    block_length = max(int(average_block_length), 1)
    restart_prob = 1.0 / float(block_length)
    sample = np.empty(n, dtype=np.int64)
    sample[0] = int(rng.integers(0, n))
    for idx in range(1, n):
        if float(rng.random()) < restart_prob:
            sample[idx] = int(rng.integers(0, n))
        else:
            sample[idx] = (int(sample[idx - 1]) + 1) % n
    return sample


def _hansen_sample_dependent_null(
    *,
    means: np.ndarray,
    stds: np.ndarray,
    observations: int,
) -> np.ndarray:
    n = max(int(observations), 2)
    if n <= math.e:
        return np.zeros_like(means)
    threshold = -math.sqrt(2.0 * math.log(math.log(float(n))))
    t_stats = np.sqrt(float(n)) * means / stds
    return np.where(t_stats <= threshold, means, 0.0)


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _float_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _int_or_none(value: int | None) -> int | None:
    if value is None:
        return None
    return int(value)
