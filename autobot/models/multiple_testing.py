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
    average_block_length: int | None = None,
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
    block_length = _resolve_average_block_length(window_count, average_block_length)
    exceed = 0
    for _ in range(bootstrap_iters_eff):
        sample = _stationary_bootstrap_indices(window_count, block_length, rng)
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
        "average_block_length": int(block_length),
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
    average_block_length: int | None = None,
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
    block_length = _resolve_average_block_length(window_count, average_block_length)
    exceed = 0
    mu_c = _hansen_sample_dependent_null(means=means, stds=stds, observations=window_count)
    for _ in range(bootstrap_iters_eff):
        sample = _stationary_bootstrap_indices(window_count, block_length, rng)
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
        "average_block_length": int(block_length),
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


def _resolve_average_block_length(window_count: int, requested: int | None) -> int:
    count = max(int(window_count), 1)
    if requested is not None and int(requested) > 0:
        return max(1, min(int(requested), count))
    return max(2, min(count, int(round(math.sqrt(count)))))


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
