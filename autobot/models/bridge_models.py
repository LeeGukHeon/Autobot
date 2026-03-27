"""Lightweight bridge models for distilling non-tabular experts into tabular runtime surfaces."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ConstantBridgeModel:
    value: float
    clip_min: float | None = None
    clip_max: float | None = None

    def predict(self, x: np.ndarray) -> np.ndarray:
        rows = int(np.asarray(x).shape[0])
        values = np.full(rows, float(self.value), dtype=np.float64)
        return _clip_values(values, clip_min=self.clip_min, clip_max=self.clip_max)


@dataclass(frozen=True)
class RidgeBridgeModel:
    intercept: float
    coefficients: tuple[float, ...]
    feature_shift: tuple[float, ...]
    feature_scale: tuple[float, ...]
    clip_min: float | None = None
    clip_max: float | None = None

    def predict(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        if matrix.ndim != 2:
            raise ValueError("RidgeBridgeModel expects a 2-D feature matrix")
        shift = np.asarray(self.feature_shift, dtype=np.float64)
        scale = np.asarray(self.feature_scale, dtype=np.float64)
        safe_scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
        normalized = (matrix - shift) / safe_scale
        coeff = np.asarray(self.coefficients, dtype=np.float64)
        values = normalized @ coeff + float(self.intercept)
        return _clip_values(values, clip_min=self.clip_min, clip_max=self.clip_max)


def fit_ridge_bridge(
    x: np.ndarray,
    y: np.ndarray,
    *,
    alpha: float = 1.0,
    clip_min: float | None = None,
    clip_max: float | None = None,
) -> ConstantBridgeModel | RidgeBridgeModel:
    matrix = np.asarray(x, dtype=np.float64)
    target = np.asarray(y, dtype=np.float64).reshape(-1)
    if matrix.ndim != 2:
        raise ValueError("fit_ridge_bridge expects a 2-D feature matrix")
    if matrix.shape[0] != target.shape[0]:
        raise ValueError("bridge feature/target row mismatch")
    if matrix.shape[0] <= 0:
        return ConstantBridgeModel(value=0.0, clip_min=clip_min, clip_max=clip_max)
    if np.allclose(target, target[0]):
        return ConstantBridgeModel(value=float(target[0]), clip_min=clip_min, clip_max=clip_max)

    shift = np.mean(matrix, axis=0)
    scale = np.std(matrix, axis=0, ddof=0)
    safe_scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
    normalized = (matrix - shift) / safe_scale
    design = np.column_stack([np.ones(normalized.shape[0], dtype=np.float64), normalized])
    penalty = np.eye(design.shape[1], dtype=np.float64)
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(
        (design.T @ design) + (float(alpha) * penalty),
        design.T @ target,
    )
    return RidgeBridgeModel(
        intercept=float(beta[0]),
        coefficients=tuple(np.asarray(beta[1:], dtype=np.float64).tolist()),
        feature_shift=tuple(np.asarray(shift, dtype=np.float64).tolist()),
        feature_scale=tuple(np.asarray(safe_scale, dtype=np.float64).tolist()),
        clip_min=clip_min,
        clip_max=clip_max,
    )


def _clip_values(values: np.ndarray, *, clip_min: float | None, clip_max: float | None) -> np.ndarray:
    resolved = np.asarray(values, dtype=np.float64)
    if clip_min is not None or clip_max is not None:
        resolved = np.clip(
            resolved,
            clip_min if clip_min is not None else -np.inf,
            clip_max if clip_max is not None else np.inf,
        )
    return resolved.astype(np.float64, copy=False)
