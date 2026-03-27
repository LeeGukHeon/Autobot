"""PyTorch-based v5 sequence trainer on top of sequence_v1 tensor contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any

import joblib
import numpy as np
import polars as pl
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

from autobot import __version__ as autobot_version

from .bridge_models import fit_ridge_bridge
from .metrics import classification_metrics, grouped_trading_metrics, trading_metrics
from .model_card import render_model_card
from .registry import RegistrySavePayload, make_run_id, save_run, update_artifact_status, update_latest_pointer
from .selection_calibration import _identity_calibration
from .selection_policy import build_selection_policy_from_recommendations
from .split import compute_time_splits, split_masks
from .train_v1 import _build_thresholds, build_selection_recommendations


DEFAULT_HORIZONS_MINUTES: tuple[int, ...] = (3, 6, 12, 24)
DEFAULT_QUANTILES: tuple[float, ...] = (0.1, 0.5, 0.9)
VALID_BACKBONES = ("patchtst", "timemixer", "tft")
VALID_PRETRAIN_METHODS = ("ts2vec_like", "timemae_like", "none")


@dataclass(frozen=True)
class TrainV5SequenceOptions:
    dataset_root: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    quote: str
    top_n: int
    start: str
    end: str
    seed: int
    backbone_family: str = "patchtst"
    pretrain_method: str = "ts2vec_like"
    batch_size: int = 16
    pretrain_epochs: int = 1
    finetune_epochs: int = 5
    learning_rate: float = 1e-3
    train_ratio: float = 0.6
    valid_ratio: float = 0.2
    test_ratio: float = 0.2
    horizons_minutes: tuple[int, ...] = DEFAULT_HORIZONS_MINUTES
    quantile_levels: tuple[float, ...] = DEFAULT_QUANTILES
    hidden_dim: int = 64
    regime_embedding_dim: int = 8
    patch_len: int = 4
    patch_stride: int = 2
    weight_decay: float = 1e-4
    run_scope: str = "manual_sequence_expert"


@dataclass(frozen=True)
class TrainV5SequenceResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    promotion_path: Path
    walk_forward_report_path: Path
    sequence_model_contract_path: Path
    predictor_contract_path: Path


@dataclass
class _SequenceSamples:
    second: np.ndarray
    minute: np.ndarray
    micro: np.ndarray
    lob: np.ndarray
    lob_global: np.ndarray
    known_covariates: np.ndarray
    y_cls: np.ndarray
    y_reg_primary: np.ndarray
    y_rank: np.ndarray
    y_reg_multi: np.ndarray
    sample_weight: np.ndarray
    ts_ms: np.ndarray
    markets: np.ndarray
    pooled_features: np.ndarray
    feature_names: tuple[str, ...]
    selected_markets: tuple[str, ...]
    rows_by_market: dict[str, int]
    horizons_minutes: tuple[int, ...]
    quantile_levels: tuple[float, ...]

    @property
    def rows(self) -> int:
        return int(self.y_cls.shape[0])


class _SequenceTorchDataset(Dataset):
    def __init__(self, samples: _SequenceSamples, indices: np.ndarray) -> None:
        self._samples = samples
        self._indices = np.asarray(indices, dtype=np.int64)

    def __len__(self) -> int:
        return int(self._indices.size)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        row_idx = int(self._indices[idx])
        return {
            "second": torch.from_numpy(self._samples.second[row_idx]).float(),
            "minute": torch.from_numpy(self._samples.minute[row_idx]).float(),
            "micro": torch.from_numpy(self._samples.micro[row_idx]).float(),
            "lob": torch.from_numpy(self._samples.lob[row_idx]).float(),
            "lob_global": torch.from_numpy(self._samples.lob_global[row_idx]).float(),
            "known_covariates": torch.from_numpy(self._samples.known_covariates[row_idx]).float(),
            "y_cls": torch.tensor(float(self._samples.y_cls[row_idx]), dtype=torch.float32),
            "y_reg_multi": torch.from_numpy(self._samples.y_reg_multi[row_idx]).float(),
            "sample_weight": torch.tensor(float(self._samples.sample_weight[row_idx]), dtype=torch.float32),
        }


SECOND_FEATURE_NAMES: tuple[str, ...] = ("close", "logret_1", "volume_base", "volume_quote")
ONE_MIN_FEATURE_NAMES: tuple[str, ...] = ("close", "logret_1", "volume_base", "volume_quote")
MICRO_FEATURE_NAMES: tuple[str, ...] = (
    "trade_events",
    "trade_imbalance",
    "spread_bps_mean",
    "depth_bid_top5_mean",
    "depth_ask_top5_mean",
    "imbalance_top5_mean",
    "microprice_bias_bps_mean",
)
LOB_PER_LEVEL_CHANNELS: tuple[str, ...] = (
    "relative_price_bps",
    "bid_size",
    "ask_size",
    "normalized_depth_share",
    "event_delta",
)
LOB_GLOBAL_CHANNELS: tuple[str, ...] = (
    "spread_bps",
    "total_depth",
    "trade_imbalance",
    "tick_size",
    "relative_tick_bps",
)


def _parse_date_to_ts_ms(value: str | None, *, end_of_day: bool = False) -> int | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10:
        parsed = datetime.fromisoformat(text)
        if end_of_day:
            parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
        parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)


def _sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _build_sequence_batch(samples: _SequenceSamples) -> dict[str, np.ndarray]:
    return {
        "second": np.asarray(samples.second, dtype=np.float32),
        "minute": np.asarray(samples.minute, dtype=np.float32),
        "micro": np.asarray(samples.micro, dtype=np.float32),
        "lob": np.asarray(samples.lob, dtype=np.float32),
        "lob_global": np.asarray(samples.lob_global, dtype=np.float32),
        "known_covariates": np.asarray(samples.known_covariates, dtype=np.float32),
    }


def _write_sequence_expert_prediction_table(
    *,
    run_dir: Path,
    samples: _SequenceSamples,
    split_labels: np.ndarray,
    estimator: V5SequenceEstimator,
) -> Path:
    payload = estimator.predict_cache_batch(_build_sequence_batch(samples))
    quantiles = np.asarray(payload["return_quantiles_by_horizon"], dtype=np.float64)
    regime = np.asarray(payload["regime_embedding"], dtype=np.float64)
    frame_payload: dict[str, Any] = {
        "market": np.asarray(samples.markets, dtype=object),
        "ts_ms": np.asarray(samples.ts_ms, dtype=np.int64),
        "split": np.asarray(split_labels, dtype=object),
        "y_cls": np.asarray(samples.y_cls, dtype=np.int64),
        "y_reg": np.asarray(samples.y_reg_primary, dtype=np.float64),
        "directional_probability_primary": np.asarray(payload["directional_probability_primary"], dtype=np.float64),
        "sequence_uncertainty_primary": np.asarray(payload["sequence_uncertainty_primary"], dtype=np.float64),
    }
    for horizon_idx, horizon in enumerate(samples.horizons_minutes):
        for quantile_idx, quantile in enumerate(samples.quantile_levels):
            frame_payload[f"return_quantile_h{int(horizon)}_q{int(round(float(quantile) * 100))}"] = quantiles[:, horizon_idx, quantile_idx]
    if regime.ndim == 2:
        for emb_idx in range(regime.shape[1]):
            frame_payload[f"regime_embedding_{int(emb_idx)}"] = regime[:, emb_idx]
    frame = pl.DataFrame(frame_payload).sort(["ts_ms", "market"])
    output_path = run_dir / "expert_prediction_table.parquet"
    frame.write_parquet(output_path)
    return output_path


class _PatchTSTEncoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, *, patch_len: int, patch_stride: int) -> None:
        super().__init__()
        self.patch_len = max(int(patch_len), 1)
        self.patch_stride = max(int(patch_stride), 1)
        self.proj = nn.Linear(self.patch_len * int(input_dim), hidden_dim)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=4,
            dim_feedforward=max(hidden_dim * 2, 32),
            dropout=0.1,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        batch, steps, feats = x.shape
        if steps < self.patch_len:
            x = torch.nn.functional.pad(x, (0, 0, self.patch_len - steps, 0))
            steps = x.shape[1]
        patches = x.unfold(1, self.patch_len, self.patch_stride)
        patches = patches.contiguous().reshape(batch, -1, self.patch_len * feats)
        hidden = self.proj(patches)
        encoded = self.encoder(hidden)
        return self.norm(encoded.mean(dim=1))


class _TimeMixerBlock(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        self.mlp = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, hidden_dim),
        )
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        hidden = self.input_proj(x)
        mixed = self.mlp(hidden)
        return self.norm(hidden + mixed)


class _TimeMixerEncoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.block1 = _TimeMixerBlock(input_dim, hidden_dim)
        self.block2 = _TimeMixerBlock(hidden_dim, hidden_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        hidden = self.block1(x)
        hidden = self.block2(hidden)
        return hidden.mean(dim=1)


class _TFTEncoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        self.lstm = nn.LSTM(hidden_dim, hidden_dim, batch_first=True)
        self.attn = nn.MultiheadAttention(hidden_dim, num_heads=4, batch_first=True)
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        hidden = self.input_proj(x)
        hidden, _ = self.lstm(hidden)
        attn_out, _ = self.attn(hidden, hidden, hidden, need_weights=False)
        return self.norm(attn_out.mean(dim=1))


def _build_backbone(*, family: str, input_dim: int, hidden_dim: int, patch_len: int, patch_stride: int) -> nn.Module:
    if family == "patchtst":
        return _PatchTSTEncoder(input_dim, hidden_dim, patch_len=patch_len, patch_stride=patch_stride)
    if family == "timemixer":
        return _TimeMixerEncoder(input_dim, hidden_dim)
    if family == "tft":
        return _TFTEncoder(input_dim, hidden_dim)
    raise ValueError(f"unsupported backbone_family: {family}")


class _V5SequenceModel(nn.Module):
    def __init__(
        self,
        *,
        backbone_family: str,
        second_dim: int,
        minute_dim: int,
        micro_dim: int,
        lob_dim: int,
        lob_global_dim: int,
        known_cov_dim: int,
        hidden_dim: int,
        regime_embedding_dim: int,
        horizons_count: int,
        quantiles_count: int,
        patch_len: int,
        patch_stride: int,
    ) -> None:
        super().__init__()
        self.second_encoder = _build_backbone(family=backbone_family, input_dim=second_dim, hidden_dim=hidden_dim, patch_len=patch_len, patch_stride=patch_stride)
        self.minute_encoder = _build_backbone(family=backbone_family, input_dim=minute_dim, hidden_dim=hidden_dim, patch_len=patch_len, patch_stride=patch_stride)
        self.micro_encoder = _build_backbone(family=backbone_family, input_dim=micro_dim, hidden_dim=hidden_dim, patch_len=patch_len, patch_stride=patch_stride)
        self.lob_encoder = _build_backbone(
            family=backbone_family,
            input_dim=lob_dim + lob_global_dim,
            hidden_dim=hidden_dim,
            patch_len=patch_len,
            patch_stride=patch_stride,
        )
        self.known_proj = nn.Sequential(
            nn.Linear(known_cov_dim, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, hidden_dim),
        )
        fused_dim = hidden_dim * 5
        self.fusion = nn.Sequential(
            nn.Linear(fused_dim, hidden_dim * 2),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.GELU(),
        )
        self.regime_head = nn.Linear(hidden_dim, regime_embedding_dim)
        self.cls_head = nn.Linear(hidden_dim, 1)
        self.quantile_head = nn.Linear(hidden_dim, horizons_count * quantiles_count)
        self.reconstruction_head = nn.Linear(hidden_dim, second_dim + minute_dim + micro_dim + lob_global_dim)
        self.horizons_count = int(horizons_count)
        self.quantiles_count = int(quantiles_count)

    def encode(self, batch: dict[str, torch.Tensor]) -> torch.Tensor:
        lob_flat = batch["lob"].reshape(batch["lob"].shape[0], batch["lob"].shape[1], -1)
        lob_input = torch.cat([lob_flat, batch["lob_global"]], dim=-1)
        parts = [
            self.second_encoder(batch["second"]),
            self.minute_encoder(batch["minute"]),
            self.micro_encoder(batch["micro"]),
            self.lob_encoder(lob_input),
            self.known_proj(batch["known_covariates"]),
        ]
        fused = torch.cat(parts, dim=-1)
        return self.fusion(fused)

    def forward(self, batch: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
        fused = self.encode(batch)
        regime_embedding = self.regime_head(fused)
        cls_logit = self.cls_head(fused).squeeze(-1)
        quantiles = self.quantile_head(fused).reshape(-1, self.horizons_count, self.quantiles_count)
        reconstruction = self.reconstruction_head(fused)
        return {
            "cls_logit": cls_logit,
            "quantiles": quantiles,
            "regime_embedding": regime_embedding,
            "reconstruction": reconstruction,
        }


@dataclass
class V5SequenceEstimator:
    model: _V5SequenceModel
    backbone_family: str
    pretrain_method: str
    horizons_minutes: tuple[int, ...]
    quantile_levels: tuple[float, ...]
    residual_sigma_by_horizon: dict[str, float]
    primary_horizon_minutes: int
    regime_embedding_dim: int
    bridge_feature_names: tuple[str, ...]
    bridge_probability_model: Any
    bridge_quantile_models: dict[str, tuple[Any, Any, Any]]

    def predict_cache_batch(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        self.model.eval()
        with torch.no_grad():
            tensors = {key: torch.from_numpy(np.asarray(value)).float() for key, value in batch.items()}
            outputs = self.model(tensors)
            quantiles = outputs["quantiles"].cpu().numpy().astype(np.float64, copy=False)
            logits = outputs["cls_logit"].cpu().numpy().astype(np.float64, copy=False)
            regime = outputs["regime_embedding"].cpu().numpy().astype(np.float64, copy=False)
        probs = 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))
        primary_index = self.horizons_minutes.index(int(self.primary_horizon_minutes))
        q10 = quantiles[:, primary_index, 0]
        q90 = quantiles[:, primary_index, -1]
        uncertainty = np.maximum((q90 - q10) / 2.5631, 1e-6)
        return {
            "directional_probability_primary": probs,
            "return_quantiles_by_horizon": quantiles,
            "sequence_uncertainty_primary": uncertainty,
            "regime_embedding": regime,
        }

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        probs = np.clip(np.asarray(self.bridge_probability_model.predict(x), dtype=np.float64), 0.0, 1.0)
        return np.column_stack([1.0 - probs, probs])

    def predict_distributional_contract(self, x: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
        matrix = np.asarray(x, dtype=np.float64)
        quantiles_by_horizon: dict[str, np.ndarray] = {}
        sigma_by_horizon: dict[str, np.ndarray] = {}
        es_proxy_by_horizon: dict[str, np.ndarray] = {}
        mu_by_horizon: dict[str, np.ndarray] = {}
        for horizon in self.horizons_minutes:
            horizon_key = f"h{int(horizon)}"
            q10_model, q50_model, q90_model = self.bridge_quantile_models[horizon_key]
            q10 = np.asarray(q10_model.predict(matrix), dtype=np.float64)
            q50 = np.asarray(q50_model.predict(matrix), dtype=np.float64)
            q90 = np.maximum(np.asarray(q90_model.predict(matrix), dtype=np.float64), q10)
            quantiles_by_horizon[horizon_key] = np.column_stack([q10, q50, q90])
            sigma_by_horizon[horizon_key] = np.maximum((q90 - q10) / 2.5631, 1e-6)
            es_proxy_by_horizon[horizon_key] = q10
            mu_by_horizon[horizon_key] = q50
        return {
            "mu_by_horizon": mu_by_horizon,
            "return_quantiles_by_horizon": quantiles_by_horizon,
            "sigma_by_horizon": sigma_by_horizon,
            "expected_shortfall_proxy_by_horizon": es_proxy_by_horizon,
        }

    def predict_uncertainty(self, x: np.ndarray) -> np.ndarray:
        payload = self.predict_distributional_contract(x)
        primary_key = f"h{int(self.primary_horizon_minutes)}"
        return np.asarray(payload["sigma_by_horizon"][primary_key], dtype=np.float64)

    def predict_panel_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        probs = np.clip(np.asarray(self.bridge_probability_model.predict(x), dtype=np.float64), 0.0, 1.0)
        distribution = self.predict_distributional_contract(x)
        primary_key = f"h{int(self.primary_horizon_minutes)}"
        mu = np.asarray(distribution["mu_by_horizon"][primary_key], dtype=np.float64)
        es = np.abs(np.asarray(distribution["expected_shortfall_proxy_by_horizon"][primary_key], dtype=np.float64))
        uncertainty = np.asarray(distribution["sigma_by_horizon"][primary_key], dtype=np.float64)
        score_lcb = np.clip(probs - uncertainty, 0.0, 1.0)
        tradability = np.clip(1.0 / (1.0 + es + uncertainty), 0.0, 1.0)
        return {
            "final_rank_score": probs,
            "final_uncertainty": uncertainty,
            "score_mean": probs,
            "score_std": uncertainty,
            "score_lcb": score_lcb,
            "final_expected_return": mu,
            "final_expected_es": es,
            "final_tradability": tradability,
            "final_alpha_lcb": mu - es - uncertainty,
        }


def _augment_batch(batch: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
    augmented = {key: value.clone() for key, value in batch.items() if key not in {"y_cls", "y_reg_multi", "sample_weight"}}
    for key in ("second", "minute", "micro", "lob", "lob_global", "known_covariates"):
        augmented[key] = augmented[key] + (torch.randn_like(augmented[key]) * 0.01)
    return augmented


def _mask_batch(batch: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
    masked = {key: value.clone() for key, value in batch.items() if key not in {"y_cls", "y_reg_multi", "sample_weight"}}
    for key in ("second", "minute", "micro", "lob", "lob_global"):
        mask = (torch.rand_like(masked[key]) > 0.15).float()
        masked[key] = masked[key] * mask
    return masked


def _pretrain_summary_target(batch: dict[str, torch.Tensor]) -> torch.Tensor:
    return torch.cat(
        [
            batch["second"].mean(dim=1),
            batch["minute"].mean(dim=1),
            batch["micro"].mean(dim=1),
            batch["lob_global"].mean(dim=1),
        ],
        dim=-1,
    )


def _run_pretrain(
    *,
    model: _V5SequenceModel,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    method: str,
    epochs: int,
    device: torch.device,
) -> None:
    if method == "none":
        return
    for _ in range(max(int(epochs), 1)):
        model.train()
        for batch in loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            optimizer.zero_grad(set_to_none=True)
            if method == "ts2vec_like":
                emb_a = model.encode(_augment_batch(batch))
                emb_b = model.encode(_augment_batch(batch))
                loss = 1.0 - torch.nn.functional.cosine_similarity(emb_a, emb_b, dim=-1).mean()
            else:
                outputs = model(_mask_batch(batch))
                loss = torch.nn.functional.mse_loss(outputs["reconstruction"], _pretrain_summary_target(batch))
            loss.backward()
            optimizer.step()


def _supervised_loss(
    *,
    outputs: dict[str, torch.Tensor],
    batch: dict[str, torch.Tensor],
    quantile_levels: tuple[float, ...],
) -> torch.Tensor:
    quantiles = outputs["quantiles"]
    target = batch["y_reg_multi"]
    weight = batch["sample_weight"].unsqueeze(-1)
    quantile_loss = 0.0
    for q_idx, quantile in enumerate(quantile_levels):
        pred = quantiles[:, :, q_idx]
        diff = target - pred
        quantile_loss = quantile_loss + torch.mean(torch.maximum(float(quantile) * diff, (float(quantile) - 1.0) * diff) * weight)
    cls_loss = torch.nn.functional.binary_cross_entropy_with_logits(
        outputs["cls_logit"],
        batch["y_cls"],
        weight=batch["sample_weight"],
    )
    return quantile_loss + (0.5 * cls_loss)


def _evaluate_loss(
    *,
    model: _V5SequenceModel,
    loader: DataLoader,
    quantile_levels: tuple[float, ...],
    device: torch.device,
) -> float:
    values: list[float] = []
    model.eval()
    with torch.no_grad():
        for batch in loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            loss = _supervised_loss(outputs=model(batch), batch=batch, quantile_levels=quantile_levels)
            values.append(float(loss.item()))
    return float(np.mean(values)) if values else 0.0


def _predict_split(*, model: _V5SequenceModel, samples: _SequenceSamples, indices: np.ndarray, device: torch.device) -> dict[str, np.ndarray]:
    loader = DataLoader(_SequenceTorchDataset(samples, indices), batch_size=64, shuffle=False)
    probs_parts: list[np.ndarray] = []
    quantile_parts: list[np.ndarray] = []
    model.eval()
    with torch.no_grad():
        for batch in loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            outputs = model(batch)
            probs_parts.append(torch.sigmoid(outputs["cls_logit"]).cpu().numpy().astype(np.float64, copy=False))
            quantile_parts.append(outputs["quantiles"].cpu().numpy().astype(np.float64, copy=False))
    return {
        "directional_probability": np.concatenate(probs_parts, axis=0) if probs_parts else np.empty(0, dtype=np.float64),
        "quantiles": np.concatenate(quantile_parts, axis=0)
        if quantile_parts
        else np.empty((0, len(samples.horizons_minutes), len(samples.quantile_levels)), dtype=np.float64),
    }


def _evaluate_sequence_split(
    *,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    sample_weight: np.ndarray,
) -> dict[str, Any]:
    cls = classification_metrics(y_cls, scores, sample_weight=sample_weight)
    trading = trading_metrics(y_cls, y_reg, scores, fee_bps_est=0.0, safety_bps=0.0, sample_weight=sample_weight)
    per_market = grouped_trading_metrics(markets=markets, y_true=y_cls, y_reg=y_reg, scores=scores, fee_bps_est=0.0, safety_bps=0.0, sample_weight=sample_weight)
    return {
        "rows": int(y_cls.size),
        "classification": cls,
        "trading": trading,
        "per_market": per_market,
    }


def _estimate_residual_sigma_by_horizon(
    *,
    targets: np.ndarray,
    quantiles: np.ndarray,
    horizons: tuple[int, ...],
    q_levels: tuple[float, ...],
) -> dict[str, float]:
    q50_idx = q_levels.index(0.5)
    payload: dict[str, float] = {}
    for idx, horizon in enumerate(horizons):
        residual = np.asarray(targets[:, idx] - quantiles[:, idx, q50_idx], dtype=np.float64)
        payload[f"h{int(horizon)}"] = float(np.std(residual, ddof=0)) if residual.size > 0 else 0.0
    return payload


def _load_sequence_samples(options: TrainV5SequenceOptions) -> _SequenceSamples:
    manifest = pl.read_parquet(options.dataset_root / "_meta" / "manifest.parquet")
    if manifest.height <= 0:
        raise ValueError("sequence_v1 manifest is empty")
    start_ts_ms = _parse_date_to_ts_ms(options.start)
    end_ts_ms = _parse_date_to_ts_ms(options.end, end_of_day=True)
    if start_ts_ms is not None:
        manifest = manifest.filter(pl.col("anchor_ts_ms") >= int(start_ts_ms))
    if end_ts_ms is not None:
        manifest = manifest.filter(pl.col("anchor_ts_ms") <= int(end_ts_ms))
    if manifest.height <= 0:
        raise ValueError("sequence_v1 manifest has no rows in the requested range")

    selected_markets = sorted(str(item).strip().upper() for item in manifest.get_column("market").unique().to_list())
    if int(options.top_n) > 0:
        selected_markets = selected_markets[: max(int(options.top_n), 1)]
    manifest = manifest.filter(pl.col("market").is_in(selected_markets))
    if manifest.height <= 0:
        raise ValueError("sequence_v1 manifest has no rows after top_n filtering")

    ws_root = options.dataset_root.parent / "ws_candle_v1" / "tf=1m"
    ws_by_market: dict[str, dict[int, float]] = {}
    for market in selected_markets:
        files = sorted((ws_root / f"market={market}").glob("*.parquet"))
        if not files:
            continue
        frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical").sort("ts_ms")
        ws_by_market[market] = {int(row["ts_ms"]): float(row["close"]) for row in frame.iter_rows(named=True)}

    second_parts: list[np.ndarray] = []
    minute_parts: list[np.ndarray] = []
    micro_parts: list[np.ndarray] = []
    lob_parts: list[np.ndarray] = []
    lob_global_parts: list[np.ndarray] = []
    covariate_parts: list[np.ndarray] = []
    y_cls_parts: list[int] = []
    y_reg_primary_parts: list[float] = []
    y_rank_parts: list[float] = []
    y_reg_multi_parts: list[list[float]] = []
    weight_parts: list[float] = []
    ts_parts: list[int] = []
    market_parts: list[str] = []
    rows_by_market: dict[str, int] = {}
    pooled_feature_names = _pooled_feature_names()

    for row in manifest.iter_rows(named=True):
        market = str(row["market"]).strip().upper()
        anchor_ts_ms = int(row["anchor_ts_ms"])
        future_returns = _compute_future_returns(ws_by_market.get(market, {}), anchor_ts_ms=anchor_ts_ms, horizons=options.horizons_minutes)
        if future_returns is None:
            continue
        payload = np.load(Path(str(row["cache_file"])))
        second_parts.append(np.asarray(payload["second_tensor"], dtype=np.float32))
        minute_parts.append(np.asarray(payload["minute_tensor"], dtype=np.float32))
        micro_parts.append(np.asarray(payload["micro_tensor"], dtype=np.float32))
        lob_parts.append(np.asarray(payload["lob_tensor"], dtype=np.float32))
        lob_global_parts.append(np.asarray(payload["lob_global_tensor"], dtype=np.float32))
        covariate_parts.append(_build_known_covariates(anchor_ts_ms=anchor_ts_ms, ws_by_market=ws_by_market))
        primary_return = float(future_returns[0])
        y_cls_parts.append(1 if primary_return > 0.0 else 0)
        y_reg_primary_parts.append(primary_return)
        y_rank_parts.append(primary_return)
        y_reg_multi_parts.append(list(future_returns))
        weight = float(
            np.mean(
                [
                    float(row.get("second_coverage_ratio") or 0.0),
                    float(row.get("minute_coverage_ratio") or 0.0),
                    float(row.get("micro_coverage_ratio") or 0.0),
                    float(row.get("lob_coverage_ratio") or 0.0),
                ]
            )
        )
        weight_parts.append(max(weight, 0.1))
        ts_parts.append(anchor_ts_ms)
        market_parts.append(market)
        rows_by_market[market] = rows_by_market.get(market, 0) + 1

    if not second_parts:
        raise ValueError("sequence_v1 has no trainable anchors with future horizon coverage")

    second_array = np.stack(second_parts, axis=0)
    minute_array = np.stack(minute_parts, axis=0)
    micro_array = np.stack(micro_parts, axis=0)
    lob_array = np.stack(lob_parts, axis=0)
    lob_global_array = np.stack(lob_global_parts, axis=0)
    known_covariates_array = np.stack(covariate_parts, axis=0)

    return _SequenceSamples(
        second=second_array,
        minute=minute_array,
        micro=micro_array,
        lob=lob_array,
        lob_global=lob_global_array,
        known_covariates=known_covariates_array,
        y_cls=np.asarray(y_cls_parts, dtype=np.int64),
        y_reg_primary=np.asarray(y_reg_primary_parts, dtype=np.float64),
        y_rank=np.asarray(y_rank_parts, dtype=np.float64),
        y_reg_multi=np.asarray(y_reg_multi_parts, dtype=np.float64),
        sample_weight=np.asarray(weight_parts, dtype=np.float64),
        ts_ms=np.asarray(ts_parts, dtype=np.int64),
        markets=np.asarray(market_parts, dtype=object),
        pooled_features=_build_pooled_sequence_features(
            second=second_array,
            minute=minute_array,
            micro=micro_array,
            lob=lob_array,
            lob_global=lob_global_array,
            known_covariates=known_covariates_array,
        ),
        feature_names=pooled_feature_names,
        selected_markets=tuple(sorted(rows_by_market.keys())),
        rows_by_market=rows_by_market,
        horizons_minutes=tuple(int(item) for item in options.horizons_minutes),
        quantile_levels=tuple(float(item) for item in options.quantile_levels),
    )


def _compute_future_returns(ws_close_map: dict[int, float], *, anchor_ts_ms: int, horizons: tuple[int, ...]) -> list[float] | None:
    current_close = ws_close_map.get(int(anchor_ts_ms))
    if current_close is None or current_close <= 0.0:
        return None
    values: list[float] = []
    for horizon in horizons:
        future_ts = int(anchor_ts_ms + (int(horizon) * 60_000))
        future_close = ws_close_map.get(future_ts)
        if future_close is None or future_close <= 0.0:
            return None
        values.append(float((future_close / current_close) - 1.0))
    return values


def _build_known_covariates(*, anchor_ts_ms: int, ws_by_market: dict[str, dict[int, float]]) -> np.ndarray:
    dt = datetime.fromtimestamp(anchor_ts_ms / 1000.0, tz=timezone.utc)
    hour = float(dt.hour)
    weekday = float(dt.weekday())
    hour_sin = np.sin(2.0 * np.pi * hour / 24.0)
    hour_cos = np.cos(2.0 * np.pi * hour / 24.0)
    weekday_sin = np.sin(2.0 * np.pi * weekday / 7.0)
    weekday_cos = np.cos(2.0 * np.pi * weekday / 7.0)
    weekend_flag = 1.0 if int(weekday) >= 5 else 0.0

    breadth_returns: list[float] = []
    for close_map in ws_by_market.values():
        current = close_map.get(anchor_ts_ms)
        previous = close_map.get(anchor_ts_ms - 60_000)
        if current is None or previous is None or previous <= 0.0:
            continue
        breadth_returns.append((current / previous) - 1.0)
    breadth_positive_ratio = float(np.mean(np.asarray(breadth_returns) > 0.0)) if breadth_returns else 0.0
    breadth_mean_return = float(np.mean(breadth_returns)) if breadth_returns else 0.0
    btc_ret = _leader_return(ws_by_market.get("KRW-BTC", {}), anchor_ts_ms)
    eth_ret = _leader_return(ws_by_market.get("KRW-ETH", {}), anchor_ts_ms)
    return np.asarray(
        [
            hour_sin,
            hour_cos,
            weekday_sin,
            weekday_cos,
            weekend_flag,
            breadth_positive_ratio,
            breadth_mean_return,
            btc_ret,
            eth_ret,
        ],
        dtype=np.float32,
    )


def _leader_return(close_map: dict[int, float], anchor_ts_ms: int) -> float:
    current = close_map.get(anchor_ts_ms)
    previous = close_map.get(anchor_ts_ms - 60_000)
    if current is None or previous is None or previous <= 0.0:
        return 0.0
    return float((current / previous) - 1.0)


def _pooled_feature_names() -> tuple[str, ...]:
    names: list[str] = []
    for prefix, features in (
        ("second", ("close", "logret", "vol_base", "vol_quote")),
        ("minute", ("close", "logret", "vol_base", "vol_quote")),
        ("micro", ("trade_events", "trade_imbalance", "spread_bps", "depth_bid_top5", "depth_ask_top5", "imbalance_top5", "microprice_bias")),
    ):
        for feature in features:
            names.extend([f"{prefix}_{feature}_last", f"{prefix}_{feature}_mean", f"{prefix}_{feature}_std"])
    for feature in ("relative_price", "bid_size", "ask_size", "depth_share", "event_delta"):
        names.extend([f"lob_{feature}_last_mean", f"lob_{feature}_time_mean", f"lob_{feature}_last_std"])
    for feature in ("spread_bps", "total_depth", "trade_imbalance", "tick_size", "relative_tick_bps"):
        names.extend([f"lob_global_{feature}_last", f"lob_global_{feature}_mean"])
    names.extend(
        [
            "cov_hour_sin",
            "cov_hour_cos",
            "cov_weekday_sin",
            "cov_weekday_cos",
            "cov_weekend_flag",
            "cov_breadth_positive_ratio",
            "cov_breadth_mean_return",
            "cov_btc_ret_1m",
            "cov_eth_ret_1m",
        ]
    )
    return tuple(names)


def _build_temporal_pool_features(block: np.ndarray) -> np.ndarray:
    payload = np.asarray(block, dtype=np.float64)
    feature_parts: list[np.ndarray] = []
    for feature_idx in range(payload.shape[2]):
        feature_values = payload[:, :, feature_idx]
        feature_parts.extend(
            [
                feature_values[:, -1],
                np.mean(feature_values, axis=1),
                np.std(feature_values, axis=1, ddof=0),
            ]
        )
    return np.column_stack(feature_parts)


def _build_pooled_sequence_features(
    *,
    second: np.ndarray,
    minute: np.ndarray,
    micro: np.ndarray,
    lob: np.ndarray,
    lob_global: np.ndarray,
    known_covariates: np.ndarray,
) -> np.ndarray:
    second_features = _build_temporal_pool_features(second)
    minute_features = _build_temporal_pool_features(minute)
    micro_features = _build_temporal_pool_features(micro)
    lob_parts: list[np.ndarray] = []
    lob_payload = np.asarray(lob, dtype=np.float64)
    for channel_idx in range(lob_payload.shape[3]):
        channel = lob_payload[:, :, :, channel_idx]
        lob_parts.extend(
            [
                np.mean(channel[:, -1, :], axis=1),
                np.mean(channel, axis=(1, 2)),
                np.std(channel[:, -1, :], axis=1, ddof=0),
            ]
        )
    lob_features = np.column_stack(lob_parts)
    lob_global_parts: list[np.ndarray] = []
    lob_global_payload = np.asarray(lob_global, dtype=np.float64)
    for channel_idx in range(lob_global_payload.shape[2]):
        channel = lob_global_payload[:, :, channel_idx]
        lob_global_parts.extend([channel[:, -1], np.mean(channel, axis=1)])
    lob_global_features = np.column_stack(lob_global_parts)
    return np.column_stack(
        [
            second_features,
            minute_features,
            micro_features,
            lob_features,
            np.asarray(lob_global_features, dtype=np.float64),
            np.asarray(known_covariates, dtype=np.float64),
        ]
    ).astype(np.float32, copy=False)


def _build_sequence_data_fingerprint(*, options: TrainV5SequenceOptions, sample_count: int) -> dict[str, Any]:
    return {
        "dataset_root": str(options.dataset_root),
        "tf": "1m_anchor_sequence",
        "quote": options.quote,
        "top_n": int(options.top_n),
        "start_ts_ms": _parse_date_to_ts_ms(options.start),
        "end_ts_ms": _parse_date_to_ts_ms(options.end, end_of_day=True),
        "manifest_sha256": _sha256_file(options.dataset_root / "_meta" / "manifest.parquet"),
        "sequence_contract_sha256": _sha256_file(options.dataset_root / "_meta" / "sequence_tensor_contract.json"),
        "lob_contract_sha256": _sha256_file(options.dataset_root / "_meta" / "lob_tensor_contract.json"),
        "sample_count": int(sample_count),
        "code_version": autobot_version,
    }


def train_and_register_v5_sequence(options: TrainV5SequenceOptions) -> TrainV5SequenceResult:
    backbone_family = str(options.backbone_family).strip().lower()
    if backbone_family not in VALID_BACKBONES:
        raise ValueError(f"backbone_family must be one of: {', '.join(VALID_BACKBONES)}")
    pretrain_method = str(options.pretrain_method).strip().lower()
    if pretrain_method not in VALID_PRETRAIN_METHODS:
        raise ValueError(f"pretrain_method must be one of: {', '.join(VALID_PRETRAIN_METHODS)}")

    started_at = time.time()
    run_id = make_run_id(seed=options.seed)
    samples = _load_sequence_samples(options)
    labels, split_info = compute_time_splits(
        samples.ts_ms,
        train_ratio=float(options.train_ratio),
        valid_ratio=float(options.valid_ratio),
        test_ratio=float(options.test_ratio),
        embargo_bars=0,
        interval_ms=60_000,
    )
    masks = split_masks(labels)
    train_idx = np.flatnonzero(masks["train"])
    valid_idx = np.flatnonzero(masks["valid"])
    test_idx = np.flatnonzero(masks["test"])
    if train_idx.size <= 0 or valid_idx.size <= 0 or test_idx.size <= 0:
        raise ValueError("v5_sequence requires non-empty train/valid/test splits")

    torch.manual_seed(int(options.seed))
    np.random.seed(int(options.seed))
    device = torch.device("cpu")

    model = _V5SequenceModel(
        backbone_family=backbone_family,
        second_dim=samples.second.shape[2],
        minute_dim=samples.minute.shape[2],
        micro_dim=samples.micro.shape[2],
        lob_dim=samples.lob.shape[2] * samples.lob.shape[3],
        lob_global_dim=samples.lob_global.shape[2],
        known_cov_dim=samples.known_covariates.shape[1],
        hidden_dim=max(int(options.hidden_dim), 16),
        regime_embedding_dim=max(int(options.regime_embedding_dim), 2),
        horizons_count=len(samples.horizons_minutes),
        quantiles_count=len(samples.quantile_levels),
        patch_len=max(int(options.patch_len), 1),
        patch_stride=max(int(options.patch_stride), 1),
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=float(options.learning_rate), weight_decay=float(options.weight_decay))
    train_loader = DataLoader(_SequenceTorchDataset(samples, train_idx), batch_size=max(int(options.batch_size), 1), shuffle=True)
    valid_loader = DataLoader(_SequenceTorchDataset(samples, valid_idx), batch_size=max(int(options.batch_size), 1), shuffle=False)

    _run_pretrain(
        model=model,
        loader=train_loader,
        optimizer=optimizer,
        method=pretrain_method,
        epochs=max(int(options.pretrain_epochs), 1),
        device=device,
    )

    best_state: dict[str, torch.Tensor] | None = None
    best_valid_loss: float | None = None
    for _epoch in range(max(int(options.finetune_epochs), 1)):
        model.train()
        for batch in train_loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            optimizer.zero_grad(set_to_none=True)
            loss = _supervised_loss(outputs=model(batch), batch=batch, quantile_levels=samples.quantile_levels)
            loss.backward()
            optimizer.step()
        valid_loss = _evaluate_loss(model=model, loader=valid_loader, quantile_levels=samples.quantile_levels, device=device)
        if best_valid_loss is None or valid_loss < best_valid_loss:
            best_valid_loss = valid_loss
            best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}

    if best_state is not None:
        model.load_state_dict(best_state)

    valid_outputs = _predict_split(model=model, samples=samples, indices=valid_idx, device=device)
    test_outputs = _predict_split(model=model, samples=samples, indices=test_idx, device=device)
    all_idx = np.arange(samples.rows, dtype=np.int64)
    all_outputs = _predict_split(model=model, samples=samples, indices=all_idx, device=device)
    valid_metrics = _evaluate_sequence_split(
        y_cls=samples.y_cls[valid_idx],
        y_reg=samples.y_reg_primary[valid_idx],
        scores=valid_outputs["directional_probability"],
        markets=samples.markets[valid_idx],
        sample_weight=samples.sample_weight[valid_idx],
    )
    test_metrics = _evaluate_sequence_split(
        y_cls=samples.y_cls[test_idx],
        y_reg=samples.y_reg_primary[test_idx],
        scores=test_outputs["directional_probability"],
        markets=samples.markets[test_idx],
        sample_weight=samples.sample_weight[test_idx],
    )
    thresholds = _build_thresholds(
        valid_scores=valid_outputs["directional_probability"],
        y_reg_valid=samples.y_reg_primary[valid_idx],
        fee_bps_est=0.0,
        safety_bps=0.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        sample_weight=samples.sample_weight[valid_idx],
    )
    selection_recommendations = build_selection_recommendations(
        valid_scores=valid_outputs["directional_probability"],
        valid_ts_ms=samples.ts_ms[valid_idx],
        thresholds=thresholds,
    )
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        score_source="score_mean",
    )
    selection_calibration = _identity_calibration(reason="SEQUENCE_IDENTITY_CALIBRATION")
    residual_sigma_by_horizon = _estimate_residual_sigma_by_horizon(
        targets=samples.y_reg_multi[valid_idx],
        quantiles=valid_outputs["quantiles"],
        horizons=samples.horizons_minutes,
        q_levels=samples.quantile_levels,
    )
    bridge_fit_mask = np.asarray(labels != "test", dtype=bool)
    bridge_probability_model = fit_ridge_bridge(
        samples.pooled_features[bridge_fit_mask],
        all_outputs["directional_probability"][bridge_fit_mask],
        clip_min=0.0,
        clip_max=1.0,
    )
    bridge_quantile_models: dict[str, tuple[Any, Any, Any]] = {}
    for horizon_idx, horizon in enumerate(samples.horizons_minutes):
        horizon_key = f"h{int(horizon)}"
        bridge_quantile_models[horizon_key] = (
            fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["quantiles"][bridge_fit_mask, horizon_idx, 0]),
            fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["quantiles"][bridge_fit_mask, horizon_idx, 1]),
            fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["quantiles"][bridge_fit_mask, horizon_idx, 2]),
        )
    estimator = V5SequenceEstimator(
        model=model.cpu(),
        backbone_family=backbone_family,
        pretrain_method=pretrain_method,
        horizons_minutes=samples.horizons_minutes,
        quantile_levels=samples.quantile_levels,
        residual_sigma_by_horizon=residual_sigma_by_horizon,
        primary_horizon_minutes=samples.horizons_minutes[0],
        regime_embedding_dim=max(int(options.regime_embedding_dim), 2),
        bridge_feature_names=samples.feature_names,
        bridge_probability_model=bridge_probability_model,
        bridge_quantile_models=bridge_quantile_models,
    )

    metrics = {
        "rows": {
            "train": int(train_idx.size),
            "valid": int(valid_idx.size),
            "test": int(test_idx.size),
            "drop": int(np.sum(labels == "drop")),
        },
        "valid_metrics": valid_metrics,
        "champion_metrics": test_metrics,
        "sequence_model": {
            "policy": "v5_sequence_v1",
            "backbone_family": backbone_family,
            "pretrain_method": pretrain_method,
            "horizons_minutes": list(samples.horizons_minutes),
            "quantile_levels": list(samples.quantile_levels),
            "regime_embedding_dim": int(options.regime_embedding_dim),
        },
    }
    leaderboard_row = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_family": options.model_family,
        "champion": "sequence_expert",
        "champion_backend": backbone_family,
        "test_roc_auc": float((test_metrics.get("classification", {}) or {}).get("roc_auc") or 0.0),
        "test_pr_auc": float((test_metrics.get("classification", {}) or {}).get("pr_auc") or 0.0),
        "test_log_loss": float((test_metrics.get("classification", {}) or {}).get("log_loss") or 0.0),
        "test_brier_score": float((test_metrics.get("classification", {}) or {}).get("brier_score") or 0.0),
        "test_precision_top5": float((((test_metrics.get("trading", {}) or {}).get("top_5pct", {}) or {}).get("precision") or 0.0)),
        "test_ev_net_top5": float((((test_metrics.get("trading", {}) or {}).get("top_5pct", {}) or {}).get("ev_net") or 0.0)),
        "rows_train": int(train_idx.size),
        "rows_valid": int(valid_idx.size),
        "rows_test": int(test_idx.size),
    }
    feature_spec = {
        "feature_columns": list(samples.feature_names),
        "input_modalities": ["second_tensor", "minute_tensor", "micro_tensor", "lob_tensor", "known_covariates"],
        "dataset_root": str(options.dataset_root),
    }
    label_spec = {
        "policy": "v5_sequence_label_contract_v1",
        "primary_horizon_minutes": int(samples.horizons_minutes[0]),
        "horizons_minutes": list(samples.horizons_minutes),
        "quantile_levels": list(samples.quantile_levels),
        "target_definition": "future 1m close return by horizon from ws_candle_v1",
    }
    train_config = {
        **asdict(options),
        "dataset_root": str(options.dataset_root),
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v5_sequence",
        "feature_columns": list(samples.feature_names),
        "selected_markets": list(samples.selected_markets),
        "autobot_version": autobot_version,
    }
    data_fingerprint = _build_sequence_data_fingerprint(options=options, sample_count=samples.rows)
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="sequence_expert",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle={"model_type": "v5_sequence_torch", "estimator": estimator},
            metrics=metrics,
            thresholds=thresholds,
            feature_spec=feature_spec,
            label_spec=label_spec,
            train_config=train_config,
            data_fingerprint=data_fingerprint,
            leaderboard_row=leaderboard_row,
            model_card_text=model_card,
            selection_recommendations=selection_recommendations,
            selection_policy=selection_policy,
            selection_calibration=selection_calibration,
            runtime_recommendations={"status": "not_runtime_wired", "reason": "FUSION_PENDING"},
        ),
        publish_pointers=False,
    )
    update_artifact_status(run_dir, status="core_saved", core_saved=True)

    sequence_model_contract_path = run_dir / "sequence_model_contract.json"
    sequence_model_contract_path.write_text(
        json.dumps(
            {
                "policy": "v5_sequence_v1",
                "backbone_family": backbone_family,
                "pretrain_method": pretrain_method,
                "horizons_minutes": list(samples.horizons_minutes),
                "quantile_levels": list(samples.quantile_levels),
                "outputs": {
                    "directional_probability": "directional_probability_primary",
                    "return_quantiles_by_horizon": "return_quantiles_by_horizon",
                    "sequence_uncertainty": "sequence_uncertainty_primary",
                    "regime_embedding_dim": int(options.regime_embedding_dim),
                },
                "input_modalities": feature_spec["input_modalities"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    predictor_contract_path = run_dir / "predictor_contract.json"
    predictor_contract_path.write_text(
        json.dumps(
            {
                "version": 1,
                "directional_probability_field": "directional_probability_primary",
                "uncertainty_field": "sequence_uncertainty_primary",
                "regime_embedding_dim": int(options.regime_embedding_dim),
                "score_mean_field": "score_mean",
                "score_std_field": "sequence_uncertainty_primary",
                "score_lcb_field": "score_lcb",
                "final_rank_score_field": "final_rank_score",
                "final_expected_return_field": "final_expected_return",
                "final_expected_es_field": "final_expected_es",
                "final_tradability_field": "final_tradability",
                "final_alpha_lcb_field": "final_alpha_lcb",
                "distributional_contract": {
                    "horizons_minutes": list(samples.horizons_minutes),
                    "quantile_levels": list(samples.quantile_levels),
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report_path.write_text(
        json.dumps(
            {
                "policy": "v5_sequence_holdout_v1",
                "split_info": {
                    "valid_start_ts": int(split_info.valid_start_ts),
                    "test_start_ts": int(split_info.test_start_ts),
                    "counts": dict(split_info.counts),
                },
                "valid_metrics": valid_metrics,
                "test_metrics": test_metrics,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    promotion_path = run_dir / "promotion_decision.json"
    promotion_path.write_text(json.dumps({"run_id": run_id, "promote": False, "status": "candidate", "reasons": ["SEQUENCE_EXPERT_READY_FUSION_PENDING"]}, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    runtime_recommendations_path = run_dir / "runtime_recommendations.json"
    runtime_recommendations_path.write_text(json.dumps({"status": "not_runtime_wired", "reason": "FUSION_PENDING"}, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    expert_prediction_table_path = _write_sequence_expert_prediction_table(
        run_dir=run_dir,
        samples=samples,
        split_labels=np.asarray(labels, dtype=object),
        estimator=estimator,
    )
    train_report_path = options.logs_root / "train_v5_sequence_report.json"
    train_report_path.parent.mkdir(parents=True, exist_ok=True)
    train_report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": "candidate",
                "started_at_utc": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                "rows": metrics["rows"],
                "leaderboard_row": leaderboard_row,
                "valid_metrics": valid_metrics,
                "test_metrics": test_metrics,
                "sequence_model_contract_path": str(sequence_model_contract_path),
                "expert_prediction_table_path": str(expert_prediction_table_path),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    update_latest_pointer(options.registry_root, options.model_family, run_id)
    update_artifact_status(run_dir, status="candidate", support_artifacts_written=True)

    return TrainV5SequenceResult(
        run_id=run_id,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=promotion_path,
        walk_forward_report_path=walk_forward_report_path,
        sequence_model_contract_path=sequence_model_contract_path,
        predictor_contract_path=predictor_contract_path,
    )
