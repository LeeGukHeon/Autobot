"""PyTorch-based v5 LOB trainer on top of sequence_v1 / lob30 contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Callable

import numpy as np
import polars as pl
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

from autobot import __version__ as autobot_version
from autobot.ops.data_platform_snapshot import resolve_ready_snapshot_id

from .bridge_models import fit_ridge_bridge
from .metrics import classification_metrics, grouped_trading_metrics, trading_metrics
from .model_card import render_model_card
from .registry import RegistrySavePayload, load_json, load_model_bundle, make_run_id, save_run, update_artifact_status
from .runtime_feature_dataset import write_runtime_feature_dataset
from .selection_calibration import _identity_calibration
from .selection_policy import build_selection_policy_from_recommendations
from .split import compute_time_splits, split_masks
from .train_v1 import _build_thresholds, build_selection_recommendations
from .train_v5_sequence import _parse_date_to_ts_ms, _sha256_file
from .v5_expert_data import load_minute_close_map_sources, strict_eval_indices, support_level_weight
from .v5_expert_tail import (
    build_v5_expert_tail_context,
    expert_tail_context_path,
    finalize_v5_expert_family_run,
    resolve_existing_v5_expert_tail_artifacts,
    run_or_reuse_v5_expert_prediction_table,
    run_or_reuse_v5_runtime_governance_artifacts,
    v5_expert_tail_stage_reusable,
)
from .v5_expert_runtime_export import (
    load_existing_expert_runtime_export,
    resolve_expert_runtime_export_paths,
    write_expert_runtime_export_metadata,
)
from autobot.data.collect.sequence_tensor_store import (
    SUPPORT_LEVEL_REDUCED_CONTEXT,
    SUPPORT_LEVEL_STRICT_FULL,
    SUPPORT_LEVEL_STRUCTURAL_INVALID,
    resolve_sequence_support_level_from_row,
)


LOB_HORIZONS_SECONDS: tuple[int, ...] = (1, 5, 30, 60)
VALID_LOB_BACKBONES = ("deeplob", "bdlob", "hlob")


@dataclass(frozen=True)
class TrainV5LobOptions:
    dataset_root: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    quote: str
    top_n: int
    start: str
    end: str
    seed: int
    backbone_family: str = "deeplob"
    batch_size: int = 16
    epochs: int = 5
    learning_rate: float = 1e-3
    train_ratio: float = 0.6
    valid_ratio: float = 0.2
    test_ratio: float = 0.2
    hidden_dim: int = 64
    temporal_hidden_dim: int = 64
    weight_decay: float = 1e-4
    run_scope: str = "manual_lob_expert"


@dataclass(frozen=True)
class TrainV5LobResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    promotion_path: Path
    walk_forward_report_path: Path
    lob_model_contract_path: Path
    predictor_contract_path: Path


@dataclass
class _LobSamples:
    lob: np.ndarray
    lob_global: np.ndarray
    micro: np.ndarray
    close_price: np.ndarray
    y_micro_alpha: np.ndarray
    y_adverse_excursion: np.ndarray
    y_five_min_alpha: np.ndarray
    y_cls: np.ndarray
    y_rank: np.ndarray
    sample_weight: np.ndarray
    support_level: np.ndarray
    ts_ms: np.ndarray
    markets: np.ndarray
    pooled_features: np.ndarray
    feature_names: tuple[str, ...]
    selected_markets: tuple[str, ...]
    rows_by_market: dict[str, int]
    support_level_counts: dict[str, int]

    @property
    def rows(self) -> int:
        return int(self.y_cls.shape[0])


class _LobTorchDataset(Dataset):
    def __init__(self, samples: _LobSamples, indices: np.ndarray) -> None:
        self._samples = samples
        self._indices = np.asarray(indices, dtype=np.int64)

    def __len__(self) -> int:
        return int(self._indices.size)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        row_idx = int(self._indices[idx])
        return {
            "lob": torch.from_numpy(self._samples.lob[row_idx]).float(),
            "lob_global": torch.from_numpy(self._samples.lob_global[row_idx]).float(),
            "micro": torch.from_numpy(self._samples.micro[row_idx]).float(),
            "y_micro_alpha": torch.from_numpy(self._samples.y_micro_alpha[row_idx]).float(),
            "y_adverse_excursion": torch.tensor(float(self._samples.y_adverse_excursion[row_idx]), dtype=torch.float32),
            "y_five_min_alpha": torch.tensor(float(self._samples.y_five_min_alpha[row_idx]), dtype=torch.float32),
            "y_cls": torch.tensor(float(self._samples.y_cls[row_idx]), dtype=torch.float32),
            "sample_weight": torch.tensor(float(self._samples.sample_weight[row_idx]), dtype=torch.float32),
        }


class _DeepLOBEncoder(nn.Module):
    def __init__(self, per_level_channels: int, hidden_dim: int, *, dropout_p: float = 0.1) -> None:
        super().__init__()
        self.conv1 = nn.Conv2d(per_level_channels, 16, kernel_size=(3, 3), padding=(1, 1))
        self.conv2 = nn.Conv2d(16, 32, kernel_size=(3, 3), padding=(1, 1))
        self.conv3 = nn.Conv2d(32, 32, kernel_size=(3, 3), padding=(1, 1))
        self.dropout = nn.Dropout(dropout_p)
        self.proj = nn.Linear(32 + 5, hidden_dim)
        self.gru = nn.GRU(hidden_dim, hidden_dim, batch_first=True)
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, lob: torch.Tensor, lob_global: torch.Tensor) -> torch.Tensor:
        x = lob.permute(0, 3, 1, 2)
        x = torch.relu(self.conv1(x))
        x = torch.relu(self.conv2(x))
        x = self.dropout(torch.relu(self.conv3(x)))
        x = x.mean(dim=3).permute(0, 2, 1)
        x = torch.cat([x, lob_global], dim=-1)
        x = self.proj(x)
        out, _ = self.gru(x)
        return self.norm(out[:, -1, :])


class _HLOBEncoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=4,
            dim_feedforward=max(hidden_dim * 2, 32),
            dropout=0.1,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, lob: torch.Tensor, lob_global: torch.Tensor) -> torch.Tensor:
        flat = lob.reshape(lob.shape[0], lob.shape[1], -1)
        x = torch.cat([flat, lob_global], dim=-1)
        x = self.input_proj(x)
        return self.norm(self.encoder(x).mean(dim=1))


class _TradeFlowEncoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.gru = nn.GRU(input_dim, hidden_dim, batch_first=True)
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.gru(x)
        return self.norm(out[:, -1, :])


class _V5LobModel(nn.Module):
    def __init__(self, *, backbone_family: str, lob_channels: int, lob_global_dim: int, micro_dim: int, hidden_dim: int, temporal_hidden_dim: int) -> None:
        super().__init__()
        if backbone_family in {"deeplob", "bdlob"}:
            self.lob_encoder = _DeepLOBEncoder(lob_channels, hidden_dim, dropout_p=(0.2 if backbone_family == "bdlob" else 0.1))
        elif backbone_family == "hlob":
            self.lob_encoder = _HLOBEncoder((30 * lob_channels) + lob_global_dim, hidden_dim)
        else:
            raise ValueError(f"unsupported backbone_family: {backbone_family}")
        self.trade_flow_encoder = _TradeFlowEncoder(micro_dim, temporal_hidden_dim)
        self.fusion = nn.Sequential(
            nn.Linear(hidden_dim + temporal_hidden_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(0.2 if backbone_family == "bdlob" else 0.1),
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
        )
        self.alpha_head = nn.Linear(hidden_dim, len(LOB_HORIZONS_SECONDS))
        self.five_min_aux_head = nn.Linear(hidden_dim, 1)
        self.adverse_head = nn.Linear(hidden_dim, 1)
        self.cls_head = nn.Linear(hidden_dim, 1)
        self.uncertainty_head = nn.Sequential(nn.Linear(hidden_dim, 1), nn.Softplus())
        self.backbone_family = backbone_family

    def forward(self, batch: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
        lob_emb = self.lob_encoder(batch["lob"], batch["lob_global"])
        flow_emb = self.trade_flow_encoder(batch["micro"])
        fused = self.fusion(torch.cat([lob_emb, flow_emb], dim=-1))
        return {
            "micro_alpha": self.alpha_head(fused),
            "five_min_alpha": self.five_min_aux_head(fused).squeeze(-1),
            "adverse_excursion": self.adverse_head(fused).squeeze(-1),
            "cls_logit": self.cls_head(fused).squeeze(-1),
            "micro_uncertainty": self.uncertainty_head(fused).squeeze(-1) + 1e-6,
        }


@dataclass
class V5LobEstimator:
    model: _V5LobModel
    backbone_family: str
    bridge_feature_names: tuple[str, ...]
    bridge_score_model: Any
    bridge_alpha_models: dict[str, Any]
    bridge_uncertainty_model: Any
    bridge_adverse_model: Any

    def predict_lob_contract(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        self.model.eval()
        with torch.no_grad():
            tensors = {key: torch.from_numpy(np.asarray(value)).float() for key, value in batch.items()}
            outputs = self.model(tensors)
            micro_alpha = outputs["micro_alpha"].cpu().numpy().astype(np.float64, copy=False)
            uncertainty = outputs["micro_uncertainty"].cpu().numpy().astype(np.float64, copy=False)
            adverse = outputs["adverse_excursion"].cpu().numpy().astype(np.float64, copy=False)
        return {
            "micro_alpha_1s": micro_alpha[:, 0],
            "micro_alpha_5s": micro_alpha[:, 1],
            "micro_alpha_30s": micro_alpha[:, 2],
            "micro_alpha_60s": micro_alpha[:, 3],
            "micro_uncertainty": uncertainty,
            "adverse_excursion_30s": adverse,
        }

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        score_mean = np.clip(np.asarray(self.bridge_score_model.predict(np.asarray(x, dtype=np.float64)), dtype=np.float64), 0.0, 1.0)
        return np.column_stack([1.0 - score_mean, score_mean])

    def predict_panel_contract(self, batch: dict[str, np.ndarray] | np.ndarray) -> dict[str, np.ndarray]:
        if isinstance(batch, dict):
            payload = self.predict_lob_contract(batch)
        else:
            matrix = np.asarray(batch, dtype=np.float64)
            payload = {
                "micro_alpha_1s": np.asarray(self.bridge_alpha_models["h1"].predict(matrix), dtype=np.float64),
                "micro_alpha_5s": np.asarray(self.bridge_alpha_models["h5"].predict(matrix), dtype=np.float64),
                "micro_alpha_30s": np.asarray(self.bridge_alpha_models["h30"].predict(matrix), dtype=np.float64),
                "micro_alpha_60s": np.asarray(self.bridge_alpha_models["h60"].predict(matrix), dtype=np.float64),
                "micro_uncertainty": np.maximum(np.asarray(self.bridge_uncertainty_model.predict(matrix), dtype=np.float64), 1e-6),
                "adverse_excursion_30s": np.asarray(self.bridge_adverse_model.predict(matrix), dtype=np.float64),
            }
        primary_alpha = payload["micro_alpha_30s"]
        uncertainty = payload["micro_uncertainty"]
        adverse = np.abs(payload["adverse_excursion_30s"])
        tradability = np.clip(1.0 / (1.0 + uncertainty), 0.0, 1.0)
        score_mean = (
            np.clip(np.asarray(self.bridge_score_model.predict(np.asarray(batch, dtype=np.float64)), dtype=np.float64), 0.0, 1.0)
            if not isinstance(batch, dict)
            else 1.0 / (1.0 + np.exp(-np.clip(primary_alpha / np.maximum(uncertainty, 1e-6), -40.0, 40.0)))
        )
        score_lcb = np.clip(score_mean - uncertainty, 0.0, 1.0)
        return {
            "final_rank_score": score_mean,
            "final_uncertainty": uncertainty,
            "score_mean": score_mean,
            "score_std": uncertainty,
            "score_lcb": score_lcb,
            "final_expected_return": primary_alpha,
            "final_expected_es": adverse,
            "final_tradability": tradability,
            "final_alpha_lcb": primary_alpha - adverse - uncertainty,
        }


def _supervised_loss(outputs: dict[str, torch.Tensor], batch: dict[str, torch.Tensor]) -> torch.Tensor:
    weight = batch["sample_weight"].unsqueeze(-1)
    alpha_loss = torch.nn.functional.smooth_l1_loss(outputs["micro_alpha"], batch["y_micro_alpha"], reduction="none")
    alpha_loss = torch.mean(alpha_loss * weight)
    aux_loss = torch.mean(torch.nn.functional.smooth_l1_loss(outputs["five_min_alpha"], batch["y_five_min_alpha"], reduction="none") * batch["sample_weight"])
    adverse_loss = torch.mean(torch.nn.functional.smooth_l1_loss(outputs["adverse_excursion"], batch["y_adverse_excursion"], reduction="none") * batch["sample_weight"])
    cls_loss = torch.nn.functional.binary_cross_entropy_with_logits(outputs["cls_logit"], batch["y_cls"], weight=batch["sample_weight"])
    primary_residual = batch["y_micro_alpha"][:, 2] - outputs["micro_alpha"][:, 2]
    uncertainty = outputs["micro_uncertainty"]
    uncertainty_loss = torch.mean(((primary_residual ** 2) / (uncertainty ** 2)) + torch.log(uncertainty ** 2))
    return alpha_loss + (0.5 * aux_loss) + (0.25 * adverse_loss) + (0.25 * cls_loss) + (0.1 * uncertainty_loss)


def _build_lob_batch(samples: _LobSamples) -> dict[str, np.ndarray]:
    return {
        "lob": np.asarray(samples.lob, dtype=np.float32),
        "lob_global": np.asarray(samples.lob_global, dtype=np.float32),
        "micro": np.asarray(samples.micro, dtype=np.float32),
    }


def _write_lob_expert_prediction_table(
    *,
    run_dir: Path,
    samples: _LobSamples,
    split_labels: np.ndarray,
    estimator: V5LobEstimator,
    output_path: Path | None = None,
) -> Path:
    payload = estimator.predict_lob_contract(_build_lob_batch(samples))
    frame = pl.DataFrame(
        {
            "market": np.asarray(samples.markets, dtype=object),
            "ts_ms": np.asarray(samples.ts_ms, dtype=np.int64),
            "split": np.asarray(split_labels, dtype=object),
            "support_level": np.asarray(samples.support_level, dtype=object),
            "y_cls": np.asarray(samples.y_cls, dtype=np.int64),
            "y_reg": np.asarray(samples.y_rank, dtype=np.float64),
            "micro_alpha_1s": np.asarray(payload["micro_alpha_1s"], dtype=np.float64),
            "micro_alpha_5s": np.asarray(payload["micro_alpha_5s"], dtype=np.float64),
            "micro_alpha_30s": np.asarray(payload["micro_alpha_30s"], dtype=np.float64),
            "micro_alpha_60s": np.asarray(payload["micro_alpha_60s"], dtype=np.float64),
            "micro_uncertainty": np.asarray(payload["micro_uncertainty"], dtype=np.float64),
            "adverse_excursion_30s": np.asarray(payload["adverse_excursion_30s"], dtype=np.float64),
        }
    ).sort(["ts_ms", "market"])
    resolved_output_path = Path(output_path) if output_path is not None else (run_dir / "expert_prediction_table.parquet")
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(resolved_output_path)
    return resolved_output_path


def _build_lob_runtime_recommendations(*, options: TrainV5LobOptions, runtime_dataset_root: Path) -> dict[str, Any]:
    return {
        "status": "lob_runtime_ready",
        "source_family": options.model_family,
        "runtime_feature_dataset_root": str(runtime_dataset_root),
    }


def _build_lob_promotion_payload(
    *,
    run_id: str,
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "reasons": ["EXPERT_FAMILY_REQUIRES_EXPLICIT_PROMOTION_PATH"],
        "checks": {
            "existing_champion_present": False,
            "walk_forward_present": True,
            "walk_forward_windows_run": 1,
            "execution_acceptance_enabled": False,
            "execution_acceptance_present": False,
            "risk_control_required": False,
        },
        "research_acceptance": {"walk_forward_summary": {"valid_metrics": valid_metrics, "test_metrics": test_metrics}},
    }


def _options_from_v5_lob_train_config(train_config: dict[str, Any]) -> TrainV5LobOptions:
    base = dict(train_config or {})
    return TrainV5LobOptions(
        dataset_root=Path(str(base["source_dataset_root"] if base.get("source_dataset_root") else base["dataset_root"])),
        registry_root=Path(str(base["registry_root"])),
        logs_root=Path(str(base["logs_root"])),
        model_family=str(base["model_family"]),
        quote=str(base["quote"]),
        top_n=int(base["top_n"]),
        start=str(base["start"]),
        end=str(base["end"]),
        seed=int(base["seed"]),
        backbone_family=str(base.get("backbone_family", "deeplob")),
        batch_size=int(base.get("batch_size", 16)),
        epochs=int(base.get("epochs", 5)),
        learning_rate=float(base.get("learning_rate", 1e-3)),
        train_ratio=float(base.get("train_ratio", 0.6)),
        valid_ratio=float(base.get("valid_ratio", 0.2)),
        test_ratio=float(base.get("test_ratio", 0.2)),
        hidden_dim=int(base.get("hidden_dim", 64)),
        temporal_hidden_dim=int(base.get("temporal_hidden_dim", 64)),
        weight_decay=float(base.get("weight_decay", 1e-4)),
        run_scope=str(base.get("run_scope", "manual_lob_expert")),
    )


def _run_lob_expert_tail(
    *,
    run_dir: Path,
    run_id: str,
    options: TrainV5LobOptions,
    samples: _LobSamples | None,
    labels: np.ndarray | None,
    estimator: V5LobEstimator,
    metrics: dict[str, Any],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    runtime_dataset_root: Path,
    runtime_dataset_written_root: Path,
    sample_payload_loader: Callable[[], tuple[_LobSamples, np.ndarray]] | None,
    resumed: bool,
) -> tuple[Path, Path]:
    tail_started_at = time.time()
    tail_context = build_v5_expert_tail_context(
        run_id=run_id,
        trainer_name="v5_lob",
        model_family=options.model_family,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        dataset_root=Path(str(runtime_dataset_root)),
        source_dataset_root=Path(str(options.dataset_root)),
        runtime_dataset_root=Path(str(runtime_dataset_written_root)),
        selected_markets=samples.selected_markets if samples is not None else tuple(str(item) for item in (load_json(run_dir / "train_config.yaml").get("selected_markets") or [])),
        support_level_counts=samples.support_level_counts if samples is not None else dict(load_json(run_dir / "train_config.yaml").get("support_level_counts") or {}),
        run_scope=options.run_scope,
    )
    existing_tail_artifacts = resolve_existing_v5_expert_tail_artifacts(
        run_dir=run_dir,
        tail_context=tail_context,
    )
    Path(expert_tail_context_path(run_dir)).write_text(
        json.dumps(tail_context, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    update_artifact_status(run_dir, tail_context_written=True)

    runtime_recommendations = _build_lob_runtime_recommendations(
        options=options,
        runtime_dataset_root=runtime_dataset_written_root,
    )
    promotion_payload = _build_lob_promotion_payload(
        run_id=run_id,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
    )
    _ = run_or_reuse_v5_runtime_governance_artifacts(
        run_dir=run_dir,
        trainer_name="v5_lob",
        model_family=options.model_family,
        run_scope=options.run_scope,
        metrics=metrics,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion_payload,
        trainer_research_reasons=["LOB_EXPERT_RUNTIME_READY"],
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        resumed=resumed,
    )
    expert_prediction_table_path = run_or_reuse_v5_expert_prediction_table(
        run_dir=run_dir,
        existing_tail_artifacts=existing_tail_artifacts,
        writer=lambda: _write_lob_expert_prediction_table(
            run_dir=run_dir,
            samples=(samples if samples is not None else sample_payload_loader()[0]),
            split_labels=np.asarray((labels if labels is not None else sample_payload_loader()[1]), dtype=object),
            estimator=estimator,
        ),
    )
    report_path = finalize_v5_expert_family_run(
        run_dir=run_dir,
        run_id=run_id,
        registry_root=options.registry_root,
        model_family=options.model_family,
        logs_root=options.logs_root,
        report_name="train_v5_lob_report.json",
        report_payload={
            "run_id": run_id,
            "status": "candidate",
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": metrics["rows"],
            "leaderboard_row": load_json(run_dir / "leaderboard_row.json"),
            "valid_metrics": valid_metrics,
            "test_metrics": test_metrics,
            "lob_model_contract_path": str(run_dir / "lob_model_contract.json"),
            "expert_prediction_table_path": str(expert_prediction_table_path),
            "runtime_dataset_root": str(runtime_dataset_written_root),
        },
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        resumed=resumed,
        tail_started_at=tail_started_at,
    )
    return expert_prediction_table_path, report_path


def _support_level_weight(level: str) -> float:
    return support_level_weight(level)


def _strict_eval_indices(indices: np.ndarray, support_levels: np.ndarray) -> np.ndarray:
    return strict_eval_indices(indices, support_levels)


def _evaluate_loss(model: _V5LobModel, loader: DataLoader, device: torch.device) -> float:
    values: list[float] = []
    model.eval()
    with torch.no_grad():
        for batch in loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            values.append(float(_supervised_loss(model(batch), batch).item()))
    return float(np.mean(values)) if values else 0.0


def _predict_split(*, model: _V5LobModel, samples: _LobSamples, indices: np.ndarray, device: torch.device) -> dict[str, np.ndarray]:
    loader = DataLoader(_LobTorchDataset(samples, indices), batch_size=64, shuffle=False)
    alpha_parts: list[np.ndarray] = []
    unc_parts: list[np.ndarray] = []
    adverse_parts: list[np.ndarray] = []
    model.eval()
    with torch.no_grad():
        for batch in loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            outputs = model(batch)
            alpha_parts.append(outputs["micro_alpha"].cpu().numpy().astype(np.float64, copy=False))
            unc_parts.append(outputs["micro_uncertainty"].cpu().numpy().astype(np.float64, copy=False))
            adverse_parts.append(outputs["adverse_excursion"].cpu().numpy().astype(np.float64, copy=False))
    return {
        "micro_alpha": np.concatenate(alpha_parts, axis=0) if alpha_parts else np.empty((0, len(LOB_HORIZONS_SECONDS)), dtype=np.float64),
        "micro_uncertainty": np.concatenate(unc_parts, axis=0) if unc_parts else np.empty(0, dtype=np.float64),
        "adverse_excursion": np.concatenate(adverse_parts, axis=0) if adverse_parts else np.empty(0, dtype=np.float64),
    }


def _evaluate_lob_split(
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


def _build_lob_score(alpha_30s: np.ndarray, uncertainty: np.ndarray) -> np.ndarray:
    logits = np.asarray(alpha_30s, dtype=np.float64) / np.maximum(np.asarray(uncertainty, dtype=np.float64), 1e-6)
    return 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))


def _pooled_lob_feature_names() -> tuple[str, ...]:
    names: list[str] = []
    for feature in ("relative_price", "bid_size", "ask_size", "depth_share", "event_delta"):
        names.extend([f"lob_{feature}_last_mean", f"lob_{feature}_time_mean", f"lob_{feature}_last_std"])
    for feature in ("spread_bps", "total_depth", "trade_imbalance", "tick_size", "relative_tick_bps"):
        names.extend([f"lob_global_{feature}_last", f"lob_global_{feature}_mean"])
    for feature in ("trade_events", "trade_imbalance", "spread_bps", "depth_bid_top5", "depth_ask_top5", "imbalance_top5", "microprice_bias"):
        names.extend([f"micro_{feature}_last", f"micro_{feature}_mean", f"micro_{feature}_std"])
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


def _build_pooled_lob_features(*, lob: np.ndarray, lob_global: np.ndarray, micro: np.ndarray) -> np.ndarray:
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
    micro_features = _build_temporal_pool_features(micro)
    return np.column_stack([lob_features, lob_global_features, micro_features]).astype(np.float32, copy=False)


def _build_lob_runtime_extra_columns(samples: _LobSamples) -> dict[str, np.ndarray]:
    rows = int(samples.rows)
    micro_last = np.asarray(samples.micro[:, -1, :], dtype=np.float64)
    ones = np.ones(rows, dtype=np.float64)
    ts_values = np.asarray(samples.ts_ms, dtype=np.int64)
    return {
        "close": np.asarray(samples.close_price, dtype=np.float64),
        "m_trade_events": micro_last[:, 0],
        "m_book_events": ones,
        "m_trade_coverage_ms": np.full(rows, 60_000, dtype=np.int64),
        "m_book_coverage_ms": np.full(rows, 60_000, dtype=np.int64),
        "m_trade_max_ts_ms": ts_values,
        "m_book_max_ts_ms": ts_values,
        "m_trade_imbalance": micro_last[:, 1],
        "m_spread_proxy": micro_last[:, 2],
        "m_depth_bid_top5_mean": micro_last[:, 3],
        "m_depth_ask_top5_mean": micro_last[:, 4],
        "m_micro_available": ones,
        "m_micro_book_available": ones,
    }


def _load_lob_samples(
    options: TrainV5LobOptions,
    *,
    selected_markets_override: tuple[str, ...] | None = None,
) -> _LobSamples:
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
    if "status" in manifest.columns:
        manifest = manifest.filter(pl.col("status") != "FAIL")
    if "cache_file" in manifest.columns:
        manifest = manifest.filter(pl.col("cache_file").cast(pl.Utf8).str.len_chars() > 0)
    if manifest.height <= 0:
        raise ValueError("sequence_v1 manifest has no readable cache rows in the requested range")

    if selected_markets_override:
        selected_markets = [
            str(item).strip().upper() for item in selected_markets_override if str(item).strip()
        ]
    else:
        selected_markets = [
            str(row["market"]).strip().upper()
            for row in (
                manifest.group_by("market")
                .len()
                .sort(["len", "market"], descending=[True, False])
                .iter_rows(named=True)
            )
            if str(row["market"]).strip()
        ]
        if int(options.top_n) > 0:
            selected_markets = selected_markets[: max(int(options.top_n), 1)]
    manifest = manifest.filter(pl.col("market").is_in(selected_markets))
    if manifest.height <= 0:
        raise ValueError("sequence_v1 manifest has no rows after top_n filtering")

    second_root = options.dataset_root.parent / "candles_second_v1" / "tf=1s"
    ws_second_root = options.dataset_root.parent / "ws_candle_v1" / "tf=1s"
    second_root = options.dataset_root.parent / "candles_second_v1" / "tf=1s"
    ws_minute_root = options.dataset_root.parent / "ws_candle_v1" / "tf=1m"
    candles_api_minute_root = options.dataset_root.parent / "candles_api_v1" / "tf=1m"
    candles_v1_minute_root = options.dataset_root.parent / "candles_v1" / "tf=1m"
    second_maps: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    minute_maps: dict[str, dict[int, float]] = {}
    for market in selected_markets:
        second_maps[market] = _load_second_close_series(second_root=second_root, ws_second_root=ws_second_root, market=market)
        minute_maps[market] = _load_minute_close_map_sources(
            market=market,
            roots=(second_root, candles_api_minute_root, candles_v1_minute_root, ws_minute_root),
        )

    lob_parts: list[np.ndarray] = []
    lob_global_parts: list[np.ndarray] = []
    micro_parts: list[np.ndarray] = []
    y_micro_alpha_parts: list[list[float]] = []
    y_adverse_parts: list[float] = []
    y_five_min_parts: list[float] = []
    y_cls_parts: list[int] = []
    y_rank_parts: list[float] = []
    close_parts: list[float] = []
    weight_parts: list[float] = []
    support_level_parts: list[str] = []
    ts_parts: list[int] = []
    market_parts: list[str] = []
    rows_by_market: dict[str, int] = {}
    support_level_counts = {
        SUPPORT_LEVEL_STRICT_FULL: 0,
        SUPPORT_LEVEL_REDUCED_CONTEXT: 0,
        SUPPORT_LEVEL_STRUCTURAL_INVALID: 0,
    }

    for row in manifest.iter_rows(named=True):
        market = str(row["market"]).strip().upper()
        anchor_ts_ms = int(row["anchor_ts_ms"])
        support_level = resolve_sequence_support_level_from_row(row)
        if support_level == SUPPORT_LEVEL_STRUCTURAL_INVALID:
            continue
        second_ts, second_close = second_maps.get(market, (np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)))
        minute_close_map = minute_maps.get(market, {})
        context_end_ts_ms = _resolve_context_end_ts_ms(anchor_ts_ms=anchor_ts_ms, second_ts=second_ts)
        micro_targets = _compute_micro_horizon_returns(second_ts=second_ts, second_close=second_close, context_end_ts_ms=context_end_ts_ms)
        if micro_targets is None:
            continue
        current_close = minute_close_map.get(int(anchor_ts_ms))
        if current_close is None or float(current_close) <= 0.0:
            continue
        five_min_alpha = _compute_five_min_alpha(minute_close_map=minute_close_map, anchor_ts_ms=anchor_ts_ms)
        if five_min_alpha is None:
            continue
        adverse_excursion = _compute_adverse_excursion(second_ts=second_ts, second_close=second_close, context_end_ts_ms=context_end_ts_ms)

        payload = np.load(Path(str(row["cache_file"])))
        lob_parts.append(np.asarray(payload["lob_tensor"], dtype=np.float32))
        lob_global_parts.append(np.asarray(payload["lob_global_tensor"], dtype=np.float32))
        micro_parts.append(np.asarray(payload["micro_tensor"], dtype=np.float32))
        y_micro_alpha_parts.append(list(micro_targets))
        y_adverse_parts.append(float(adverse_excursion))
        y_five_min_parts.append(float(five_min_alpha))
        y_cls_parts.append(1 if float(micro_targets[2]) > 0.0 else 0)
        y_rank_parts.append(float(micro_targets[2]))
        close_parts.append(float(current_close))
        weight = float(
            np.mean(
                [
                    float(row.get("lob_coverage_ratio") or 0.0),
                    float(row.get("micro_coverage_ratio") or 0.0),
                ]
            )
        )
        weight_parts.append(max(weight * _support_level_weight(support_level), 0.1))
        support_level_parts.append(support_level)
        ts_parts.append(anchor_ts_ms)
        market_parts.append(market)
        rows_by_market[market] = rows_by_market.get(market, 0) + 1
        support_level_counts[support_level] += 1

    if not lob_parts:
        raise ValueError("sequence_v1 has no lob-trainable anchors with short-horizon label coverage")

    lob_array = np.stack(lob_parts, axis=0)
    lob_global_array = np.stack(lob_global_parts, axis=0)
    micro_array = np.stack(micro_parts, axis=0)

    return _LobSamples(
        lob=lob_array,
        lob_global=lob_global_array,
        micro=micro_array,
        close_price=np.asarray(close_parts, dtype=np.float64),
        y_micro_alpha=np.asarray(y_micro_alpha_parts, dtype=np.float64),
        y_adverse_excursion=np.asarray(y_adverse_parts, dtype=np.float64),
        y_five_min_alpha=np.asarray(y_five_min_parts, dtype=np.float64),
        y_cls=np.asarray(y_cls_parts, dtype=np.int64),
        y_rank=np.asarray(y_rank_parts, dtype=np.float64),
        sample_weight=np.asarray(weight_parts, dtype=np.float64),
        support_level=np.asarray(support_level_parts, dtype=object),
        ts_ms=np.asarray(ts_parts, dtype=np.int64),
        markets=np.asarray(market_parts, dtype=object),
        pooled_features=_build_pooled_lob_features(lob=lob_array, lob_global=lob_global_array, micro=micro_array),
        feature_names=_pooled_lob_feature_names(),
        selected_markets=tuple(sorted(rows_by_market.keys())),
        rows_by_market=rows_by_market,
        support_level_counts=support_level_counts,
    )


def _export_lob_expert_prediction_table_window(*, run_dir: Path, start: str, end: str) -> dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = replace(_options_from_v5_lob_train_config(train_config), start=str(start), end=str(end))
    data_platform_ready_snapshot_id = (
        str(train_config.get("data_platform_ready_snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd())
    )
    selected_markets = tuple(
        str(item).strip() for item in (train_config.get("selected_markets") or []) if str(item).strip()
    )
    requested_selected_markets = list(selected_markets)
    existing_export = load_existing_expert_runtime_export(run_dir, start, end)
    existing_metadata = dict(existing_export.get("metadata") or {})
    paths = dict(existing_export.get("paths") or {})
    export_path = Path(str(paths.get("export_path")))
    metadata_path = Path(str(paths.get("metadata_path")))
    if (
        bool(existing_export.get("exists", False))
        and str(existing_metadata.get("run_id") or "").strip() == run_dir.name
        and str(existing_metadata.get("data_platform_ready_snapshot_id") or "").strip() == data_platform_ready_snapshot_id
        and str(existing_metadata.get("start") or "").strip() == str(start).strip()
        and str(existing_metadata.get("end") or "").strip() == str(end).strip()
    ):
        return {
            "run_id": run_dir.name,
            "trainer": "v5_lob",
            "model_family": str(train_config.get("model_family") or options.model_family).strip(),
            "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
            "start": str(start).strip(),
            "end": str(end).strip(),
            "rows": int(existing_metadata.get("rows", 0) or 0),
            "selected_markets": list(existing_metadata.get("selected_markets") or []),
            "export_path": str(export_path),
            "metadata_path": str(metadata_path),
            "reused": True,
            "source_mode": "existing_export",
        }

    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable lob estimator: {run_dir}")
    selected_markets_source = "train_selected_markets"
    fallback_reason = ""
    try:
        samples = _load_lob_samples(options, selected_markets_override=selected_markets)
    except ValueError as exc:
        if not selected_markets or "top_n filtering" not in str(exc):
            raise
        samples = _load_lob_samples(options, selected_markets_override=None)
        selected_markets_source = "window_available_markets_fallback"
        fallback_reason = "TRAIN_SELECTED_MARKETS_EMPTY_IN_RUNTIME_WINDOW"
    split_labels = np.full(samples.rows, "runtime", dtype=object)
    export_path = _write_lob_expert_prediction_table(
        run_dir=run_dir,
        samples=samples,
        split_labels=split_labels,
        estimator=estimator,
        output_path=export_path,
    )
    metadata = {
        "version": 1,
        "policy": "v5_expert_runtime_export_v1",
        "run_id": run_dir.name,
        "trainer": "v5_lob",
        "model_family": str(train_config.get("model_family") or options.model_family).strip(),
        "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
        "start": str(start).strip(),
        "end": str(end).strip(),
        "rows": int(samples.rows),
        "requested_selected_markets": requested_selected_markets,
        "selected_markets": list(samples.selected_markets),
        "selected_markets_source": selected_markets_source,
        "fallback_reason": fallback_reason,
    }
    metadata_path = write_expert_runtime_export_metadata(
        run_dir=run_dir,
        start=start,
        end=end,
        payload=metadata,
    )
    return {
        **metadata,
        "export_path": str(export_path),
        "metadata_path": str(metadata_path),
        "reused": False,
        "source_mode": "fresh_export",
    }


def materialize_v5_lob_runtime_export(*, run_dir: Path, start: str, end: str) -> dict[str, Any]:
    return _export_lob_expert_prediction_table_window(run_dir=run_dir, start=start, end=end)


def _load_second_close_series(*, second_root: Path, ws_second_root: Path, market: str) -> tuple[np.ndarray, np.ndarray]:
    frames: list[pl.DataFrame] = []
    for base in (second_root, ws_second_root):
        files = sorted((base / f"market={market}").glob("*.parquet"))
        if files:
            frames.append(pl.concat([pl.read_parquet(path) for path in files], how="vertical"))
    if not frames:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)
    frame = (
        pl.concat(frames, how="vertical")
        .with_row_index("__row_id")
        .sort(["ts_ms", "__row_id"])
        .unique(subset=["ts_ms"], keep="last")
        .sort("ts_ms")
        .drop("__row_id")
    )
    return (
        frame.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
        frame.get_column("close").to_numpy().astype(np.float64, copy=False),
    )


def _load_minute_close_map_sources(*, market: str, roots: tuple[Path, ...]) -> dict[int, float]:
    return load_minute_close_map_sources(market=market, roots=roots)


def _resolve_context_end_ts_ms(*, anchor_ts_ms: int, second_ts: np.ndarray) -> int:
    if second_ts.size <= 0:
        return int(anchor_ts_ms)
    window_end = int(anchor_ts_ms + 59_000)
    mask = (second_ts >= int(anchor_ts_ms)) & (second_ts <= window_end)
    if not np.any(mask):
        return int(anchor_ts_ms)
    return int(second_ts[mask][-1])


def _compute_micro_horizon_returns(*, second_ts: np.ndarray, second_close: np.ndarray, context_end_ts_ms: int) -> list[float] | None:
    if second_ts.size <= 0 or second_close.size <= 0:
        return None
    current_idx = np.searchsorted(second_ts, int(context_end_ts_ms), side="right") - 1
    if current_idx < 0:
        return None
    current_close = float(second_close[current_idx])
    if current_close <= 0.0:
        return None
    values: list[float] = []
    for horizon in LOB_HORIZONS_SECONDS:
        target_ts = int(context_end_ts_ms + (int(horizon) * 1000))
        future_idx = np.searchsorted(second_ts, target_ts, side="right") - 1
        if future_idx < current_idx:
            return None
        future_close = float(second_close[future_idx])
        values.append(float((future_close / current_close) - 1.0))
    return values


def _compute_five_min_alpha(*, minute_close_map: dict[int, float], anchor_ts_ms: int) -> float | None:
    current = minute_close_map.get(int(anchor_ts_ms))
    future = minute_close_map.get(int(anchor_ts_ms + (5 * 60_000)))
    if current is None or future is None or current <= 0.0:
        return None
    return float((future / current) - 1.0)


def _compute_adverse_excursion(*, second_ts: np.ndarray, second_close: np.ndarray, context_end_ts_ms: int) -> float:
    current_idx = np.searchsorted(second_ts, int(context_end_ts_ms), side="right") - 1
    current_close = float(second_close[max(current_idx, 0)])
    if current_close <= 0.0:
        return 0.0
    future_end = int(context_end_ts_ms + 30_000)
    start_idx = max(current_idx, 0)
    end_idx = np.searchsorted(second_ts, future_end, side="right")
    window = second_close[start_idx:end_idx]
    if window.size <= 0:
        return 0.0
    min_return = float(np.min((window / current_close) - 1.0))
    return min_return


def train_and_register_v5_lob(options: TrainV5LobOptions) -> TrainV5LobResult:
    backbone_family = str(options.backbone_family).strip().lower()
    if backbone_family not in VALID_LOB_BACKBONES:
        raise ValueError(f"backbone_family must be one of: {', '.join(VALID_LOB_BACKBONES)}")

    started_at = time.time()
    run_id = make_run_id(seed=options.seed)
    samples = _load_lob_samples(options)
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
        raise ValueError("v5_lob requires non-empty train/valid/test splits")

    torch.manual_seed(int(options.seed))
    np.random.seed(int(options.seed))
    device = torch.device("cpu")
    model = _V5LobModel(
        backbone_family=backbone_family,
        lob_channels=int(samples.lob.shape[3]),
        lob_global_dim=int(samples.lob_global.shape[2]),
        micro_dim=int(samples.micro.shape[2]),
        hidden_dim=max(int(options.hidden_dim), 16),
        temporal_hidden_dim=max(int(options.temporal_hidden_dim), 16),
    ).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=float(options.learning_rate), weight_decay=float(options.weight_decay))
    train_loader = DataLoader(_LobTorchDataset(samples, train_idx), batch_size=max(int(options.batch_size), 1), shuffle=True)
    valid_loader = DataLoader(_LobTorchDataset(samples, valid_idx), batch_size=max(int(options.batch_size), 1), shuffle=False)

    best_state: dict[str, torch.Tensor] | None = None
    best_valid_loss: float | None = None
    for _epoch in range(max(int(options.epochs), 1)):
        model.train()
        for batch in train_loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            optimizer.zero_grad(set_to_none=True)
            loss = _supervised_loss(model(batch), batch)
            loss.backward()
            optimizer.step()
        valid_loss = _evaluate_loss(model, valid_loader, device)
        if best_valid_loss is None or valid_loss < best_valid_loss:
            best_valid_loss = valid_loss
            best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
    if best_state is not None:
        model.load_state_dict(best_state)

    valid_outputs = _predict_split(model=model, samples=samples, indices=valid_idx, device=device)
    test_outputs = _predict_split(model=model, samples=samples, indices=test_idx, device=device)
    all_idx = np.arange(samples.rows, dtype=np.int64)
    all_outputs = _predict_split(model=model, samples=samples, indices=all_idx, device=device)
    valid_eval_idx = _strict_eval_indices(valid_idx, samples.support_level)
    test_eval_idx = _strict_eval_indices(test_idx, samples.support_level)
    valid_eval_positions = np.searchsorted(valid_idx, valid_eval_idx)
    test_eval_positions = np.searchsorted(test_idx, test_eval_idx)
    valid_scores = _build_lob_score(valid_outputs["micro_alpha"][:, 2], valid_outputs["micro_uncertainty"])
    test_scores = _build_lob_score(test_outputs["micro_alpha"][:, 2], test_outputs["micro_uncertainty"])
    valid_metrics = _evaluate_lob_split(
        y_cls=samples.y_cls[valid_eval_idx],
        y_reg=samples.y_rank[valid_eval_idx],
        scores=valid_scores[valid_eval_positions],
        markets=samples.markets[valid_eval_idx],
        sample_weight=samples.sample_weight[valid_eval_idx],
    )
    test_metrics = _evaluate_lob_split(
        y_cls=samples.y_cls[test_eval_idx],
        y_reg=samples.y_rank[test_eval_idx],
        scores=test_scores[test_eval_positions],
        markets=samples.markets[test_eval_idx],
        sample_weight=samples.sample_weight[test_eval_idx],
    )
    thresholds = _build_thresholds(
        valid_scores=valid_scores[valid_eval_positions],
        y_reg_valid=samples.y_rank[valid_eval_idx],
        fee_bps_est=0.0,
        safety_bps=0.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        sample_weight=samples.sample_weight[valid_eval_idx],
    )
    selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores[valid_eval_positions],
        valid_ts_ms=samples.ts_ms[valid_eval_idx],
        thresholds=thresholds,
    )
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        score_source="score_mean",
    )
    selection_calibration = _identity_calibration(reason="LOB_IDENTITY_CALIBRATION")
    bridge_fit_mask = np.asarray(labels != "test", dtype=bool)
    bridge_score_model = fit_ridge_bridge(
        samples.pooled_features[bridge_fit_mask],
        _build_lob_score(all_outputs["micro_alpha"][:, 2], all_outputs["micro_uncertainty"])[bridge_fit_mask],
        clip_min=0.0,
        clip_max=1.0,
    )
    bridge_alpha_models = {
        "h1": fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["micro_alpha"][bridge_fit_mask, 0]),
        "h5": fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["micro_alpha"][bridge_fit_mask, 1]),
        "h30": fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["micro_alpha"][bridge_fit_mask, 2]),
        "h60": fit_ridge_bridge(samples.pooled_features[bridge_fit_mask], all_outputs["micro_alpha"][bridge_fit_mask, 3]),
    }
    bridge_uncertainty_model = fit_ridge_bridge(
        samples.pooled_features[bridge_fit_mask],
        np.maximum(all_outputs["micro_uncertainty"][bridge_fit_mask], 1e-6),
        clip_min=1e-6,
    )
    bridge_adverse_model = fit_ridge_bridge(
        samples.pooled_features[bridge_fit_mask],
        all_outputs["adverse_excursion"][bridge_fit_mask],
    )
    estimator = V5LobEstimator(
        model=model.cpu(),
        backbone_family=backbone_family,
        bridge_feature_names=samples.feature_names,
        bridge_score_model=bridge_score_model,
        bridge_alpha_models=bridge_alpha_models,
        bridge_uncertainty_model=bridge_uncertainty_model,
        bridge_adverse_model=bridge_adverse_model,
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
        "support_level_counts": dict(samples.support_level_counts),
        "evaluation_support_policy": {
            "valid": SUPPORT_LEVEL_STRICT_FULL if valid_eval_idx.size != valid_idx.size else "mixed_available",
            "test": SUPPORT_LEVEL_STRICT_FULL if test_eval_idx.size != test_idx.size else "mixed_available",
        },
        "lob_model": {
            "policy": "v5_lob_v1",
            "backbone_family": backbone_family,
            "outputs": ["micro_alpha_1s", "micro_alpha_5s", "micro_alpha_30s", "micro_uncertainty"],
            "auxiliary_targets": ["micro_alpha_60s", "five_min_alpha", "adverse_excursion_30s"],
        },
    }
    leaderboard_row = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_family": options.model_family,
        "champion": "lob_expert",
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
    runtime_dataset_root = options.registry_root / options.model_family / run_id / "runtime_feature_dataset"
    feature_spec = {
        "feature_columns": list(samples.feature_names),
        "input_modalities": ["lob_tensor", "lob_global_tensor", "micro_tensor"],
        "dataset_root": str(runtime_dataset_root),
    }
    label_spec = {
        "policy": "v5_lob_label_contract_v1",
        "horizons_seconds": list(LOB_HORIZONS_SECONDS),
        "primary_horizon_seconds": 30,
        "auxiliary_targets": ["micro_alpha_60s", "five_min_alpha", "adverse_excursion_30s"],
    }
    data_platform_ready_snapshot_id = resolve_ready_snapshot_id(project_root=Path.cwd())
    train_config = {
        **asdict(options),
        "dataset_root": str(runtime_dataset_root),
        "source_dataset_root": str(options.dataset_root),
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v5_lob",
        "feature_columns": list(samples.feature_names),
        "selected_markets": list(samples.selected_markets),
        "support_level_counts": dict(samples.support_level_counts),
        "autobot_version": autobot_version,
        "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
    }
    runtime_recommendations = _build_lob_runtime_recommendations(
        options=options,
        runtime_dataset_root=runtime_dataset_root,
    )
    data_fingerprint = {
        "dataset_root": str(options.dataset_root),
        "tf": "lob_short_horizon",
        "quote": options.quote,
        "top_n": int(options.top_n),
        "start_ts_ms": _parse_date_to_ts_ms(options.start),
        "end_ts_ms": _parse_date_to_ts_ms(options.end, end_of_day=True),
        "manifest_sha256": _sha256_file(options.dataset_root / "_meta" / "manifest.parquet"),
        "sample_count": int(samples.rows),
        "code_version": autobot_version,
        "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
    }
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="lob_expert",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle={"model_type": "v5_lob_torch", "estimator": estimator},
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
            runtime_recommendations=runtime_recommendations,
        ),
        publish_pointers=False,
    )
    update_artifact_status(run_dir, status="core_saved", core_saved=True)

    lob_model_contract_path = run_dir / "lob_model_contract.json"
    lob_model_contract_path.write_text(
        json.dumps(
            {
                "policy": "v5_lob_v1",
                "backbone_family": backbone_family,
                "input_modalities": feature_spec["input_modalities"],
                "short_horizons_seconds": list(LOB_HORIZONS_SECONDS),
                "outputs": {
                    "micro_alpha_1s": "micro_alpha_1s",
                    "micro_alpha_5s": "micro_alpha_5s",
                    "micro_alpha_30s": "micro_alpha_30s",
                    "micro_uncertainty": "micro_uncertainty",
                },
                "auxiliary_targets": ["micro_alpha_60s", "five_min_alpha", "adverse_excursion_30s"],
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
                "micro_alpha_1s_field": "micro_alpha_1s",
                "micro_alpha_5s_field": "micro_alpha_5s",
                "micro_alpha_30s_field": "micro_alpha_30s",
                "micro_uncertainty_field": "micro_uncertainty",
                "score_mean_field": "score_mean",
                "score_std_field": "micro_uncertainty",
                "score_lcb_field": "score_lcb",
                "final_rank_score_field": "final_rank_score",
                "final_expected_return_field": "final_expected_return",
                "final_expected_es_field": "final_expected_es",
                "final_tradability_field": "final_tradability",
                "final_alpha_lcb_field": "final_alpha_lcb",
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
                "policy": "v5_lob_holdout_v1",
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
    runtime_dataset_written_root = write_runtime_feature_dataset(
        output_root=runtime_dataset_root,
        tf="5m",
        feature_columns=samples.feature_names,
        markets=samples.markets,
        ts_ms=samples.ts_ms,
        x=samples.pooled_features,
        y_cls=samples.y_cls,
        y_reg=samples.y_rank,
        y_rank=samples.y_rank,
        sample_weight=samples.sample_weight,
        extra_columns=_build_lob_runtime_extra_columns(samples),
    )
    expert_prediction_table_path, train_report_path = _run_lob_expert_tail(
        run_dir=run_dir,
        run_id=run_id,
        options=options,
        samples=samples,
        labels=labels,
        estimator=estimator,
        metrics=metrics,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        runtime_dataset_root=runtime_dataset_root,
        runtime_dataset_written_root=runtime_dataset_written_root,
        sample_payload_loader=None,
        resumed=False,
    )

    return TrainV5LobResult(
        run_id=run_id,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=run_dir / "promotion_decision.json",
        walk_forward_report_path=walk_forward_report_path,
        lob_model_contract_path=lob_model_contract_path,
        predictor_contract_path=predictor_contract_path,
    )


def resume_v5_lob_tail(*, run_dir: Path) -> TrainV5LobResult:
    run_dir = Path(run_dir).resolve()
    if not run_dir.exists():
        raise FileNotFoundError(f"run_dir not found: {run_dir}")
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = _options_from_v5_lob_train_config(train_config)
    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable lob estimator: {run_dir}")
    metrics = load_json(run_dir / "metrics.json")
    thresholds = load_json(run_dir / "thresholds.json")
    leaderboard_row = load_json(run_dir / "leaderboard_row.json")
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report = load_json(walk_forward_report_path)
    valid_metrics = dict((walk_forward_report.get("valid_metrics") or {}))
    test_metrics = dict((walk_forward_report.get("test_metrics") or {}))
    data_platform_ready_snapshot_id = (
        str(train_config.get("data_platform_ready_snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd())
    )
    runtime_dataset_root = Path(str(train_config.get("dataset_root") or run_dir / "runtime_feature_dataset"))
    tail_context = build_v5_expert_tail_context(
        run_id=run_dir.name,
        trainer_name="v5_lob",
        model_family=options.model_family,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        dataset_root=runtime_dataset_root,
        source_dataset_root=options.dataset_root,
        runtime_dataset_root=runtime_dataset_root,
        selected_markets=tuple(str(item) for item in (train_config.get("selected_markets") or [])),
        support_level_counts=dict(train_config.get("support_level_counts") or {}),
        run_scope=options.run_scope,
    )
    existing_tail_artifacts = resolve_existing_v5_expert_tail_artifacts(
        run_dir=run_dir,
        tail_context=tail_context,
    )
    needs_samples = not v5_expert_tail_stage_reusable(
        existing_tail_artifacts=existing_tail_artifacts,
        stage_name="expert_prediction_table",
    )
    samples: _LobSamples | None = None
    labels: np.ndarray | None = None
    if needs_samples:
        samples = _load_lob_samples(options)
        labels, _split_info = compute_time_splits(
            samples.ts_ms,
            train_ratio=float(options.train_ratio),
            valid_ratio=float(options.valid_ratio),
            test_ratio=float(options.test_ratio),
            embargo_bars=0,
            interval_ms=60_000,
        )
    lazy_sample_payload: dict[str, Any] = {}

    def _load_sample_payload() -> tuple[_LobSamples, np.ndarray]:
        if "samples" not in lazy_sample_payload or "labels" not in lazy_sample_payload:
            lazy_samples = _load_lob_samples(options)
            lazy_labels, _ = compute_time_splits(
                lazy_samples.ts_ms,
                train_ratio=float(options.train_ratio),
                valid_ratio=float(options.valid_ratio),
                test_ratio=float(options.test_ratio),
                embargo_bars=0,
                interval_ms=60_000,
            )
            lazy_sample_payload["samples"] = lazy_samples
            lazy_sample_payload["labels"] = lazy_labels
        return lazy_sample_payload["samples"], lazy_sample_payload["labels"]

    expert_prediction_table_path, train_report_path = _run_lob_expert_tail(
        run_dir=run_dir,
        run_id=run_dir.name,
        options=options,
        samples=samples,
        labels=labels,
        estimator=estimator,
        metrics=metrics,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        runtime_dataset_root=runtime_dataset_root,
        runtime_dataset_written_root=runtime_dataset_root,
        sample_payload_loader=_load_sample_payload,
        resumed=True,
    )
    return TrainV5LobResult(
        run_id=run_dir.name,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=run_dir / "promotion_decision.json",
        walk_forward_report_path=walk_forward_report_path,
        lob_model_contract_path=run_dir / "lob_model_contract.json",
        predictor_contract_path=run_dir / "predictor_contract.json",
    )
