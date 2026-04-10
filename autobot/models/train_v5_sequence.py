"""PyTorch-based v5 sequence trainer on top of sequence_v1 tensor contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import gc
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Callable

import joblib
import numpy as np
import polars as pl
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

from autobot import __version__ as autobot_version
from autobot.ops.data_platform_snapshot import resolve_ready_snapshot_id
from autobot.strategy.v5_post_model_contract import annotate_v5_runtime_recommendations

from .bridge_models import fit_ridge_bridge
from .metrics import classification_metrics, grouped_trading_metrics, trading_metrics
from .model_card import render_model_card
from .registry import RegistrySavePayload, load_json, load_model_bundle, make_run_id, save_run, update_artifact_status
from .runtime_feature_dataset import write_runtime_feature_dataset
from .selection_calibration import _identity_calibration
from .selection_policy import build_selection_policy_from_recommendations
from .split import compute_time_splits, split_masks
from .train_v1 import _build_thresholds, build_selection_recommendations
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
    OPERATING_WINDOW_TIMEZONE,
    build_operating_window_mask,
    build_ts_date_coverage_payload,
    load_existing_expert_runtime_export,
    load_anchor_export_keys,
    parse_operating_date_to_ts_ms,
    resolve_expert_runtime_export_paths,
    write_expert_runtime_export_metadata,
)
from .v5_runtime_artifacts import persist_v5_runtime_governance_artifacts
from .v5_domain_weighting import (
    build_v5_domain_weighting_report,
    resolve_v5_domain_weighting_components,
    write_v5_domain_weighting_report,
)
from .ood_generalization import build_ood_generalization_report, write_ood_generalization_report
from autobot.data.collect.sequence_tensor_store import (
    SUPPORT_LEVEL_REDUCED_CONTEXT,
    SUPPORT_LEVEL_STRICT_FULL,
    SUPPORT_LEVEL_STRUCTURAL_INVALID,
    resolve_sequence_support_level_from_row,
)


DEFAULT_HORIZONS_MINUTES: tuple[int, ...] = (3, 6, 12, 24)
DEFAULT_QUANTILES: tuple[float, ...] = (0.1, 0.5, 0.9)
SEQUENCE_BACKBONE_ALIASES: dict[str, str] = {
    "patchtst": "patchtst_v1",
    "patchtst_v1": "patchtst_v1",
    "timemixer": "timemixer_v1",
    "timemixer_v1": "timemixer_v1",
    "tft": "tft_v1",
    "tft_v1": "tft_v1",
}
SEQUENCE_BACKBONE_IMPL_FAMILIES: dict[str, str] = {
    "patchtst_v1": "patchtst",
    "timemixer_v1": "timemixer",
    "tft_v1": "tft",
}
VALID_BACKBONES = tuple(SEQUENCE_BACKBONE_ALIASES.keys())
SEQUENCE_PRETRAIN_METHOD_ALIASES: dict[str, str] = {
    "ts2vec_like": "ts2vec_v1",
    "ts2vec_v1": "ts2vec_v1",
    "timemae_like": "timemae_v1",
    "timemae_v1": "timemae_v1",
    "none": "none",
}
SEQUENCE_PRETRAIN_IMPL_METHODS: dict[str, str] = {
    "ts2vec_v1": "ts2vec_like",
    "timemae_v1": "timemae_like",
    "none": "none",
}
VALID_PRETRAIN_METHODS = tuple(SEQUENCE_PRETRAIN_METHOD_ALIASES.keys())
LEADER_MARKETS: tuple[str, ...] = ("KRW-BTC", "KRW-ETH")
SEQUENCE_EXPERT_PREDICTION_CHUNK_ROWS = 2048


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
    backbone_family: str = "patchtst_v1"
    pretrain_method: str = "ts2vec_v1"
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
    sequence_pretrain_contract_path: Path
    sequence_pretrain_report_path: Path
    sequence_pretrain_encoder_path: Path
    domain_weighting_report_path: Path


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
    support_level: np.ndarray
    ts_ms: np.ndarray
    markets: np.ndarray
    pooled_features: np.ndarray
    feature_names: tuple[str, ...]
    selected_markets: tuple[str, ...]
    rows_by_market: dict[str, int]
    support_level_counts: dict[str, int]
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
    return parse_operating_date_to_ts_ms(
        value,
        end_of_day=end_of_day,
        timezone_name=OPERATING_WINDOW_TIMEZONE,
    )


def _normalize_sequence_backbone_family(value: str | None) -> str:
    resolved = str(value or "").strip().lower() or "patchtst_v1"
    canonical = SEQUENCE_BACKBONE_ALIASES.get(resolved)
    if canonical is None:
        raise ValueError(f"backbone_family must be one of: {', '.join(VALID_BACKBONES)}")
    return canonical


def _sequence_backbone_impl_family(canonical_family: str) -> str:
    resolved = str(canonical_family or "").strip().lower()
    if resolved not in SEQUENCE_BACKBONE_IMPL_FAMILIES:
        raise ValueError(f"unsupported canonical sequence backbone: {canonical_family}")
    return SEQUENCE_BACKBONE_IMPL_FAMILIES[resolved]


def _normalize_sequence_pretrain_method(value: str | None) -> str:
    resolved = str(value or "").strip().lower() or "ts2vec_v1"
    canonical = SEQUENCE_PRETRAIN_METHOD_ALIASES.get(resolved)
    if canonical is None:
        raise ValueError(f"pretrain_method must be one of: {', '.join(VALID_PRETRAIN_METHODS)}")
    return canonical


def _sequence_pretrain_impl_method(canonical_method: str) -> str:
    resolved = str(canonical_method or "").strip().lower()
    if resolved not in SEQUENCE_PRETRAIN_IMPL_METHODS:
        raise ValueError(f"unsupported canonical sequence pretrain method: {canonical_method}")
    return SEQUENCE_PRETRAIN_IMPL_METHODS[resolved]


def _slice_sequence_samples(samples: _SequenceSamples, mask: np.ndarray) -> _SequenceSamples:
    resolved_mask = np.asarray(mask, dtype=bool)
    if resolved_mask.shape[0] != samples.rows:
        raise ValueError("sequence runtime export mask length mismatch")
    rows_by_market: dict[str, int] = {}
    support_level_counts = {
        SUPPORT_LEVEL_STRICT_FULL: 0,
        SUPPORT_LEVEL_REDUCED_CONTEXT: 0,
        SUPPORT_LEVEL_STRUCTURAL_INVALID: 0,
    }
    filtered_markets = np.asarray(samples.markets[resolved_mask], dtype=object)
    filtered_support = np.asarray(samples.support_level[resolved_mask], dtype=object)
    for item in filtered_markets:
        market = str(item).strip()
        if market:
            rows_by_market[market] = rows_by_market.get(market, 0) + 1
    for item in filtered_support:
        level = str(item).strip()
        if level in support_level_counts:
            support_level_counts[level] += 1
    return _SequenceSamples(
        second=np.asarray(samples.second[resolved_mask]),
        minute=np.asarray(samples.minute[resolved_mask]),
        micro=np.asarray(samples.micro[resolved_mask]),
        lob=np.asarray(samples.lob[resolved_mask]),
        lob_global=np.asarray(samples.lob_global[resolved_mask]),
        known_covariates=np.asarray(samples.known_covariates[resolved_mask]),
        y_cls=np.asarray(samples.y_cls[resolved_mask]),
        y_reg_primary=np.asarray(samples.y_reg_primary[resolved_mask]),
        y_rank=np.asarray(samples.y_rank[resolved_mask]),
        y_reg_multi=np.asarray(samples.y_reg_multi[resolved_mask]),
        sample_weight=np.asarray(samples.sample_weight[resolved_mask]),
        support_level=np.asarray(filtered_support),
        ts_ms=np.asarray(samples.ts_ms[resolved_mask]),
        markets=filtered_markets,
        pooled_features=np.asarray(samples.pooled_features[resolved_mask]),
        feature_names=samples.feature_names,
        selected_markets=tuple(sorted(rows_by_market.keys())),
        rows_by_market=rows_by_market,
        support_level_counts=support_level_counts,
        horizons_minutes=samples.horizons_minutes,
        quantile_levels=samples.quantile_levels,
    )


def _slice_sequence_samples_by_indices(samples: _SequenceSamples, indices: np.ndarray) -> _SequenceSamples:
    resolved_indices = np.asarray(indices, dtype=np.int64)
    return _SequenceSamples(
        second=np.asarray(samples.second[resolved_indices]),
        minute=np.asarray(samples.minute[resolved_indices]),
        micro=np.asarray(samples.micro[resolved_indices]),
        lob=np.asarray(samples.lob[resolved_indices]),
        lob_global=np.asarray(samples.lob_global[resolved_indices]),
        known_covariates=np.asarray(samples.known_covariates[resolved_indices]),
        y_cls=np.asarray(samples.y_cls[resolved_indices]),
        y_reg_primary=np.asarray(samples.y_reg_primary[resolved_indices]),
        y_rank=np.asarray(samples.y_rank[resolved_indices]),
        y_reg_multi=np.asarray(samples.y_reg_multi[resolved_indices]),
        sample_weight=np.asarray(samples.sample_weight[resolved_indices]),
        support_level=np.asarray(samples.support_level[resolved_indices]),
        ts_ms=np.asarray(samples.ts_ms[resolved_indices]),
        markets=np.asarray(samples.markets[resolved_indices]),
        pooled_features=np.asarray(samples.pooled_features[resolved_indices]),
        feature_names=samples.feature_names,
        selected_markets=samples.selected_markets,
        rows_by_market=samples.rows_by_market,
        support_level_counts=samples.support_level_counts,
        horizons_minutes=samples.horizons_minutes,
        quantile_levels=samples.quantile_levels,
    )


def _align_sequence_samples_to_anchor_export(
    *,
    samples: _SequenceSamples,
    anchor_export_path: Path,
) -> tuple[_SequenceSamples, dict[str, Any]]:
    anchor_frame = load_anchor_export_keys(anchor_export_path)
    source_frame = pl.DataFrame(
        {
            "source_row_idx": np.arange(samples.rows, dtype=np.int64),
            "market": np.asarray(samples.markets, dtype=object),
            "source_ts_ms": np.asarray(samples.ts_ms, dtype=np.int64),
        }
    ).sort(["market", "source_ts_ms"])
    anchor_with_index = anchor_frame.with_row_index("anchor_row_idx").rename({"ts_ms": "anchor_ts_ms"})
    aligned = anchor_with_index.join_asof(
        source_frame,
        left_on="anchor_ts_ms",
        right_on="source_ts_ms",
        by="market",
        strategy="backward",
        check_sortedness=False,
    )
    if aligned.get_column("source_row_idx").null_count() > 0:
        raise ValueError("sequence runtime export could not align all panel anchors")
    source_indices = aligned.get_column("source_row_idx").to_numpy().astype(np.int64, copy=False)
    aligned_samples = _slice_sequence_samples_by_indices(samples, source_indices)
    anchor_ts_values = aligned.get_column("anchor_ts_ms").to_numpy().astype(np.int64, copy=False)
    anchor_markets = aligned.get_column("market").to_numpy()
    rows_by_market: dict[str, int] = {}
    for item in anchor_markets:
        market = str(item).strip()
        if market:
            rows_by_market[market] = rows_by_market.get(market, 0) + 1
    aligned_samples = _SequenceSamples(
        second=aligned_samples.second,
        minute=aligned_samples.minute,
        micro=aligned_samples.micro,
        lob=aligned_samples.lob,
        lob_global=aligned_samples.lob_global,
        known_covariates=aligned_samples.known_covariates,
        y_cls=aligned_samples.y_cls,
        y_reg_primary=aligned_samples.y_reg_primary,
        y_rank=aligned_samples.y_rank,
        y_reg_multi=aligned_samples.y_reg_multi,
        sample_weight=aligned_samples.sample_weight,
        support_level=aligned_samples.support_level,
        ts_ms=np.asarray(anchor_ts_values),
        markets=np.asarray(anchor_markets),
        pooled_features=aligned_samples.pooled_features,
        feature_names=aligned_samples.feature_names,
        selected_markets=tuple(sorted(rows_by_market.keys())),
        rows_by_market=rows_by_market,
        support_level_counts=aligned_samples.support_level_counts,
        horizons_minutes=aligned_samples.horizons_minutes,
        quantile_levels=aligned_samples.quantile_levels,
    )
    lag_values = (
        aligned.get_column("anchor_ts_ms").cast(pl.Int64) - aligned.get_column("source_ts_ms").cast(pl.Int64)
    ).to_numpy()
    return aligned_samples, {
        "anchor_export_path": str(Path(anchor_export_path).resolve()),
        "anchor_row_count": int(anchor_frame.height),
        "anchor_alignment_complete": True,
        "source_anchor_min_ts_ms": int(aligned.get_column("source_ts_ms").min()),
        "source_anchor_max_ts_ms": int(aligned.get_column("source_ts_ms").max()),
        "anchor_match_lag_ms_max": int(np.max(lag_values)) if lag_values.size > 0 else 0,
    }


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


def _predict_sequence_contract_chunked(
    *,
    estimator: V5SequenceEstimator,
    batch: dict[str, np.ndarray],
    chunk_rows: int = SEQUENCE_EXPERT_PREDICTION_CHUNK_ROWS,
) -> dict[str, np.ndarray]:
    resolved_chunk_rows = max(int(chunk_rows), 1)
    row_count = 0
    for value in batch.values():
        array = np.asarray(value)
        if array.ndim <= 0:
            continue
        row_count = int(array.shape[0])
        break
    if row_count <= 0 or row_count <= resolved_chunk_rows:
        return estimator.predict_cache_batch(batch)

    directional_parts: list[np.ndarray] = []
    uncertainty_parts: list[np.ndarray] = []
    quantile_parts: list[np.ndarray] = []
    regime_parts: list[np.ndarray] = []
    for start_idx in range(0, row_count, resolved_chunk_rows):
        end_idx = min(start_idx + resolved_chunk_rows, row_count)
        chunk_batch = {
            key: np.asarray(value)[start_idx:end_idx]
            for key, value in batch.items()
        }
        chunk_payload = estimator.predict_cache_batch(chunk_batch)
        directional_parts.append(np.asarray(chunk_payload["directional_probability_primary"], dtype=np.float64))
        uncertainty_parts.append(np.asarray(chunk_payload["sequence_uncertainty_primary"], dtype=np.float64))
        quantile_parts.append(np.asarray(chunk_payload["return_quantiles_by_horizon"], dtype=np.float64))
        regime_parts.append(np.asarray(chunk_payload["regime_embedding"], dtype=np.float64))
    return {
        "directional_probability_primary": np.concatenate(directional_parts, axis=0) if directional_parts else np.empty(0, dtype=np.float64),
        "sequence_uncertainty_primary": np.concatenate(uncertainty_parts, axis=0) if uncertainty_parts else np.empty(0, dtype=np.float64),
        "return_quantiles_by_horizon": np.concatenate(quantile_parts, axis=0) if quantile_parts else np.empty((0, 0, 0), dtype=np.float64),
        "regime_embedding": np.concatenate(regime_parts, axis=0) if regime_parts else np.empty((0, 0), dtype=np.float64),
    }


def _build_sequence_expert_prediction_schema(
    *,
    samples: _SequenceSamples,
    regime_embedding_dim: int,
) -> dict[str, pl.DataType]:
    schema: dict[str, pl.DataType] = {
        "market": pl.Utf8,
        "ts_ms": pl.Int64,
        "split": pl.Utf8,
        "support_level": pl.Utf8,
        "y_cls": pl.Int64,
        "y_reg": pl.Float64,
        "directional_probability_primary": pl.Float64,
        "sequence_uncertainty_primary": pl.Float64,
    }
    for horizon in samples.horizons_minutes:
        for quantile in samples.quantile_levels:
            schema[f"return_quantile_h{int(horizon)}_q{int(round(float(quantile) * 100))}"] = pl.Float64
    for emb_idx in range(max(int(regime_embedding_dim), 0)):
        schema[f"regime_embedding_{int(emb_idx)}"] = pl.Float64
    return schema


def _build_sequence_expert_prediction_chunk_payload(
    *,
    samples: _SequenceSamples,
    split_labels: np.ndarray,
    payload: dict[str, np.ndarray],
    regime_embedding_dim: int,
) -> dict[str, Any]:
    quantiles = np.asarray(payload["return_quantiles_by_horizon"], dtype=np.float64)
    regime = np.asarray(payload["regime_embedding"], dtype=np.float64)
    frame_payload: dict[str, Any] = {
        "market": np.asarray(samples.markets, dtype=object),
        "ts_ms": np.asarray(samples.ts_ms, dtype=np.int64),
        "split": np.asarray(split_labels, dtype=object),
        "support_level": np.asarray(samples.support_level, dtype=object),
        "y_cls": np.asarray(samples.y_cls, dtype=np.int64),
        "y_reg": np.asarray(samples.y_reg_primary, dtype=np.float64),
        "directional_probability_primary": np.asarray(payload["directional_probability_primary"], dtype=np.float64),
        "sequence_uncertainty_primary": np.asarray(payload["sequence_uncertainty_primary"], dtype=np.float64),
    }
    for horizon_idx, horizon in enumerate(samples.horizons_minutes):
        for quantile_idx, quantile in enumerate(samples.quantile_levels):
            frame_payload[f"return_quantile_h{int(horizon)}_q{int(round(float(quantile) * 100))}"] = quantiles[:, horizon_idx, quantile_idx]
    if regime.ndim == 2:
        for emb_idx in range(min(regime.shape[1], max(int(regime_embedding_dim), 0))):
            frame_payload[f"regime_embedding_{int(emb_idx)}"] = regime[:, emb_idx]
    return frame_payload


def _write_sequence_expert_prediction_table(
    *,
    run_dir: Path,
    samples: _SequenceSamples,
    split_labels: np.ndarray,
    estimator: V5SequenceEstimator,
    output_path: Path | None = None,
) -> Path:
    schema = _build_sequence_expert_prediction_schema(
        samples=samples,
        regime_embedding_dim=int(getattr(estimator, "regime_embedding_dim", 0) or 0),
    )
    frame = pl.DataFrame(schema=schema)
    resolved_output_path = Path(output_path) if output_path is not None else (run_dir / "expert_prediction_table.parquet")
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = resolved_output_path.with_suffix(resolved_output_path.suffix + ".tmp")
    if temp_output_path.exists():
        temp_output_path.unlink()
    row_count = int(samples.rows)
    if row_count <= 0:
        frame.write_parquet(temp_output_path)
        temp_output_path.replace(resolved_output_path)
        return resolved_output_path

    market_keys = np.asarray([str(item) for item in samples.markets], dtype=object)
    sort_order = np.lexsort((market_keys, np.asarray(samples.ts_ms, dtype=np.int64)))

    try:
        import pyarrow.parquet as pq

        writer = None
        try:
            for start_idx in range(0, row_count, SEQUENCE_EXPERT_PREDICTION_CHUNK_ROWS):
                end_idx = min(start_idx + SEQUENCE_EXPERT_PREDICTION_CHUNK_ROWS, row_count)
                chunk_indices = np.asarray(sort_order[start_idx:end_idx], dtype=np.int64)
                chunk_samples = _slice_sequence_samples_by_indices(samples, chunk_indices)
                chunk_split_labels = np.asarray(split_labels[chunk_indices], dtype=object)
                chunk_payload = estimator.predict_cache_batch(_build_sequence_batch(chunk_samples))
                chunk_frame = pl.DataFrame(
                    _build_sequence_expert_prediction_chunk_payload(
                        samples=chunk_samples,
                        split_labels=chunk_split_labels,
                        payload=chunk_payload,
                        regime_embedding_dim=int(getattr(estimator, "regime_embedding_dim", 0) or 0),
                    ),
                    schema=schema,
                )
                chunk_table = chunk_frame.to_arrow()
                if writer is None:
                    writer = pq.ParquetWriter(str(temp_output_path), chunk_table.schema, compression="zstd")
                writer.write_table(chunk_table)
            if writer is None:
                frame.write_parquet(temp_output_path)
            else:
                writer.close()
                writer = None
        finally:
            if writer is not None:
                writer.close()
    except ImportError:
        payload = _predict_sequence_contract_chunked(
            estimator=estimator,
            batch=_build_sequence_batch(samples),
        )
        frame = pl.DataFrame(
            _build_sequence_expert_prediction_chunk_payload(
                samples=samples,
                split_labels=np.asarray(split_labels, dtype=object),
                payload=payload,
                regime_embedding_dim=int(getattr(estimator, "regime_embedding_dim", 0) or 0),
            ),
            schema=schema,
        ).sort(["ts_ms", "market"])
        frame.write_parquet(temp_output_path)
    temp_output_path.replace(resolved_output_path)
    return resolved_output_path


def _build_sequence_runtime_recommendations(
    *,
    options: TrainV5SequenceOptions,
    runtime_dataset_root: Path,
    domain_details: dict[str, Any] | None = None,
    pretrain_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    details = dict(domain_details or {})
    report = dict(pretrain_report or {})
    canonical_backbone = _normalize_sequence_backbone_family(options.backbone_family)
    canonical_pretrain = _normalize_sequence_pretrain_method(options.pretrain_method)
    return annotate_v5_runtime_recommendations({
        "status": "sequence_runtime_ready",
        "source_family": options.model_family,
        "runtime_feature_dataset_root": str(runtime_dataset_root),
        "sequence_variant_name": f"{canonical_backbone}__{canonical_pretrain}",
        "sequence_backbone_name": canonical_backbone,
        "sequence_pretrain_method": canonical_pretrain,
        "sequence_pretrain_ready": bool(report.get("status") == "enabled"),
        "sequence_pretrain_status": str(report.get("status") or ("disabled" if canonical_pretrain == "none" else "enabled")).strip(),
        "sequence_pretrain_objective": str(
            report.get("objective_name")
            or (
                "none"
                if canonical_pretrain == "none"
                else ("ts2vec_alignment_variance_v1" if canonical_pretrain == "ts2vec_v1" else "timemae_masked_reconstruction_v1")
            )
        ).strip(),
        "sequence_pretrain_best_epoch": int(report.get("best_epoch") or 0),
        "sequence_pretrain_encoder_present": bool(report.get("status") == "enabled"),
        "domain_weighting_policy": str(details.get("policy") or "v5_domain_weighting_v1").strip() or "v5_domain_weighting_v1",
        "domain_weighting_source_kind": str(details.get("source_kind") or "regime_inverse_frequency_v1").strip() or "regime_inverse_frequency_v1",
        "domain_weighting_enabled": bool(details.get("enabled", False)),
    })


def _build_sequence_promotion_payload(
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


def _options_from_v5_sequence_train_config(train_config: dict[str, Any]) -> TrainV5SequenceOptions:
    base = dict(train_config or {})
    return TrainV5SequenceOptions(
        dataset_root=Path(str(base["source_dataset_root"] if base.get("source_dataset_root") else base["dataset_root"])),
        registry_root=Path(str(base["registry_root"])),
        logs_root=Path(str(base["logs_root"])),
        model_family=str(base["model_family"]),
        quote=str(base["quote"]),
        top_n=int(base["top_n"]),
        start=str(base["start"]),
        end=str(base["end"]),
        seed=int(base["seed"]),
        backbone_family=_normalize_sequence_backbone_family(str(base.get("backbone_family", "patchtst_v1"))),
        pretrain_method=_normalize_sequence_pretrain_method(str(base.get("pretrain_method", "ts2vec_v1"))),
        batch_size=int(base.get("batch_size", 16)),
        pretrain_epochs=int(base.get("pretrain_epochs", 1)),
        finetune_epochs=int(base.get("finetune_epochs", 5)),
        learning_rate=float(base.get("learning_rate", 1e-3)),
        train_ratio=float(base.get("train_ratio", 0.6)),
        valid_ratio=float(base.get("valid_ratio", 0.2)),
        test_ratio=float(base.get("test_ratio", 0.2)),
        horizons_minutes=tuple(int(item) for item in (base.get("horizons_minutes") or DEFAULT_HORIZONS_MINUTES)),
        quantile_levels=tuple(float(item) for item in (base.get("quantile_levels") or DEFAULT_QUANTILES)),
        hidden_dim=int(base.get("hidden_dim", 64)),
        regime_embedding_dim=int(base.get("regime_embedding_dim", 8)),
        patch_len=int(base.get("patch_len", 4)),
        patch_stride=int(base.get("patch_stride", 2)),
        weight_decay=float(base.get("weight_decay", 1e-4)),
        run_scope=str(base.get("run_scope", "manual_sequence_expert")),
    )


def _run_sequence_expert_tail(
    *,
    run_dir: Path,
    run_id: str,
    options: TrainV5SequenceOptions,
    samples: _SequenceSamples | None,
    labels: np.ndarray | None,
    estimator: V5SequenceEstimator,
    metrics: dict[str, Any],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    runtime_dataset_root: Path,
    runtime_dataset_written_root: Path,
    sample_payload_loader: Callable[[], tuple[_SequenceSamples, np.ndarray]] | None,
    resumed: bool,
) -> tuple[Path, Path]:
    tail_started_at = time.time()
    existing_train_config = load_json(run_dir / "train_config.yaml")
    tail_context = build_v5_expert_tail_context(
        run_id=run_id,
        trainer_name="v5_sequence",
        model_family=options.model_family,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        dataset_root=Path(str(runtime_dataset_root)),
        source_dataset_root=Path(str(options.dataset_root)),
        runtime_dataset_root=Path(str(runtime_dataset_written_root)),
        selected_markets=samples.selected_markets if samples is not None else tuple(str(item) for item in (existing_train_config.get("selected_markets") or [])),
        support_level_counts=samples.support_level_counts if samples is not None else dict(existing_train_config.get("support_level_counts") or {}),
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

    runtime_recommendations = _build_sequence_runtime_recommendations(
        options=options,
        runtime_dataset_root=runtime_dataset_written_root,
        pretrain_report=load_json(run_dir / "sequence_pretrain_report.json"),
    )
    promotion_payload = _build_sequence_promotion_payload(
        run_id=run_id,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
    )
    _ = run_or_reuse_v5_runtime_governance_artifacts(
        run_dir=run_dir,
        trainer_name="v5_sequence",
        model_family=options.model_family,
        run_scope=options.run_scope,
        metrics=metrics,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion_payload,
        trainer_research_reasons=["SEQUENCE_EXPERT_RUNTIME_READY"],
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        resumed=resumed,
    )
    expert_prediction_table_path = run_or_reuse_v5_expert_prediction_table(
        run_dir=run_dir,
        existing_tail_artifacts=existing_tail_artifacts,
        writer=lambda: _write_sequence_expert_prediction_table(
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
        report_name="train_v5_sequence_report.json",
        report_payload={
            "run_id": run_id,
            "status": "candidate",
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": metrics["rows"],
            "leaderboard_row": load_json(run_dir / "leaderboard_row.json"),
            "valid_metrics": valid_metrics,
            "test_metrics": test_metrics,
            "sequence_model_contract_path": str(run_dir / "sequence_model_contract.json"),
            "expert_prediction_table_path": str(expert_prediction_table_path),
            "runtime_dataset_root": str(runtime_dataset_written_root),
        },
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        resumed=resumed,
        tail_started_at=tail_started_at,
    )
    return expert_prediction_table_path, report_path


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
        self.pretrain_projection_head = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, hidden_dim),
        )
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


def _mask_batch_with_ratio(batch: dict[str, torch.Tensor], *, mask_ratio: float) -> dict[str, torch.Tensor]:
    masked = {key: value.clone() for key, value in batch.items() if key not in {"y_cls", "y_reg_multi", "sample_weight"}}
    ratio = min(max(float(mask_ratio), 0.0), 0.95)
    for key in ("second", "minute", "micro", "lob", "lob_global"):
        keep_mask = (torch.rand_like(masked[key]) > ratio).float()
        masked[key] = masked[key] * keep_mask
    return masked


def _temporal_crop_tensor(tensor: torch.Tensor, *, crop_ratio: float, start_fraction: float) -> torch.Tensor:
    if tensor.ndim < 3:
        return tensor
    steps = int(tensor.shape[1])
    if steps <= 1:
        return tensor
    bounded_ratio = min(max(float(crop_ratio), 0.25), 1.0)
    crop_steps = max(1, min(steps, int(round(steps * bounded_ratio))))
    start_max = max(steps - crop_steps, 0)
    start_idx = min(start_max, max(0, int(round(start_max * float(start_fraction)))))
    end_idx = start_idx + crop_steps
    return tensor[:, start_idx:end_idx, ...]


def _temporal_crop_batch(batch: dict[str, torch.Tensor], *, crop_ratio: float, start_fraction: float) -> dict[str, torch.Tensor]:
    cropped = {key: value.clone() for key, value in batch.items() if key not in {"y_cls", "y_reg_multi", "sample_weight"}}
    for key in ("second", "minute", "micro", "lob", "lob_global"):
        cropped[key] = _temporal_crop_tensor(cropped[key], crop_ratio=crop_ratio, start_fraction=start_fraction)
    return cropped


def _build_ts2vec_pretrain_views(batch: dict[str, torch.Tensor]) -> tuple[dict[str, torch.Tensor], dict[str, torch.Tensor], dict[str, torch.Tensor]]:
    view_a = _temporal_crop_batch(_augment_batch(batch), crop_ratio=0.75, start_fraction=0.0)
    view_b = _temporal_crop_batch(_augment_batch(batch), crop_ratio=0.75, start_fraction=1.0)
    crop_view = _temporal_crop_batch(batch, crop_ratio=0.50, start_fraction=0.5)
    return view_a, view_b, crop_view


def _encoder_dim(model: _V5SequenceModel) -> int:
    final_layer = model.pretrain_projection_head[-1]
    if isinstance(final_layer, nn.Linear):
        return int(final_layer.out_features)
    return 0


def _capture_pretrain_encoder_state(model: _V5SequenceModel) -> dict[str, Any]:
    return {
        "second_encoder": model.second_encoder.state_dict(),
        "minute_encoder": model.minute_encoder.state_dict(),
        "micro_encoder": model.micro_encoder.state_dict(),
        "lob_encoder": model.lob_encoder.state_dict(),
        "pretrain_projection_head": model.pretrain_projection_head.state_dict(),
    }


def _load_pretrain_encoder_state(model: _V5SequenceModel, state: dict[str, Any]) -> None:
    if not state:
        return
    for key, module in {
        "second_encoder": model.second_encoder,
        "minute_encoder": model.minute_encoder,
        "micro_encoder": model.micro_encoder,
        "lob_encoder": model.lob_encoder,
        "pretrain_projection_head": model.pretrain_projection_head,
    }.items():
        payload = state.get(key)
        if isinstance(payload, dict) and payload:
            module.load_state_dict(payload)


def _build_encoder_norm_summary(state: dict[str, Any]) -> dict[str, Any]:
    module_means: dict[str, float] = {}
    all_norms: list[float] = []
    for module_name, module_state in dict(state or {}).items():
        if not isinstance(module_state, dict):
            continue
        norms: list[float] = []
        for tensor in module_state.values():
            if isinstance(tensor, torch.Tensor):
                norm_value = float(torch.linalg.vector_norm(tensor.detach().float()).cpu().item())
                norms.append(norm_value)
                all_norms.append(norm_value)
        module_means[module_name] = float(np.mean(norms)) if norms else 0.0
    return {
        "module_mean_l2_norms": module_means,
        "global_mean_l2_norm": float(np.mean(all_norms)) if all_norms else 0.0,
        "parameter_tensor_count": int(len(all_norms)),
    }


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


def _off_diagonal_mean_square(matrix: torch.Tensor) -> torch.Tensor:
    if matrix.ndim != 2 or matrix.shape[0] <= 1:
        return torch.tensor(0.0, device=matrix.device)
    centered = matrix - matrix.mean(dim=0, keepdim=True)
    cov = centered.T @ centered / float(max(matrix.shape[0] - 1, 1))
    mask = ~torch.eye(cov.shape[0], dtype=torch.bool, device=cov.device)
    values = cov[mask]
    if values.numel() <= 0:
        return torch.tensor(0.0, device=matrix.device)
    return torch.mean(values ** 2)


def _run_pretrain(
    *,
    model: _V5SequenceModel,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    method: str,
    epochs: int,
    device: torch.device,
) -> dict[str, Any]:
    if method == "none":
        return {
            "policy": "sequence_pretrain_report_v1",
            "status": "disabled",
            "objective_name": "none",
            "epochs_run": 0,
            "epoch_losses": [],
            "final_loss": None,
            "objective_components": [],
            "epoch_component_history": [],
            "final_component_values": {},
            "best_epoch": 0,
            "encoder_dim": int(_encoder_dim(model)),
            "mask_ratio_schedule": [],
            "augmentation_policy": [],
        }
    epoch_losses: list[float] = []
    epoch_component_history: list[dict[str, float]] = []
    resolved_epochs = max(int(epochs), 1)
    mask_ratio_schedule = (
        np.linspace(0.20, 0.40, resolved_epochs).astype(np.float64).tolist()
        if method != "ts2vec_like"
        else []
    )
    augmentation_policy = (
        ["gaussian_noise_v1", "temporal_crop_mismatch_v1"]
        if method == "ts2vec_like"
        else ["masked_reconstruction_v1", "mask_ratio_schedule_v1"]
    )
    for epoch_index in range(resolved_epochs):
        model.train()
        batch_losses: list[float] = []
        batch_components: list[dict[str, float]] = []
        for batch in loader:
            batch = {key: value.to(device) for key, value in batch.items()}
            optimizer.zero_grad(set_to_none=True)
            if method == "ts2vec_like":
                view_a, view_b, crop_view = _build_ts2vec_pretrain_views(batch)
                emb_a = model.pretrain_projection_head(model.encode(view_a))
                emb_b = model.pretrain_projection_head(model.encode(view_b))
                emb_crop = model.pretrain_projection_head(model.encode(crop_view))
                emb_a = torch.nn.functional.normalize(emb_a, dim=-1)
                emb_b = torch.nn.functional.normalize(emb_b, dim=-1)
                emb_crop = torch.nn.functional.normalize(emb_crop, dim=-1)
                alignment_loss = torch.mean((emb_a - emb_b) ** 2)
                crop_alignment_loss = torch.mean((emb_a - emb_crop) ** 2) + torch.mean((emb_b - emb_crop) ** 2)
                std_a = torch.sqrt(torch.var(emb_a, dim=0, unbiased=False) + 1e-6)
                std_b = torch.sqrt(torch.var(emb_b, dim=0, unbiased=False) + 1e-6)
                variance_loss = torch.mean(torch.relu(1.0 - std_a)) + torch.mean(torch.relu(1.0 - std_b))
                covariance_loss = _off_diagonal_mean_square(emb_a) + _off_diagonal_mean_square(emb_b)
                loss = alignment_loss + (0.5 * crop_alignment_loss) + (0.1 * variance_loss) + (0.01 * covariance_loss)
                batch_components.append(
                    {
                        "alignment_loss": float(alignment_loss.detach().cpu().item()),
                        "crop_alignment_loss": float(crop_alignment_loss.detach().cpu().item()),
                        "variance_loss": float(variance_loss.detach().cpu().item()),
                        "covariance_loss": float(covariance_loss.detach().cpu().item()),
                    }
                )
            else:
                mask_ratio = float(mask_ratio_schedule[epoch_index])
                masked_batch = _mask_batch_with_ratio(batch, mask_ratio=mask_ratio)
                outputs = model(masked_batch)
                reconstruction_loss = torch.nn.functional.mse_loss(outputs["reconstruction"], _pretrain_summary_target(batch))
                loss = reconstruction_loss
                batch_components.append(
                    {
                        "reconstruction_loss": float(reconstruction_loss.detach().cpu().item()),
                        "mask_ratio": mask_ratio,
                    }
                )
            loss.backward()
            optimizer.step()
            batch_losses.append(float(loss.detach().cpu().item()))
        epoch_losses.append(float(np.mean(batch_losses)) if batch_losses else 0.0)
        if batch_components:
            component_summary: dict[str, float] = {}
            for key in batch_components[0].keys():
                component_summary[key] = float(
                    np.mean([float(item.get(key, 0.0)) for item in batch_components])
                )
            epoch_component_history.append(component_summary)
    objective_name = "ts2vec_alignment_variance_v1" if method == "ts2vec_like" else "timemae_masked_reconstruction_v1"
    best_epoch = int(np.argmin(np.asarray(epoch_losses, dtype=np.float64)) + 1) if epoch_losses else 0
    return {
        "policy": "sequence_pretrain_report_v1",
        "status": "enabled",
        "objective_name": objective_name,
        "epochs_run": int(resolved_epochs),
        "epoch_losses": list(epoch_losses),
        "final_loss": float(epoch_losses[-1]) if epoch_losses else None,
        "epoch_component_history": epoch_component_history,
        "final_component_values": dict(epoch_component_history[-1] if epoch_component_history else {}),
        "best_epoch": best_epoch,
        "encoder_dim": int(_encoder_dim(model)),
        "mask_ratio_schedule": [float(item) for item in mask_ratio_schedule],
        "augmentation_policy": list(augmentation_policy),
        "objective_components": (
            ["alignment_loss", "crop_alignment_loss", "variance_loss", "covariance_loss"]
            if method == "ts2vec_like"
            else ["reconstruction_loss", "mask_ratio"]
        ),
    }


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


def _support_level_weight(level: str) -> float:
    return support_level_weight(level)


def _strict_eval_indices(indices: np.ndarray, support_levels: np.ndarray) -> np.ndarray:
    return strict_eval_indices(indices, support_levels)


def _load_sequence_samples(
    options: TrainV5SequenceOptions,
    *,
    selected_markets_override: tuple[str, ...] | None = None,
) -> _SequenceSamples:
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
    ws_root = options.dataset_root.parent / "ws_candle_v1" / "tf=1m"
    candles_api_root = options.dataset_root.parent / "candles_api_v1" / "tf=1m"
    candles_v1_root = options.dataset_root.parent / "candles_v1" / "tf=1m"
    ws_by_market: dict[str, dict[int, float]] = {}
    source_markets = list(dict.fromkeys([*selected_markets, *LEADER_MARKETS]))
    for market in source_markets:
        close_map = _load_minute_close_map_sources(
            market=market,
            roots=(second_root, candles_api_root, candles_v1_root, ws_root),
        )
        if close_map:
            ws_by_market[market] = close_map

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
    support_level_parts: list[str] = []
    ts_parts: list[int] = []
    market_parts: list[str] = []
    rows_by_market: dict[str, int] = {}
    support_level_counts = {
        SUPPORT_LEVEL_STRICT_FULL: 0,
        SUPPORT_LEVEL_REDUCED_CONTEXT: 0,
        SUPPORT_LEVEL_STRUCTURAL_INVALID: 0,
    }
    pooled_feature_names = _pooled_feature_names()

    for row in manifest.iter_rows(named=True):
        market = str(row["market"]).strip().upper()
        anchor_ts_ms = int(row["anchor_ts_ms"])
        support_level = resolve_sequence_support_level_from_row(row)
        if support_level == SUPPORT_LEVEL_STRUCTURAL_INVALID:
            continue
        future_returns = _compute_future_residual_returns(
            ws_by_market=ws_by_market,
            market=market,
            anchor_ts_ms=anchor_ts_ms,
            horizons=options.horizons_minutes,
        )
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
        weight_parts.append(max(weight * _support_level_weight(support_level), 0.1))
        support_level_parts.append(support_level)
        ts_parts.append(anchor_ts_ms)
        market_parts.append(market)
        rows_by_market[market] = rows_by_market.get(market, 0) + 1
        support_level_counts[support_level] += 1

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
        support_level=np.asarray(support_level_parts, dtype=object),
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
        support_level_counts=support_level_counts,
        horizons_minutes=tuple(int(item) for item in options.horizons_minutes),
        quantile_levels=tuple(float(item) for item in options.quantile_levels),
    )


def _load_sequence_runtime_export_samples(
    *,
    options: TrainV5SequenceOptions,
    train_config: dict[str, Any],
    output_start: str,
    output_end: str,
    selected_markets_override: tuple[str, ...] | None,
    anchor_export_path: Path | None,
) -> tuple[_SequenceSamples, dict[str, Any]]:
    if selected_markets_override is not None:
        context_start = min(
            [value for value in [str(train_config.get("start") or "").strip(), str(output_start).strip()] if value]
        )
        context_options = replace(options, start=context_start, end=str(output_end))
        context_samples = _load_sequence_samples(context_options, selected_markets_override=selected_markets_override)
        if anchor_export_path is not None:
            aligned_samples, anchor_contract = _align_sequence_samples_to_anchor_export(
                samples=context_samples,
                anchor_export_path=anchor_export_path,
            )
            return (
                aligned_samples,
                {
                    "generation_context_window": {
                        "start": str(context_options.start).strip(),
                        "end": str(context_options.end).strip(),
                        "source": "train_window_to_panel_anchor_runtime_output_window",
                    },
                    "output_window": {
                        "start": str(output_start).strip(),
                        "end": str(output_end).strip(),
                    },
                    **anchor_contract,
                },
            )
        output_mask = np.asarray(
            build_operating_window_mask(
                context_samples.ts_ms,
                start=str(output_start).strip(),
                end=str(output_end).strip(),
                timezone_name=OPERATING_WINDOW_TIMEZONE,
            ),
            dtype=bool,
        )
        if not np.any(output_mask):
            raise ValueError("sequence runtime export produced no rows in requested certification window")
        return (
            _slice_sequence_samples(context_samples, output_mask),
            {
                "generation_context_window": {
                    "start": str(context_options.start).strip(),
                    "end": str(context_options.end).strip(),
                    "source": "train_window_to_runtime_output_window",
                },
                "output_window": {
                    "start": str(output_start).strip(),
                    "end": str(output_end).strip(),
                },
            },
        )
    output_samples = _load_sequence_samples(options, selected_markets_override=None)
    return (
        output_samples,
        {
            "generation_context_window": {
                "start": str(options.start).strip(),
                "end": str(options.end).strip(),
                "source": "output_window_only",
            },
            "output_window": {
                "start": str(output_start).strip(),
                "end": str(output_end).strip(),
            },
        },
    )


def _export_sequence_expert_prediction_table_window(
    *,
    run_dir: Path,
    start: str,
    end: str,
    selected_markets_override: tuple[str, ...] | None = None,
    anchor_export_path: Path | None = None,
    resolve_markets_only: bool = False,
) -> dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = replace(_options_from_v5_sequence_train_config(train_config), start=str(start), end=str(end))
    data_platform_ready_snapshot_id = (
        str(train_config.get("data_platform_ready_snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd())
    )
    train_selected_markets = tuple(
        str(item).strip() for item in (train_config.get("selected_markets") or []) if str(item).strip()
    )
    selected_markets = (
        tuple(str(item).strip() for item in selected_markets_override if str(item).strip())
        if selected_markets_override is not None
        else train_selected_markets
    )
    requested_selected_markets = list(selected_markets)
    existing_export = load_existing_expert_runtime_export(run_dir, start, end)
    existing_metadata = dict(existing_export.get("metadata") or {})
    paths = dict(existing_export.get("paths") or {})
    export_path = Path(str(paths.get("export_path")))
    metadata_path = Path(str(paths.get("metadata_path")))
    if (
        selected_markets_override is None
        and (not resolve_markets_only)
        and
        bool(existing_export.get("exists", False))
        and str(existing_metadata.get("run_id") or "").strip() == run_dir.name
        and str(existing_metadata.get("data_platform_ready_snapshot_id") or "").strip() == data_platform_ready_snapshot_id
        and str(existing_metadata.get("start") or "").strip() == str(start).strip()
        and str(existing_metadata.get("end") or "").strip() == str(end).strip()
        and existing_metadata.get("coverage_start_ts_ms") is not None
        and existing_metadata.get("coverage_end_ts_ms") is not None
        and str(existing_metadata.get("coverage_start_date") or "").strip()
        and str(existing_metadata.get("coverage_end_date") or "").strip()
        and str(existing_metadata.get("window_timezone") or "").strip() == OPERATING_WINDOW_TIMEZONE
        and (
            anchor_export_path is None
            or (
                bool(existing_metadata.get("anchor_alignment_complete", False))
                and str(existing_metadata.get("anchor_export_path") or "").strip() == str(Path(anchor_export_path).resolve())
            )
        )
    ):
        return {
            "run_id": run_dir.name,
            "trainer": "v5_sequence",
            "model_family": str(train_config.get("model_family") or options.model_family).strip(),
            "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
            "start": str(start).strip(),
            "end": str(end).strip(),
            "coverage_start_ts_ms": int(existing_metadata.get("coverage_start_ts_ms", 0) or 0),
            "coverage_end_ts_ms": int(existing_metadata.get("coverage_end_ts_ms", 0) or 0),
            "coverage_start_date": str(existing_metadata.get("coverage_start_date") or ""),
            "coverage_end_date": str(existing_metadata.get("coverage_end_date") or ""),
            "coverage_dates": list(existing_metadata.get("coverage_dates") or []),
            "window_timezone": str(existing_metadata.get("window_timezone") or ""),
            "anchor_alignment_complete": bool(existing_metadata.get("anchor_alignment_complete", False)),
            "anchor_export_path": str(existing_metadata.get("anchor_export_path") or ""),
            "rows": int(existing_metadata.get("rows", 0) or 0),
            "requested_selected_markets": list(existing_metadata.get("requested_selected_markets") or []),
            "selected_markets": list(existing_metadata.get("selected_markets") or []),
            "selected_markets_source": str(existing_metadata.get("selected_markets_source") or ""),
            "fallback_reason": str(existing_metadata.get("fallback_reason") or ""),
            "export_path": str(export_path),
            "metadata_path": str(metadata_path),
            "reused": True,
            "source_mode": "existing_export",
        }

    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable sequence estimator: {run_dir}")
    selected_markets_source = (
        "acceptance_common_runtime_universe"
        if selected_markets_override is not None
        else "train_selected_markets"
    )
    fallback_reason = ""
    try:
        samples, export_window_contract = _load_sequence_runtime_export_samples(
            options=options,
            train_config=train_config,
            output_start=start,
            output_end=end,
            selected_markets_override=(
                selected_markets
                if (selected_markets_override is not None or selected_markets)
                else None
            ),
            anchor_export_path=anchor_export_path,
        )
    except ValueError as exc:
        if selected_markets_override is not None:
            raise
        if (
            not selected_markets
            or (
                "top_n filtering" not in str(exc)
                and "requested certification window" not in str(exc)
            )
        ):
            raise
        samples, export_window_contract = _load_sequence_runtime_export_samples(
            options=options,
            train_config=train_config,
            output_start=start,
            output_end=end,
            selected_markets_override=None,
            anchor_export_path=None,
        )
        selected_markets_source = "window_available_markets_fallback"
        fallback_reason = "TRAIN_SELECTED_MARKETS_EMPTY_IN_RUNTIME_WINDOW"
    ts_values = np.asarray(samples.ts_ms, dtype=np.int64)
    coverage_payload = build_ts_date_coverage_payload(ts_values, timezone_name=OPERATING_WINDOW_TIMEZONE)
    metadata = {
        "version": 1,
        "policy": "v5_expert_runtime_export_v1",
        "run_id": run_dir.name,
        "trainer": "v5_sequence",
        "model_family": str(train_config.get("model_family") or options.model_family).strip(),
        "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
        "start": str(start).strip(),
        "end": str(end).strip(),
        "coverage_start_ts_ms": int(ts_values.min()) if ts_values.size > 0 else 0,
        "coverage_end_ts_ms": int(ts_values.max()) if ts_values.size > 0 else 0,
        **coverage_payload,
        **export_window_contract,
        "anchor_alignment_complete": bool(export_window_contract.get("anchor_alignment_complete", False)),
        "anchor_export_path": str(export_window_contract.get("anchor_export_path") or ""),
        "rows": int(samples.rows),
        "requested_selected_markets": requested_selected_markets,
        "selected_markets": list(samples.selected_markets),
        "selected_markets_source": selected_markets_source,
        "fallback_reason": fallback_reason,
    }
    if resolve_markets_only:
        return {
            **metadata,
            "export_path": "",
            "metadata_path": "",
            "reused": False,
            "source_mode": "resolve_markets_only",
        }
    split_labels = np.full(samples.rows, "runtime", dtype=object)
    export_path = _write_sequence_expert_prediction_table(
        run_dir=run_dir,
        samples=samples,
        split_labels=split_labels,
        estimator=estimator,
        output_path=export_path,
    )
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


def materialize_v5_sequence_runtime_export(
    *,
    run_dir: Path,
    start: str,
    end: str,
    selected_markets_override: tuple[str, ...] | None = None,
    anchor_export_path: Path | None = None,
    resolve_markets_only: bool = False,
) -> dict[str, Any]:
    return _export_sequence_expert_prediction_table_window(
        run_dir=run_dir,
        start=start,
        end=end,
        selected_markets_override=selected_markets_override,
        anchor_export_path=anchor_export_path,
        resolve_markets_only=resolve_markets_only,
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


def _compute_future_residual_returns(
    *,
    ws_by_market: dict[str, dict[int, float]],
    market: str,
    anchor_ts_ms: int,
    horizons: tuple[int, ...],
) -> list[float] | None:
    market_key = str(market or "").strip().upper()
    raw_returns = _compute_future_returns(
        ws_by_market.get(market_key, {}),
        anchor_ts_ms=anchor_ts_ms,
        horizons=horizons,
    )
    if raw_returns is None:
        return None
    leader_return_sets: list[list[float]] = []
    for leader_market in LEADER_MARKETS:
        if leader_market == market_key:
            continue
        leader_returns = _compute_future_returns(
            ws_by_market.get(leader_market, {}),
            anchor_ts_ms=anchor_ts_ms,
            horizons=horizons,
        )
        if leader_returns is not None:
            leader_return_sets.append(leader_returns)
    if not leader_return_sets:
        return raw_returns
    leader_mean = np.mean(np.asarray(leader_return_sets, dtype=np.float64), axis=0)
    return [
        float(raw - baseline)
        for raw, baseline in zip(raw_returns, leader_mean.tolist(), strict=False)
    ]


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


def _load_minute_close_map_sources(*, market: str, roots: tuple[Path, ...]) -> dict[int, float]:
    return load_minute_close_map_sources(market=market, roots=roots)


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


def _build_sequence_runtime_extra_columns(samples: _SequenceSamples) -> dict[str, np.ndarray]:
    rows = int(samples.rows)
    minute_close = np.asarray(samples.minute[:, -1, 0], dtype=np.float64)
    micro_last = np.asarray(samples.micro[:, -1, :], dtype=np.float64)
    ones = np.ones(rows, dtype=np.float64)
    ts_values = np.asarray(samples.ts_ms, dtype=np.int64)
    return {
        "close": minute_close,
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
    backbone_family = _normalize_sequence_backbone_family(options.backbone_family)
    backbone_impl_family = _sequence_backbone_impl_family(backbone_family)
    pretrain_method = _normalize_sequence_pretrain_method(options.pretrain_method)
    pretrain_impl_method = _sequence_pretrain_impl_method(pretrain_method)

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
    support_weight = np.asarray([_support_level_weight(level) for level in samples.support_level], dtype=np.float64)
    data_quality_weight = np.maximum(
        np.asarray(samples.sample_weight, dtype=np.float64) / np.maximum(support_weight, 1e-12),
        1e-12,
    )
    weight_components = resolve_v5_domain_weighting_components(
        markets=samples.markets,
        ts_ms=samples.ts_ms,
        split_labels=labels,
        base_sample_weight=np.ones(samples.rows, dtype=np.float64),
        data_quality_weight=data_quality_weight,
        support_weight=support_weight,
    )
    samples = replace(
        samples,
        sample_weight=np.asarray(weight_components["final_sample_weight"], dtype=np.float64),
    )
    train_idx = np.flatnonzero(masks["train"])
    valid_idx = np.flatnonzero(masks["valid"])
    test_idx = np.flatnonzero(masks["test"])
    if train_idx.size <= 0 or valid_idx.size <= 0 or test_idx.size <= 0:
        raise ValueError("v5_sequence requires non-empty train/valid/test splits")

    torch.manual_seed(int(options.seed))
    np.random.seed(int(options.seed))
    device = torch.device("cpu")

    model = _V5SequenceModel(
        backbone_family=backbone_impl_family,
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

    pretrain_report = _run_pretrain(
        model=model,
        loader=train_loader,
        optimizer=optimizer,
        method=pretrain_impl_method,
        epochs=max(int(options.pretrain_epochs), 1),
        device=device,
    )
    pretrain_encoder_state = _capture_pretrain_encoder_state(model) if pretrain_impl_method != "none" else {}
    if pretrain_encoder_state:
        pretrain_report = {
            **dict(pretrain_report or {}),
            "encoder_norm_summary": _build_encoder_norm_summary(pretrain_encoder_state),
        }
        _load_pretrain_encoder_state(model, pretrain_encoder_state)

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
    valid_eval_idx = _strict_eval_indices(valid_idx, samples.support_level)
    test_eval_idx = _strict_eval_indices(test_idx, samples.support_level)
    valid_eval_positions = np.searchsorted(valid_idx, valid_eval_idx)
    test_eval_positions = np.searchsorted(test_idx, test_eval_idx)
    valid_metrics = _evaluate_sequence_split(
        y_cls=samples.y_cls[valid_eval_idx],
        y_reg=samples.y_reg_primary[valid_eval_idx],
        scores=valid_outputs["directional_probability"][valid_eval_positions],
        markets=samples.markets[valid_eval_idx],
        sample_weight=samples.sample_weight[valid_eval_idx],
    )
    test_metrics = _evaluate_sequence_split(
        y_cls=samples.y_cls[test_eval_idx],
        y_reg=samples.y_reg_primary[test_eval_idx],
        scores=test_outputs["directional_probability"][test_eval_positions],
        markets=samples.markets[test_eval_idx],
        sample_weight=samples.sample_weight[test_eval_idx],
    )
    thresholds = _build_thresholds(
        valid_scores=valid_outputs["directional_probability"][valid_eval_positions],
        y_reg_valid=samples.y_reg_primary[valid_eval_idx],
        fee_bps_est=0.0,
        safety_bps=0.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        sample_weight=samples.sample_weight[valid_eval_idx],
    )
    selection_recommendations = build_selection_recommendations(
        valid_scores=valid_outputs["directional_probability"][valid_eval_positions],
        valid_ts_ms=samples.ts_ms[valid_eval_idx],
        thresholds=thresholds,
    )
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        score_source="score_mean",
    )
    selection_calibration = _identity_calibration(reason="SEQUENCE_IDENTITY_CALIBRATION")
    residual_sigma_by_horizon = _estimate_residual_sigma_by_horizon(
        targets=samples.y_reg_multi[valid_eval_idx],
        quantiles=valid_outputs["quantiles"][valid_eval_positions],
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
            "support_level_counts": dict(samples.support_level_counts),
            "evaluation_support_policy": {
                "valid": SUPPORT_LEVEL_STRICT_FULL if valid_eval_idx.size != valid_idx.size else "mixed_available",
                "test": SUPPORT_LEVEL_STRICT_FULL if test_eval_idx.size != test_idx.size else "mixed_available",
            },
        "sequence_model": {
            "policy": "v5_sequence_v1",
            "backbone_family": backbone_family,
            "backbone_impl_family": backbone_impl_family,
            "pretrain_method": pretrain_method,
            "pretrain_impl_method": pretrain_impl_method,
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
    runtime_dataset_root = options.registry_root / options.model_family / run_id / "runtime_feature_dataset"
    feature_spec = {
        "feature_columns": list(samples.feature_names),
        "input_modalities": ["second_tensor", "minute_tensor", "micro_tensor", "lob_tensor", "known_covariates"],
        "dataset_root": str(runtime_dataset_root),
    }
    label_spec = {
        "policy": "v5_sequence_label_contract_v1",
        "primary_horizon_minutes": int(samples.horizons_minutes[0]),
        "horizons_minutes": list(samples.horizons_minutes),
        "quantile_levels": list(samples.quantile_levels),
        "target_family": "leader_residualized_return_v1",
        "residualization_policy": "market_return_minus_available_leader_basket_mean",
        "leader_markets": list(LEADER_MARKETS),
        "target_definition": "future 1m close leader-residualized return by horizon from ws_candle_v1 with canonical 1m candle fallback",
    }
    data_platform_ready_snapshot_id = resolve_ready_snapshot_id(project_root=Path.cwd())
    train_config = {
        **asdict(options),
        "dataset_root": str(runtime_dataset_root),
        "source_dataset_root": str(options.dataset_root),
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v5_sequence",
        "feature_columns": list(samples.feature_names),
        "selected_markets": list(samples.selected_markets),
        "support_level_counts": dict(samples.support_level_counts),
        "autobot_version": autobot_version,
        "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
        "backbone_family": backbone_family,
        "backbone_impl_family": backbone_impl_family,
        "pretrain_method": pretrain_method,
        "pretrain_impl_method": pretrain_impl_method,
        "sequence_variant_name": f"{backbone_family}__{pretrain_method}",
        "sequence_pretrain_ready": bool(pretrain_encoder_state),
        "sequence_pretrain_status": str((pretrain_report or {}).get("status") or ("disabled" if pretrain_method == "none" else "enabled")),
        "sequence_pretrain_objective": str((pretrain_report or {}).get("objective_name") or "none"),
        "sequence_pretrain_best_epoch": int((pretrain_report or {}).get("best_epoch") or 0),
        "sequence_pretrain_encoder_present": bool(pretrain_encoder_state),
    }
    runtime_recommendations = _build_sequence_runtime_recommendations(
        options=options,
        runtime_dataset_root=runtime_dataset_root,
        domain_details=dict(weight_components["domain_details"] or {}),
        pretrain_report=pretrain_report,
    )
    data_fingerprint = _build_sequence_data_fingerprint(options=options, sample_count=samples.rows)
    data_fingerprint["data_platform_ready_snapshot_id"] = data_platform_ready_snapshot_id
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
            runtime_recommendations=runtime_recommendations,
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
                "backbone_impl_family": backbone_impl_family,
                "pretrain_method": pretrain_method,
                "pretrain_impl_method": pretrain_impl_method,
                "target_family": "leader_residualized_return_v1",
                "residualization_policy": "market_return_minus_available_leader_basket_mean",
                "leader_markets": list(LEADER_MARKETS),
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
    sequence_pretrain_contract_path = run_dir / "sequence_pretrain_contract.json"
    sequence_pretrain_report_path = run_dir / "sequence_pretrain_report.json"
    sequence_pretrain_encoder_path = run_dir / "sequence_pretrain_encoder.pt"
    if pretrain_encoder_state:
        torch.save(pretrain_encoder_state, sequence_pretrain_encoder_path)
    sequence_pretrain_report_path.write_text(
        json.dumps(
            {
                **dict(pretrain_report or {}),
                "backbone_family": backbone_family,
                "backbone_impl_family": backbone_impl_family,
                "pretrain_method": pretrain_method,
                "pretrain_impl_method": pretrain_impl_method,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    sequence_pretrain_contract_path.write_text(
        json.dumps(
            {
                "policy": "sequence_pretrain_contract_v1",
                "backbone_family": backbone_family,
                "backbone_impl_family": backbone_impl_family,
                "pretrain_method": pretrain_method,
                "pretrain_impl_method": pretrain_impl_method,
                "target_family": "leader_residualized_return_v1",
                "status": "enabled" if pretrain_encoder_state else "disabled",
                "pretrain_ready": bool(pretrain_encoder_state),
                "encoder_artifact_path": str(sequence_pretrain_encoder_path) if pretrain_encoder_state else "",
                "pretrain_report_path": str(sequence_pretrain_report_path),
                "objective_name": str((pretrain_report or {}).get("objective_name") or "none"),
                "objective_components": list((pretrain_report or {}).get("objective_components") or []),
                "final_loss": (pretrain_report or {}).get("final_loss"),
                "best_epoch": int((pretrain_report or {}).get("best_epoch") or 0),
                "encoder_dim": int((pretrain_report or {}).get("encoder_dim") or 0),
                "mask_ratio_schedule": list((pretrain_report or {}).get("mask_ratio_schedule") or []),
                "augmentation_policy": list((pretrain_report or {}).get("augmentation_policy") or []),
                "epochs": int(max(int(options.pretrain_epochs), 1)),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    domain_weighting_report_path = write_v5_domain_weighting_report(
        run_dir=run_dir,
        payload=build_v5_domain_weighting_report(
            run_id=run_id,
            trainer_name="v5_sequence",
            model_family=options.model_family,
            component_order=["base_sample_weight", "data_quality_weight", "support_level_weight", "domain_weight"],
            final_sample_weight=np.asarray(weight_components["final_sample_weight"], dtype=np.float64),
            base_sample_weight=np.asarray(weight_components["base_sample_weight"], dtype=np.float64),
            data_quality_weight=np.asarray(weight_components["data_quality_weight"], dtype=np.float64),
            support_weight=np.asarray(weight_components["support_weight"], dtype=np.float64),
            domain_weight=np.asarray(weight_components["domain_weight"], dtype=np.float64),
            domain_details=dict(weight_components["domain_details"] or {}),
        ),
    )
    ood_generalization_report_path = write_ood_generalization_report(
        run_dir=run_dir,
        payload=build_ood_generalization_report(
            run_id=run_id,
            trainer_name="v5_sequence",
            model_family=options.model_family,
            source_kind=str((weight_components.get("domain_details") or {}).get("source_kind") or "regime_inverse_frequency_v1"),
            markets=samples.markets,
            split_labels=labels,
            effective_sample_weight=np.asarray(weight_components["final_sample_weight"], dtype=np.float64),
            invariant_penalty_enabled=False,
            regime_bucket_labels=np.asarray(labels, dtype=object),
            extra_summary={"target_family": "leader_residualized_return_v1"},
        ),
    )
    runtime_recommendations_path = run_dir / "runtime_recommendations.json"
    runtime_recommendations_payload = load_json(runtime_recommendations_path)
    runtime_recommendations_payload.update(
        {
            "sequence_pretrain_ready": bool(pretrain_encoder_state),
            "sequence_pretrain_best_epoch": int((pretrain_report or {}).get("best_epoch") or 0),
            "sequence_pretrain_encoder_present": bool(pretrain_encoder_state),
            "sequence_pretrain_contract_path": str(sequence_pretrain_contract_path),
            "sequence_pretrain_report_path": str(sequence_pretrain_report_path),
            "sequence_pretrain_encoder_path": str(sequence_pretrain_encoder_path) if pretrain_encoder_state else "",
            "ood_status": "informative_ready",
            "ood_source_kind": str((weight_components.get("domain_details") or {}).get("source_kind") or "regime_inverse_frequency_v1"),
            "ood_penalty_enabled": True,
            "ood_generalization_report_path": str(ood_generalization_report_path),
        }
    )
    runtime_recommendations_path.write_text(
        json.dumps(runtime_recommendations_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    train_config_path = run_dir / "train_config.yaml"
    train_config_payload = load_json(train_config_path)
    train_config_payload.update(
        {
            "sequence_pretrain_ready": bool(pretrain_encoder_state),
            "sequence_pretrain_best_epoch": int((pretrain_report or {}).get("best_epoch") or 0),
            "sequence_pretrain_encoder_present": bool(pretrain_encoder_state),
            "sequence_pretrain_contract_path": str(sequence_pretrain_contract_path),
            "sequence_pretrain_report_path": str(sequence_pretrain_report_path),
            "sequence_pretrain_encoder_path": str(sequence_pretrain_encoder_path) if pretrain_encoder_state else "",
            "ood_status": "informative_ready",
            "ood_source_kind": str((weight_components.get("domain_details") or {}).get("source_kind") or "regime_inverse_frequency_v1"),
            "ood_penalty_enabled": True,
            "ood_generalization_report_path": str(ood_generalization_report_path),
        }
    )
    train_config_path.write_text(
        json.dumps(train_config_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
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
    runtime_dataset_written_root = write_runtime_feature_dataset(
        output_root=runtime_dataset_root,
        tf="5m",
        feature_columns=samples.feature_names,
        markets=samples.markets,
        ts_ms=samples.ts_ms,
        x=samples.pooled_features,
        y_cls=samples.y_cls,
        y_reg=samples.y_reg_primary,
        y_rank=samples.y_rank,
        sample_weight=samples.sample_weight,
        extra_columns=_build_sequence_runtime_extra_columns(samples),
    )
    del train_loader, valid_loader, optimizer, best_state
    del valid_outputs, test_outputs, all_outputs
    del train_idx, valid_idx, test_idx
    del valid_eval_idx, test_eval_idx, valid_eval_positions, test_eval_positions
    del bridge_fit_mask, bridge_probability_model, bridge_quantile_models
    del support_weight, data_quality_weight, weight_components
    del pretrain_encoder_state, pretrain_report
    del model
    gc.collect()
    expert_prediction_table_path, train_report_path = _run_sequence_expert_tail(
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

    return TrainV5SequenceResult(
        run_id=run_id,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=run_dir / "promotion_decision.json",
        walk_forward_report_path=walk_forward_report_path,
        sequence_model_contract_path=sequence_model_contract_path,
        predictor_contract_path=predictor_contract_path,
        sequence_pretrain_contract_path=sequence_pretrain_contract_path,
        sequence_pretrain_report_path=sequence_pretrain_report_path,
        sequence_pretrain_encoder_path=sequence_pretrain_encoder_path,
        domain_weighting_report_path=domain_weighting_report_path,
    )


def resume_v5_sequence_tail(*, run_dir: Path) -> TrainV5SequenceResult:
    run_dir = Path(run_dir).resolve()
    if not run_dir.exists():
        raise FileNotFoundError(f"run_dir not found: {run_dir}")
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = _options_from_v5_sequence_train_config(train_config)
    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable sequence estimator: {run_dir}")
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
        trainer_name="v5_sequence",
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
    samples: _SequenceSamples | None = None
    labels: np.ndarray | None = None
    if needs_samples:
        samples = _load_sequence_samples(options)
        labels, _split_info = compute_time_splits(
            samples.ts_ms,
            train_ratio=float(options.train_ratio),
            valid_ratio=float(options.valid_ratio),
            test_ratio=float(options.test_ratio),
            embargo_bars=0,
            interval_ms=60_000,
        )
    lazy_sample_payload: dict[str, Any] = {}

    def _load_sample_payload() -> tuple[_SequenceSamples, np.ndarray]:
        if "samples" not in lazy_sample_payload or "labels" not in lazy_sample_payload:
            lazy_samples = _load_sequence_samples(options)
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

    expert_prediction_table_path, train_report_path = _run_sequence_expert_tail(
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
    return TrainV5SequenceResult(
        run_id=run_dir.name,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=run_dir / "promotion_decision.json",
        walk_forward_report_path=walk_forward_report_path,
        sequence_model_contract_path=run_dir / "sequence_model_contract.json",
        predictor_contract_path=run_dir / "predictor_contract.json",
        sequence_pretrain_contract_path=run_dir / "sequence_pretrain_contract.json",
        sequence_pretrain_report_path=run_dir / "sequence_pretrain_report.json",
        sequence_pretrain_encoder_path=run_dir / "sequence_pretrain_encoder.pt",
        domain_weighting_report_path=run_dir / "domain_weighting_report.json",
    )
