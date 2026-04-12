from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.predictor import load_predictor_from_registry
from autobot.models.registry import load_json
from autobot.models.train_v5_fusion import TrainV5FusionOptions, train_and_register_v5_fusion
from autobot.models.train_v5_lob import (
    _LobSamples,
    _predict_lob_contract_chunked,
    _write_lob_expert_prediction_table,
    TrainV5LobOptions,
    _load_minute_close_map_sources,
    materialize_v5_lob_runtime_export,
    resume_v5_lob_tail,
    train_and_register_v5_lob,
)
from autobot.models.train_v5_tradability import (
    TrainV5TradabilityOptions,
    materialize_v5_tradability_runtime_export,
    train_and_register_v5_tradability,
)
from autobot.models.v5_expert_runtime_export import write_expert_runtime_export_metadata


def _write_downstream_expert_run(
    *,
    registry_root: Path,
    family: str,
    run_id: str,
    trainer: str,
    snapshot_id: str,
    rows: list[dict[str, object]],
) -> tuple[Path, Path]:
    run_dir = registry_root / family / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    table_path = run_dir / "expert_prediction_table.parquet"
    markets = sorted({str(row.get("market") or "").strip().upper() for row in rows if str(row.get("market") or "").strip()})
    pl.DataFrame(rows).write_parquet(table_path)
    (run_dir / "train_config.yaml").write_text(
        json.dumps(
            {
                "trainer": trainer,
                "model_family": family,
                "data_platform_ready_snapshot_id": snapshot_id,
                "selected_markets": markets,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    runtime_payload: dict[str, object]
    if trainer == "v5_panel_ensemble":
        runtime_payload = {
            "status": "v5_panel_ensemble_runtime_ready",
            "source_family": family,
            "data_platform_ready_snapshot_id": snapshot_id,
            "exit": {
                "version": 1,
                "recommended_exit_mode": "risk",
                "recommended_exit_mode_source": "test_panel_runtime",
                "recommended_exit_mode_reason_code": "TEST_PANEL_RUNTIME",
            },
            "execution": {
                "policy": "empirical_fill_frontier_v1",
                "stage_decision_mode": "sequential_positive_net_edge_v1",
                "stage_order": ["JOIN"],
                "stages": [
                    {
                        "stage": "JOIN",
                        "supported": True,
                        "validation_comparable": True,
                        "recommended_price_mode": "JOIN",
                        "recommended_timeout_bars": 2,
                        "recommended_replace_max": 1,
                        "expected_fill_probability": 0.8,
                        "expected_slippage_bps": 4.0,
                        "expected_cleanup_cost_bps": 7.0,
                        "expected_miss_cost_bps": 8.0,
                    }
                ],
                "frontier_summary": {
                    "supported_stage_count": 1,
                    "best_stage_by_objective": "JOIN",
                    "best_stage_by_fill_probability": "JOIN",
                    "best_stage_by_time_to_fill": "JOIN",
                },
                "recommended_price_mode": "JOIN",
                "recommended_timeout_bars": 2,
                "recommended_replace_max": 1,
                "recommendation_source": "test_panel_runtime",
            },
            "risk_control": {
                "status": "not_required",
                "contract_status": "not_required",
                "operating_mode": "test_panel_runtime",
            },
            "trade_action": {"status": "ready", "policy": "test_trade_action"},
        }
    else:
        runtime_payload = {
            "status": f"{trainer}_runtime_ready",
            "source_family": family,
            "data_platform_ready_snapshot_id": snapshot_id,
        }
    (run_dir / "runtime_recommendations.json").write_text(
        json.dumps(runtime_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return run_dir, table_path


def _write_downstream_runtime_export(
    *,
    run_dir: Path,
    start: str,
    end: str,
    rows: list[dict[str, object]],
    anchor_export_path: str = "",
) -> Path:
    export_path = run_dir / "_runtime_exports" / f"{start}__{end}" / "expert_prediction_table.parquet"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(export_path)
    write_expert_runtime_export_metadata(
        run_dir=run_dir,
        start=start,
        end=end,
        payload={
            "start": start,
            "end": end,
            "coverage_start_ts_ms": int(min(int(row["ts_ms"]) for row in rows)) if rows else 0,
            "coverage_end_ts_ms": int(max(int(row["ts_ms"]) for row in rows)) if rows else 0,
            "coverage_start_date": start,
            "coverage_end_date": end,
            "coverage_dates": [start] if start == end else [start, end],
            "window_timezone": "Asia/Seoul",
            "selected_markets": sorted({str(row.get("market") or "").strip().upper() for row in rows if str(row.get("market") or "").strip()}),
            "requested_selected_markets": sorted({str(row.get("market") or "").strip().upper() for row in rows if str(row.get("market") or "").strip()}),
            "selected_markets_source": "acceptance_common_runtime_universe",
            "fallback_reason": "",
            "anchor_alignment_complete": bool(anchor_export_path),
            "anchor_export_path": anchor_export_path,
            "rows": len(rows),
        },
    )
    return export_path


def _train_minimal_lob_result(tmp_path: Path) -> object:
    dataset_root = tmp_path / "parquet" / "sequence_v1"
    meta_root = dataset_root / "_meta"
    cache_root = dataset_root / "cache" / "market=KRW-BTC" / "date=2026-03-27"
    meta_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)

    anchors = [1_774_569_600_000 + (idx * 60_000) for idx in range(12)]
    manifest_rows = []
    for idx, anchor_ts_ms in enumerate(anchors):
        cache_file = cache_root / f"anchor-{anchor_ts_ms}.npz"
        np.savez_compressed(
            cache_file,
            second_tensor=np.full((4, 4), 0.1 + idx, dtype=np.float32),
            minute_tensor=np.full((1, 4), 0.2 + idx, dtype=np.float32),
            micro_tensor=np.full((1, 7), 0.3 + idx, dtype=np.float32),
            lob_tensor=np.full((4, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((4, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((4,), dtype=np.float32),
        )
        manifest_rows.append(
            {
                "market": "KRW-BTC",
                "date": "2026-03-27",
                "anchor_ts_ms": anchor_ts_ms,
                "anchor_utc": "2026-03-27T00:00:00+00:00",
                "status": "OK",
                "reasons_json": "[]",
                "error_message": None,
                "cache_file": str(cache_file),
                "second_coverage_ratio": 1.0,
                "minute_coverage_ratio": 1.0,
                "micro_coverage_ratio": 1.0,
                "lob_coverage_ratio": 1.0,
                "built_at_ms": anchor_ts_ms + 1,
            }
        )
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")

    second_root = tmp_path / "parquet" / "candles_second_v1" / "tf=1s" / "market=KRW-BTC"
    second_root.mkdir(parents=True, exist_ok=True)
    second_rows = []
    for minute_idx, anchor_ts_ms in enumerate(anchors):
        base_price = 100.0 + minute_idx
        for sec in range(0, 65):
            ts_ms = anchor_ts_ms + (sec * 1_000)
            second_rows.append(
                {
                    "ts_ms": ts_ms,
                    "open": base_price,
                    "high": base_price + 0.2,
                    "low": base_price - 0.2,
                    "close": base_price + (sec * 0.01),
                    "volume_base": 1.0,
                    "volume_quote": 100.0,
                    "volume_quote_est": False,
                }
            )
    pl.DataFrame(second_rows).write_parquet(second_root / "part-000.parquet")

    minute_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    minute_root.mkdir(parents=True, exist_ok=True)
    minute_rows = []
    for idx in range(20):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 200.0 + idx
        minute_rows.append(
            {
                "ts_ms": ts_ms,
                "open": close - 0.5,
                "high": close + 0.5,
                "low": close - 0.7,
                "close": close,
                "volume_base": 2.0,
                "volume_quote": 200.0,
                "volume_quote_est": False,
            }
        )
    pl.DataFrame(minute_rows).write_parquet(minute_root / "part-000.parquet")

    options = TrainV5LobOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_lob",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        backbone_family="deeplob",
        batch_size=4,
        epochs=1,
        hidden_dim=16,
        temporal_hidden_dim=16,
    )
    return train_and_register_v5_lob(options)


def test_predict_lob_contract_chunked_preserves_order(tmp_path: Path) -> None:
    class _FakeEstimator:
        def __init__(self) -> None:
            self.calls: list[list[float]] = []

        def predict_lob_contract(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
            row_ids = np.asarray(batch["lob"][:, 0, 0, 0], dtype=np.float64)
            self.calls.append(row_ids.tolist())
            return {
                "micro_alpha_1s": row_ids + 1.0,
                "micro_alpha_5s": row_ids + 5.0,
                "micro_alpha_30s": row_ids + 30.0,
                "micro_alpha_60s": row_ids + 60.0,
                "micro_uncertainty": row_ids + 0.5,
                "adverse_excursion_30s": row_ids + 0.25,
            }

    estimator = _FakeEstimator()
    batch = {
        "lob": np.arange(5, dtype=np.float32).reshape(5, 1, 1, 1),
        "lob_global": np.zeros((5, 1, 1), dtype=np.float32),
        "micro": np.zeros((5, 1, 1), dtype=np.float32),
    }

    payload = _predict_lob_contract_chunked(
        estimator=estimator,
        batch=batch,
        chunk_rows=2,
    )

    assert estimator.calls == [[0.0, 1.0], [2.0, 3.0], [4.0]]
    assert payload["micro_alpha_30s"].tolist() == [30.0, 31.0, 32.0, 33.0, 34.0]
    assert payload["micro_uncertainty"].tolist() == [0.5, 1.5, 2.5, 3.5, 4.5]


def test_write_lob_expert_prediction_table_streams_sorted_chunks(tmp_path: Path) -> None:
    class _FakeEstimator:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        def predict_lob_contract(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
            row_ids = np.asarray(batch["lob"][:, 0, 0, 0], dtype=np.int64)
            self.calls.append(row_ids.tolist())
            return {
                "micro_alpha_1s": row_ids.astype(np.float64) + 1.0,
                "micro_alpha_5s": row_ids.astype(np.float64) + 5.0,
                "micro_alpha_30s": row_ids.astype(np.float64) + 30.0,
                "micro_alpha_60s": row_ids.astype(np.float64) + 60.0,
                "micro_uncertainty": row_ids.astype(np.float64) + 0.5,
                "adverse_excursion_30s": row_ids.astype(np.float64) + 0.25,
            }

    estimator = _FakeEstimator()
    samples = _LobSamples(
        lob=np.asarray([[[[2.0]]], [[[0.0]]], [[[1.0]]]], dtype=np.float32),
        lob_global=np.zeros((3, 1, 1), dtype=np.float32),
        micro=np.zeros((3, 1, 1), dtype=np.float32),
        close_price=np.asarray([10.0, 20.0, 30.0], dtype=np.float64),
        y_micro_alpha=np.zeros((3, 4), dtype=np.float64),
        y_adverse_excursion=np.zeros(3, dtype=np.float64),
        y_five_min_alpha=np.zeros(3, dtype=np.float64),
        y_cls=np.asarray([1, 0, 1], dtype=np.int64),
        y_rank=np.asarray([0.3, 0.1, 0.2], dtype=np.float64),
        sample_weight=np.ones(3, dtype=np.float64),
        support_level=np.asarray(["strict_full", "reduced_context", "strict_full"], dtype=object),
        ts_ms=np.asarray([3000, 1000, 1000], dtype=np.int64),
        markets=np.asarray(["KRW-BTC", "KRW-ETH", "KRW-BTC"], dtype=object),
        pooled_features=np.zeros((3, 2), dtype=np.float64),
        feature_names=("a", "b"),
        selected_markets=("KRW-BTC", "KRW-ETH"),
        rows_by_market={"KRW-BTC": 2, "KRW-ETH": 1},
        support_level_counts={"strict_full": 2, "reduced_context": 1, "structural_invalid": 0},
    )
    split_labels = np.asarray(["test", "train", "valid"], dtype=object)

    output_path = _write_lob_expert_prediction_table(
        run_dir=tmp_path,
        samples=samples,
        split_labels=split_labels,
        estimator=estimator,
        output_path=tmp_path / "expert_prediction_table.parquet",
    )

    assert output_path.exists()
    written = pl.read_parquet(output_path)
    assert written.get_column("ts_ms").to_list() == [1000, 1000, 3000]
    assert written.get_column("market").to_list() == ["KRW-BTC", "KRW-ETH", "KRW-BTC"]
    assert written.get_column("micro_alpha_30s").to_list() == [31.0, 30.0, 32.0]
    assert estimator.calls == [[1, 0, 2]]


def test_train_v5_lob_writes_core_contract_artifacts(tmp_path: Path) -> None:
    dataset_root = tmp_path / "parquet" / "sequence_v1"
    meta_root = dataset_root / "_meta"
    cache_root = dataset_root / "cache" / "market=KRW-BTC" / "date=2026-03-27"
    meta_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)

    anchors = [1_774_569_600_000 + (idx * 60_000) for idx in range(12)]
    manifest_rows = []
    for idx, anchor_ts_ms in enumerate(anchors):
        cache_file = cache_root / f"anchor-{anchor_ts_ms}.npz"
        np.savez_compressed(
            cache_file,
            second_tensor=np.full((4, 4), 0.1 + idx, dtype=np.float32),
            minute_tensor=np.full((1, 4), 0.2 + idx, dtype=np.float32),
            micro_tensor=np.full((1, 7), 0.3 + idx, dtype=np.float32),
            lob_tensor=np.full((4, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((4, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((4,), dtype=np.float32),
        )
        manifest_rows.append(
            {
                "market": "KRW-BTC",
                "date": "2026-03-27",
                "anchor_ts_ms": anchor_ts_ms,
                "anchor_utc": "2026-03-27T00:00:00+00:00",
                "status": "OK",
                "reasons_json": "[]",
                "error_message": None,
                "cache_file": str(cache_file),
                "second_coverage_ratio": 1.0,
                "minute_coverage_ratio": 1.0,
                "micro_coverage_ratio": 1.0,
                "lob_coverage_ratio": 1.0,
                "built_at_ms": anchor_ts_ms + 1,
            }
        )
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")

    second_root = tmp_path / "parquet" / "candles_second_v1" / "tf=1s" / "market=KRW-BTC"
    second_root.mkdir(parents=True, exist_ok=True)
    second_rows = []
    for minute_idx, anchor_ts_ms in enumerate(anchors):
        base_price = 100.0 + minute_idx
        for sec in range(0, 65):
            ts_ms = anchor_ts_ms + (sec * 1_000)
            second_rows.append(
                {
                    "ts_ms": ts_ms,
                    "open": base_price,
                    "high": base_price + 0.2,
                    "low": base_price - 0.2,
                    "close": base_price + (sec * 0.01),
                    "volume_base": 1.0,
                    "volume_quote": 100.0,
                    "volume_quote_est": False,
                }
            )
    pl.DataFrame(second_rows).write_parquet(second_root / "part-000.parquet")

    minute_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    minute_root.mkdir(parents=True, exist_ok=True)
    minute_rows = []
    for idx in range(20):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 200.0 + idx
        minute_rows.append(
            {
                "ts_ms": ts_ms,
                "open": close - 0.5,
                "high": close + 0.5,
                "low": close - 0.7,
                "close": close,
                "volume_base": 2.0,
                "volume_quote": 200.0,
                "volume_quote_est": False,
            }
        )
    pl.DataFrame(minute_rows).write_parquet(minute_root / "part-000.parquet")

    options = TrainV5LobOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_lob",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        backbone_family="deeplob",
        batch_size=4,
        epochs=1,
        hidden_dim=16,
        temporal_hidden_dim=16,
        run_scope="scheduled_daily_dependency_v5_lob",
    )
    result = train_and_register_v5_lob(options)

    assert result.run_dir.exists()
    assert result.lob_model_contract_path.exists()
    assert result.predictor_contract_path.exists()
    assert result.lob_backbone_contract_path.exists()
    assert result.lob_target_contract_path.exists()
    assert result.domain_weighting_report_path.exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
    assert (result.run_dir / "runtime_feature_dataset" / "_meta" / "feature_spec.json").exists()
    assert result.walk_forward_report_path.exists()
    assert result.promotion_path.exists()
    assert (result.run_dir / "expert_tail_context.json").exists()
    assert load_json(result.run_dir / "train_config.yaml")["trainer"] == "v5_lob"
    assert load_json(result.lob_model_contract_path)["policy"] == "v5_lob_v1"
    assert load_json(result.lob_backbone_contract_path)["policy"] == "lob_backbone_contract_v1"
    assert load_json(result.lob_backbone_contract_path)["backbone_family"] == "deeplob_v1"
    assert load_json(result.lob_target_contract_path)["policy"] == "lob_target_contract_v1"
    assert load_json(result.lob_target_contract_path)["primary_horizon_seconds"] == 30
    assert load_json(result.domain_weighting_report_path)["policy"] == "v5_domain_weighting_v1"
    assert load_json(result.run_dir / "runtime_recommendations.json")["lob_backbone_name"] == "deeplob_v1"
    assert load_json(result.run_dir / "runtime_recommendations.json")["lob_uncertainty_head"] == "softplus_scalar"
    assert load_json(result.predictor_contract_path)["micro_alpha_30s_field"] == "micro_alpha_30s"
    assert load_json(result.train_report_path)["resumed"] is False
    assert float(load_json(result.train_report_path)["tail_duration_sec"]) >= 0.0
    artifact_status = load_json(result.run_dir / "artifact_status.json")
    assert artifact_status["tail_context_written"] is True
    assert artifact_status["runtime_recommendations_complete"] is True
    assert artifact_status["promotion_complete"] is True
    assert artifact_status["decision_surface_complete"] is True
    assert artifact_status["expert_prediction_table_complete"] is True
    predictor = load_predictor_from_registry(
        registry_root=tmp_path / "registry",
        model_ref=result.run_id,
        model_family="train_v5_lob",
    )
    payload = predictor.predict_score_contract(np.zeros((2, len(predictor.feature_columns)), dtype=np.float64))
    assert payload["final_rank_score"].shape == (2,)
    assert (tmp_path / "registry" / "train_v5_lob" / "latest.json").exists()


def test_resume_v5_lob_tail_reuses_existing_artifacts(tmp_path: Path) -> None:
    dataset_root = tmp_path / "parquet" / "sequence_v1"
    meta_root = dataset_root / "_meta"
    cache_root = dataset_root / "cache" / "market=KRW-BTC" / "date=2026-03-27"
    meta_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)
    anchors = [1_774_569_600_000 + (idx * 60_000) for idx in range(12)]
    manifest_rows = []
    for idx, anchor_ts_ms in enumerate(anchors):
        cache_file = cache_root / f"anchor-{anchor_ts_ms}.npz"
        np.savez_compressed(
            cache_file,
            second_tensor=np.full((4, 4), 0.1 + idx, dtype=np.float32),
            minute_tensor=np.full((1, 4), 0.2 + idx, dtype=np.float32),
            micro_tensor=np.full((1, 7), 0.3 + idx, dtype=np.float32),
            lob_tensor=np.full((4, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((4, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((4,), dtype=np.float32),
        )
        manifest_rows.append(
            {
                "market": "KRW-BTC",
                "date": "2026-03-27",
                "anchor_ts_ms": anchor_ts_ms,
                "anchor_utc": "2026-03-27T00:00:00+00:00",
                "status": "OK",
                "reasons_json": "[]",
                "error_message": None,
                "cache_file": str(cache_file),
                "second_coverage_ratio": 1.0,
                "minute_coverage_ratio": 1.0,
                "micro_coverage_ratio": 1.0,
                "lob_coverage_ratio": 1.0,
                "built_at_ms": anchor_ts_ms + 1,
            }
        )
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")
    second_root = tmp_path / "parquet" / "candles_second_v1" / "tf=1s" / "market=KRW-BTC"
    second_root.mkdir(parents=True, exist_ok=True)
    second_rows = []
    for minute_idx, anchor_ts_ms in enumerate(anchors):
        base_price = 100.0 + minute_idx
        for sec in range(0, 65):
            ts_ms = anchor_ts_ms + (sec * 1_000)
            second_rows.append({"ts_ms": ts_ms, "open": base_price, "high": base_price + 0.2, "low": base_price - 0.2, "close": base_price + (sec * 0.01), "volume_base": 1.0, "volume_quote": 100.0, "volume_quote_est": False})
    pl.DataFrame(second_rows).write_parquet(second_root / "part-000.parquet")
    minute_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    minute_root.mkdir(parents=True, exist_ok=True)
    minute_rows = []
    for idx in range(20):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 200.0 + idx
        minute_rows.append({"ts_ms": ts_ms, "open": close - 0.5, "high": close + 0.5, "low": close - 0.7, "close": close, "volume_base": 2.0, "volume_quote": 200.0, "volume_quote_est": False})
    pl.DataFrame(minute_rows).write_parquet(minute_root / "part-000.parquet")
    options = TrainV5LobOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_lob",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        backbone_family="deeplob",
        batch_size=4,
        epochs=1,
        hidden_dim=16,
        temporal_hidden_dim=16,
    )
    result = train_and_register_v5_lob(options)
    report_before = load_json(result.train_report_path)
    resumed = resume_v5_lob_tail(run_dir=result.run_dir)
    report_after = load_json(resumed.train_report_path)
    assert resumed.run_id == result.run_id
    assert report_after["resumed"] is True
    assert report_after["data_platform_ready_snapshot_id"] == report_before["data_platform_ready_snapshot_id"]
    assert (result.run_dir / "expert_prediction_table.parquet").exists()


def test_train_v5_lob_load_minute_close_map_sources_reads_date_partitions(tmp_path: Path) -> None:
    root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC" / "date=2026-03-27"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_774_569_548_855, 1_774_569_607_341],
            "close": [200.0, 201.0],
        }
    ).write_parquet(root / "part-000.parquet")

    close_map = _load_minute_close_map_sources(market="KRW-BTC", roots=(tmp_path / "parquet" / "ws_candle_v1" / "tf=1m",))

    assert close_map[1_774_569_600_000] == 200.0
    assert close_map[1_774_569_660_000] == 201.0


def test_materialize_v5_lob_runtime_export_writes_window_artifacts(tmp_path: Path) -> None:
    dataset_root = tmp_path / "parquet" / "sequence_v1"
    meta_root = dataset_root / "_meta"
    cache_root = dataset_root / "cache" / "market=KRW-BTC" / "date=2026-03-27"
    meta_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)
    anchors = [1_774_569_600_000 + (idx * 60_000) for idx in range(12)]
    manifest_rows = []
    for idx, anchor_ts_ms in enumerate(anchors):
        cache_file = cache_root / f"anchor-{anchor_ts_ms}.npz"
        np.savez_compressed(
            cache_file,
            second_tensor=np.full((4, 4), 0.1 + idx, dtype=np.float32),
            minute_tensor=np.full((1, 4), 0.2 + idx, dtype=np.float32),
            micro_tensor=np.full((1, 7), 0.3 + idx, dtype=np.float32),
            lob_tensor=np.full((4, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((4, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((4,), dtype=np.float32),
        )
        manifest_rows.append(
            {
                "market": "KRW-BTC",
                "date": "2026-03-27",
                "anchor_ts_ms": anchor_ts_ms,
                "anchor_utc": "2026-03-27T00:00:00+00:00",
                "status": "OK",
                "reasons_json": "[]",
                "error_message": None,
                "cache_file": str(cache_file),
                "second_coverage_ratio": 1.0,
                "minute_coverage_ratio": 1.0,
                "micro_coverage_ratio": 1.0,
                "lob_coverage_ratio": 1.0,
                "built_at_ms": anchor_ts_ms + 1,
            }
        )
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")
    second_root = tmp_path / "parquet" / "candles_second_v1" / "tf=1s" / "market=KRW-BTC"
    second_root.mkdir(parents=True, exist_ok=True)
    second_rows = []
    for minute_idx, anchor_ts_ms in enumerate(anchors):
        base_price = 100.0 + minute_idx
        for sec in range(0, 65):
            ts_ms = anchor_ts_ms + (sec * 1_000)
            second_rows.append({"ts_ms": ts_ms, "open": base_price, "high": base_price + 0.2, "low": base_price - 0.2, "close": base_price + (sec * 0.01), "volume_base": 1.0, "volume_quote": 100.0, "volume_quote_est": False})
    pl.DataFrame(second_rows).write_parquet(second_root / "part-000.parquet")
    minute_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    minute_root.mkdir(parents=True, exist_ok=True)
    minute_rows = []
    for idx in range(20):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 200.0 + idx
        minute_rows.append({"ts_ms": ts_ms, "open": close - 0.5, "high": close + 0.5, "low": close - 0.7, "close": close, "volume_base": 2.0, "volume_quote": 200.0, "volume_quote_est": False})
    pl.DataFrame(minute_rows).write_parquet(minute_root / "part-000.parquet")
    options = TrainV5LobOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_lob",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        backbone_family="deeplob",
        batch_size=4,
        epochs=1,
        hidden_dim=16,
        temporal_hidden_dim=16,
    )
    result = train_and_register_v5_lob(options)
    export = materialize_v5_lob_runtime_export(run_dir=result.run_dir, start="2026-03-27", end="2026-03-27")
    assert Path(export["export_path"]).exists()
    assert Path(export["metadata_path"]).exists()
    assert export["rows"] > 0
    assert export["reused"] is False


def test_materialize_v5_lob_runtime_export_falls_back_to_window_markets(tmp_path: Path) -> None:
    dataset_root = tmp_path / "parquet" / "sequence_v1"
    meta_root = dataset_root / "_meta"
    btc_cache_root = dataset_root / "cache" / "market=KRW-BTC" / "date=2026-03-27"
    eth_cache_root = dataset_root / "cache" / "market=KRW-ETH" / "date=2026-03-28"
    meta_root.mkdir(parents=True, exist_ok=True)
    btc_cache_root.mkdir(parents=True, exist_ok=True)
    eth_cache_root.mkdir(parents=True, exist_ok=True)
    btc_anchors = [1_774_569_600_000 + (idx * 60_000) for idx in range(12)]
    eth_anchors = [1_774_656_000_000 + (idx * 60_000) for idx in range(12)]
    manifest_rows = []
    for anchors, market, cache_root, date_value in (
        (btc_anchors, "KRW-BTC", btc_cache_root, "2026-03-27"),
        (eth_anchors, "KRW-ETH", eth_cache_root, "2026-03-28"),
    ):
        for idx, anchor_ts_ms in enumerate(anchors):
            cache_file = cache_root / f"anchor-{anchor_ts_ms}.npz"
            np.savez_compressed(
                cache_file,
                second_tensor=np.full((4, 4), 0.1 + idx, dtype=np.float32),
                minute_tensor=np.full((1, 4), 0.2 + idx, dtype=np.float32),
                micro_tensor=np.full((1, 7), 0.3 + idx, dtype=np.float32),
                lob_tensor=np.full((4, 30, 5), 0.4 + idx, dtype=np.float32),
                lob_global_tensor=np.full((4, 5), 0.5 + idx, dtype=np.float32),
                second_mask=np.ones((4,), dtype=np.float32),
                minute_mask=np.ones((1,), dtype=np.float32),
                micro_mask=np.ones((1,), dtype=np.float32),
                lob_mask=np.ones((4,), dtype=np.float32),
            )
            manifest_rows.append(
                {
                    "market": market,
                    "date": date_value,
                    "anchor_ts_ms": anchor_ts_ms,
                    "anchor_utc": "2026-03-27T00:00:00+00:00",
                    "status": "OK",
                    "reasons_json": "[]",
                    "error_message": None,
                    "cache_file": str(cache_file),
                    "second_coverage_ratio": 1.0,
                    "minute_coverage_ratio": 1.0,
                    "micro_coverage_ratio": 1.0,
                    "lob_coverage_ratio": 1.0,
                    "built_at_ms": anchor_ts_ms + 1,
                }
            )
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")

    for market, anchors in (("KRW-BTC", btc_anchors), ("KRW-ETH", eth_anchors)):
        second_root = tmp_path / "parquet" / "candles_second_v1" / "tf=1s" / f"market={market}"
        second_root.mkdir(parents=True, exist_ok=True)
        second_rows = []
        for minute_idx, anchor_ts_ms in enumerate(anchors):
            base_price = 100.0 + minute_idx
            for sec in range(0, 65):
                ts_ms = anchor_ts_ms + (sec * 1_000)
                second_rows.append(
                    {
                        "ts_ms": ts_ms,
                        "open": base_price,
                        "high": base_price + 0.2,
                        "low": base_price - 0.2,
                        "close": base_price + (sec * 0.01),
                        "volume_base": 1.0,
                        "volume_quote": 100.0,
                        "volume_quote_est": False,
                    }
                )
        pl.DataFrame(second_rows).write_parquet(second_root / "part-000.parquet")

        minute_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / f"market={market}"
        minute_root.mkdir(parents=True, exist_ok=True)
        minute_rows = []
        for idx in range(40):
            ts_ms = anchors[0] + (idx * 60_000)
            close = 200.0 + idx
            minute_rows.append(
                {
                    "ts_ms": ts_ms,
                    "open": close - 0.5,
                    "high": close + 0.5,
                    "low": close - 0.7,
                    "close": close,
                    "volume_base": 2.0,
                    "volume_quote": 200.0,
                    "volume_quote_est": False,
                }
            )
        pl.DataFrame(minute_rows).write_parquet(minute_root / "part-000.parquet")

    options = TrainV5LobOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_lob",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        backbone_family="deeplob",
        batch_size=4,
        epochs=1,
        hidden_dim=16,
        temporal_hidden_dim=16,
    )
    result = train_and_register_v5_lob(options)

    export = materialize_v5_lob_runtime_export(run_dir=result.run_dir, start="2026-03-28", end="2026-03-28")

    assert Path(export["export_path"]).exists()
    assert export["rows"] > 0
    assert export["reused"] is False
    assert export["requested_selected_markets"] == ["KRW-BTC"]
    assert export["selected_markets"] == ["KRW-ETH"]
    assert export["selected_markets_source"] == "window_available_markets_fallback"
    assert export["fallback_reason"] == "TRAIN_SELECTED_MARKETS_EMPTY_IN_RUNTIME_WINDOW"


def test_train_v5_lob_outputs_feed_tradability_and_fusion_chain(tmp_path: Path) -> None:
    lob_result = _train_minimal_lob_result(tmp_path)
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    lob_table_path = lob_result.run_dir / "expert_prediction_table.parquet"
    lob_frame = pl.read_parquet(lob_table_path).sort(["ts_ms", "market"])
    lob_train_config = load_json(lob_result.run_dir / "train_config.yaml")
    snapshot_id = str(lob_train_config.get("data_platform_ready_snapshot_id") or "").strip() or "snapshot-lob-chain-001"
    lob_train_config["data_platform_ready_snapshot_id"] = snapshot_id
    (lob_result.run_dir / "train_config.yaml").write_text(
        json.dumps(lob_train_config, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    panel_rows: list[dict[str, object]] = []
    sequence_rows: list[dict[str, object]] = []
    private_rows: list[dict[str, object]] = []
    for idx, row in enumerate(lob_frame.iter_rows(named=True)):
        reg_value = (-0.02 if idx % 4 == 0 else 0.01 + (0.002 * idx))
        cls_value = 1 if reg_value > 0 else 0
        panel_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "split": row["split"],
                "y_cls": cls_value,
                "y_reg": reg_value,
                "final_rank_score": 0.35 + (reg_value * 4.0),
                "final_expected_return": reg_value * 0.9,
                "final_expected_es": abs(reg_value) * 0.3,
                "final_tradability": 0.75,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": (reg_value * 0.9) - (abs(reg_value) * 0.3) - 0.05,
            }
        )
        sequence_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "support_level": row.get("support_level", "strict_full"),
                "directional_probability_primary": 0.6 if cls_value > 0 else 0.4,
                "sequence_uncertainty_primary": 0.04,
                "return_quantile_h3_q10": reg_value * 0.5,
                "return_quantile_h3_q50": reg_value * 0.8,
                "return_quantile_h3_q90": reg_value * 1.1,
                "regime_embedding_0": float(idx % 3),
                "regime_embedding_1": float((idx + 1) % 3),
            }
        )
        private_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "decision_bucket_ts_ms": row["ts_ms"],
                "decision_bar_interval_ms": 60_000,
                "y_tradeable": 1 if idx % 2 == 0 else 0,
                "y_fill_within_deadline": 1 if idx % 3 != 0 else 0,
                "y_shortfall_bps": 0.5 + (idx * 0.2),
                "y_adverse_tolerance": 1 if idx % 4 != 0 else 0,
            }
        )

    panel_run_dir, panel_path = _write_downstream_expert_run(
        registry_root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-from-lob-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=panel_rows,
    )
    sequence_run_dir, sequence_path = _write_downstream_expert_run(
        registry_root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-from-lob-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=sequence_rows,
    )

    private_root = tmp_path / "data" / "parquet" / "private_execution_v1"
    private_meta = private_root / "_meta"
    private_meta.mkdir(parents=True, exist_ok=True)
    (private_meta / "build_report.json").write_text(json.dumps({"status": "PASS"}, ensure_ascii=False), encoding="utf-8")
    (private_meta / "validate_report.json").write_text(json.dumps({"status": "PASS", "pass": True}, ensure_ascii=False), encoding="utf-8")
    private_part = private_root / "market=KRW-BTC" / "date=2026-03-27"
    private_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(private_rows).write_parquet(private_part / "part-000.parquet")

    tradability_result = train_and_register_v5_tradability(
        TrainV5TradabilityOptions(
            panel_input_path=panel_path,
            sequence_input_path=sequence_path,
            lob_input_path=lob_table_path,
            private_execution_root=private_root,
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_tradability",
            quote="KRW",
            start="2026-03-27",
            end="2026-03-27",
            seed=19,
        )
    )

    lob_runtime = materialize_v5_lob_runtime_export(
        run_dir=lob_result.run_dir,
        start="2026-03-27",
        end="2026-03-27",
    )
    lob_runtime_frame = pl.read_parquet(Path(str(lob_runtime["export_path"]))).sort(["ts_ms", "market"])
    panel_runtime_rows: list[dict[str, object]] = []
    sequence_runtime_rows: list[dict[str, object]] = []
    for idx, row in enumerate(lob_runtime_frame.iter_rows(named=True)):
        reg_value = 0.015 + (0.002 * idx)
        panel_runtime_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "split": "runtime",
                "y_cls": 1,
                "y_reg": reg_value,
                "final_rank_score": 0.45 + (reg_value * 3.0),
                "final_expected_return": reg_value * 0.9,
                "final_expected_es": abs(reg_value) * 0.25,
                "final_tradability": 0.78,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": (reg_value * 0.9) - (abs(reg_value) * 0.25) - 0.05,
            }
        )
        sequence_runtime_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "support_level": row.get("support_level", "strict_full"),
                "directional_probability_primary": 0.62,
                "sequence_uncertainty_primary": 0.04,
                "return_quantile_h3_q10": reg_value * 0.5,
                "return_quantile_h3_q50": reg_value * 0.8,
                "return_quantile_h3_q90": reg_value * 1.1,
                "regime_embedding_0": float(idx % 2),
                "regime_embedding_1": float((idx + 1) % 2),
            }
        )
    panel_runtime_path = _write_downstream_runtime_export(
        run_dir=panel_run_dir,
        start="2026-03-27",
        end="2026-03-27",
        rows=panel_runtime_rows,
    )
    sequence_runtime_path = _write_downstream_runtime_export(
        run_dir=sequence_run_dir,
        start="2026-03-27",
        end="2026-03-27",
        rows=sequence_runtime_rows,
        anchor_export_path=str(panel_runtime_path.resolve()),
    )
    tradability_runtime = materialize_v5_tradability_runtime_export(
        run_dir=tradability_result.run_dir,
        start="2026-03-27",
        end="2026-03-27",
        panel_runtime_input_path=panel_runtime_path,
        sequence_runtime_input_path=sequence_runtime_path,
        lob_runtime_input_path=Path(str(lob_runtime["export_path"])),
    )

    fusion_result = train_and_register_v5_fusion(
        TrainV5FusionOptions(
            panel_input_path=panel_path,
            sequence_input_path=sequence_path,
            lob_input_path=lob_table_path,
            tradability_input_path=tradability_result.run_dir / "expert_prediction_table.parquet",
            panel_runtime_input_path=panel_runtime_path,
            sequence_runtime_input_path=sequence_runtime_path,
            lob_runtime_input_path=Path(str(lob_runtime["export_path"])),
            tradability_runtime_input_path=Path(str(tradability_runtime["export_path"])),
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_fusion",
            quote="KRW",
            start="2026-03-27",
            end="2026-03-27",
            runtime_start="2026-03-27",
            runtime_end="2026-03-27",
            seed=23,
            stacker_family="linear",
        )
    )

    tradability_contract = load_json(tradability_result.tradability_model_contract_path)
    assert tradability_contract["input_experts"]["lob"]["run_id"] == lob_result.run_id
    assert tradability_contract["input_experts"]["lob"]["trainer"] == "v5_lob"

    fusion_input_contract = load_json(fusion_result.run_dir / "fusion_input_contract.json")
    fusion_runtime_contract = load_json(fusion_result.run_dir / "fusion_runtime_input_contract.json")
    assert fusion_input_contract["inputs"]["lob"]["run_id"] == lob_result.run_id
    assert fusion_input_contract["inputs"]["lob"]["trainer"] == "v5_lob"
    assert fusion_input_contract["inputs"]["tradability"]["run_id"] == tradability_result.run_id
    assert fusion_runtime_contract["inputs"]["lob"]["run_id"] == lob_result.run_id
    assert fusion_runtime_contract["inputs"]["tradability"]["run_id"] == tradability_result.run_id
    assert fusion_runtime_contract["runtime_coverage_summary"]["experts"]["lob"]["missing_rows"] == 0
    assert fusion_runtime_contract["runtime_coverage_summary"]["experts"]["tradability"]["missing_rows"] == 0
    assert Path(tradability_runtime["export_path"]).exists()
    assert fusion_result.run_dir.exists()
