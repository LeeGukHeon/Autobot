from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from autobot.features.feature_spec import (
    FeatureSetV1Config,
    FeatureWindows,
    LabelV1Config,
    TimeRangeConfig,
    UniverseConfig,
)
from autobot.features.pipeline_v3 import (
    FeatureBuildV3Options,
    FeaturesV3BuildConfig,
    FeaturesV3Config,
    FeaturesV3ValidateConfig,
    _select_v3_universe_markets,
    build_features_dataset_v3,
)


def test_build_features_v3_dry_run_applies_quality_adjusted_universe_selection(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    base_root = parquet_root / "candles_api_v1"
    micro_root = parquet_root / "micro_v1"

    lookback_start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    build_day_ts = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    _write_tf(
        base_root=base_root,
        tf="5m",
        market="KRW-GOOD",
        start_ts=lookback_start_ts,
        count=288,
        interval_ms=300_000,
        volume_quote=100.0,
    )
    _write_tf(
        base_root=base_root,
        tf="5m",
        market="KRW-BAD",
        start_ts=lookback_start_ts,
        count=288,
        interval_ms=300_000,
        volume_quote=500.0,
    )
    _write_tf(
        base_root=base_root,
        tf="1m",
        market="KRW-GOOD",
        start_ts=lookback_start_ts,
        count=1_440,
        interval_ms=60_000,
        volume_quote=10.0,
    )
    _write_tf(
        base_root=base_root,
        tf="1m",
        market="KRW-BAD",
        start_ts=lookback_start_ts,
        count=120,
        interval_ms=60_000,
        volume_quote=10.0,
    )
    _write_micro_market(micro_root=micro_root, market="KRW-GOOD", start_ts=build_day_ts)
    _write_micro_market(micro_root=micro_root, market="KRW-BAD", start_ts=build_day_ts)
    _write_micro_validate_ok(micro_root)

    config = _make_config(
        parquet_root=parquet_root,
        features_root=features_root,
        top_n=1,
        universe_mode="static_start",
        fixed_list=(),
    )
    summary = build_features_dataset_v3(
        config,
        FeatureBuildV3Options(
            tf="5m",
            quote="KRW",
            top_n=1,
            start="2026-01-02",
            end="2026-01-02",
            feature_set="v3",
            label_set="v1",
            dry_run=True,
        ),
    )

    assert summary.selected_markets == ("KRW-GOOD",)
    report_path = features_root / "features_v3_test" / "_meta" / "universe_quality_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["selected_markets"] == ["KRW-GOOD"]
    score_by_market = {str(item["market"]): float(item["score"]) for item in payload.get("candidates", [])}
    assert score_by_market["KRW-GOOD"] > score_by_market["KRW-BAD"]


def test_select_v3_universe_fixed_list_fallback_records_warning(tmp_path: Path) -> None:
    config = _make_config(
        parquet_root=tmp_path / "parquet",
        features_root=tmp_path / "features",
        top_n=1,
        universe_mode="fixed_list",
        fixed_list=("KRW-NOTFOUND",),
    )
    selected, report = _select_v3_universe_markets(
        config=config,
        tf="5m",
        top_n=1,
        quote="KRW",
        start_ts_ms=int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000),
        base_candles_root=tmp_path / "parquet" / "candles_api_v1",
        discovered_windows=[{"market": "KRW-BTC", "rows": 10}],
    )
    assert selected == ["KRW-BTC"]
    assert "FIXED_LIST_INSUFFICIENT_FILLED_FROM_DISCOVERED" in report.get("warnings", [])
    assert int(report.get("fallback", {}).get("filled_count", 0)) == 1


def _make_config(
    *,
    parquet_root: Path,
    features_root: Path,
    top_n: int,
    universe_mode: str,
    fixed_list: tuple[str, ...],
) -> FeaturesV3Config:
    return FeaturesV3Config(
        build=FeaturesV3BuildConfig(
            output_dataset="features_v3_test",
            tf="5m",
            base_candles_dataset="candles_api_v1",
            micro_dataset="micro_v1",
            high_tfs=("15m", "60m", "240m"),
            one_m_synth_weight_floor=0.2,
            one_m_synth_weight_power=2.0,
            universe_quality_enabled=True,
            universe_quality_lookback_days=1,
            universe_quality_beta=2.0,
            universe_quality_q_floor=0.2,
            universe_quality_oversample_factor=3,
            min_rows_for_train=1,
            require_micro_validate_pass=True,
        ),
        parquet_root=parquet_root,
        features_root=features_root,
        universe=UniverseConfig(
            quote="KRW",
            mode=universe_mode,
            top_n=top_n,
            lookback_days=1,
            fixed_list=fixed_list,
        ),
        time_range=TimeRangeConfig(start="2026-01-02", end="2026-01-02"),
        feature_set_v1=FeatureSetV1Config(
            windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
            enable_factor_features=False,
            factor_markets=(),
            enable_liquidity_rank=False,
        ),
        label_v1=LabelV1Config(horizon_bars=2, thr_bps=15.0, neutral_policy="drop", fee_bps_est=10.0, safety_bps=5.0),
        validation=FeaturesV3ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )


def _write_tf(
    *,
    base_root: Path,
    tf: str,
    market: str,
    start_ts: int,
    count: int,
    interval_ms: int,
    volume_quote: float,
) -> None:
    part_dir = base_root / f"tf={tf}" / f"market={market}"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = [start_ts + i * interval_ms for i in range(count)]
    close = [100.0 + (i * 0.01) for i in range(count)]
    volume_base = [max(float(volume_quote) / max(c, 1e-9), 0.0) for c in close]
    pl.DataFrame(
        {
            "ts_ms": ts,
            "open": [value - 0.02 for value in close],
            "high": [value + 0.05 for value in close],
            "low": [value - 0.05 for value in close],
            "close": close,
            "volume_base": volume_base,
            "volume_quote": [float(volume_quote) for _ in range(count)],
            "volume_quote_est": [False for _ in range(count)],
        }
    ).write_parquet(part_dir / "part.parquet")


def _write_micro_market(*, micro_root: Path, market: str, start_ts: int) -> None:
    part_dir = micro_root / "tf=5m" / f"market={market}" / "date=2026-01-02"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = [start_ts + i * 300_000 for i in range(12)]
    pl.DataFrame({"ts_ms": ts}).write_parquet(part_dir / "part-000.parquet")


def _write_micro_validate_ok(micro_root: Path) -> None:
    meta_dir = micro_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "validate_report.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "checked_files": 1,
                "fail_files": 0,
                "warn_files": 0,
                "ok_files": 1,
                "details": [
                    {
                        "file": str(micro_root / "tf=5m" / "market=KRW-GOOD" / "date=2026-01-02" / "part-000.parquet"),
                        "tf": "5m",
                        "market": "KRW-GOOD",
                        "date": "2026-01-02",
                        "rows": 12,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
