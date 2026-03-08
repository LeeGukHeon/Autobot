from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import polars as pl

from autobot.execution.order_supervisor import OrderExecProfile

_MODULE_PATH = Path(__file__).resolve().parents[1] / "autobot" / "strategy" / "operational_overlay_v1.py"
_SPEC = importlib.util.spec_from_file_location("autobot.strategy.operational_overlay_v1_test", _MODULE_PATH)
assert _SPEC is not None
assert _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

ModelAlphaOperationalSettings = _MODULE.ModelAlphaOperationalSettings
build_regime_snapshot_from_scored_frame = _MODULE.build_regime_snapshot_from_scored_frame
compute_micro_quality_composite = _MODULE.compute_micro_quality_composite
resolve_operational_execution_overlay = _MODULE.resolve_operational_execution_overlay
load_calibrated_operational_settings = _MODULE.load_calibrated_operational_settings
resolve_operational_max_positions = _MODULE.resolve_operational_max_positions
resolve_operational_risk_multiplier = _MODULE.resolve_operational_risk_multiplier


def _ts_ms(hour_utc: int) -> int:
    return int(datetime(2026, 3, 8, hour_utc, 0, tzinfo=timezone.utc).timestamp() * 1000)


def test_resolve_operational_risk_multiplier_is_monotonic() -> None:
    settings = ModelAlphaOperationalSettings(risk_multiplier_min=0.8, risk_multiplier_max=1.2)

    low = resolve_operational_risk_multiplier(settings=settings, regime_score=0.1, micro_quality_score=0.1)
    high = resolve_operational_risk_multiplier(settings=settings, regime_score=0.9, micro_quality_score=0.9)

    assert 0.8 <= low < high <= 1.2


def test_resolve_operational_risk_multiplier_uses_empirical_model_when_enabled() -> None:
    settings = ModelAlphaOperationalSettings(
        risk_multiplier_min=0.8,
        risk_multiplier_max=1.2,
        empirical_state_score_model_enabled=True,
        empirical_state_score_intercept=-1.0,
        empirical_state_score_regime_coef=2.0,
        empirical_state_score_breadth_coef=1.5,
        empirical_state_score_micro_coef=2.0,
        empirical_state_score_output_scale=1.0,
    )

    low = resolve_operational_risk_multiplier(
        settings=settings,
        regime_score=0.2,
        breadth_ratio=0.2,
        micro_quality_score=0.2,
    )
    high = resolve_operational_risk_multiplier(
        settings=settings,
        regime_score=0.9,
        breadth_ratio=0.9,
        micro_quality_score=0.9,
    )

    assert 0.8 <= low < high <= 1.2


def test_resolve_operational_max_positions_scales_with_regime() -> None:
    settings = ModelAlphaOperationalSettings(max_positions_scale_min=0.5, max_positions_scale_max=1.5)

    assert resolve_operational_max_positions(
        base_max_positions=4,
        settings=settings,
        regime_score=0.9,
        breadth_ratio=0.05,
    ) == 1
    assert resolve_operational_max_positions(
        base_max_positions=4,
        settings=settings,
        regime_score=0.9,
        breadth_ratio=0.8,
    ) >= 4


def test_resolve_operational_max_positions_uses_empirical_model_when_enabled() -> None:
    settings = ModelAlphaOperationalSettings(
        max_positions_scale_min=0.5,
        max_positions_scale_max=1.5,
        empirical_state_score_model_enabled=True,
        empirical_state_score_intercept=-1.0,
        empirical_state_score_regime_coef=2.0,
        empirical_state_score_breadth_coef=1.5,
        empirical_state_score_micro_coef=1.0,
        empirical_state_score_output_scale=1.0,
    )

    low = resolve_operational_max_positions(
        base_max_positions=4,
        settings=settings,
        regime_score=0.3,
        breadth_ratio=0.3,
        micro_quality_score=0.3,
    )
    high = resolve_operational_max_positions(
        base_max_positions=4,
        settings=settings,
        regime_score=0.9,
        breadth_ratio=0.9,
        micro_quality_score=0.9,
    )

    assert 1 <= low <= high


def test_build_regime_snapshot_from_scored_frame_reads_market_state_columns() -> None:
    frame = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "model_prob": [0.9, 0.8, 0.7],
            "market_dispersion_12": [0.04, 0.05, 0.03],
            "m_trade_coverage_ms": [60_000, 55_000, 58_000],
            "m_book_coverage_ms": [60_000, 60_000, 60_000],
            "m_spread_proxy": [4.0, 5.0, 6.0],
            "m_depth_top5_notional_krw": [4_000_000.0, 5_000_000.0, 4_500_000.0],
        }
    )

    snapshot = build_regime_snapshot_from_scored_frame(
        scored=frame,
        eligible_rows=2,
        scored_rows=3,
        ts_ms=_ts_ms(13),
    )

    assert 0.0 < snapshot.regime_score <= 1.0
    assert snapshot.session_bucket == "asia_us_overlap"
    assert snapshot.breadth_ratio == 2.0 / 3.0


def test_compute_micro_quality_composite_and_overlay_modes() -> None:
    settings = ModelAlphaOperationalSettings()
    base = OrderExecProfile(
        timeout_ms=900_000,
        replace_interval_ms=900_000,
        max_replaces=4,
        price_mode="JOIN",
        max_chase_bps=15,
        min_replace_interval_ms_global=1_500,
        post_only=False,
    )
    low_snapshot = SimpleNamespace(
        market="KRW-BTC",
        snapshot_ts_ms=_ts_ms(2),
        last_event_ts_ms=_ts_ms(2) - 60_000,
        trade_events=1,
        trade_coverage_ms=1_000,
        trade_notional_krw=100.0,
        trade_imbalance=0.0,
        trade_source="ws",
        spread_bps_mean=80.0,
        depth_top5_notional_krw=10_000.0,
        book_events=1,
        book_coverage_ms=1_000,
        book_available=True,
    )
    high_snapshot = SimpleNamespace(
        market="KRW-BTC",
        snapshot_ts_ms=_ts_ms(13),
        last_event_ts_ms=_ts_ms(13),
        trade_events=10,
        trade_coverage_ms=60_000,
        trade_notional_krw=2_000_000.0,
        trade_imbalance=0.0,
        trade_source="ws",
        spread_bps_mean=3.0,
        depth_top5_notional_krw=5_000_000.0,
        book_events=10,
        book_coverage_ms=60_000,
        book_available=True,
    )

    low_quality = compute_micro_quality_composite(
        micro_snapshot=low_snapshot,
        now_ts_ms=_ts_ms(2),
        settings=settings,
    )
    high_quality = compute_micro_quality_composite(
        micro_snapshot=high_snapshot,
        now_ts_ms=_ts_ms(13),
        settings=settings,
    )

    assert low_quality is not None
    assert high_quality is not None
    assert low_quality.score < high_quality.score

    low_decision = resolve_operational_execution_overlay(
        base_profile=base,
        settings=settings,
        micro_quality=low_quality,
        ts_ms=_ts_ms(2),
    )
    assert low_decision.abort_reason == "MICRO_QUALITY_TOO_LOW"

    conservative_snapshot = SimpleNamespace(
        market="KRW-BTC",
        snapshot_ts_ms=_ts_ms(2),
        last_event_ts_ms=_ts_ms(2),
        trade_events=5,
        trade_coverage_ms=50_000,
        trade_notional_krw=300_000.0,
        trade_imbalance=0.0,
        trade_source="ws",
        spread_bps_mean=10.0,
        depth_top5_notional_krw=800_000.0,
        book_events=5,
        book_coverage_ms=50_000,
        book_available=True,
    )
    conservative_quality = compute_micro_quality_composite(
        micro_snapshot=conservative_snapshot,
        now_ts_ms=_ts_ms(2),
        settings=settings,
    )
    conservative_decision = resolve_operational_execution_overlay(
        base_profile=base,
        settings=settings,
        micro_quality=conservative_quality,
        ts_ms=_ts_ms(2),
    )
    assert conservative_decision.abort_reason is None
    assert conservative_decision.exec_profile.price_mode == "PASSIVE_MAKER"
    assert conservative_decision.exec_profile.timeout_ms == 1_125_000
    assert conservative_decision.exec_profile.replace_interval_ms == 1_350_000
    assert conservative_decision.exec_profile.max_replaces == 2
    assert conservative_decision.exec_profile.max_chase_bps == 11

    aggressive_decision = resolve_operational_execution_overlay(
        base_profile=base,
        settings=settings,
        micro_quality=high_quality,
        ts_ms=_ts_ms(13),
    )
    assert aggressive_decision.abort_reason is None
    assert aggressive_decision.exec_profile.price_mode == "CROSS_1T"
    assert aggressive_decision.exec_profile.timeout_ms == 675_000
    assert aggressive_decision.exec_profile.replace_interval_ms == 450_000
    assert aggressive_decision.exec_profile.max_replaces == 5
    assert aggressive_decision.exec_profile.max_chase_bps == 20


def test_load_calibrated_operational_settings_reads_artifact(tmp_path: Path) -> None:
    artifact_path = tmp_path / "latest.json"
    artifact_path.write_text(
        """{
  "calibrated_settings": {
    "risk_multiplier_min": 0.72,
    "risk_multiplier_max": 1.28,
    "max_positions_scale_max": 1.65,
    "empirical_state_score_model_enabled": true,
    "empirical_state_score_intercept": -0.5,
    "empirical_state_score_regime_coef": 1.2,
    "empirical_state_score_breadth_coef": 0.8,
    "empirical_state_score_micro_coef": 1.1,
    "empirical_state_score_output_scale": 0.9
  }
}""",
        encoding="utf-8",
    )
    base = ModelAlphaOperationalSettings(
        calibration_artifact_path=str(artifact_path),
        risk_multiplier_min=0.8,
        risk_multiplier_max=1.2,
        max_positions_scale_max=1.5,
    )

    calibrated = load_calibrated_operational_settings(base_settings=base)

    assert calibrated.risk_multiplier_min == 0.72
    assert calibrated.risk_multiplier_max == 1.28
    assert calibrated.max_positions_scale_max == 1.65
    assert calibrated.empirical_state_score_model_enabled is True
    assert calibrated.empirical_state_score_regime_coef == 1.2
