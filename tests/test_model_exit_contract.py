from __future__ import annotations

import json

from autobot.common.model_exit_contract import normalize_model_exit_plan_payload
from autobot.live.model_risk_plan import build_model_exit_plan_from_position, extract_model_exit_plan
from autobot.strategy.model_alpha_v1 import ModelAlphaExitSettings, ModelAlphaSettings, build_model_alpha_exit_plan_payload


def test_normalize_model_exit_plan_payload_adds_canonical_ratio_aliases() -> None:
    payload = normalize_model_exit_plan_payload(
        {
            "source": "model_alpha_v1",
            "version": 1,
            "mode": "risk",
            "hold_bars": 6,
            "interval_ms": 300_000,
            "timeout_delta_ms": 1_800_000,
            "tp_pct": 0.02,
            "sl_pct": 0.01,
            "trailing_pct": 0.005,
            "expected_exit_fee_rate": 0.0005,
            "expected_exit_slippage_bps": 3.0,
        }
    )

    assert payload["bar_interval_ms"] == 300_000
    assert payload["tp_ratio"] == 0.02
    assert payload["sl_ratio"] == 0.01
    assert payload["trailing_ratio"] == 0.005
    assert payload["expected_exit_fee_ratio"] == 0.0005
    assert payload["tp_pct"] == 0.02
    assert payload["expected_exit_fee_rate"] == 0.0005


def test_build_model_alpha_exit_plan_payload_emits_canonical_aliases() -> None:
    payload = build_model_alpha_exit_plan_payload(
        settings=ModelAlphaSettings(
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=6,
                risk_scaling_mode="volatility_scaled",
                risk_vol_feature="rv_36",
                tp_vol_multiplier=2.5,
                sl_vol_multiplier=1.5,
                trailing_vol_multiplier=0.75,
                tp_pct=0.02,
                sl_pct=0.01,
                trailing_pct=0.005,
            )
        ),
        row=None,
        interval_ms=300_000,
    )

    assert payload["bar_interval_ms"] == 300_000
    assert payload["timeout_delta_ms"] == 1_800_000
    assert payload["tp_ratio"] == 0.02
    assert payload["sl_ratio"] == 0.01
    assert payload["trailing_ratio"] == 0.005
    assert payload["risk_scaling_mode"] == "volatility_scaled"
    assert payload["risk_vol_feature"] == "rv_36"
    assert payload["tp_vol_multiplier"] == 2.5
    assert payload["sl_vol_multiplier"] == 1.5
    assert payload["trailing_vol_multiplier"] == 0.75


def test_build_model_alpha_exit_plan_payload_keeps_tp_above_cost_floor() -> None:
    payload = build_model_alpha_exit_plan_payload(
        settings=ModelAlphaSettings(
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=6,
                tp_pct=0.0001,
                sl_pct=0.01,
                trailing_pct=0.0,
                expected_exit_fee_bps=5.0,
                expected_exit_slippage_bps=2.5,
            )
        ),
        row=None,
        interval_ms=300_000,
        observed_entry_fee_rate=0.0005,
    )

    assert payload["min_tp_floor_pct"] > 0.0001
    assert payload["tp_pct"] == 0.0001


def test_extract_model_exit_plan_normalizes_nested_intent_meta() -> None:
    payload = extract_model_exit_plan(
        {
            "strategy": {
                "meta": {
                    "model_exit_plan": {
                        "source": "model_alpha_v1",
                        "mode": "risk",
                        "hold_bars": 3,
                        "interval_ms": 300_000,
                        "tp_pct": 0.01,
                        "sl_pct": 0.02,
                        "trailing_pct": 0.0,
                    }
                }
            }
        }
    )

    assert payload is not None
    assert payload["bar_interval_ms"] == 300_000
    assert payload["tp_ratio"] == 0.01
    assert payload["sl_ratio"] == 0.02


def test_build_model_exit_plan_from_position_backfills_ratio_aliases() -> None:
    payload = build_model_exit_plan_from_position(
        {
            "tp": json.loads(
                json.dumps(
                    {
                        "enabled": True,
                        "source": "model_alpha_v1",
                        "mode": "risk",
                        "hold_bars": 6,
                        "timeout_delta_ms": 1_800_000,
                        "risk_scaling_mode": "volatility_scaled",
                        "risk_vol_feature": "rv_12",
                        "tp_vol_multiplier": 2.0,
                        "sl_vol_multiplier": 1.0,
                        "trailing_vol_multiplier": 0.5,
                        "tp_pct": 2.0,
                    }
                )
            ),
            "sl": {
                "enabled": True,
                "source": "model_alpha_v1",
                "mode": "risk",
                "hold_bars": 6,
                "timeout_delta_ms": 1_800_000,
                "sl_pct": 1.0,
            },
            "trailing": {
                "enabled": True,
                "source": "model_alpha_v1",
                "mode": "risk",
                "hold_bars": 6,
                "timeout_delta_ms": 1_800_000,
                "trail_pct": 0.015,
                "high_watermark_price_str": "110.5",
                "armed_ts_ms": 1234,
            },
        }
    )

    assert payload is not None
    assert payload["bar_interval_ms"] == 300_000
    assert payload["tp_ratio"] == 0.02
    assert payload["sl_ratio"] == 0.01
    assert payload["trailing_ratio"] == 0.015
    assert payload["risk_scaling_mode"] == "volatility_scaled"
    assert payload["risk_vol_feature"] == "rv_12"
    assert payload["tp_vol_multiplier"] == 2.0
    assert payload["sl_vol_multiplier"] == 1.0
    assert payload["trailing_vol_multiplier"] == 0.5
    assert payload["high_watermark_price"] == 110.5
    assert payload["armed_ts_ms"] == 1234
