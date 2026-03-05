from __future__ import annotations

import polars as pl

from autobot.features.feature_set_v3 import _attach_sample_weight, feature_columns_v3


def test_feature_columns_v3_includes_one_m_coverage_fields() -> None:
    cols = set(feature_columns_v3())
    assert "one_m_synth_ratio" in cols
    assert "one_m_real_count" in cols
    assert "one_m_real_volume_sum" in cols


def test_attach_sample_weight_downweights_high_synth_ratio() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_000_000],
            "one_m_synth_ratio": [0.0, 1.0],
        }
    )
    weighted = _attach_sample_weight(
        frame,
        half_life_days=1_000_000.0,
        synth_weight_floor=0.2,
        synth_weight_power=1.0,
    )
    values = weighted.get_column("sample_weight").to_list()
    assert abs(values[0] - 1.0) < 1e-9
    assert abs(values[1] - 0.2) < 1e-9
