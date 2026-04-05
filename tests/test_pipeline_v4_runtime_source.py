from __future__ import annotations

import polars as pl

from autobot.features.pipeline_v4 import _derive_high_tf_close_candles_from_base


def test_derive_high_tf_close_candles_from_base_uses_bucket_end_close() -> None:
    base = pl.DataFrame(
        {
            "ts_ms": [
                300_000,
                600_000,
                3_300_000,
                3_600_000,
                3_900_000,
                7_200_000,
            ],
            "close": [
                101.0,
                102.0,
                109.0,
                110.0,
                111.0,
                120.0,
            ],
        }
    )

    derived = _derive_high_tf_close_candles_from_base(base, high_tf="60m")

    assert derived.to_dicts() == [
        {"ts_ms": 3_600_000, "close": 110.0},
        {"ts_ms": 7_200_000, "close": 120.0},
    ]
