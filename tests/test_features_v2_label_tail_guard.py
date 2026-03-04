from __future__ import annotations

import polars as pl

from autobot.features.feature_set_v2 import apply_label_tail_guard


def test_label_tail_guard_drops_last_h_rows() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 1, 2, 3, 4],
            "y_reg": [0.1, 0.2, 0.3, 0.4, 0.5],
            "y_cls": [1, 1, 0, 0, 1],
        }
    )
    guarded, dropped = apply_label_tail_guard(frame, horizon_bars=2)
    assert dropped == 2
    assert guarded.height == 3
    assert guarded.get_column("ts_ms").to_list() == [0, 1, 2]
