from __future__ import annotations

import numpy as np

from autobot.models.multiple_testing import (
    TrialWindowMatrix,
    build_trial_window_differential_matrix,
    run_hansen_spa,
    run_white_reality_check,
)


def test_build_trial_window_differential_matrix_aligns_common_windows() -> None:
    candidate_trial_panel = [
        {
            "trial": 0,
            "windows": [
                {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.004}}}},
                {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.003}}}},
                {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.002}}}},
            ],
        },
        {
            "trial": 1,
            "windows": [
                {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.005}}}},
                {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0035}}}},
                {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0025}}}},
            ],
        },
    ]
    champion_windows = [
        {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.001}}}},
        {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0005}}}},
        {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0001}}}},
    ]

    matrix = build_trial_window_differential_matrix(candidate_trial_panel, champion_windows)

    assert matrix is not None
    assert matrix.trial_ids == [0, 1]
    assert matrix.window_indices == [0, 1, 2]
    assert matrix.panel_keys == ["0:0", "1:0", "2:0"]
    assert matrix.differential_matrix.shape == (2, 3)
    assert matrix.differential_matrix[0, 0] == 0.003


def test_build_trial_window_differential_matrix_prefers_oos_slices_when_available() -> None:
    candidate_trial_panel = [
        {
            "trial": 0,
            "windows": [
                {
                    "window_index": 0,
                    "oos_slices": [
                        {"slice_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.004}}}},
                        {"slice_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.006}}}},
                    ],
                }
            ],
        },
        {
            "trial": 1,
            "windows": [
                {
                    "window_index": 0,
                    "oos_slices": [
                        {"slice_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.005}}}},
                        {"slice_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.007}}}},
                    ],
                }
            ],
        },
    ]
    champion_windows = [
        {
            "window_index": 0,
            "oos_slices": [
                {"slice_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.001}}}},
                {"slice_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.002}}}},
            ],
        }
    ]

    matrix = build_trial_window_differential_matrix(candidate_trial_panel, champion_windows)

    assert matrix is not None
    assert matrix.panel_keys == ["0:0", "0:1"]
    assert matrix.window_indices == [0, 0]
    assert matrix.differential_matrix.shape == (2, 2)
    assert matrix.differential_matrix[0, 1] == 0.004


def test_run_white_reality_check_detects_candidate_edge() -> None:
    matrix = build_trial_window_differential_matrix(
        candidate_trial_panel=[
            {
                "trial": 0,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.005}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.004}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.006}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0055}}}},
                ],
            },
            {
                "trial": 1,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0045}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0042}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0058}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0051}}}},
                ],
            },
            {
                "trial": 2,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0048}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0041}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0056}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0050}}}},
                ],
            },
        ],
        champion_windows=[
            {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
            {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0009}}}},
            {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0011}}}},
            {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
        ],
    )

    result = run_white_reality_check(matrix, bootstrap_iters=200, alpha=0.20, seed=7)

    assert result["comparable"] is True
    assert result["best_mean_diff_ev_net"] > 0.0
    assert 0.0 <= result["p_value"] <= 1.0
    assert result["bootstrap_method"] == "stationary"
    assert result["average_block_length"] >= 2
    assert result["block_length_source"].startswith("auto_")


def test_run_hansen_spa_detects_candidate_edge() -> None:
    matrix = build_trial_window_differential_matrix(
        candidate_trial_panel=[
            {
                "trial": 0,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0065}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0060}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0070}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0068}}}},
                    {"window_index": 4, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0067}}}},
                    {"window_index": 5, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0069}}}},
                ],
            },
            {
                "trial": 1,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0062}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0061}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0071}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0066}}}},
                    {"window_index": 4, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0068}}}},
                    {"window_index": 5, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0065}}}},
                ],
            },
            {
                "trial": 2,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0064}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0063}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0072}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0069}}}},
                    {"window_index": 4, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0066}}}},
                    {"window_index": 5, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0067}}}},
                ],
            },
        ],
        champion_windows=[
            {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0012}}}},
            {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
            {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0013}}}},
            {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0011}}}},
            {"window_index": 4, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
            {"window_index": 5, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0012}}}},
        ],
    )

    result = run_hansen_spa(matrix, bootstrap_iters=200, alpha=0.20, seed=7)

    assert result["comparable"] is True
    assert result["best_mean_diff_ev_net"] > 0.0
    assert 0.0 <= result["p_value"] <= 1.0
    assert result["bootstrap_method"] == "stationary"
    assert result["average_block_length"] >= 2
    assert result["block_length_source"].startswith("auto_")


def test_multiple_testing_respects_manual_block_length_override() -> None:
    matrix = TrialWindowMatrix(
        trial_ids=[0, 1],
        window_indices=[0, 1, 2, 3, 4, 5],
        panel_keys=["0:0", "1:0", "2:0", "3:0", "4:0", "5:0"],
        differential_matrix=np.asarray(
            [
                [0.0040, 0.0044, 0.0042, 0.0046, 0.0045, 0.0047],
                [0.0038, 0.0040, 0.0041, 0.0043, 0.0042, 0.0044],
            ],
            dtype=np.float64,
        ),
    )

    white = run_white_reality_check(matrix, bootstrap_iters=100, average_block_length=5, seed=11)
    hansen = run_hansen_spa(matrix, bootstrap_iters=100, average_block_length=4, seed=11)

    assert white["average_block_length"] == 5
    assert white["block_length_source"] == "manual_override"
    assert hansen["average_block_length"] == 4
    assert hansen["block_length_source"] == "manual_override"


def test_auto_block_length_grows_with_dependence_strength() -> None:
    strong_series_a = np.asarray(
        [
            0.0010,
            0.0013,
            0.0016,
            0.0019,
            0.0021,
            0.0024,
            0.0027,
            0.0030,
            0.0032,
            0.0035,
            0.0038,
            0.0040,
            0.0043,
            0.0046,
            0.0048,
            0.0051,
        ],
        dtype=np.float64,
    )
    strong_series_b = np.asarray(
        [
            0.0009,
            0.0012,
            0.0015,
            0.0018,
            0.0020,
            0.0023,
            0.0026,
            0.0028,
            0.0031,
            0.0034,
            0.0036,
            0.0039,
            0.0042,
            0.0044,
            0.0047,
            0.0050,
        ],
        dtype=np.float64,
    )
    weak_series_a = np.asarray(
        [
            0.0030,
            -0.0012,
            0.0018,
            -0.0004,
            0.0023,
            -0.0019,
            0.0007,
            0.0012,
            -0.0024,
            0.0020,
            -0.0009,
            0.0005,
            0.0016,
            -0.0015,
            0.0003,
            -0.0007,
        ],
        dtype=np.float64,
    )
    weak_series_b = np.asarray(
        [
            -0.0010,
            0.0014,
            -0.0006,
            0.0021,
            -0.0013,
            0.0008,
            0.0011,
            -0.0022,
            0.0017,
            -0.0003,
            0.0004,
            -0.0018,
            0.0013,
            -0.0005,
            0.0020,
            -0.0011,
        ],
        dtype=np.float64,
    )
    strongly_dependent = TrialWindowMatrix(
        trial_ids=[0, 1],
        window_indices=list(range(int(strong_series_a.size))),
        panel_keys=[f"{idx}:0" for idx in range(int(strong_series_a.size))],
        differential_matrix=np.asarray(
            [
                strong_series_a,
                strong_series_b,
            ],
            dtype=np.float64,
        ),
    )
    weakly_dependent = TrialWindowMatrix(
        trial_ids=[0, 1],
        window_indices=list(range(int(weak_series_a.size))),
        panel_keys=[f"{idx}:0" for idx in range(int(weak_series_a.size))],
        differential_matrix=np.asarray(
            [
                weak_series_a,
                weak_series_b,
            ],
            dtype=np.float64,
        ),
    )

    strong = run_white_reality_check(strongly_dependent, bootstrap_iters=100, average_block_length="auto", seed=5)
    weak = run_white_reality_check(weakly_dependent, bootstrap_iters=100, average_block_length="auto", seed=5)

    assert strong["block_length_source"].startswith("auto_")
    assert weak["block_length_source"].startswith("auto_")
    assert strong["average_block_length"] >= weak["average_block_length"]
    assert strong["block_length_dependence_strength"] >= weak["block_length_dependence_strength"]


def test_multiple_testing_returns_insufficient_when_windows_do_not_overlap() -> None:
    matrix = build_trial_window_differential_matrix(
        candidate_trial_panel=[
            {
                "trial": 0,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.001}}}},
                ],
            },
            {
                "trial": 1,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.002}}}},
                ],
            },
        ],
        champion_windows=[
            {"window_index": 5, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0}}}},
        ],
    )

    assert matrix is None
    assert run_white_reality_check(matrix)["comparable"] is False
    assert run_hansen_spa(matrix)["decision"] == "insufficient_evidence"
