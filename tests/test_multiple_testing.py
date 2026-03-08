from __future__ import annotations

from autobot.models.multiple_testing import (
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
    assert matrix.differential_matrix.shape == (2, 3)
    assert matrix.differential_matrix[0, 0] == 0.003


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
    assert result["candidate_edge"] is True
    assert result["decision"] == "candidate_edge"
    assert result["p_value"] <= 0.20


def test_run_hansen_spa_detects_candidate_edge() -> None:
    matrix = build_trial_window_differential_matrix(
        candidate_trial_panel=[
            {
                "trial": 0,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0035}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0030}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0040}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0038}}}},
                ],
            },
            {
                "trial": 1,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0032}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0031}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0041}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0036}}}},
                ],
            },
            {
                "trial": 2,
                "windows": [
                    {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0034}}}},
                    {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0033}}}},
                    {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0042}}}},
                    {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0039}}}},
                ],
            },
        ],
        champion_windows=[
            {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0012}}}},
            {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
            {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0013}}}},
            {"window_index": 3, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0011}}}},
        ],
    )

    result = run_hansen_spa(matrix, bootstrap_iters=200, alpha=0.20, seed=7)

    assert result["comparable"] is True
    assert result["candidate_edge"] is True
    assert result["decision"] == "candidate_edge"
    assert result["p_value"] <= 0.20


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
