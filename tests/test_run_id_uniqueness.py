from autobot.backtest.run_id import build_backtest_run_id
from autobot.paper.run_id import build_paper_run_id


def test_backtest_run_id_is_unique_for_same_inputs() -> None:
    first = build_backtest_run_id(
        tf="5m",
        markets=["KRW-BTC", "KRW-ETH"],
        from_ts_ms=1,
        to_ts_ms=2,
        seed=42,
    )
    second = build_backtest_run_id(
        tf="5m",
        markets=["KRW-BTC", "KRW-ETH"],
        from_ts_ms=1,
        to_ts_ms=2,
        seed=42,
    )
    assert first != second
    assert first.startswith("backtest-")
    assert second.startswith("backtest-")


def test_paper_run_id_is_unique() -> None:
    first = build_paper_run_id()
    second = build_paper_run_id()
    assert first != second
    assert first.startswith("paper-")
    assert second.startswith("paper-")
