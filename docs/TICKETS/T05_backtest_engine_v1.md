# T05 - Backtest Engine v1

## Goal
- Reuse PaperRun contracts (`OrderIntent`, order/fill/portfolio logs) on parquet candle history.
- Enforce no-lookahead execution:
  - signal/intents at bar `t` close
  - matching only from bar `t+1`.
- Produce reproducible run artifacts under `data/backtest/runs/{run_id}`.

## Scope Implemented
- New runtime modules:
  - `autobot/backtest/engine.py`
  - `autobot/backtest/loader.py`
  - `autobot/backtest/fill_model.py`
  - `autobot/backtest/exchange.py`
  - `autobot/backtest/universe.py`
  - `autobot/backtest/metrics.py`
  - `autobot/backtest/reporting.py`
  - `autobot/backtest/run_id.py`
- CLI expansion:
  - `python -m autobot.cli backtest run ...`
- Config expansion:
  - `config/backtest.yaml`

## Runtime Notes
- Fill model: candle-touch limit fill (buy: `next.low <= limit`, sell: `next.high >= limit`).
- Lookahead guard: newly submitted orders are activated on next bar index only.
- Data loader:
  - reads `data/parquet/candles_v1/tf=.../market=.../*.parquet`
  - supports sparse default and optional dense-grid synthesis.
- Universe:
  - `fixed_list` (explicit markets)
  - `static_start` top-N by quote volume sum at start window.

## Artifacts
- `events.jsonl`
- `orders.jsonl`
- `fills.jsonl`
- `equity.csv`
- `summary.json`

## Tests Added
- `tests/test_backtest_fill_model.py`
- `tests/test_backtest_loader.py`
- `tests/test_backtest_exchange.py`
- `tests/test_backtest_engine_integration.py`
