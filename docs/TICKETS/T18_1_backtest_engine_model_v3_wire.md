# T18.1: Wire v3 Model Into Real Backtest Engine

## Goal
- Connect `train_v3_mtf_micro` model scores to real backtest execution flow:
  - strategy decision
  - limit order submission
  - next-bar touch fill
  - timeout/cancel/replace
  - portfolio PnL and artifacts

## Scope
- Added model strategy path:
  - `backtest run --strategy model_alpha_v1`
  - `model_ref/model_family/feature_set` wiring
- Added strategy components:
  - `autobot/strategy/model_alpha_v1.py`
  - `autobot/backtest/strategy_adapter.py`
  - `autobot/models/predictor.py`
  - `autobot/models/dataset_loader.py` ts-group loader
- Backtest engine wiring:
  - ts-batch decision timing (`signal at ts`, order active from next bar)
  - cross-sectional selection (`top_pct`, `min_prob`, `min_candidates_per_ts`, no forced min-1)
  - hold/risk exit mode
  - cooldown + max positions
  - fill-to-strategy state callback

## Artifacts
- Existing:
  - `summary.json`, `events.jsonl`, `orders.jsonl`, `fills.jsonl`, `equity.csv`
- Added:
  - `trades.csv`
  - `per_market.csv`
  - `selection_stats.json`
  - `debug_mismatch.json`

## Config/CLI
- `config/strategy.yaml`:
  - `strategy.model_alpha_v1.*` block added
- `config/backtest.yaml`:
  - `backtest.strategy.*` block added
- `python -m autobot.cli backtest run` extended with:
  - `--strategy model_alpha_v1`
  - `--model-ref`, `--model-family`, `--feature-set`
  - `--entry`, `--top-pct`, `--min-prob`, `--min-cands-per-ts`
  - `--exit-mode`, `--hold-bars`, `--tp-pct`, `--sl-pct`, `--trailing-pct`
  - `--cooldown-bars`, `--max-positions-total`
  - `--execution-price-mode`, `--execution-timeout-bars`, `--execution-replace-max`
  - `--start`, `--end`

## Notes
- `model_alpha_v1` currently enforces `feature_set=v3`.
- `micro mandatory` policy is honored by design:
  - missing feature row at `(ts, market)` => no signal and counted in `debug_mismatch.json`.
