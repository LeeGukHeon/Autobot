# T04 - Live PaperRun v1

## Goal
- Build an end-to-end live paper-trading loop:
  - WS ticker ingest -> Top20 universe -> candidate generation -> trade gate -> paper execution.
- Keep the flow close to real operation (`signal -> order -> fill -> logs`) while staying non-destructive.

## Scope Implemented
- New runtime modules:
  - `autobot/paper/engine.py`
  - `autobot/paper/sim_exchange.py`
  - `autobot/paper/fill_model.py`
  - `autobot/execution/intent.py`
  - `autobot/strategy/candidates_v1.py`
  - `autobot/strategy/trade_gate_v1.py`
  - `autobot/common/event_store.py`
- CLI expansion:
  - `python -m autobot.cli paper run --duration-sec 600 --quote KRW --top-n 20`
- Config updates:
  - `config/risk.yaml` to `risk.*` schema for paper runtime controls
  - `config/strategy.yaml` to `strategy.universe` + `strategy.candidates_v1`

## Core Runtime Behavior
- **MarketDataHub**
  - Maintains latest ticker cache and short rolling history for momentum.
- **UniverseProviderTop20**
  - Reuses T03 scanner (`TopTradeValueScanner`) and refreshes top-N by `acc_trade_price_24h`.
  - Supports hold-time to reduce churn.
- **CandidateGenerator v1**
  - Rule-based momentum + trade-value score.
  - Emits side (`bid`/`ask`), score, ref price, and debug meta.
  - When no momentum candidate exists at startup, a one-time fallback candidate can seed pipeline validation.
- **TradeGate v1**
  - Blocks on min total, balance, position duplication, max positions, and failure cooldown.
- **PaperExecutionGateway v1**
  - `limit` only.
  - Touch-fill model (ticker-based).
  - Timeout -> cancel -> bounded reprice attempts.
- **RulesProvider (cached)**
  - Uses `/v1/orders/chance` when credentials are available.
  - Uses `/v1/orderbook/instruments` for `tick_size` when available.
  - Falls back safely with inferred tick size + default fee/min-total when API data is unavailable.

## Logging Artifacts
- Runtime output directory:
  - `data/paper/runs/{run_id}/`
- Files:
  - `events.jsonl`
  - `orders.jsonl`
  - `fills.jsonl`
  - `equity.csv`
- Event types include:
  - `MARKET_SNAPSHOT`, `UNIVERSE_UPDATE`, `CANDIDATES`, `INTENT_CREATED`
  - `ORDER_SUBMITTED`, `ORDER_CANCELED`, `ORDER_FILLED`
  - `PORTFOLIO_SNAPSHOT`, `RUN_STARTED`, `RUN_COMPLETED`

## Tests Added
- `tests/test_paper_fill_model.py`
- `tests/test_paper_sim_exchange.py`
- `tests/test_trade_gate_v1.py`
- `tests/test_paper_engine_integration.py`

## Validation
- `python -m pytest -q`
  - Result: all tests passing (50 total at implementation time).
- Integration test validates:
  - candidate -> intent -> order submit -> fill path
  - run artifacts are generated (`events/orders/fills/equity`)

## Notes
- Market order is not used in Paper v1 (`limit` only).
- REST rule calls are cached and lazily resolved to avoid request bursts.
- Fill model is intentionally simplified for pipeline verification and can be upgraded to orderbook-based fill in a follow-up ticket.
