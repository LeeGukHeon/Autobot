# ADR 0005 - Live PaperRun v1 Architecture

## Status
Accepted (2026-03-03)

## Context
- The project needed an end-to-end runtime loop that mirrors live behavior without placing real orders.
- Existing modules already provided:
  - Upbit REST/WS clients (T02/T03)
  - top-N scanner from ticker stream
- To keep future backtest/live parity and enable C++ execution integration, shared contracts for intent/order/fill/portfolio were required.

## Decision
- Implement PaperRun v1 as a composable pipeline:
  - `MarketDataHub` (ticker cache/history)
  - `UniverseProviderTop20` (scanner + hold-time)
  - `CandidateGeneratorV1` (rule-based)
  - `TradeGateV1` (minimal risk checks)
  - `PaperExecutionGateway` + `PaperSimExchange` (limit-only lifecycle)
- Standardize the intent/order/fill data contracts in Python now (`execution/intent.py`, `paper/sim_exchange.py`).
- Persist append-only runtime artifacts under `data/paper/runs/{run_id}`.
- Add CLI entrypoint:
  - `python -m autobot.cli paper run ...`

## Consequences
### Positive
- Practical live-paper dry-run loop with real WS data path.
- Clear separation of concerns for strategy/risk/execution.
- Deterministic, replayable logs for debugging and future analytics.
- Easier migration path to shared execution contract across paper/backtest/live.

### Trade-offs
- Touch-fill is simplified versus real matching.
- Rules API data may be partially unavailable without private credentials.
  - Runtime uses safe fallback defaults and cache.
- Current loop is sync-REST + async-WS mixed; more aggressive optimization can be deferred.

## Follow-up
- Upgrade fill model to orderbook-aware execution (v1.1).
- Share risk exits (TP/SL/trailing) across paper/backtest/live.
- Add richer run report metrics from event logs.
