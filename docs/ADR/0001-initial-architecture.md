# ADR 0001: Initial Architecture

- Status: Accepted
- Date: 2026-03-02
- Context: Upbit AutoBot v1 bootstrap

## Summary
Adopt a config-first, interface-based architecture that allows strategy/risk logic to be reused unchanged across backtest, paper trading, and live trading by swapping execution gateways.

## Decisions
1. Separate modules by concern: Data, Features, Models, Strategy, Risk, Execution, Upbit client, Backtest.
2. Treat Upbit `orders/chance` and `orderbook/instruments` as the source of truth for fees, limits, order types, and tick size.
3. Keep `ord_type=market` disabled; use limit-centric execution with emergency `best + ioc/fok` mode.
4. Implement centralized rate-limit handling using group-aware limits + `Remaining-Req` header.
5. Keep runtime artifacts (`data/`, `models/`, `logs/`) out of git.
6. Start with Python-first implementation, add C++ acceleration only for clear bottlenecks.

## Consequences
- Reduced coupling and safer mode transitions (`backtest -> paper -> live`).
- Extra upfront work in interfaces/state management, but lower long-term migration risk.
- More robust order validation and fewer exchange rejection errors.

## Follow-up
- T01: CSV -> Parquet ingest and schema contract.
- T02: Upbit REST client + JWT auth + rate limiter.
- T03: OrderRulesCache and pre-trade validator.
