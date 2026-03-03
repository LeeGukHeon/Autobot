# ADR 0006 - Backtest Lookahead and Fill Model

## Status
Accepted (2026-03-03)

## Context
- PaperRun already standardized intent/order/fill/portfolio contracts.
- Backtest v1 needed to reuse the same contracts while preventing OHLC lookahead bias.
- Upbit candle data can be sparse (no trade -> no candle), so loader behavior had to handle sparse bars by default.

## Decision
- Keep strategy/execution contracts aligned with PaperRun (`OrderIntent`, order/fill records, equity snapshots).
- Use a strict timing rule:
  - evaluate/generate intents at bar `t`
  - match only from bar `t+1`.
- Adopt a candle-touch limit fill model:
  - `bid`: fill when `next.low <= limit`
  - `ask`: fill when `next.high >= limit`
  - fill price = limit price (conservative).
- Provide sparse loader as default and optional dense-grid synthesis for strategies that assume contiguous bars.

## Consequences
### Positive
- Same execution contract can be extended across backtest/paper/live.
- Lookahead bias is explicitly prevented by runtime behavior and tests.
- Run artifacts are consistent with PaperRun (`events/orders/fills/equity` + `summary`).

### Trade-offs
- v1 excludes partial-fill depth realism and latency/slippage modeling.
- Historical tick-size regime changes are not replayed; latest rule cache/fallback is used.

## Follow-up
- Add realism upgrades (slippage, partial fill, latency).
- Add schedule-based dynamic universe and experiment registry integration.
