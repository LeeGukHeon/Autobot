# T18.3 Operational Runtime Overlay v1

## Goal
- Keep alpha selection learned and stable.
- Keep backtest on one fixed compare profile for candidate-vs-champion sanity checks.
- Move current-market adaptation into a paper/live operational layer only.

## Why
- Recent crypto cross-sectional studies support keeping the alpha contract stable and handling regime adaptation with liquidity, volatility, and session-aware overlays rather than re-tuning selection every day.
- This ticket maps that literature into the current `model_alpha_v1` + `paper` architecture. It does **not** try to replicate paper coefficients directly.

## Literature Mapping
- `Machine learning and the cross-section of cryptocurrency returns` (IRFA 2024)
  - runtime should respect learned alpha outputs instead of daily online re-tuning.
- `Cross-cryptocurrency return predictability` (JEDC 2024)
  - regime adaptation is better handled with spillover/market-state context than ad-hoc selection retuning.
- `Cross-sectional interactions in cryptocurrency returns` (IRFA 2025)
  - liquidity, risk, and past return interactions justify regime-aware position and slot control.
- `Periodicity in Cryptocurrency Volatility and Liquidity` (JFEC / OUP)
  - session and overlap effects justify time-of-day sensitive execution posture.
- `Order Book Liquidity on Crypto Exchanges` (JRFM 2025)
  - spread/depth/snapshot freshness are natural building blocks for a micro quality score.
- `Cryptocurrency market risk-managed momentum strategies` (FRL 2025)
  - runtime risk control should support both defensive contraction and selective expansion in favorable regimes.

## Design Principles
- `backtest = sanity gate`
  - fixed compare profile only
  - no online operational overlay
- `paper = final gate`
  - learned runtime selection
  - operational overlay enabled
- `live/paper current-market adaptation`
  - `micro quality gate`
  - `risk multiplier`
  - `max_positions` override
  - `execution aggressiveness`

## Implemented In v1
1. `rolling paper evidence`
- paper summary now records rolling-window evidence instead of only end-of-run aggregates
- current fields:
  - `rolling_window_minutes`
  - `rolling_windows_total`
  - `rolling_active_windows`
  - `rolling_nonnegative_active_window_ratio`
  - `rolling_positive_active_window_ratio`
  - `rolling_max_fill_concentration_ratio`
  - `rolling_max_window_drawdown_pct`
  - `rolling_worst_window_realized_pnl_quote`

2. `risk multiplier`
- derived from regime score and micro feature quality
- used only in paper/live-style runtime
- applied on top of existing position sizing, not instead of alpha

3. `dynamic max_positions`
- runtime can shrink or expand effective slot count within configured bounds
- driven by breadth/regime quality rather than raw alpha score retuning

4. `execution aggressiveness`
- runtime now adjusts `price_mode` conservatively/aggressively from micro quality and session state
- current implementation also scales:
  - `timeout_ms`
  - `replace_interval_ms`
  - `max_replaces`
  - `max_chase_bps`
- conservative mode slows execution recycling and reduces chase budget
- aggressive mode tightens execution timing and slightly expands chase budget

5. `micro quality composite score`
- built from:
  - spread
  - depth
  - trade coverage
  - book coverage
  - snapshot age
- can hard-block trading when the score is catastrophically low

## Current Contract
- runtime paper/live:
  - learned `min_prob`
  - learned `top_pct/min_candidates` when `selection_recommendations.json` exists
  - operational overlay enabled
- acceptance:
  - fixed compare profile for backtest sanity gate
  - learned runtime for paper final gate
  - rolling evidence included in paper final gate
  - direct `micro_quality_score_mean` floor included in paper final gate
  - short-history median run quality / pnl evidence included in paper final gate
  - backtest sanity gate now also records a lightweight `deflated_sharpe_ratio_est`

## Remaining Work
- calibrate paper rolling and history thresholds with longer live evidence
- add a regime-aware `execution cap` that respects venue-specific liquidity ceilings
- turn the current heuristic micro quality score into a fully first-class acceptance metric rather than a companion gate
- wire stronger multiple-testing correction such as `Reality Check / SPA` in addition to the current lightweight DSR-style sanity check
