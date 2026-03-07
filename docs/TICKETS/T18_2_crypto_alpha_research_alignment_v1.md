# T18.2: Crypto Alpha Research Alignment v1

## Goal
- Align the next alpha contract with recent cryptocurrency cross-sectional research while preserving the current production baseline.
- Upgrade the system by using already-collected candles, 1m context, and micro data more effectively, instead of first expanding raw data scope.
- Implement the next wave as an explicit new contract, not as an in-place mutation of `v3`.

## Current Baseline To Freeze
- Feature contract: `feature_set=v3`
- Label contract: `label_set=v1`
- Trainer: `train_v3_mtf_micro`
- Runtime strategy: `model_alpha_v1`

Current baseline files:
- `autobot/features/feature_set_v3.py`
- `autobot/features/labeling_v1.py`
- `autobot/models/train_v3_mtf_micro.py`
- `autobot/strategy/model_alpha_v1.py`

Freeze policy:
- Do not change `label_v1` semantics in place.
- Do not append research-only columns to `features_v3` in place.
- Do not overload `train_v3_mtf_micro` with many `if feature_set == ...` branches.
- Keep `model_alpha_v1` as the stable selection/runtime handoff unless the strategy contract itself changes.

## Why This Ticket Exists
Current system status:
- `label_v1` is based on future close-to-close threshold classification over `12` bars.
- Runtime alpha is cross-sectional ranking with threshold + `top_pct` selection.
- Current v3 features are strong on:
  - base return and volatility
  - 1m aggregation
  - 15m/60m/240m context
  - mandatory micro
- Current v3 features are weak on:
  - cross-coin spillover
  - explicit market breadth and concentration
  - 24/7 intraday periodicity
  - explicit interaction features
  - training labels aligned to net return / cross-sectional selection

This means the next bottleneck is not "collect more raw data first" but "use existing data with a better crypto-specific objective and feature contract".

## Research Alignment
The next design is intentionally aligned to recent cryptocurrency papers that are close to the current program.

### 1. Machine learning and the cross-section of cryptocurrency returns
- Source: International Review of Financial Analysis, 2024
- Link: `https://doi.org/10.1016/j.irfa.2024.103244`
- Why it matters here:
  - Supports cross-sectional crypto return prediction as a valid target.
  - Finds that simple signals such as price, past alpha, momentum, and illiquidity carry substantial value.
  - Suggests we should improve target alignment and signal composition before jumping to overly complex architectures.

### 2. Cross-cryptocurrency return predictability
- Source: Journal of Economic Dynamics and Control, 2024
- Link: `https://doi.org/10.1016/j.jedc.2024.104863`
- Why it matters here:
  - Directly supports lagged returns of other cryptocurrencies as predictors.
  - Matches the current need for leader-lag, spillover, and limited-attention style features.

### 3. Cross-sectional interactions in cryptocurrency returns
- Source: International Review of Financial Analysis, 2025
- Link: `https://doi.org/10.1016/j.irfa.2024.103809`
- Why it matters here:
  - Finds strong interaction effects among liquidity, risk, and past returns.
  - Supports adding a small number of explicit interaction/regime features instead of only raw standalone columns.

### 4. A Trend Factor for the Cross-Section of Cryptocurrency Returns
- Source: Journal of Financial and Quantitative Analysis, 2025
- Link: `https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178`
- Why it matters here:
  - Supports aggregate trend signals built from price and volume related technical inputs.
  - Fits the current v3 design better than adding unrelated macro or on-chain stacks first.

### 5. Periodicity in Cryptocurrency Volatility and Liquidity
- Source: Journal of Financial Econometrics, 2024 issue
- Link: `https://doi.org/10.1093/jjfinec/nbac034`
- Why it matters here:
  - Shows systematic day-of-week, hour-of-day, and within-hour patterns in crypto volatility and volume.
  - Supports explicit periodicity features for a 24/7 market.

### 6. Intraday and daily dynamics of cryptocurrency
- Source: International Review of Economics and Finance, 2024
- Link: `https://doi.org/10.1016/j.iref.2024.103658`
- Why it matters here:
  - Shows intraday periodicity tied to major exchange operating times and token/native differences.
  - Supports adding overlap/session features rather than assuming crypto has no time-of-day structure.

## Architecture Decision
The next wave should be added as a new contract:

- `feature_set=v4`
- `label_set=v2`
- `trainer=train_v4_crypto_cs`

Runtime policy:
- Keep `model_alpha_v1` as the serving strategy initially.
- Do not create `model_alpha_v2` unless runtime behavior changes beyond score interpretation, selection, sizing, or execution handoff already supported today.

Reason:
- The main change is in data contract and model target, not in the runtime order-intent contract.
- This reduces legacy sprawl and preserves current paper/live service behavior.

## Detailed Design

### A. Label v2: Cross-Sectional and Net-of-Cost Aligned

#### Problem in current code
- `label_v1` is defined in `autobot/features/labeling_v1.py`.
- It creates:
  - `y_reg = log(close[t+h]) - log(close[t])`
  - `y_cls = 1 / 0 / neutral-drop`
- This is useful as a first approximation, but runtime alpha is not a pure absolute-direction classifier.
- Runtime alpha actually does:
  - thresholding
  - cross-sectional ranking
  - top-slice selection
  - size scaling based on model score

#### Proposed contract
- Keep the old columns only in `label_v1`.
- Introduce `label_v2` with at least:
  - `y_reg_net_12`
  - `y_rank_cs_12`
  - `y_cls_topq_12`

Definitions:
- `y_reg_net_12`
  - future `12 x 5m` close-to-close log return after estimated fee and safety haircut
- `y_rank_cs_12`
  - percentile rank of `y_reg_net_12` across active markets at the same `ts`
- `y_cls_topq_12`
  - `1` if rank is in top quantile, `0` if in bottom quantile, else neutral/drop

Expected effect:
- Align training target with actual cross-sectional selection logic.
- Reduce mismatch between "absolute up/down" labels and "best relative coin at this timestamp" runtime decisions.

Legacy rule:
- Do not retrofit these columns into `label_v1`.
- Add a new builder module, for example `autobot/features/labeling_v2_crypto_cs.py`.

### B. Feature Set v4: Keep v3 Core, Add Crypto-Specific Packs

#### Problem in current code
- `feature_set_v3.py` already covers:
  - base returns/volatility/body/range/volume
  - 1m aggregation
  - 15m/60m/240m context
  - mandatory micro
  - sample weights
- It does not directly encode:
  - spillover from leader coins
  - market breadth / dispersion
  - explicit periodicity
  - explicit interaction features

#### Proposed v4 structure
- Build `feature_set_v4` from shared blocks:
  - base v3 block
  - spillover block
  - trend-volume block
  - periodicity block
  - interaction block

#### v4 Block 1: Spillover and Breadth
- Add features such as:
  - `btc_ret_1`, `btc_ret_3`, `btc_ret_12`
  - `eth_ret_1`, `eth_ret_3`, `eth_ret_12`
  - `leader_basket_ret_1`, `leader_basket_ret_3`, `leader_basket_ret_12`
  - `market_breadth_pos_1`
  - `market_breadth_pos_12`
  - `market_dispersion_12`
  - `turnover_concentration_hhi`
  - `rel_strength_vs_btc_12`

Expected effect:
- Capture slow diffusion and cross-coin spillover.
- Better fit to the cross-cryptocurrency predictability literature.

#### v4 Block 2: Aggregate Trend and Volume Trend
- Add a small aggregate signal family:
  - `price_trend_short`
  - `price_trend_med`
  - `price_trend_long`
  - `volume_trend_long`
  - `trend_consensus`
  - `trend_vs_market`

Expected effect:
- Preserve the current technical/micro flavor while adding a trend aggregate closer to recent crypto cross-sectional findings.

#### v4 Block 3: Periodicity
- Add:
  - `hour_sin`, `hour_cos`
  - `dow_sin`, `dow_cos`
  - `weekend_flag`
  - `asia_us_overlap_flag`
  - `utc_session_bucket`

Expected effect:
- Make 24/7 market structure visible to the model.
- Improve time-of-day and intraweek adaptation without adding new external datasets.

#### v4 Block 4: Interaction Features
- Add a limited interaction pack:
  - `mom_x_illiq`
  - `mom_x_spread`
  - `spread_x_vol`
  - `rel_strength_x_btc_regime`
  - `one_m_pressure_x_spread`
  - `volume_z_x_trend`

Expected effect:
- Encode the interaction effects reported in recent crypto cross-sectional work without requiring a much more complex model stack.

Legacy rule:
- Do not duplicate `feature_set_v3.py` into a large copy and edit inline.
- First extract shared builders from `feature_set_v3.py` into smaller reusable blocks.
- `feature_set_v4.py` should be a thin composition layer, not a forked monolith.

### C. Trainer: New Wrapper, Shared Core

#### Problem in current code
- `train_v3_mtf_micro.py` is a good baseline trainer, but it is tightly named and scoped to the current `v3/v1` classification contract.
- Overloading it with many target-specific branches will create version confusion.

#### Proposed structure
- Keep `train_v3_mtf_micro.py` as frozen baseline wrapper.
- Extract common training core utilities where possible.
- Add a new wrapper:
  - `train_v4_crypto_cs.py`

Target support:
- first priority:
  - regression on `y_reg_net_12`
  - rank-style evaluation by top-slice portfolio outcomes
- optional comparison:
  - binary top-quantile classification on `y_cls_topq_12`

Why not jump to deep learning first:
- Recent crypto evidence here supports strong performance from relatively simple, interpretable signals.
- The current codebase already has a good XGBoost path and robust offline/paper runtime integration.

### D. Validation and Acceptance

#### Problem in current code
- Current production acceptance is stronger than before, but the model training side is still centered around one split per run.

#### Proposed design
- New v4 acceptance should require:
  - rolling walk-forward windows
  - balanced Pareto comparison on:
    - realized pnl
    - max drawdown
    - fill rate
    - slippage
  - no in-place override of v3 champion lane

Initial acceptance policy:
- v4 starts as parallel research lane only.
- No direct promote to current champion until:
  - multiple rolling windows pass
  - paper soak parity is confirmed

### E. Runtime and Live Feature Builder

#### Problem in current code
- `LIVE_V3` exists, but it only knows the v3 contract.
- Adding new features directly into the current live builder before offline win would create unnecessary runtime churn.

#### Proposed design
- Phase 1:
  - offline feature build + trainer + backtest only
- Phase 2:
  - add `LIVE_V4` only after offline acceptance
- Phase 3:
  - allow paper alpha preset to switch to v4 candidate/champion

Legacy rule:
- Do not mutate `LIVE_V3` into a multi-contract builder full of conditional branches.
- Keep `LIVE_V3` stable and add `LIVE_V4` only when the v4 contract is frozen.

## Implementation Order

Current implementation checkpoint:
- Phase 1 is in place:
  - `labeling_v2_crypto_cs.py`
  - `feature_set=v4` offline dataset lane
- Phase 3 has started:
  - `train_v4_crypto_cs.py` added as a separate offline trainer lane
  - current scope supports single-split offline training with `task=cls|reg`
  - rolling walk-forward acceptance is still pending
  - paper/live parity for `v4` is still pending

### Phase 0: Refactor For Clean Extension
- Extract shared feature-building blocks from `feature_set_v3.py`
- Extract shared trainer evaluation helpers if needed
- Keep runtime strategy untouched

### Phase 1: Label v2
- Add `labeling_v2_crypto_cs.py`
- Add `label_spec_v2`
- Build offline dataset with current v3-style inputs and new labels

### Phase 2: Feature Set v4
- Add spillover and breadth
- Add trend aggregate pack
- Add periodicity pack
- Add interaction pack

### Phase 3: Trainer and Offline Evaluation
- Add `train_v4_crypto_cs.py`
- Add rolling walk-forward evaluation
- Compare against v3 champion on balanced Pareto basis

### Phase 4: Runtime Parity
- Add `LIVE_V4`
- Add paper/backtest preset support
- Keep live service off until paper parity is proven

### Phase 5: Promote and Sunset
- If v4 proves superior and stable:
  - promote v4 candidate to champion lane
  - keep v3 as legacy fallback for one transition period
  - only then consider removal of truly dead compatibility code

## Explicit Non-Goals
- Adding on-chain data first
- Adding news or social sentiment first
- Replacing the runtime with RL first
- Forcing `model_alpha_v2` before the data/model contract proves itself

## Definition of Done
- New docs:
  - this ticket
  - config and contract schema updates when implementation starts
- Clean extension path:
  - no in-place mutation of `v3/v1`
  - no copy-paste monolith fork of `feature_set_v3.py`
- New research lane artifacts:
  - `feature_set=v4`
  - `label_set=v2`
  - `trainer=train_v4_crypto_cs`
- Acceptance:
  - rolling backtest comparison
  - paper parity after offline win

## Notes for Implementation
- The first milestone should be label and feature contract work, not runtime order-engine changes.
- The current runtime path is already much healthier than the training objective alignment.
- If a proposed improvement requires changing both dataset contract and runtime contract at once, split it into two tickets.
