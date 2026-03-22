# Training Pipeline Research Compare 2026-03-23

- Date: 2026-03-23
- Scope: `v4` train/selection/runtime pipeline, OCI scheduler/runtime state, recent crypto alpha/execution literature
- Goal: explain why recent performance feels unstable by comparing the implemented system against practical research methods and current server evidence
- Follow-up action plan:
  - `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

## 1. Bottom Line

The current system is not failing for one reason.

The strongest combined explanation is:

1. the research lane is more sophisticated than before, but its acceptance and promotion evidence chain is still inconsistent
2. offline and live runtime feature/execution semantics still diverge in important places
3. execution policy layers can override each other in a way that makes observed live behavior different from the run artifact's recommended behavior
4. recent live candidate trading evidence is negative even when expected edge is positive

This means the immediate priority is not "add another model."

The immediate priority is:

1. make train -> acceptance -> promotion -> live runtime one coherent contract
2. verify that runtime uses the same execution posture the trained run actually recommended
3. measure missed-entry and breaker-driven loss loops before changing alpha features again

## 2. Current Pipeline Map

### 2.1 Offline dataset

The current primary offline lane is:

- feature set: `v4`
- label set: `v2`
- trainer: `train_v4_crypto_cs`

Main steps:

1. build `features_v4`
2. attach `labeling_v2_crypto_cs`
3. load dataset in `dataset_loader.py`
4. train XGBoost classifier / regressor / ranker
5. run walk-forward evidence
6. build selection recommendations and isotonic selection calibration
7. run execution acceptance and runtime recommendation grid search
8. build trade-action and execution-risk-control artifacts
9. persist registry artifacts for paper/live

### 2.2 Candidate generation at runtime

The effective model-driven path is not `candidates_v1`.

The active learned path is:

1. `ModelPredictor.predict_scores`
2. optional `selection_calibration` via isotonic OOS mapping
3. `selection_policy` resolving effective selection fraction and min candidates
4. `ModelAlphaStrategyV1` building intents
5. `trade_action_policy` selecting hold vs risk behavior and notional multiplier
6. `execution_risk_control` deciding threshold and size ladder
7. `runtime_recommendations.execution` selecting preferred execution posture
8. `micro_order_policy` and operational overlay modifying that posture again
9. live/paper submit path

This is powerful, but it also means instability can come from any of those layers, not only the predictive model.

## 3. What The Current Champion Artifact Says

Server pointers on 2026-03-23 all resolve to:

- `latest = latest_candidate = champion = 20260322T093201Z-s42-da19a911`

Important artifact facts for that run:

- `selection_policy.selection_fraction = 0.0250`
- `selection_policy.threshold_key = top_5pct`
- `selection_policy.selection_recommendation_source = walk_forward_objective_optimizer`
- `selection_calibration.mode = isotonic_oos_v1`
- `selection_calibration.fit_rows = 18245`
- `runtime_recommendations.exit.recommended_exit_mode = risk`
- `runtime_recommendations.exit.recommended_hold_bars = 9`
- `runtime_recommendations.exit.recommended_risk_vol_feature = rv_36`
- `runtime_recommendations.execution.recommended_price_mode = JOIN`
- `runtime_recommendations.execution.recommended_replace_max = 1`
- `runtime_recommendations.execution.recommended_timeout_bars = 2`
- `runtime_recommendations.risk_control.operating_mode = safety_executor_only_v1`
- `runtime_recommendations.risk_control.live_gate.enabled = false`

The strongest governance inconsistency is:

- `trainer_research_evidence.pass = false`
- offline compare says `champion_edge`
- execution compare is not comparable
- risk control live gate is disabled by design
- but the same run was later manually promoted to champion

So the current champion should be treated as:

- a manually promoted operational champion
- not a champion that cleanly passed the full trainer evidence contract

## 4. OCI Runtime State

Observed OCI scheduler/runtime state on 2026-03-23:

- scheduler is `systemd`, not cron
- active timers include:
  - `autobot-live-execution-policy.timer`
  - `autobot-v4-challenger-promote.timer`
  - `autobot-v4-challenger-spawn.timer`
  - `autobot-v4-rank-shadow.timer`
  - `autobot-storage-retention.timer`
- running services include:
  - `autobot-dashboard.service`
  - `autobot-live-alpha-candidate.service`
  - `autobot-paper-v4.service`
  - `autobot-paper-v4-replay.service`
  - `autobot-ws-public.service`
- failed services include:
  - `autobot-v4-rank-shadow.service`
  - `autobot-live-alpha-replay-shadow.service`
- main live is inactive:
  - `autobot-live-alpha.service`

Rollout state is also important:

- candidate live is armed in `canary`
- `LIVE_BREAKER_ACTIVE` is present
- `start_allowed = false`
- `order_emission_allowed = false`

So "service active" does not currently mean "system is allowed to trade."

## 5. Recent Empirical Evidence From Live Candidate

Candidate live DB evidence is negative:

- `trade_journal` closed trades: `120`
- average realized pnl pct: about `-0.0868%`
- total realized pnl quote: about `-760`

Recent trade samples show a repeated pattern:

- positive expected edge at entry
- frequent `notional_multiplier = 1.5`
- realized outcomes still negative in many closed trades
- repeated `CANCELLED_ENTRY` with `MAX_REPLACES_REACHED`

Execution-attempt evidence:

- `FILLED = 118`
- `MISSED = 50`
- dominant execution modes:
  - `PASSIVE_MAKER = 119`
  - `JOIN = 48`
  - `CROSS_1T = 1`

This means the live system is suffering a real missed-entry problem, not only an offline-score problem.

## 6. Offline vs Live Gaps Still Present

### 6.1 Feature distribution gap

Offline `v4` training drops rows without mandatory micro.

Live runtime still contains zero-fill behavior in the multi-TF runtime builder path:

- missing feature values become `0.0`
- missing micro-derived fields can propagate as neutral zeros

That is dangerous because:

1. calibrated selection scores assume offline feature distributions
2. trade-action bins depend on `rv_12`, `rv_36`, `atr_pct_14`, and related state fields
3. zero-filled runtime states can silently route decisions into the wrong bins

### 6.2 Fill semantics gap

Backtest uses candle-touch semantics.

Paper uses ticker-touch semantics.

Live uses real exchange execution with:

- queue risk
- missed fills
- replacement timing
- breaker escalation

This gap is already acknowledged in the repo's live execution parity redesign docs and is still materially present.

### 6.3 Execution override layering

The trained run recommends `JOIN`, but live execution can still become more conservative because:

1. runtime recommendation picks a base profile
2. micro order policy assigns a liquidity tier
3. `_merge_profiles_conservative()` keeps the more conservative price mode

So a run can recommend `JOIN`, while live still spends much of its time in `PASSIVE_MAKER`.

Additional direct finding from the live execution override audit:

- for the recent candidate live sample, run-level recommendation was `JOIN`
- observed final submit matched `JOIN` only about `28%`
- at least `30` recent attempts were directly switched by `execution_policy` from `JOIN` to `PASSIVE_MAKER`

This means the current live divergence is not explained only by `micro_order_policy`.
The `live_execution_policy` layer itself is already selecting `PASSIVE_MAKER` in a meaningful share of cases.

## 7. Literature That Best Matches The Real Problems

### 7.1 Cross-sectional alpha papers

Recent papers most aligned with the current alpha lane:

- Machine learning and the cross-section of cryptocurrency returns (IRFA 2024)
  - practical value after costs
  - strongest in hard-to-arbitrage names
  - supports broad characteristic sets plus non-linear learners
- Cross-cryptocurrency return predictability (JEDC 2024)
  - supports lagged cross-coin spillovers
  - directly justifies leader-lag and breadth features
- Cross-sectional interactions in cryptocurrency returns (IRFA 2025)
  - supports explicit interaction terms rather than only raw standalone factors
- A Trend Factor for the Cross-Section of Cryptocurrency Returns (JFQA 2025)
  - builds an aggregate technical signal from 28 price/volume indicators across multiple horizons
  - uses ML aggregation, weekly value-weighted sorting, and cost-aware practical checks
- Order flow and cryptocurrency returns (Journal of Financial Markets, 2026 online)
  - shows world order flow dominates fundamentals for prediction
  - supports non-linear ML and robust portfolio sorts

These papers support the direction of `v4`:

- cross-sectional targets
- spillover/breadth
- trend aggregation
- interaction features
- order-flow information

### 7.2 Execution and deployment papers

Recent papers most aligned with the current execution gap:

- The Good, the Bad, and Latency: Exploratory Trading on Bybit and Binance (SSRN 2024)
  - millions of live orders on real crypto exchanges
  - shows systematic adverse selection, fill worsening, and immediate-fail risk
  - directly relevant to our paper/live fill optimism gap
- Deep Reinforcement Learning for Cryptocurrency Trading: Practical Approach to Address Backtest Overfitting (arXiv 2022 / AAAI 2023 bridge)
  - uses CPCV / walk-forward style validation tooling in code
  - explicitly rejects overfitted agents instead of trusting raw backtest profits
- Towards Generalizable Reinforcement Learning for Trade Execution (arXiv 2023)
  - frames execution as offline RL with dynamic context
  - argues overfitting comes from large context space and limited context samples
  - proposes context aggregation for better generalization
- Turn-of-the-candle effect in bitcoin returns (PMC article)
  - uses exchange-specific 1-minute data, fees, spreads, probabilistic Sharpe ratio, and out-of-sample validation
  - directly supports explicit intraday timing structure in crypto

## 8. Where Our System Is Strong vs Weak Relative To The Literature

### 8.1 Stronger than many papers

The codebase is already stronger than most academic prototypes in:

- state persistence
- restart/reconcile logic
- candidate vs champion runtime separation
- trade journal and execution attempt logging
- walk-forward evidence
- isotonic selection calibration
- execution-aware runtime recommendation search
- online risk adaptation and breaker integration

### 8.2 Still weaker than the best practical methods

Relative to the strongest practical papers, the current weak spots are:

1. promotion still allows manual override to dominate weak research evidence
2. live feature missing-value semantics are not yet fully aligned with offline training semantics
3. paper/backtest fill models remain too optimistic relative to real exchange behavior
4. runtime execution posture can be overridden by downstream conservative policies without a clean audit of the final chosen reason
5. online risk-control is active, but its live gate is disabled by design, so the contract is only partially enforced
6. local order-flow features are only single-venue local panels, while the latest order-flow literature uses broader world-flow information

## 9. Most Likely Root Causes Of The Current "Always Losing" Feel

In priority order:

1. live missed-entry loop
   - many `MISSED` attempts
   - repeated `CANCELLED_ENTRY`
   - `MAX_REPLACES_REACHED`
2. positive expected edge is not translating into realized edge
   - recent closed trades often have positive expected net edge but negative realized pnl
3. live breaker loop compounds execution misses
   - `RISK_CONTROL_ONLINE_BREACH_STREAK`
   - `RISK_EXIT_STUCK_MAX_REPLACES`
   - intermittent sync/runtime breaker noise
4. offline-live feature mismatch remains plausible
   - zero-fill runtime fields
   - micro-quality and state-feature distortions
5. selection and sizing are still aggressive in bad cases
   - many recent losing trades use high `notional_multiplier`
   - selection fractions and size ladders can still push concentrated exposure on a narrow candidate set

## 10. Recommended Next Work Order

### Phase 1: Execution truth before new alpha work

1. prove what final execution profile is actually used at each live attempt
2. log base recommendation vs micro policy override vs operational overlay override vs final submit action
3. produce mode-by-mode realized PnL and miss-rate reports

### Phase 2: Offline-live feature parity audit

1. count runtime rows where state features were zero-filled
2. compare live runtime feature distributions against offline training distributions
3. especially audit:
   - `rv_12`
   - `rv_36`
   - `atr_pct_14`
   - micro/order-flow columns

### Phase 3: Promotion contract hardening

1. separate manual promote state from evidence-passed state
2. preserve original trainer evidence even after manual champion mutation
3. stop interpreting manual champion status as validated champion status

### Phase 4: Replace optimistic fill assumptions

1. use real `execution_attempts` to fit fill/miss/shortfall models
2. make paper and live share the same execution contract
3. stop treating touch-fill paper behavior as good enough parity

## 11. Working Conclusion

The current `v4` research direction is broadly correct.

The main problem is not that the feature ideas are obviously wrong.
The main problem is that:

- validation evidence,
- promotion semantics,
- runtime feature parity,
- and execution realism

are still not tightly locked together.

If we fix only the predictive model now, we will probably keep re-learning the same lesson:

- good-looking offline edge,
- unstable live fills,
- breaker escalation,
- and disappointing realized PnL.
