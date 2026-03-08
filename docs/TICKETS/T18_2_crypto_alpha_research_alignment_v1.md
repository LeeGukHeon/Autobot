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
- Phase 2 is effectively complete for the first offline lane:
  - `spillover + breadth + periodicity + trend-volume + interaction` pack is now wired into `pipeline_v4`
  - current added columns:
    - `btc_ret_{1,3,12}`
    - `eth_ret_{1,3,12}`
    - `leader_basket_ret_{1,3,12}`
    - `market_breadth_pos_{1,12}`
    - `market_dispersion_12`
    - `turnover_concentration_hhi`
    - `rel_strength_vs_btc_12`
    - `hour_sin`, `hour_cos`
    - `dow_sin`, `dow_cos`
    - `weekend_flag`
    - `asia_us_overlap_flag`
    - `utc_session_bucket`
    - `price_trend_short`
    - `price_trend_med`
    - `price_trend_long`
    - `volume_trend_long`
    - `trend_consensus`
    - `trend_vs_market`
    - `mom_x_illiq`
    - `mom_x_spread`
    - `spread_x_vol`
    - `rel_strength_x_btc_regime`
    - `one_m_pressure_x_spread`
    - `volume_z_x_trend`
  - next major gap is no longer offline feature coverage, but execution-aware acceptance and live parity
  - `features_v4.min_rows_for_train` is currently relaxed to `4000`
    - reason: current server micro history is still shallow enough that `5000` blocks the first real `v4` lane runs
    - intent: keep the research lane moving without mutating the frozen `v3` baseline
- Phase 3 has started:
  - `train_v4_crypto_cs.py` added as a separate offline trainer lane
  - current scope supports single-split offline training with `task=cls|reg`
  - anchored rolling walk-forward evidence is now added for the `v4` lane
  - current offline compare uses `balanced_pareto_offline` on walk-forward aggregate metrics:
    - `ev_net_top5`
    - `precision_top5`
    - `pr_auc`
    - `log_loss`
  - execution-aware backtest acceptance is now wired as a separate report:
    - `execution_acceptance_report.json`
    - current compare policy: `balanced_pareto_execution`
    - current metrics:
      - realized pnl
      - max drawdown
      - fill rate
      - slippage
  - `model_alpha_v1` backtest path now accepts `feature_set=v4` for this research lane
  - paper/live parity for `v4` was the next major gap
- Phase 4 is now in progress:
  - `LiveFeatureProviderV4` is added as a thin composition layer over `LIVE_V3`
  - `LIVE_V3` itself remains stable; no multi-contract mutation was added there
  - current `LIVE_V4` path:
    - reuses the stable `LIVE_V3` base frame
    - applies `spillover + breadth + periodicity + trend-volume + interaction` packs after the v3 build
    - projects only the requested `v4` contract columns
    - if any requested `v4` contract column is missing, it now returns an empty frame with `hard_gate_triggered=true`
    - missing columns are reported as `MISSING_V4_FEATURE_COLUMNS` instead of being silently zero-filled
  - `PaperRunEngine` now accepts:
    - `paper_feature_provider=LIVE_V4`
    - `strategy=model_alpha_v1`
    - `feature_set=v4`
  - `paper alpha` presets now include:
    - `live_v4`
    - `candidate_v4`
    - `offline_v4`
  - runtime now prefers learned selection breadth from the model run itself:
    - each run may write `selection_recommendations.json`
    - runtime `model_alpha_v1` uses the active threshold key's:
      - `recommended_top_pct`
      - `recommended_min_candidates_per_ts`
    - `min_prob` still stays unset and resolves from the model registry threshold
  - runtime presets keep lane-specific breadth numbers only as fallback values when a model run has no recommendation entry:
    - `live_v3/offline_v3`: fallback `top_pct=0.10`, fallback `min_candidates_per_ts=3`
    - `live_v4/candidate_v4/offline_v4`: fallback `top_pct=0.50`, fallback `min_candidates_per_ts=1`
  - `backtest alpha` and `paper alpha` parser choices now allow `--feature-set v4`
  - live service rollout is still intentionally pending
- Phase 5 has started:
  - `candidate_acceptance.ps1` is extracted as the generic acceptance runner
  - `v3_candidate_acceptance.ps1` is now only a thin v3 wrapper
  - `v4_candidate_acceptance.ps1` is added as the v4 wrapper with:
    - `trainer=v4_crypto_cs`
    - `feature_set=v4`
    - `label_set=v2`
    - `candidate_model_ref=latest_candidate_v4`
    - `champion_model_ref=champion_v4`
    - `paper_feature_provider=live_v4`
    - `trainer_evidence_mode=required`
    - acceptance compare profile:
      - `backtest_top_pct=0.5`
      - `backtest_min_prob=0.0`
      - `backtest_min_candidates_per_ts=1`
    - trainer-side execution acceptance now consumes the same fixed compare overrides instead of falling back to baseline `strategy.yaml` selection defaults
  - lane wrappers keep acceptance criteria explicit instead of inheriting generic defaults:
    - shared compare profile for `v3` and `v4`: `paper_final_balanced`, `top_pct=0.50`, `min_prob=0.0`, `min_candidates=1`, `paper_max_fallback_ratio=0.20`, `paper_min_orders_filled=2`, `paper_min_realized_pnl_quote=0.0`
    - lane-specific difference remains only in trainer evidence:
      - `v3`: `trainer_evidence=ignore`
      - `v4`: `trainer_evidence=required`
    - generic `candidate_acceptance.ps1` defaults are aligned to the same compare profile, so direct/manual invocation does not silently fall back to older legacy constants
    - acceptance intentionally does not consume learned runtime selection recommendations; it keeps one fixed breadth profile so candidate vs champion comparison stays constant across retrains
    - promote semantics:
      - backtest = sanity gate only
      - paper soak = final promote gate
      - offline candidate-vs-champion compare remains in the report as informational evidence
  - the `v4` relaxed runtime breadth settings are a temporary bootstrap lane, not a permanent production target:
    - runtime `live_v4/candidate_v4` keeps fallback `top_pct=0.50`, fallback `min_candidates_per_ts=1` until `v4` has roughly `14+` effective days of usable history
  - current result:
    - v4 can use the same `train -> backtest compare -> paper soak -> promote` contract
    - generic acceptance now reads trainer-side `promotion_decision.json` evidence and can require:
      - walk-forward evidence
      - execution acceptance evidence
      - candidate-edge compare outcomes from the trainer lane
    - this prevents the v4 lane from silently falling back to a pure single-window `v3`-style decision
    - server/runtime rollout is still intentionally left on the v3 lane until v4 passes paper evidence
    - current rollout should use a shared `00:10` orchestrator:
      - run the shared daily collection pipeline once
      - rebuild `features_v3` and `features_v4` for the same batch date
      - fan out `v3` and `v4` acceptance in parallel after feature refresh
    - a later `v4`-only timer remains only as a fallback path, not the preferred rollout

## Handoff To T18.3
- T18.2 keeps the alpha / feature / trainer / promotion contracts aligned with recent crypto cross-sectional research.
- Current-market adaptation is intentionally not solved by daily selection retuning.
- That responsibility moves to `T18.3 Operational Runtime Overlay v1`:
  - rolling paper evidence
  - risk multiplier
  - dynamic `max_positions`
  - `price_mode` aggressiveness overlay
  - micro quality composite
- This split is intentional:
  - T18.2 = learned alpha contract
  - T18.3 = runtime operational adaptation

### Ops Hardening Notes
- `install_server_runtime_services.ps1` now exposes:
  - `live_v3`
  - `live_v4`
  - `candidate_v4`
  - `offline_v4`
  as valid paper runtime presets, while keeping the default rollout on `live_v3`
  - when `live_v4` or `offline_v4` is started on a fresh server with no `champion_v4` pointer yet, the installer bootstraps `champion_v4` from `latest_candidate_v4` (or `latest_v4`) before starting the service
- `oci_paper_run_and_pull.cmd` now externalizes:
  - `PAPER_PRESET`
  - `MODEL_REF`
  so ad-hoc OCI paper runs can target v4 without editing the command body
  - the command now defers to `paper alpha --preset` runtime defaults rather than hardcoding `min_prob/top_pct/min_candidates`
- `autobot_center.ps1` now allows:
  - `trainer=v4_crypto_cs`
  - `features build --feature-set v4 --label-set v2`
  from the train wizard path
- `daily_candidate_acceptance_for_server.ps1` is the server wrapper for delayed post-collection acceptance runs
  - intended default for the v4 lane:
    - `SkipDailyPipeline=true`
    - `SkipReportRefresh=true`
    - `BlockOnActiveUnits=autobot-daily-micro.service`
  - reason:
    - reuse the same `00:10` collection outputs without re-running collection or rewriting the shared daily report
- `install_server_daily_acceptance_service.ps1` installs a dedicated timer/service pair for the v4 parallel lane
  - intended default units:
    - `autobot-daily-v4-accept.service`
    - `autobot-daily-v4-accept.timer`
  - intended default schedule:
    - `04:20 KST`
  - rationale:
    - the current `autobot-daily-micro.service` can run until roughly `03:xx`
    - a later timer lowers overlap risk while keeping the same batch date
- `daily_parallel_acceptance_for_server.ps1` is the preferred same-time orchestrator
  - current intended order:
    - shared `daily_micro_pipeline_for_server.ps1`
    - `features build --feature-set v3`
    - `features build --feature-set v4 --label-set v2`
    - parallel launch of `v3_candidate_acceptance.ps1` and `v4_candidate_acceptance.ps1`
  - both acceptance wrappers run with:
    - `SkipDailyPipeline`
    - `SkipReportRefresh`
  - runtime pairing:
    - `v3` lane owns `autobot-paper-alpha.service`
    - `v4` lane owns `autobot-paper-v4.service`
  - reason:
    - same batch date
    - same paper window
    - no duplicated collection/report mutation
    - each lane can promote/restart independently without touching the other lane's always-on paper
  - operational hardening now also includes:
    - the orchestrator reads each child lane's unique `report=` path from stdout before falling back to lane-global `latest.json`
    - `candidate_acceptance.ps1` does the same for `paper_micro_smoke` output so same-lane manual runs do not silently poison paper soak status
    - `paper_micro_smoke.ps1` separates `min_orders_submitted` failure from true fallback-ratio failure instead of treating all zero-order windows as `fallback_ratio=1.0`
    - ad-hoc `paper alpha --preset live_v4` is now safe on a fresh registry: if `champion_v4` is missing, runtime falls back to `latest_candidate_v4`, then `latest_v4`
- `install_server_daily_parallel_acceptance_service.ps1` rewires the existing `autobot-daily-micro.service` override to the shared orchestrator
  - current target timer remains:
    - `autobot-daily-micro.timer`
    - `OnCalendar=*-*-* 00:10:00`
  - also disables `autobot-daily-v4-accept.timer` when the same-time rollout is selected

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
- Compare against `v4` champion on balanced Pareto basis using offline aggregate metrics first
- Add execution-aware backtest acceptance as a separate evidence report, while keeping paper/live parity as a later phase

### Phase 4: Runtime Parity
- Add `LIVE_V4`
- Add paper/backtest preset support
- Keep live service off until paper parity is proven
  - current status:
    - `LIVE_V4` paper provider implemented
    - v4 paper presets implemented
    - live runtime deployment still not started

### Phase 5: Promote and Sunset
- If v4 proves superior and stable:
  - promote v4 candidate to champion lane
  - keep v3 as legacy fallback for one transition period
  - only then consider removal of truly dead compatibility code
  - current status:
    - acceptance runner and wrappers are ready
    - automated server adoption is not enabled yet

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
