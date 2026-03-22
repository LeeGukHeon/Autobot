# Runtime Execution Findings And Action Plan 2026-03-23

- Date: 2026-03-23
- Scope: `v4` train/selection/runtime pipeline, live candidate execution behavior, execution-policy deployment, next action plan
- Related docs:
  - `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
  - `docs/LIVE_EXECUTION_PARITY_REDESIGN.md`
  - `logs/live_execution_override_audit_remote/latest.md`

## 1. Executive Summary

The current production-style problem is not best described as:

- "the alpha model is simply bad"

It is better described as:

- the research and runtime contracts are partially misaligned
- the live execution stack still converts too little modeled edge into realized edge
- promotion semantics and research-evidence semantics are not the same thing
- candidate live has accumulated enough negative realized evidence that execution must be treated as the first debugging surface

The most important current conclusion is:

- the live execution path diverges from the run-level recommendation before orders reach the exchange
- and, even when expected edge is positive, realized conversion is too weak or negative too often

## 2. What We Proved

### 2.1 Run-level recommendation and live behavior diverge

The active server run is:

- `20260322T093201Z-s42-da19a911`

Its registry artifacts say:

- `runtime_recommendations.execution.recommended_price_mode = JOIN`
- `runtime_recommendations.execution.recommended_timeout_bars = 2`
- `runtime_recommendations.execution.recommended_replace_max = 1`

But live candidate execution evidence shows:

- final execution attempts: `169`
- filled: `119`
- missed: `50`
- final submit `PASSIVE_MAKER`: `120`
- final submit `JOIN`: `48`
- final submit `CROSS_1T`: `1`
- run recommendation match rate: about `28.4%`

This means the live candidate runtime is not mostly expressing the run-level `JOIN` recommendation.

### 2.2 The main suppressor is not only `micro_order_policy`

We specifically investigated whether:

- `micro_order_policy`
- `operational overlay`
- `execution_policy`

was pushing the runtime into maker behavior.

For the recent observed sample:

- `operational_demote_to_passive_maker_count = 0`
- `micro_policy_demote_to_passive_maker_count = 0`
- `execution_policy_demote_to_passive_maker_count = 30`

Interpretation:

- in the current evidence window, the strongest direct demotion we could prove came from `live_execution_policy`
- not from `micro_order_policy`
- not from the operational overlay

This does **not** prove those layers never matter.
It proves that the recent detected and recoverable divergence is already happening inside the execution-policy selector itself.

### 2.3 Positive expected edge often does not become positive realized PnL

Recent candidate live evidence:

- `trade_journal.CLOSED = 120`
- average realized pnl pct: about `-0.0868%`
- total realized pnl quote: about `-759.98`
- `CANCELLED_ENTRY = 49`

Execution-attempt diagnostics:

- `positive_expected_net_edge_closed_losses = 62`
- `positive_expected_net_edge_missed_attempts = 48`

Interpretation:

- many trades or missed entries started from positive modeled edge
- but execution and/or exit conversion was weak enough that realized outcomes still lost money

This is the core practical failure mode.

### 2.4 Breaker and rollout states distort observed behavior

Before the latest deploy, rollout often had:

- `order_emission_allowed = false`
- `start_allowed = false`

Recent breaker reasons in the audit included:

- `MODEL_POINTER_UNRESOLVED`
- `SMALL_ACCOUNT_CANARY_MULTIPLE_ACTIVE_MARKETS`
- `RISK_EXIT_STUCK_MAX_REPLACES`
- `LOCAL_POSITION_MISSING_ON_EXCHANGE`
- `RISK_CONTROL_ONLINE_BREACH_STREAK`
- `LIVE_PUBLIC_WS_STREAM_FAILED`

Interpretation:

- some observed runtime inactivity is not pure strategy inactivity
- parts of it are breaker/rollout suppression
- this has to be considered when reading live performance or "no trades" periods

## 3. Governance Conclusion

The current champion run is operationally champion, but not research-clean champion.

Why:

- `trainer_research_evidence.pass = false`
- `promotion_decision.promotion_mode = manual`
- the run was later advanced to champion through manual promotion semantics

So the current champion should be read as:

- "currently deployed champion"

not as:

- "fully evidence-passing champion under the intended trainer governance contract"

This distinction matters because otherwise we will over-trust the current reference point.

## 4. What We Changed

### 4.1 Added execution override audit tooling

New files:

- `autobot/ops/live_execution_override_audit.py`
- `scripts/report_live_execution_override_audit.py`
- `tests/test_live_execution_override_audit.py`

Purpose:

- summarize run recommendation versus actual live action
- quantify mode-level fill/miss behavior
- quantify realized PnL by execution mode
- summarize breaker and rollout state
- preserve a reusable audit path for future regression checks

### 4.2 Added execution attempt outcome preservation

Updated:

- `autobot/live/execution_attempts.py`

Purpose:

- stop losing submission-stage metadata when later updates overwrite the same execution attempt
- preserve execution decision context across:
  - submit
  - ws update
  - fill
  - cancel
  - rebind

This is necessary for long-lived runtime forensics.

### 4.3 Added richer trade-journal entry summaries

Updated:

- `autobot/live/trade_journal.py`

Purpose:

- preserve:
  - `execution_policy`
  - `micro_order_policy`
  - `micro_diagnostics`
  - `operational_overlay`
  - `execution_trace`

inside `entry_meta_json`

This makes post-trade forensics possible even when execution attempts are incomplete or when we inspect historical rows after the fact.

### 4.4 Added execution trace instrumentation in live submit path

Updated:

- `autobot/live/model_alpha_runtime_execute.py`

Purpose:

- record step-by-step execution posture evolution:
  - run-level recommended price mode
  - initial strategy exec profile
  - post-operational overlay profile
  - post-canary timeout cap profile
  - post-micro-order-policy profile
  - execution-policy-selected action/mode
  - final submit mode and order type

This is the direct instrumentation needed to answer:

- "who changed JOIN into PASSIVE_MAKER?"

### 4.5 Tuned live execution policy for strong-edge canary cases

Updated:

- `autobot/models/live_execution_policy.py`

New behavior:

- when rollout mode is `canary`
- and expected edge is strong enough
- and the selector would otherwise choose `PASSIVE_MAKER`
- but a more aggressive `JOIN` or `CROSS_1T` option has positive utility and is not too far behind

then the selector may escalate to the faster stage.

Current constants:

- `CANARY_STRONG_EDGE_THRESHOLD_BPS = 50.0`
- `CANARY_STRONG_EDGE_UTILITY_MARGIN_BPS = 20.0`

Selection reason code:

- `CANARY_STRONG_EDGE_STAGE_ESCALATION`

This is intentionally conservative.
It does not force aggressive execution everywhere.
It only reduces maker bias in high-edge canary situations where the utility gap is still small enough.

## 5. Deployment Status

Deployed commit:

- `017b177383798993abe084084278505ca26ced27`

Git status:

- pushed to `origin/main`
- pulled on OCI

Services restarted after deploy:

- `autobot-live-alpha-candidate.service`
- `autobot-paper-v4.service`
- `autobot-paper-v4-replay.service`

Observed post-restart state:

- candidate live: `active (running)`
- paper v4: `active (running)`
- paper v4 replay: `active (running)`

Observed rollout status after restart:

- `breaker_clear = true`
- `start_allowed = true`
- `order_emission_allowed = true`

This means the new code is now loaded into the live candidate runtime.

## 6. What The New Policy Would Change On Historical Data

Using the current server execution contract and replaying recent stored candidate cases:

- rows inspected: `169`
- rows where old selected mode was explicitly `PASSIVE_MAKER`: `27`
- rows among those that would switch to `JOIN` under the new logic: `5`

Meaning:

- the patch is not a wholesale re-architecture
- it is a targeted de-biasing of very strong-edge canary cases
- it should reduce missed high-conviction passive entries
- but it will not fix the whole execution problem by itself

Examples of switched cases:

- expected edge around `110 - 118 bps`
- previous selected mode: `PASSIVE_MAKER`
- new selected mode: `JOIN`
- reason: `CANARY_STRONG_EDGE_STAGE_ESCALATION`

## 7. What This Does Not Yet Solve

### 7.1 The execution contract itself is still maker-biased

The server-side combined execution contract currently reports:

- `best_stage_by_fill_probability = JOIN`
- `best_stage_by_objective = JOIN`
- `best_stage_by_time_to_fill = PASSIVE_MAKER`

and yet many actual selected cases still end up maker-side.

This suggests one or more of:

1. candidate action sets are still too constrained by incoming base profiles
2. state-bucket coverage is sparse in important regions
3. the current miss-cost model is not punitive enough in the most harmful missed-entry situations
4. the utility proxy still underprices missed opportunity cost for strong-edge signals

### 7.2 Historical rows do not yet contain the new full execution trace

The new `execution_trace` instrumentation is in code now, but:

- historical rows were created before the deploy
- so recent audit fallback still depends on `entry_meta_json` summaries rather than true step-by-step trace

We need fresh live attempts after the deploy to fully benefit from the new trace.

### 7.3 Paper/live parity is still not solved

Paper and backtest already read:

- `logs/live_execution_policy/combined_live_execution_policy.json`

But this does not fully close parity because:

1. paper fill semantics remain simulated
2. queue effects and missed-entry economics are still approximate
3. live uses additional continuity/breaker/state-machine paths that paper does not

So parity is improved by shared contract use, but not finished.

## 8. Current Best Interpretation

The most accurate current interpretation is:

1. alpha direction is not obviously wrong
2. execution conversion is too weak
3. the live execution policy was itself too maker-friendly in recent canary operation
4. the new patch reduces that bias in a narrow but important subset
5. we now need new post-deploy evidence to see whether the miss loop actually falls

In short:

- we have moved from speculation to measurable execution-path diagnosis
- but we have not yet completed the execution redesign

## 9. Immediate Next Tasks

### Priority A: Collect post-deploy live evidence

We should let the newly deployed candidate runtime accumulate fresh attempts and then rerun:

- `scripts/report_live_execution_override_audit.py`

Goal:

- confirm whether new trace fields are populated
- confirm whether `CANARY_STRONG_EDGE_STAGE_ESCALATION` actually appears in real submit records
- check whether:
  - maker share drops
  - miss rate drops
  - positive-edge missed attempts drop

### Priority B: Improve the execution contract itself

Main next target:

- `autobot/models/live_execution_policy.py`

Likely work:

1. make miss-cost more state-sensitive for strong-edge signals
2. penalize slow fill paths more directly when canary capital is small
3. consider a stronger urgency rule for:
   - `edge_strong`
   - fresh micro
   - acceptable spread
   - adequate depth
4. inspect whether action-set generation from `candidate_action_codes_for_price_mode()` is still too restrictive

### Priority C: Surface stage-trace metrics in reports

Main targets:

- `autobot/ops/live_execution_override_audit.py`
- dashboard/reporting paths

Desired metrics:

- stage-level mode transition counts
- `JOIN -> PASSIVE_MAKER` versus `PASSIVE_MAKER -> JOIN`
- utility gap between old and overridden action
- realized PnL by:
  - selected action code
  - selected reason code
  - urgency override on/off

### Priority D: Refresh paper/live parity diagnostics

Main targets:

- `autobot/paper/engine.py`
- `autobot/backtest/engine.py`

Desired work:

1. emit the same execution-policy decision summary into paper/backtest events
2. compare:
   - selected mode distribution
   - selected utility
   - miss-cost assumptions
3. identify where paper still benefits from simulation optimism even while sharing the same policy artifact

## 10. Medium-Term Tasks

### 10.1 Promotion contract hardening

Main target:

- separate manual operational championing from research-evidence-passing champion status

Why:

- current champion status can otherwise mislead future analysis

### 10.2 Runtime feature parity audit

Main target:

- compare offline feature distribution vs live runtime feature distribution for:
  - `rv_12`
  - `rv_36`
  - `atr_pct_14`
  - micro/order-flow columns

Why:

- if runtime values are materially different from training values, execution tuning alone will not solve the full problem

### 10.3 Shared execution-evidence governance

Main target:

- make train, backtest, paper, and live all point at the same explainable execution contract evidence lineage

Why:

- current evidence flow is still good but not yet single-source-of-truth clean

## 11. Concrete Commands

### Local tests

```powershell
python -m pytest tests/test_live_execution_policy.py tests/test_live_execution_override_audit.py -q
```

### Local audit against a local DB

```powershell
python scripts/report_live_execution_override_audit.py `
  --db-path data/state/live_candidate/live_state.db `
  --registry-root models/registry `
  --model-family train_v4_crypto_cs `
  --output-dir logs/live_execution_override_audit `
  --print-json
```

### Server audit

```bash
cd /home/ubuntu/MyApps/Autobot
PYTHONPATH=/home/ubuntu/MyApps/Autobot .venv/bin/python scripts/report_live_execution_override_audit.py \
  --db-path data/state/live_candidate/live_state.db \
  --registry-root models/registry \
  --model-family train_v4_crypto_cs \
  --output-dir logs/live_execution_override_audit \
  --print-json
```

### Refresh shared execution contract artifact on server

```bash
cd /home/ubuntu/MyApps/Autobot
/snap/powershell/current/opt/powershell/pwsh -NoProfile -ExecutionPolicy Bypass \
  -File scripts/refresh_live_execution_policy.ps1 \
  -ProjectRoot /home/ubuntu/MyApps/Autobot \
  -PythonExe /home/ubuntu/MyApps/Autobot/.venv/bin/python
```

### Service restart after code deploy

```bash
sudo systemctl restart autobot-live-alpha-candidate.service
sudo systemctl restart autobot-paper-v4.service
sudo systemctl restart autobot-paper-v4-replay.service
```

## 12. Final Working Conclusion

As of 2026-03-23, the best working conclusion is:

- the system’s biggest current weakness is execution conversion, not raw alpha direction
- the live execution policy itself was already part of the maker bias problem
- we now have:
  - an audit path
  - deployed code changes
  - a small but real reduction in canary maker bias for strong-edge cases

The next decision should be made only after:

1. collecting fresh post-deploy live attempts
2. rerunning the audit
3. checking whether miss rate and positive-edge wasted-entry rate actually improve

If those do not improve enough, the next target should be:

- the utility and miss-cost formulation inside `live_execution_policy`

before touching the alpha model again.
