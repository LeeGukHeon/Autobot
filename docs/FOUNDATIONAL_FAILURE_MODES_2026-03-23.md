# Foundational Failure Modes 2026-03-23

- Date: 2026-03-23
- Status: current structural diagnosis
- Scope: why the system is unstable even before fine-grained alpha tuning
- Related docs:
  - `docs/PROGRAM_RUNBOOK.md`
  - `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
  - `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

## 1. Why This Document Exists

The current system has an obvious execution problem, but execution is not the whole story.

If we only tune:

- `PASSIVE_MAKER` vs `JOIN`
- fill-rate thresholds
- micro order policy
- trade-action rules

we risk improving one visible symptom while leaving the deeper instability in place.

This document is the structural view.

Its goal is to answer:

1. what the real root problems are
2. which of them are primary causes versus secondary symptoms
3. what evidence supports each claim
4. what order the next work should happen in
5. what a future maintainer should read next without losing context

## 2. Recommended Reading Order

Use this order for the current topic.

1. `docs/README.md`
2. `docs/PROGRAM_RUNBOOK.md`
3. this file
4. `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
5. `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

Then move into code in this order:

1. `autobot/models/train_v4_crypto_cs.py`
2. `autobot/models/predictor.py`
3. `autobot/strategy/model_alpha_v1.py`
4. `autobot/live/model_alpha_runtime.py`
5. `autobot/live/model_alpha_runtime_execute.py`
6. `autobot/models/live_execution_policy.py`
7. `autobot/ops/live_execution_override_audit.py`

## 3. Executive Bottom Line

The system is unstable because four foundational layers are weak at the same time:

1. measurement and governance are not trustworthy enough
2. the training / validation window is too thin to produce stable model selection
3. offline and live feature semantics are not fully aligned
4. the live execution contract is being learned from a small and biased evidence base

Those four layers then produce visible second-order problems:

- missed entries
- breaker escalation
- confusing candidate vs champion interpretation
- unstable realized PnL
- overreaction to short recent history

In short:

- execution is the most visible problem
- but governance, validation depth, feature parity, and evidence quality are the more foundational problems

## 4. Root Cause Stack

The current failure stack is best understood in this order:

1. `measurement failure`
   - we cannot fully trust what "best model" or "champion" means right now
2. `validation fragility`
   - even if the trainer is working correctly, the evidence window is thin
3. `distribution mismatch`
   - live runtime does not always see the same feature semantics as training
4. `execution evidence poverty`
   - the execution contract is trained on too little and too imbalanced live data
5. `runtime suppressors`
   - breakers, rollout, and state-machine noise can distort observed behavior further

Only after those are understood should we spend serious effort on:

- new factors
- new model families
- deeper action-policy tuning

## 5. Detailed Root Causes

### 5.1 Evaluation / Promotion Contract Collapse

#### Why this is foundational

If we do not know whether the current champion is actually better than the candidate, every downstream comparison becomes questionable.

This is the most important measurement problem.

#### Current evidence

- current pointers on server:
  - `latest = latest_candidate = champion = 20260322T093201Z-s42-da19a911`
- current `champion.json` records:
  - `promotion_mode = manual`
- current run had:
  - `trainer_research_evidence.pass = false`
- the same run still became champion operationally

#### Why this is not a small paperwork issue

When:

- `champion`
- `latest`
- `latest_candidate`

all point to the same run, then:

- candidate vs champion comparison loses meaning
- backtest / paper / execution acceptance become harder to interpret
- any later runtime loss can be misread as "model underperformed" rather than "reference point was never properly validated"

#### Primary consequences

- unclear benchmark
- weak promotion trust
- contaminated measurement of progress

#### What has to be fixed

1. separate `manual operational champion` from `research-evidence-passing champion`
2. preserve original trainer evidence even after manual promotion
3. stop letting pointer state imply research validity

### 5.2 Training / Validation Window Is Too Thin

#### Why this is foundational

A model can look unstable simply because the evaluation window is too short and too noisy.

This is a structural variance problem, not necessarily a model-quality problem.

#### Current evidence

- recent strict split policy was effectively:
  - `17 days train`
  - `1 day certification`
- earlier runs fell into:
  - `bootstrap-only`
  - `insufficient contiguous micro history`
- the system has already shown:
  - window ramping
  - bootstrap fallback behavior
  - certification windows that are very short

#### Why this matters

When the holdout is that short:

- one bad day dominates the conclusion
- one venue-specific micro anomaly can swing selection
- walk-forward evidence exists, but the actual usable recent lane still remains thin

This makes:

- threshold choice unstable
- promotion evidence fragile
- runtime recommendation search noisy

#### Primary consequences

- unstable leaderboard interpretation
- high variance in selected thresholds / policies
- easy overreaction to recent noise

#### What has to be fixed

1. widen the admissible evaluation regime when possible
2. keep split-policy history and current production pointers isolated
3. treat short-window evidence as low-confidence evidence, not final truth

### 5.3 Offline-Live Feature Drift

#### Why this is foundational

Even a correct model will degrade if the runtime feature distribution is not the same distribution it was trained on.

#### Current evidence

Offline:

- `v4` training drops rows without mandatory micro
- label and feature building assume a validated offline `micro_v1` contract

Runtime:

- live / paper micro overlay is not the same thing as offline `micro_v1`
- some runtime paths still use zero-fill behavior for missing feature values
- historical analysis already flagged:
  - `rv_12`
  - `rv_36`
  - `atr_pct_14`
  - micro/order-flow columns
  as likely drift-sensitive inputs

#### Why this matters

The following runtime layers all depend on those values:

- `selection_calibration`
- `trade_action_policy`
- `execution_risk_control`
- `operational overlay`

So one drift source can distort:

- ranking
- notional scaling
- action family choice
- risk gating

#### Primary consequences

- model score calibration drift
- wrong bin assignment in trade-action logic
- misleading risk-control thresholds
- apparent "execution underperformance" that is actually feature misalignment

#### What has to be fixed

1. audit live feature distributions against training distributions
2. quantify zero-filled runtime feature frequency
3. explicitly compare offline and runtime semantics for core state features

### 5.4 Execution Contract Sample Scarcity And Bias

#### Why this is foundational

The execution contract is only as good as the live evidence it was built from.
If the evidence is sparse, imbalanced, or self-reinforcing, the execution selector will become biased.

#### Current evidence

Current shared execution contract on server is effectively derived from:

- `169` recent final execution attempts

Observed action sample sizes:

- `LIMIT_GTC_PASSIVE_MAKER = 120`
- `LIMIT_GTC_JOIN = 48`
- `BEST_IOC = 1`

Observed submit mix:

- final submit `PASSIVE_MAKER = 120`
- final submit `JOIN = 48`
- final submit `CROSS_1T = 1`

Observed live execution override summary:

- run recommendation was `JOIN`
- match rate was only about `28.4%`
- `execution_policy` directly demoted `JOIN -> PASSIVE_MAKER` in `30` cases

#### Why this matters

With this evidence base:

- state-action estimates are noisy
- aggressive actions are under-sampled
- the policy can become self-reinforcing:
  - choose maker often
  - gather more maker data
  - continue believing maker is dominant

That is a structural data-generation problem.

#### Primary consequences

- execution policy maker bias
- weak confidence in action-level utility
- underexplored aggressive execution states

#### What has to be fixed

1. keep collecting fresh post-deploy live attempts
2. make miss-cost more state-sensitive for strong-edge signals
3. reduce self-reinforcing maker bias in canary mode
4. improve action-set coverage and confidence diagnostics

### 5.5 Risk-Control Asymmetry

#### Why this is foundational

The current risk-control system is not symmetrical.

It is strong in post-loss suppression, but weaker as a pre-submit protective gate.

#### Current evidence

Current runtime risk-control state:

- `operating_mode = safety_executor_only_v1`
- `live_gate.enabled = false`
- `online_adaptation.enabled = true`
- online halt logic is active

Observed breaker reasons include:

- `RISK_CONTROL_ONLINE_BREACH_STREAK`
- `RISK_CONTROL_MARTINGALE_EVIDENCE`

#### Why this matters

This means the system often behaves like:

1. allow risky or marginal behavior
2. wait for enough bad outcomes
3. then halt strongly

That creates a trader experience of:

- weak prevention
- strong post-damage braking

#### Primary consequences

- bad user-facing loss clustering
- late intervention
- confusing interpretation of whether risk-control is "working"

#### What has to be fixed

1. decide whether live gate should remain disabled by design
2. if yes, explain and instrument that as an explicit policy choice
3. if no, move part of online evidence back into pre-submit gating

### 5.6 State-Machine / Operational Noise

#### Why this is foundational

Even a good model and execution policy will look bad if the runtime keeps tripping over continuity issues.

#### Current evidence

Observed recurring breaker/event reasons included:

- `MODEL_POINTER_UNRESOLVED`
- `MODEL_POINTER_DIVERGENCE`
- `LOCAL_POSITION_MISSING_ON_EXCHANGE`
- `LOCAL_OPEN_ORDER_NOT_FOUND_ON_EXCHANGE`
- `RISK_EXIT_STUCK_MAX_REPLACES`
- `LIVE_PUBLIC_WS_STREAM_FAILED`
- `WS_PUBLIC_STALE`
- `SMALL_ACCOUNT_CANARY_MULTIPLE_ACTIVE_MARKETS`

Observed current service noise:

- `autobot-v4-rank-shadow.service`: failed
- `autobot-live-alpha-replay-shadow.service`: failed
- `autobot-v4-challenger-spawn.service`: sometimes still activating when inspected

#### Why this matters

This class of issue creates:

- duplicate or missing lifecycle evidence
- unnecessary breaker arming
- runtime pauses that are not alpha failures
- hard-to-read live performance

#### Primary consequences

- false interpretation of model quality
- polluted execution evidence
- intermittent suppression of new intents

#### What has to be fixed

1. continue reducing lifecycle drift between runtime, reconcile, and breakers
2. keep rollout / breaker / runtime checkpoints as the true status source
3. treat state-machine noise as performance contamination, not just ops annoyance

### 5.7 Selection / Sizing Concentration

#### Why this is foundational

Concentrated selection and aggressive notional scaling amplify every upstream mistake.

#### Current evidence

Current champion run:

- `selection_fraction ≈ 0.025`
- `min_candidates_per_ts = 1`

Recent live evidence:

- many losing trades show positive expected edge
- many recent cases use `notional_multiplier = 1.5`

#### Why this matters

This means:

- the system may choose a very narrow candidate set
- and then size the chosen idea aggressively

So any error in:

- score calibration
- action selection
- execution policy
- runtime feature state

can become a larger realized loss than it otherwise would.

#### Primary consequences

- PnL volatility amplification
- concentration of error
- fragile behavior in noisy regimes

#### What has to be fixed

1. revisit whether current selection fraction is too tight for current evidence depth
2. measure realized PnL by notional multiplier bucket
3. reduce aggressive scaling until execution evidence quality improves

## 6. Primary Versus Secondary Causes

Primary causes:

1. evaluation / promotion contract collapse
2. thin training / validation windows
3. offline-live feature drift
4. execution contract sample scarcity and bias

Secondary but still important causes:

1. risk-control asymmetry
2. state-machine / operational noise
3. selection / sizing concentration

Visible symptoms:

- PASSIVE_MAKER overuse
- missed entries
- breaker storms
- noisy realized losses
- unstable performance impression

## 7. Dependency Graph

The causal chain is roughly:

1. weak governance and weak validation depth
   -> bad confidence about what the "best" run is
2. feature drift between offline and live
   -> distorted runtime scores and policies
3. sparse and biased execution evidence
   -> execution policy favors the wrong stage too often
4. concentrated selection and sizing
   -> small errors become meaningful losses
5. online risk-control and breaker noise
   -> system halts after damage instead of preventing it cleanly

This is why the system can feel chaotic even when individual modules are "working."

## 8. What Not To Do Yet

Do not do these first:

1. switch model family just because recent realized PnL is poor
2. add many new features before measuring live drift
3. retune candidate generation blindly without fixing benchmark trust
4. overreact to one recent runtime slice without strengthening validation interpretation

These may produce more movement, but they are unlikely to solve the structural instability.

## 9. Correct Priority Order

### Step 1: Post-Deploy Execution Trace Collection

Let the newly deployed candidate runtime collect fresh attempts.

Goal:

- confirm that the new `execution_trace` is actually populated
- confirm whether `CANARY_STRONG_EDGE_STAGE_ESCALATION` appears in live operation
- measure whether:
  - maker share falls
  - miss rate falls
  - positive-edge missed attempts fall

### Step 2: Offline-Live Feature Drift Audit

Target:

- compare live runtime feature distributions against training distributions

Highest-priority columns:

- `rv_12`
- `rv_36`
- `atr_pct_14`
- key micro columns
- order-flow columns

Goal:

- determine whether runtime is operating in a different feature world than offline training

### Step 3: Champion / Promotion Contract Repair

Target:

- clean separation between:
  - manual operational champion
  - evidence-passing champion

Goal:

- restore trust in comparisons and promotion meaning

### Step 4: Execution Contract Rework

Target:

- improve miss-cost and urgency modeling
- reduce self-reinforcing maker bias

Goal:

- better conversion of modeled edge into realized edge

### Step 5: Only Then Revisit Alpha / Candidate Tuning

Once the previous four are cleaner:

- revisit selection fraction
- revisit notional multiplier policy
- revisit factor and candidate-generation tuning

## 10. Handoff For Next Context

If another model or maintainer picks this up, the next context should start here:

### Must-read docs

1. `docs/PROGRAM_RUNBOOK.md`
2. this file
3. `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`
4. `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`

### Must-read code

1. `autobot/live/model_alpha_runtime_execute.py`
2. `autobot/models/live_execution_policy.py`
3. `autobot/ops/live_execution_override_audit.py`
4. `autobot/strategy/model_alpha_v1.py`
5. `autobot/models/train_v4_crypto_cs.py`

### Current operating assumptions to preserve

1. the current champion is not necessarily a research-clean champion
2. execution is the first live debugging surface
3. feature drift is still unmeasured and therefore still dangerous
4. do not treat current execution-policy artifact as statistically mature

### Most useful commands

```bash
cd /home/ubuntu/MyApps/Autobot
python -m autobot.cli live rollout status
python -m autobot.cli live breaker status
python scripts/report_live_execution_override_audit.py --db-path data/state/live_candidate/live_state.db --registry-root models/registry --model-family train_v4_crypto_cs --output-dir logs/live_execution_override_audit --print-json
```

### Main next question

The next context should answer this exact question:

- after the 2026-03-23 canary execution-policy patch, do fresh live attempts show lower maker bias and lower positive-edge miss rate?

If yes:

- move to offline-live feature drift audit

If no:

- continue inside `autobot/models/live_execution_policy.py`
- improve miss-cost and urgency handling before touching alpha

## 11. Short Final Summary

The system is unstable because:

1. its benchmark / champion semantics are partially broken
2. its validation depth is thin
3. its runtime feature distribution may not match offline training
4. its execution contract is learned from limited and biased evidence

Everything else is downstream from that.

So the correct next order is:

1. collect fresh post-deploy execution evidence
2. run offline-live feature drift audit
3. repair promotion / benchmark semantics
4. only then retune alpha and candidate generation
