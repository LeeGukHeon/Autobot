# Live Runtime Postmortem 2026-03-21

- Status: incident postmortem
- Operational authority: no
- Current runtime truth:
  - `docs/PROGRAM_RUNBOOK.md`
  - `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

- Date: 2026-03-21
- Scope: `live` / `paper` / `reconcile` / `risk` / `execution` incidents observed after the March 20 refactor and runtime-policy changes
- Goal: map each observed production issue to its likely introduction point, recovery fix, and remaining contract gap

## 1. Executive Summary

The recent failures were not one single bug.

They were a cluster of state-machine contract gaps exposed after:

1. live execution policy and execution-contract paths were added
2. live startup / recovery logic was split and refactored
3. private WS, closed-order backfill, trade journal, reconcile, and breaker layers started depending on each other more tightly

The common pattern was:

- one layer recorded enough evidence to prove the lifecycle had advanced
- but another layer did not recognize that evidence
- the mismatch then armed a breaker or crashed the runtime

In short:

- this was mostly a `cross-module contract drift` problem
- not a single isolated algorithmic failure
- and not purely a ?쐋egacy cleanup deleted required code??problem

That said, the cleanup/refactor period did make the drift easier to trigger because responsibility became more distributed.

## 2. Incident Map

### 2.1 `RISK_EXIT_STUCK_MAX_REPLACES`

- Symptom:
  - protective exit order already filled
  - risk plan already effectively closed
  - breaker reason persisted and continued to block new intents
- Why it happened:
  - `risk_manager` armed the breaker when replace budget was exhausted
  - later `risk_closed` / projection close paths did not clear that reason
- Evidence path that existed but was not consumed:
  - `orders.state=done`
  - `risk_plans.state=CLOSED`
- Fix:
  - [breakers.py](d:/MyApps/Autobot/autobot/live/breakers.py)
  - [live_risk_manager.py](d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
  - [model_alpha_projection.py](d:/MyApps/Autobot/autobot/live/model_alpha_projection.py)
  - commit `34d33bf`

### 2.2 `LOCAL_POSITION_MISSING_ON_EXCHANGE`

- Symptom:
  - exchange position already gone after exit
  - local `positions` row remained
  - sync cycle armed `LOCAL_POSITION_MISSING_ON_EXCHANGE`
- Why it happened:
  - reconcile recognized:
    - done ask orders
    - missing-on-exchange-after-exit-plan
  - but did not recognize:
    - already `close_verified=true` trade journal rows
- Evidence path that existed but was not consumed:
  - `trade_journal.status=CLOSED`
  - `exit_meta.close_verified=true`
  - `close_mode=managed_exit_order`
- Fix:
  - [reconcile.py](d:/MyApps/Autobot/autobot/live/reconcile.py)
  - commit `18cde26`

### 2.3 `LIVE_PUBLIC_WS_STREAM_FAILED` on malformed public events

- Symptom:
  - canary runtime stopped with `float(None)` style errors
  - breaker reason said public WS stream failed
- Why it happened:
  - parser-normalized paths were not the only consumers
  - multiple runtime consumers still assumed numeric ticker/trade fields could never be `None`
- Paths that needed hardening:
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
  - [paper/engine.py](d:/MyApps/Autobot/autobot/paper/engine.py)
  - [top20_scanner.py](d:/MyApps/Autobot/autobot/strategy/top20_scanner.py)
  - [candidates_v1.py](d:/MyApps/Autobot/autobot/strategy/candidates_v1.py)
- Fixes:
  - `9600467` `Ignore malformed public ws micro events`
  - `e55b3ca` `Guard malformed public ticker events`

### 2.4 `LIVE_PUBLIC_WS_STREAM_FAILED` stale reason

- Symptom:
  - service was healthy again
  - stale public-WS failure reason still remained in breaker state
- Why it happened:
  - runtime recovery cleared `WS_PUBLIC_STALE`
  - but not `LIVE_PUBLIC_WS_STREAM_FAILED`
- Fix:
  - [daemon.py](d:/MyApps/Autobot/autobot/live/daemon.py)
  - commit `25ff9ec`

### 2.5 Misclassified runtime crashes as WS failures

- Symptom:
  - breaker reason said `LIVE_PUBLIC_WS_STREAM_FAILED`
  - but traceback pointed to non-WS code paths
- Confirmed example:
  - best-order submit crashed in direct gateway, but runtime still armed the WS failure reason
- Why it happened:
  - broad `except Exception` in live runtime mapped every loop failure to the same WS-specific reason code
- Fix:
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
  - [daemon.py](d:/MyApps/Autobot/autobot/live/daemon.py)
  - [breakers.py](d:/MyApps/Autobot/autobot/live/breakers.py)
  - commit pending in current investigation branch when this report was written
  - reason split:
    - real WS path: `LIVE_PUBLIC_WS_STREAM_FAILED`
    - generic loop path: `LIVE_RUNTIME_LOOP_FAILED`

### 2.6 Best-order direct gateway crash

- Symptom:
  - runtime died during submit
  - traceback:
    - `direct_gateway._format_decimal(intent.volume)`
    - `intent.volume is None`
- Why it happened:
  - `best bid` orders are valid with:
    - `price` set
    - `volume=None`
  - direct REST gateway still serialized both fields as mandatory decimals
- Fix:
  - [direct_gateway.py](d:/MyApps/Autobot/autobot/execution/direct_gateway.py)
  - commit `9117d36`

### 2.7 Online risk-control percentage-unit mismatch

- Symptom:
  - live online risk-control halt thresholds behaved harsher than intended
- Why it happened:
  - `trade_journal.realized_pnl_pct` is stored in percentage points
    - for example `-1.15`
  - online risk-control severe threshold uses decimal-return semantics
    - for example `0.01 == 1%`
  - live adaptation compared these directly without dividing by `100`
- Fix:
  - [model_alpha_runtime_execute.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)
  - commit `8f258e5`

### 2.8 Execution-contract parity regressions

- Symptom:
  - certification / backtest windows suddenly produced `fills=0`
  - or runtime evidence was skipped unexpectedly
- Why it happened:
  - execution-contract path entered backtest/paper/live simultaneously
  - acceptance presets still assumed learned execution was off
  - one backtest branch used `snapshot` before initialization
- Fixes:
  - `eec6d48` `Fix backtest execution contract snapshot init`
  - `948f149` `Skip execution contract when learned execution is off`

### 2.9 Skip-train acceptance window drift

- Symptom:
  - acceptance-only rerun failed for certification overlap even though the original run had passed
- Why it happened:
  - rerun reused candidate run id
  - but recomputed split-policy windows instead of reusing the original decision surface
- Fix:
  - `b025380` `Reuse split policy windows in skip-train acceptance`

## 3. Why These Bugs Clustered

The cluster formed because the live lifecycle is now spread across:

- [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
- [model_alpha_runtime_execute.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)
- [ws_handlers.py](d:/MyApps/Autobot/autobot/live/ws_handlers.py)
- [trade_journal.py](d:/MyApps/Autobot/autobot/live/trade_journal.py)
- [reconcile.py](d:/MyApps/Autobot/autobot/live/reconcile.py)
- [live_risk_manager.py](d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- [breakers.py](d:/MyApps/Autobot/autobot/live/breakers.py)
- [direct_gateway.py](d:/MyApps/Autobot/autobot/execution/direct_gateway.py)

The split itself is not wrong, but the contracts between them were under-specified.

The missing contracts were mostly of this form:

1. which artifact counts as ?쐏osition closed??2. which artifact counts as ?쐃xit complete??3. which layer is responsible for clearing which breaker
4. which layer owns malformed-input defense
5. which exception reason should be armed for a crash

## 4. Current Strongest Root-Cause Pattern

The strongest pattern is:

`One module writes lifecycle truth, another module still waits for a different artifact.`

Examples:

- journal said closed, reconcile still wanted done ask order
- order said done, breaker still wanted explicit risk-clear path
- runtime had already recovered, breaker still wanted explicit stale-reason clear
- order intent was valid for `best` ord_type, direct gateway still wanted both `price` and `volume`

## 5. What Was Not Primarily Caused By Legacy Cleanup

The following were not mainly caused by `candidate_acceptance` / wrapper modularization:

- direct gateway best-order crash
- malformed public ticker consumers
- reconcile missing verified-close evidence
- stale breaker clear paths
- online risk-control unit mismatch

Those incidents lived mostly in the live/runtime/execution surface.

## 6. Remaining Risk After Current Fixes

The following risk still remains even after the hotfixes:

1. generic state-machine ownership is still distributed across too many files
2. runtime crash classification can still be confusing unless every broad-except path is categorized consistently
3. `myAsset` is still the fastest way to delete positions, while reconcile remains the fallback repair path
4. canary profitability is still weak on recent runs, independent of the lifecycle fixes

## 7. Recommended Next Structural Work

### 7.1 Freeze a lifecycle contract table

Create one authoritative contract for:

- order accepted
- entry filled
- exit submitted
- exit verified
- position deleted
- risk plan closed
- breaker armed
- breaker cleared

and list:

- source artifact
- consuming module
- fallback artifact

### 7.2 Add transition tests, not only unit tests

The highest-value tests are multi-step transition tests:

- `private ws order done -> journal close -> position delete -> breaker clear`
- `closed order backfill -> reconcile -> local stale position removed`
- `best bid submit -> direct gateway REST payload omits volume`
- `runtime recover -> stale loop/WS breaker cleared`

### 7.3 Reduce semantic drift in reason codes

Runtime should distinguish:

- real WS stream failure
- generic runtime loop failure
- sync-cycle exchange inconsistency

Those should not share one reason code.

## 8. Bottom Line

The system was not failing because ?쐔he websocket had no data.??
It was failing because:

- malformed or partial inputs were not defended everywhere
- close evidence was not recognized consistently
- breakers were not always cleared by the layer that had the recovery evidence
- one runtime exception class was being mislabeled as another

This was a state-machine contract problem first, and only secondarily a runtime strategy problem.

