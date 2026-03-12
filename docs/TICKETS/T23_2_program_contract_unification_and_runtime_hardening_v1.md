# T23.2 - Program Contract Unification and Runtime Hardening v1

## Goal

Refactor and harden the program structure without breaking current operation.

This ticket exists because the system is no longer blocked mainly by model ideas or data volume. The main risk has shifted to:

- duplicated runtime contracts
- oversized core modules
- hidden coupling between training, runtime, dashboard, and live recovery
- operational fragility if a large refactor is done carelessly

This is therefore a **high-risk, compatibility-constrained hardening ticket**.

It is not a generic cleanup ticket.

## Why This Is Dangerous

This refactor can easily break:

- server systemd units
- scheduled daily training / acceptance / promotion scripts
- candidate and champion pointer resolution
- paper/live runtime startup
- restart-safe reconcile and risk resumption
- dashboard visibility
- artifact consumers across backtest / paper / live

If done carelessly, the result can be:

- scheduler jobs silently failing
- runtime loading the wrong model pointer
- live restarting into a contract mismatch
- journal / risk plan / dashboard divergence
- promotion scripts promoting but runtimes not rebinding correctly

So the ticket must explicitly optimize for **external contract stability first, internal cleanliness second**.

## Core Principle

External behavior must not change during the initial slices.

The first implementation phase is:

- behavior-preserving refactor
- additive contract hardening
- improved observability

It is **not**:

- CLI redesign
- artifact redesign without compatibility layer
- service renaming
- schema-breaking migration

## Protected External Contracts

The following are treated as frozen contracts unless a dedicated migration sub-ticket explicitly changes them.

### 1. CLI Contracts

These command families must continue to work with the same top-level names and expected argument behavior:

- `python -m autobot.cli model train --trainer v4_crypto_cs ...`
- `python -m autobot.cli model daily-v4 --mode spawn_only ...`
- `python -m autobot.cli paper alpha ...`
- `python -m autobot.cli backtest run ...`
- `python -m autobot.cli live run ...`
- existing `data / collect / micro / features / exec` command families

Hard rule:

- internal handler modules may be split
- public command names, argument names, defaults, and exit-code meaning must remain compatible

### 2. Registry Pointer Contracts

These aliases are frozen:

- `champion_v4`
- `latest_v4`
- `latest_candidate_v4`

These meanings are frozen:

- `champion_v4` is the shared paper/live production pointer
- `latest_candidate_v4` is the candidate pointer

Hard rule:

- no silent renaming
- no additional live-only pointer introduced as implicit default

### 3. Runtime Artifact Contracts

The following files are frozen by name and location for the first hardening phase:

- `train_config.yaml`
- `thresholds.json`
- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`
- `runtime_recommendations.json`

Hard rule:

- new fields are allowed
- stricter validation is allowed
- file removal, file rename, or mandatory field deletion is not allowed in the first compatibility phase

### 4. Server Script Contracts

The following operational scripts are considered protected:

- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/v4_governed_candidate_acceptance.ps1`
- `scripts/v4_promotable_candidate_acceptance.ps1`
- `scripts/v4_scout_candidate_acceptance.ps1`
- `scripts/v4_rank_shadow_candidate_acceptance.ps1`
- `scripts/install_server_runtime_services.ps1`
- `scripts/install_server_live_runtime_service.ps1`
- `scripts/install_server_dashboard_service.ps1`
- `scripts/install_server_daily_acceptance_service.ps1`
- `scripts/install_server_daily_parallel_acceptance_service.ps1`
- `scripts/install_server_rank_shadow_service.ps1`

Hard rule:

- no breaking parameter rename
- no changed exit code semantics
- no changed expected output/report path without compatibility support

### 5. systemd Unit Contracts

These unit names are protected:

- `autobot-paper-v4.service`
- `autobot-paper-v4-challenger.service`
- `autobot-ws-public.service`
- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`
- `autobot-v4-challenger-spawn.service`
- `autobot-v4-challenger-promote.service`
- `autobot-v4-rank-shadow.service`
- `autobot-dashboard.service`

Timer names are also protected.

Hard rule:

- no renaming in this ticket
- no changed startup assumptions for these units unless compatibility tested

### 6. Live State DB Contracts

These tables are protected:

- `positions`
- `orders`
- `intents`
- `risk_plans`
- `trade_journal`
- `checkpoints`
- `breaker_state`
- `breaker_events`
- `order_lineage`
- `run_locks`

Hard rule:

- additive schema migration only in the first phase
- no destructive rename/drop
- no unit change without explicit migration helpers

## Current Structural Problems To Solve

### 1. Exit State Has No Single Canonical Runtime SSOT

The same exit decision currently appears in:

- `strategy.meta.model_exit_plan`
- `positions.tp_json/sl_json/trailing_json`
- `risk_plans`
- `trade_journal.entry_meta`
- dashboard summaries

This causes:

- projection drift risk
- display mismatch risk
- unit mismatch risk
- runtime backfill complexity

### 2. Core Modules Are Too Large

Current file sizes are already a warning sign:

- `autobot/cli.py`
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/live/model_alpha_runtime.py`
- `autobot/dashboard_server.py`
- `autobot/strategy/model_alpha_v1.py`

This creates:

- expensive reviews
- fragile edits
- hard-to-localize regressions
- coupling between unrelated concerns

### 3. Dashboard Has Duplicated Rendering Logic

There is currently rendering logic in both:

- `autobot/dashboard_assets/dashboard.js`
- server-embedded fallback HTML/JS in `autobot/dashboard_server.py`

This increases mismatch risk for:

- units
- percent display
- live risk-plan representation

### 4. Continuity Coverage Is Not Yet One-Piece Strong

Many critical tests exist, but one integrated path still needs to be formalized:

- artifact creation
- registry resolution
- runtime contract load
- restart recovery
- risk-plan regeneration
- dashboard consistency

### 5. Runtime Contracts Still Depend On Normalization Glue

Current runtime contracts work, but some flows still depend on:

- `legacy_backfilled`
- `manual_fallback`
- implicit unit assumptions
- multi-step normalization glue

That is acceptable as transition logic, but it should not remain the long-term architecture.

## Non-Goals

This ticket does not include:

- new alpha ideas
- new model families
- exchange expansion
- data volume expansion
- changing business logic just because files are being split
- changing the current champion/candidate governance policy

## Required Migration Strategy

The work must proceed in the following order.

### Phase A - Stabilize And Document Contracts

Before moving logic:

- document protected interfaces
- add tests for current expected external behavior
- add explicit schema/unit expectations

### Phase B - Extract Internals Behind Stable Interfaces

Only after Phase A:

- split internal modules
- preserve public import/use behavior
- keep all current command surfaces and file outputs stable

### Phase C - Canonicalize Runtime State

Only after extraction:

- define the canonical exit-state contract
- make other records explicit projections
- retain compatibility readers until all consumers are migrated

### Phase D - Tighten Validation

Only after compatibility coverage exists:

- reject ambiguous fields earlier
- reduce legacy backfills
- add version requirements where possible

## Proposed Slices

### Slice 1 - Compatibility Harness First

Deliverables:

- tests that freeze current external behavior for:
  - CLI command parsing and exit codes
  - server script expectations where practical
  - registry pointer resolution
  - runtime artifact load
  - dashboard critical fields

Acceptance:

- no implementation split yet unless required for testability
- compatibility baseline is recorded in tests

### Slice 2 - Canonical Exit Contract Design

Deliverables:

- explicit canonical exit-state schema doc
- unit naming conventions:
  - `_ratio`
  - `_pct_points`
  - `_bps`
  - `_ts_ms`
- projection map:
  - canonical source
  - derived projections
  - write path
  - read path

Acceptance:

- no destructive schema change
- compatibility readers stay in place

### Slice 3 - `model_alpha_v1` / `model_alpha_runtime` Safe Split

Deliverables:

- extract runtime contract resolver logic
- extract feature row requirements
- extract risk-plan projection logic
- extract intent submission and sync helpers

Acceptance:

- same strategy behavior
- same runtime outputs
- same live restart behavior
- no service/script change required

### Slice 4 - Trainer / CLI Internal Split

Deliverables:

- split `train_v4_crypto_cs.py` by concern:
  - dataset preparation
  - selection contract
  - trade-action contract
  - runtime recommendation synthesis
  - evidence/report writing
- split `cli.py` by command family internally

Acceptance:

- same command names
- same arg names
- same output artifact names
- same pointer behavior

### Slice 5 - Dashboard Rendering Unification

Deliverables:

- one authoritative formatting path for critical live/risk-plan units
- minimal fallback rendering only
- asset contract tests

Acceptance:

- no display mismatch for percent/unit-critical fields
- same snapshot API shape unless additive

### Slice 6 - Continuity End-to-End Pack

Deliverables:

- integrated test scenario covering:
  - trained artifact load
  - live runtime startup
  - active model-risk plan
  - restart
  - reconcile/resume
  - dashboard snapshot consistency

Acceptance:

- restart-safe scenario is green
- no projection drift across journal/risk_plan/dashboard for the tested path

### Slice 7 - Runtime Schema Hardening

Deliverables:

- explicit versioning for runtime contracts where feasible
- stricter validation
- reduced silent `legacy_backfilled` reliance

Acceptance:

- strictness increases without breaking old active artifacts unexpectedly
- migration/compatibility path is explicit

## Verification Matrix

Every slice must verify all affected areas below before completion.

### 1. Training

- trainer runs still complete
- artifact names and locations remain unchanged
- acceptance scripts still read outputs successfully

### 2. Registry / Pointer

- `champion_v4`
- `latest_v4`
- `latest_candidate_v4`

must resolve exactly as before.

### 3. Backtest

- predictor still loads runtime contracts
- strategy still consumes the same runtime recommendation shape

### 4. Paper

- `autobot-paper-v4.service` equivalent runtime contract still loads
- candidate paper still binds `latest_candidate_v4`

### 5. Live

- main live still binds the expected pointer
- candidate live still binds the expected pointer
- restart reconcile still reconstructs position/risk state

### 6. Dashboard

- live risk plans
- recent intents
- recent trades
- runtime artifact summary

must remain internally consistent.

### 7. Server Automation

- daily training scripts still run without changed flags
- promotion/restart flow still works
- systemd unit names and timers remain valid

## Rollback Requirements

Every implementation slice must preserve a simple rollback path.

Minimum rollback requirement:

- `git revert` of the slice commit(s) must be enough to restore behavior

If any slice requires:

- manual DB migration
- artifact rewrite
- service rename
- script parameter change

then that slice must be split further and get its own migration note.

## Acceptance

This ticket is complete only if all of the following are true.

- server scheduler compatibility remains intact
- systemd unit compatibility remains intact
- CLI compatibility remains intact
- registry pointer semantics remain intact
- runtime artifact filenames/locations remain intact
- live restart/reconcile remains intact
- dashboard critical fields remain unit-safe and internally consistent
- exit-state representation has a documented canonical source
- large core modules are reduced by responsibility extraction, not by behavior change
- end-to-end regression coverage is stronger than before the refactor

## Expected Outcome

After T23.2, the program should remain operationally identical from the outside, but become:

- easier to maintain
- harder to break by accident
- clearer in state ownership
- safer to evolve in future methodology tickets

This ticket should reduce software-structure risk without introducing new operational risk.
