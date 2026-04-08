# 00:20 Nightly Cycle Trace, Post-Acceptance Handoff

## 0. Purpose

This document traces the practical handoff layer that starts after Step 4 acceptance logic:

- pointer updates
- candidate adoption semantics
- paper runtime model binding
- promote-to-champion semantics
- live runtime model binding / resume behavior

The purpose is to answer:

1. what really happens after `candidate_acceptance`
2. which pointers and files are mutated
3. how paper/live runtime decide what run to load
4. whether this handoff is consistent with the Step 1~4 snapshot-driven chain


## 1. Important Scope Clarification

There is no separate top-level nightly `Step 5` wrapper.

Instead, the “after Step 4” world is spread across:

- `scripts/candidate_acceptance.ps1`
- `autobot/models/registry.py`
- `autobot/paper/engine.py`
- `autobot/live/model_handoff.py`
- `autobot/live/daemon.py`
- runtime install scripts

So this document should be read as:

- “post-acceptance handoff layer”

rather than:

- “independent nightly step”


## 2. Pointer Semantics

The most important pointer contracts are:

- `latest`
- `latest_candidate`
- `champion`

### 2.1 Meaning

- `latest`
  - latest trained run in a family
- `latest_candidate`
  - latest acceptance-adopted candidate
- `champion`
  - current production/paper champion

This distinction is critical.

Current code intentionally allows:

- a run to become `latest`
- without becoming `latest_candidate`

if acceptance fails later.

That is why downstream must not silently treat:

- `latest`
- `latest_candidate`

as interchangeable.


## 3. What Step 4 Mutates

Inside `scripts/candidate_acceptance.ps1`, after the late gates:

### 3.1 Candidate pointer update

If:

- `overallPass == true`
- lane is not shadow-only
- lane promotion is allowed
- not dry-run
- candidate run id exists

then Step 4 updates:

- family-local `latest_candidate.json`
- global `models/registry/latest_candidate.json`

through:

- `Update-LatestCandidatePointers`
- which delegates to `Update-V4LatestCandidatePointers`

### 3.2 Artifact status

Regardless of pointer/promote outcome, Step 4 updates:

- `artifact_status.json`

through:

- `Update-RunArtifactStatus`

with fields such as:

- `acceptance_completed`
- `candidate_adoptable`
- `candidate_adopted`
- `promoted`
- `status`

### 3.3 Promote

If:

- `overallPass == true`
- `SkipPromote == false`
- lane governance allows it
- paper soak is not intentionally skipped

then Step 4 runs:

- `python -m autobot.cli model promote --model-ref <candidate_run_id> --model-family <family>`

Promotion is not the same as `latest_candidate`.
It is the champion handoff.

### 3.4 Restart units

Only after promote succeeds:

- `Invoke-RestartUnits`

can restart runtime target units.

So:

- acceptance pass alone does not guarantee live runtime restart
- successful promotion is the real restart trigger


## 4. Registry Implementation Contract

`autobot/models/registry.py` implements the pointer semantics.

### 4.1 `update_latest_pointer`

Writes:

- `latest.json`

### 4.2 `update_latest_candidate_pointer`

Writes:

- `latest_candidate.json`

### 4.3 `set_champion_pointer`

Writes:

- `champion.json`

### 4.4 `promote_run_to_champion`

Important behavior:

- first resolves run dir
- then calls `ensure_run_completeness(..., require_acceptance_completed=True)`
- only complete runs can be promoted

This is an important downstream safety check.
It means promotion is not just a pointer rewrite.
It requires an acceptance-complete artifact status.


## 5. Current Server Pointer State

Current server observation for `train_v5_fusion`:

- `models/registry/train_v5_fusion/latest.json`
  - present
  - currently points at:
    - `20260406T171500Z-s42-67abadc6`
- `models/registry/train_v5_fusion/latest_candidate.json`
  - absent
- `models/registry/latest_candidate.json`
  - absent
- `models/registry/train_v5_fusion/champion.json`
  - still points to earlier champion run

This exactly matches the latest failed acceptance artifact:

- train completed
- run became `latest`
- but acceptance did not adopt it as candidate
- and champion did not move

This is an intended contract, not a bug by itself.


## 6. Current Run Artifact Status

For the current failed latest fusion run:

- run id:
  - `20260406T171500Z-s42-67abadc6`
- `artifact_status.json`
  - `status = acceptance_incomplete`
  - `acceptance_completed = false`
  - `candidate_adoptable = false`
  - `candidate_adopted = false`
  - `promoted = false`

That is consistent with:

- no `latest_candidate`
- no champion promote


## 7. Paper Runtime Binding

`autobot/paper/engine.py` defines runtime metadata through:

- `_resolve_paper_runtime_metadata`

It reads environment variables such as:

- `AUTOBOT_PAPER_UNIT_NAME`
- `AUTOBOT_PAPER_RUNTIME_ROLE`
- `AUTOBOT_PAPER_LANE`
- `AUTOBOT_PAPER_MODEL_REF_PINNED`

and resolves:

- `paper_runtime_model_ref`
- `paper_runtime_model_family`
- `paper_runtime_feature_set`
- `paper_runtime_model_run_id`

Important semantic split:

- `paper_runtime_role = champion`
  - lane summarized as `paper_champion`
- `paper_runtime_role = candidate/challenger`
  - lane summarized as `paper_candidate`

So paper reporting already distinguishes:

- champion paper lane
- candidate paper lane


## 8. Runtime Install Defaults

`scripts/install_server_runtime_services.ps1` hard-codes several runtime presets.

Important v5 defaults:

- champion paper runtime:
  - `RuntimeModelRef = champion`
  - `BootstrapRefs = latest_candidate, latest`
  - `RuntimeRole = champion`
- candidate paper runtime:
  - `RuntimeModelRef = latest_candidate`

This matters because:

- champion paper service expects champion by default
- candidate paper service expects latest candidate by default

So a missing `latest_candidate` pointer can directly explain:

- no candidate paper lane handoff


## 9. Live Runtime Binding

`autobot/live/model_handoff.py` and `autobot/live/daemon.py` define live runtime binding.

### 9.1 `resolve_live_model_ref_source`

Normalizes aliases such as:

- `champion_v4 -> champion`
- `latest_candidate_v4 -> latest_candidate`

### 9.2 `resolve_live_runtime_model_contract`

Given:

- registry root
- requested model ref
- model family

it resolves:

- resolved pointer name
- resolved pointer run id
- champion pointer run id
- live runtime model run id
- live runtime model run dir

### 9.3 Candidate fallback on restart

`_resolve_runtime_model_contract_with_candidate_fallback` in `live/daemon.py` has an important behavior:

- if requested source is `latest_candidate`
- and that pointer cannot currently resolve
- but previous pinned run id exists
- daemon may fall back to previous pinned run

with warning metadata like:

- `startup_resolution_fallback = previous_pinned_run`

This is restart-stability logic.

It is useful operationally, but it also means:

- current live binding is not purely “current pointer at all times”
- it can be “previous pinned run” during recovery situations


## 10. Live Runtime Health Contract

`live/daemon.py` builds runtime health from:

- pinned runtime contract
- current runtime contract
- ws public contract
- feature platform contract

It writes:

- `live_runtime_health`

Important fields include:

- `live_runtime_model_run_id`
- `champion_pointer_run_id`
- `expected_pointer_run_id`
- `model_pointer_divergence`
- `promote_happened_while_down`
- `feature_platform_ready`
- `feature_platform_reason_codes`

This means live runtime has its own explicit consistency layer after acceptance/promotion.


## 11. Post-Acceptance Consistency Findings

### F1. `latest` vs `latest_candidate` is intentionally split

Current judgment:

- correct and necessary

Reason:

- latest trained run is not always latest acceptable run

### F2. Promotion safety is stronger than simple pointer rewrite

Current judgment:

- good

Reason:

- `promote_run_to_champion()` requires acceptance-complete artifact status

### F3. Paper/live runtime binding is pointer-driven but not purely pointer-only

Current judgment:

- nuanced

Reason:

- candidate/champion defaults are clear
- but live daemon may fall back to previous pinned run during restart recovery

This is likely correct for availability, but it means the live handoff layer is not a pure pointer mirror.

### F4. Current failed latest fusion run is downstream-consistent

Observed:

- `latest` points to failed latest train run
- `latest_candidate` absent
- champion unchanged
- artifact status says `acceptance_incomplete`

Current judgment:

- consistent
- this part of the handoff layer looks correct


## 12. Remaining Open Questions

These still need deeper verification before calling the post-acceptance handoff fully closed:

1. when Step 4 succeeds, does `latest_candidate` immediately become the exact run that candidate paper / canary live consume under all current unit configs?
2. after real promote, do restart target units always rebind to the new champion pointer without stale fallback artifacts?
3. is `current_state.json` always aligned with `latest_candidate` in the v5 lane after successful adoption?
4. are there any remaining cases where `latest` is accidentally used in place of `latest_candidate` for candidate-facing lanes?


## 13. Conservative Status

This post-acceptance handoff layer is not fully closed yet.

What is already strong:

- pointer semantics are now clearly understood
- promotion requires acceptance-complete status
- paper/live binding contracts are identified
- current server pointer state is internally consistent with the failed acceptance artifact

What still needs live verification:

- a successful representative Step 4 run
- candidate adoption into `latest_candidate`
- paper/canary/runtime handoff after that adoption
- eventual promote-to-champion handoff

So this layer should be treated as:

- `analysis quality: strong`
- `completion judgment: still open pending a successful full handoff run`

