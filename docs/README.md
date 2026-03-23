# Docs Guide

- Updated: 2026-03-23
- Purpose: separate current operational truth from historical design and investigation documents

## Current Truth

Read these first when you need the current system behavior.

- `docs/PROGRAM_RUNBOOK.md`
  - current operational SSOT for lifecycle, runtime topology, services, timers, recovery, and deploy pattern
- `docs/CONFIG_SCHEMA.md`
  - current config and runtime contract reference
- `docs/CHANGE_POLICY.md`
  - current change-management rules
- `docs/EXIT_STATE_CONTRACT.md`
  - current exit-state contract

## Current Analysis

Read these when investigating current performance or execution behavior.

- `docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md`
  - current structural root-cause diagnosis and next-context handoff
- `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
  - current research comparison and training/runtime diagnosis
- `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`
  - current execution findings, deployment status, and concrete next actions
- `docs/EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md`
  - current zero-fill veto failure analysis and research-backed execution-contract redesign

## Historical Root Docs

These remain useful, but they are not the operational SSOT unless explicitly restated in the current runbook.

- `docs/ROADMAP.md`
- `docs/LIFECYCLE_AUDIT_2026-03-20.md`
- `docs/CODEBASE_LIFECYCLE_AUDIT_2026-03-20.md`
- `docs/CODE_REVIEW_2026-03-18.md`
- `docs/LIVE_EXECUTION_PARITY_REDESIGN.md`
- `docs/LIVE_RUNTIME_POSTMORTEM_2026-03-21.md`
- `docs/RISK_CONTROL_SAFETY_LAYER_REDESIGN.md`
- `docs/REPLAY_PLAN_2026-03-21.md`
- `docs/V4_NATIVE_BUILDER_ANALYSIS_2026-03-22.md`
- `docs/V4_LEGACY_DEPENDENCY_FINDINGS_2026-03-22.md`
- `docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md`
- `docs/API_NOTES.md`
- `docs/PNL_REALIZATION_FLOW.md`

Use them for:

- design background
- incident context
- migration notes
- cleanup candidates

Do not treat them as the final word over:

- current code
- current OCI state
- current runbook
- current execution findings documents

## Directories

- `docs/ADR/`
  - architecture decision records; historical but still useful for why a contract exists
- `docs/TICKETS/`
  - design backlog and implementation history; not current operational truth
- `docs/reports/`
  - generated or historical reports; not current operational truth unless specifically promoted into the runbook

## Reading Order

1. `docs/PROGRAM_RUNBOOK.md`
2. `docs/CONFIG_SCHEMA.md`
3. `docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md`
4. `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
5. `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`
6. `docs/EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md`
7. only then read older root docs, ADRs, tickets, and reports as background

## Rule

When docs disagree:

1. current code wins
2. current OCI runtime state wins over older text
3. `docs/PROGRAM_RUNBOOK.md` wins over historical documents
4. the two `2026-03-23` analysis docs win over older investigation notes for current execution diagnosis
