# Docs Guide

- Updated: 2026-03-25
- Purpose: point implementation and investigation work to the right documents quickly

## Start Here

If the goal is to continue implementation work, read these first in order:

1. [CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md](/d:/MyApps/Autobot/docs/CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md)
2. [NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md](/d:/MyApps/Autobot/docs/NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md)
3. [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)

Do not start from older analysis notes or by jumping directly into code changes.

Unless the user explicitly waives it, implementation work is not complete at local code-change time.
It must also be committed, pushed, pulled on the OCI server, and then validated there.

OCI server access reference:
- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

## Current SSOT

- [PROGRAM_RUNBOOK.md](/d:/MyApps/Autobot/docs/PROGRAM_RUNBOOK.md)
  - current operational SSOT for lifecycle, runtime topology, services, timers, recovery, and deployment pattern
- [CONFIG_SCHEMA.md](/d:/MyApps/Autobot/docs/CONFIG_SCHEMA.md)
  - current config and runtime contract reference
- [CHANGE_POLICY.md](/d:/MyApps/Autobot/docs/CHANGE_POLICY.md)
  - current change-management rules
- [EXIT_STATE_CONTRACT.md](/d:/MyApps/Autobot/docs/EXIT_STATE_CONTRACT.md)
  - current exit-state contract
- [REPLAY_LEGACY_CLEANUP_POLICY_2026-03-25.md](/d:/MyApps/Autobot/docs/REPLAY_LEGACY_CLEANUP_POLICY_2026-03-25.md)
  - current replay legacy exclusion policy for server topology

## Active Blueprints

- [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)

## Current Analysis

- [FOUNDATIONAL_FAILURE_MODES_2026-03-23.md](/d:/MyApps/Autobot/docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md)
- [TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md](/d:/MyApps/Autobot/docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md)
- [RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md](/d:/MyApps/Autobot/docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md)
- [EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md](/d:/MyApps/Autobot/docs/EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md)

## Historical Background

These remain useful for background, but they are not the operational SSOT unless explicitly restated elsewhere.

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

## Directories

- `docs/ADR/`
  - architecture decision records
- `docs/TICKETS/`
  - design backlog and implementation history
- `docs/reports/`
  - generated or historical reports

## Rule

When documents disagree:

1. current code wins
2. current OCI runtime state wins over older text
3. [PROGRAM_RUNBOOK.md](/d:/MyApps/Autobot/docs/PROGRAM_RUNBOOK.md) wins over historical documents
4. the current mandatory principles and execution protocol win over older working habits
