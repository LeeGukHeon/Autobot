# Reports Guide

- Updated: 2026-03-23
- Purpose: clarify that `docs/reports/` contains generated and historical report artifacts, not current operational truth

This directory contains:

- generated daily reports
- integration reports
- templates for older report flows

Use these reports for:

- historical evidence
- implementation snapshots
- audit trails

Do not use these reports as the first source for:

- current OCI runtime state
- current live execution behavior
- current champion/candidate governance

Current truth lives in:

- `docs/PROGRAM_RUNBOOK.md`
- `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
- `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

If a report contradicts current code or current runbook:

- the report is a historical record
- current code and current runbook win
