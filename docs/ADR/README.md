# ADR Guide

- Updated: 2026-03-23
- Purpose: explain how to read architecture decision records without mistaking them for live operational state

`docs/ADR/` is historical architecture context.

Use ADRs for:

- why a contract or boundary was introduced
- what architectural tradeoff was chosen at the time
- what assumptions existed when the change was made

Do not use ADRs as the first source for:

- current service state
- current deployment topology
- current pointer behavior
- current runtime execution findings

Read this order instead:

1. `docs/PROGRAM_RUNBOOK.md`
2. `docs/CONFIG_SCHEMA.md`
3. current dated analysis docs in `docs/`
4. relevant ADRs only after that

If an ADR conflicts with current code or current runbook:

- ADR remains historical design intent
- current code and runbook win
