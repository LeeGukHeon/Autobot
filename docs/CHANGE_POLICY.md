# Change Policy

## Purpose
Define a mandatory change format so legacy logic is explicitly removed before new logic is inserted.

## Required Ticket Sections
Every logic-change ticket must include both sections below.

```text
[DELETE]
- List files/modules removed
- List imports/configs removed

[ADD]
- List files/modules added
- List imports/configs added
- List schema/config updates
```

## Rules
1. Code can be deleted, but design history must be preserved in ADR documents.
2. Each ticket must link to at least one ADR if architectural behavior changes.
3. New modules must be wired through interface boundaries (Strategy/Risk/Execution separation).
4. Runtime safety updates (rate limit, order validation, kill-switch) require tests before merge.
