# ADR 0004 - Package SSOT Consolidation to Root `autobot/`

## Status
Accepted (2026-03-03)

## Context
- Initial bootstrap used `python/autobot/` as the primary package path.
- A root `autobot/` bridge was introduced for `python -m autobot.cli` convenience.
- Subsequent tickets accumulated real implementation in root `autobot/`.
- Dual-package maintenance increased confusion and drift risk.

## Decision
- Source-of-truth (SSOT) package is fixed to:
  - `D:\MyApps\Autobot\autobot\`
- `python/autobot/` is no longer a development target.
- `python/autobot/` is intentionally minimized to deprecation guidance only.
- Optional one-way sync script is provided:
  - `scripts/sync_python_autobot.ps1`
  - Source: `.\autobot\`
  - Target: `.\python\autobot\`
  - Excludes: `__pycache__`, `*.pyc`, `tests`, `logs`, `data`, `models`

## Consequences
### Positive
- Single runtime and development path for all tickets.
- Eliminates duplicate edits and mismatch risk.
- Clearer onboarding and operational runbook.

### Trade-offs
- Running from `D:\MyApps\Autobot\python` is not a supported default path.
- If a mirrored package is needed for tooling, explicit sync is required.

## Migration Notes
- Legacy implementation files under `python/autobot/` were removed.
- Root CLI path remains:
  - `python -m autobot.cli ...`
