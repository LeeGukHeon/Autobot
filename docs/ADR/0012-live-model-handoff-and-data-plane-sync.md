# ADR 0012: Live Model Handoff And Shared Data-Plane Sync

- Date: 2026-03-09
- Status: Accepted
- Context: `T22.7` closes the gap between the daily `champion_v4` promotion loop and the future live runtime. Paper and live must not drift onto different model pointers or different public-data epochs.

## Decision
- `champion_v4` remains the single source of truth for both champion paper and live runtime.
- Live runtime startup order is fixed to:
  - `reconcile`
  - `resume risk state`
  - `bind current champion run id`
- The bound live run id is persisted as a checkpoint and compared against the current `champion_v4` pointer on every cycle.
- If the pinned live run id differs from the current champion pointer, live raises `MODEL_POINTER_DIVERGENCE` and halts new intents.
- Live health also reads the same `ws-public` meta plane used by the daily loop:
  - `ws_public_health.json`
  - `ws_collect_report.json`
  - `ws_validate_report.json`
  - latest `micro_v1` aggregate report when available
- Excessive public-data staleness raises `WS_PUBLIC_STALE` and halts new intents.
- Daily promote persists a cutover artifact with:
  - previous champion run id
  - new champion run id
  - promotion timestamp
  - restarted target units

## Consequences
- Live no longer runs on an implicit or stale model contract.
- Promote/restart events become auditable across paper and future live target units.
- Restart continuity for TP/SL/trailing remains intact because model binding happens only after reconcile and risk resumption.
- If a promote happens while live is down, the next restart records `promote_happened_while_down=true` in the runtime contract checkpoint.
