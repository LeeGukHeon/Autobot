# T23.1 - Ops Dashboard v1

## Goal

Expose a read-only Korean dashboard that shows the current state of:

- training / acceptance
- challenger loop
- paper runtime
- live runtime
- ws-public data plane
- core systemd units

on a single externally reachable page.

## Scope

- Run on the Oracle server as a dedicated systemd service.
- Avoid port `80`; use a separate direct-IP port.
- Keep the stack simple enough to maintain with the current Python runtime.
- Read existing artifacts and state directly:
  - `logs/model_v4_acceptance/latest.json`
  - `logs/model_v4_challenger/latest.json`
  - `logs/live_rollout/latest.json`
  - `data/paper/runs/*/summary.json`
  - `data/state/**/live_state.db`
  - `data/raw_ws/upbit/_meta/*`
  - `systemctl show`

## Design

- Read-only dashboard server: `python -m autobot.dashboard_server`
- Default bind:
  - host: `0.0.0.0`
  - port: `8088`
- Single page UI with auto-refresh and a JSON snapshot API.
- No dependency on StockMaster and no reverse proxy requirement.

## Non-Goals

- write actions
- order mutation
- auth layer by default
- replacing existing CLI status commands

## Acceptance

- `GET /` returns a Korean dashboard page.
- `GET /api/snapshot` returns a current JSON snapshot.
- systemd install script can deploy the dashboard as `autobot-dashboard.service`.
- page shows current status for training, paper, live, ws-public, and major timers/services without requiring manual SSH.
