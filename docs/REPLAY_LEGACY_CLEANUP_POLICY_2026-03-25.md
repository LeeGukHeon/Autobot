# REPLAY LEGACY CLEANUP POLICY 2026-03-25

## 0. Purpose

This document defines the mandatory policy for the observed replay clone and replay services.

It exists because:

- the user explicitly stated replay is not part of the intended operating path
- the current server still contains replay-specific clone and service remnants
- future topology/reporting/automation must not silently treat replay as a target lane

This is a legacy-exclusion policy, not a replay-research ban.

Future replay or certification work may still exist, but it must be implemented as an offline certification lane inside the main repo.
It must not depend on a separate long-running replay clone/service as part of target server topology.

## 1. Scope

This policy applies to:

- sibling replay-like clone paths such as `/home/ubuntu/MyApps/Autobot_replay_*`
- replay-specific runtime services such as:
  - `autobot-paper-v4-replay.service`
  - `autobot-live-alpha-replay-shadow.service`

This policy does not apply to:

- normal champion/candidate paper/live lanes
- offline event replay engines implemented inside the main repo

## 2. Policy

### 2.1 Classification

Replay clone/service is classified as:

- `legacy`
- `excluded_from_target_topology`

### 2.2 Target Topology Rule

Target topology is explicitly two-lane only:

- champion lane
- candidate lane

Replay service/clone must not be counted as:

- champion paper
- candidate paper
- champion live
- candidate live
- required background service

### 2.3 Allowed Cleanup State

Replay artifacts may temporarily remain on disk, but they must be treated as archived legacy assets.

Allowed operational end state:

- replay service stopped
- replay service disabled
- replay clone left on disk as archived legacy path, or removed later

### 2.4 Future Work Rule

If replay-based certification is needed later:

- build it inside the main repo
- run it as offline certification tooling
- do not restore replay as a long-running target runtime unit

## 3. Required Implementation Effects

### 3.1 Reporting

Runtime topology artifacts must explicitly classify replay as legacy and excluded from target topology.

### 3.2 Installers

Current server install flows must disable replay legacy services by default so future installs do not silently preserve them as active topology members.

### 3.3 Server State

Current server runtime should stop and disable replay legacy services unless the user explicitly overrides that choice.

## 4. Current Applied State

As of this policy:

- replay clone/service is not part of target architecture
- topology/reporting should surface replay as legacy-only
- split challenger installer should disable replay service remnants by default

## 5. References

- [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
- [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)
- [NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md](/d:/MyApps/Autobot/docs/NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md)
