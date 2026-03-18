# AUTOBOT CODE REVIEW

- Review date: 2026-03-18
- Scope: full repo static review, OCI runtime inspection, targeted regression runs
- Method:
  - parallel sub-agent audit across training, live, data, ops, and tests
  - local code inspection
  - OCI service / timer / rollout inspection
  - targeted pytest runs

Follow-up status:

- after the initial review pass, the repo now includes an OCI installer for `autobot-ws-public.service`
- transport coverage was also extended with targeted `UpbitHttpClient` regression tests

## 1. Tests Run During Review

Executed locally:

- `python -m pytest -q tests/test_search_budget.py tests/test_cli_daily_v4.py tests/test_live_model_alpha_runtime.py tests/test_live_resume.py tests/test_daily_champion_challenger_spawn_handling.py tests/test_daily_split_service_install.py tests/test_live_rollout.py tests/test_live_daemon.py`
  - result: `115 passed`
- `python -m pytest -q tests/test_daily_candidate_acceptance_for_server.py tests/test_daily_parallel_acceptance_for_server.py tests/test_daily_rank_shadow_cycle_for_server.py tests/test_candidate_canary_report.py tests/test_paper_engine_integration.py tests/test_paper_live_feature_provider_v4.py tests/test_live_risk_manager.py tests/test_storage_retention_service_install.py tests/test_ws_public_ops.py tests/test_ws_public_plan.py tests/test_ws_public_collect_validate.py tests/test_pipeline_v4_label_v2.py`
  - result: `46 passed`

Review note:

- targeted suites passed
- this does not remove the uncovered transport / websocket blind spots called out below

## 2. Highest-Priority Findings

### Critical

1. Dashboard is externally exposed and unauthenticated by default.

- `scripts/install_server_dashboard_service.ps1:6`
- `autobot/dashboard_server.py:203`
- `autobot/dashboard_server.py:1641`
- `autobot/dashboard_server.py:1761`
- `autobot/dashboard_server.py:1805`

Why it matters:

- installer binds to `0.0.0.0`
- dashboard serves `/`, `/api/snapshot`, `/api/stream`
- snapshot includes live state, service status, and account-derived data
- current implementation has no auth layer

2. V4 publishes registry pointers before the candidate run is fully durable.

- `autobot/models/train_v4_crypto_cs.py:369`
- `autobot/models/train_v4_crypto_cs.py:388`
- `autobot/models/train_v4_crypto_cs.py:396`
- `autobot/models/train_v4_crypto_cs.py:504`
- `autobot/models/registry.py:43`
- `scripts/candidate_acceptance.ps1:3590`

Why it matters:

- `latest` and then `latest_candidate` can move before runtime/governance artifacts are fully written
- a crash in that gap can leave paper/live/dashboard pointed at an incomplete run

3. Split-policy history probes can corrupt family `latest` pointers.

- `scripts/candidate_acceptance.ps1:1160`
- `scripts/candidate_acceptance.ps1:1231`
- `autobot/models/registry.py:66`

Why it matters:

- helper trains real runs in the production family
- `save_run()` advances `latest`
- helper then deletes the probe run without restoring the pointer
- `latest` can temporarily point at a run that no longer exists

4. `shadow` mode can still place real protective exit orders when live risk is enabled.

- `autobot/cli.py:4221`
- `autobot/live/model_alpha_runtime.py:342`
- `autobot/risk/live_risk_manager.py:304`

Why it matters:

- current implementation blocks new intent emission in shadow
- but protective live exits can still reach the exchange
- that breaks the common operator assumption that “shadow means no real orders”

5. Startup reconcile policy `cancel` cancels all bot-owned open orders, not only unknown ones.

- `autobot/live/reconcile.py:64`
- `autobot/live/reconcile.py:80`

Why it matters:

- on restart, resumable entry/exit orders can be torn down instead of recovered
- this is especially dangerous during live continuity / recovery windows

6. Runtime micro contract is materially different from training `micro_v1`.

- `autobot/live/model_alpha_runtime.py:1355`
- `autobot/paper/engine.py:2382`
- `autobot/strategy/micro_snapshot.py:220`
- `autobot/strategy/micro_snapshot.py:281`
- `autobot/paper/live_features_v3.py:359`
- `autobot/paper/live_features_v3.py:738`

Why it matters:

- paper/live “live_ws” micro is ticker-derived proxy data
- training `micro_v1` is merged trade + book micro
- spread/depth / source flags / micro-quality logic can drift materially at runtime

7. `WS_PUBLIC_STALE` can be suppressed by metadata writes instead of true market-data receipt.

- `autobot/live/model_handoff.py:68`
- `autobot/live/model_handoff.py:156`
- `autobot/data/collect/ws_public_collector.py:835`
- `autobot/data/collect/ws_public_collector.py:1449`

Why it matters:

- health timer rewrites metadata even if trades/books are stale
- validate/report generation can also refresh timestamps
- live can believe public data is fresh when feed receipt is actually stale

### High

8. Live units can stay down permanently after exit code `2`.

- `scripts/install_server_live_runtime_service.ps1:105`
- `scripts/install_server_live_runtime_service.ps1:106`
- `autobot/cli.py:4301`
- `autobot/cli.py:4307`

Why it matters:

- service uses `RestartPreventExitStatus=2`
- CLI returns `2` for deliberate halts and broad config/runtime failures
- transient failures can therefore suppress restart instead of self-healing

9. Split-policy selector can train the final candidate on the wrong shared v4 feature manifest/universe.

- `scripts/candidate_acceptance.ps1:1298`
- `scripts/candidate_acceptance.ps1:3114`
- `scripts/candidate_acceptance.ps1:3325`
- `scripts/candidate_acceptance.ps1:3560`
- `autobot/models/dataset_loader.py:116`

Why it matters:

- holdout probes rebuild shared `features_v4`
- final selected holdout only rewrites dates
- selected holdout is not rebuilt before training
- final training can inherit the last probed universe instead of the chosen one

10. Python transport and websocket adapters have weak test coverage at the exact interfaces that hit Upbit.

- `autobot/upbit/http_client.py:77`
- `autobot/upbit/rate_limiter.py:44`
- `autobot/execution/grpc_gateway.py:40`
- `autobot/upbit/ws/ws_client.py:30`
- `autobot/upbit/ws/ws_rate_limiter.py:10`

Why it matters:

- request auth / retry / 429 / 418 / remaining-req sync / reconnect / keepalive paths are weakly covered or untested

11. Dashboard training pointer summary and challenger summary are partially broken.

- `autobot/dashboard_server.py:39`
- `autobot/dashboard_server.py:632`
- `autobot/dashboard_server.py:1601`
- `scripts/daily_champion_challenger_v4_for_server.ps1:864`
- `scripts/daily_champion_challenger_v4_for_server.ps1:927`

Why it matters:

- dashboard reads `train_config.yaml` through JSON parser
- challenger summary expects fields daily loop does not write
- operators can see null or stale fields and make wrong decisions

## 3. Medium-Priority Findings

1. Live micro staleness age becomes `0` once any event exists in window.

- `autobot/strategy/micro_snapshot.py:256`
- `autobot/strategy/micro_gate_v1.py:80`

2. `require_micro_validate_pass` does not require a current validation report and ignores freshness/window overlap.

- `autobot/features/pipeline_v3.py:279`
- `autobot/features/pipeline_v3.py:1551`
- `autobot/features/pipeline_v4.py:301`

3. Transient order-detail lookup failure during reconcile closes the local order immediately.

- `autobot/live/reconcile.py:192`
- `autobot/live/reconcile.py:226`

4. `LOCAL_POSITION_MISSING_ON_EXCHANGE` breaker path is effectively dead.

- `autobot/live/reconcile.py:430`
- `autobot/live/reconcile.py:520`
- `autobot/live/breakers.py:470`

5. Private-WS pump completion is treated as a stale-stream breaker without distinguishing clean completion.

- `autobot/live/model_alpha_runtime.py:1328`

6. Daily micro report can certify WS health for a different date than the aggregated micro dataset date.

- `scripts/daily_micro_pipeline.ps1:271`
- `scripts/daily_micro_pipeline.ps1:412`

7. Search-budget policy knobs are partly dead code.

- `autobot/models/search_budget.py:250`
- `autobot/models/search_budget.py:293`
- `autobot/models/search_budget.py:297`

Why it matters:

- duplicate `_normalize_run_scope()` definitions exist
- `_max_runtime_profile()` always returns `compact`
- policy structure suggests richer behavior than actual implementation

8. Rollout auto-refresh cannot self-heal a missing test-order seed.

- `autobot/live/daemon.py:373`
- `autobot/live/daemon.py:379`
- `autobot/live/daemon.py:388`

Why it matters:

- auto-refresh only works when an existing test-order record already contains `market`, `side`, and `ord_type`
- a newly armed unit with no seed record remains blocked until an operator manually runs `live rollout test-order`

## 4. OCI Snapshot On 2026-03-18

Observed from live OCI inspection on 2026-03-18:

- repo SHA matched local: `159de4b7019bef95fd59ebe3e6eb9d10d1e41da2`
- `cron` for user `ubuntu` was empty
- system uses `systemd` timers as the real scheduler

Observed service state:

- `autobot-paper-v4.service`: active
- `autobot-paper-v4-challenger.service`: active
- `autobot-live-alpha.service`: inactive
- `autobot-live-alpha-candidate.service`: active
- `autobot-ws-public.service`: active
- `autobot-dashboard.service`: active

Observed rollout state:

- global latest rollout artifact targeted candidate live canary
- main live had a scoped disarm artifact with note `main live parked pending promote evidence`
- candidate live was armed but breaker-blocked

Observed historical failure signatures in logs:

- candidate live had a past `latest_candidate` pointer resolution failure
- main live had past exit-2 halts related to rollout / breaker gating

## 5. Coverage Assessment

Strong areas:

- live state store, reconcile, resume, journal, breakers
- handoff / rollout / continuity contracts
- paper/backtest integration
- C++ executor unit/fault tests

Weak areas:

- Python REST transport
- Python WS client / rate-limit client
- gRPC gateway contract surface
- real live bootstrap assembly
- several submit-reject / admissibility / replace-lineage error branches

## 6. Recommended Fix Order

Recommended next fixes, in order:

1. lock down dashboard exposure or add auth immediately
2. add a transactional “finalize run then move pointers” boundary for v4 training
3. isolate split-policy history runs from production pointers
4. enforce true no-order semantics for `shadow` mode
5. fix runtime micro contract drift or explicitly downgrade runtime reliance on proxy micro
6. change ws-public freshness to depend on actual receive timestamps, not report writes
7. revisit `RestartPreventExitStatus=2` semantics for live services
8. add focused tests for Python transport / websocket / bootstrap assembly

## 7. Residual Notes

- This review was deep but still finite. The highest residual risk is at the boundary between filesystem state, registry pointers, server orchestration scripts, and live runtime gates.
- The codebase has strong architecture and a good amount of regression coverage, but several lifecycle edges are still not transactional enough for unattended production confidence.
