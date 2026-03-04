# INTEGRATION_REPORT_2026-03-04_T15

## 0) Summary
- Ticket: `T15`
- Scope: `MicroGate v1` as execution risk filter (not alpha input)
- Root: `D:\MyApps\Autobot`
- Result:
  - `MicroGateV1` + `MicroSnapshotProvider(offline/live-guard)` implemented
  - `TradeGateV1` integrated with micro `OK/WARN/BLOCK` + `gate_reasons/diagnostics`
  - `paper/backtest` emit micro counters + `summary.json` + `micro_gate_blocked.json`
  - current `live/daemon` path is state-sync only (no strategy/trade-gate call site), so T15 wiring target is `paper/backtest`
  - New unit/integration tests added
  - `python -m pytest -q` => `150 passed, 4 warnings`

## 1) Executed Commands / Settings
### 1.1 Tests
- `python -m pytest -q tests/test_trade_gate_v1.py tests/test_paper_engine_integration.py tests/test_backtest_engine_integration.py tests/test_micro_gate_trade_only_rules.py tests/test_micro_gate_book_rules_optional.py tests/test_micro_snapshot_offline_provider.py tests/test_micro_snapshot_live_ws_rate_limit_guard.py tests/test_paper_engine_micro_gate_integration.py tests/test_backtest_engine_micro_gate_integration.py`
- `python -m pytest -q`

### 1.2 Backtest OFF vs ON
- OFF:
  - `python -m autobot.cli backtest run --market KRW-BTC --tf 5m --duration-days 1 --micro-gate off`
  - run_id: `backtest-20260304-102220-5f39c79ba3`
- ON:
  - `python -m autobot.cli backtest run --market KRW-BTC --tf 5m --duration-days 1 --micro-gate on --micro-gate-mode trade_only --micro-gate-on-missing warn_allow`
  - run_id: `backtest-20260304-102228-5f39c79ba3`

### 1.3 Paper OFF vs ON
- OFF:
  - `python -m autobot.cli paper run --duration-sec 20 --quote KRW --top-n 5 --micro-gate off`
  - run_id: `paper-20260304-102247`
- ON:
  - `python -m autobot.cli paper run --duration-sec 20 --quote KRW --top-n 5 --micro-gate on --micro-gate-mode trade_only --micro-gate-on-missing warn_allow`
  - run_id: `paper-20260304-102315`

### 1.4 Config diff (strategy)
- `config/strategy.yaml`:
  - inserted `strategy.micro_gate.*` block (enabled/mode/on_missing/stale/trade/book/live_ws)

## 2) OFF vs ON Comparison (T15 Required Metrics)

### 2.1 Backtest (`KRW-BTC`, `5m`, `duration_days=1`)
| Metric | OFF | ON |
|---|---:|---:|
| candidates_total | 4 | 4 |
| candidates_blocked_by_micro | 0 | 0 |
| blocked_ratio | 0.0000 | 0.0000 |
| orders_submitted | 1 | 1 |
| orders_filled | 1 | 1 |
| fill_ratio | 1.0000 | 1.0000 |
| cancels | 0 | 0 |
| realized_pnl_quote | 0.0 | 0.0 |
| unrealized_pnl_quote | 52.0362 | 52.0362 |

### 2.2 Paper (`duration_sec=20`, `quote=KRW`, `top_n=5`)
| Metric | OFF | ON |
|---|---:|---:|
| candidates_total | 1 | 1 |
| candidates_blocked_by_micro | 0 | 0 |
| blocked_ratio | 0.0000 | 0.0000 |
| orders_submitted | 1 | 1 |
| orders_filled | 1 | 1 |
| fill_ratio | 1.0000 | 1.0000 |
| cancels | 0 | 0 |
| realized_pnl_quote | 0.0 | 0.0 |
| unrealized_pnl_quote | -58.1395 | 0.0 |

## 3) Blocked Reasons TopN
### 3.1 Backtest ON (`events.jsonl`)
- `DUPLICATE_ENTRY`: 3
- `MICRO_MISSING`: 1 (`TRADE_GATE_WARN_ALLOW` path, not block)
- Micro block reasons (`summary.micro_blocked_reasons`): empty

### 3.2 Paper ON (`events.jsonl`)
- `MICRO_MISSING`: 1 (`TRADE_GATE_WARN_ALLOW`)
- Micro block reasons (`summary.micro_blocked_reasons`): empty

## 4) Conclusion
- Result: `INCONCLUSIVE`
- Reason:
  - ON/OFF 모두 `candidates_blocked_by_micro = 0`으로 trade reduction/quality change를 관찰하지 못함
  - filled trades가 매우 작음 (`backtest=1`, `paper=1`) -> T15 해석 규칙(`filled < 30`)에 따라 결론 보류

## 5) Legacy Change Markers ([DELETE]/[INSERT])
- `autobot/strategy/trade_gate_v1.py`
  - [DELETE] 최종 `ALLOW` 즉시 반환(기존 core gate만 사용)
  - [INSERT] micro snapshot 조회 + `MicroGateV1.evaluate` 후 `BLOCK/WARN_ALLOW` 분기, `gate_reasons/diagnostics` 반환
- `autobot/paper/engine.py`
  - [DELETE] `TRADE_GATE_BLOCKED` payload에 단일 `reason_code/detail`만 기록
  - [INSERT] `severity/gate_reasons/diagnostics` 기록 + micro blocked counters 집계 + `summary.json`, `micro_gate_blocked.json`
- `autobot/backtest/engine.py`
  - [DELETE] summary에 micro gate 관련 카운터 없음
  - [INSERT] `candidates_blocked_by_micro`, `micro_blocked_ratio`, `micro_blocked_reasons` 집계/저장
- `autobot/cli.py`
  - [INSERT] `--micro-gate`, `--micro-gate-mode`, `--micro-gate-on-missing` (paper/backtest)
  - [INSERT] `strategy.micro_gate.*` -> `MicroGateSettings` 파싱

## 6) Added/Updated Files
- Added:
  - `autobot/strategy/micro_gate_v1.py`
  - `autobot/strategy/micro_snapshot.py`
  - `tests/test_micro_gate_trade_only_rules.py`
  - `tests/test_micro_gate_book_rules_optional.py`
  - `tests/test_micro_snapshot_offline_provider.py`
  - `tests/test_micro_snapshot_live_ws_rate_limit_guard.py`
  - `tests/test_paper_engine_micro_gate_integration.py`
  - `tests/test_backtest_engine_micro_gate_integration.py`
  - `docs/reports/INTEGRATION_REPORT_2026-03-04_T15.md`
- Updated:
  - `autobot/strategy/trade_gate_v1.py`
  - `autobot/strategy/__init__.py`
  - `autobot/paper/engine.py`
  - `autobot/backtest/engine.py`
  - `autobot/cli.py`
  - `config/strategy.yaml`
  - `docs/CONFIG_SCHEMA.md`

## 7) Next Ticket (one)
- Proposed next: `T15.1-C`
  - Apply micro to order parameters (timeout/reprice aggressiveness) instead of entry blocking only.
