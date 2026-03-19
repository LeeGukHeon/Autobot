# PnL Realization Flow

## Scope

이 문서는 `v4 / model_alpha_v1` 기준으로 다음 3단계의 손익 흐름을 연결해서 설명한다.

1. 학습(trainer)
2. acceptance(후보 검증 / 승급 게이트)
3. paper runtime(실시간 종이매매)

핵심 목적은 "`realized_pnl_quote`가 어디서 실제로 만들어지고, 어디서 승급 판단에 쓰이는가"를 코드 기준으로 분리해서 보는 것이다.

관련 핵심 파일:

- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/train_v4_execution.py`
- `autobot/models/execution_acceptance.py`
- `autobot/models/runtime_recommendations.py`
- `scripts/candidate_acceptance.ps1`
- `autobot/common/paper_lane_evidence.py`
- `autobot/paper/engine.py`
- `autobot/paper/sim_exchange.py`
- `autobot/strategy/model_alpha_v1.py`
- `autobot/strategy/model_alpha_runtime_contract.py`


## One-Line Summary

`학습`은 직접 realized PnL을 만들지 않는다.  
실제 손익은 모두 `backtest / execution acceptance / paper simulation`에서 만들어지고, 그 결과가 trainer artifact, acceptance gate, paper promote decision으로 흘러간다.


## SSOT Artifacts

손익 관련 SSOT는 아래 파일들이다.

- trainer execution compare:
  - `logs/train_v4_execution_backtest/runs/*/summary.json`
  - `models/registry/<family>/<run_id>/execution_acceptance_report.json`
  - `models/registry/<family>/<run_id>/runtime_recommendations.json`
- candidate acceptance backtest:
  - `data/backtest/runs/*/summary.json`
  - `logs/model_v4_acceptance/*.json`
- paper runtime:
  - 실행 중: `data/paper/runs/<paper-run-id>/events.jsonl`, `equity.csv`, `orders.jsonl`, `fills.jsonl`
  - 종료 후: `data/paper/runs/<paper-run-id>/summary.json`
- promote evidence:
  - `logs/model_v4_challenger/*.json`
  - `autobot.common.paper_lane_evidence` 결과 payload


## 1. Trainer Flow

### 1.1 Pure fitting

`train_v4_crypto_cs.py`의 booster fitting 자체는 classification / regression metric을 만든다.  
여기서는 거래 손익이 아니라 모델 예측력과 selection contract를 만든다.

생성물:

- `metrics.json`
- `thresholds.json`
- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`

중요:

- 이 단계의 `metrics`는 "모델 품질"이지 "실거래 손익"이 아니다.
- 손익 기반 artifact는 아래 execution-aware 단계에서 추가된다.


### 1.2 Trainer execution acceptance

trainer는 `_run_execution_acceptance_v4()`를 통해 candidate와 current champion을 같은 backtest engine으로 비교한다.

구현 위치:

- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/train_v4_execution.py`
- `autobot/models/execution_acceptance.py`

흐름:

1. `run_execution_acceptance_v4()`가 `ExecutionAcceptanceOptions`를 만든다.
2. `run_execution_acceptance()`가 candidate backtest를 먼저 돈다.
3. champion pointer가 있으면 champion backtest도 돈다.
4. 두 summary를 `compare_execution_balanced_pareto()`로 비교한다.

중요한 계약:

- trainer execution acceptance는
  - `selection.use_learned_recommendations = False`
  - 즉 candidate/champion 비교를 위해 selection breadth를 고정한다.
- 여기서 나온 손익은 승급용 execution evidence다.

실제 PnL source:

- `BacktestRunSummary.realized_pnl_quote`
- `BacktestRunSummary.max_drawdown_pct`
- `BacktestRunSummary.fill_rate`
- `BacktestRunSummary.slippage_bps_mean`


### 1.3 Runtime recommendation optimizer

trainer는 `_build_runtime_recommendations_v4()`에서 runtime용 exit / execution 추천 계약을 별도로 만든다.

구현 위치:

- `autobot/models/train_v4_execution.py`
- `autobot/models/runtime_recommendations.py`

흐름:

1. `optimize_runtime_recommendations()`가 candidate run 하나를 대상으로 여러 execution 정책 조합을 backtest한다.
2. 먼저 `hold family`를 탐색한다.
3. 다음 `risk family`를 탐색한다.
4. hold vs risk 비교 후 `recommended_exit_mode`, `recommended_hold_bars`, risk scaling 추천을 만든다.
5. 이어서 execution profile(`price_mode`, `timeout_bars`, `replace_max`)을 grid search 한다.

중요한 계약:

- 여기서는
  - `selection.use_learned_recommendations = True`
  - `exit.use_learned_exit_mode = False`
  - `execution.use_learned_recommendations = False`
- 즉 selection breadth는 learned contract를 허용하지만,
  exit/execution은 "추천을 만들기 위해" 아직 고정 조합으로 직접 backtest한다.

생성물:

- `runtime_recommendations.json.exit`
- `runtime_recommendations.json.execution`
- `runtime_recommendations.json.trade_action`
- `runtime_recommendations.json.risk_control`


### 1.4 Trainer governance

trainer는 위 execution artifacts를 `promotion_decision.json`, `trainer_research_evidence.json`, `decision_surface.json`에 넣는다.

즉 trainer 단계 손익 흐름은:

`fit -> execution acceptance backtests -> runtime recommendation backtests -> governance docs`


## 2. Acceptance Flow

### 2.1 Acceptance는 trainer artifact를 읽는다

`scripts/candidate_acceptance.ps1`는 새 candidate run을 만들거나 읽고, trainer가 이미 만들어 둔 execution / runtime / governance artifact를 읽는다.

여기서 중요한 점:

- acceptance는 trainer execution evidence를 참고한다.
- 하지만 acceptance 자체도 별도의 candidate/champion backtest를 다시 돈다.


### 2.2 Candidate vs champion backtest gate

acceptance의 backtest compare는 trainer runtime과 동일하지 않다.

실제 비교는 고정 compare profile이다.

대표 예:

- `--exit-mode hold`
- `--hold-bars 6`
- `--top-pct`, `--min-prob`, `--min-cands-per-ts`는 acceptance profile에서 고정

즉 acceptance backtest는 "승급 비교용 frozen profile"에 가깝다.

가드에 쓰이는 값:

- candidate `realized_pnl_quote`
- candidate `orders_filled`
- candidate `deflated_sharpe_ratio_est`
- champion 대비
  - `pnl delta`
  - `max_drawdown_pct`
  - `fill_rate`
  - `slippage_bps_mean`
  - `calmar_like_score`

최종적으로 `gates.backtest.pass`가 만들어진다.


### 2.3 Paper soak gate

`SkipPaperSoak`가 아니면 acceptance는 paper runtime을 따로 돌려 paper evidence를 읽는다.

관련 값:

- `realized_pnl_quote`
- `orders_filled`
- `micro_quality_score_mean`
- `rolling_nonnegative_active_window_ratio`
- `rolling_positive_active_window_ratio`
- `rolling_max_fill_concentration_ratio`

여기서는 단순 realized PnL 하나만 보지 않고:

- 실현손익
- micro 품질
- rolling window 안정성
- 과도한 집중도

를 함께 본다.

`paper_final_balanced` 정책이면 paper gate가 최종 promote 결정권을 가진다.


### 2.4 Overall acceptance decision

acceptance 최종 흐름:

1. candidate backtest sanity
2. champion delta compare
3. trainer evidence gate
4. budget / lane governance gate
5. optional paper soak gate
6. `overall_pass`
7. promote or keep candidate

즉 acceptance의 손익 흐름은:

`candidate backtest realized pnl + champion compare + paper soak realized pnl`


## 3. Paper Runtime Flow

### 3.1 Entry / exit intent generation

paper runtime은 `PaperRunEngine`가 `ModelAlphaStrategyV1`를 주기적으로 호출해 bid/ask intent를 만든다.

관련 파일:

- `autobot/paper/engine.py`
- `autobot/strategy/model_alpha_v1.py`

`ModelAlphaStrategyV1.on_ts()`는:

- 현재 active universe
- 최신 가격
- 현재 open positions

을 받아

- entry intent
- exit intent

를 동시에 만든다.


### 3.2 Exit reason is decided here

`ModelAlphaStrategyV1._resolve_exit_reason()`가 실제 exit reason을 만든다.

모드별 동작:

- `hold`
  - `SL`
  - `HOLD_TIMEOUT`
- `risk`
  - `TP`
  - `SL`
  - `TRAILING`
  - `TIMEOUT`

exit plan source:

- base settings
- learned runtime recommendation contract
- intratrade volatility reprice
- dynamic exit overlay

즉 paper 손익은 모델의 점수만이 아니라, 이 exit plan 계약에 크게 좌우된다.


### 3.3 Where realized PnL is actually booked

실제 realized PnL은 `PaperSimExchange._fill_order()`의 ask fill branch에서 계산된다.

구현:

- bid fill:
  - position 평균단가 갱신
  - 아직 realized PnL 없음
- ask fill:
  - `realized_pnl_quote += (fill_price - avg_entry_price) * matched_volume`
  - 그 뒤 `fee_quote` 차감

즉 paper에서 realized PnL의 회계 SSOT는 exchange fill 시점이다.


### 3.4 In-flight vs final PnL

paper run이 살아 있을 때:

- `events.jsonl`의 `PORTFOLIO_SNAPSHOT`
- `equity.csv`

가 interim realized / unrealized PnL SSOT다.

paper run이 종료될 때:

- `summary.json`이 최종 SSOT가 된다.

`summary.json`에는 다음이 들어간다.

- `realized_pnl_quote`
- `unrealized_pnl_quote`
- `max_drawdown_pct`
- `orders_filled`
- `fill_rate`
- `micro_quality_score_mean`
- `rolling_evidence`


## 4. Biggest Contract Mismatches

현재 가장 헷갈리기 쉬운 부분은 아래 4개가 서로 완전히 같지 않다는 점이다.

1. trainer pure fit metrics
2. trainer execution acceptance backtests
3. acceptance fixed compare backtests
4. actual paper runtime

대표 차이:

- trainer execution acceptance:
  - selection learned recommendation off
- runtime recommendation search:
  - selection learned recommendation on
  - exit/execution learned recommendation off while searching
- acceptance compare:
  - frozen hold-style compare profile
- paper runtime:
  - learned selection
  - learned exit / hold bars / risk settings
  - learned execution settings
  - live feature provider
  - live micro / operational overlay

즉 "acceptance에서 이김"과 "paper에서 지금 이김"은 같은 계약이 아니다.


## 5. Patched Bug Relevant To PnL

`2026-03-19`에 확인된 버그:

- paper/backtest가 유니버스에서 탈락한 포지션을 exit 평가 집합에서 빼버릴 수 있었다.
- 결과적으로 `TIMEOUT / SL`이 걸려야 하는 포지션이 장시간 미청산으로 남을 수 있었다.

패치:

- `paper/backtest`가 이제 실제 전체 오픈 포지션 집합을 `open_markets`로 넘긴다.
- 전략도 방어적으로 `open_markets + self._positions`를 기준으로 exit/position-limit 계산을 한다.

관련 커밋:

- `ed9744c9447f2ec604c5a7ebfd178b8ad1091a4d`


## 6. Debugging Checklist

손익이 이상하면 아래 순서로 본다.

1. trainer run dir:
   - `execution_acceptance_report.json`
   - `runtime_recommendations.json`
2. acceptance report:
   - `logs/model_v4_acceptance/latest.json`
   - `gates.backtest`
   - `gates.paper`
3. current paper run:
   - `events.jsonl`
   - `equity.csv`
   - `orders.jsonl`
   - `fills.jsonl`
4. 종료 후:
   - `summary.json`
5. promote evidence:
   - `logs/model_v4_challenger/latest.json`
   - `autobot.common.paper_lane_evidence`


## Practical Takeaway

현재 구조에서 "손익"은 하나가 아니다.

- trainer는 execution-aware simulated PnL을 만들고
- acceptance는 frozen compare PnL과 paper soak PnL을 본다
- paper runtime은 live-style contract로 실제 running PnL을 만든다

따라서 특정 candidate가 왜 acceptance는 통과했는데 paper는 약한지를 보려면,
항상 "어느 단계의 PnL인지"부터 분리해서 봐야 한다.
