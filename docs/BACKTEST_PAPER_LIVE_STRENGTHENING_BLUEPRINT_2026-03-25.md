# BACKTEST PAPER LIVE STRENGTHENING BLUEPRINT 2026-03-25

## 0. Executive Summary

이 문서는 [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)의 다음 단계 문서다.

앞 문서의 결론이 "학습모델을 강하게 만들려면 `v5_panel -> v5_sequence -> v5_lob -> v5_fusion`으로 가야 한다"였다면, 이번 문서의 결론은 다음이다.

- 강한 학습모델만으로는 충분하지 않다.
- 그 모델을 믿을 수 있게 해 주는 `백테스트 / 페이퍼 / 라이브 검증 방법론`이 같이 강해져야 한다.
- 현재 프로그램은 이미 이 방향으로 상당히 좋은 토대를 갖고 있지만, 아직도 가장 큰 약점은 `evaluation fidelity gap`과 `promotion evidence methodology gap`이다.

현재 코드 기준으로 보면:

- `ModelAlphaStrategyV1`가 backtest/paper/live에서 공유되고
- execution/micro policy contract도 꽤 잘 공유되며
- live는 restart-safe state/reconcile/breaker/rollout까지 갖고 있다.

이건 큰 강점이다.

하지만 여전히 남아 있는 핵심 문제는 아래다.

1. 백테스트는 여전히 기본적으로 `candle-driven simulator`다.
2. paper는 live data plane을 쓰지만 여전히 `실거래 결과`가 아니라 simulator 결과다.
3. live는 진짜 실행이지만, 새 정책을 `counterfactual`로 충분히 식별하는 로그 체계가 아직 약하다.
4. champion/challenger 비교는 aggregate evidence가 강하고, `same opportunity` 기준의 matched comparison이 약하다.
5. promote 판단이 통계적으로 더 강한 `anytime-valid sequential evidence`까지는 아직 가지 못했다.

따라서 지금 필요한 것은 단순한 "더 리얼한 시뮬레이터"가 아니다.

필요한 것은 다음 7가지다.

1. `Fast research backtest`와 `high-fidelity certification replay`를 분리
2. raw WS trade/orderbook + second candle + private order outcome을 연결한 `event replay lane`
3. simulator를 private execution 로그로 보정하는 `execution twin`
4. 행동확률과 대안행동을 기록하는 `counterfactual opportunity log`
5. champion/challenger를 같은 입력 스트림에서 비교하는 `paired paper harness`
6. canary live를 `time-uniform sequential evidence`로 판정하는 승급 계약
7. 100GB 저장 예산 안에서 돌아가는 `hot/warm/cold retention policy`

가장 중요한 결론 하나만 고르면 이것이다.

현재 프로그램을 진짜 한 단계 끌어올리는 1순위 작업은:

- `더 많은 XGBoost 튜닝`이 아니라
- `paired paper + event replay certification + counterfactual logging`이다.

왜냐하면 강한 모델을 만들어도, 그 모델을 현재 방법론으로 검증하면 여전히 "실전에서 이겼는가"보다 "현재 시뮬레이터에서 이겼는가"를 많이 보게 되기 때문이다.


## 1. Relationship To The Training Blueprint

앞 문서의 초점은 `alpha learner`였다.

OCI server access reference:

- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

이번 문서의 초점은 그 alpha learner가 배포되기 전후의 평가/실행 체계다.

두 문서는 다음처럼 연결된다.

- 학습모델 문서:
  - 더 강한 alpha / uncertainty / tradability-aware prediction을 만들자
- 이번 문서:
  - 그 출력을 믿을 수 있게 만드는 evaluation ladder와 runtime evidence contract를 만들자

둘은 분리되어 있지만, 실제 구현은 같이 움직여야 한다.

예를 들어:

- `v5_fusion`이 `final_score`, `uncertainty`, `tradability`를 낸다면
- backtest/paper/live는 그것을 같은 방식으로 소비해야 하고
- promotion은 그 결과를 matched-opportunity 기준으로 비교해야 한다.


## 2. Scope

포함:

- 현재 backtest / paper / live 코드 경로의 분석
- 현재 evidence / promotion 방법론의 분석
- stronger methodology 제안
- 100GB 저장 예산을 고려한 데이터/로그 설계
- 구현 로드맵

포함하지 않는 것:

- alpha 모델 내부 구조의 상세 설계
- feature engineering 자체
- exchange adapter의 언어별 구현 세부

주의:

- Upbit 공식 기능 확인은 Upbit Developer Center 문서를 기준으로 했다.
- 저장소 설정은 `config/upbit.yaml`에서 `api.upbit.com`과 KRW 마켓을 사용한다.
- 따라서 `global-docs.upbit.com` 기준 문서와 한국 실환경 가용성은 배포 전에 한 번 더 대조해야 한다.


## 3. Current System Reconstruction

### 3.1 Shared Signal Contract

현재 시스템의 가장 큰 강점은 `전략 로직 공유`다.

핵심 공유 축:

- [model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)
- [predictor.py](/d:/MyApps/Autobot/autobot/models/predictor.py)
- [model_alpha_runtime_contract.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_runtime_contract.py)
- [live_execution_policy.py](/d:/MyApps/Autobot/autobot/models/live_execution_policy.py)
- [execution_risk_control.py](/d:/MyApps/Autobot/autobot/models/execution_risk_control.py)

의미:

- backtest, paper, live가 완전히 다른 전략을 쓰는 구조가 아니다.
- 같은 predictor artifact와 같은 strategy class를 가능한 한 공유한다.
- 이 점은 매우 좋다. 많은 트레이딩 시스템이 여기서부터 이미 무너진다.


### 3.2 Backtest Current Architecture

중심 파일:

- [backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)
- [backtest/exchange.py](/d:/MyApps/Autobot/autobot/backtest/exchange.py)
- [paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)
- [execution/order_supervisor.py](/d:/MyApps/Autobot/autobot/execution/order_supervisor.py)

현재 backtest는 다음 구조다.

1. candle dataset 로드
2. static start universe 선정
3. strategy on_ts 실행
4. intent 생성
5. trade gate / micro gate / micro order policy / learned execution policy 적용
6. simulator submit / fill / cancel / replace
7. fill records / timing / slippage / execution structure 요약

좋은 점:

- event-driven engine 구조가 깔끔하다.
- `best/ioc/fok` immediate taker 경로와 resting limit 경로를 구분하려고 노력했다.
- partial fill, first fill vs complete fill, slippage, execution_structure 등 보고 체계가 좋다.
- `execution_contract`를 읽어 learned execution action을 실제 backtest에 연결한다.

한계:

- 기본 시간축은 여전히 candle bar다.
- raw orderbook / trade replay가 있더라도 기본 lane이 `exchange-grade replay`는 아니다.
- hidden liquidity, queue aging, participant-level arrivals/cancels, latency jitter는 근사치다.
- `static_start` universe는 live top-N universe drift를 완전히 재현하지 못한다.


### 3.3 Paper Current Architecture

중심 파일:

- [paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
- [paper/live_features_v4.py](/d:/MyApps/Autobot/autobot/paper/live_features_v4.py)
- [paper/live_features_v4_native.py](/d:/MyApps/Autobot/autobot/paper/live_features_v4_native.py)
- [paper/live_features_online_core.py](/d:/MyApps/Autobot/autobot/paper/live_features_online_core.py)

현재 paper는 다음 구조다.

1. 실시간 ticker WS 구독
2. top-N universe 갱신
3. live-like feature provider로 현재 row 생성
4. same strategy class 실행
5. same micro/execution stack 적용
6. paper simulator 체결
7. rolling paper evidence 축적

좋은 점:

- backtest보다 live에 훨씬 가깝다.
- live WS micro provider를 그대로 쓸 수 있다.
- operational overlay, runtime risk multiplier, micro quality metrics까지 집계한다.
- `paper_lane_evidence`와 canary report 체계가 이미 있다.

한계:

- 여전히 simulator fill이다.
- champion/challenger가 꼭 같은 순간 같은 opportunity를 exact matched 방식으로 비교되는 것은 아니다.
- paper evidence는 aggregate quality는 좋지만, "동일 opportunity 조건에서 challenger가 champion보다 나은가"를 직접 보지는 않는다.


### 3.4 Live Current Architecture

중심 파일:

- [live/model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
- [live/model_alpha_runtime_execute.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)
- [live/model_alpha_runtime_supervisor.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_supervisor.py)
- [live/daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)
- [live/reconcile.py](/d:/MyApps/Autobot/autobot/live/reconcile.py)
- [live/state_store.py](/d:/MyApps/Autobot/autobot/live/state_store.py)
- [risk/live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)

현재 live는 다음 구조다.

1. runtime model contract pinning
2. public WS + private WS + REST reconcile
3. same strategy evaluation
4. admissibility / trade gate / execution risk control / canary caps
5. submit through direct REST or executor gateway
6. state store / private ws / reconcile / breaker / rollout로 운영 안전성 보장

좋은 점:

- restart safety가 강하다.
- state store와 reconcile이 잘 되어 있다.
- rollout, canary, breaker, runtime handoff가 있다.
- order lineage, risk plan, trade journal, execution attempt DB가 있다.

한계:

- 새 정책의 counterfactual outcome을 충분히 식별할 로그가 아직 약하다.
- execution policy는 action을 고르지만 행동확률(propensity)을 일반적인 OPE 친화적 형식으로 저장하지 않는다.
- live outcome은 진짜이지만, "왜 이겼는가"를 policy level로 분해해 재학습하는 루프가 아직 완전하지 않다.


### 3.5 Current Evidence / Promotion Path

관련 축:

- [execution_acceptance.py](/d:/MyApps/Autobot/autobot/models/execution_acceptance.py)
- [execution_validation.py](/d:/MyApps/Autobot/autobot/models/execution_validation.py)
- [paper_lane_evidence.py](/d:/MyApps/Autobot/autobot/common/paper_lane_evidence.py)
- [candidate_canary_report.py](/d:/MyApps/Autobot/autobot/live/candidate_canary_report.py)
- [candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1)
- [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)

현재 evidence는 다음을 합친다.

- offline walk-forward
- execution-aware backtest comparison
- rolling paper lane evidence
- canary / candidate journal reports
- scripted acceptance / adoption / promote steps

좋은 점:

- 여러 단계 evidence를 보려고 한다.
- 완전히 한 지표만 보는 구조가 아니다.

한계:

- matched-opportunity paired comparison이 부족하다.
- live canary evidence가 더 강한 sequential statistics로 정식화되지는 않았다.
- trainer evidence, execution acceptance, paper evidence, canary evidence가 같은 language로 연결된 단일 `belief update contract`까지는 아직 가지 못했다.


### 3.6 Local Verification Note

이번 분석 중 아래 대표 테스트들을 직접 실행했고 모두 통과했다.

- `tests/test_backtest_fill_model.py`
- `tests/test_backtest_exchange.py`
- `tests/test_execution_reporting_parity.py`
- `tests/test_paper_fill_model.py`
- `tests/test_paper_engine_integration.py`
- `tests/test_paper_live_feature_provider_v4.py`
- `tests/test_live_reconcile.py`
- `tests/test_live_breakers.py`
- `tests/test_live_execution_policy.py`
- `tests/test_paper_lane_evidence.py`
- `tests/test_live_rollout.py`
- `tests/test_execution_acceptance.py`
- `tests/test_execution_validation.py`

합계 111개 테스트가 통과했다.

이는 현재 코드가 적어도 계약 측면에서 상당히 단단하다는 뜻이다.
즉 이번 문서의 제안은 "지금 코드가 엉망이라 처음부터 갈아엎자"가 아니라, "이미 좋은 뼈대 위에 methodology를 한 단계 올리자"에 가깝다.


## 4. What Is Already Good

이 시스템은 이미 아래를 갖고 있다.

1. `strategy contract sharing`
2. `micro-aware execution policy`
3. `paper/live feature provider sharing`
4. `execution timing / slippage / payoff structure reporting`
5. `restart-safe live state machine`
6. `paper lane and canary evidence tooling`
7. `promotion governance artifacts`

이걸 과소평가하면 안 된다.

실제로 많은 자동매매 시스템은:

- backtest는 다른 전략
- paper는 다른 feature
- live는 다른 execution path

로 분리되어 있어서 결과를 신뢰할 수 없다.

현재 저장소는 그 문제를 상당 부분 피해 갔다.


## 5. Where The Methodology Is Still Weak

### 5.1 Backtest Fidelity Ceiling

현 backtest는 잘 만든 `bar/snapshot hybrid simulator`지만, 기본적으로는 아직 exchange-grade message replay가 아니다.

남는 문제:

- queue priority
- hidden liquidity
- participant arrivals/cancels
- true latency and out-of-order event timing
- same-opportunity exact replay


### 5.2 Paper Is Better Than Backtest, But Not Matched-Pair Evidence

paper는 live market data를 쓰므로 훨씬 낫다.

하지만 현재 evidence는 보통 아래 식이다.

- champion runs aggregate
- challenger runs aggregate
- window 내 집계 비교

이 방식은 강하지만, 여전히 다음 리스크가 있다.

- 두 모델이 정확히 같은 market set / same tick / same opportunity를 보지 않았을 수 있음
- regime drift가 aggregate comparison을 오염시킬 수 있음


### 5.3 Logged Policy Evaluation Is Weak

현재 live/paper는 action 결과를 저장하지만, OPE 친화적인 형태는 아직 부족하다.

빠진 것:

- candidate action set 전체
- 선택 행동의 propensity
- no-trade decision propensity
- counterfactual feature snapshot

이게 없으면 새 정책을 과거 로그로 robust하게 평가하기 어렵다.


### 5.4 Execution Model Identification Is Still Hard

현재 execution learner는:

- state bucket
- pseudo-count
- miss cost model

중심이다.

좋은 실전형 구조지만, 더 강하게 만들려면 아래가 필요하다.

- personalized private execution logs
- state-dependent fill probability model
- queue-reactive calibration
- partial fill / replace / cancel transition model


### 5.5 Promotion Uses Strong Gates But Not Yet Strongest Statistics

현재 canary / paper / acceptance는 꽤 보수적이다.

하지만 아직 아래까지 가진 않았다.

- anytime-valid sequential lower confidence bound
- time-uniform stopping rule for promote / abort
- paired live shadow evidence with explicit null hypothesis


### 5.6 The Biggest Missing Concept: Opportunity Matching

강한 평가의 핵심은:

- "총 수익이 더 좋다"보다
- "같은 opportunity에서 더 좋다"다.

현재 프로그램은 opportunity logging이 부분적이다.

강한 방법론은 모든 lane에서 아래를 기록해야 한다.

- decision timestamp
- market universe
- feature hash / score bundle
- candidate set
- chosen action / skipped reason
- realized outcome


## 6. Stronger End-To-End Methodology

## 6.1 Build A Truth Ladder

평가 계층을 아래처럼 명확히 나눈다.

### L0. Fast Research Backtest

목적:

- alpha research
- ablation
- broad search

특징:

- 현재 `backtest/engine.py` 중심
- 빠름
- 많은 시도 가능

판정:

- 연구용 prior
- promote 근거의 일부일 뿐


### L1. Snapshot-Aware Certification Backtest

목적:

- execution-aware certification

특징:

- second candle + micro snapshot + historical orderbook overlay 사용
- 현재보다 더 보수적인 fill model

판정:

- fast lane보다 강한 certification prior


### L2. Event Replay Certification

목적:

- high-fidelity replay

특징:

- raw WS trade/orderbook event replay
- event clock 기준 simulator
- queue-reactive calibration

판정:

- simulator 기반 최종 offline certification


### L3. Paired Paper Shadow

목적:

- live data plane에서 matched-opportunity comparison

특징:

- champion / challenger가 같은 입력 스트림을 동시에 봄
- 같은 decision clock
- 같은 market universe

판정:

- online but non-capital evidence


### L4. Canary Live

목적:

- 실제 자본/체결 환경에서 bounded-risk evidence

특징:

- tiny size
- sequential test
- anytime-valid abort / promote logic

판정:

- production gate


### L5. Champion Live

목적:

- production

특징:

- only after L0-L4 pass


## 6.2 Keep The Current Candle Backtest, But Reposition It

현재 backtest는 버릴 필요가 없다.

다만 의미를 바꿔야 한다.

- 지금: execution acceptance의 핵심 판정 축 중 하나
- 권장: `fast research lane`

즉:

- 속도와 연구 반복성 담당
- 최종 promote evidence의 단독 주체는 아님


## 6.3 Add A Dedicated Event Replay Certification Lane

새 엔진을 추가한다.

권장 파일:

- `autobot/backtest/event_replay_engine.py`
- `autobot/backtest/event_replay_exchange.py`
- `autobot/backtest/replay_loader.py`

입력 데이터:

- raw WS trades
- raw WS orderbook
- second candles
- tick size / chance snapshots

핵심 아이디어:

- bar close가 아니라 event timestamp가 시간의 기준
- order submit / replace / cancel / fill을 event stream과 interleave
- queue state를 per-price level 단위로 유지

이건 ABIDES 같은 fully fledged exchange sim까지는 아니더라도, 현재 구조보다 훨씬 강한 certification lane이 된다 [E1].


## 6.4 Calibrate The Simulator With A Personalized Execution Twin

실거래 자동매매에서는 public market data만으로는 execution outcome을 다 알 수 없다.

새로 필요한 것:

- `MyOrder` private WS
- local `execution_attempts`
- `trade_journal`

이걸 합쳐 아래 모델을 만든다.

### Execution Twin Outputs

- `P(first_fill <= t | state, action)`
- `P(full_fill <= t | state, action)`
- expected shortfall
- partial-fill probability
- replace probability
- cancel probability

권장 모델 축:

- survival / hazard model
- state-dependent fill model
- queue-reactive calibration layer

이 방향은 LOB fill probability와 survival literature와 잘 맞는다 [E2][E3][E4].


## 6.5 Add Counterfactual Opportunity Logging

강한 OPE를 하려면 로그가 바뀌어야 한다.

현재 기록보다 더 필요한 것:

- 당시 feature snapshot hash
- predictor outputs
- candidate action set 전체
- 선택 action
- 각 action의 predicted utility
- behavior policy propensity
- no-trade decision 이유

새 artifact 예시:

- `opportunity_log.parquet`
- `counterfactual_action_log.parquet`

컬럼 예시:

- `ts_ms`
- `market`
- `run_id`
- `lane`
- `feature_hash`
- `selection_score`
- `uncertainty`
- `expected_edge_bps`
- `candidate_actions_json`
- `chosen_action`
- `chosen_action_propensity`
- `skip_reason_code`
- `realized_outcome_json`


## 6.6 Use Off-Policy Evaluation For Execution Policy

새 execution policy를 매번 full canary로 검증하는 것은 비효율적이다.

따라서 `execution action policy`는 contextual bandit으로 보고 OPE를 추가하는 것이 좋다.

문맥(context):

- micro state
- alpha score
- uncertainty
- spread / depth / age / tick size

행동(action):

- skip
- passive maker
- join
- cross
- ord_type / tif variant

보상(reward):

- realized execution utility
- implementation shortfall adjusted edge

권장 OPE:

- doubly robust OPE
- shrinkage / clipping 기반 finite-sample 안정화

이 방향은 contextual bandit OPE literature와 맞고, deterministic policy 로그만으로는 한계가 있으므로 약한 탐색이 병행되어야 한다 [E5].


## 6.7 Add Controlled Exploration, But Only In Safe Lanes

OPE가 제대로 되려면 behavior policy가 모든 action에 약간의 확률질량을 줘야 한다.

하지만 live에서 무작정 탐색하면 위험하다.

권장:

- `paper paired lane`에서 탐색 적극 사용
- `canary live`에서는 tiny-size, safe-state에서만 제한적 탐색
- `champion live`에서는 기본적으로 탐색 없음

탐색 예시:

- top 2 execution actions 사이 randomized tie-break
- uncertainty high 구간 only
- notional very small 구간 only


## 6.8 Replace Aggregate Paper Comparison With Paired Paper Harness

새 paper methodology의 핵심은 이것이다.

현재:

- champion paper service
- challenger paper service
- 나중에 aggregate summary 비교

권장:

- 하나의 WS feed fanout
- 하나의 event clock
- 두 strategy를 동시에 실행
- 두 exchange clone을 동시에 돌림
- 동일 opportunity에서 직접 비교

권장 파일:

- `autobot/paper/paired_engine.py`
- `autobot/paper/paired_reporting.py`

핵심 출력:

- matched-opportunity pnl delta
- matched fill delta
- matched slippage delta
- matched no-trade delta
- disagreement taxonomy


## 6.9 Make Canary Live A Sequential Test, Not Just A Threshold List

canary live evidence는 매 시점 모니터링되고 언제든 멈출 수 있어야 한다.

권장 방식:

- closed trade 단위 또는 matched opportunity 단위 reward stream 정의
- challenger minus champion delta stream 생성
- time-uniform confidence sequence 또는 e-value 기반 모니터링

의사결정:

- LCB > 0 and risk constraints satisfied -> promote eligible
- UCB < 0 or drawdown / divergence breach -> abort
- otherwise continue

이런 식의 anytime-valid monitoring은 current breaker philosophy와도 잘 맞는다.
현재 코드에도 martingale/e-process 계열 사고방식이 이미 일부 들어 있으므로 확장성이 좋다 [E6].


## 6.10 Add Live Divergence Monitoring As A First-Class Artifact

promotion에 중요한 것은 pnl만이 아니다.

다음 divergence를 같이 봐야 한다.

### Feature Divergence

- same ts, same market에서 paper/live feature mismatch

### Decision Divergence

- same context에서 champion/challenger 선택 행동 차이

### Execution Divergence

- expected fill vs actual fill 차이
- expected time-to-fill vs actual

### Outcome Divergence

- expected edge vs realized edge

권장 산출물:

- `live_divergence_report.json`
- `paper_live_parity_report.json`


## 7. Upbit Data Requirements For The Stronger Runtime Methodology

## 7.1 Already Useful

현재도 유용한 데이터:

- public ticker WS
- public trade WS
- public orderbook WS
- recent trades REST
- orderbook instruments
- order chance
- account balances
- open/closed order endpoints


## 7.2 High-Value Additions

추가적으로 특히 중요한 것:

1. `1s candles`
2. `WebSocket candle`
3. `30-level orderbook`
4. `MyOrder`
5. `MyAsset`

의미:

- `1s candles`는 event replay의 scaffold가 된다.
- `WebSocket candle`은 live/paper feature clock을 더 안정적으로 맞춰 준다.
- `30-level orderbook`은 certification replay와 LOB execution twin의 핵심이다.
- `MyOrder`는 personalized fill model의 핵심 라벨이다.
- `MyAsset`은 account-state conditional policy의 보조 입력이다.


## 7.3 Official Upbit Implications

공식 문서 기준으로 확인한 중요한 점:

- Upbit는 `List Second Candles`를 제공한다 [U1].
- WebSocket candle은 `1s`, `1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `60m`, `240m`를 실시간으로 지원한다고 공지했다 [U2].
- orderbook은 2025년 5월부터 30 levels로 확장되었고, WebSocket `orderbook_units`도 15 -> 30으로 늘어났다 [U3].
- `Get Available Order Info`는 지정 pair의 order availability 정보를 준다 [U4].
- `MyOrder`와 `MyAsset`은 private WebSocket으로 개인 주문/자산 데이터를 준다 [U5][U6].

이 조합이면 현재보다 훨씬 강한 certification/live feedback loop가 가능하다.


## 8. 100GB Storage Budget Plan

100GB 정도의 가용 용량이면 아래 정도가 현실적이다.

### 8.1 Recommended Budget

| 영역 | 예산 | 비고 |
| --- | ---: | --- |
| raw/public WS trade + orderbook hot window | 25 GB | top 20~30 시장, rolling retention |
| 30-level orderbook compressed certification store | 20 GB | certification용 priority 저장 |
| second candles / websocket candle cache | 8 GB | 1s/1m 중심 |
| private execution / MyOrder / MyAsset / attempt export | 7 GB | long retention 가능 |
| opportunity / counterfactual / paired paper logs | 10 GB | 가장 ROI 높음 |
| backtest / paper / live run artifacts | 15 GB | summary + key event logs |
| model / oof / calibration / temp cache | 10 GB | training + replay cache |
| free headroom | 5 GB | 안전 여유 |


### 8.2 Retention Policy

권장:

- Hot:
  - 최근 14~30일 raw orderbook/trade
- Warm:
  - second candles, paired paper logs, execution attempts 90일+
- Cold:
  - aggregated artifacts, summaries, model outputs 장기 보존

핵심:

- raw 30-level orderbook를 무기한 다 저장하지 말고
- certification / replay에 필요한 구간만 hot/warm으로 유지
- 대신 `opportunity_log`, `execution_attempts`, `trade_journal`, `paper paired evidence`는 길게 보관


## 9. Concrete Repository Roadmap

## 9.1 New Datasets

권장 추가 dataset:

- `data/parquet/candles_second_v1`
- `data/parquet/ws_candle_v1`
- `data/parquet/lob30_v1`
- `data/parquet/execution_attempt_export_v1`
- `data/parquet/opportunity_log_v1`
- `data/parquet/paired_paper_v1`


## 9.2 New Python Modules

권장 모듈:

- `autobot/backtest/event_replay_engine.py`
- `autobot/backtest/event_replay_exchange.py`
- `autobot/backtest/replay_loader.py`
- `autobot/models/execution_calibration.py`
- `autobot/models/offpolicy_evaluation.py`
- `autobot/models/confidence_sequences.py`
- `autobot/paper/paired_engine.py`
- `autobot/paper/paired_reporting.py`
- `autobot/live/divergence_monitor.py`
- `autobot/ops/promotion_evidence.py`


## 9.3 New Artifacts

권장 artifact:

- `event_replay_certification_report.json`
- `paired_paper_report.json`
- `opportunity_log.parquet`
- `counterfactual_action_log.parquet`
- `execution_calibration_report.json`
- `live_divergence_report.json`
- `promotion_confidence_sequence.json`


## 9.4 New CLI / Script Layer

권장 명령 또는 스크립트:

- `autobot replay certify`
- `autobot paper paired-run`
- `autobot live export-opportunities`
- `autobot model ope-execution`
- `scripts/render_paired_paper_report.py`
- `scripts/report_live_divergence.py`


## 10. Recommended First Build Order

## 10.1 Phase A: Highest ROI

먼저 할 것:

1. `opportunity_log` 추가
2. `paired paper harness` 추가
3. `paper lane evidence`를 matched-opportunity 방식으로 업그레이드

이유:

- 구현 난이도 대비 효과가 가장 크다.
- 새 학습모델이 나오기 전에도 즉시 효용이 있다.
- promotion quality를 가장 빨리 올릴 수 있다.


## 10.2 Phase B: Certification Replay

다음:

1. `lob30_v1` 저장
2. `event replay certification lane`
3. simulator calibration

이유:

- backtest 신뢰도를 실질적으로 올린다.
- execution policy / fill model 재학습 근거가 생긴다.


## 10.3 Phase C: OPE and Sequential Promotion

그 다음:

1. 행동 propensity logging
2. DR-OPE
3. time-uniform confidence sequence canary evidence

이유:

- full live test를 줄일 수 있다.
- promote 판단이 훨씬 더 principled해진다.


## 11. Strongest Combined Methodology With The Training Blueprint

학습모델 문서와 이번 문서를 합치면 가장 강한 방법론은 아래다.

### 11.1 Train

- `v5_panel / sequence / lob / fusion`
- uncertainty + tradability 포함

### 11.2 Offline Research

- fast backtest로 broad search
- event replay certification으로 narrowed candidate 검증

### 11.3 Online Non-Capital Validation

- paired paper harness
- matched-opportunity evidence
- divergence monitoring

### 11.4 Online Capital Validation

- tiny canary
- time-uniform sequential evidence
- drawdown / divergence / execution failure abort

### 11.5 Promote

- only if:
  - offline certification pass
  - paired paper matched evidence pass
  - canary sequential LCB pass
  - operational health clear

이게 현재 저장소의 운영 철학과 가장 잘 맞는 `strong model + strong methodology` 조합이다.


## 12. What Not To Do

### 12.1 Do Not Replace Everything With One Giant Simulator

하나의 ultra-realistic simulator로 모든 연구를 돌리려 하면 너무 느리다.

정답은:

- fast lane
- certification lane

분리다.


### 12.2 Do Not Promote Based Only On Aggregate Paper PnL

aggregate PnL은 regime mix와 opportunity mismatch에 오염된다.

matched opportunity evidence가 필요하다.


### 12.3 Do Not Add OPE Without Propensity Logging

행동확률이 없으면 OPE는 쉽게 왜곡된다.


### 12.4 Do Not Use Live Canary As A Blind Runtime Smoke Test

canary는 단순 "돌아간다"가 아니라 statistical evidence lane이어야 한다.


## 13. Final Recommendation

현재 프로그램은 이미 운영형 자동매매 시스템으로서 꽤 강하다.

다만 다음 단계의 병목은 학습모델보다 오히려:

- evaluation fidelity
- matched evidence
- counterfactual logging
- sequential promotion statistics

이다.

가장 높은 ROI의 다음 세 가지는 명확하다.

1. `paired paper harness`
2. `opportunity/counterfactual logging`
3. `event replay certification lane`

그리고 그 다음이:

4. `personalized execution twin`
5. `DR-OPE`
6. `canary confidence sequence`

100GB 정도의 용량이 있다면, 이 로드맵은 충분히 현실적이다.
모든 raw 데이터를 영구 보관하는 것이 아니라, `high-fidelity replay에 꼭 필요한 hot data`와 `정책 식별에 꼭 필요한 opportunity/execution logs`에 우선권을 주는 것이 핵심이다.


## 14. References

### Current Project Documents

- [D1] [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [D2] [LIVE_PARITY_EXECUTION_SIMULATOR_DESIGN_2026-03-24.md](/d:/MyApps/Autobot/docs/LIVE_PARITY_EXECUTION_SIMULATOR_DESIGN_2026-03-24.md)
- [D3] [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)


### Research / Methodology

- [E1] Byrd, Hybinette, Hybinette Balch, *ABIDES: Towards High-Fidelity Market Simulation for AI Research*  
  https://arxiv.org/abs/1904.12066

- [E2] Lokin, Yu, *Fill Probabilities in a Limit Order Book with State-Dependent Stochastic Order Flows*  
  https://arxiv.org/abs/2403.02572

- [E3] Bodor, Carlier, *A Novel Approach to Queue-Reactive Models: The Importance of Order Sizes*  
  https://arxiv.org/abs/2405.18594

- [E4] Arroyo et al., *Deep Attentive Survival Analysis in Limit Order Books*  
  https://arxiv.org/abs/2306.05479

- [E5] Su, Dimakopoulou, Krishnamurthy, Dudik, *Doubly Robust Off-Policy Evaluation with Shrinkage*  
  https://proceedings.mlr.press/v119/su20a.html

- [E6] Howard, Ramdas, McAuliffe, Sekhon, *Time-uniform Chernoff Bounds via Nonnegative Supermartingales*  
  https://arxiv.org/abs/1808.03204


### Official Upbit Documentation

- [U1] List Second Candles  
  https://global-docs.upbit.com/reference/list-candles-seconds

- [U2] WebSocket Candle minute interval addition notice  
  https://global-docs.upbit.com/changelog/websocket_candles_miniutes

- [U3] 30-Level Order Book expansion  
  https://global-docs.upbit.com/v1.2.2/changelog/orderbook_expansion

- [U4] Get Available Order Info  
  https://global-docs.upbit.com/reference/available-order-information

- [U5] MyOrder WebSocket  
  https://global-docs.upbit.com/reference/websocket-myorder

- [U6] MyAsset WebSocket  
  https://global-docs.upbit.com/reference/websocket-myasset

- [U7] Create Order  
  https://global-docs.upbit.com/reference/new-order

- [U8] WebSocket Orderbook  
  https://global-docs.upbit.com/reference/websocket-orderbook

- [U9] List Trading Pairs  
  https://global-docs.upbit.com/reference/list-trading-pairs

- [U10] Recent Trades History  
  https://global-docs.upbit.com/reference/recent-trades-history

- [U11] List Orderbook Instruments  
  https://global-docs.upbit.com/reference/list-orderbook-instruments
