# RISK AND LIVE CONTROL STRENGTHENING BLUEPRINT 2026-03-25

## 0. Executive Summary

이 문서는 앞서 작성한 두 문서:

- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)

의 세 번째 축이다.

이번 문서의 초점은:

- 매수/매도 sizing
- 진입 허용 여부
- 청산/보호 주문
- 온라인 halt / breaker
- canary/live 운영 중 risk budget

이다.

현재 코드 기준으로 보면 이 저장소는 이미 생각보다 강한 risk/live control 뼈대를 갖고 있다.

이미 있는 것:

- `trade_action`
- `execution_risk_control`
- `size_ladder`
- `online_adaptation`
- `martingale/e-process halt`
- `live admissibility`
- `small_account`
- `protective risk manager`
- `dynamic exit overlay`
- `path risk guidance`
- `breaker / rollout / reconcile`

즉 문제는 "리스크 관리가 없다"가 아니다.

문제는 더 미묘하다.

1. 일부 risk layer가 아직도 `second alpha selector`처럼 동작할 위험이 있다.
2. 대부분 risk logic이 `single-trade local logic` 중심이고 `portfolio-level capital-at-risk engine`이 약하다.
3. uncertainty와 distributional prediction을 upstream alpha에서 충분히 직접 받지 못한다.
4. live halt 판단은 좋아지고 있지만, 아직 `time-uniform statistical control plane`까지는 완전히 가지 않았다.
5. protective exit는 많이 좋아졌지만 아직 `optimal stopping + execution-aware liquidation`의 완성형은 아니다.

따라서 가장 강한 방향은 다음과 같다.

- alpha는 `what to buy`
- tradability는 `can I actually execute it`
- risk system은 `how much, under what budget, and when to stop`

를 담당하게 역할을 분리해야 한다.

이번 문서의 최종 제안은 아래다.

1. `white-box portfolio risk budget engine`을 추가한다.
2. `risk_control`은 hard alpha veto가 아니라 `size, halt, degradation, liquidation` 중심 safety engine이 된다.
3. upstream 모델에서 `expected return + ES/CVaR proxy + uncertainty + tradability`를 받아 risk budget 계산에 직접 쓴다.
4. live에서는 `account state + opportunity log + execution twin + confidence sequence`를 합쳐 dynamic risk state를 관리한다.
5. canary/promotion은 pnl 하나가 아니라 `risk-adjusted lower confidence bound`와 `severe loss / divergence control`을 같이 본다.


## 1. Relationship To The Other Two Blueprints

세 문서는 각각 다른 레이어를 다룬다.

OCI server access reference:

- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

### 문서 1: 학습모델

- 더 강한 predictor
- uncertainty
- tradability-aware outputs

### 문서 2: 평가와 실행 검증

- 더 강한 backtest
- paired paper
- event replay certification
- counterfactual logging

### 이번 문서 3: risk/live control

- sizing
- risk budget
- exits
- breakers
- live online protection

세 문서를 합치면 다음 구조가 된다.

- predictor outputs
  -> evaluation ladder
  -> risk allocation
  -> live monitoring
  -> promotion decision


## 2. Scope

포함:

- 현재 리스크/라이브 제어 코드 분석
- 현재 약점
- 더 강한 risk/live methodology
- 공식 Upbit 데이터 계약과의 연결
- 구현 제안

제외:

- alpha 모델 내부 설계 자체
- simulator fidelity 자체

주의:

- Upbit private/public API 가용성은 Upbit Developer Center 기준으로 확인했다.
- 저장소 설정은 `config/upbit.yaml`의 `api.upbit.com`과 KRW 마켓을 사용한다.
- 한국 실환경 parity는 배포 전 재검증이 필요하다.


## 3. Current Local Architecture

### 3.1 Core Modules

핵심 파일:

- [execution_risk_control.py](/d:/MyApps/Autobot/autobot/models/execution_risk_control.py)
- [trade_action_policy.py](/d:/MyApps/Autobot/autobot/models/trade_action_policy.py)
- [model_alpha_runtime_execute.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)
- [live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- [admissibility.py](/d:/MyApps/Autobot/autobot/live/admissibility.py)
- [small_account.py](/d:/MyApps/Autobot/autobot/live/small_account.py)
- [dynamic_exit_overlay.py](/d:/MyApps/Autobot/autobot/common/dynamic_exit_overlay.py)
- [path_risk_guidance.py](/d:/MyApps/Autobot/autobot/common/path_risk_guidance.py)
- [operational_overlay_v1.py](/d:/MyApps/Autobot/autobot/strategy/operational_overlay_v1.py)
- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)
- [model_risk_plan.py](/d:/MyApps/Autobot/autobot/live/model_risk_plan.py)
- [state_store.py](/d:/MyApps/Autobot/autobot/live/state_store.py)


### 3.2 Current Decision Chain

현재 진입 시 의사결정 체인은 대략 이렇다.

1. predictor score / selection
2. `trade_action`
3. `risk_control` static / size ladder / online state
4. `operational overlay`
5. `small_account` and `live admissibility`
6. `execution policy`
7. submit

청산 시에는:

1. model exit plan
2. path risk guidance
3. dynamic exit overlay
4. risk manager protective order
5. replace / cancel / close


### 3.3 What Is Already Strong

현재 코드의 강점은 다음이다.

#### 3.3.1 Admissibility Is White-Box

[admissibility.py](/d:/MyApps/Autobot/autobot/live/admissibility.py)는:

- tick size
- min total
- free balance
- fee reserve
- dust remainder
- expected edge after estimated cost

를 명시적으로 검사한다.

이건 매우 좋다.


#### 3.3.2 Small-Account Reality Is Explicit

[small_account.py](/d:/MyApps/Autobot/autobot/live/small_account.py)는:

- cost breakdown
- min total
- fee reserve
- canary slot violations

를 별도 계층으로 둔다.

이건 실계정 bot에서 매우 중요한 설계다.


#### 3.3.3 Protective Exit Is Event-Sourced

[live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)는:

- risk plan
- exit trigger
- replace
- cancel reject handling
- breaker recovery

를 event 기반으로 관리한다.

단순 `if price < stop: sell`보다 훨씬 낫다.


#### 3.3.4 Online Risk Halt Already Exists

[execution_risk_control.py](/d:/MyApps/Autobot/autobot/models/execution_risk_control.py)와 [model_alpha_runtime_execute.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)는 이미:

- online threshold adaptation
- Hoeffding UCB
- martingale/e-process style halt state

를 갖고 있다.

즉 완전히 primitive한 시스템이 아니다.


### 3.4 Local Validation Note

이번 분석 중 아래 대표 테스트를 직접 실행했고 모두 통과했다.

- `tests/test_live_risk_manager.py`
- `tests/test_exit_path_risk.py`
- `tests/test_operational_overlay_v1.py`
- `tests/test_operational_overlay_calibration.py`
- `tests/test_execution_risk_control.py`
- `tests/test_trade_action_policy.py`
- `tests/test_runtime_recommendations.py`

합계 73개 테스트 통과.

따라서 이번 문서의 제안은 현재 코드의 부정을 전제로 하지 않는다.
현재 기반은 좋고, 이제 철학과 계층을 더 명확히 정리해야 한다는 쪽이다.


## 4. Current Weaknesses

### 4.1 Risk Ownership Is Not Fully Clean

현재도 [RISK_CONTROL_SAFETY_LAYER_REDESIGN.md](/d:/MyApps/Autobot/docs/RISK_CONTROL_SAFETY_LAYER_REDESIGN.md)에서 지적하듯, risk control이 alpha ownership을 가져가면 안 된다.

문제는 다음이다.

- selection이 candidate를 고른다
- trade_action이 action과 requested notional을 정한다
- risk_control이 또 threshold gate를 가지면 second alpha가 된다

현재 코드가 이걸 많이 정리했지만, 앞으로도 강한 원칙이 필요하다.


### 4.2 Portfolio Layer Is Too Thin

현재 시스템은 아래를 잘 본다.

- per-trade admissibility
- per-position protective exit
- max positions total

하지만 아래는 아직 약하다.

- gross exposure budget
- cluster exposure budget
- correlated market concentration
- BTC/ETH beta concentration
- liquidity concentration
- time-of-day conditional aggregate risk

즉 `single-position local safety`는 강한데 `portfolio-aware capital allocation`은 아직 약하다.


### 4.3 Most Risk Inputs Are Still Derived Heuristically

현재 risk logic은 종종 아래 입력을 쓴다.

- expected_edge
- expected_es
- tail_probability
- micro_quality_score

좋지만 많은 부분이 upstream strong model output이라기보다:

- OOS replay
- bucketed estimate
- heuristic overlay

에서 온다.

강한 시스템으로 가려면:

- uncertainty
- downside quantile
- ES/CVaR proxy
- tradability probability

를 upstream model이 더 직접 공급해야 한다.


### 4.4 Exit Logic Is Improved, But Not Yet Optimal Stopping

[dynamic_exit_overlay.py](/d:/MyApps/Autobot/autobot/common/dynamic_exit_overlay.py)와 [path_risk_guidance.py](/d:/MyApps/Autobot/autobot/common/path_risk_guidance.py)는 많이 좋아졌다.

하지만 아직도 핵심은:

- tightened TP/SL
- continuation capture heuristic

에 가까운 편이다.

즉:

- exit now value
- continue value
- liquidation cost
- alpha decay

를 명시적으로 비교하는 완전한 stopping controller는 아니다.


### 4.5 Breakers Are Rich, But Statistical Semantics Are Heterogeneous

[breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)는 실무적으로 매우 좋다.

하지만 reason code가 많아질수록 필요한 것이 있다.

- 어떤 reason은 deterministic safety rule
- 어떤 reason은 statistical deterioration
- 어떤 reason은 infrastructure failure

이 세 가지가 섞이면 운영 판단이 어려워진다.

즉 `breaker taxonomy`를 더 엄격히 해야 한다.


### 4.6 Live Risk Uses Realized History, But Counterfactual History Is Weak

현재 online adaptation은 실제 closed trade history를 본다.

좋다.

하지만 부족한 것:

- "이 trade가 아니라 다른 size/action이었으면?"
- "이때 skip이 더 나았는가?"
- "같은 alpha라도 market cluster risk 때문에 줄였어야 했는가?"

즉 live risk memory가 factual history에는 강하지만 counterfactual policy evaluation에는 약하다.


## 5. Stronger Methodology

## 5.1 Cleanly Separate Three Responsibilities

강한 구조의 핵심은 아래 세 역할 분리다.

### Alpha Ownership

- 무엇을 살지/안 살지
- 어떤 시장이 상대적으로 우수한지

담당:

- selection
- trade_action
- predictor uncertainty / tradability outputs


### Safety Ownership

- 얼마나 살지
- 어떤 조건에서는 아예 줄이거나 정지할지
- 포지션을 어떻게 청산할지

담당:

- portfolio risk budget
- admissibility
- small account
- live risk manager
- breakers


### Execution Ownership

- 어떻게 제출할지
- 언제 replace / cancel / escalate 할지

담당:

- execution policy
- micro order policy
- order supervisor

이 원칙을 문서/코드/artifact contract 모두에 반영해야 한다.


## 5.2 Add A White-Box Portfolio Risk Budget Engine

현재 가장 필요한 신규 계층이다.

새 모듈 예시:

- `autobot/risk/portfolio_budget.py`

입력:

- predictor output:
  - `expected_return`
  - `expected_es`
  - `uncertainty`
  - `tradability_prob`
- live state:
  - open positions
  - quote cash
  - realized PnL streak
  - runtime regime
- market state:
  - spread
  - depth
  - volatility
  - leader correlation proxy

출력:

- `position_budget_fraction`
- `max_notional_quote`
- `cluster_budget_remaining`
- `risk_reason_codes`


### 5.2.1 Risk Budget Axes

최소한 아래 축을 둬야 한다.

- cash-at-risk budget
- per-trade max loss budget
- per-cluster exposure budget
- correlation / leader concentration budget
- liquidity-adjusted size cap
- uncertainty haircut


### 5.2.2 Cluster Definition

crypto KRW market에서는 아래 정도면 충분하다.

- BTC-led cluster
- ETH-led cluster
- high-beta alt cluster
- illiquid tail cluster

처음에는 learned cluster가 아니라 simple rule-based cluster로 시작해도 충분하다.


## 5.3 Position Sizing Should Become Distribution-Aware

현재 sizing은:

- `prob_ramp`
- trade_action multiplier
- size_ladder

조합이다.

이걸 더 강하게 만들려면 sizing을 다음 형태로 바꿔야 한다.

`size = base_budget * alpha_strength * tradability * confidence_haircut * portfolio_remaining_budget`

여기서:

- `alpha_strength`
  - expected return 또는 LCB
- `tradability`
  - fill probability, expected shortfall
- `confidence_haircut`
  - uncertainty가 높을수록 축소
- `portfolio_remaining_budget`
  - 현재 열린 포지션과 상관관계까지 반영


### 5.3.1 Fractional Kelly Is Only A Final Layer

Kelly류 sizing은 tempting하지만 그대로 쓰면 과격해질 수 있다.

권장:

- full Kelly 금지
- fractional Kelly only
- 그리고 그것도 `uncertainty-adjusted edge`와 `ES/CVaR cap` 뒤에 사용

즉 Kelly는 sizing의 코어가 아니라 최종 미세 조정이어야 한다.


## 5.4 Add Conformal / OCE Risk Control At The Decision Boundary

가장 강한 방법론 중 하나는 conformal risk control 계열을 진입 boundary에 연결하는 것이다.

의미:

- 단순히 평균 edge > 0 이면 진입
- 가 아니라
- 특정 bounded risk metric이 calibration set에서 통제되도록 threshold를 선택

적용 가능한 risk metric 예시:

- severe loss probability
- nonpositive return rate
- entry after-cost false-positive rate
- tail loss exceedance rate

관련 방향:

- Conformal Risk Training / Conformal Risk Control은 bounded monotone risk를 finite-sample 통제하는 틀을 준다 [R1].

실무 적용 방식:

- 최초에는 full end-to-end CRT가 아니라
- `post-hoc risk-calibrated threshold`로 시작

예:

- `alpha_lcb > 0`
- and `estimated severe loss risk <= q`
- and `portfolio budget available`


## 5.5 Exit System Should Become A Two-Layer Controller

청산은 두 층으로 나누는 것이 좋다.

### Layer A: Alpha/Path Exit

질문:

- 지금 계속 들고 가는 것이 가치가 있는가?

입력:

- path risk
- alpha decay
- unrealized pnl
- uncertainty


### Layer B: Liquidation Execution

질문:

- 닫기로 결정했다면 어떻게 가장 안전하게 닫을 것인가?

입력:

- spread
- depth
- queue state
- fill probability
- urgency

현재는 이 둘이 많이 섞여 있다.

앞으로는:

- `exit_decision`
- `liquidation_policy`

를 분리한 artifact로 가는 것이 가장 좋다.


## 5.6 Upgrade Path-Risk To A Proper Continuation Value Controller

[DYNAMIC_CONTINUATION_EXIT_DESIGN_2026-03-24.md](/d:/MyApps/Autobot/docs/DYNAMIC_CONTINUATION_EXIT_DESIGN_2026-03-24.md)가 이 방향의 좋은 초안이다.

강화 방향:

- `exit_now_value_net`
- `continue_value_net`
- `expected_liquidation_cost`
- `alpha_decay_penalty`

를 명시적으로 비교

추가해야 할 것:

- confidence-adjusted continuation value
- execution twin 기반 immediate liquidation cost
- portfolio spillover effect

예:

- open positions가 이미 많고
- same cluster exposure가 높고
- continuation uncertainty가 커지면

같은 expected return이라도 더 빠르게 이익 실현하는 것이 합리적일 수 있다.


## 5.7 Strengthen Online Halt Logic With Confidence Sequences

현재 online halt는:

- Hoeffding UCB
- martingale/e-process style

를 이미 쓰고 있다.

이건 매우 좋은 방향이다.

다음 단계는 이것을 더 체계화하는 것이다.

### Halt Families

1. infrastructure halt
2. model/data divergence halt
3. risk-performance halt
4. execution-quality halt

각 family마다 다른 통계량을 쓴다.


### Recommended Online Statistics

- nonpositive return rate confidence sequence
- severe loss rate confidence sequence
- execution miss-rate confidence sequence
- expected-vs-realized edge gap confidence sequence
- paper/live feature divergence rate confidence sequence

관련 이론 축:

- time-uniform Chernoff / confidence sequence / nonnegative supermartingale 계열 [R2]


## 5.8 Add A Risk Budget Ledger

새 checkpoint 또는 artifact:

- `risk_budget_ledger`

기록 항목:

- current total cash-at-risk
- cluster utilization
- uncertainty-weighted exposure
- recent severe-loss evidence
- current risk regime
- budget reason codes

이걸 남기면 나중에 다음 질문에 답할 수 있다.

- 왜 size가 줄었는가?
- 왜 동일 score인데 진입이 막혔는가?
- 왜 canary는 허용되고 champion은 막혔는가?


## 5.9 Make Breakers More Typed

권장 taxonomy:

### Type 1: Infra Breakers

- WS stale
- auth failure
- nonce failure
- executor stream stale

### Type 2: State Integrity Breakers

- unknown positions
- identifier collision
- order lineage corruption

### Type 3: Statistical Risk Breakers

- nonpositive-rate breach
- severe-loss-rate breach
- martingale evidence
- feature divergence

### Type 4: Operational Policy Breakers

- rollout not armed
- canary single-slot violation
- manual kill switch

이렇게 분리하면 자동 recovery policy도 더 분명해진다.


## 5.10 Add Feature and Decision Divergence Breakers

앞 문서에서 제안한 paired paper / divergence monitoring과 직접 연결된다.

새로 추가할 통제:

- feature divergence > threshold
- decision divergence > threshold under pinned model
- expected edge calibration collapse

이건 alpha 자체 문제가 아니라 runtime plane의 contract drift를 빨리 잡는 용도다.


## 5.11 Use Private Upbit Data More Aggressively

공식 Upbit private 데이터 중 risk/live에 특히 유용한 것:

- Get Account Balances [U1]
- Get Available Order Info (`orders/chance`) [U2]
- List Open Orders [U3]
- List Closed Orders [U4]
- Cancel and New Order [U5]
- MyOrder private WS [U6]
- MyAsset private WS [U7]

강화 포인트:

- account locked/free 상태를 더 자주 risk ledger에 반영
- `MyOrder`를 protective exit truth source 1순위로 사용
- `MyAsset`로 balance propagation latency를 측정
- chance/min_total/tick_size drift를 cached assumption이 아니라 runtime monitored contract로 승격


## 5.12 Add Execution-Calibrated Protective Liquidation Policy

protective exit는 보수적이어야 한다.

권장:

- normal alpha entry execution policy와
- protective liquidation policy를 분리

예:

- alpha entry는 maker/join/cross ladder
- protective liquidation은 urgency tiers:
  - soft exit
  - normal protective
  - urgent defensive
  - emergency flatten

입력:

- stop breach magnitude
- time since trigger
- current depth/spread
- queue state
- breaker state

현재 risk manager는 replace escalation이 있지만, policy family 자체를 더 명확히 나누는 것이 좋다.


## 6. Recommended New Architecture

## 6.1 Decision Surfaces

최종적으로는 진입/유지/청산을 아래 4개의 surface로 분리한다.

1. `alpha_surface`
2. `portfolio_risk_surface`
3. `execution_surface`
4. `protective_liquidation_surface`


## 6.2 Entry Decision Formula

권장 진입 공식 예시:

`entry_allowed = alpha_lcb > 0`

and

- tradability_prob >= threshold
- severe_loss_risk <= threshold
- cluster_budget_remaining > 0
- account_after_reserve_valid
- no active statistical breaker

즉 진입 허용은 learned single threshold 하나가 아니라 `white-box conjunctive contract`가 되어야 한다.


## 6.3 Sizing Formula

권장 sizing 공식 예시:

`target_notional = base_notional * f_alpha * f_confidence * f_tradability * f_liquidity * f_portfolio`

각 factor는 `[0, 1]` 또는 bounded multiplier로 둔다.

이렇게 해야 추후 audit가 쉽다.


## 6.4 Exit Formula

권장 청산 공식 예시:

`exit_when exit_now_value_net > continue_value_lcb`

or

- hard stop breach
- severe micro deterioration
- portfolio budget emergency
- statistical halt


## 6.5 Live Monitoring Formula

live state는 아래를 계속 추적한다.

- realized edge gap
- severe loss rate
- miss rate
- feature divergence
- model pointer divergence

그리고 각 항목은:

- warn
- degrade
- halt new intents
- flatten

중 어느 단계인지 명시적으로 갖는다.


## 7. Concrete Repository Changes

## 7.1 New Modules

권장 신규 모듈:

- `autobot/risk/portfolio_budget.py`
- `autobot/risk/confidence_monitor.py`
- `autobot/risk/divergence_budget.py`
- `autobot/risk/liquidation_policy.py`
- `autobot/ops/risk_budget_report.py`


## 7.2 Existing Modules To Extend

- `autobot/models/execution_risk_control.py`
- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/risk/live_risk_manager.py`
- `autobot/live/admissibility.py`
- `autobot/live/breakers.py`
- `autobot/live/small_account.py`
- `autobot/common/path_risk_guidance.py`
- `autobot/common/dynamic_exit_overlay.py`


## 7.3 New Artifacts

- `risk_budget_ledger.json`
- `live_risk_state.json`
- `live_risk_confidence_sequence.json`
- `protective_liquidation_report.json`
- `feature_divergence_report.json`
- `portfolio_exposure_report.json`


## 8. Priority Order

## 8.1 Highest ROI

1. portfolio risk budget engine
2. typed breaker taxonomy
3. risk budget ledger
4. execution-calibrated protective liquidation policy


## 8.2 Next

1. confidence-sequence online monitors
2. feature/decision divergence breaker
3. conformal risk-calibrated entry thresholds


## 8.3 Then

1. uncertainty-aware sizing with upstream v5 model outputs
2. cluster-aware portfolio budgets
3. matched paper/live risk attribution


## 9. Strongest Combined Methodology Across All Three Blueprints

세 문서를 합치면 strongest system은 아래다.

### 9.1 Predictor

- distributional, uncertainty-aware, tradability-aware alpha model

### 9.2 Evaluation

- fast backtest
- event replay certification
- paired paper
- canary sequential evidence

### 9.3 Risk

- white-box portfolio budget
- conformal / confidence-sequence risk control
- execution-calibrated liquidation policy

### 9.4 Live

- pinned model contract
- opportunity log
- divergence monitors
- typed breakers
- rollout-aware promotion

이 조합이면:

- alpha가 강하고
- 검증이 강하고
- live safety가 강한

시스템이 된다.


## 10. Final Recommendation

현재 시스템의 다음 병목은 "리스크 로직이 없다"가 아니다.

진짜 병목은:

- risk ownership ambiguity
- portfolio-level budget 부재
- uncertainty-aware sizing 부재
- stronger statistical live monitors 부재

다음 네 가지가 가장 중요하다.

1. `portfolio risk budget engine`
2. `risk budget ledger`
3. `typed breakers + confidence-sequence monitors`
4. `execution-calibrated protective liquidation`

이 네 가지를 앞선 두 문서의 제안과 결합하면, 현재 코드베이스는 단순 자동매매봇이 아니라:

- stronger predictive model
- stronger evidence ladder
- stronger live safety control

를 모두 갖춘 운영형 시스템으로 올라갈 수 있다.


## 11. References

### Internal Documents

- [D1] [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [D2] [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [D3] [RISK_CONTROL_SAFETY_LAYER_REDESIGN.md](/d:/MyApps/Autobot/docs/RISK_CONTROL_SAFETY_LAYER_REDESIGN.md)
- [D4] [DYNAMIC_CONTINUATION_EXIT_DESIGN_2026-03-24.md](/d:/MyApps/Autobot/docs/DYNAMIC_CONTINUATION_EXIT_DESIGN_2026-03-24.md)
- [D5] [LIVE_RUNTIME_POSTMORTEM_2026-03-21.md](/d:/MyApps/Autobot/docs/LIVE_RUNTIME_POSTMORTEM_2026-03-21.md)
- [D6] [RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md](/d:/MyApps/Autobot/docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md)


### Research / Methodology

- [R1] Yeh, Christianson, Wierman, Yue, *Conformal Risk Training: End-to-End Optimization of Conformal Risk Control*  
  https://arxiv.org/abs/2510.08748

- [R2] Howard, Ramdas, McAuliffe, Sekhon, *Time-uniform Chernoff Bounds via Nonnegative Supermartingales*  
  https://arxiv.org/abs/1808.03204

- [R3] Lokin, Yu, *Fill Probabilities in a Limit Order Book with State-Dependent Stochastic Order Flows*  
  https://arxiv.org/abs/2403.02572

- [R4] Bodor, Carlier, *A Novel Approach to Queue-Reactive Models: The Importance of Order Sizes*  
  https://arxiv.org/abs/2405.18594

- [R5] Arroyo et al., *Deep Attentive Survival Analysis in Limit Order Books*  
  https://arxiv.org/abs/2306.05479

- [R6] Su, Dimakopoulou, Krishnamurthy, Dudik, *Doubly Robust Off-Policy Evaluation with Shrinkage*  
  https://proceedings.mlr.press/v119/su20a.html

- [R7] Wang, Liang, Kallus, Sun, *Risk-Sensitive RL with Optimized Certainty Equivalents via Reduction to Standard RL*  
  https://arxiv.org/abs/2403.06323


### Official Upbit Documentation

- [U1] Get Account Balances  
  https://global-docs.upbit.com/reference/overall-account-inquiry

- [U2] Get Available Order Info  
  https://global-docs.upbit.com/reference/available-order-information

- [U3] List Open Orders  
  https://global-docs.upbit.com/reference/open-order

- [U4] List Closed Orders  
  https://global-docs.upbit.com/reference/closed-order

- [U5] Cancel and New Order  
  https://global-docs.upbit.com/reference/cancel-and-new-order

- [U6] MyOrder WebSocket  
  https://global-docs.upbit.com/reference/websocket-myorder

- [U7] MyAsset WebSocket  
  https://global-docs.upbit.com/reference/websocket-myasset

- [U8] Create Order  
  https://global-docs.upbit.com/reference/new-order

- [U9] Authentication / permission overview  
  https://global-docs.upbit.com/reference/auth
