# UPBIT AUTOTRADING TRAINING MODEL STRENGTHENING BLUEPRINT 2026-03-25

## 0. Executive Summary

이 문서는 현재 코드베이스의 `학습모델 자체`만 따로 떼어 깊게 평가하고, 이를 실제로 훨씬 더 강한 학습 스택으로 바꾸기 위한 방법론을 체계화한 설계 문서다.

현재 프로그램의 학습모델은 본질적으로 다음 구조다.

- 입력: `features_v4`의 5분봉 패널 행(row) 특징
- 라벨: `12 bar` 기준의 cross-sectional rank / net return / top-quantile classification
- 본 모델: XGBoost 계열 `cls / reg / rank` 중 하나
- 후처리: walk-forward selection recommendation, selection policy, isotonic calibration

이 구조는 실전형 운영 시스템의 상층부와 잘 결합되어 있지만, 학습모델 그 자체는 아직 다음 한계를 가진다.

- 5분봉 row-wise tabular scorer 중심이라 intrabar event ordering을 거의 잃는다.
- 라벨이 단일 horizon(12 bar)에 치우쳐 있고, `point estimate` 성격이 강하다.
- alpha 자체가 약한 부분을 downstream heuristic 레이어가 많이 메우는 구조다.
- uncertainty, OOD robustness, self-supervised pretraining, raw LOB sequence modeling이 사실상 없다.

따라서 "정말 강력한 학습모델"로 가려면 `더 많은 sweep`, `더 많은 feature cross`, `더 깊은 XGBoost` 정도로는 부족하다. 모델 축 자체를 다음처럼 바꿔야 한다.

1. `single-row tabular scorer` -> `multi-resolution, multi-task, uncertainty-aware ensemble`
2. `5m aggregate-only` -> `5m panel + 1s/1m sequence + 30-level orderbook + private execution labels`
3. `single-horizon score` -> `multi-horizon distribution + rank + tradability`
4. `point prediction` -> `uncertainty-adjusted score / lower confidence bound`
5. `pure ERM` -> `domain shift aware / invariant / importance-weighted training`

이 문서의 핵심 제안은 다음이다.

- 단기적으로는 현행 v4 위에 `multi-horizon label bundle + cls/reg/rank ensemble + stronger rank objective + uncertainty calibration`을 얹는 `v5_panel`을 먼저 만든다.
- 중기적으로는 `1s/1m sequence encoder`와 `30-level LOB encoder`를 추가해 `v5_sequence`, `v5_lob`을 만든다.
- 최종적으로는 `panel + sequence + LOB + tradability`를 스태킹하는 `v5_fusion`을 만들고, 런타임에는 fused score와 uncertainty를 공급한다.

이 로드맵을 따르면 현재 시스템의 강점인 운영 안정성은 유지하면서, alpha를 downstream heuristic에 덜 의존하는 방향으로 실질적으로 강화할 수 있다.


## 1. Scope

이 문서의 범위는 `학습모델`이다.

OCI server access reference:

- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

주의:

- Upbit 데이터 가용성 항목은 공식 Upbit Developer Center 문서를 기준으로 확인했다.
- 현재 저장소 설정은 `config/upbit.yaml`에서 `https://api.upbit.com`과 KRW 마켓을 사용한다.
- 따라서 `global-docs.upbit.com`에서 확인한 기능과 필드가 한국 실거래 엔드포인트에서도 완전히 동일한지는 실제 배포 전 별도 검증이 필요하다.

포함:

- 현재 코드의 학습모델 구조와 병목
- 현재 원천데이터와 추가 가능한 Upbit 데이터
- 논문 기반의 모델 후보군과 적용 이유
- 저장소에 맞는 구현 순서
- 어떤 모델을 먼저 만들고 어떤 것은 뒤로 미뤄야 하는지

의도적으로 제외:

- breaker, rollout, reconcile, promote automation 자체의 운영 설계
- execution simulator의 low-level parity 이슈
- 리스크 매니저의 주문 대체/정리 로직

단, 학습모델 산출물이 downstream 레이어를 어떻게 단순화할 수 있는지는 함께 다룬다.


## 2. Current Codebase: Training Model Only

### 2.1 Data to Feature Path

현재 학습 경로의 중심 파일은 아래와 같다.

- `autobot/features/pipeline_v4.py`
- `autobot/features/feature_set_v4_live_base.py`
- `autobot/features/feature_set_v4.py`
- `autobot/features/labeling_v2_crypto_cs.py`
- `autobot/models/dataset_loader.py`
- `autobot/models/train_v4_core.py`
- `autobot/models/train_v4_models.py`
- `autobot/models/train_v4_walkforward.py`
- `autobot/models/train_v4_crypto_cs.py`

현행 데이터 흐름은 다음과 같다.

1. raw candles / raw ws trades / raw ws orderbook / rest ticks 수집
2. micro 1m/5m 집계
3. base 5m candles + 1m aggregate + high TF + micro mandatory join
4. spillover / breadth / periodicity / trend-volume / order-flow / interaction feature 추가
5. cross-sectional label 생성
6. dataset loader로 all-market panel matrix 생성

핵심 특징:

- base frequency는 사실상 `5m`
- micro는 대부분 `aggregated micro statistics`
- raw event order 자체는 거의 살아남지 않음
- feature row 하나가 `한 시점-한 마켓`의 snapshot 역할을 함


### 2.2 Current Label Design

현재 v4 label은 [labeling_v2_crypto_cs.py]의 다음 세 축이다.

- `y_reg_net_12`: horizon 12 bar 로그수익률에서 fee + safety를 뺀 값
- `y_rank_cs_12`: 같은 시점 cross-section 내 rank percentile
- `y_cls_topq_12`: 상위 quantile / 하위 quantile 이진 분류

장점:

- cross-sectional selection 목적과 잘 맞는다.
- 거래비용을 라벨 생성 시점부터 반영한다.
- neutral을 제거해 신호 대 잡음비를 높이려는 의도가 있다.

한계:

- horizon이 사실상 `12 bar`에 고정되어 있다.
- `distribution`이 아니라 `point target` 중심이다.
- upside / downside / tail / path dependence 정보가 본 모델에 직접 들어가지 않는다.
- "예측은 좋지만 실제로는 체결/비용 때문에 못 쓰는 경우"를 alpha head가 직접 배우지 않는다.


### 2.3 Current Model Family

현재 학습 본체는 [train_v4_models.py]의 XGBoost 계열이다.

- `XGBClassifier`
- `XGBRegressor`
- `XGBRanker(rank:pairwise)`

훈련 절차:

1. 시간 분할 train/valid/test
2. classifier / regressor / ranker 중 하나 sweep
3. valid 기준 경제 목적 정렬 키로 best params 선택
4. walk-forward OOS 평가
5. selection recommendation / policy / calibration 생성

현행 학습모델의 실질적 정체:

- 강한 tabular baseline
- cross-sectional ranking을 어느 정도 의식한 설계
- 하지만 여전히 `single-stage gradient-boosted tree scorer`


### 2.4 What Is Learned vs What Is Not

현재 시스템에서 "학습된 것처럼 보이지만 사실은 alpha 본모델이 아닌 것"을 분리해야 한다.

실제 본모델이 학습하는 것:

- `X -> score`

본모델 이후 OOS replay / grid / heuristic / threshold search로 만들어지는 것:

- selection recommendation
- selection policy
- selection calibration
- exit runtime recommendation
- execution runtime recommendation
- trade action policy
- execution risk control

즉, 현재 프로그램은 운영 시스템 전체로 보면 매우 고급이지만, alpha learner 단독으로 보면 아직 `다운스트림 보정 레이어의 도움을 많이 받는 구조`다.


## 3. Why the Current Model Hits a Ceiling

### 3.1 The Biggest Bottleneck: Event Ordering Loss

현재 특징은 대부분 5분 시점 row로 압축된다.

그 결과:

- 5분 안에서 어떤 순서로 체결과 호가 변화가 일어났는지 사라진다.
- 같은 `m_trade_imbalance`, `m_spread_proxy`를 가져도 micro path가 전혀 다른 경우를 구분하지 못한다.
- orderbook level 간 dependency, queue depletion, repeated sweep, liquidity fade 같은 미시 구조를 alpha가 직접 학습하지 못한다.

이 문제는 raw LOB / short-horizon sequence model 없이는 근본적으로 해결되지 않는다.


### 3.2 Single-Horizon Supervision

현재 라벨의 의미는 사실상 `12 bar 후 결과`다.

실전에서는 다음이 모두 중요하다.

- 3 bar, 6 bar, 12 bar, 24 bar의 horizon별 일관성
- 초반에 급하게 오른 뒤 되돌림이 오는지
- downside tail이 큰지
- alpha는 맞지만 너무 늦게 실현되는지

현 모델은 이 정보를 downstream exit/trade-action 계층에서 간접적으로 보완하고 있다.


### 3.3 Weak Native Uncertainty

현재 alpha head는 score를 주지만, 다음을 본격적으로 주지 않는다.

- prediction interval
- epistemic uncertainty
- regime shift underconfidence / overconfidence
- score-level lower confidence bound

그 결과 selection policy와 sizing, 그리고 trade-action 레이어가 alpha 불확실성을 충분히 직접 활용하지 못한다.


### 3.4 OOD Generalization Is Underpowered

현재 코드에는 `live_domain_reweighting`이 있지만, 이는 좋은 시작점일 뿐이다.

부족한 부분:

- 명시적 environment inference가 약함
- invariant representation 학습이 없음
- time-varying regime split을 본모델이 구조적으로 배우지 않음
- covariate shift를 mostly row-level importance weighting으로만 다룸


### 3.5 One Model Does Too Much

현재는 하나의 score가 downstream 거의 모든 판단의 출발점이다.

하지만 실제로는 서로 다른 문제가 섞여 있다.

- expected return prediction
- relative cross-sectional ranking
- downside / tail prediction
- tradability / realizability
- uncertainty estimation

이걸 단일 score로 뭉개면 downstream heuristic가 복잡해질 수밖에 없다.


## 4. Upbit Data: What We Already Have and What We Can Add

## 4.1 Already Used in the Repo

현재 저장소는 이미 다음 데이터를 활용한다.

- candles REST / parquet
- public WebSocket trade
- public WebSocket orderbook
- REST trades backfill
- micro 1m/5m aggregate
- trading-pair universe
- market rules / tick size / min total

하지만 학습모델 입장에서는 대부분 `aggregated feature row`로 축약해서 사용하고 있다.


## 4.2 Additional Official Upbit Data Worth Using

아래는 공식 Upbit 문서 기준으로 현재 학습모델 강화에 유의미한 데이터다.

| 데이터 | 공식 제공 여부 | 학습모델 가치 | 코멘트 |
| --- | --- | --- | --- |
| REST 1-second candles | 있음 | 높음 | 최근 3개월 제한이 있지만 intrabar path 학습에 매우 유용 |
| WebSocket candle (`1s`, `1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `60m`, `240m`) | 있음 | 높음 | 실시간 sequence builder 단순화 가능 |
| WebSocket orderbook 30 levels | 있음 | 매우 높음 | LOB alpha의 핵심. 저장비용 큼 |
| Recent trades history REST | 있음 | 중간~높음 | trade WS 누락 보완, 짧은 히스토리 복원 |
| Trading pairs list | 있음 | 중간 | 상장/폐지/거래가능 시장 metadata |
| Orderbook instruments (tick size) | 있음 | 중간~높음 | 미세 체결/price discretization feature에 중요 |
| Available order info (`/orders/chance`) | 있음 | 중간 | order type / fee / min-max / allowed side 정보 |
| MyOrder private WS | 있음 | 높음 | personalized tradability / fill-quality label 생성 가능 |
| MyAsset private WS | 있음 | 중간 | inventory / account-state conditional policy 학습 가능 |


### 4.2.1 Highest ROI Additions

학습모델 관점에서 ROI가 높은 순서는 아래와 같다.

1. `1s candle + realtime candle WS`
2. `30-level orderbook`
3. `MyOrder private WS`
4. `Recent trades history REST`

이 중 1, 2, 3이 있으면 alpha / tradability / uncertainty를 분리한 강한 모델을 만들 수 있다.


### 4.2.2 Storage Reality

주의:

- 30-level orderbook raw 저장은 데이터 폭증을 일으킨다.
- second candle는 3개월 제한이라 long history backbone으로는 부족하다.
- 따라서 `모든 원시 데이터 장기 저장`보다 `선택 저장 + sequence tensor store`가 현실적이다.

권장:

- top 20 또는 top 30 유동성 시장에 한정
- 100ms event stream 전체 보관 대신 `1s aligned snapshot + event deltas` 저장
- 학습용 tensor cache를 따로 구성


## 5. Primary-Source Research Directions

아래 논문들은 "현재 프로그램을 진짜 강한 학습모델로 바꾸는 데 직접적으로 도움이 되는 축"만 뽑은 것이다.

### 5.1 Cross-Sectional Asset Pricing / Nonlinear Return Modeling

- Gu, Kelly, Xiu, *Empirical Asset Pricing via Machine Learning* [R1]
- Chen, Pelger, Zhu, *Deep Learning in Asset Pricing* [R2]
- Kelly, Kuznetsov, Malamud, Xu, *Artificial Intelligence Asset Pricing Models* [R20]

시사점:

- cross-section 예측은 여전히 강한 tabular baseline이 매우 중요하다.
- nonlinear predictor가 유효하며, factor-like structure와 macro/regime conditioning을 같이 다루는 것이 성능에 중요하다.
- 최근에는 transformer를 stochastic discount factor 구조에 직접 심는 방식까지 제안되고 있어, `cross-asset information sharing`을 학습모델 내부에서 직접 처리하는 방향이 유망하다.
- 즉, 현재의 panel ranker 축은 버릴 것이 아니라 `더 정교하게 강화`해야 한다.


### 5.2 Learning-to-Rank

- Burges et al., *Learning to Rank with Nonsmooth Cost Functions (LambdaRank)* [R3]
- Wang et al., *The LambdaLoss Framework for Ranking Metric Optimization* [R4]

시사점:

- "top-k를 고르는 문제"를 BCE보다 ranking objective로 직접 다루는 편이 더 자연스럽다.
- 현행 `rank:pairwise`는 좋은 시작점이지만, metric-driven ranking loss와 multi-horizon ranking을 더 밀어붙일 여지가 크다.


### 5.3 Tabular Deep Learning

- Arik and Pfister, *TabNet* [R5]
- Somepalli et al., *SAINT* [R6]

시사점:

- 금융 패널은 여전히 GBDT가 강하지만, row/column attention과 self-supervised tabular pretraining은 대체 후보가 될 수 있다.
- 다만 이 저장소에서는 GBDT를 대체하기보다 `보조 expert`로 넣는 것이 더 현실적이다.


### 5.4 Time-Series Sequence Modeling

- Lim et al., *Temporal Fusion Transformers* [R7]
- Nie et al., *PatchTST* [R8]
- Wang et al., *TimeMixer* [R9]

시사점:

- multi-horizon 예측을 직접 출력하고, static / known / observed covariates를 구조적으로 다루려면 TFT가 매우 깔끔하다.
- 긴 lookback과 patch 기반 self-supervised 전이를 노리려면 PatchTST가 강하다.
- 계산 효율까지 고려하면 TimeMixer가 실전 구현에 유리하다.


### 5.5 Self-Supervised Pretraining for Time Series

- Yue et al., *TS2Vec* [R10]
- Cheng et al., *TimeMAE* [R11]

시사점:

- Upbit raw data는 unlabeled data가 압도적으로 많다.
- 지금처럼 supervised label만 써서 처음부터 끝까지 훈련하는 것은 데이터 사용 효율이 낮다.
- 1s/1m/micro sequence를 먼저 self-supervised pretrain한 뒤 alpha heads를 fine-tune하는 것이 강한 방향이다.


### 5.6 Raw Limit Order Book Modeling

- Sirignano, *Deep Learning for Limit Order Books* [R12]
- Zhang et al., *DeepLOB* [R13]
- Zhang et al., *BDLOB* [R14]
- Briola et al., *HLOB* [R15]

시사점:

- raw orderbook에는 aggregate micro features가 잃어버리는 정보가 많다.
- uncertainty-aware LOB modeling까지 포함하면 `alpha + confidence`를 같이 줄 수 있다.
- 단기 horizon 예측력은 이 축이 가장 크게 개선될 가능성이 높다.


### 5.7 OOD Generalization / Domain Adaptation

- Liu et al., *FOIL: Time-Series Forecasting for Out-of-Distribution Generalization Using Invariant Learning* [R16]
- Jin et al., *Domain Adaptation for Time Series Forecasting via Attention Sharing* [R17]

시사점:

- historical train vs future live는 본질적으로 OOD 문제다.
- 현재 저장소의 live-domain reweighting은 좋은 시작점이지만, environment-invariant training까지 가면 한 단계 더 강해질 수 있다.


### 5.8 Uncertainty / Conformal

- Lee et al., *Transformer Conformal Prediction for Time Series* [R18]
- Liao et al., *The Uncertainty of Machine Learning Predictions in Asset Pricing* [R19]

시사점:

- 예측 평균만 쓰지 말고 예측 구간과 confidence를 같이 써야 한다.
- ranking도 `point estimate`가 아니라 `uncertainty-adjusted score`로 바꾸면 selection 품질이 더 좋아질 수 있다.


## 6. Target End-State: What "Really Strong" Should Mean Here

이 저장소에서 강한 학습모델의 정의는 단순히 accuracy가 높다는 뜻이 아니다.

강한 학습모델은 아래를 동시에 만족해야 한다.

- cross-sectional top-k 선택력이 강하다.
- 여러 horizon에서 방향성이 일관된다.
- downside / tail / uncertainty를 같이 출력한다.
- regime shift에서 calibration 붕괴가 덜하다.
- 실행/청산 heuristic가 alpha의 약점을 대신 메우는 비중이 줄어든다.
- current runtime stack에 artifact 형태로 무리 없이 붙는다.

즉 목표는 `single score`가 아니라 다음을 내는 모델이다.

- `expected_return_h3, h6, h12, h24`
- `return_quantiles_h*`
- `cross_sectional_rank_score`
- `tradability_prob`
- `uncertainty_sigma`
- `alpha_lcb`


## 7. Recommended New Model Architecture

### 7.1 North-Star Architecture

권장 최종 구조:

1. `Panel Ranker Expert`
2. `Sequence Expert`
3. `LOB Expert`
4. `Tradability Expert`
5. `Fusion Meta-Model`

이 중 1은 현행 v4를 강화한 것, 2와 3은 새로 추가되는 것, 4는 private execution data를 쓰는 보조 head, 5는 최종 alpha artifact 생성기다.


### 7.2 Expert 1: Panel Ranker Expert

현재 `features_v4`를 가장 잘 활용하는 고성능 tabular expert다.

권장 사항:

- 단일 `cls/reg/rank` 중 하나를 고르는 방식 대신 `세 모델 동시 학습`
- 후보:
  - XGBoost ranker (`rank:pairwise` 또는 metric-aware variant)
  - LightGBM LambdaRank / XE_NDCG_MART 비교
  - CatBoostRanker(YetiRankPairwise) 비교
- 출력:
  - `rank_score`
  - `p_topk`
  - `mu_h3, mu_h6, mu_h12, mu_h24`
  - fold ensemble variance 기반 uncertainty

핵심 개선:

- label을 `12 bar` 하나로 두지 말고 `3/6/12/24`
- `raw return` 대신 `residualized return` 사용
- cross-sectional group weight를 turnover / micro quality / regime reliability로 조정


### 7.3 Expert 2: Sequence Expert

이 expert는 "같은 5분 row로 압축되기 전"의 dynamics를 배우는 역할이다.

입력 후보:

- 최근 `N`개의 1초 또는 1분 candle sequence
- 최근 `N`개의 micro aggregate sequence
- known covariates:
  - session bucket
  - weekday / holiday proxy
  - market-wide breadth / BTC/ETH leader features

권장 아키텍처:

- 1순위 production candidate: `PatchTST`
- 2순위 compute-efficient candidate: `TimeMixer`
- 3순위 interpretable candidate: `TFT`

권장 출력:

- multi-horizon quantile forecast
- directional probability
- sequence uncertainty
- regime embedding

실무적 판단:

- TFT는 설명 가능성이 높아 연구/해석에 좋다.
- PatchTST는 patching과 self-supervised 확장성이 좋아 production backbone 후보로 좋다.
- TimeMixer는 서버 비용과 latency가 빡빡할 때 좋다.


### 7.4 Expert 3: LOB Expert

이 expert는 현재 시스템에서 사실상 비어 있는 가장 큰 alpha 구멍을 메운다.

입력:

- 30-level orderbook snapshots
- level별 price / size
- recent event delta
- short trade flow sequence

권장 라벨:

- 1초 / 5초 / 30초 / 60초 mid-price move
- 5분 horizon alpha 보조 target
- short-term adverse excursion

권장 아키텍처:

- baseline: `DeepLOB`
- uncertainty-aware variant: `BDLOB`
- newer structural variant benchmark: `HLOB`

출력:

- `micro_alpha_1s`
- `micro_alpha_5s`
- `micro_alpha_30s`
- `micro_uncertainty`

사용 방식:

- 직접 5분 매매 시그널로 쓰기보다 `fusion feature` 또는 `alpha veto/boost`로 먼저 사용


### 7.5 Expert 4: Tradability Expert

이건 price alpha가 아니라 "실제로 쓸 수 있는 alpha인가"를 학습한다.

입력:

- Panel / Sequence / LOB expert 출력
- 당시 spread / depth / queue / tick size
- private execution history
- own order outcomes from `MyOrder`

타겟:

- `filled_within_deadline`
- `fill_fraction`
- `implementation_shortfall`
- `cancel_or_replace_likelihood`

역할:

- 현재 runtime execution policy가 휴리스틱으로 하는 일을 일부 학습기로 이동
- alpha head와 분리해 놓으면 더 깨끗한 설계가 가능

주의:

- 이는 alpha price model과 분리해야 한다.
- alpha가 맞더라도 realizability가 낮으면 최종 fusion score를 낮추는 식으로 쓰는 것이 안전하다.


### 7.6 Expert 5: Fusion Meta-Model

최종 모델은 각 expert의 출력들을 받아 아래를 산출한다.

- `final_rank_score`
- `final_expected_return`
- `final_expected_es`
- `final_tradability`
- `final_uncertainty`
- `final_alpha_lcb`

권장 fusion 방식:

- 1단계: simple linear / monotone GBDT stacking
- 2단계: regime-aware gating mixture-of-experts

초기에는 단순한 stacked GBDT가 가장 현실적이다.


## 8. Label System Redesign

### 8.1 Multi-Horizon Label Bundle

현재:

- `12 bar` 중심

권장:

- `h in {3, 6, 12, 24}`에 대해 모두 생성

예시:

- `y_reg_net_h3`
- `y_reg_net_h6`
- `y_reg_net_h12`
- `y_reg_net_h24`
- `y_rank_cs_h3`
- `y_rank_cs_h6`
- `y_rank_cs_h12`
- `y_rank_cs_h24`


### 8.2 Residualized Target

현재 feature에는 이미 BTC/ETH/market breadth 정보가 있다.

따라서 target도 다음처럼 개선하는 것이 좋다.

- raw return
- BTC residual return
- ETH residual return
- leader basket residual return

목적:

- 시장 전체 beta가 아니라 `idiosyncratic alpha`를 더 잘 학습
- cross-sectional top-k의 질 개선


### 8.3 Distributional Target

단순 mean return 대신 아래를 직접 학습한다.

- q10
- q50
- q90

또는

- expected downside deviation
- expected shortfall proxy

이렇게 하면 현재 trade_action / risk_control이 OOS binning으로 근사하는 tail 정보를 upstream model이 더 직접 공급할 수 있다.


### 8.4 Tradability Meta-Label

새 메타 라벨 예시:

- `y_tradeable = 1` if
  - predicted alpha realized after costs > 0
  - and fill within deadline likely
  - and short-term adverse move stays within tolerance

이 라벨은 반드시 private execution / simulator labels와 연결해서 생성해야 한다.


## 9. Data Engineering Plan for the New Models

### 9.1 Sequence Store

새로운 저장 레이어를 추가한다.

- `data/parquet/candles_second_v1`
- `data/parquet/sequence_v1`
- `data/parquet/lob30_v1`

권장 내용:

- 1초 candles
- 1분 rolling micro sequence tensor
- 30-level orderbook snapshot tensor


### 9.2 LOB Tensor Contract

권장 텐서 shape 예시:

- `T x L x C`
- `T`: lookback steps
- `L`: orderbook levels (30)
- `C`: per-level channels
  - relative price
  - bid size
  - ask size
  - normalized depth share
  - event delta

추가 global channels:

- spread
- total depth
- trade imbalance
- tick size / relative tick


### 9.3 Private Execution Label Store

현재 저장소는 live state DB와 execution attempt 기록을 이미 갖고 있다.

학습용으로는 이를 별도 export해서 아래 데이터셋으로 만들면 좋다.

- `data/parquet/private_execution_v1`

컬럼 예시:

- market
- ts_ms
- action_code
- ord_type
- time_in_force
- requested_price
- requested_volume
- spread_bps
- depth_top5_notional_krw
- snapshot_age_ms
- model_prob
- expected_edge_bps
- final_state
- first_fill_ts_ms
- full_fill_ts_ms
- shortfall_bps


## 10. Concrete Model Roadmap

### 10.1 Phase 1: Immediate Upgrade Inside Current Infra

목표:

- `train_v4`의 철학을 유지하되 학습모델만 먼저 강하게 만들기

권장 작업:

1. `features_v4` 유지
2. multi-horizon labels 추가
3. `cls/reg/rank` 동시 학습
4. out-of-fold stacking meta-model 추가
5. isotonic + uncertainty calibration 추가
6. 최종 artifact를 `final_rank_score + uncertainty`로 표준화

새 trainer 이름 예시:

- `train_v5_panel_ensemble.py`

이 단계만으로도 현재보다 의미 있는 개선 가능성이 높다.


### 10.2 Phase 2: Sequence Backbone

목표:

- 5분 row 압축으로 잃어버린 intrabar dynamics 복원

권장 작업:

1. second candles + realtime candle WS 수집
2. 1초/1분 sequence store 생성
3. TS2Vec 또는 TimeMAE pretraining
4. PatchTST / TimeMixer / TFT supervised fine-tuning

새 trainer 이름 예시:

- `train_v5_sequence.py`


### 10.3 Phase 3: LOB Expert

목표:

- raw orderbook information을 직접 alpha에 연결

권장 작업:

1. 30-level orderbook 저장
2. snapshot/event tensor build
3. DeepLOB baseline
4. BDLOB uncertainty variant
5. HLOB benchmark

새 trainer 이름 예시:

- `train_v5_lob.py`


### 10.4 Phase 4: Fusion

목표:

- panel / sequence / LOB / tradability를 하나의 alpha contract로 융합

권장 작업:

1. OOF predictions 수집
2. fusion meta-model 학습
3. calibrated LCB 산출
4. runtime artifact 생성

새 trainer 이름 예시:

- `train_v5_fusion.py`


## 11. Recommended First Production-Bound Build

가장 추천하는 첫 버전은 아래다.

### V5-A: Practical Strong Model

- 입력:
  - 기존 `features_v4`
  - 추가된 multi-horizon labels
- 모델:
  - XGBRanker
  - XGBClassifier
  - XGBRegressor
  - optional LightGBM ranker benchmark
- 출력:
  - stacked score
  - multi-horizon mean / quantiles
  - uncertainty

이유:

- 현재 저장소와 가장 잘 맞는다.
- 구현 난이도 대비 성능 개선 가능성이 높다.
- runtime stack 변경이 최소화된다.


### V5-B: Stronger But Still Practical

- V5-A +
- sequence encoder (`PatchTST` 또는 `TimeMixer`)

이게 사실상 "강한 학습모델"의 첫 번째 real target이다.


### V5-C: Best Long-Term Ceiling

- V5-B +
- LOB expert +
- tradability expert +
- fusion

이 단계부터는 현재 시스템의 downstream heuristic 비중을 실질적으로 줄일 수 있다.


## 12. How This Changes the Current Program

### 12.1 What Can Stay

아래는 초기에는 그대로 둬도 된다.

- registry / artifact / pointer 구조
- walk-forward / CPCV-lite / factor block governance
- paper / backtest / live strategy skeleton
- runtime execution / risk / rollout / breaker


### 12.2 What Must Change

반드시 바뀌어야 하는 부분:

- label contract
- dataset contract
- trainer family
- predictor artifact schema
- selection score semantics


### 12.3 Strongest Downstream Benefit

학습모델이 강해지면 downstream에서 가장 크게 달라지는 것은 이것이다.

- trade_action 정책이 덜 heuristic해진다.
- execution_risk_control이 upstream uncertainty를 바로 쓸 수 있다.
- selection policy가 point estimate가 아니라 LCB 기반 top-k가 가능해진다.
- exit 추천이 upstream horizon distribution을 직접 쓸 수 있다.


## 13. What Not To Do

아래는 비추천이다.

### 13.1 Do Not Just Make XGBoost Bigger

단순히:

- sweep trial 수 증가
- feature 수 증가
- max_depth 증가

만으로는 구조적 한계가 해결되지 않는다.


### 13.2 Do Not Jump Straight to End-to-End RL

현재 저장소와 데이터 구조에서 RL을 바로 production alpha learner로 쓰는 것은 시기상조다.

이유:

- label 효율이 떨어진다.
- simulator mismatch가 alpha mismatch보다 더 커질 수 있다.
- 디버깅 난이도가 급증한다.


### 13.3 Do Not Mix Alpha and Tradability into One Head Too Early

price alpha와 execution realizability는 분리해서 배우는 것이 더 좋다.


### 13.4 Do Not Reuse the Same Window for Everything

현재도 코드에서 경고를 남기듯, training window를 execution acceptance / runtime recommendation / promotion evidence에 재사용하는 것은 methodology 상 약점이다.

강한 학습모델을 만들수록 `final certification window` 분리가 더 중요해진다.


## 14. Minimal Action Plan

### 14.1 Best 30-Day Plan

1. `label_v3`를 추가해 `3/6/12/24 bar` multi-horizon residualized target bundle 생성
2. `train_v5_panel_ensemble` 작성
3. `cls/reg/rank` 동시 학습 + OOF stacking + uncertainty export
4. runtime predictor contract에 `score_mean`, `score_std`, `score_lcb` 추가
5. selection policy를 `score_lcb` 기준으로도 비교


### 14.2 Best 60-90 Day Plan

1. `1s candle` 및 `WS candle` 수집 파이프라인 추가
2. `sequence_v1` dataset 작성
3. TS2Vec 또는 TimeMAE pretraining
4. PatchTST 또는 TimeMixer fine-tuning
5. fusion trainer 추가


### 14.3 Best 90-180 Day Plan

1. `30-level orderbook` 저장
2. LOB expert 추가
3. private execution label store 추가
4. tradability expert 추가
5. full fusion rollout


## 15. Final Recommendation

현재 프로그램의 학습모델은 "나쁜 모델"이 아니다.

오히려:

- feature engineering이 좋고
- cross-sectional 목적이 분명하며
- OOS governance와 운영 artifact가 잘 되어 있다.

하지만 학습모델 자체를 정말 강하게 만들려면, 이제는 아래로 넘어가야 한다.

- `single row-wise booster`
  ->
- `multi-task panel expert + sequence expert + LOB expert + uncertainty-aware fusion`

가장 현실적인 첫 걸음은 `v5_panel_ensemble`이다.

가장 큰 알파 천장 개선은 `sequence + LOB`에서 나온다.

가장 시스템적으로 좋은 종착점은 `fused score + uncertainty + tradability`를 runtime contract의 중심으로 바꾸는 것이다.


## 16. Primary References

### Research

- [R1] Gu, Kelly, Xiu, *Empirical Asset Pricing via Machine Learning*  
  https://www.nber.org/papers/w25398

- [R2] Chen, Pelger, Zhu, *Deep Learning in Asset Pricing*  
  https://arxiv.org/abs/1904.00745

- [R3] Burges, Ragno, Le, *Learning to Rank with Nonsmooth Cost Functions*  
  https://research.google/pubs/learning-to-rank-with-nonsmooth-cost-functions/

- [R4] Wang et al., *The LambdaLoss Framework for Ranking Metric Optimization*  
  https://research.google/pubs/the-lambdaloss-framework-for-ranking-metric-optimization/

- [R5] Arik, Pfister, *TabNet: Attentive Interpretable Tabular Learning*  
  https://arxiv.org/abs/1908.07442

- [R6] Somepalli et al., *SAINT: Improved Neural Networks for Tabular Data via Row Attention and Contrastive Pre-Training*  
  https://arxiv.org/abs/2106.01342

- [R7] Lim et al., *Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting*  
  https://arxiv.org/abs/1912.09363

- [R8] Nie et al., *A Time Series is Worth 64 Words: Long-term Forecasting with Transformers*  
  https://arxiv.org/abs/2211.14730

- [R9] Wang et al., *TimeMixer*  
  https://arxiv.org/pdf/2405.14616

- [R10] Yue et al., *TS2Vec: Towards Universal Representation of Time Series*  
  https://arxiv.org/abs/2106.10466

- [R11] Cheng et al., *TimeMAE: Self-Supervised Representations of Time Series with Decoupled Masked Autoencoders*  
  https://arxiv.org/abs/2303.00320

- [R12] Sirignano, *Deep Learning for Limit Order Books*  
  https://arxiv.org/abs/1601.01987

- [R13] Zhang, Zohren, Roberts, *DeepLOB*  
  https://arxiv.org/abs/1808.03668

- [R14] Zhang, Zohren, Roberts, *BDLOB*  
  https://arxiv.org/abs/1811.10041

- [R15] Briola, Bartolucci, Aste, *HLOB*  
  https://arxiv.org/abs/2405.18938

- [R16] Liu et al., *Time-Series Forecasting for Out-of-Distribution Generalization Using Invariant Learning (FOIL)*  
  https://arxiv.org/abs/2406.09130

- [R17] Jin et al., *Domain Adaptation for Time Series Forecasting via Attention Sharing*  
  https://arxiv.org/abs/2102.06828

- [R18] Lee, Xu, Xie, *Transformer Conformal Prediction for Time Series*  
  https://arxiv.org/abs/2406.05332

- [R19] Liao et al., *The Uncertainty of Machine Learning Predictions in Asset Pricing*  
  https://arxiv.org/abs/2503.00549

- [R20] Kelly, Kuznetsov, Malamud, Xu, *Artificial Intelligence Asset Pricing Models*  
  https://www.nber.org/papers/w33351


### Official Upbit Documentation

- [U1] Upbit API Reference index  
  https://global-docs.upbit.com/reference/auth

- [U2] List Second Candles  
  https://global-docs.upbit.com/reference/seconds-candles

- [U3] WebSocket Candle  
  https://global-docs.upbit.com/reference/websocket-candle

- [U4] Recent Trades History  
  https://global-docs.upbit.com/reference/recent-trades-history

- [U5] List Trading Pairs  
  https://global-docs.upbit.com/reference/list-trading-pairs

- [U6] List Orderbook Instruments  
  https://global-docs.upbit.com/reference/list-orderbook-instruments

- [U7] WebSocket Orderbook  
  https://global-docs.upbit.com/reference/websocket-orderbook

- [U8] Get Available Order Info  
  https://global-docs.upbit.com/reference/available-order-information

- [U9] MyOrder WebSocket  
  https://global-docs.upbit.com/reference/websocket-myorder

- [U10] MyAsset WebSocket  
  https://global-docs.upbit.com/reference/websocket-myasset

- [U11] Create Order  
  https://global-docs.upbit.com/reference/order

- [U12] Orderbook 30-level expansion changelog  
  https://global-docs.upbit.com/v1.2.2/changelog/orderbook_expansion

- [U13] WebSocket Candle minute interval expansion changelog  
  https://global-docs.upbit.com/changelog/websocket_candles_miniutes
