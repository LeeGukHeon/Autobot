# T18.7 Daily Champion-Challenger Loop v1

## 목표

- `v4` 단일 운영 레인에서 `champion` 상시 paper와 `challenger` 일일 재학습/검증을 분리한다.
- 장중 즉시 승급 대신 일일 컷오프에서만 promote 하도록 고정한다.
- 이후 실시간 실거래 레인이 붙더라도 동일 `champion_v4` 포인터와 재기동 훅을 재사용할 수 있게 만든다.

## 기본 루프

1. 직전 challenger가 있으면 컷오프 시점에 champion 대비 paper evidence를 비교한다.
2. challenger가 promote 조건을 만족하면 `champion_v4`로 승급한다.
3. 승급 후 `champion` runtime unit과 optional promotion target units를 재기동한다.
4. 전일 batch date로 `v4` partial acceptance를 실행한다.
   - `train`
   - fixed-profile `backtest sanity gate`
   - `-SkipPaperSoak`
   - `-SkipPromote`
5. backtest sanity를 통과한 candidate만 `challenger` paper runtime으로 기동한다.
6. challenger는 다음 컷오프까지 day-long paper evidence를 축적한다.

## 서비스 역할

- `autobot-paper-v4.service`
  - 역할: `champion`
  - preset: `live_v4`
- `autobot-paper-v4-challenger.service`
  - 역할: `challenger`
  - preset: `live_v4`
  - model ref: candidate run id pinned
- `autobot-daily-micro.service`
  - wrapper: `scripts/daily_champion_challenger_v4_for_server.ps1`
  - timer: `autobot-daily-micro.timer` at `00:10 KST`

## 메타데이터 계약

Paper summary와 `RUN_STARTED` 이벤트에는 아래 메타데이터를 남긴다.

- `paper_unit_name`
- `paper_runtime_role`
- `paper_lane`
- `paper_runtime_model_ref`
- `paper_runtime_model_ref_pinned`
- `run_started_ts_ms`
- `run_completed_ts_ms`

이 메타데이터는 champion/challenger evidence 비교와 이후 live unit 재기동 확장성의 SSOT로 사용한다.

## 승급 비교 기준

현재 v1 비교는 `autobot.common.paper_lane_evidence`가 담당한다.

- challenger minimum hours
- minimum filled orders
- minimum realized pnl
- minimum micro quality
- minimum nonnegative active-window ratio
- champion 대비:
  - pnl not worse
  - drawdown not materially worse
  - micro quality not worse
  - nonnegative ratio not worse
  - fill rate not worse

이 비교는 운영 자동화용 day-long paper gate이며, 기존 fixed compare `backtest sanity gate`를 대체하지 않는다.

## 미래 live 확장

- live runtime이 추가되면 `PromotionTargetUnits`에 live unit 이름만 추가한다.
- promote 기준과 model pointer는 `champion_v4` 하나를 공유한다.
- paper와 live가 다른 service unit을 써도, 승급 포인터/재기동 훅은 공통으로 유지한다.

## 비목표

- 장중 자동 승급
- 당일 candidate를 즉시 champion으로 교체
- separate `v3` production lane 복구
