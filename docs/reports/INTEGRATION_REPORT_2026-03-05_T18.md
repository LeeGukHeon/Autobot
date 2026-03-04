# INTEGRATION_REPORT_2026-03-05_T18

## 0) Summary
- Ticket: `T18`
- 정책: `micro mandatory` 유지 (micro 없으면 row 생성/학습 제외)
- 상태: `PASS`
- 핵심 요약:
  - v3 features/build/validate/stats, v3 train/eval, modelbt run 완료
  - v1 동일 기간 재학습(`train_v1_t18`)까지 수행 후 v3-v1 delta 산출

## 1) Effective 기간 (micro mandatory 교집합)
- `requested_start/end`: `2026-02-24` / `2026-03-05`
- `effective_start/end`: `2026-02-24` / `2026-03-04`
- 기간 축소 원인:
  - `micro coverage`: `YES`
  - `multitf staleness`: `NO` (`dropped_multitf_stale=0`, validate `staleness_fail_rows=0`)

## 2) Row 감소 Breakdown
- `rows_base_total`: `63,664`
- `dropped_no_micro`: `28,457`
- `dropped_multitf_stale`: `0`
- `dropped_1m_window_missing`: `19,659`
- `dropped_label_tail_guard`: `324`
- `rows_final`: `9,166`

참고:
- `rows_after_label`: `37,623`
- `micro_keep_ratio_after_label (rows_final / rows_after_label)`: `0.243628`

## 3) Micro 품질 지표
- `book_available_ratio`: `0.117609`
- `trade_source_ws_ratio`: `0.118372`
- `micro_available_ratio` (final rows 기준): `1.000000`

시장별 하위 5 (book_available_ratio):
- `KRW-ENS`: `0.000000`
- `KRW-F`: `0.000000`
- `KRW-BREV`: `0.000000`
- `KRW-AUCTION`: `0.000000`
- `KRW-BOUNTY`: `0.000000`

시장별 상위 5 (book_available_ratio):
- `KRW-BTC`: `0.720000`
- `KRW-ARDR`: `0.666667`
- `KRW-XRP`: `0.633028`
- `KRW-ETH`: `0.587302`
- `KRW-ENSO`: `0.379679`

시장별 하위 5 (micro_keep_ratio_after_label):
- `KRW-ATH`: `0.000000` (`rows_final=0 / rows_after_label=0`)
- `KRW-APT`: `0.000000` (`rows_final=0 / rows_after_label=0`)
- `KRW-BARD`: `0.000000` (`rows_final=0 / rows_after_label=0`)
- `KRW-EGLD`: `0.000000` (`rows_final=0 / rows_after_label=0`)
- `KRW-PENGU`: `0.000000` (`rows_final=0 / rows_after_label=0`)

시장별 상위 5 (micro_keep_ratio_after_label):
- `KRW-CYBER`: `0.969152` (`377 / 389`)
- `KRW-BOUNTY`: `0.802247` (`357 / 445`)
- `KRW-AUCTION`: `0.797945` (`233 / 292`)
- `KRW-F`: `0.793814` (`154 / 194`)
- `KRW-BIRB`: `0.536820` (`503 / 937`)

## 4) 학습/평가 Split 정보
- Split 방식: `time_order` (time split + `embargo_bars=12`)
- 비율: `train/valid/test = 0.70 / 0.15 / 0.15`

v3 (`features_v3`, 같은 기간):
- `rows_train`: `5,822` / `label_positive_rate`: `0.474064`
- `rows_valid`: `1,242` / `label_positive_rate`: `0.415459`
- `rows_test`: `1,781` / `label_positive_rate`: `0.558675`

v1 동일기간 재학습 (`train_v1_t18`, 같은 기간):
- `rows_train`: `65,056` / `label_positive_rate`: `0.477266`
- `rows_valid`: `12,672` / `label_positive_rate`: `0.421796`
- `rows_test`: `13,168` / `label_positive_rate`: `0.518150`

## 5) 모델 성능 지표 (v1 동일기간 재학습 비교)
v3 (`train_v3_mtf_micro`, test):
- `ROC-AUC`: `0.718156`
- `PR-AUC`: `0.746209`
- `LogLoss`: `0.816610`
- `Precision@Top5%`: `0.877778`
- `EV_net@Top5%`: `0.010042`

v1 동일기간 재학습 (`train_v1_t18`, test):
- `ROC-AUC`: `0.569565`
- `PR-AUC`: `0.570645`
- `LogLoss`: `0.761838`
- `Precision@Top5%`: `0.634294`
- `EV_net@Top5%`: `0.003030`

Delta (`v3 - v1`):
- `ROC-AUC`: `+0.148591`
- `PR-AUC`: `+0.175564`
- `LogLoss`: `+0.054772` (낮을수록 좋으므로 v3 열화)
- `Precision@Top5%`: `+0.243483`
- `EV_net@Top5%`: `+0.007011`

## 6) modelbt Proxy 결과
- `trades_count`: `2,156`
- `win_rate`: `0.530612`
- `avg_return_net`: `0.001420`
- `max_drawdown`: `27.518289`
- `equity_end`: `19.605317`

Top5% 진입 빈도(실거래 가능성 점검):
- `selected_rows / scored_rows = 2,156 / 9,166 = 0.235217 (23.52%)`
- 해석: 설정값 `top_pct=0.05` 대비 실제 선택률이 높음.
  - 원인: `modelbt`가 `ts_ms`별 cross-sectional top-%를 고르며 `ceil`+최소 1개 선택 규칙을 사용하기 때문.

## 7) 산출물 경로 (Registry/Report/Run)
- v3 run:
  - `run_id`: `20260304T171401Z-s42-9f9d66de`
  - `model registry path`: `models/registry/train_v3_mtf_micro/20260304T171401Z-s42-9f9d66de`
- v1 동일기간 재학습 run:
  - `run_id`: `20260304T172405Z-s42-94713d5e`
  - `model registry path`: `models/registry/train_v1_t18/20260304T172405Z-s42-94713d5e`
- features v3:
  - `build report`: `data/features/features_v3/_meta/build_report.json`
  - `validate report`: `data/features/features_v3/_meta/validate_report.json`
- modelbt:
  - `run path`: `data/backtest/runs/modelbt-20260304T171551Z-e72106333e`
  - `summary`: `data/backtest/runs/modelbt-20260304T171551Z-e72106333e/summary.json`
  - `diagnostics`: `data/backtest/runs/modelbt-20260304T171551Z-e72106333e/diagnostics.json`
