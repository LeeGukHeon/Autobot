# INTEGRATION_REPORT_2026-03-03_T14

## 1) 실행 환경
- OS / Python / CPU / RAM: Windows 10 / Python 3.14.2 / local workstation / 16GB
- tf/quote/top_n/start/end: 5m / KRW / 3 / 2024-01-01 / 2024-01-15 (smoke run)
- feature_spec hash / label_spec hash:
  - `97d648e94456861868eb3527adf515b796634876dee360a022c7573357658b68`
  - `ab2577e545df8d9985f42486fb9560d43b84657fffbbc9e148eb717123e9a9a7`
- commands:
  - `python -m autobot.cli model train --tf 5m --quote KRW --top-n 3 --start 2024-01-01 --end 2024-01-15 --feature-set v1 --label-set v1 --task cls --run-baseline true --run-booster true --booster-sweep-trials 2 --seed 42 --nthread 4`
  - `python -m autobot.cli model eval --model-ref latest --split test --report-csv logs/t14_eval_sample.csv`

## 2) 데이터 요약
- source: `data/features/features_v1`
- rows_total: 10688
- train/valid/test/drop: 7549 / 1520 / 1502 / 117
- class balance(pos rate):
  - valid: 0.453289
  - test : 0.470040
- embargo bars: 12

## 3) 모델 결과(요약)
### Baseline(A1)
- ROC-AUC / PR-AUC / LogLoss: 0.460562 / 0.468067 / 1.867131
- Precision@Top1/5/10%: 0.562500 / 0.605263 / 0.536424
- ev_net@Top5%: 0.000140

### Booster(B1)
- ROC-AUC / PR-AUC / LogLoss: 0.461550 / 0.434123 / 1.055232
- Precision@Top1/5/10%: 0.250000 / 0.328947 / 0.350993
- ev_net@Top5%: -0.003783

## 4) Sweep 결과(booster)
- trials: 2
- best params (HGB fallback): learning_rate=0.1616, max_depth=6, max_leaf_nodes=36, min_samples_leaf=63, l2_regularization=1.3947, max_iter=450
- test score(최종 1회): Precision@Top5%=0.328947, PR-AUC=0.434123

## 5) Champion 선정
- champion: baseline (sgd_logistic)
- 근거:
  - test Precision@Top5%: 0.605263 (booster 0.328947)
  - test PR-AUC: 0.468067 (booster 0.434123)
  - test EV@Top5%: 0.000140 (booster -0.003783)

## 6) 다음 액션(성능 최우선)
- sweep trials 확대: 2 -> 15 -> 50 (xgboost 설치 후 재실행 권장)
- horizon/threshold 재튜닝 후 재학습
- 현재 run은 `PR_AUC_BELOW_GATE`로 T13.1 권고 플래그가 활성화됨 (orderbook/trade WS 수집)
