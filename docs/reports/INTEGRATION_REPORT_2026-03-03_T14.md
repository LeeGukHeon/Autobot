# INTEGRATION_REPORT_2026-03-03_T14

## 1) Execution Environment
- OS / Python / RAM: Windows 10 / Python 3.14.2 / 16GB
- Packages:
  - scikit-learn 1.8.0
  - joblib 1.5.3
  - xgboost 3.2.0
- tf/quote/top_n/start/end: 5m / KRW / 20 / 2024-01-01 / 2026-03-01
- run_id: `20260303T112316Z-s42-c29a555a`
- commands:
  - `python -m autobot.cli model train --tf 5m --quote KRW --top-n 20 --start 2024-01-01 --end 2026-03-01 --feature-set v1 --label-set v1 --task cls --run-baseline true --run-booster true --booster-sweep-trials 15 --seed 42 --nthread 6`
  - `python -m autobot.cli model eval --model-ref latest --split test --report-csv logs/t14_eval_full.csv`

## 2) Data Summary
- source: `data/features/features_v1`
- feature_spec hash: `97d648e94456861868eb3527adf515b796634876dee360a022c7573357658b68`
- label_spec hash: `ab2577e545df8d9985f42486fb9560d43b84657fffbbc9e148eb717123e9a9a7`
- rows_total: 3,241,445
- split rows (train/valid/test/drop): 2,325,501 / 463,176 / 452,018 / 750
- embargo bars: 12
- class balance (pos rate):
  - test: 0.480709

## 3) Model Results (Summary)
### Baseline (A1, SGD)
- ROC-AUC / PR-AUC / LogLoss: 0.559819 / 0.520650 / 0.689137
- Precision@Top1/5/10%: 0.498783 / 0.520906 / 0.531127
- ev_net@Top5%: -0.000995

### Booster (B1, XGBoost)
- ROC-AUC / PR-AUC / LogLoss: 0.564382 / 0.533478 / 0.690405
- Precision@Top1/5/10%: 0.586596 / 0.575329 / 0.572807
- ev_net@Top5%: 0.000071

## 4) Sweep Results (Booster)
- trials: 15
- best params:
  - learning_rate=0.06221013104432857
  - max_depth=6
  - subsample=0.7902819704903735
  - colsample_bytree=0.6907637396203536
  - min_child_weight=5.688697962777573
  - reg_lambda=3.7786393915424803
  - reg_alpha=1.6653563921156749
  - max_bin=256
- best valid selection key:
  - precision_top5=0.615096
  - pr_auc=0.548864
  - roc_auc=0.555573
- test score (single final evaluation):
  - precision_top5=0.575329
  - pr_auc=0.533478
  - roc_auc=0.564382

## 5) Champion Selection
- champion: `booster` (`xgboost`)
- evidence:
  - test Precision@Top5%: 0.575329 (baseline 0.520906)
  - test PR-AUC: 0.533478 (baseline 0.520650)
  - test ROC-AUC: 0.564382 (baseline 0.559819)

## 6) Threshold Outputs
- top_1pct: 0.7250073552131653
- top_5pct: 0.6405096054077148
- top_10pct: 0.6010412573814392
- ev_opt: 0.8260555550456067
- ev_opt_ev_net: 0.00390746388764217
- ev_opt_selected_rows: 464

## 7) Stability / T13.1 Gate
- per-market summary @Top5:
  - precision mean/std: 0.565630 / 0.054241
  - ev_net mean/std: -0.000205 / 0.001456
  - positive markets: 8 / 20
- automatic decision:
  - `trigger_t13_1 = false`
  - reasons: none

## 8) Runtime Report
- train duration: 1597.592 sec
- memory estimate during train: 287.49 MB (feature tensors only)
- train report path: `logs/train_report.json`
