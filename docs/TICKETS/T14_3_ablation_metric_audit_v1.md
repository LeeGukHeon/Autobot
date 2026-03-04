# T14.3 - Micro Ablation + Metric Audit + Next Lever Decision v1

## Goal
- v2 성능 악화 원인을 추측이 아닌 ablation으로 분해한다.
- LogLoss/ROC-AUC/PR-AUC 계산 경로를 독립 audit으로 검증한다.
- 결과 규칙(R1~R4)에 따라 다음 티켓 1개를 확정한다.

## Scope (INSERT only)
- Added `autobot/models/metric_audit.py`
  - `audit_predictions(...)`
  - `audit_registered_model(...)`
  - `logs/metric_audit_<run_id>.json` 출력
- Added `autobot/models/ablation.py`
  - `run_ablation(...)`
  - `select_ablation_feature_columns(...)` for A0~A4
  - `logs/t14_3_ablation_results.csv`
  - `logs/t14_3_ablation_summary.json`
- Updated `autobot/cli.py`
  - `python -m autobot.cli model audit --model-ref <ref> --split <split>`
  - `python -m autobot.cli model ablate --feature-set v2 ...`
- Updated `autobot/models/__init__.py` exports
- Added tests
  - `tests/test_metric_audit.py`
  - `tests/test_ablation.py`

## CLI
### Metric audit
```bash
python -m autobot.cli model audit --model-ref latest_v1 --split test
python -m autobot.cli model audit --model-ref latest_v2 --split test
```

### Ablation
```bash
python -m autobot.cli model ablate ^
  --feature-set v2 ^
  --tf 5m --start 2026-02-24 --end 2026-03-03 ^
  --quote KRW --top-n 20 ^
  --ablations A0,A1,A2,A3,A4 ^
  --booster-sweep-trials 30 --seed 42 --nthread 6
```

## Ablation Definitions
- A0: OHLC only (`m_*` 제외)
- A1: OHLC + trade micro only (`m_trade_*` + trade meta)
- A2: OHLC + book micro only (`m_book_*` + `m_micro_available`)
- A3: OHLC + full micro (v2 full)
- A4: micro only (`m_*`)

## Diagnostic Metrics Included
- `rows_train/valid/test`
- label positive rate (train/valid/test)
- `micro_coverage_ratio` (T14.2 동일 정의: feature_rows / candle_rows)
- `micro_coverage_p50_ms`, `micro_coverage_p90_ms`
- `book_available_ratio` (`m_book_events > 0`)
- `trade_source_ws/rest/none/other_ratio`

## Test
- `python -m pytest -q tests/test_metric_audit.py tests/test_ablation.py`
- `python -m pytest -q`

결과:
- `138 passed, 4 warnings`

## Notes
- 기존 `train_v1`, `train_v2_micro` 학습/평가 경로 변경 없음.
- 평가 계산 자체 변경 없이 audit/ablation 경로만 추가.
