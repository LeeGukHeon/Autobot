# INTEGRATION_REPORT_2026-03-04_T14_3

## 1) 실행 요약
- Ticket: `T14.3`
- Root: `D:\MyApps\Autobot`
- 기준 기간(UTC): `2026-02-24` ~ `2026-03-03`
- 기준 유니버스: `KRW top20`
- 상태:
  - Metric Audit 구현 + 실행 완료
  - A0~A4 Ablation 구현 + 실행 완료
  - 규칙(R1~R4) 적용으로 다음 티켓 1개 확정 완료
  - 테스트 통과: `138 passed, 4 warnings`

## 2) Metric Audit 결과 (설계서 1번)
### 2.1 latest_v1
- command:
  - `python -m autobot.cli model audit --model-ref latest_v1 --split test`
- run_id: `20260303T235252Z-s42-a2e13210`
- status: `PASS`
- mismatch: `없음 (diff 0.0)`
- report: `logs/metric_audit_20260303T235252Z-s42-a2e13210.json`

### 2.2 latest_v2
- command:
  - `python -m autobot.cli model audit --model-ref latest_v2 --split test`
- run_id: `20260303T235511Z-s42-0861f465`
- status: `PASS`
- mismatch: `없음 (diff 0.0)`
- report: `logs/metric_audit_20260303T235511Z-s42-0861f465.json`

### 2.3 결론
- R4(`metrics audit FAIL`)는 발동하지 않음.
- `LogLoss=2.128114...` 값은 계산 구현 문제보다는 데이터/모델 특성으로 해석 가능.

## 3) Ablation 결과 (설계서 2번)
### 3.1 실행 커맨드
```bash
python -m autobot.cli model ablate --feature-set v2 --tf 5m --start 2026-02-24 --end 2026-03-03 --quote KRW --top-n 20 --ablations A0,A1,A2,A3,A4 --booster-sweep-trials 30 --seed 42 --nthread 6
```

### 3.2 산출물
- CSV: `logs/t14_3_ablation_results.csv`
- Summary: `logs/t14_3_ablation_summary.json`

### 3.3 결과표 (test split)
| Ablation | ROC-AUC | PR-AUC | LogLoss | Precision@Top5% | EV_net@Top5% |
|---|---:|---:|---:|---:|---:|
| A0 (OHLC) | 0.559535 | 0.495509 | 0.878648 | 0.580952 | 0.001387 |
| A1 (OHLC+trade) | 0.555758 | 0.498025 | 2.245682 | 0.580952 | 0.000511 |
| A2 (OHLC+book) | 0.560073 | 0.499198 | 0.786800 | 0.561905 | -0.000701 |
| A3 (OHLC+full micro) | 0.535788 | 0.478041 | 1.662864 | 0.552381 | 0.000013 |
| A4 (micro only) | 0.525874 | 0.463671 | 1.042912 | 0.438095 | -0.001742 |

best:
- `best_ablation_by_prec_top5 = A1` (A0와 동률 precision)
- `best_ablation_by_ev_top5 = A0`
- `best_ablation_by_pr_auc = A2`

## 4) 필수 진단 지표 (설계서 2.3)
- `rows_total=11,975`, `rows_train=7,722`, `rows_valid=1,645`, `rows_test=2,082`
- label distribution:
  - train positive rate: `0.482517`
  - valid positive rate: `0.416413`
  - test positive rate: `0.442843`
- micro coverage:
  - `candles_rows=43,112`
  - `micro_coverage_ratio=0.2777648914`
  - `micro_coverage_p50_ms=235,866`
  - `micro_coverage_p90_ms=290,224`
- book/trade availability:
  - `book_available_ratio=0.0076826722`
  - `trade_source_rest_ratio=0.9923173278`
  - `trade_source_ws_ratio=0.0076826722`
  - `trade_source_none_ratio=0.0`

해석 포인트:
- `book_available_ratio < 0.01`로 book micro는 사실상 거의 없음.
- trade source는 거의 `rest`이며 ws 비율이 매우 낮음.
- full micro(A3)가 A0 대비 EV/precision 모두 우세하지 않음.

## 5) 규칙(R1~R4) 적용 결과 (설계서 3번)
- R1 점검 (A1 vs A0):
  - `ΔPrecision@Top5 = 0.000000` (기준 +0.02 미달)
  - `ΔEV_net@Top5 = -0.000876` (기준 +0.0002 미달)
  - R1 미충족
- R2 점검 (A2/A3 + 낮은 book coverage):
  - `book_available_ratio=0.00768`은 매우 낮음
  - 다만 A2/A3가 A0 대비 핵심 지표(Top5 precision/EV) 우세하지 않아 본 규칙 조건 미충족
- R3 점검 (micro 전반 해로움):
  - A0가 A1/A3 대비 `EV_net@Top5` 우세
  - A0가 A3 대비 `Precision@Top5`도 우세
  - R3 충족으로 판정
- R4:
  - audit PASS로 미충족

### 확정된 다음 티켓 (1개)
- **`T15`**: micro를 모델 입력 중심이 아니라 TradeGate/리스크 필터 중심으로 전환 검증

## 6) 작업 중 실패/경고 이력 및 해결
### 6.1 실패: audit 실행 예외
- 증상:
  - `AttributeError: 'dict' object has no attribute 'train'`
- 원인:
  - `split_info.counts`가 dataclass 속성이 아닌 dict인데 attribute 접근함
- 해결:
  - dict key 접근으로 수정 (`counts.get(...)`)
  - 재실행 후 v1/v2 모두 audit `PASS`

### 6.2 경고: ablation 실행 중 sklearn warning 다수
- 증상:
  - `UndefinedMetricWarning: Only one class is present in y_true`
  - 일부 per-market 계산에서 발생
- 원인:
  - per-market 분할 샘플이 작아 단일 클래스 구간 존재
- 조치:
  - 실행은 정상 완료(치명 오류 아님)
  - 요약 지표는 전체 split 기준 값으로 정상 생성됨

## 7) 구현 파일
- 추가:
  - `autobot/models/metric_audit.py`
  - `autobot/models/ablation.py`
  - `tests/test_metric_audit.py`
  - `tests/test_ablation.py`
  - `docs/TICKETS/T14_3_ablation_metric_audit_v1.md`
  - `docs/reports/INTEGRATION_REPORT_2026-03-04_T14_3.md`
- 변경:
  - `autobot/cli.py`
  - `autobot/models/__init__.py`

## 8) 검증
- `python -m pytest -q tests/test_metric_audit.py tests/test_ablation.py`
- `python -m pytest -q`
- 결과: `138 passed, 4 warnings`
