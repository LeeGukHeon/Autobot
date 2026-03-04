# INTEGRATION_REPORT_2026-03-04_T14_2

## 1) 실행 요약
- Ticket: `T14.2`
- Root: `D:\MyApps\Autobot`
- 완료 상태:
  - `features_v2` 기준 v2 학습/평가/레지스트리 저장 완료
  - 동일 기간 v1 vs v2 비교 리포트 생성 완료
  - promotion 정책 평가 완료 (`candidate` 유지)
  - 전체 테스트 통과 (`133 passed`)

## 2) 사용 데이터 (필수 항목 1)
- v2 학습 윈도우: `2026-02-24` ~ `2026-03-03` (UTC 기준)
- `features_v2` stats:
  - `rows_total=11,975`
  - `min_ts_ms=1771891200000` (`2026-02-24T00:00:00Z`)
  - `max_ts_ms=1772553600000` (`2026-03-03T16:00:00Z`)
  - `join_match_ratio=0.4082768166`
  - `micro_available_ratio=1.0`
- distinct_dates (precondition 계산): `8`

## 3) Integrity 체크 결과 (필수 항목 2)
- source: `models/registry/train_v2_micro/20260303T235511Z-s42-0861f465/metrics.json`
- precondition integrity:
  - `candle_integrity_ok=true`
  - `label_integrity_ok=true`
  - `micro_integrity_ok=true`
  - 상세:
    - `candle_non_null_ratio=1.0`
    - `label_non_null_ratio=1.0`
    - `micro_source_valid_ratio=1.0`
    - `micro_meta_non_null_ratio=1.0`
    - `micro_dtype_ok=true`
- coverage (진단 전용):
  - `candles_rows=43,112`
  - `micro_coverage_ratio=0.2777648914`
  - `level=OK`

## 4) v2 성능 (필수 항목 3)
- latest v2 run:
  - run_id: `20260303T235511Z-s42-0861f465`
  - run_dir: `models/registry/train_v2_micro/20260303T235511Z-s42-0861f465`
- test metrics (`model eval --model-ref latest_v2 --split test`):
  - ROC-AUC: `0.5444835066`
  - PR-AUC: `0.4752365722`
  - LogLoss: `2.1281140001`
  - Precision@Top1%: `0.4761904762`
  - Precision@Top5%: `0.5142857143`
  - Precision@Top10%: `0.5023923445`
  - EV_net@Top5%: `-0.0010846635`
- artifacts:
  - `logs/t14_2_eval_full.csv`
  - `logs/train_v2_report.json`

## 5) v1 대비 비교 (필수 항목 4)
- latest v1 run (동일 기간 전용 학습):
  - run_id: `20260303T235252Z-s42-a2e13210`
  - dataset: `data/features/features_v1_t14_2` (v2에서 micro 컬럼 제외한 v1-only 파생셋)
- compare command:
  - `python -m autobot.cli model compare --a latest_v1 --b latest_v2 --start 2026-02-24 --end 2026-03-03`
  - output: `logs/t14_2_compare.json`
- delta (v2 - v1):
  - `precision_top5=-0.0666666667`
  - `ev_net_top5=-0.0024720056`
  - `pr_auc=-0.0202727918`
  - `roc_auc=-0.0150516119`
  - `log_loss_improve=-1.2494664174` (음수: v2가 더 나쁨)
- per-market common markets: `20`

## 6) Promotion 판단 (필수 항목 5)
- source: `models/registry/train_v2_micro/20260303T235511Z-s42-0861f465/promotion_decision.json`
- 결과: `candidate` 유지 (`promote=false`)
- 미충족 사유:
  - `IMPROVEMENT_THRESHOLD_NOT_MET`
  - `PER_MARKET_COLLAPSE_DETECTED`
  - `DISTINCT_DATES_BELOW_PROMOTION_MIN` (`8 < 30`)
  - `REPRODUCIBILITY_NOT_MET`

## 7) 구현/코드 변경
- 신규:
  - `autobot/models/train_v2_micro.py`
  - `docs/ADR/0010-champion-promotion-policy-v2-micro.md`
  - `tests/test_dataset_loader_v2_nested.py`
  - `tests/test_train_v2_micro_preconditions.py`
  - `tests/test_model_compare_v2.py`
- 변경:
  - `autobot/cli.py`
    - `model train --trainer {v1,v2_micro}`
    - `feature-set {v1,v2}`
    - `model compare` 서브커맨드 추가
    - `latest_v1/latest_v2` alias 처리
  - `autobot/models/dataset_loader.py`
    - `date=*` partition 로드 지원
    - `m_trade_source` 숫자 인코딩(0:none, 1:rest, 2:ws)
  - `autobot/models/__init__.py`

## 8) 테스트 결과 (DoD)
- 실행: `python -m pytest -q`
- 결과: `133 passed, 4 warnings`

## 9) 다음 액션 (필수 항목 6)
1. WS 수집 시간을 늘려 full-micro(book 포함) 비율 확장.
2. v2 데이터 일수 `>=30` 확보 후 재학습 2회로 promotion 재평가.
3. sweep trial 확대 및 라벨 파라미터(`horizon_bars`, `thr_bps`) 튜닝.
4. micro parquet 스키마 정규화 CLI(HF) 추가로 수동 재캐스팅 제거.

## 10) 작업 중 실패 및 해결 이력
### 10.1 `features build(v2)` 전 마켓 실패
- 증상:
  - `data type mismatch for column book_min_ts_ms: incoming: Int64 != target: Null`
  - 전 마켓 `FAIL`, `rows_total=0`
- 원인:
  - `micro_v1` parquet 일부 파일이 `book_*`/orderbook 관련 컬럼을 `Null dtype`으로 저장했고, 다른 파일은 `Int64`/`Float`로 저장되어 파일 간 스키마 충돌 발생
- 해결:
  - `data/parquet/micro_v1` 파일 전체를 기대 스키마로 재캐스팅(in-place rewrite)
  - 재실행 후 `features build(v2)` 정상 진행

### 10.2 `rows_total=92`로 학습 행 붕괴
- 증상:
  - 빌드는 성공했지만 `rows_total=92`
- 원인:
  - 기본 v2 micro 필터(`book_events>=1`, `book_coverage_ms>=60000`)가 REST trade-only 구간을 과도하게 제거
- 해결:
  - `--min-book-events 0 --min-book-coverage-ms 0`로 필터 완화
  - 동일 기간 재빌드 후 `rows_total=11,975` 확보

### 10.3 v2 데이터 로딩 실패 (`no feature rows found`)
- 증상:
  - v2 학습 초기 시 `load_feature_dataset`에서 행 0 판단
- 원인:
  - 기존 loader가 `date=*` partition 파일 구조를 탐색하지 않음
  - `m_trade_source` 문자열 컬럼을 float 강제 캐스팅해 v2 입력과 불일치
- 해결:
  - loader 보강:
    - `date=*/*.parquet` 탐색 지원
    - `m_trade_source` 인코딩 (`none=0`, `rest=1`, `ws=2`)

### 10.4 `model compare` 실패 (`latest_v1` 동일기간 데이터 부재)
- 증상:
  - `python -m autobot.cli model compare --a latest_v1 --b latest_v2 ...` 실행 시 `no feature rows found`
- 원인:
  - 기존 `latest_v1` 런은 장기 기간 기준으로 학습되었고, 요청한 단기 윈도우와 데이터 조건이 달라 동일기간 평가 데이터가 맞지 않음
  - `features_v1`를 candles 소스로 다시 빌드하려 했으나 해당 윈도우 캔들 입력 공백으로 `input market frame is empty`
- 해결:
  - `features_v2`에서 micro 컬럼을 제거한 동일기간 `v1-only` 파생셋(`data/features/features_v1_t14_2`) 생성
  - 해당 데이터셋으로 `train_v1` 동일기간 재학습 후 compare 재실행
  - 결과적으로 `latest_v1 vs latest_v2` 동일기간 비교 완성

### 10.5 테스트 실행 커맨드 실패 (`pytest` 미인식)
- 증상:
  - `pytest -q ...` 실행 시 명령 미인식
- 원인:
  - 환경 PATH에 `pytest` 실행파일 미노출
- 해결:
  - `python -m pytest -q ...` 형태로 실행 경로 고정
  - 최종 전체 테스트 `133 passed`

### 10.6 비교 리포트 파일 인코딩 이슈
- 증상:
  - `logs/t14_2_compare.json` 파싱 시 UTF-16/UTF-8 인코딩 혼선
- 원인:
  - PowerShell 리다이렉션(`>`) 기본 인코딩 영향
- 해결:
  - Python으로 JSON을 직접 UTF-8로 재저장

