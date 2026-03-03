# INTEGRATION_REPORT_2026-03-03_T13

## 1) 실행 환경
- OS: Microsoft Windows 10 Pro (10.0.19045)
- Python: 3.14.2
- RAM: 16,699,668 KB (TotalVisibleMemorySize)
- Command lines:
  - features build: `python -m autobot.cli features build --tf 5m --quote KRW --top-n 20 --start 2024-01-01 --end 2026-03-01 --feature-set v1 --label-set v1 --workers 1 --fail-on-warn false`
  - features validate: `python -m autobot.cli features validate --tf 5m --quote KRW --top-n 20`

## 2) Build 결과
- discovered_markets: 239
- selected_markets:
  - KRW-SEI, KRW-BSV, KRW-SOL, KRW-BTC, KRW-XRP, KRW-CTC, KRW-ETC, KRW-MINA, KRW-ASTR, KRW-STX, KRW-AXS, KRW-BCH, KRW-SAND, KRW-ETH, KRW-ARB, KRW-SUI, KRW-AVAX, KRW-XEC, KRW-ADA, KRW-POL
- processed_markets: 20
- ok/warn/fail: 20 / 0 / 0
- rows_total: 3,241,445
- min_ts/max_ts: 1704067800000 / 1771670700000
- output_path: `data/features/features_v1`
- feature_spec_hash: `19469e3342328e147db0ad45522efacf05d9abf7ba41131a502cf3eae5a92bf9`
- label_spec_hash: `2c5859f177c4c0bd13ca5e576dfced797e726110b9423b7a5cbbb0c7b4f0bc60`

## 3) Validate 결과
- schema_ok: true
- null_ratio_overall: 0.0
- worst_columns_top5:
  - atr_14: 0.0
  - btc_log_ret_1: 0.0
  - btc_rv_36: 0.0
  - candle_ok: 0.0
  - ema_12: 0.0
- label_distribution:
  - pos/neg/neutral: 1,592,542 / 1,648,903 / 0
- leakage_smoke: PASS

## 4) 샘플 확인
- KRW-BTC head(5 rows):
  - `{'ts_ms': 1704067800000, 'log_ret_1': 0.0017645470798015594, 'rv_36': 0.0009141307673417032, 'ema_ratio': -2.230971040262375e-05, 'rsi_14': 64.0805892944336, 'y_reg': -0.002201080322265625, 'y_cls': 0}`
  - `{'ts_ms': 1704068100000, 'log_ret_1': 0.0004013926663901657, 'rv_36': 0.0009119776077568531, 'ema_ratio': 0.00028941596974618733, 'rsi_14': 65.51203155517578, 'y_reg': -0.002254486083984375, 'y_cls': 0}`
  - `{'ts_ms': 1704068400000, 'log_ret_1': -0.0005934200598858297, 'rv_36': 0.0009190517594106495, 'ema_ratio': 0.00047708229976706207, 'rsi_14': 61.60374069213867, 'y_reg': -0.00185394287109375, 'y_cls': 0}`
  - `{'ts_ms': 1704069300000, 'log_ret_1': -0.00015718465147074312, 'rv_36': 0.0008873101323843002, 'ema_ratio': 0.000669339788146317, 'rsi_14': 58.75144958496094, 'y_reg': 0.0016231536865234375, 'y_cls': 1}`
  - `{'ts_ms': 1704069600000, 'log_ret_1': -0.0007862940547056496, 'rv_36': 0.0008975410019047558, 'ema_ratio': 0.0006444485625252128, 'rsi_14': 53.700809478759766, 'y_reg': 0.0028629302978515625, 'y_cls': 1}`
- feature NaN 여부: head(5)에서 null 0개, validate 기준 전체 null_ratio 0.0
- label sanity: `y_cls`가 0/1로 정상 생성, `neutral_policy=drop`으로 neutral 0

## 5) 다음 액션(성능 우선)
- T14 진행 조건:
  - validate PASS
  - label 분포가 극단적이지 않음(현재 pos 49.13%, neg 50.87%)
- “추가 데이터 필요” 판정:
  - (T14 결과로 판단) AUC/Precision@K 미달 시 T13.1 발행
