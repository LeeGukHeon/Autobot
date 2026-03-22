# V4 Feature Dependency Inventory

- Status: dependency inventory snapshot
- Operational authority: no
- Use for:
  - feature dependency lookup, not current runtime truth

- Source: `feature_columns_v4_contract`
- Total features: `112`
- Features that truly need pre-3/4 history: `0`
- Features that can be built from 3/4 onward with only in-window warmup: `112`
- Features still tied to legacy v3 code paths/contracts: `0`

## Block Counts

- `v3_base_core`: `10`
- `v3_high_tf_core`: `15`
- `v3_micro_core`: `36`
- `v3_one_m_core`: `9`
- `v4_interactions`: `6`
- `v4_order_flow_panel_v1`: `9`
- `v4_periodicity`: `7`
- `v4_spillover_breadth`: `14`
- `v4_trend_volume`: `6`

## Interpretation

- The true pre-3/4 history blocker is `v4_ctrend_v1`.
- The rest require only bounded in-window warmup after 2026-03-04.
- So the clean migration path is:
  1. remove/replace `ctrend_v1` if you want a strict 3/4-forward-only runtime contract
  2. keep auditing bounded warmup behavior, especially 240m high-tf coverage and one_m continuity

## Feature Table

| feature | block | pre-3/4 history | legacy code dep | producer |
|---|---|---:|---:|---|
| `logret_1` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `logret_3` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `logret_12` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `logret_36` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `vol_12` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `vol_36` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `range_pct` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `body_pct` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `volume_log` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `volume_z` | `v3_base_core` | `false` | `false` | `compute_base_features_v4_live_base` |
| `one_m_count` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_ret_mean` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_ret_std` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_volume_sum` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_range_mean` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_missing_ratio` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_synth_ratio` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_real_count` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `one_m_real_volume_sum` | `v3_one_m_core` | `false` | `false` | `aggregate_1m_for_base + join_1m_aggregate (v4 live base path)` |
| `tf15m_ret_1` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf15m_ret_3` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf15m_vol_3` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf15m_trend_slope` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf15m_regime_flag` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf60m_ret_1` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf60m_ret_3` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf60m_vol_3` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf60m_trend_slope` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf60m_regime_flag` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf240m_ret_1` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf240m_ret_3` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf240m_vol_3` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf240m_trend_slope` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `tf240m_regime_flag` | `v3_high_tf_core` | `false` | `false` | `compute_high_tf_features + join_high_tf_asof (v4 live base path)` |
| `m_trade_source` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_events` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_book_events` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_min_ts_ms` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_max_ts_ms` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_book_min_ts_ms` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_book_max_ts_ms` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_coverage_ms` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_book_coverage_ms` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_micro_trade_available` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_micro_book_available` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_micro_available` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_count` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_buy_count` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_sell_count` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_volume_total` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_buy_volume` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_sell_volume` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_imbalance` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_vwap` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_avg_trade_size` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_max_trade_size` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_last_trade_price` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_mid_mean` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_spread_bps_mean` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_depth_bid_top5_mean` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_depth_ask_top5_mean` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_imbalance_top5_mean` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_microprice_bias_bps_mean` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_book_update_count` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_spread_proxy` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_volume_base` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_trade_buy_ratio` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_signed_volume` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_source_ws` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `m_source_rest` | `v3_micro_core` | `false` | `false` | `MicroSnapshotProvider -> prefixed micro columns` |
| `btc_ret_1` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `btc_ret_3` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `btc_ret_12` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `eth_ret_1` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `eth_ret_3` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `eth_ret_12` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `leader_basket_ret_1` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `leader_basket_ret_3` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `leader_basket_ret_12` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `market_breadth_pos_1` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `market_breadth_pos_12` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `market_dispersion_12` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `turnover_concentration_hhi` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `rel_strength_vs_btc_12` | `v4_spillover_breadth` | `false` | `false` | `attach_spillover_breadth_features_v4` |
| `hour_sin` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `hour_cos` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `dow_sin` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `dow_cos` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `weekend_flag` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `asia_us_overlap_flag` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `utc_session_bucket` | `v4_periodicity` | `false` | `false` | `attach_periodicity_features_v4` |
| `price_trend_short` | `v4_trend_volume` | `false` | `false` | `attach_trend_volume_features_v4` |
| `price_trend_med` | `v4_trend_volume` | `false` | `false` | `attach_trend_volume_features_v4` |
| `price_trend_long` | `v4_trend_volume` | `false` | `false` | `attach_trend_volume_features_v4` |
| `volume_trend_long` | `v4_trend_volume` | `false` | `false` | `attach_trend_volume_features_v4` |
| `trend_consensus` | `v4_trend_volume` | `false` | `false` | `attach_trend_volume_features_v4` |
| `trend_vs_market` | `v4_trend_volume` | `false` | `false` | `attach_trend_volume_features_v4` |
| `oflow_v1_signed_volume_imbalance_1` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_signed_count_imbalance_1` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_signed_volume_imbalance_3` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_signed_volume_imbalance_12` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_flow_sign_persistence_12` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_depth_conditioned_flow_1` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_trade_book_imbalance_gap_1` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_spread_conditioned_flow_1` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `oflow_v1_microprice_conditioned_flow_1` | `v4_order_flow_panel_v1` | `false` | `false` | `attach_order_flow_panel_v1` |
| `mom_x_illiq` | `v4_interactions` | `false` | `false` | `attach_interaction_features_v4` |
| `mom_x_spread` | `v4_interactions` | `false` | `false` | `attach_interaction_features_v4` |
| `spread_x_vol` | `v4_interactions` | `false` | `false` | `attach_interaction_features_v4` |
| `rel_strength_x_btc_regime` | `v4_interactions` | `false` | `false` | `attach_interaction_features_v4` |
| `one_m_pressure_x_spread` | `v4_interactions` | `false` | `false` | `attach_interaction_features_v4` |
| `volume_z_x_trend` | `v4_interactions` | `false` | `false` | `attach_interaction_features_v4` |

