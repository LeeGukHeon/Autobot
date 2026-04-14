"""Builder for 2-stage market-state and label datasets on top of the source plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
from typing import Any

import numpy as np
import polars as pl

from ..collect.fixed_collection_contract import resolve_fixed_collection_markets
from ..micro.raw_readers import iter_jsonl_zst_rows, parse_date_range
from ..sources.trades.writer import read_raw_trade_part_file


UTC = timezone.utc
BUCKET_INTERVAL_MS = 5_000
ONE_MIN_MS = 60_000
ONE_DAY_MS = 86_400_000
DEFAULT_QUOTE = "KRW"
DEFAULT_JOIN_EXTRA_COST_BPS = 3.0
DEFAULT_DEPTH_HAIRCUT = 0.50
DEFAULT_NO_TRADE_THRESHOLD_BPS = 3.0
DEFAULT_UPBIT_FEE_BPS = 5.0
DEFAULT_MAX_JOIN_SPREAD_BPS = 20.0
PRIMARY_HORIZON_MINUTES = 20
SECONDARY_HORIZON_MINUTES = (10, 40)
ALL_HORIZON_MINUTES = (10, 20, 40)

MARKET_STATE_SCHEMA: dict[str, pl.DataType] = {
    "market": pl.Utf8,
    "bucket_start_ts_ms": pl.Int64,
    "bucket_end_ts_ms": pl.Int64,
    "bucket_date_utc": pl.Utf8,
    "last_price": pl.Float64,
    "acc_trade_price_24h": pl.Float64,
    "signed_change_rate": pl.Float64,
    "ticker_age_ms": pl.Int64,
    "trade_events_5s": pl.Int64,
    "trade_events_15s": pl.Int64,
    "trade_events_60s": pl.Int64,
    "trade_notional_5s": pl.Float64,
    "trade_notional_60s": pl.Float64,
    "buy_volume_5s": pl.Float64,
    "sell_volume_5s": pl.Float64,
    "signed_volume_5s": pl.Float64,
    "trade_imbalance_5s": pl.Float64,
    "vwap_5s": pl.Float64,
    "large_trade_ratio_60s": pl.Float64,
    "best_bid": pl.Float64,
    "best_ask": pl.Float64,
    "spread_bps": pl.Float64,
    "bid_depth_top1_krw": pl.Float64,
    "ask_depth_top1_krw": pl.Float64,
    "bid_depth_top5_krw": pl.Float64,
    "ask_depth_top5_krw": pl.Float64,
    "queue_imbalance_top1": pl.Float64,
    "queue_imbalance_top5": pl.Float64,
    "microprice": pl.Float64,
    "microprice_bias_bps": pl.Float64,
    "book_update_count_5s": pl.Int64,
    "ret_1m": pl.Float64,
    "ret_5m": pl.Float64,
    "ret_15m": pl.Float64,
    "ret_60m": pl.Float64,
    "rv_1m_5m_window": pl.Float64,
    "rv_1m_15m_window": pl.Float64,
    "atr_pct_14": pl.Float64,
    "distance_from_15m_high_low": pl.Float64,
    "btc_rel_strength_5m": pl.Float64,
    "eth_rel_strength_5m": pl.Float64,
    "market_cap_rank_fixed30": pl.Int64,
    "universe_breadth_up_ratio": pl.Float64,
    "universe_notional_rank_pct": pl.Float64,
    "ticker_available": pl.Boolean,
    "trade_available": pl.Boolean,
    "book_available": pl.Boolean,
    "candle_context_available": pl.Boolean,
    "source_quality_score": pl.Float64,
}

TRADEABLE_LABEL_SCHEMA: dict[str, pl.DataType] = {
    "market": pl.Utf8,
    "bucket_start_ts_ms": pl.Int64,
    "bucket_end_ts_ms": pl.Int64,
    "bucket_date_utc": pl.Utf8,
    "label_available_20m": pl.Boolean,
    "spread_quality_pass_20m": pl.Boolean,
    "liquidity_pass_20m": pl.Boolean,
    "structure_pass_20m": pl.Boolean,
    "tradeable_20m": pl.Int8,
}

NET_EDGE_LABEL_SCHEMA: dict[str, pl.DataType] = {
    "market": pl.Utf8,
    "bucket_start_ts_ms": pl.Int64,
    "bucket_end_ts_ms": pl.Int64,
    "bucket_date_utc": pl.Utf8,
    "entry_best_ask": pl.Float64,
    "entry_best_ask_depth_top5_krw": pl.Float64,
    "entry_spread_bps": pl.Float64,
    "gross_return_10m_bps": pl.Float64,
    "gross_return_20m_bps": pl.Float64,
    "gross_return_40m_bps": pl.Float64,
    "net_edge_10m_bps": pl.Float64,
    "net_edge_20m_bps": pl.Float64,
    "net_edge_40m_bps": pl.Float64,
    "future_best_bid_10m": pl.Float64,
    "future_best_bid_20m": pl.Float64,
    "future_best_bid_40m": pl.Float64,
    "future_bid_depth_top5_krw_10m": pl.Float64,
    "future_bid_depth_top5_krw_20m": pl.Float64,
    "future_bid_depth_top5_krw_40m": pl.Float64,
}

DERIVED_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "run_id": pl.Utf8,
    "dataset_name": pl.Utf8,
    "date": pl.Utf8,
    "market": pl.Utf8,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "part_file": pl.Utf8,
    "built_at_ms": pl.Int64,
}


@dataclass(frozen=True)
class MarketStateBuildOptions:
    start: str
    end: str
    quote: str = DEFAULT_QUOTE
    markets: tuple[str, ...] | None = None
    raw_ws_root: Path = Path("data/raw_ws/upbit/public")
    raw_trade_root: Path = Path("data/raw_trade_v1")
    candles_root: Path = Path("data/parquet/candles_api_v1")
    market_state_root: Path = Path("data/derived/market_state_v1")
    tradeable_label_root: Path = Path("data/derived/tradeable_label_v1")
    net_edge_label_root: Path = Path("data/derived/net_edge_label_v1")
    config_dir: Path = Path("config")
    bucket_interval_ms: int = BUCKET_INTERVAL_MS
    join_extra_cost_bps: float = DEFAULT_JOIN_EXTRA_COST_BPS
    depth_haircut: float = DEFAULT_DEPTH_HAIRCUT
    no_trade_threshold_bps: float = DEFAULT_NO_TRADE_THRESHOLD_BPS
    fee_bps: float = DEFAULT_UPBIT_FEE_BPS
    max_join_spread_bps: float = DEFAULT_MAX_JOIN_SPREAD_BPS
    closed_utc_dates_only: bool = True


@dataclass(frozen=True)
class MarketStateBuildSummary:
    run_id: str
    dates: tuple[str, ...]
    selected_markets: tuple[str, ...]
    built_pairs: int
    skipped_pairs: int
    market_state_manifest_file: Path
    tradeable_manifest_file: Path
    net_edge_manifest_file: Path
    build_report_file: Path


def build_market_state_v1_datasets(options: MarketStateBuildOptions) -> MarketStateBuildSummary:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dates = _resolve_target_dates(
        start=str(options.start).strip(),
        end=str(options.end).strip(),
        closed_utc_dates_only=bool(options.closed_utc_dates_only),
    )
    selected_markets = _resolve_selected_markets(
        config_dir=Path(options.config_dir),
        quote=str(options.quote).strip().upper() or DEFAULT_QUOTE,
        explicit_markets=tuple(options.markets or ()),
    )
    market_rank_map = {market: idx + 1 for idx, market in enumerate(selected_markets)}
    build_details: list[dict[str, Any]] = []
    market_state_manifest_rows: list[dict[str, Any]] = []
    tradeable_manifest_rows: list[dict[str, Any]] = []
    net_edge_manifest_rows: list[dict[str, Any]] = []
    built_pairs = 0
    skipped_pairs = 0

    for date_value in dates:
        date_ticker_frames = _load_date_ticker_frames(
            raw_ws_root=options.raw_ws_root,
            date_value=date_value,
            selected_markets=selected_markets,
        )
        date_orderbook_frames = _load_date_orderbook_frames(
            raw_ws_root=options.raw_ws_root,
            date_value=date_value,
            selected_markets=selected_markets,
        )
        date_trade_frames = _load_date_trade_frames(
            raw_trade_root=options.raw_trade_root,
            date_value=date_value,
            selected_markets=selected_markets,
        )
        candle_cache: dict[tuple[str, str, int, int], pl.DataFrame] = {}
        date_ticker_rows_total = sum(int(frame.height) for frame in date_ticker_frames.values())
        date_orderbook_rows_total = sum(int(frame.height) for frame in date_orderbook_frames.values())
        date_trade_rows_total = sum(int(frame.height) for frame in date_trade_frames.values())
        per_market_frames: dict[str, pl.DataFrame] = {}
        per_market_tradeable_labels: dict[str, pl.DataFrame] = {}
        per_market_net_edge_labels: dict[str, pl.DataFrame] = {}

        for market in selected_markets:
            state_frame, tradeable_labels, net_edge_labels, detail = _build_market_date_payload(
                options=options,
                date_value=date_value,
                market=market,
                market_cap_rank=market_rank_map[market],
                ticker_frame=date_ticker_frames.get(market, _empty_ticker_frame()),
                orderbook_frame=date_orderbook_frames.get(market, _empty_orderbook_frame()),
                trade_frame=date_trade_frames.get(market, _empty_trade_frame()),
                candle_cache=candle_cache,
            )
            detail["date_ticker_rows_total"] = int(date_ticker_rows_total)
            detail["date_orderbook_rows_total"] = int(date_orderbook_rows_total)
            detail["date_trade_rows_total"] = int(date_trade_rows_total)
            detail["date_selected_markets"] = int(len(selected_markets))
            build_details.append(detail)
            if state_frame.height <= 0:
                skipped_pairs += 1
                continue
            per_market_frames[market] = state_frame
            per_market_tradeable_labels[market] = tradeable_labels
            per_market_net_edge_labels[market] = net_edge_labels

        if per_market_frames:
            _attach_universe_context(per_market_frames)
            _attach_leader_relative_strength(
                per_market_frames,
                leader_market="KRW-BTC",
                target_column="btc_rel_strength_5m",
            )
            _attach_leader_relative_strength(
                per_market_frames,
                leader_market="KRW-ETH",
                target_column="eth_rel_strength_5m",
            )

        for market in sorted(per_market_frames.keys()):
            state_frame = per_market_frames[market]
            tradeable_labels = per_market_tradeable_labels[market]
            net_edge_labels = per_market_net_edge_labels[market]
            _remove_existing_market_date_pair(root=options.market_state_root, date_value=date_value, market=market)
            _remove_existing_market_date_pair(root=options.tradeable_label_root, date_value=date_value, market=market)
            _remove_existing_market_date_pair(root=options.net_edge_label_root, date_value=date_value, market=market)
            built_at_ms = int(datetime.now(UTC).timestamp() * 1000)
            state_part = _write_market_date_parquet(
                root=options.market_state_root,
                dataset_name="market_state_v1",
                frame=state_frame,
                run_id=run_id,
                date_value=date_value,
                market=market,
            )
            tradeable_part = _write_market_date_parquet(
                root=options.tradeable_label_root,
                dataset_name="tradeable_label_v1",
                frame=tradeable_labels,
                run_id=run_id,
                date_value=date_value,
                market=market,
            )
            net_edge_part = _write_market_date_parquet(
                root=options.net_edge_label_root,
                dataset_name="net_edge_label_v1",
                frame=net_edge_labels,
                run_id=run_id,
                date_value=date_value,
                market=market,
            )
            built_pairs += 1
            market_state_manifest_rows.append(_manifest_row(run_id, "market_state_v1", date_value, market, built_at_ms, state_part))
            tradeable_manifest_rows.append(_manifest_row(run_id, "tradeable_label_v1", date_value, market, built_at_ms, tradeable_part))
            net_edge_manifest_rows.append(_manifest_row(run_id, "net_edge_label_v1", date_value, market, built_at_ms, net_edge_part))

    _replace_manifest_pairs(
        root=options.market_state_root,
        rebuilt_pairs=[(row["date"], row["market"]) for row in market_state_manifest_rows],
    )
    _replace_manifest_pairs(
        root=options.tradeable_label_root,
        rebuilt_pairs=[(row["date"], row["market"]) for row in tradeable_manifest_rows],
    )
    _replace_manifest_pairs(
        root=options.net_edge_label_root,
        rebuilt_pairs=[(row["date"], row["market"]) for row in net_edge_manifest_rows],
    )
    _append_manifest_rows(options.market_state_root, market_state_manifest_rows)
    _append_manifest_rows(options.tradeable_label_root, tradeable_manifest_rows)
    _append_manifest_rows(options.net_edge_label_root, net_edge_manifest_rows)
    _write_market_state_contracts(options=options, selected_markets=selected_markets)

    report = {
        "policy": "market_state_v1_build_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_id": run_id,
        "dates": list(dates),
        "selected_markets": list(selected_markets),
        "quote": str(options.quote).strip().upper() or DEFAULT_QUOTE,
        "bucket_interval_ms": int(options.bucket_interval_ms),
        "join_extra_cost_bps": float(options.join_extra_cost_bps),
        "depth_haircut": float(options.depth_haircut),
        "no_trade_threshold_bps": float(options.no_trade_threshold_bps),
        "fee_bps": float(options.fee_bps),
        "max_join_spread_bps": float(options.max_join_spread_bps),
        "built_pairs": int(built_pairs),
        "skipped_pairs": int(skipped_pairs),
        "market_state_root": str(options.market_state_root),
        "tradeable_label_root": str(options.tradeable_label_root),
        "net_edge_label_root": str(options.net_edge_label_root),
        "details": build_details,
    }
    build_report_path = options.market_state_root / "_meta" / "build_report.json"
    build_report_path.parent.mkdir(parents=True, exist_ok=True)
    build_report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest_path = options.market_state_root / "_meta" / "market_state_v1_latest.json"
    latest_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return MarketStateBuildSummary(
        run_id=run_id,
        dates=dates,
        selected_markets=selected_markets,
        built_pairs=built_pairs,
        skipped_pairs=skipped_pairs,
        market_state_manifest_file=_manifest_path(options.market_state_root),
        tradeable_manifest_file=_manifest_path(options.tradeable_label_root),
        net_edge_manifest_file=_manifest_path(options.net_edge_label_root),
        build_report_file=build_report_path,
    )


def _build_market_date_payload(
    *,
    options: MarketStateBuildOptions,
    date_value: str,
    market: str,
    market_cap_rank: int,
    ticker_frame: pl.DataFrame,
    orderbook_frame: pl.DataFrame,
    trade_frame: pl.DataFrame,
    candle_cache: dict[tuple[str, str, int, int], pl.DataFrame],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    bucket_interval_ms = max(int(options.bucket_interval_ms), 1)
    date_start_ts_ms = _parse_date_to_ts_ms(date_value)
    date_end_ts_ms = _parse_date_to_ts_ms(date_value, end_of_day=True)
    if date_start_ts_ms is None or date_end_ts_ms is None:
        raise ValueError(f"invalid date: {date_value}")

    source_min_ts = _min_non_null(
        ticker_frame.get_column("ticker_ts_ms").min() if ticker_frame.height > 0 else None,
        orderbook_frame.get_column("book_ts_ms").min() if orderbook_frame.height > 0 else None,
        trade_frame.get_column("event_ts_ms").min() if trade_frame.height > 0 else None,
    )
    source_max_ts = _max_non_null(
        ticker_frame.get_column("ticker_ts_ms").max() if ticker_frame.height > 0 else None,
        orderbook_frame.get_column("book_ts_ms").max() if orderbook_frame.height > 0 else None,
        trade_frame.get_column("event_ts_ms").max() if trade_frame.height > 0 else None,
    )
    if source_min_ts is None or source_max_ts is None:
        return (
            pl.DataFrame(schema=MARKET_STATE_SCHEMA),
            pl.DataFrame(schema=TRADEABLE_LABEL_SCHEMA),
            pl.DataFrame(schema=NET_EDGE_LABEL_SCHEMA),
            {"date": date_value, "market": market, "status": "WARN", "reasons": ["SOURCE_EVENTS_MISSING"]},
        )

    bucket_start_floor = _bucket_floor(max(int(source_min_ts), int(date_start_ts_ms)), bucket_interval_ms)
    bucket_end_floor = _bucket_floor(min(int(source_max_ts), int(date_end_ts_ms)), bucket_interval_ms)
    if bucket_end_floor < bucket_start_floor:
        return (
            pl.DataFrame(schema=MARKET_STATE_SCHEMA),
            pl.DataFrame(schema=TRADEABLE_LABEL_SCHEMA),
            pl.DataFrame(schema=NET_EDGE_LABEL_SCHEMA),
            {"date": date_value, "market": market, "status": "WARN", "reasons": ["EMPTY_BUCKET_RANGE"]},
        )

    bucket_count = ((bucket_end_floor - bucket_start_floor) // bucket_interval_ms) + 1
    bucket_starts = np.arange(bucket_count, dtype=np.int64) * int(bucket_interval_ms) + int(bucket_start_floor)
    bucket_ends = bucket_starts + int(bucket_interval_ms)
    bucket_frame = pl.DataFrame(
        {
            "market": [market] * int(bucket_count),
            "bucket_start_ts_ms": bucket_starts,
            "bucket_end_ts_ms": bucket_ends,
            "bucket_date_utc": [date_value] * int(bucket_count),
        }
    )

    trade_bucket_features = _build_trade_bucket_features(
        trade_frame=trade_frame,
        bucket_starts=bucket_starts,
        bucket_ends=bucket_ends,
        bucket_interval_ms=bucket_interval_ms,
    )
    bucket_frame = bucket_frame.join(trade_bucket_features, on=["bucket_start_ts_ms", "bucket_end_ts_ms"], how="left")

    if ticker_frame.height > 0:
        bucket_frame = bucket_frame.join_asof(
            ticker_frame.sort("ticker_ts_ms"),
            left_on="bucket_end_ts_ms",
            right_on="ticker_ts_ms",
            strategy="backward",
        )
    else:
        bucket_frame = bucket_frame.with_columns(
            [
                pl.lit(None, dtype=pl.Int64).alias("ticker_ts_ms"),
                pl.lit(None, dtype=pl.Float64).alias("last_price"),
                pl.lit(None, dtype=pl.Float64).alias("acc_trade_price_24h"),
                pl.lit(None, dtype=pl.Utf8).alias("market_state"),
                pl.lit(None, dtype=pl.Utf8).alias("market_warning"),
            ]
        )
    bucket_frame = bucket_frame.with_columns(
        (pl.col("bucket_end_ts_ms") - pl.col("ticker_ts_ms")).cast(pl.Int64).alias("ticker_age_ms")
    )

    if orderbook_frame.height > 0:
        bucket_frame = bucket_frame.join_asof(
            orderbook_frame.sort("book_ts_ms"),
            left_on="bucket_end_ts_ms",
            right_on="book_ts_ms",
            strategy="backward",
        )
    else:
        bucket_frame = bucket_frame.with_columns(
            [
                pl.lit(None, dtype=pl.Int64).alias("book_ts_ms"),
                pl.lit(None, dtype=pl.Float64).alias("best_bid"),
                pl.lit(None, dtype=pl.Float64).alias("best_ask"),
                pl.lit(None, dtype=pl.Float64).alias("spread_bps"),
                pl.lit(None, dtype=pl.Float64).alias("bid_depth_top1_krw"),
                pl.lit(None, dtype=pl.Float64).alias("ask_depth_top1_krw"),
                pl.lit(None, dtype=pl.Float64).alias("bid_depth_top5_krw"),
                pl.lit(None, dtype=pl.Float64).alias("ask_depth_top5_krw"),
                pl.lit(None, dtype=pl.Float64).alias("queue_imbalance_top1"),
                pl.lit(None, dtype=pl.Float64).alias("queue_imbalance_top5"),
                pl.lit(None, dtype=pl.Float64).alias("microprice"),
                pl.lit(None, dtype=pl.Float64).alias("microprice_bias_bps"),
                pl.lit(None, dtype=pl.Int64).alias("book_update_count_5s"),
            ]
        )

    candles_1m = _load_candle_tf_frame(
        candles_root=options.candles_root,
        market=market,
        tf="1m",
        start_ts_ms=max(int(date_start_ts_ms) - ONE_DAY_MS - (15 * ONE_MIN_MS), 0),
        end_ts_ms=int(date_end_ts_ms),
        cache=candle_cache,
    )
    candles_5m = _load_candle_tf_frame(
        candles_root=options.candles_root,
        market=market,
        tf="5m",
        start_ts_ms=max(int(date_start_ts_ms) - ONE_DAY_MS, 0),
        end_ts_ms=int(date_end_ts_ms),
        cache=candle_cache,
    )
    candles_15m = _load_candle_tf_frame(
        candles_root=options.candles_root,
        market=market,
        tf="15m",
        start_ts_ms=max(int(date_start_ts_ms) - ONE_DAY_MS, 0),
        end_ts_ms=int(date_end_ts_ms),
        cache=candle_cache,
    )
    candles_60m = _load_candle_tf_frame(
        candles_root=options.candles_root,
        market=market,
        tf="60m",
        start_ts_ms=max(int(date_start_ts_ms) - ONE_DAY_MS, 0),
        end_ts_ms=int(date_end_ts_ms),
        cache=candle_cache,
    )
    bucket_frame = _attach_candle_context(
        bucket_frame=bucket_frame,
        candles_1m=candles_1m,
        candles_5m=candles_5m,
        candles_15m=candles_15m,
        candles_60m=candles_60m,
    )

    bucket_frame = bucket_frame.with_columns(
        [
            pl.lit(int(market_cap_rank)).cast(pl.Int64).alias("market_cap_rank_fixed30"),
            pl.lit(None, dtype=pl.Float64).alias("btc_rel_strength_5m"),
            pl.lit(None, dtype=pl.Float64).alias("eth_rel_strength_5m"),
            pl.lit(None, dtype=pl.Float64).alias("universe_breadth_up_ratio"),
            pl.lit(None, dtype=pl.Float64).alias("universe_notional_rank_pct"),
        ]
    ).with_columns(
        [
            ((pl.col("ticker_ts_ms").is_not_null()) & (pl.col("ticker_age_ms") >= 0) & (pl.col("ticker_age_ms") <= int(bucket_interval_ms))).alias("ticker_available"),
            (pl.col("trade_events_60s") > 0).alias("trade_available"),
            ((pl.col("book_ts_ms").is_not_null()) & ((pl.col("bucket_end_ts_ms") - pl.col("book_ts_ms")) >= 0) & ((pl.col("bucket_end_ts_ms") - pl.col("book_ts_ms")) <= int(bucket_interval_ms))).alias("book_available"),
            (
                pl.col("ret_1m").is_not_null()
                & pl.col("ret_5m").is_not_null()
                & pl.col("ret_15m").is_not_null()
                & pl.col("ret_60m").is_not_null()
                & pl.col("rv_1m_5m_window").is_not_null()
                & pl.col("rv_1m_15m_window").is_not_null()
                & pl.col("atr_pct_14").is_not_null()
                & pl.col("distance_from_15m_high_low").is_not_null()
            ).alias("candle_context_available"),
        ]
    ).with_columns(
        (
            pl.col("ticker_available").cast(pl.Float64)
            + pl.col("trade_available").cast(pl.Float64)
            + pl.col("book_available").cast(pl.Float64)
            + pl.col("candle_context_available").cast(pl.Float64)
        ).truediv(4.0).alias("source_quality_score")
    )

    bucket_frame = _align_schema(bucket_frame, schema=MARKET_STATE_SCHEMA)
    tradeable_labels, net_edge_labels = _build_labels(
        state_frame=bucket_frame,
        join_extra_cost_bps=float(options.join_extra_cost_bps),
        fee_bps=float(options.fee_bps),
        depth_haircut=float(options.depth_haircut),
        no_trade_threshold_bps=float(options.no_trade_threshold_bps),
        max_join_spread_bps=float(options.max_join_spread_bps),
    )
    return (
        bucket_frame,
        tradeable_labels,
        net_edge_labels,
        {
            "date": date_value,
            "market": market,
            "status": "OK",
            "rows": int(bucket_frame.height),
            "ticker_rows": int(ticker_frame.height),
            "orderbook_rows": int(orderbook_frame.height),
            "trade_rows": int(trade_frame.height),
            "bucket_start_ts_ms": int(bucket_frame.get_column("bucket_start_ts_ms").min()) if bucket_frame.height > 0 else None,
            "bucket_end_ts_ms": int(bucket_frame.get_column("bucket_end_ts_ms").max()) if bucket_frame.height > 0 else None,
        },
    )


def _build_trade_bucket_features(
    *,
    trade_frame: pl.DataFrame,
    bucket_starts: np.ndarray,
    bucket_ends: np.ndarray,
    bucket_interval_ms: int,
) -> pl.DataFrame:
    base = pl.DataFrame({"bucket_start_ts_ms": bucket_starts, "bucket_end_ts_ms": bucket_ends})
    if trade_frame.height <= 0:
        return base.with_columns(
            [
                pl.lit(0, dtype=pl.Int64).alias("trade_events_5s"),
                pl.lit(0, dtype=pl.Int64).alias("trade_events_15s"),
                pl.lit(0, dtype=pl.Int64).alias("trade_events_60s"),
                pl.lit(0.0, dtype=pl.Float64).alias("trade_notional_5s"),
                pl.lit(0.0, dtype=pl.Float64).alias("trade_notional_60s"),
                pl.lit(0.0, dtype=pl.Float64).alias("buy_volume_5s"),
                pl.lit(0.0, dtype=pl.Float64).alias("sell_volume_5s"),
                pl.lit(0.0, dtype=pl.Float64).alias("signed_volume_5s"),
                pl.lit(0.0, dtype=pl.Float64).alias("trade_imbalance_5s"),
                pl.lit(np.nan, dtype=pl.Float64).alias("vwap_5s"),
                pl.lit(0.0, dtype=pl.Float64).alias("large_trade_ratio_60s"),
            ]
        )

    frame = trade_frame.with_columns(
        [
            (((pl.col("event_ts_ms") / int(bucket_interval_ms)).floor()) * int(bucket_interval_ms)).cast(pl.Int64).alias("bucket_start_ts_ms"),
            (pl.col("price") * pl.col("volume")).cast(pl.Float64).alias("notional_krw"),
            (pl.when(pl.col("side") == "buy").then(pl.col("volume")).otherwise(0.0)).cast(pl.Float64).alias("buy_volume"),
            (pl.when(pl.col("side") == "sell").then(pl.col("volume")).otherwise(0.0)).cast(pl.Float64).alias("sell_volume"),
        ]
    )
    aggregated = (
        frame.group_by("bucket_start_ts_ms")
        .agg(
            [
                pl.len().cast(pl.Int64).alias("trade_events_5s"),
                pl.col("notional_krw").sum().cast(pl.Float64).alias("trade_notional_5s"),
                pl.col("buy_volume").sum().cast(pl.Float64).alias("buy_volume_5s"),
                pl.col("sell_volume").sum().cast(pl.Float64).alias("sell_volume_5s"),
                pl.col("volume").sum().cast(pl.Float64).alias("trade_volume_5s"),
                ((pl.col("notional_krw").sum()) / pl.col("volume").sum()).cast(pl.Float64).alias("vwap_5s"),
            ]
        )
        .sort("bucket_start_ts_ms")
    )
    merged = base.join(aggregated, on="bucket_start_ts_ms", how="left").with_columns(
        [
            pl.col("trade_events_5s").fill_null(0).cast(pl.Int64),
            pl.col("trade_notional_5s").fill_null(0.0).cast(pl.Float64),
            pl.col("buy_volume_5s").fill_null(0.0).cast(pl.Float64),
            pl.col("sell_volume_5s").fill_null(0.0).cast(pl.Float64),
            pl.col("trade_volume_5s").fill_null(0.0).cast(pl.Float64),
        ]
    ).with_columns(
        [
            (pl.col("buy_volume_5s") - pl.col("sell_volume_5s")).alias("signed_volume_5s"),
            (
                (pl.col("buy_volume_5s") - pl.col("sell_volume_5s"))
                / pl.max_horizontal(pl.col("trade_volume_5s"), pl.lit(1e-12))
            ).alias("trade_imbalance_5s"),
            pl.col("trade_events_5s").rolling_sum(window_size=3, min_samples=1).alias("trade_events_15s"),
            pl.col("trade_events_5s").rolling_sum(window_size=12, min_samples=1).alias("trade_events_60s"),
            pl.col("trade_notional_5s").rolling_sum(window_size=12, min_samples=1).alias("trade_notional_60s"),
        ]
    )
    large_trade_ratio = _compute_large_trade_ratio_60s(
        trade_ts_ms=trade_frame.get_column("event_ts_ms").to_numpy().astype(np.int64, copy=False),
        trade_notional=np.asarray((trade_frame.get_column("price") * trade_frame.get_column("volume")).to_numpy(), dtype=np.float64),
        bucket_ends=bucket_ends,
    )
    return merged.with_columns(pl.Series(name="large_trade_ratio_60s", values=large_trade_ratio).cast(pl.Float64)).drop("trade_volume_5s")


def _compute_large_trade_ratio_60s(
    *,
    trade_ts_ms: np.ndarray,
    trade_notional: np.ndarray,
    bucket_ends: np.ndarray,
) -> np.ndarray:
    ts_values = np.asarray(trade_ts_ms, dtype=np.int64)
    notionals = np.asarray(trade_notional, dtype=np.float64)
    result = np.zeros(bucket_ends.shape[0], dtype=np.float64)
    left = 0
    right = 0
    for idx, bucket_end in enumerate(np.asarray(bucket_ends, dtype=np.int64)):
        window_start = int(bucket_end) - 60_000
        while left < ts_values.size and int(ts_values[left]) <= int(window_start):
            left += 1
        while right < ts_values.size and int(ts_values[right]) <= int(bucket_end):
            right += 1
        if right <= left:
            continue
        window = notionals[left:right]
        total = float(np.sum(window))
        if total <= 0.0:
            continue
        threshold = float(np.quantile(window, 0.90))
        large_total = float(np.sum(window[window >= threshold]))
        result[idx] = large_total / total if total > 0.0 else 0.0
    return result


def _attach_candle_context(
    *,
    bucket_frame: pl.DataFrame,
    candles_1m: pl.DataFrame,
    candles_5m: pl.DataFrame,
    candles_15m: pl.DataFrame,
    candles_60m: pl.DataFrame,
) -> pl.DataFrame:
    current = bucket_frame
    one_m_features = _prepare_1m_context_frame(candles_1m)
    five_m_features = _prepare_tf_return_frame(candles_5m, prefix="5m")
    fifteen_m_features = _prepare_tf_return_frame(candles_15m, prefix="15m")
    sixty_m_features = _prepare_tf_return_frame(candles_60m, prefix="60m")

    if one_m_features.height > 0:
        current = current.join_asof(
            one_m_features.rename({"ts_ms": "ts_ms_1m"}).sort("ts_ms_1m"),
            left_on="bucket_end_ts_ms",
            right_on="ts_ms_1m",
            strategy="backward",
        )
    else:
        current = current.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("ret_1m"),
                pl.lit(None, dtype=pl.Float64).alias("rv_1m_5m_window"),
                pl.lit(None, dtype=pl.Float64).alias("rv_1m_15m_window"),
                pl.lit(None, dtype=pl.Float64).alias("atr_pct_14"),
                pl.lit(None, dtype=pl.Float64).alias("distance_from_15m_high_low"),
                pl.lit(None, dtype=pl.Float64).alias("close_1m"),
            ]
        )
    for frame, prefix in ((five_m_features, "5m"), (fifteen_m_features, "15m"), (sixty_m_features, "60m")):
        if frame.height > 0:
            key_name = f"ts_ms_{prefix}"
            current = current.join_asof(
                frame.rename({"ts_ms": key_name}).sort(key_name),
                left_on="bucket_end_ts_ms",
                right_on=key_name,
                strategy="backward",
            )
        else:
            current = current.with_columns(pl.lit(None, dtype=pl.Float64).alias(f"ret_{prefix}"))

    if one_m_features.height > 0:
        close_24h_frame = one_m_features.select([pl.col("ts_ms").alias("ts_ms_24h_source"), pl.col("close_1m").alias("close_24h_ago")]).sort("ts_ms_24h_source")
        current = current.with_columns((pl.col("bucket_end_ts_ms") - pl.lit(ONE_DAY_MS, dtype=pl.Int64)).alias("__lookup_24h_ts_ms")).join_asof(
            close_24h_frame,
            left_on="__lookup_24h_ts_ms",
            right_on="ts_ms_24h_source",
            strategy="backward",
        ).drop("__lookup_24h_ts_ms")
        current = current.with_columns(
            (
                pl.when(pl.col("close_24h_ago").is_not_null() & (pl.col("close_24h_ago") > 0) & pl.col("last_price").is_not_null())
                .then((pl.col("last_price") / pl.col("close_24h_ago")) - 1.0)
                .otherwise(None)
            ).cast(pl.Float64).alias("signed_change_rate")
        ).drop("close_24h_ago", "ts_ms_24h_source")
    else:
        current = current.with_columns(pl.lit(None, dtype=pl.Float64).alias("signed_change_rate"))
    return current.drop(
        [
            name
            for name in ("ts_ms_1m", "ts_ms_5m", "ts_ms_15m", "ts_ms_60m", "close_1m")
            if name in current.columns
        ]
    )


def _prepare_1m_context_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return pl.DataFrame()
    result = frame.sort("ts_ms").with_columns(
        [
            (pl.col("close") / pl.col("close").shift(1) - 1.0).alias("ret_1m"),
            (pl.col("close").log() - pl.col("close").shift(1).log()).alias("__log_ret"),
            pl.col("close").alias("close_1m"),
            pl.col("close").rolling_max(window_size=15, min_samples=1).alias("__high_15m"),
            pl.col("close").rolling_min(window_size=15, min_samples=1).alias("__low_15m"),
            pl.max_horizontal(
                [
                    (pl.col("high") - pl.col("low")),
                    (pl.col("high") - pl.col("close").shift(1)).abs(),
                    (pl.col("low") - pl.col("close").shift(1)).abs(),
                ]
            ).alias("__true_range"),
        ]
    ).with_columns(
        [
            (pl.col("__log_ret") * pl.col("__log_ret")).rolling_sum(window_size=5, min_samples=1).alias("rv_1m_5m_window"),
            (pl.col("__log_ret") * pl.col("__log_ret")).rolling_sum(window_size=15, min_samples=1).alias("rv_1m_15m_window"),
            (pl.col("__true_range").rolling_mean(window_size=14, min_samples=14) / pl.col("close")).alias("atr_pct_14"),
            (
                pl.when((pl.col("__high_15m") - pl.col("__low_15m")).abs() > 1e-12)
                .then((pl.col("close") - pl.col("__low_15m")) / (pl.col("__high_15m") - pl.col("__low_15m")))
                .otherwise(0.5)
            ).cast(pl.Float64).alias("distance_from_15m_high_low"),
        ]
    )
    return result.select(["ts_ms", "ret_1m", "rv_1m_5m_window", "rv_1m_15m_window", "atr_pct_14", "distance_from_15m_high_low", "close_1m"])


def _prepare_tf_return_frame(frame: pl.DataFrame, *, prefix: str) -> pl.DataFrame:
    if frame.height <= 0:
        return pl.DataFrame()
    return frame.sort("ts_ms").with_columns(
        (pl.col("close") / pl.col("close").shift(1) - 1.0).cast(pl.Float64).alias(f"ret_{prefix}")
    ).select(["ts_ms", f"ret_{prefix}"])


def _attach_universe_context(per_market_frames: dict[str, pl.DataFrame]) -> None:
    combined = pl.concat(list(per_market_frames.values()), how="vertical").sort(["bucket_end_ts_ms", "market"])
    breadth = (
        combined.group_by("bucket_end_ts_ms")
        .agg((pl.col("ret_5m") > 0.0).cast(pl.Float64).mean().alias("universe_breadth_up_ratio"))
        .sort("bucket_end_ts_ms")
    )
    ranked_rows: list[pl.DataFrame] = []
    for _bucket_end, group in combined.group_by("bucket_end_ts_ms", maintain_order=True):
        group_sorted = group.sort("trade_notional_60s", descending=True).with_row_index("__rank")
        denom = max(int(group_sorted.height) - 1, 1)
        ranked_rows.append(
            group_sorted.with_columns(
                (1.0 - (pl.col("__rank").cast(pl.Float64) / float(denom))).cast(pl.Float64).alias("universe_notional_rank_pct")
            ).drop("__rank")
        )
    ranked = pl.concat(ranked_rows, how="vertical").select(["market", "bucket_end_ts_ms", "universe_notional_rank_pct"])
    for market, frame in list(per_market_frames.items()):
        updated = frame.join(breadth, on="bucket_end_ts_ms", how="left").join(
            ranked.filter(pl.col("market") == market).drop("market"),
            on="bucket_end_ts_ms",
            how="left",
        )
        per_market_frames[market] = updated


def _attach_leader_relative_strength(
    per_market_frames: dict[str, pl.DataFrame],
    *,
    leader_market: str,
    target_column: str,
) -> None:
    leader = per_market_frames.get(leader_market)
    if leader is None or leader.height <= 0:
        return
    leader_key = leader.select(["bucket_end_ts_ms", pl.col("ret_5m").alias(f"__{target_column}_leader")])
    for market, frame in list(per_market_frames.items()):
        updated = frame.join(leader_key, on="bucket_end_ts_ms", how="left").with_columns(
            (
                pl.when(pl.col("ret_5m").is_not_null() & pl.col(f"__{target_column}_leader").is_not_null())
                .then(pl.col("ret_5m") - pl.col(f"__{target_column}_leader"))
                .otherwise(None)
            ).cast(pl.Float64).alias(target_column)
        ).drop(f"__{target_column}_leader")
        if market == leader_market:
            updated = updated.with_columns(pl.lit(0.0, dtype=pl.Float64).alias(target_column))
        per_market_frames[market] = updated


def _build_labels(
    *,
    state_frame: pl.DataFrame,
    join_extra_cost_bps: float,
    fee_bps: float,
    depth_haircut: float,
    no_trade_threshold_bps: float,
    max_join_spread_bps: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    frame = state_frame.sort("bucket_end_ts_ms")
    total_cost_bps = (float(fee_bps) * 2.0) + float(join_extra_cost_bps)
    label_frame = frame.select(
        [
            "market",
            "bucket_start_ts_ms",
            "bucket_end_ts_ms",
            "bucket_date_utc",
            "best_ask",
            "ask_depth_top5_krw",
            "spread_bps",
            "best_bid",
            "bid_depth_top5_krw",
            "ticker_available",
            "trade_available",
            "book_available",
            "candle_context_available",
            "source_quality_score",
        ]
    ).rename(
        {
            "best_ask": "entry_best_ask",
            "ask_depth_top5_krw": "entry_best_ask_depth_top5_krw",
            "spread_bps": "entry_spread_bps",
        }
    )
    for horizon_minutes in ALL_HORIZON_MINUTES:
        bucket_shift = int((horizon_minutes * 60_000) // BUCKET_INTERVAL_MS)
        label_frame = label_frame.with_columns(
            [
                pl.col("best_bid").shift(-bucket_shift).alias(f"future_best_bid_{horizon_minutes}m"),
                pl.col("bid_depth_top5_krw").shift(-bucket_shift).alias(f"future_bid_depth_top5_krw_{horizon_minutes}m"),
            ]
        ).with_columns(
            [
                (
                    pl.when(pl.col("entry_best_ask").is_not_null() & (pl.col("entry_best_ask") > 0) & pl.col(f"future_best_bid_{horizon_minutes}m").is_not_null() & (pl.col(f"future_best_bid_{horizon_minutes}m") > 0))
                    .then(((pl.col(f"future_best_bid_{horizon_minutes}m") / pl.col("entry_best_ask")) - 1.0) * 10_000.0)
                    .otherwise(None)
                ).cast(pl.Float64).alias(f"gross_return_{horizon_minutes}m_bps"),
                (
                    pl.when(pl.col("entry_best_ask").is_not_null() & (pl.col("entry_best_ask") > 0) & pl.col(f"future_best_bid_{horizon_minutes}m").is_not_null() & (pl.col(f"future_best_bid_{horizon_minutes}m") > 0))
                    .then((((pl.col(f"future_best_bid_{horizon_minutes}m") / pl.col("entry_best_ask")) - 1.0) * 10_000.0) - float(total_cost_bps))
                    .otherwise(None)
                ).cast(pl.Float64).alias(f"net_edge_{horizon_minutes}m_bps"),
            ]
        )

    tradeable_labels = label_frame.with_columns(
        [
            (
                pl.col("entry_best_ask").is_not_null()
                & pl.col("future_best_bid_20m").is_not_null()
                & pl.col("book_available")
                & pl.col("ticker_available")
                & pl.col("trade_available")
                & pl.col("candle_context_available")
            ).alias("label_available_20m"),
            (
                pl.col("entry_spread_bps").is_not_null()
                & (pl.col("entry_spread_bps") <= float(max_join_spread_bps))
            ).alias("spread_quality_pass_20m"),
            (
                (pl.col("entry_best_ask_depth_top5_krw").fill_null(0.0) * float(depth_haircut) > 0.0)
                & (pl.col("future_bid_depth_top5_krw_20m").fill_null(0.0) * float(depth_haircut) > 0.0)
            ).alias("liquidity_pass_20m"),
            (
                pl.col("ticker_available")
                & pl.col("trade_available")
                & pl.col("book_available")
                & pl.col("candle_context_available")
            ).alias("structure_pass_20m"),
        ]
    ).with_columns(
        (
            pl.when(
                pl.col("label_available_20m")
                & pl.col("spread_quality_pass_20m")
                & pl.col("liquidity_pass_20m")
                & pl.col("structure_pass_20m")
                & (pl.col("net_edge_20m_bps") > float(no_trade_threshold_bps))
            )
            .then(1)
            .when(pl.col("label_available_20m"))
            .then(0)
            .otherwise(None)
        ).cast(pl.Int8).alias("tradeable_20m")
    ).select(list(TRADEABLE_LABEL_SCHEMA.keys()))

    net_edge_labels = label_frame.select(list(NET_EDGE_LABEL_SCHEMA.keys()))
    return (_align_schema(tradeable_labels, schema=TRADEABLE_LABEL_SCHEMA), _align_schema(net_edge_labels, schema=NET_EDGE_LABEL_SCHEMA))


def _load_date_ticker_frames(
    *,
    raw_ws_root: Path,
    date_value: str,
    selected_markets: tuple[str, ...],
) -> dict[str, pl.DataFrame]:
    selected = {str(market).strip().upper() for market in selected_markets if str(market).strip()}
    rows_by_market: dict[str, list[dict[str, Any]]] = {market: [] for market in selected}
    for path in sorted((Path(raw_ws_root) / "ticker" / f"date={date_value}").glob("hour=*/*.jsonl.zst")):
        if not path.is_file():
            continue
        for row in iter_jsonl_zst_rows(path):
            if str(row.get("channel", "")).strip().lower() != "ticker":
                continue
            market = str(row.get("market", "")).strip().upper()
            if market not in selected:
                continue
            ts_ms = _to_int(row.get("ts_ms"))
            price = _to_float(row.get("trade_price"))
            acc_trade_price_24h = _to_float(row.get("acc_trade_price_24h"))
            if ts_ms is None or price is None or acc_trade_price_24h is None:
                continue
            rows_by_market.setdefault(market, []).append(
                {
                    "ticker_ts_ms": int(ts_ms),
                    "last_price": float(price),
                    "acc_trade_price_24h": float(acc_trade_price_24h),
                    "market_state": str(row.get("market_state") or "").strip().upper() or None,
                    "market_warning": str(row.get("market_warning") or "").strip().upper() or None,
                }
            )
    return {
        market: (
            pl.DataFrame(rows).sort("ticker_ts_ms").unique(subset=["ticker_ts_ms"], keep="last", maintain_order=True)
            if rows
            else _empty_ticker_frame()
        )
        for market, rows in rows_by_market.items()
    }


def _load_date_orderbook_frames(
    *,
    raw_ws_root: Path,
    date_value: str,
    selected_markets: tuple[str, ...],
) -> dict[str, pl.DataFrame]:
    selected = {str(market).strip().upper() for market in selected_markets if str(market).strip()}
    rows_by_market: dict[str, list[dict[str, Any]]] = {market: [] for market in selected}
    for path in sorted((Path(raw_ws_root) / "orderbook" / f"date={date_value}").glob("hour=*/*.jsonl.zst")):
        if not path.is_file():
            continue
        for row in iter_jsonl_zst_rows(path):
            if str(row.get("channel", "")).strip().lower() != "orderbook":
                continue
            market = str(row.get("market", "")).strip().upper()
            if market not in selected:
                continue
            ts_ms = _to_int(row.get("ts_ms"))
            bid1_price = _to_float(row.get("bid1_price"))
            ask1_price = _to_float(row.get("ask1_price"))
            bid1_size = _to_float(row.get("bid1_size")) or 0.0
            ask1_size = _to_float(row.get("ask1_size")) or 0.0
            if ts_ms is None or bid1_price is None or ask1_price is None:
                continue
            if bid1_price <= 0.0 or ask1_price <= 0.0 or ask1_price < bid1_price:
                continue
            bid_depth_top5_krw = 0.0
            ask_depth_top5_krw = 0.0
            bid_size_top5 = 0.0
            ask_size_top5 = 0.0
            for level in range(1, 6):
                level_bid_price = _to_float(row.get(f"bid{level}_price")) or 0.0
                level_ask_price = _to_float(row.get(f"ask{level}_price")) or 0.0
                level_bid_size = _to_float(row.get(f"bid{level}_size")) or 0.0
                level_ask_size = _to_float(row.get(f"ask{level}_size")) or 0.0
                bid_depth_top5_krw += max(level_bid_price * level_bid_size, 0.0)
                ask_depth_top5_krw += max(level_ask_price * level_ask_size, 0.0)
                bid_size_top5 += max(level_bid_size, 0.0)
                ask_size_top5 += max(level_ask_size, 0.0)
            mid = (float(bid1_price) + float(ask1_price)) / 2.0
            if mid <= 0.0:
                continue
            spread_bps = ((float(ask1_price) - float(bid1_price)) / mid) * 10_000.0
            queue_imbalance_top1 = _safe_ratio(float(bid1_size) - float(ask1_size), float(bid1_size) + float(ask1_size))
            queue_imbalance_top5 = _safe_ratio(float(bid_size_top5) - float(ask_size_top5), float(bid_size_top5) + float(ask_size_top5))
            microprice = None
            microprice_bias_bps = None
            if (float(bid1_size) + float(ask1_size)) > 0.0:
                microprice = ((float(ask1_price) * float(bid1_size)) + (float(bid1_price) * float(ask1_size))) / (float(bid1_size) + float(ask1_size))
                microprice_bias_bps = ((float(microprice) - float(mid)) / float(mid)) * 10_000.0
            rows_by_market.setdefault(market, []).append(
                {
                    "book_ts_ms": int(ts_ms),
                    "best_bid": float(bid1_price),
                    "best_ask": float(ask1_price),
                    "spread_bps": float(spread_bps),
                    "bid_depth_top1_krw": float(bid1_price) * float(bid1_size),
                    "ask_depth_top1_krw": float(ask1_price) * float(ask1_size),
                    "bid_depth_top5_krw": float(bid_depth_top5_krw),
                    "ask_depth_top5_krw": float(ask_depth_top5_krw),
                    "queue_imbalance_top1": float(queue_imbalance_top1),
                    "queue_imbalance_top5": float(queue_imbalance_top5),
                    "microprice": float(microprice) if microprice is not None else None,
                    "microprice_bias_bps": float(microprice_bias_bps) if microprice_bias_bps is not None else None,
                }
            )

    result: dict[str, pl.DataFrame] = {}
    for market in selected_markets:
        rows = rows_by_market.get(market, [])
        if not rows:
            result[market] = _empty_orderbook_frame()
            continue
        frame = pl.DataFrame(rows).sort("book_ts_ms").unique(subset=["book_ts_ms"], keep="last", maintain_order=True)
        bucket_events = frame.with_columns(
            (((pl.col("book_ts_ms") / int(BUCKET_INTERVAL_MS)).floor()) * int(BUCKET_INTERVAL_MS)).cast(pl.Int64).alias("bucket_start_ts_ms")
        ).group_by("bucket_start_ts_ms").agg(pl.len().cast(pl.Int64).alias("book_update_count_5s"))
        result[market] = frame.with_columns(
            (((pl.col("book_ts_ms") / int(BUCKET_INTERVAL_MS)).floor()) * int(BUCKET_INTERVAL_MS)).cast(pl.Int64).alias("bucket_start_ts_ms")
        ).join(bucket_events, on="bucket_start_ts_ms", how="left").drop("bucket_start_ts_ms")
    return result


def _load_date_trade_frames(
    *,
    raw_trade_root: Path,
    date_value: str,
    selected_markets: tuple[str, ...],
) -> dict[str, pl.DataFrame]:
    result: dict[str, pl.DataFrame] = {}
    for market in selected_markets:
        result[market] = _load_market_trade_frame(raw_trade_root=raw_trade_root, date_value=date_value, market=market)
    return result


def _load_market_ticker_frame(*, raw_ws_root: Path, date_value: str, market: str) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted((Path(raw_ws_root) / "ticker" / f"date={date_value}").glob("hour=*/*.jsonl.zst")):
        if not path.is_file():
            continue
        for row in iter_jsonl_zst_rows(path):
            if str(row.get("channel", "")).strip().lower() != "ticker":
                continue
            if str(row.get("market", "")).strip().upper() != market:
                continue
            ts_ms = _to_int(row.get("ts_ms"))
            price = _to_float(row.get("trade_price"))
            acc_trade_price_24h = _to_float(row.get("acc_trade_price_24h"))
            if ts_ms is None or price is None or acc_trade_price_24h is None:
                continue
            rows.append(
                {
                    "ticker_ts_ms": int(ts_ms),
                    "last_price": float(price),
                    "acc_trade_price_24h": float(acc_trade_price_24h),
                    "market_state": str(row.get("market_state") or "").strip().upper() or None,
                    "market_warning": str(row.get("market_warning") or "").strip().upper() or None,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "ticker_ts_ms": pl.Int64,
                "last_price": pl.Float64,
                "acc_trade_price_24h": pl.Float64,
                "market_state": pl.Utf8,
                "market_warning": pl.Utf8,
            }
        )
    return pl.DataFrame(rows).sort("ticker_ts_ms").unique(subset=["ticker_ts_ms"], keep="last", maintain_order=True)


def _load_market_orderbook_frame(*, raw_ws_root: Path, date_value: str, market: str) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted((Path(raw_ws_root) / "orderbook" / f"date={date_value}").glob("hour=*/*.jsonl.zst")):
        if not path.is_file():
            continue
        for row in iter_jsonl_zst_rows(path):
            if str(row.get("channel", "")).strip().lower() != "orderbook":
                continue
            if str(row.get("market", "")).strip().upper() != market:
                continue
            ts_ms = _to_int(row.get("ts_ms"))
            bid1_price = _to_float(row.get("bid1_price"))
            ask1_price = _to_float(row.get("ask1_price"))
            bid1_size = _to_float(row.get("bid1_size")) or 0.0
            ask1_size = _to_float(row.get("ask1_size")) or 0.0
            if ts_ms is None or bid1_price is None or ask1_price is None:
                continue
            if bid1_price <= 0.0 or ask1_price <= 0.0 or ask1_price < bid1_price:
                continue
            bid_depth_top5_krw = 0.0
            ask_depth_top5_krw = 0.0
            bid_size_top5 = 0.0
            ask_size_top5 = 0.0
            for level in range(1, 6):
                level_bid_price = _to_float(row.get(f"bid{level}_price")) or 0.0
                level_ask_price = _to_float(row.get(f"ask{level}_price")) or 0.0
                level_bid_size = _to_float(row.get(f"bid{level}_size")) or 0.0
                level_ask_size = _to_float(row.get(f"ask{level}_size")) or 0.0
                bid_depth_top5_krw += max(level_bid_price * level_bid_size, 0.0)
                ask_depth_top5_krw += max(level_ask_price * level_ask_size, 0.0)
                bid_size_top5 += max(level_bid_size, 0.0)
                ask_size_top5 += max(level_ask_size, 0.0)
            mid = (float(bid1_price) + float(ask1_price)) / 2.0
            if mid <= 0.0:
                continue
            spread_bps = ((float(ask1_price) - float(bid1_price)) / mid) * 10_000.0
            queue_imbalance_top1 = _safe_ratio(float(bid1_size) - float(ask1_size), float(bid1_size) + float(ask1_size))
            queue_imbalance_top5 = _safe_ratio(float(bid_size_top5) - float(ask_size_top5), float(bid_size_top5) + float(ask_size_top5))
            microprice = None
            microprice_bias_bps = None
            if (float(bid1_size) + float(ask1_size)) > 0.0:
                microprice = ((float(ask1_price) * float(bid1_size)) + (float(bid1_price) * float(ask1_size))) / (float(bid1_size) + float(ask1_size))
                microprice_bias_bps = ((float(microprice) - float(mid)) / float(mid)) * 10_000.0
            rows.append(
                {
                    "book_ts_ms": int(ts_ms),
                    "best_bid": float(bid1_price),
                    "best_ask": float(ask1_price),
                    "spread_bps": float(spread_bps),
                    "bid_depth_top1_krw": float(bid1_price) * float(bid1_size),
                    "ask_depth_top1_krw": float(ask1_price) * float(ask1_size),
                    "bid_depth_top5_krw": float(bid_depth_top5_krw),
                    "ask_depth_top5_krw": float(ask_depth_top5_krw),
                    "queue_imbalance_top1": float(queue_imbalance_top1),
                    "queue_imbalance_top5": float(queue_imbalance_top5),
                    "microprice": float(microprice) if microprice is not None else None,
                    "microprice_bias_bps": float(microprice_bias_bps) if microprice_bias_bps is not None else None,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "book_ts_ms": pl.Int64,
                "best_bid": pl.Float64,
                "best_ask": pl.Float64,
                "spread_bps": pl.Float64,
                "bid_depth_top1_krw": pl.Float64,
                "ask_depth_top1_krw": pl.Float64,
                "bid_depth_top5_krw": pl.Float64,
                "ask_depth_top5_krw": pl.Float64,
                "queue_imbalance_top1": pl.Float64,
                "queue_imbalance_top5": pl.Float64,
                "microprice": pl.Float64,
                "microprice_bias_bps": pl.Float64,
            }
        )
    frame = pl.DataFrame(rows).sort("book_ts_ms").unique(subset=["book_ts_ms"], keep="last", maintain_order=True)
    bucket_events = frame.with_columns(
        (((pl.col("book_ts_ms") / int(BUCKET_INTERVAL_MS)).floor()) * int(BUCKET_INTERVAL_MS)).cast(pl.Int64).alias("bucket_start_ts_ms")
    ).group_by("bucket_start_ts_ms").agg(pl.len().cast(pl.Int64).alias("book_update_count_5s"))
    return frame.with_columns(
        (((pl.col("book_ts_ms") / int(BUCKET_INTERVAL_MS)).floor()) * int(BUCKET_INTERVAL_MS)).cast(pl.Int64).alias("bucket_start_ts_ms")
    ).join(bucket_events, on="bucket_start_ts_ms", how="left").drop("bucket_start_ts_ms")


def _load_market_trade_frame(*, raw_trade_root: Path, date_value: str, market: str) -> pl.DataFrame:
    market_dir = Path(raw_trade_root) / f"date={date_value}" / f"market={market}"
    if not market_dir.exists():
        return pl.DataFrame(schema={"event_ts_ms": pl.Int64, "price": pl.Float64, "volume": pl.Float64, "side": pl.Utf8})
    rows: list[dict[str, Any]] = []
    for path in sorted(market_dir.glob("*.jsonl.zst")):
        if not path.is_file():
            continue
        for row in read_raw_trade_part_file(path):
            event_ts_ms = _to_int(row.get("event_ts_ms"))
            price = _to_float(row.get("price"))
            volume = _to_float(row.get("volume"))
            side = str(row.get("side") or "").strip().lower()
            if event_ts_ms is None or price is None or volume is None or side not in {"buy", "sell"}:
                continue
            rows.append({"event_ts_ms": int(event_ts_ms), "price": float(price), "volume": float(volume), "side": side})
    if not rows:
        return pl.DataFrame(schema={"event_ts_ms": pl.Int64, "price": pl.Float64, "volume": pl.Float64, "side": pl.Utf8})
    return pl.DataFrame(rows).sort("event_ts_ms")


def _load_candle_tf_frame(
    *,
    candles_root: Path,
    market: str,
    tf: str,
    start_ts_ms: int,
    end_ts_ms: int,
    cache: dict[tuple[str, str, int, int], pl.DataFrame] | None = None,
) -> pl.DataFrame:
    cache_key = (str(market).strip().upper(), str(tf).strip().lower(), int(start_ts_ms), int(end_ts_ms))
    if cache is not None and cache_key in cache:
        return cache[cache_key]
    files = _market_candle_files(
        candles_root=candles_root,
        tf=tf,
        market=market,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    if not files:
        frame = pl.DataFrame(schema={"ts_ms": pl.Int64, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume_base": pl.Float64})
        if cache is not None:
            cache[cache_key] = frame
        return frame
    lazy = (
        pl.scan_parquet([str(path) for path in files])
        .select(
            [
                pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
                pl.col("open").cast(pl.Float64).alias("open"),
                pl.col("high").cast(pl.Float64).alias("high"),
                pl.col("low").cast(pl.Float64).alias("low"),
                pl.col("close").cast(pl.Float64).alias("close"),
                pl.col("volume_base").cast(pl.Float64).alias("volume_base"),
            ]
        )
        .filter((pl.col("ts_ms") >= int(start_ts_ms)) & (pl.col("ts_ms") <= int(end_ts_ms)))
        .sort("ts_ms")
        .unique(subset=["ts_ms"], keep="last", maintain_order=True)
    )
    frame = _collect_lazy(lazy)
    if cache is not None:
        cache[cache_key] = frame
    return frame


def _resolve_target_dates(*, start: str, end: str, closed_utc_dates_only: bool) -> tuple[str, ...]:
    requested = parse_date_range(start=start, end=end)
    if not closed_utc_dates_only:
        return requested
    today_utc = datetime.now(UTC).date().isoformat()
    max_closed = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    if start > max_closed:
        return ()
    return tuple(item for item in requested if item < today_utc)


def _resolve_selected_markets(*, config_dir: Path, quote: str, explicit_markets: tuple[str, ...]) -> tuple[str, ...]:
    if explicit_markets:
        return tuple(str(item).strip().upper() for item in explicit_markets if str(item).strip())
    return resolve_fixed_collection_markets(
        config_dir=Path(config_dir),
        quote=str(quote).strip().upper() or DEFAULT_QUOTE,
        explicit_markets=None,
    )


def _write_market_state_contracts(*, options: MarketStateBuildOptions, selected_markets: tuple[str, ...]) -> None:
    feature_contract = {
        "policy": "market_state_v1_feature_contract",
        "bucket_interval_ms": int(options.bucket_interval_ms),
        "quote": str(options.quote).strip().upper() or DEFAULT_QUOTE,
        "selected_markets": list(selected_markets),
        "feature_columns": list(MARKET_STATE_SCHEMA.keys()),
        "formulas": {
            "queue_imbalance": "(bid_size - ask_size) / max(bid_size + ask_size, eps)",
            "trade_imbalance": "signed_volume / max(total_volume, eps)",
            "microprice": "(ask * bid_size + bid * ask_size) / max(bid_size + ask_size, eps)",
            "microprice_bias_bps": "(microprice - mid) / mid * 10000",
            "realized_variance_window": "sum(log_return_i^2)",
        },
        "source_roots": {
            "raw_ws_root": str(options.raw_ws_root),
            "raw_trade_root": str(options.raw_trade_root),
            "candles_root": str(options.candles_root),
        },
    }
    label_contract = {
        "policy": "market_state_v1_label_contract",
        "primary_horizon_minutes": PRIMARY_HORIZON_MINUTES,
        "secondary_horizons_minutes": list(SECONDARY_HORIZON_MINUTES),
        "price_basis": "conservative_executable_join",
        "fee_bps": float(options.fee_bps),
        "join_extra_cost_bps": float(options.join_extra_cost_bps),
        "depth_haircut": float(options.depth_haircut),
        "no_trade_threshold_bps": float(options.no_trade_threshold_bps),
        "max_join_spread_bps": float(options.max_join_spread_bps),
        "position_direction": "long_only",
    }
    for root, payload, name in (
        (options.market_state_root, feature_contract, "feature_contract.json"),
        (options.tradeable_label_root, label_contract, "label_contract.json"),
        (options.net_edge_label_root, label_contract, "label_contract.json"),
    ):
        meta_dir = Path(root) / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        (meta_dir / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_market_date_parquet(
    *,
    root: Path,
    dataset_name: str,
    frame: pl.DataFrame,
    run_id: str,
    date_value: str,
    market: str,
) -> dict[str, Any]:
    target_dir = Path(root) / f"date={date_value}" / f"market={market}"
    target_dir.mkdir(parents=True, exist_ok=True)
    part_path = target_dir / f"part-{run_id}.parquet"
    frame.write_parquet(part_path, compression="zstd")
    return {
        "dataset_name": dataset_name,
        "rows": int(frame.height),
        "min_ts_ms": int(frame.get_column("bucket_end_ts_ms").min()) if frame.height > 0 else 0,
        "max_ts_ms": int(frame.get_column("bucket_end_ts_ms").max()) if frame.height > 0 else 0,
        "part_file": str(part_path),
    }


def _manifest_row(run_id: str, dataset_name: str, date_value: str, market: str, built_at_ms: int, part: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "dataset_name": dataset_name,
        "date": date_value,
        "market": market,
        "rows": int(part.get("rows", 0)),
        "min_ts_ms": int(part.get("min_ts_ms", 0)),
        "max_ts_ms": int(part.get("max_ts_ms", 0)),
        "part_file": str(part.get("part_file") or ""),
        "built_at_ms": int(built_at_ms),
    }


def _manifest_path(root: Path) -> Path:
    return Path(root) / "_meta" / "manifest.parquet"


def _append_manifest_rows(root: Path, rows: list[dict[str, Any]]) -> None:
    path = _manifest_path(root)
    if not rows:
        if not path.exists():
            _save_manifest(path, pl.DataFrame([], schema=DERIVED_MANIFEST_SCHEMA, orient="row"))
        return
    incoming = pl.DataFrame(rows, schema=DERIVED_MANIFEST_SCHEMA, orient="row")
    if path.exists():
        combined = pl.concat([_load_manifest(path), incoming], how="vertical")
    else:
        combined = incoming
    _save_manifest(path, combined)


def _replace_manifest_pairs(*, root: Path, rebuilt_pairs: list[tuple[str, str]]) -> None:
    path = _manifest_path(root)
    if not rebuilt_pairs or not path.exists():
        return
    rebuilt = {(str(date_value), str(market).upper()) for date_value, market in rebuilt_pairs}
    frame = _load_manifest(path)
    if frame.height <= 0:
        return
    filtered = frame.filter(
        ~pl.struct(["date", "market"]).map_elements(
            lambda item: (str(item["date"]), str(item["market"]).upper()) in rebuilt,
            return_dtype=pl.Boolean,
        )
    )
    _save_manifest(path, filtered)


def _load_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame([], schema=DERIVED_MANIFEST_SCHEMA, orient="row")
    frame = pl.read_parquet(path)
    return _align_schema(frame, schema=DERIVED_MANIFEST_SCHEMA)


def _save_manifest(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _align_schema(frame, schema=DERIVED_MANIFEST_SCHEMA).write_parquet(path, compression="zstd")


def _remove_existing_market_date_pair(*, root: Path, date_value: str, market: str) -> None:
    target = Path(root) / f"date={date_value}" / f"market={market}"
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)


def _market_candle_files(
    *,
    candles_root: Path,
    tf: str,
    market: str,
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
) -> list[Path]:
    market_dir = Path(candles_root) / f"tf={str(tf).strip().lower()}" / f"market={str(market).strip().upper()}"
    if not market_dir.exists():
        return []
    files = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if files:
        return files
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        date_label = date_dir.name.replace("date=", "", 1).strip()
        if start_ts_ms is not None or end_ts_ms is not None:
            date_start = _parse_date_to_ts_ms(date_label)
            date_end = _parse_date_to_ts_ms(date_label, end_of_day=True)
            if date_start is not None and date_end is not None:
                if start_ts_ms is not None and int(date_end) < int(start_ts_ms):
                    continue
                if end_ts_ms is not None and int(date_start) > int(end_ts_ms):
                    continue
        nested.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
    return nested


def _empty_ticker_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "ticker_ts_ms": pl.Int64,
            "last_price": pl.Float64,
            "acc_trade_price_24h": pl.Float64,
            "market_state": pl.Utf8,
            "market_warning": pl.Utf8,
        }
    )


def _empty_orderbook_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "book_ts_ms": pl.Int64,
            "best_bid": pl.Float64,
            "best_ask": pl.Float64,
            "spread_bps": pl.Float64,
            "bid_depth_top1_krw": pl.Float64,
            "ask_depth_top1_krw": pl.Float64,
            "bid_depth_top5_krw": pl.Float64,
            "ask_depth_top5_krw": pl.Float64,
            "queue_imbalance_top1": pl.Float64,
            "queue_imbalance_top5": pl.Float64,
            "microprice": pl.Float64,
            "microprice_bias_bps": pl.Float64,
            "book_update_count_5s": pl.Int64,
        }
    )


def _empty_trade_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={"event_ts_ms": pl.Int64, "price": pl.Float64, "volume": pl.Float64, "side": pl.Utf8})


def _align_schema(frame: pl.DataFrame, *, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if frame.height <= 0:
        return pl.DataFrame(schema=schema)
    expressions: list[pl.Expr] = []
    for name, dtype in schema.items():
        if name in frame.columns:
            expressions.append(pl.col(name).cast(dtype, strict=False).alias(name))
        else:
            expressions.append(pl.lit(None, dtype=dtype).alias(name))
    return frame.select(expressions)


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def _parse_date_to_ts_ms(value: str, *, end_of_day: bool = False) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text).replace(tzinfo=UTC)
    except ValueError:
        return None
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    return int(dt.timestamp() * 1000)


def _bucket_floor(ts_ms: int, interval_ms: int) -> int:
    value = int(ts_ms)
    interval = max(int(interval_ms), 1)
    return (value // interval) * interval


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(resolved):
        return None
    return resolved


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(float(denominator)) <= 1e-12:
        return 0.0
    return float(numerator) / float(denominator)


def _min_non_null(*values: Any) -> int | None:
    resolved = [int(item) for item in values if item is not None]
    return min(resolved) if resolved else None


def _max_non_null(*values: Any) -> int | None:
    resolved = [int(item) for item in values if item is not None]
    return max(resolved) if resolved else None
