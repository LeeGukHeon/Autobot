"""Train-ready tabular slice builder for the v6 edge2stage stack."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl

from ..collect.fixed_collection_contract import resolve_fixed_collection_markets
from ..micro.raw_readers import parse_date_range


UTC = timezone.utc
DEFAULT_QUOTE = "KRW"
TRAINING_SLICE_SCHEMA: dict[str, pl.DataType] = {
    "market": pl.Utf8,
    "bucket_start_ts_ms": pl.Int64,
    "bucket_end_ts_ms": pl.Int64,
    "operating_date_kst": pl.Utf8,
    "bucket_date_utc": pl.Utf8,
    "last_price": pl.Float64,
    "acc_trade_price_24h": pl.Float64,
    "signed_change_rate": pl.Float64,
    "ticker_age_ms": pl.Int64,
    "ticker_proxy_available": pl.Boolean,
    "ticker_source_kind": pl.Utf8,
    "ticker_source_kind_code": pl.Int8,
    "acc_trade_price_24h_log1p": pl.Float64,
    "trade_events_5s": pl.Int64,
    "trade_events_15s": pl.Int64,
    "trade_events_60s": pl.Int64,
    "trade_notional_5s": pl.Float64,
    "trade_notional_60s": pl.Float64,
    "trade_notional_5s_log1p": pl.Float64,
    "trade_notional_60s_log1p": pl.Float64,
    "buy_volume_5s": pl.Float64,
    "sell_volume_5s": pl.Float64,
    "signed_volume_5s": pl.Float64,
    "trade_imbalance_5s": pl.Float64,
    "vwap_5s": pl.Float64,
    "vwap_5s_vs_last_bps": pl.Float64,
    "large_trade_ratio_60s": pl.Float64,
    "best_bid": pl.Float64,
    "best_ask": pl.Float64,
    "spread_bps": pl.Float64,
    "bid_depth_top1_krw": pl.Float64,
    "ask_depth_top1_krw": pl.Float64,
    "bid_depth_top5_krw": pl.Float64,
    "ask_depth_top5_krw": pl.Float64,
    "bid_depth_top1_log1p": pl.Float64,
    "ask_depth_top1_log1p": pl.Float64,
    "bid_depth_top5_log1p": pl.Float64,
    "ask_depth_top5_log1p": pl.Float64,
    "depth_to_notional_bid_top5_log_ratio": pl.Float64,
    "depth_to_notional_ask_top5_log_ratio": pl.Float64,
    "queue_imbalance_top1": pl.Float64,
    "queue_imbalance_top5": pl.Float64,
    "microprice": pl.Float64,
    "microprice_bias_bps": pl.Float64,
    "book_update_count_5s": pl.Int64,
    "book_updates_per_trade": pl.Float64,
    "ret_1m": pl.Float64,
    "ret_5m": pl.Float64,
    "ret_15m": pl.Float64,
    "ret_60m": pl.Float64,
    "rv_1m_5m_window": pl.Float64,
    "rv_1m_15m_window": pl.Float64,
    "atr_pct_14": pl.Float64,
    "spread_over_atr": pl.Float64,
    "notional_burst_5s_vs_60s": pl.Float64,
    "distance_from_15m_high_low": pl.Float64,
    "btc_rel_strength_5m": pl.Float64,
    "eth_rel_strength_5m": pl.Float64,
    "market_cap_rank_fixed30": pl.Int64,
    "market_cap_rank_pct": pl.Float64,
    "universe_breadth_up_ratio": pl.Float64,
    "universe_notional_rank_pct": pl.Float64,
    "source_quality_score": pl.Float64,
    "ticker_available": pl.Boolean,
    "trade_available": pl.Boolean,
    "book_available": pl.Boolean,
    "candle_context_available": pl.Boolean,
    "label_available_20m": pl.Boolean,
    "spread_quality_pass_20m": pl.Boolean,
    "liquidity_pass_20m": pl.Boolean,
    "structure_pass_20m": pl.Boolean,
    "structural_tradeable_20m": pl.Int8,
    "tradeable_20m": pl.Int8,
    "net_edge_10m_bps": pl.Float64,
    "net_edge_20m_bps": pl.Float64,
    "net_edge_40m_bps": pl.Float64,
}

SLICE_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "run_id": pl.Utf8,
    "date": pl.Utf8,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "part_file": pl.Utf8,
    "built_at_ms": pl.Int64,
}

_TICKER_SOURCE_KIND_CODE = {
    "missing": 0,
    "candle_proxy": 1,
    "ws_raw": 2,
}

_FEATURE_COLUMNS: tuple[str, ...] = (
    "signed_change_rate",
    "ticker_age_ms",
    "ticker_proxy_available",
    "ticker_source_kind_code",
    "acc_trade_price_24h_log1p",
    "trade_events_5s",
    "trade_events_15s",
    "trade_events_60s",
    "trade_notional_5s_log1p",
    "trade_notional_60s_log1p",
    "notional_burst_5s_vs_60s",
    "buy_volume_5s",
    "sell_volume_5s",
    "signed_volume_5s",
    "trade_imbalance_5s",
    "vwap_5s_vs_last_bps",
    "large_trade_ratio_60s",
    "spread_bps",
    "spread_over_atr",
    "bid_depth_top1_log1p",
    "ask_depth_top1_log1p",
    "bid_depth_top5_log1p",
    "ask_depth_top5_log1p",
    "depth_to_notional_bid_top5_log_ratio",
    "depth_to_notional_ask_top5_log_ratio",
    "queue_imbalance_top1",
    "queue_imbalance_top5",
    "microprice_bias_bps",
    "book_update_count_5s",
    "book_updates_per_trade",
    "ret_1m",
    "ret_5m",
    "ret_15m",
    "ret_60m",
    "rv_1m_5m_window",
    "rv_1m_15m_window",
    "atr_pct_14",
    "distance_from_15m_high_low",
    "btc_rel_strength_5m",
    "eth_rel_strength_5m",
    "market_cap_rank_pct",
    "universe_breadth_up_ratio",
    "universe_notional_rank_pct",
    "source_quality_score",
)


@dataclass(frozen=True)
class MarketStateTrainingSliceBuildOptions:
    start: str
    end: str
    quote: str = DEFAULT_QUOTE
    markets: tuple[str, ...] | None = None
    market_state_root: Path = Path("data/derived/market_state_v1")
    tradeable_label_root: Path = Path("data/derived/tradeable_label_v1")
    net_edge_label_root: Path = Path("data/derived/net_edge_label_v1")
    out_root: Path = Path("data/derived/market_state_training_slice_v1")
    config_dir: Path = Path("config")
    skip_existing_complete: bool = True


@dataclass(frozen=True)
class MarketStateTrainingSliceBuildSummary:
    run_id: str
    dates: tuple[str, ...]
    selected_markets: tuple[str, ...]
    built_dates: int
    reused_dates: int
    rows_total: int
    manifest_file: Path
    build_report_file: Path


def build_market_state_training_slice_v1(options: MarketStateTrainingSliceBuildOptions) -> MarketStateTrainingSliceBuildSummary:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    requested_dates = _resolve_requested_dates(str(options.start).strip(), str(options.end).strip())
    selected_markets = _resolve_selected_markets(
        config_dir=Path(options.config_dir),
        quote=str(options.quote).strip().upper() or DEFAULT_QUOTE,
        explicit_markets=tuple(options.markets or ()),
    )
    market_state_manifest = _load_manifest(Path(options.market_state_root) / "_meta" / "manifest.parquet", schema_suffix="market")
    tradeable_manifest = _load_manifest(Path(options.tradeable_label_root) / "_meta" / "manifest.parquet", schema_suffix="market")
    net_edge_manifest = _load_manifest(Path(options.net_edge_label_root) / "_meta" / "manifest.parquet", schema_suffix="market")

    available_pairs = (
        market_state_manifest.select(["date", "market", "part_file"])
        .rename({"part_file": "market_state_part"})
        .join(
            tradeable_manifest.select(["date", "market", "part_file"]).rename({"part_file": "tradeable_part"}),
            on=["date", "market"],
            how="inner",
        )
        .join(
            net_edge_manifest.select(["date", "market", "part_file"]).rename({"part_file": "net_edge_part"}),
            on=["date", "market"],
            how="inner",
        )
        .filter(pl.col("date").is_in(list(requested_dates)))
    )
    if selected_markets:
        available_pairs = available_pairs.filter(pl.col("market").is_in(list(selected_markets)))

    built_dates = 0
    reused_dates = 0
    rows_total = 0
    manifest_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    existing_reusable_dates = (
        _load_existing_reusable_dates(out_root=options.out_root, selected_markets=selected_markets)
        if bool(options.skip_existing_complete)
        else set()
    )

    for date_value in requested_dates:
        if date_value in existing_reusable_dates:
            reused_dates += 1
            detail_rows.append({"date": date_value, "rows": None, "markets": int(len(selected_markets)), "status": "SKIPPED_ALREADY_COMPLETE"})
            continue
        pair_rows = available_pairs.filter(pl.col("date") == date_value)
        if pair_rows.height <= 0:
            continue
        frames: list[pl.DataFrame] = []
        for row in pair_rows.iter_rows(named=True):
            ms = pl.read_parquet(str(row["market_state_part"]))
            tl = pl.read_parquet(str(row["tradeable_part"]))
            ne = pl.read_parquet(str(row["net_edge_part"]))
            merged = ms.join(
                tl,
                on=["market", "bucket_start_ts_ms", "bucket_end_ts_ms", "operating_date_kst", "bucket_date_utc"],
                how="inner",
                suffix="_tradeable",
            ).join(
                ne,
                on=["market", "bucket_start_ts_ms", "bucket_end_ts_ms", "operating_date_kst", "bucket_date_utc"],
                how="inner",
                suffix="_netedge",
            )
            frames.append(merged)
        if not frames:
            continue
        combined = pl.concat(frames, how="vertical").sort(["bucket_start_ts_ms", "market"])
        combined = _normalize_training_slice_frame(combined)
        combined = combined.filter(pl.col("label_available_20m"))
        if combined.height <= 0:
            detail_rows.append({"date": date_value, "rows": 0, "markets": 0, "status": "SKIPPED_LABEL_EMPTY"})
            continue
        built_at_ms = int(datetime.now(UTC).timestamp() * 1000)
        _remove_existing_date(options.out_root, date_value)
        part = _write_slice_part(options.out_root, combined, run_id, date_value)
        built_dates += 1
        rows_total += int(combined.height)
        manifest_rows.append(
            {
                "run_id": run_id,
                "date": date_value,
                "rows": int(combined.height),
                "min_ts_ms": int(combined.get_column("bucket_end_ts_ms").min()),
                "max_ts_ms": int(combined.get_column("bucket_end_ts_ms").max()),
                "part_file": str(part),
                "built_at_ms": built_at_ms,
            }
        )
        detail_rows.append(
            {
                "date": date_value,
                "rows": int(combined.height),
                "markets": int(combined.get_column("market").n_unique()),
                "ticker_source_kind_counts": combined.group_by("ticker_source_kind").len().sort("ticker_source_kind").to_dicts(),
                "tradeable_positive_ratio": float(combined.get_column("tradeable_20m").fill_null(0).cast(pl.Float64).mean()),
                "status": "OK",
            }
        )

    manifest_path = options.out_root / "_meta" / "manifest.parquet"
    _replace_manifest_dates(manifest_path=manifest_path, rebuilt_dates=[str(row["date"]) for row in manifest_rows])
    _append_manifest_rows(manifest_path=manifest_path, rows=manifest_rows)
    _write_slice_contracts(options.out_root, selected_markets)
    report = {
        "policy": "market_state_training_slice_v1_build_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_id": run_id,
        "dates": list(requested_dates),
        "selected_markets": list(selected_markets),
        "built_dates": int(built_dates),
        "reused_dates": int(reused_dates),
        "rows_total": int(rows_total),
        "skip_existing_complete": bool(options.skip_existing_complete),
        "out_root": str(options.out_root),
        "details": detail_rows,
    }
    build_report_path = options.out_root / "_meta" / "build_report.json"
    build_report_path.parent.mkdir(parents=True, exist_ok=True)
    build_report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest_path = options.out_root / "_meta" / "market_state_training_slice_v1_latest.json"
    latest_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return MarketStateTrainingSliceBuildSummary(
        run_id=run_id,
        dates=requested_dates,
        selected_markets=selected_markets,
        built_dates=built_dates,
        reused_dates=reused_dates,
        rows_total=rows_total,
        manifest_file=manifest_path,
        build_report_file=build_report_path,
    )


def _resolve_requested_dates(start: str, end: str) -> tuple[str, ...]:
    return parse_date_range(start=start, end=end)


def _resolve_selected_markets(*, config_dir: Path, quote: str, explicit_markets: tuple[str, ...]) -> tuple[str, ...]:
    if explicit_markets:
        return tuple(str(item).strip().upper() for item in explicit_markets if str(item).strip())
    return resolve_fixed_collection_markets(config_dir=config_dir, quote=quote, explicit_markets=None)


def _load_manifest(path: Path, *, schema_suffix: str) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame(schema={"date": pl.Utf8, "market": pl.Utf8, "part_file": pl.Utf8})
    return pl.read_parquet(path).select(["date", "market", "part_file"])


def _normalize_training_slice_frame(frame: pl.DataFrame) -> pl.DataFrame:
    result = frame.with_columns(
        [
            pl.col("ticker_source_kind")
            .fill_null("missing")
            .replace_strict(_TICKER_SOURCE_KIND_CODE, default=0)
            .cast(pl.Int8)
            .alias("ticker_source_kind_code"),
        ]
    )
    for name, dtype in TRAINING_SLICE_SCHEMA.items():
        if name not in result.columns:
            result = result.with_columns(pl.lit(None, dtype=dtype).alias(name))

    numeric_fill_zero = [
        "last_price",
        "acc_trade_price_24h",
        "signed_change_rate",
        "ticker_age_ms",
        "trade_events_5s",
        "trade_events_15s",
        "trade_events_60s",
        "trade_notional_5s",
        "trade_notional_60s",
        "buy_volume_5s",
        "sell_volume_5s",
        "signed_volume_5s",
        "trade_imbalance_5s",
        "vwap_5s",
        "large_trade_ratio_60s",
        "best_bid",
        "best_ask",
        "spread_bps",
        "bid_depth_top1_krw",
        "ask_depth_top1_krw",
        "bid_depth_top5_krw",
        "ask_depth_top5_krw",
        "queue_imbalance_top1",
        "queue_imbalance_top5",
        "microprice",
        "microprice_bias_bps",
        "book_update_count_5s",
        "ret_1m",
        "ret_5m",
        "ret_15m",
        "ret_60m",
        "rv_1m_5m_window",
        "rv_1m_15m_window",
        "atr_pct_14",
        "distance_from_15m_high_low",
        "btc_rel_strength_5m",
        "eth_rel_strength_5m",
        "market_cap_rank_fixed30",
        "universe_breadth_up_ratio",
        "universe_notional_rank_pct",
        "source_quality_score",
    ]
    bool_fill_false = [
        "ticker_proxy_available",
        "ticker_available",
        "trade_available",
        "book_available",
        "candle_context_available",
        "label_available_20m",
        "spread_quality_pass_20m",
        "liquidity_pass_20m",
        "structure_pass_20m",
    ]
    for column in numeric_fill_zero:
        result = result.with_columns(pl.col(column).cast(pl.Float64, strict=False).fill_null(0.0).fill_nan(0.0).alias(column))
    for column in bool_fill_false:
        result = result.with_columns(pl.col(column).cast(pl.Boolean, strict=False).fill_null(False).alias(column))
    result = result.with_columns(
        [
            pl.col("tradeable_20m").cast(pl.Int8, strict=False).fill_null(0).alias("tradeable_20m"),
            (
                pl.when(
                    pl.col("label_available_20m")
                    & pl.col("spread_quality_pass_20m")
                    & pl.col("liquidity_pass_20m")
                    & pl.col("structure_pass_20m")
                )
                .then(1)
                .otherwise(0)
            ).cast(pl.Int8).alias("structural_tradeable_20m"),
        ]
    )
    result = result.with_columns(
        [
            (pl.col("acc_trade_price_24h").clip(lower_bound=0.0) + 1.0).log().alias("acc_trade_price_24h_log1p"),
            (pl.col("trade_notional_5s").clip(lower_bound=0.0) + 1.0).log().alias("trade_notional_5s_log1p"),
            (pl.col("trade_notional_60s").clip(lower_bound=0.0) + 1.0).log().alias("trade_notional_60s_log1p"),
            (
                pl.when(pl.col("last_price") > 0.0)
                .then(((pl.col("vwap_5s") / pl.col("last_price")) - 1.0) * 10_000.0)
                .otherwise(0.0)
            ).alias("vwap_5s_vs_last_bps"),
            (pl.col("bid_depth_top1_krw").clip(lower_bound=0.0) + 1.0).log().alias("bid_depth_top1_log1p"),
            (pl.col("ask_depth_top1_krw").clip(lower_bound=0.0) + 1.0).log().alias("ask_depth_top1_log1p"),
            (pl.col("bid_depth_top5_krw").clip(lower_bound=0.0) + 1.0).log().alias("bid_depth_top5_log1p"),
            (pl.col("ask_depth_top5_krw").clip(lower_bound=0.0) + 1.0).log().alias("ask_depth_top5_log1p"),
            (
                ((pl.col("bid_depth_top5_krw").clip(lower_bound=0.0) + 1.0).log())
                - ((pl.col("trade_notional_60s").clip(lower_bound=0.0) + 1.0).log())
            ).alias("depth_to_notional_bid_top5_log_ratio"),
            (
                ((pl.col("ask_depth_top5_krw").clip(lower_bound=0.0) + 1.0).log())
                - ((pl.col("trade_notional_60s").clip(lower_bound=0.0) + 1.0).log())
            ).alias("depth_to_notional_ask_top5_log_ratio"),
            (
                pl.col("book_update_count_5s")
                / pl.max_horizontal(pl.col("trade_events_5s"), pl.lit(1.0))
            ).alias("book_updates_per_trade"),
            (
                pl.col("spread_bps")
                / pl.max_horizontal(pl.col("atr_pct_14") * pl.lit(10_000.0), pl.lit(1e-6))
            ).alias("spread_over_atr"),
            (
                pl.col("trade_notional_5s")
                / pl.max_horizontal(pl.col("trade_notional_60s") / pl.lit(12.0), pl.lit(1.0))
            ).alias("notional_burst_5s_vs_60s"),
            (
                pl.col("market_cap_rank_fixed30")
                / pl.lit(30.0)
            ).alias("market_cap_rank_pct"),
        ]
    )
    return result.select([name for name in TRAINING_SLICE_SCHEMA.keys()])


def _write_slice_part(out_root: Path, frame: pl.DataFrame, run_id: str, date_value: str) -> Path:
    target_dir = Path(out_root) / f"date={date_value}"
    target_dir.mkdir(parents=True, exist_ok=True)
    part_path = target_dir / f"part-{run_id}.parquet"
    frame.write_parquet(part_path, compression="zstd")
    return part_path


def _save_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        pl.DataFrame([], schema=SLICE_MANIFEST_SCHEMA, orient="row").write_parquet(path, compression="zstd")
        return
    pl.DataFrame(rows, schema=SLICE_MANIFEST_SCHEMA, orient="row").write_parquet(path, compression="zstd")


def _load_slice_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame([], schema=SLICE_MANIFEST_SCHEMA, orient="row")
    return pl.read_parquet(path).select(list(SLICE_MANIFEST_SCHEMA.keys()))


def _replace_manifest_dates(*, manifest_path: Path, rebuilt_dates: list[str]) -> None:
    if not rebuilt_dates or not manifest_path.exists():
        return
    rebuilt = {str(item) for item in rebuilt_dates}
    frame = _load_slice_manifest(manifest_path)
    if frame.height <= 0:
        return
    filtered = frame.filter(~pl.col("date").is_in(list(rebuilt)))
    _save_manifest(manifest_path, filtered.to_dicts())


def _append_manifest_rows(*, manifest_path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        if not manifest_path.exists():
            _save_manifest(manifest_path, [])
        return
    incoming = pl.DataFrame(rows, schema=SLICE_MANIFEST_SCHEMA, orient="row")
    if manifest_path.exists():
        combined = pl.concat([_load_slice_manifest(manifest_path), incoming], how="vertical")
    else:
        combined = incoming
    _save_manifest(manifest_path, combined.to_dicts())


def _remove_existing_date(root: Path, date_value: str) -> None:
    target = Path(root) / f"date={date_value}"
    if target.exists():
        for path in target.glob("*.parquet"):
            path.unlink(missing_ok=True)


def _load_existing_reusable_dates(*, out_root: Path, selected_markets: tuple[str, ...]) -> set[str]:
    feature_spec_path = Path(out_root) / "_meta" / "feature_spec.json"
    if not feature_spec_path.exists():
        return set()
    try:
        feature_spec = json.loads(feature_spec_path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    existing_selected = tuple(str(item).strip().upper() for item in (feature_spec.get("selected_markets") or []) if str(item).strip())
    if tuple(selected_markets) != existing_selected:
        return set()
    manifest = _load_slice_manifest(Path(out_root) / "_meta" / "manifest.parquet")
    reusable: set[str] = set()
    for row in manifest.iter_rows(named=True):
        part_file = Path(str(row.get("part_file") or ""))
        if part_file.exists():
            reusable.add(str(row["date"]))
    return reusable


def _write_slice_contracts(out_root: Path, selected_markets: tuple[str, ...]) -> None:
    meta_dir = Path(out_root) / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    feature_spec = {
        "policy": "market_state_training_slice_v1_feature_spec",
        "feature_columns": list(_FEATURE_COLUMNS),
        "auxiliary_columns": [
            "market",
            "bucket_start_ts_ms",
            "bucket_end_ts_ms",
            "operating_date_kst",
            "bucket_date_utc",
            "ticker_source_kind",
        ],
        "categorical_mappings": {
            "ticker_source_kind_code": dict(_TICKER_SOURCE_KIND_CODE),
        },
        "selected_markets": list(selected_markets),
    }
    label_spec = {
        "policy": "market_state_training_slice_v1_label_spec",
        "primary_class_label": "tradeable_20m",
        "stage_a_class_label": "structural_tradeable_20m",
        "primary_regression_label": "net_edge_20m_bps",
        "auxiliary_labels": ["net_edge_10m_bps", "net_edge_40m_bps"],
    }
    slice_contract = {
        "policy": "market_state_training_slice_v1",
        "label_filter": "label_available_20m == true",
        "categorical_encoding": "deterministic_integer_code",
    }
    (meta_dir / "feature_spec.json").write_text(json.dumps(feature_spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (meta_dir / "label_spec.json").write_text(json.dumps(label_spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (meta_dir / "slice_contract.json").write_text(json.dumps(slice_contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
