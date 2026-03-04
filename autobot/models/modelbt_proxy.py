"""Fast signal backtest proxy for registered models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from autobot.backtest.metrics import max_drawdown_pct
from autobot.data import expected_interval_ms

from .dataset_loader import build_dataset_request, feature_columns_from_spec, load_feature_dataset
from .registry import load_json, load_model_bundle, resolve_run_dir
from .train_v1 import _predict_scores


@dataclass(frozen=True)
class ModelBtProxyOptions:
    registry_root: Path
    parquet_root: Path
    base_candles_dataset: str
    out_root: Path
    model_ref: str
    model_family: str | None
    tf: str
    quote: str
    top_n: int
    start: str
    end: str
    select_mode: str = "top_pct"
    top_pct: float = 0.05
    hold_bars: int = 6
    fee_bps: float = 5.0


@dataclass(frozen=True)
class ModelBtProxyResult:
    run_dir: Path
    equity_csv: Path
    trades_csv: Path
    summary_json: Path
    diagnostics_json: Path
    summary: dict[str, Any]


def run_modelbt_proxy(options: ModelBtProxyOptions) -> ModelBtProxyResult:
    run_dir_model = resolve_run_dir(
        options.registry_root,
        model_ref=str(options.model_ref).strip(),
        model_family=(str(options.model_family).strip() if options.model_family else None),
    )
    train_config = load_json(run_dir_model / "train_config.yaml")
    if not train_config:
        raise ValueError(f"invalid train_config.yaml at {run_dir_model}")

    dataset_root = Path(str(train_config.get("dataset_root", "")))
    if not dataset_root.exists():
        raise FileNotFoundError(f"dataset_root not found: {dataset_root}")
    feature_cols = tuple(str(item) for item in train_config.get("feature_columns", []))
    if not feature_cols:
        feature_cols = feature_columns_from_spec(dataset_root)

    request = build_dataset_request(
        dataset_root=dataset_root,
        tf=str(options.tf).strip().lower(),
        quote=str(options.quote).strip().upper(),
        top_n=max(int(options.top_n), 1),
        start=options.start,
        end=options.end,
        batch_rows=max(int(train_config.get("batch_rows", 200_000)), 1),
    )
    dataset = load_feature_dataset(request, feature_columns=feature_cols)
    bundle = load_model_bundle(run_dir_model)
    scores = _predict_scores(bundle, dataset.X)

    scored = pl.DataFrame(
        {
            "ts_ms": dataset.ts_ms.astype(np.int64, copy=False),
            "market": dataset.markets.astype(str, copy=False),
            "score": scores.astype(np.float64, copy=False),
            "y_reg": dataset.y_reg.astype(np.float64, copy=False),
        }
    ).sort(["ts_ms", "market"])

    selected = _select_top_pct(scored, top_pct=float(options.top_pct))
    selected_rows = int(selected.height)

    base_candles_root = _resolve_base_candles_root(
        parquet_root=options.parquet_root,
        base_candles_dataset=options.base_candles_dataset,
    )
    interval_ms = expected_interval_ms(options.tf)
    start_ts_ms = int(scored.get_column("ts_ms").min()) if scored.height > 0 else 0
    end_ts_ms = int(scored.get_column("ts_ms").max()) + int(options.hold_bars) * interval_ms if scored.height > 0 else 0
    prices = _load_close_frame(
        dataset_root=base_candles_root,
        tf=options.tf,
        markets=tuple(sorted(set(selected.get_column("market").to_list())) if selected.height > 0 else []),
        from_ts_ms=start_ts_ms,
        to_ts_ms=end_ts_ms,
        hold_bars=max(int(options.hold_bars), 1),
    )
    trades = (
        selected.join(prices, on=["market", "ts_ms"], how="left")
        .filter(pl.col("close").is_not_null() & pl.col("close_fwd").is_not_null() & (pl.col("close") > 0.0))
        .with_columns(
            [
                (pl.col("close_fwd") / pl.col("close") - 1.0).alias("ret_raw"),
                (pl.col("close_fwd") / pl.col("close") - 1.0 - float(options.fee_bps) / 10_000.0).alias("ret_net"),
            ]
        )
        .sort(["ts_ms", "score"], descending=[False, True])
    )

    bar_equity = _equity_curve_from_trades(trades)
    summary = _summary_payload(
        trades=trades,
        equity=bar_equity,
        selected_rows=selected_rows,
        scored_rows=int(scored.height),
        fee_bps=float(options.fee_bps),
        hold_bars=max(int(options.hold_bars), 1),
    )

    diagnostics = {
        "run_model_ref": str(options.model_ref),
        "run_model_family": options.model_family,
        "train_run_dir": str(run_dir_model),
        "rows_scored": int(scored.height),
        "rows_selected": selected_rows,
        "rows_traded": int(trades.height),
        "rows_dropped_no_horizon": max(selected_rows - int(trades.height), 0),
        "dropped_no_micro": 0,
        "per_market": _per_market_stats(trades),
    }

    run_id = _build_modelbt_run_id(options=options, selected_rows=selected_rows)
    run_dir = options.out_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    equity_csv = run_dir / "equity.csv"
    trades_csv = run_dir / "trades.csv"
    summary_json = run_dir / "summary.json"
    diagnostics_json = run_dir / "diagnostics.json"

    bar_equity.write_csv(equity_csv)
    trades.write_csv(trades_csv)
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    diagnostics_json.write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ModelBtProxyResult(
        run_dir=run_dir,
        equity_csv=equity_csv,
        trades_csv=trades_csv,
        summary_json=summary_json,
        diagnostics_json=diagnostics_json,
        summary=summary,
    )


def _select_top_pct(frame: pl.DataFrame, *, top_pct: float) -> pl.DataFrame:
    pct = min(max(float(top_pct), 0.0001), 1.0)
    if frame.height <= 0:
        return frame
    ranked = frame.with_columns(
        [
            pl.len().over("ts_ms").cast(pl.Int64).alias("__n"),
            pl.col("score").rank(method="ordinal", descending=True).over("ts_ms").cast(pl.Int64).alias("__rank"),
        ]
    ).with_columns(
        [
            (pl.col("__n").cast(pl.Float64) * pct).ceil().clip(lower_bound=1.0).cast(pl.Int64).alias("__k"),
        ]
    )
    return ranked.filter(pl.col("__rank") <= pl.col("__k")).drop(["__n", "__rank", "__k"])


def _resolve_base_candles_root(*, parquet_root: Path, base_candles_dataset: str) -> Path:
    value = str(base_candles_dataset).strip() or "auto"
    if value.lower() != "auto":
        path = Path(value)
        if path.exists():
            return path
        if path.is_absolute():
            raise FileNotFoundError(f"base candles dataset path not found: {path}")
        candidate = parquet_root / path
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"base candles dataset not found: {candidate}")
    for name in ("candles_api_v1", "candles_v1"):
        candidate = parquet_root / name
        if candidate.exists():
            return candidate
    raise FileNotFoundError("unable to resolve base candles dataset from parquet_root")


def _load_close_frame(
    *,
    dataset_root: Path,
    tf: str,
    markets: tuple[str, ...],
    from_ts_ms: int,
    to_ts_ms: int,
    hold_bars: int,
) -> pl.DataFrame:
    if not markets:
        return pl.DataFrame(schema={"market": pl.Utf8, "ts_ms": pl.Int64, "close": pl.Float64, "close_fwd": pl.Float64})
    frames: list[pl.DataFrame] = []
    for market in markets:
        files = _candle_files(dataset_root=dataset_root, tf=tf, market=market)
        if not files:
            continue
        frame = (
            _collect_lazy(
                pl.scan_parquet([str(path) for path in files]).filter(
                    (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))
                ).select([pl.col("ts_ms").cast(pl.Int64), pl.col("close").cast(pl.Float64)])
            )
            .sort("ts_ms")
            .with_columns(pl.lit(market).alias("market"))
            .with_columns(pl.col("close").shift(-int(hold_bars)).over("market").alias("close_fwd"))
            .select(["market", "ts_ms", "close", "close_fwd"])
        )
        if frame.height > 0:
            frames.append(frame)
    if not frames:
        return pl.DataFrame(schema={"market": pl.Utf8, "ts_ms": pl.Int64, "close": pl.Float64, "close_fwd": pl.Float64})
    return pl.concat(frames, how="vertical_relaxed")


def _candle_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if date_dir.is_dir():
            nested.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
    if nested:
        return nested
    direct = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if direct:
        return direct
    legacy = market_dir / "part.parquet"
    return [legacy] if legacy.exists() else []


def _equity_curve_from_trades(trades: pl.DataFrame) -> pl.DataFrame:
    if trades.height <= 0:
        return pl.DataFrame({"ts_ms": [], "ret_net_mean": [], "equity": []})
    bars = trades.group_by("ts_ms").agg(pl.col("ret_net").mean().alias("ret_net_mean")).sort("ts_ms")
    return bars.with_columns((pl.col("ret_net_mean") + 1.0).cum_prod().alias("equity"))


def _summary_payload(
    *,
    trades: pl.DataFrame,
    equity: pl.DataFrame,
    selected_rows: int,
    scored_rows: int,
    fee_bps: float,
    hold_bars: int,
) -> dict[str, Any]:
    trades_count = int(trades.height)
    win_rate = float(trades.filter(pl.col("ret_net") > 0.0).height) / float(trades_count) if trades_count > 0 else 0.0
    avg_return = float(trades.get_column("ret_net").mean()) if trades_count > 0 else 0.0
    equity_values = [float(item) for item in equity.get_column("equity").to_list()] if equity.height > 0 else []
    equity_end = float(equity_values[-1]) if equity_values else 1.0
    return {
        "trades_count": trades_count,
        "selected_rows": int(selected_rows),
        "scored_rows": int(scored_rows),
        "win_rate": win_rate,
        "avg_return_net": avg_return,
        "max_drawdown": max_drawdown_pct(equity_values),
        "equity_end": equity_end,
        "fee_bps": float(fee_bps),
        "hold_bars": int(hold_bars),
    }


def _per_market_stats(trades: pl.DataFrame) -> list[dict[str, Any]]:
    if trades.height <= 0:
        return []
    grouped = (
        trades.group_by("market")
        .agg(
            [
                pl.len().alias("trades"),
                pl.col("ret_net").mean().alias("avg_return_net"),
                (pl.col("ret_net") > 0.0).sum().alias("wins"),
            ]
        )
        .sort("market")
    )
    rows: list[dict[str, Any]] = []
    for row in grouped.iter_rows(named=True):
        trades_count = int(row.get("trades") or 0)
        wins = int(row.get("wins") or 0)
        rows.append(
            {
                "market": str(row.get("market")),
                "trades": trades_count,
                "wins": wins,
                "win_rate": (float(wins) / float(trades_count)) if trades_count > 0 else 0.0,
                "avg_return_net": float(row.get("avg_return_net") or 0.0),
            }
        )
    return rows


def _build_modelbt_run_id(*, options: ModelBtProxyOptions, selected_rows: int) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "model_ref": options.model_ref,
        "model_family": options.model_family,
        "tf": options.tf,
        "quote": options.quote,
        "top_n": int(options.top_n),
        "start": options.start,
        "end": options.end,
        "top_pct": float(options.top_pct),
        "hold_bars": int(options.hold_bars),
        "selected_rows": int(selected_rows),
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    return f"modelbt-{stamp}-{digest}"


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
