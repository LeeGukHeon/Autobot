"""Feature dataset loading helpers for model training."""

from __future__ import annotations

from dataclasses import dataclass
import heapq
import json
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import polars as pl

from autobot.features.feature_spec import parse_date_to_ts_ms, sha256_file, sha256_json


@dataclass(frozen=True)
class DatasetRequest:
    dataset_root: Path
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    markets: tuple[str, ...] = ()
    batch_rows: int = 200_000


@dataclass(frozen=True)
class FeatureBatch:
    market: str
    ts_ms: np.ndarray
    X: np.ndarray
    y_cls: np.ndarray
    y_reg: np.ndarray
    y_rank: np.ndarray
    sample_weight: np.ndarray

    @property
    def rows(self) -> int:
        return int(self.X.shape[0])


@dataclass(frozen=True)
class FeatureDataset:
    X: np.ndarray
    y_cls: np.ndarray
    y_reg: np.ndarray
    y_rank: np.ndarray
    sample_weight: np.ndarray
    ts_ms: np.ndarray
    markets: np.ndarray
    feature_names: tuple[str, ...]
    selected_markets: tuple[str, ...]
    rows_by_market: dict[str, int]

    @property
    def rows(self) -> int:
        return int(self.X.shape[0])


@dataclass(frozen=True)
class FeatureTsGroup:
    ts_ms: int
    frame: pl.DataFrame


def build_dataset_request(
    *,
    dataset_root: Path,
    tf: str,
    quote: str | None = None,
    top_n: int | None = None,
    start: str | None = None,
    end: str | None = None,
    markets: tuple[str, ...] | None = None,
    batch_rows: int = 200_000,
) -> DatasetRequest:
    start_ts_ms = parse_date_to_ts_ms(start) if start else None
    end_ts_ms = parse_date_to_ts_ms(end, end_of_day=True) if end else None
    if start_ts_ms is not None and end_ts_ms is not None and end_ts_ms < start_ts_ms:
        raise ValueError("end must be >= start")
    normalized_markets = tuple(str(item).strip().upper() for item in (markets or ()) if str(item).strip())
    return DatasetRequest(
        dataset_root=Path(dataset_root),
        tf=str(tf).strip().lower(),
        quote=(str(quote).strip().upper() if quote else None),
        top_n=(max(int(top_n), 1) if top_n is not None else None),
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        markets=normalized_markets,
        batch_rows=max(int(batch_rows), 1),
    )


def load_feature_spec(dataset_root: Path) -> dict[str, Any]:
    return _load_json(dataset_root / "_meta" / "feature_spec.json")


def load_label_spec(dataset_root: Path) -> dict[str, Any]:
    return _load_json(dataset_root / "_meta" / "label_spec.json")


def feature_columns_from_spec(dataset_root: Path) -> tuple[str, ...]:
    spec = load_feature_spec(dataset_root)
    cols = spec.get("feature_columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError("feature_spec.json is missing non-empty feature_columns")
    return tuple(str(col) for col in cols)


def select_markets(request: DatasetRequest) -> list[str]:
    if request.markets:
        return list(request.markets)

    manifest_path = request.dataset_root / "_meta" / "manifest.parquet"
    quote_prefix = f"{request.quote}-" if request.quote else ""
    tf_value = request.tf

    if manifest_path.exists():
        manifest = pl.read_parquet(manifest_path)
        if manifest.height > 0 and "tf" in manifest.columns and "market" in manifest.columns:
            filtered = manifest.filter(pl.col("tf") == tf_value)
            if quote_prefix:
                filtered = filtered.filter(pl.col("market").str.starts_with(quote_prefix))
            if request.top_n is not None and request.top_n > 0:
                filtered = filtered.head(request.top_n)
            markets = [str(row["market"]).strip().upper() for row in filtered.iter_rows(named=True)]
            markets = [market for market in markets if market]
            if markets:
                return markets

    tf_dir = request.dataset_root / f"tf={request.tf}"
    if not tf_dir.exists():
        return []
    markets: list[str] = []
    for entry in tf_dir.iterdir():
        if not entry.is_dir() or not entry.name.startswith("market="):
            continue
        market = entry.name.replace("market=", "", 1).strip().upper()
        if not market:
            continue
        if quote_prefix and not market.startswith(quote_prefix):
            continue
        markets.append(market)
    markets.sort()
    if request.top_n is not None and request.top_n > 0:
        markets = markets[: request.top_n]
    return markets


def iter_feature_batches(
    request: DatasetRequest,
    *,
    feature_columns: tuple[str, ...] | None = None,
    y_cls_column: str = "y_cls",
    y_reg_column: str = "y_reg",
    y_rank_column: str = "y_rank",
) -> Iterator[FeatureBatch]:
    feature_cols = feature_columns or feature_columns_from_spec(request.dataset_root)
    y_cls_name = str(y_cls_column).strip() or "y_cls"
    y_reg_name = str(y_reg_column).strip() or "y_reg"
    y_rank_name = str(y_rank_column).strip() or "y_rank"
    selected_markets = select_markets(request)
    for market in selected_markets:
        frame = _scan_market_frame(
            request=request,
            market=market,
            feature_columns=feature_cols,
            y_cls_column=y_cls_name,
            y_reg_column=y_reg_name,
            y_rank_column=y_rank_name,
        )
        if frame.height <= 0:
            continue
        frame = frame.drop_nulls(subset=["y_cls"])
        if frame.height <= 0:
            continue
        total_rows = frame.height
        batch_rows = max(int(request.batch_rows), 1)
        for offset in range(0, total_rows, batch_rows):
            chunk = frame.slice(offset, batch_rows)
            x = chunk.select(list(feature_cols)).to_numpy().astype(np.float32, copy=False)
            y_cls = chunk.get_column("y_cls").to_numpy().astype(np.int8, copy=False)
            y_reg = chunk.get_column("y_reg").to_numpy().astype(np.float32, copy=False)
            y_rank = chunk.get_column("y_rank").to_numpy().astype(np.float32, copy=False)
            sample_weight = chunk.get_column("sample_weight").to_numpy().astype(np.float32, copy=False)
            ts_ms = chunk.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
            yield FeatureBatch(
                market=market,
                ts_ms=ts_ms,
                X=x,
                y_cls=y_cls,
                y_reg=y_reg,
                y_rank=y_rank,
                sample_weight=sample_weight,
            )


def load_feature_dataset(
    request: DatasetRequest,
    *,
    feature_columns: tuple[str, ...] | None = None,
    y_cls_column: str = "y_cls",
    y_reg_column: str = "y_reg",
    y_rank_column: str = "y_rank",
) -> FeatureDataset:
    feature_cols = feature_columns or feature_columns_from_spec(request.dataset_root)
    x_parts: list[np.ndarray] = []
    y_cls_parts: list[np.ndarray] = []
    y_reg_parts: list[np.ndarray] = []
    y_rank_parts: list[np.ndarray] = []
    weight_parts: list[np.ndarray] = []
    ts_parts: list[np.ndarray] = []
    market_parts: list[np.ndarray] = []
    rows_by_market: dict[str, int] = {}
    selected_markets: set[str] = set()

    for batch in iter_feature_batches(
        request,
        feature_columns=feature_cols,
        y_cls_column=y_cls_column,
        y_reg_column=y_reg_column,
        y_rank_column=y_rank_column,
    ):
        x_parts.append(batch.X)
        y_cls_parts.append(batch.y_cls)
        y_reg_parts.append(batch.y_reg)
        y_rank_parts.append(batch.y_rank)
        weight_parts.append(batch.sample_weight)
        ts_parts.append(batch.ts_ms)
        market_values = np.full(batch.rows, batch.market, dtype=object)
        market_parts.append(market_values)
        rows_by_market[batch.market] = rows_by_market.get(batch.market, 0) + batch.rows
        selected_markets.add(batch.market)

    if not x_parts:
        raise ValueError("no feature rows found for the requested train dataset")

    x = np.concatenate(x_parts, axis=0)
    y_cls = np.concatenate(y_cls_parts, axis=0)
    y_reg = np.concatenate(y_reg_parts, axis=0)
    y_rank = np.concatenate(y_rank_parts, axis=0)
    sample_weight = np.concatenate(weight_parts, axis=0)
    ts_ms = np.concatenate(ts_parts, axis=0)
    markets = np.concatenate(market_parts, axis=0)

    order = np.lexsort((markets, ts_ms))
    return FeatureDataset(
        X=x[order],
        y_cls=y_cls[order],
        y_reg=y_reg[order],
        y_rank=y_rank[order],
        sample_weight=sample_weight[order],
        ts_ms=ts_ms[order],
        markets=markets[order],
        feature_names=feature_cols,
        selected_markets=tuple(sorted(selected_markets)),
        rows_by_market=rows_by_market,
    )


def load_feature_aux_frame(
    request: DatasetRequest,
    *,
    columns: tuple[str, ...],
    y_cls_column: str = "y_cls",
    y_reg_column: str = "y_reg",
    y_rank_column: str = "y_rank",
) -> pl.DataFrame:
    selected_markets = select_markets(request)
    normalized_columns = tuple(str(col).strip() for col in columns if str(col).strip())
    frames: list[pl.DataFrame] = []
    for market in selected_markets:
        frame = _scan_market_aux_frame(
            request=request,
            market=market,
            columns=normalized_columns,
            y_cls_column=y_cls_column,
            y_reg_column=y_reg_column,
            y_rank_column=y_rank_column,
        )
        if frame.height <= 0:
            continue
        frame = frame.drop_nulls(subset=["y_cls"])
        if frame.height <= 0:
            continue
        frames.append(frame)
    if not frames:
        raise ValueError("no auxiliary feature rows found for the requested train dataset")
    return pl.concat(frames, how="vertical").sort(["ts_ms", "market"])


def iter_feature_rows_grouped_by_ts(
    request: DatasetRequest,
    *,
    feature_columns: tuple[str, ...] | None = None,
    extra_columns: tuple[str, ...] = ("close",),
) -> Iterator[FeatureTsGroup]:
    """Yield per-ts market rows without materializing a full all-market frame."""

    feature_cols = feature_columns or feature_columns_from_spec(request.dataset_root)
    selected_markets = select_markets(request)
    if not selected_markets:
        return

    normalized_extra_cols = tuple(str(col).strip() for col in extra_columns if str(col).strip())
    ordered_cols = ("ts_ms", "market", *feature_cols, *normalized_extra_cols)
    heap: list[tuple[int, int, dict[str, Any], Iterator[dict[str, Any]]]] = []
    seq = 0

    for market in selected_markets:
        frame = _scan_market_rows(
            request=request,
            market=market,
            feature_columns=feature_cols,
            extra_columns=normalized_extra_cols,
        )
        if frame.height <= 0:
            continue
        row_iter = frame.iter_rows(named=True)
        first = next(row_iter, None)
        if first is None:
            continue
        heapq.heappush(heap, (int(first["ts_ms"]), seq, first, row_iter))
        seq += 1

    while heap:
        ts_ms = int(heap[0][0])
        rows: list[dict[str, Any]] = []
        while heap and int(heap[0][0]) == ts_ms:
            _, seq_id, row, row_iter = heapq.heappop(heap)
            rows.append(row)
            nxt = next(row_iter, None)
            if nxt is not None:
                heapq.heappush(heap, (int(nxt["ts_ms"]), seq_id, nxt, row_iter))
        if not rows:
            continue
        frame = pl.DataFrame(rows)
        present_cols = [col for col in ordered_cols if col in frame.columns]
        yield FeatureTsGroup(ts_ms=ts_ms, frame=frame.select(present_cols).sort("market"))


def build_data_fingerprint(
    *,
    request: DatasetRequest,
    selected_markets: tuple[str, ...],
    total_rows: int,
) -> dict[str, Any]:
    manifest_path = request.dataset_root / "_meta" / "manifest.parquet"
    feature_spec_path = request.dataset_root / "_meta" / "feature_spec.json"
    label_spec_path = request.dataset_root / "_meta" / "label_spec.json"
    return {
        "dataset_root": str(request.dataset_root),
        "tf": request.tf,
        "quote": request.quote,
        "top_n": request.top_n,
        "start_ts_ms": request.start_ts_ms,
        "end_ts_ms": request.end_ts_ms,
        "selected_markets": list(selected_markets),
        "rows_total": int(total_rows),
        "manifest_sha256": sha256_file(manifest_path),
        "feature_spec_sha256": sha256_file(feature_spec_path),
        "label_spec_sha256": sha256_file(label_spec_path),
        "request_sha256": sha256_json(
            {
                "dataset_root": str(request.dataset_root),
                "tf": request.tf,
                "quote": request.quote,
                "top_n": request.top_n,
                "start_ts_ms": request.start_ts_ms,
                "end_ts_ms": request.end_ts_ms,
                "markets": list(request.markets),
                "batch_rows": request.batch_rows,
            }
        ),
    }


def _scan_market_frame(
    *,
    request: DatasetRequest,
    market: str,
    feature_columns: tuple[str, ...],
    y_cls_column: str,
    y_reg_column: str,
    y_rank_column: str,
) -> pl.DataFrame:
    market_files = _market_files(request.dataset_root, request.tf, market)
    if not market_files:
        return pl.DataFrame()

    lazy = pl.scan_parquet([str(path) for path in market_files])
    schema = lazy.collect_schema()
    names = set(schema.names())

    required_missing = [name for name in ("ts_ms", y_cls_column) if name not in names]
    if required_missing:
        raise ValueError(f"missing required columns in {market}: {required_missing}")

    expressions: list[pl.Expr] = [pl.col("ts_ms").cast(pl.Int64).alias("ts_ms")]
    for col in feature_columns:
        if col not in names:
            raise ValueError(f"feature column missing in {market}: {col}")
        expressions.append(_feature_to_float_expr(col, schema=schema))
    expressions.append(pl.col(y_cls_column).cast(pl.Int8).alias("y_cls"))
    if y_reg_column in names:
        expressions.append(pl.col(y_reg_column).cast(pl.Float32).alias("y_reg"))
    else:
        expressions.append(pl.lit(np.nan, dtype=pl.Float32).alias("y_reg"))
    if y_rank_column in names:
        expressions.append(pl.col(y_rank_column).cast(pl.Float32).alias("y_rank"))
    else:
        expressions.append(pl.lit(np.nan, dtype=pl.Float32).alias("y_rank"))
    if "sample_weight" in names:
        expressions.append(pl.col("sample_weight").cast(pl.Float32).fill_null(1.0).alias("sample_weight"))
    else:
        expressions.append(pl.lit(1.0, dtype=pl.Float32).alias("sample_weight"))

    selected = lazy.select(expressions)
    if request.start_ts_ms is not None:
        selected = selected.filter(pl.col("ts_ms") >= int(request.start_ts_ms))
    if request.end_ts_ms is not None:
        selected = selected.filter(pl.col("ts_ms") <= int(request.end_ts_ms))
    selected = selected.sort("ts_ms")
    return _collect_lazy(selected)


def _scan_market_rows(
    *,
    request: DatasetRequest,
    market: str,
    feature_columns: tuple[str, ...],
    extra_columns: tuple[str, ...],
) -> pl.DataFrame:
    market_files = _market_files(request.dataset_root, request.tf, market)
    if not market_files:
        return pl.DataFrame()

    lazy = pl.scan_parquet([str(path) for path in market_files])
    schema = lazy.collect_schema()
    names = set(schema.names())

    required_missing = [name for name in ("ts_ms",) if name not in names]
    if required_missing:
        raise ValueError(f"missing required columns in {market}: {required_missing}")

    expressions: list[pl.Expr] = [pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"), pl.lit(market).alias("market")]
    for col in feature_columns:
        if col not in names:
            raise ValueError(f"feature column missing in {market}: {col}")
        expressions.append(_feature_to_float_expr(col, schema=schema))
    for col in extra_columns:
        if col in names:
            expressions.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
        else:
            expressions.append(pl.lit(None, dtype=pl.Float64).alias(col))

    selected = lazy.select(expressions)
    if request.start_ts_ms is not None:
        selected = selected.filter(pl.col("ts_ms") >= int(request.start_ts_ms))
    if request.end_ts_ms is not None:
        selected = selected.filter(pl.col("ts_ms") <= int(request.end_ts_ms))
    selected = selected.sort("ts_ms")
    return _collect_lazy(selected)


def _scan_market_aux_frame(
    *,
    request: DatasetRequest,
    market: str,
    columns: tuple[str, ...],
    y_cls_column: str,
    y_reg_column: str,
    y_rank_column: str,
) -> pl.DataFrame:
    market_files = _market_files(request.dataset_root, request.tf, market)
    if not market_files:
        return pl.DataFrame()

    lazy = pl.scan_parquet([str(path) for path in market_files])
    schema = lazy.collect_schema()
    names = set(schema.names())
    required_missing = [name for name in ("ts_ms", y_cls_column) if name not in names]
    if required_missing:
        raise ValueError(f"missing required columns in {market}: {required_missing}")

    expressions: list[pl.Expr] = [
        pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
        pl.lit(market).alias("market"),
        pl.col(y_cls_column).cast(pl.Int8).alias("y_cls"),
    ]
    if y_reg_column in names:
        expressions.append(pl.col(y_reg_column).cast(pl.Float32).alias("y_reg"))
    else:
        expressions.append(pl.lit(np.nan, dtype=pl.Float32).alias("y_reg"))
    if y_rank_column in names:
        expressions.append(pl.col(y_rank_column).cast(pl.Float32).alias("y_rank"))
    else:
        expressions.append(pl.lit(np.nan, dtype=pl.Float32).alias("y_rank"))
    if "sample_weight" in names:
        expressions.append(pl.col("sample_weight").cast(pl.Float32).fill_null(1.0).alias("sample_weight"))
    else:
        expressions.append(pl.lit(1.0, dtype=pl.Float32).alias("sample_weight"))

    seen: set[str] = set()
    for col in columns:
        if col in seen:
            continue
        seen.add(col)
        if col in names:
            dtype = schema.get(col)
            if dtype == pl.Boolean:
                expressions.append(pl.col(col).cast(pl.Int8).cast(pl.Float64).alias(col))
            else:
                expressions.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
        else:
            expressions.append(pl.lit(None, dtype=pl.Float64).alias(col))

    selected = lazy.select(expressions)
    if request.start_ts_ms is not None:
        selected = selected.filter(pl.col("ts_ms") >= int(request.start_ts_ms))
    if request.end_ts_ms is not None:
        selected = selected.filter(pl.col("ts_ms") <= int(request.end_ts_ms))
    return _collect_lazy(selected.sort("ts_ms"))


def _market_files(dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []
    files = sorted(market_dir.glob("part-*.parquet"))
    if files:
        return files
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        nested.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
    if nested:
        return nested
    return []


def _feature_to_float_expr(column: str, *, schema: Any) -> pl.Expr:
    dtype = schema.get(column)
    if dtype == pl.Boolean:
        return pl.col(column).cast(pl.Int8).cast(pl.Float32).alias(column)
    if dtype in {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    }:
        return pl.col(column).cast(pl.Float32).alias(column)
    if column == "m_trade_source":
        source = pl.col(column).cast(pl.Utf8).str.to_lowercase()
        return (
            pl.when(source == "ws")
            .then(2.0)
            .when(source == "rest")
            .then(1.0)
            .otherwise(0.0)
            .cast(pl.Float32)
            .alias(column)
        )
    return pl.col(column).cast(pl.Float32, strict=False).alias(column)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
