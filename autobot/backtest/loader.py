"""Parquet candle loader for backtest runtime."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from autobot.data import expected_interval_ms

from .types import CandleBar


class CandleDataLoader:
    def __init__(
        self,
        *,
        parquet_root: str | Path,
        dataset_name: str,
        tf: str,
        dense_grid: bool = False,
    ) -> None:
        self.parquet_root = Path(parquet_root)
        self.dataset_name = str(dataset_name).strip()
        self.tf = str(tf).strip().lower()
        self.dense_grid = bool(dense_grid)
        self._dataset_dir = self.parquet_root / self.dataset_name

    @property
    def tf_dir(self) -> Path:
        return self._dataset_dir / f"tf={self.tf}"

    def list_markets(self, *, quote: str | None = None) -> list[str]:
        tf_dir = self.tf_dir
        if not tf_dir.exists():
            return []

        quote_prefix = f"{str(quote).strip().upper()}-" if quote is not None else ""
        markets: list[str] = []
        for entry in tf_dir.iterdir():
            if not entry.is_dir():
                continue
            name = entry.name
            if not name.startswith("market="):
                continue
            market = name.split("=", 1)[1].strip().upper()
            if not market:
                continue
            if quote_prefix and not market.startswith(quote_prefix):
                continue
            markets.append(market)
        markets.sort()
        return markets

    def load_market_bars(
        self,
        *,
        market: str,
        from_ts_ms: int | None = None,
        to_ts_ms: int | None = None,
    ) -> list[CandleBar]:
        frame = self.load_market_frame(market=market, from_ts_ms=from_ts_ms, to_ts_ms=to_ts_ms)
        if frame.height <= 0:
            return []

        market_value = str(market).strip().upper()
        bars = [
            CandleBar(
                market=market_value,
                ts_ms=int(row["ts_ms"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume_base=float(row["volume_base"]),
                volume_quote=(float(row["volume_quote"]) if row["volume_quote"] is not None else None),
                volume_quote_est=bool(row["volume_quote_est"]),
            )
            for row in frame.iter_rows(named=True)
        ]

        if self.dense_grid:
            return self._densify(bars=bars)
        return bars

    def load_market_frame(
        self,
        *,
        market: str,
        from_ts_ms: int | None = None,
        to_ts_ms: int | None = None,
    ) -> pl.DataFrame:
        market_value = str(market).strip().upper()
        market_dir = self.tf_dir / f"market={market_value}"
        if not market_dir.exists():
            return pl.DataFrame()

        parquet_glob = str((market_dir / "*.parquet").resolve())
        lazy = pl.scan_parquet(parquet_glob)
        if from_ts_ms is not None:
            lazy = lazy.filter(pl.col("ts_ms") >= int(from_ts_ms))
        if to_ts_ms is not None:
            lazy = lazy.filter(pl.col("ts_ms") <= int(to_ts_ms))

        selected = lazy.select(
            [
                pl.col("ts_ms").cast(pl.Int64),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume_base").cast(pl.Float64),
                pl.col("volume_quote").cast(pl.Float64),
                pl.col("volume_quote_est").cast(pl.Boolean),
            ]
        ).sort("ts_ms")
        return _collect_lazy(selected)

    def market_time_bounds(self, *, market: str) -> tuple[int, int] | None:
        market_value = str(market).strip().upper()
        market_dir = self.tf_dir / f"market={market_value}"
        if not market_dir.exists():
            return None
        parquet_glob = str((market_dir / "*.parquet").resolve())
        frame = (
            pl.scan_parquet(parquet_glob)
            .select(
                [
                    pl.col("ts_ms").cast(pl.Int64).min().alias("min_ts_ms"),
                    pl.col("ts_ms").cast(pl.Int64).max().alias("max_ts_ms"),
                ]
            )
        )
        frame = _collect_lazy(frame)
        if frame.height <= 0:
            return None
        min_ts = frame.item(row=0, column="min_ts_ms")
        max_ts = frame.item(row=0, column="max_ts_ms")
        if min_ts is None or max_ts is None:
            return None
        return (int(min_ts), int(max_ts))

    def _densify(self, *, bars: list[CandleBar]) -> list[CandleBar]:
        if len(bars) <= 1:
            return list(bars)

        interval_ms = expected_interval_ms(self.tf)
        dense: list[CandleBar] = [bars[0]]
        previous = bars[0]

        for current in bars[1:]:
            next_expected = previous.ts_ms + interval_ms
            while next_expected < current.ts_ms:
                synthetic = CandleBar(
                    market=previous.market,
                    ts_ms=next_expected,
                    open=previous.close,
                    high=previous.close,
                    low=previous.close,
                    close=previous.close,
                    volume_base=0.0,
                    volume_quote=0.0,
                    volume_quote_est=False,
                )
                dense.append(synthetic)
                previous = synthetic
                next_expected = previous.ts_ms + interval_ms

            dense.append(current)
            previous = current

        return dense


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
