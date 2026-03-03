"""Universe selection helpers for backtest runtime."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass
class StaticUniverseProvider:
    _markets: list[str]

    def update_if_needed(self, ts_ms: int) -> bool:
        _ = ts_ms
        return False

    def markets(self) -> list[str]:
        return list(self._markets)


def build_static_start_universe(
    *,
    parquet_root: str | Path,
    dataset_name: str,
    tf: str,
    quote: str,
    top_n: int,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
) -> list[str]:
    root = Path(parquet_root)
    pattern = _duckdb_glob(root=root, dataset_name=dataset_name)
    quote_prefix = f"{str(quote).strip().upper()}-%"
    tf_value = str(tf).strip().lower()
    top_n_value = max(int(top_n), 1)

    with duckdb.connect(database=":memory:") as connection:
        start_frame = connection.execute(
            """
            SELECT MIN(ts_ms) AS min_ts_ms
            FROM read_parquet(?, hive_partitioning=1)
            WHERE tf = ? AND market LIKE ?
            """,
            [pattern, tf_value, quote_prefix],
        ).fetchone()

        if start_frame is None or start_frame[0] is None:
            return []

        start_ts_ms = int(from_ts_ms if from_ts_ms is not None else start_frame[0])
        end_ts_ms = int(to_ts_ms if to_ts_ms is not None else start_ts_ms + 86_400_000 - 1)
        if end_ts_ms < start_ts_ms:
            end_ts_ms = start_ts_ms

        rows = connection.execute(
            """
            SELECT market, SUM(COALESCE(volume_quote, close * volume_base)) AS trade_value
            FROM read_parquet(?, hive_partitioning=1)
            WHERE tf = ?
              AND market LIKE ?
              AND ts_ms >= ?
              AND ts_ms <= ?
            GROUP BY market
            ORDER BY trade_value DESC, market ASC
            LIMIT ?
            """,
            [pattern, tf_value, quote_prefix, start_ts_ms, end_ts_ms, top_n_value],
        ).fetchall()

    markets = [str(item[0]).strip().upper() for item in rows if item and item[0]]
    deduped = sorted(set(markets), key=markets.index)
    return deduped


def _duckdb_glob(*, root: Path, dataset_name: str) -> str:
    glob_path = (root / dataset_name / "tf=*" / "market=*" / "*.parquet").resolve()
    return str(glob_path).replace("\\", "/")
