from __future__ import annotations

import csv
from pathlib import Path

import polars as pl

from autobot.data.duckdb_utils import DuckDBSettings
from autobot.data.ingest_csv_to_parquet import IngestOptions, ingest_dataset


def test_ingest_dataset_creates_partitioned_parquet_and_manifest(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    out_dir = tmp_path / "parquet"
    raw_dir.mkdir(parents=True, exist_ok=True)

    _write_sample_csv(
        raw_dir / "upbit_KRW_ABC_1m_full.csv",
        [
            [1_700_000_000_000, 10, 11, 9, 10.5, 100],
            [1_700_000_060_000, 10.5, 12, 10, 11.5, 120],
            [1_700_000_120_000, 11.5, 13, 11, 12.5, 140],
        ],
    )
    _write_sample_csv(
        raw_dir / "upbit_KRW_ABC_5m_full.csv",
        [
            [1_700_000_000_000, 20, 21, 19, 20.5, 200],
            [1_700_000_300_000, 20.5, 22, 20, 21.5, 210],
            [1_700_000_600_000, 21.5, 23, 21, 22.5, 220],
        ],
    )
    _write_sample_csv(
        raw_dir / "upbit_KRW_XYZ_15m_full.csv",
        [
            [1_700_000_000_000, 30, 31, 29, 30.5, 300],
            [1_700_000_900_000, 30.5, 32, 30, 31.5, 310],
            [1_700_001_800_000, 31.5, 33, 31, 32.5, 320],
        ],
    )

    options = IngestOptions(
        raw_dir=raw_dir,
        out_dir=out_dir,
        mode="overwrite",
        workers=1,
        engine="duckdb",
        duckdb=DuckDBSettings(temp_directory=str(tmp_path / "duckdb_tmp")),
    )

    summary = ingest_dataset(options)

    assert summary.fail_files == 0
    assert summary.processed_files == 3
    assert summary.ok_files == 3
    assert summary.manifest_file.exists()
    assert summary.report_file.exists()

    manifest = pl.read_parquet(summary.manifest_file)
    assert manifest.height == 3
    assert set(manifest["status"].to_list()) == {"OK"}
    assert manifest["rows"].to_list() == [3, 3, 3]

    one_min_part = out_dir / "candles_v1" / "tf=1m" / "market=KRW-ABC" / "part.parquet"
    assert one_min_part.exists()
    frame = pl.read_parquet(one_min_part)
    assert frame.columns == [
        "ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume_base",
        "volume_quote",
        "volume_quote_est",
    ]
    assert frame.schema["ts_ms"] == pl.Int64
    assert frame.schema["open"] == pl.Float64
    assert frame.schema["high"] == pl.Float64
    assert frame.schema["low"] == pl.Float64
    assert frame.schema["close"] == pl.Float64
    assert frame.schema["volume_base"] == pl.Float64
    assert frame.schema["volume_quote"] == pl.Float64
    assert frame.schema["volume_quote_est"] == pl.Boolean


def _write_sample_csv(path: Path, rows: list[list[float]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)
