from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.data.manifest import append_manifest_rows, load_manifest, normalize_manifest_rows


def test_normalize_manifest_rows_coerces_types_and_reasons_json() -> None:
    frame = normalize_manifest_rows(
        [
            {
                "quote": "KRW",
                "market": "KRW-BTC",
                "source_csv_size": "123",
                "source_csv_mtime": "456",
                "rows": "10",
                "duplicates_dropped": "1",
                "non_monotonic_found": "true",
                "gaps_found": "2",
                "invalid_rows_dropped": "oops",
                "ohlc_violations": "OHLC consistency violations: 1",
                "status": "warn",
                "status_reasons": ["DUPLICATES_DROPPED"],
            }
        ]
    )

    row = frame.to_dicts()[0]
    assert row["source_csv_size"] == 123
    assert row["non_monotonic_found"] is True
    assert row["invalid_rows_dropped"] is None
    assert row["ohlc_violations"] is None
    assert row["status"] == "WARN"
    assert json.loads(row["reasons_json"]) == ["DUPLICATES_DROPPED"]


def test_append_manifest_rows_uses_fixed_schema(tmp_path: Path) -> None:
    manifest_file = tmp_path / "_meta" / "manifest.parquet"
    append_manifest_rows(
        manifest_file,
        [
            {
                "source_csv_relpath": "a.csv",
                "source_csv_size": 100,
                "source_csv_mtime": 1,
                "ingested_at": 10,
                "rows": 5,
                "status": "OK",
                "reasons_json": json.dumps(["GAPS_FOUND"]),
            },
            {
                "source_csv_relpath": "b.csv",
                "source_csv_size": 120,
                "source_csv_mtime": 2,
                "ingested_at": 11,
                "rows": "6",
                "status": "FAIL",
                "error_message": "OHLC consistency violations: 1",
            },
        ],
    )

    manifest = load_manifest(manifest_file)
    assert manifest.height == 2
    assert manifest.schema["rows"] == pl.Int64
    assert manifest.schema["non_monotonic_found"] == pl.Boolean
    assert manifest.schema["reasons_json"] == pl.Utf8
    assert set(manifest["status"].to_list()) == {"OK", "FAIL"}

