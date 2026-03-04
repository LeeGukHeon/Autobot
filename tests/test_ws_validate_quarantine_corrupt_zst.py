from __future__ import annotations

import json
from pathlib import Path

from autobot.data.collect.validate_ws_public import validate_ws_public_raw_dataset


def test_ws_validate_quarantine_corrupt_zst_file(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "public"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)

    bad_file = (
        raw_root
        / "trade"
        / "date=2026-03-04"
        / "hour=07"
        / "part-20260304T071436Z-000001.jsonl.zst"
    )
    bad_file.parent.mkdir(parents=True, exist_ok=True)
    bad_file.write_bytes(b"this-is-not-a-zstd-frame")

    summary = validate_ws_public_raw_dataset(
        raw_root=raw_root,
        meta_dir=meta_dir,
        report_path=meta_dir / "ws_validate_report.json",
        date_filter="2026-03-04",
        quarantine_corrupt=True,
        quarantine_dir=tmp_path / "raw_ws" / "upbit" / "_quarantine",
        min_age_sec=0,
    )

    assert summary.checked_files == 1
    assert summary.fail_files == 0
    assert summary.quarantined_files == 1
    assert not bad_file.exists()

    quarantine_report = json.loads((meta_dir / "ws_quarantine_report.json").read_text(encoding="utf-8"))
    entries = quarantine_report.get("entries")
    assert isinstance(entries, list)
    assert len(entries) == 1
    assert entries[0]["reason"] == "ZSTD_CORRUPT"
    assert Path(entries[0]["new_path"]).exists()

