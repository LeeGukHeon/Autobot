"""Builder for canonical raw_trade_v1 from WS-primary and REST-repair inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any

import polars as pl

from ...micro.raw_readers import (
    discover_rest_tick_files,
    discover_ws_files,
    iter_jsonl_zst_rows,
    parse_date_range,
)
from .manifest import (
    append_raw_trade_manifest_rows,
    load_raw_trade_manifest,
    manifest_path,
    save_raw_trade_manifest,
)
from .raw_trade_v1 import (
    merge_canonical_trade_rows,
    normalize_rest_trade_row,
    normalize_ws_trade_row,
)
from .writer import write_raw_trade_partitions


@dataclass(frozen=True)
class RawTradeBuildOptions:
    raw_ws_root: Path = Path("data/raw_ws/upbit/public")
    raw_ticks_root: Path = Path("data/raw_ticks/upbit/trades")
    out_root: Path = Path("data/raw_trade_v1")
    meta_dir: Path = Path("data/raw_trade_v1/_meta")
    start: str = ""
    end: str = ""
    markets: tuple[str, ...] | None = None
    prefer_source_order: tuple[str, ...] = ("ws", "rest")

    @property
    def build_report_path(self) -> Path:
        return self.meta_dir / "build_report.json"


@dataclass(frozen=True)
class RawTradeBuildSummary:
    run_id: str
    dates: tuple[str, ...]
    selected_markets: tuple[str, ...]
    built_pairs: int
    skipped_pairs: int
    ws_rows_total: int
    rest_rows_total: int
    merged_rows_total: int
    build_report_file: Path
    manifest_file: Path


def build_raw_trade_v1_dataset(options: RawTradeBuildOptions) -> RawTradeBuildSummary:
    dates = parse_date_range(start=str(options.start).strip(), end=str(options.end).strip())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    selected_markets = tuple(
        str(market).strip().upper()
        for market in (options.markets or ())
        if str(market).strip()
    )
    market_filter = set(selected_markets) if selected_markets else None

    manifest_rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    built_pairs = 0
    skipped_pairs = 0
    ws_rows_total = 0
    rest_rows_total = 0
    merged_rows_total = 0
    rebuilt_pairs: list[tuple[str, str]] = []

    for date_value in dates:
        ws_rows_by_market = _load_ws_rows_by_market(
            raw_ws_root=options.raw_ws_root,
            date_value=date_value,
            market_filter=market_filter,
        )
        rest_rows_by_market = _load_rest_rows_by_market(
            raw_ticks_root=options.raw_ticks_root,
            date_value=date_value,
            market_filter=market_filter,
        )
        candidate_markets = sorted(set(ws_rows_by_market) | set(rest_rows_by_market) | set(selected_markets))
        for market in candidate_markets:
            ws_rows = ws_rows_by_market.get(market, [])
            rest_rows = rest_rows_by_market.get(market, [])
            merged_rows = merge_canonical_trade_rows(
                ws_rows,
                rest_rows,
                prefer_source_order=tuple(options.prefer_source_order),
            )
            ws_rows_total += len(ws_rows)
            rest_rows_total += len(rest_rows)
            merged_rows_total += len(merged_rows)

            reasons: list[str] = []
            status = "OK"
            if not ws_rows:
                reasons.append("WS_SOURCE_MISSING")
            if not rest_rows:
                reasons.append("REST_SOURCE_MISSING")
            if not merged_rows:
                status = "WARN"
                reasons.append("NO_CANONICAL_ROWS")

            detail = {
                "date": date_value,
                "market": market,
                "source_ws_rows": len(ws_rows),
                "source_rest_rows": len(rest_rows),
                "source_merged_rows": len(merged_rows),
                "status": status,
                "reasons": list(reasons),
            }
            if not merged_rows:
                skipped_pairs += 1
                details.append(detail)
                continue

            _remove_existing_trade_pair(
                out_root=options.out_root,
                date_value=date_value,
                market=market,
            )
            rebuilt_pairs.append((date_value, market))
            written_parts = write_raw_trade_partitions(
                out_root=options.out_root,
                trades=merged_rows,
                run_id=f"{run_id}-{date_value}-{market}",
            )
            built_pairs += 1
            detail["written_parts"] = [str(item.get("part_file")) for item in written_parts]
            detail["min_ts_ms"] = min(int(item["min_ts_ms"]) for item in written_parts)
            detail["max_ts_ms"] = max(int(item["max_ts_ms"]) for item in written_parts)
            details.append(detail)

            built_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            for part in written_parts:
                manifest_rows.append(
                    {
                        "run_id": run_id,
                        "date": date_value,
                        "market": market,
                        "rows": int(part.get("rows", 0)),
                        "min_ts_ms": int(part.get("min_ts_ms", 0)),
                        "max_ts_ms": int(part.get("max_ts_ms", 0)),
                        "source_ws_rows": len(ws_rows),
                        "source_rest_rows": len(rest_rows),
                        "source_merged_rows": len(merged_rows),
                        "status": status,
                        "reasons_json": json.dumps(reasons, ensure_ascii=False),
                        "part_file": str(part.get("part_file") or ""),
                        "built_at_ms": built_at_ms,
                    }
                )

    manifest_file = manifest_path(options.out_root)
    _replace_existing_manifest_pairs(
        manifest_file=manifest_file,
        rebuilt_pairs=rebuilt_pairs,
    )
    append_raw_trade_manifest_rows(manifest_file, manifest_rows)

    report = {
        "policy": "raw_trade_v1_build_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "raw_ws_root": str(options.raw_ws_root),
        "raw_ticks_root": str(options.raw_ticks_root),
        "out_root": str(options.out_root),
        "dates": list(dates),
        "selected_markets": list(selected_markets),
        "prefer_source_order": list(options.prefer_source_order),
        "built_pairs": built_pairs,
        "skipped_pairs": skipped_pairs,
        "ws_rows_total": ws_rows_total,
        "rest_rows_total": rest_rows_total,
        "merged_rows_total": merged_rows_total,
        "manifest_file": str(manifest_file),
        "details": details,
    }
    options.build_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.build_report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return RawTradeBuildSummary(
        run_id=run_id,
        dates=tuple(dates),
        selected_markets=selected_markets,
        built_pairs=built_pairs,
        skipped_pairs=skipped_pairs,
        ws_rows_total=ws_rows_total,
        rest_rows_total=rest_rows_total,
        merged_rows_total=merged_rows_total,
        build_report_file=options.build_report_path,
        manifest_file=manifest_file,
    )


def _load_ws_rows_by_market(
    *,
    raw_ws_root: Path,
    date_value: str,
    market_filter: set[str] | None,
) -> dict[str, list[dict[str, Any]]]:
    rows_by_market: dict[str, list[dict[str, Any]]] = {}
    for path in discover_ws_files(raw_ws_root=raw_ws_root, channel="trade", date_value=date_value):
        for row in iter_jsonl_zst_rows(path):
            normalized = normalize_ws_trade_row(row)
            if normalized is None:
                continue
            market = str(normalized["market"])
            if market_filter and market not in market_filter:
                continue
            rows_by_market.setdefault(market, []).append(normalized)
    return rows_by_market


def _load_rest_rows_by_market(
    *,
    raw_ticks_root: Path,
    date_value: str,
    market_filter: set[str] | None,
) -> dict[str, list[dict[str, Any]]]:
    rows_by_market: dict[str, list[dict[str, Any]]] = {}
    for path in discover_rest_tick_files(
        raw_ticks_root=raw_ticks_root,
        date_value=date_value,
        markets=market_filter,
    ):
        for row in iter_jsonl_zst_rows(path):
            normalized = normalize_rest_trade_row(row)
            if normalized is None:
                continue
            market = str(normalized["market"])
            if market_filter and market not in market_filter:
                continue
            rows_by_market.setdefault(market, []).append(normalized)
    return rows_by_market


def _remove_existing_trade_pair(*, out_root: Path, date_value: str, market: str) -> None:
    pair_root = Path(out_root) / f"date={date_value}" / f"market={market}"
    if pair_root.exists():
        shutil.rmtree(pair_root, ignore_errors=True)


def _replace_existing_manifest_pairs(
    *,
    manifest_file: Path,
    rebuilt_pairs: list[tuple[str, str]],
) -> None:
    if not rebuilt_pairs or not manifest_file.exists():
        return
    frame = load_raw_trade_manifest(manifest_file)
    if frame.height <= 0:
        return
    rebuilt_keys = {(str(date_value), str(market).upper()) for date_value, market in rebuilt_pairs}
    filtered = frame.filter(
        ~(
            pl.struct(["date", "market"]).map_elements(
                lambda item: (str(item["date"]), str(item["market"]).upper()) in rebuilt_keys,
                return_dtype=pl.Boolean,
            )
        )
    )
    save_raw_trade_manifest(manifest_file, filtered)
