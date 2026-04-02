"""Historical trade-driven candle bootstrap from raw_ws trade jsonl.zst."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .candle_manifest import append_manifest_rows, manifest_path
from .candle_writer import write_candle_partition
from ..micro.raw_readers import discover_ws_files, iter_jsonl_zst_rows, normalize_ws_trade_row, parse_date_range


@dataclass(frozen=True)
class RawWsTradeCandleBackfillOptions:
    raw_ws_root: Path = Path("data/raw_ws/upbit/public")
    parquet_root: Path = Path("data/parquet")
    start: str = ""
    end: str = ""
    quote: str = "KRW"
    markets: tuple[str, ...] = ()
    include_ws_candle_1s: bool = True
    include_ws_candle_1m: bool = True
    include_second_candle_1s: bool = True
    meta_dir: Path = Path("data/collect/_meta")

    @property
    def ws_candle_root(self) -> Path:
        return self.parquet_root / "ws_candle_v1"

    @property
    def second_candle_root(self) -> Path:
        return self.parquet_root / "candles_second_v1"

    @property
    def summary_path(self) -> Path:
        return self.meta_dir / "raw_ws_trade_candle_backfill_report.json"


@dataclass(frozen=True)
class RawWsTradeCandleBackfillSummary:
    start: str
    end: str
    dates_processed: int
    raw_files: int
    raw_rows: int
    parsed_rows: int
    filtered_rows: int
    rows_written_total: int
    market_tf_partitions_written: int
    summary_path: Path
    details: tuple[dict[str, Any], ...]


def backfill_trade_candles_from_raw_ws(options: RawWsTradeCandleBackfillOptions) -> RawWsTradeCandleBackfillSummary:
    dates = parse_date_range(start=options.start, end=options.end)
    requested_markets = {str(item).strip().upper() for item in options.markets if str(item).strip()}
    details: list[dict[str, Any]] = []
    manifest_rows_ws: list[dict[str, Any]] = []
    manifest_rows_second: list[dict[str, Any]] = []
    raw_files = 0
    raw_rows = 0
    parsed_rows = 0
    filtered_rows = 0
    rows_written_total = 0
    partitions_written = 0

    for date_value in dates:
        files = discover_ws_files(raw_ws_root=options.raw_ws_root, channel="trade", date_value=date_value)
        raw_files += len(files)
        day_payload = _aggregate_trade_files_for_date(
            files=files,
            quote=str(options.quote).strip().upper() or "KRW",
            requested_markets=requested_markets,
            include_ws_candle_1s=bool(options.include_ws_candle_1s),
            include_ws_candle_1m=bool(options.include_ws_candle_1m),
            include_second_candle_1s=bool(options.include_second_candle_1s),
        )
        raw_rows += int(day_payload["raw_rows"])
        parsed_rows += int(day_payload["parsed_rows"])
        filtered_rows += int(day_payload["filtered_rows"])

        written = _persist_trade_candle_day(
            options=options,
            date_value=date_value,
            day_payload=day_payload,
            manifest_rows_ws=manifest_rows_ws,
            manifest_rows_second=manifest_rows_second,
        )
        rows_written_total += int(written["rows_written_total"])
        partitions_written += int(written["partitions_written"])
        details.append(
            {
                "date": date_value,
                "raw_files": len(files),
                "raw_rows": int(day_payload["raw_rows"]),
                "parsed_rows": int(day_payload["parsed_rows"]),
                "filtered_rows": int(day_payload["filtered_rows"]),
                "markets_seen": int(len(day_payload["markets_seen"])),
                "rows_written_total": int(written["rows_written_total"]),
                "partitions_written": int(written["partitions_written"]),
            }
        )

    if manifest_rows_ws:
        append_manifest_rows(manifest_path(options.ws_candle_root), manifest_rows_ws)
    if manifest_rows_second:
        append_manifest_rows(manifest_path(options.second_candle_root), manifest_rows_second)
    source_run_ids = [
        item
        for item in [
            str(_load_json_or_empty(options.raw_ws_root.parent / "_meta" / "ws_public_health.json").get("run_id") or "").strip(),
            str(_load_json_or_empty(options.raw_ws_root.parent / "_meta" / "ws_collect_report.json").get("run_id") or "").strip(),
            str(_load_json_or_empty(options.raw_ws_root.parent / "_meta" / "ws_validate_report.json").get("run_id") or "").strip(),
        ]
        if item
    ]
    if rows_written_total > 0:
        build_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if manifest_rows_ws:
            _write_dataset_build_report(
                dataset_root=options.ws_candle_root,
                dataset_name="ws_candle_v1",
                run_id=build_run_id,
                collect_report_path=options.summary_path,
                source_root=options.raw_ws_root,
                source_run_ids=source_run_ids,
                rows_written_total=sum(int(item.get("rows") or 0) for item in manifest_rows_ws),
                partition_count=len(manifest_rows_ws),
            )
        if manifest_rows_second:
            _write_dataset_build_report(
                dataset_root=options.second_candle_root,
                dataset_name="candles_second_v1",
                run_id=build_run_id,
                collect_report_path=options.summary_path,
                source_root=options.raw_ws_root,
                source_run_ids=source_run_ids,
                rows_written_total=sum(int(item.get("rows") or 0) for item in manifest_rows_second),
                partition_count=len(manifest_rows_second),
            )

    report = {
        "policy": "raw_ws_trade_candle_backfill_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_ws_root": str(options.raw_ws_root),
        "parquet_root": str(options.parquet_root),
        "start": options.start,
        "end": options.end,
        "quote": options.quote,
        "requested_markets": list(options.markets),
        "include_ws_candle_1s": bool(options.include_ws_candle_1s),
        "include_ws_candle_1m": bool(options.include_ws_candle_1m),
        "include_second_candle_1s": bool(options.include_second_candle_1s),
        "dates_processed": len(dates),
        "raw_files": raw_files,
        "raw_rows": raw_rows,
        "parsed_rows": parsed_rows,
        "filtered_rows": filtered_rows,
        "rows_written_total": rows_written_total,
        "market_tf_partitions_written": partitions_written,
        "ws_manifest_path": str(manifest_path(options.ws_candle_root)),
        "second_manifest_path": str(manifest_path(options.second_candle_root)),
        "details": details,
    }
    options.summary_path.parent.mkdir(parents=True, exist_ok=True)
    options.summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return RawWsTradeCandleBackfillSummary(
        start=options.start,
        end=options.end,
        dates_processed=len(dates),
        raw_files=raw_files,
        raw_rows=raw_rows,
        parsed_rows=parsed_rows,
        filtered_rows=filtered_rows,
        rows_written_total=rows_written_total,
        market_tf_partitions_written=partitions_written,
        summary_path=options.summary_path,
        details=tuple(details),
    )


def _aggregate_trade_files_for_date(
    *,
    files: list[Path],
    quote: str,
    requested_markets: set[str],
    include_ws_candle_1s: bool,
    include_ws_candle_1m: bool,
    include_second_candle_1s: bool,
) -> dict[str, Any]:
    intervals: list[tuple[str, int, str]] = []
    if include_ws_candle_1s:
        intervals.append(("ws_candle_v1", 1_000, "1s"))
    if include_ws_candle_1m:
        intervals.append(("ws_candle_v1", 60_000, "1m"))
    if include_second_candle_1s:
        intervals.append(("candles_second_v1", 1_000, "1s"))

    buckets: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    markets_seen: set[str] = set()
    raw_rows = 0
    parsed_rows = 0
    filtered_rows = 0

    for path in files:
        for raw in iter_jsonl_zst_rows(path):
            raw_rows += 1
            normalized = normalize_ws_trade_row(raw)
            if normalized is None:
                continue
            parsed_rows += 1
            market = str(normalized["market"]).strip().upper()
            if requested_markets and market not in requested_markets:
                filtered_rows += 1
                continue
            if quote and not market.startswith(f"{quote}-"):
                filtered_rows += 1
                continue
            markets_seen.add(market)
            event_ts_ms = int(normalized["event_ts_ms"])
            price = float(normalized["price"])
            volume = float(normalized["volume"])
            for dataset_name, interval_ms, tf in intervals:
                bucket_ts_ms = _bucket_end_ts_ms(event_ts_ms=event_ts_ms, interval_ms=interval_ms)
                _update_candle_bucket(
                    buckets=buckets,
                    key=(dataset_name, tf, market, bucket_ts_ms),
                    event_ts_ms=event_ts_ms,
                    price=price,
                    volume=volume,
                )

    return {
        "raw_rows": raw_rows,
        "parsed_rows": parsed_rows,
        "filtered_rows": filtered_rows,
        "markets_seen": sorted(markets_seen),
        "buckets": buckets,
    }


def _persist_trade_candle_day(
    *,
    options: RawWsTradeCandleBackfillOptions,
    date_value: str,
    day_payload: dict[str, Any],
    manifest_rows_ws: list[dict[str, Any]],
    manifest_rows_second: list[dict[str, Any]],
) -> dict[str, int]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for (dataset_name, tf, market, _bucket_ts_ms), row in day_payload["buckets"].items():
        grouped.setdefault((dataset_name, tf, market), []).append(row)

    rows_written_total = 0
    partitions_written = 0
    window_tag = f"raw_ws_trade_backfill__{date_value}"
    collected_at = int(datetime.now(timezone.utc).timestamp())

    for (dataset_name, tf, market), candles in sorted(grouped.items()):
        candles_sorted = sorted(candles, key=lambda item: int(item["ts_ms"]))
        dataset_root = options.ws_candle_root if dataset_name == "ws_candle_v1" else options.second_candle_root
        write_result = write_candle_partition(
            dataset_root=dataset_root,
            tf=tf,
            market=market,
            candles=candles_sorted,
        )
        rows_written_total += int(write_result.get("rows", 0))
        partitions_written += 1
        manifest_row = {
            "dataset_name": dataset_name,
            "source": "raw_ws_trade_backfill",
            "window_tag": window_tag,
            "market": market,
            "tf": tf,
            "rows": int(write_result.get("rows", 0)),
            "min_ts_ms": write_result.get("min_ts_ms"),
            "max_ts_ms": write_result.get("max_ts_ms"),
            "calls_made": 0,
            "status": "OK",
            "reasons_json": json.dumps([], ensure_ascii=False),
            "error_message": None,
            "part_file": str(write_result.get("part_file", "")),
            "collected_at": collected_at,
        }
        if dataset_name == "ws_candle_v1":
            manifest_rows_ws.append(manifest_row)
        else:
            manifest_rows_second.append(manifest_row)

    return {
        "rows_written_total": rows_written_total,
        "partitions_written": partitions_written,
    }


def _update_candle_bucket(
    *,
    buckets: dict[tuple[str, str, str, int], dict[str, Any]],
    key: tuple[str, str, str, int],
    event_ts_ms: int,
    price: float,
    volume: float,
) -> None:
    bucket = buckets.get(key)
    volume_quote = float(price * volume)
    if bucket is None:
        buckets[key] = {
            "ts_ms": int(key[3]),
            "open": float(price),
            "high": float(price),
            "low": float(price),
            "close": float(price),
            "volume_base": float(volume),
            "volume_quote": float(volume_quote),
            "volume_quote_est": False,
            "__first_event_ts_ms": int(event_ts_ms),
            "__last_event_ts_ms": int(event_ts_ms),
        }
        return
    bucket["high"] = max(float(bucket["high"]), float(price))
    bucket["low"] = min(float(bucket["low"]), float(price))
    bucket["volume_base"] = float(bucket["volume_base"]) + float(volume)
    bucket["volume_quote"] = float(bucket["volume_quote"]) + float(volume_quote)
    if int(event_ts_ms) <= int(bucket["__first_event_ts_ms"]):
        bucket["open"] = float(price)
        bucket["__first_event_ts_ms"] = int(event_ts_ms)
    if int(event_ts_ms) >= int(bucket["__last_event_ts_ms"]):
        bucket["close"] = float(price)
        bucket["__last_event_ts_ms"] = int(event_ts_ms)


def _bucket_end_ts_ms(*, event_ts_ms: int, interval_ms: int) -> int:
    interval = max(int(interval_ms), 1)
    value = int(event_ts_ms)
    if value <= 0:
        return value
    if (value % interval) == 0:
        return value
    return ((value // interval) * interval) + interval


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill candle datasets from raw_ws trade history.")
    parser.add_argument("--raw-ws-root", default="data/raw_ws/upbit/public")
    parser.add_argument("--parquet-root", default="data/parquet")
    parser.add_argument("--meta-dir", default="data/collect/_meta")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--quote", default="KRW")
    parser.add_argument("--markets", default="")
    parser.add_argument("--skip-ws-candle-1s", action="store_true")
    parser.add_argument("--skip-ws-candle-1m", action="store_true")
    parser.add_argument("--skip-second-candle-1s", action="store_true")
    return parser.parse_args()


def _load_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_dataset_build_report(
    *,
    dataset_root: Path,
    dataset_name: str,
    run_id: str,
    collect_report_path: Path,
    source_root: Path,
    source_run_ids: list[str],
    rows_written_total: int,
    partition_count: int,
) -> None:
    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": str(run_id).strip(),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_name": str(dataset_name).strip(),
        "dataset_root": str(dataset_root),
        "manifest_file": str(manifest_path(dataset_root)),
        "collect_report_file": str(collect_report_path),
        "source_mode": "raw_ws_trade_backfill",
        "source_roots": [str(source_root)],
        "source_contract_ids": ["raw_ws_dataset:upbit_public"],
        "source_run_ids": list(source_run_ids),
        "summary": {
            "rows_written_total": int(rows_written_total),
            "partitions_written": int(partition_count),
        },
    }
    (meta_root / "build_report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> int:
    args = _parse_args()
    markets = tuple(str(item).strip().upper() for item in str(args.markets or "").split(",") if str(item).strip())
    summary = backfill_trade_candles_from_raw_ws(
        RawWsTradeCandleBackfillOptions(
            raw_ws_root=Path(args.raw_ws_root),
            parquet_root=Path(args.parquet_root),
            meta_dir=Path(args.meta_dir),
            start=str(args.start).strip(),
            end=str(args.end).strip(),
            quote=str(args.quote).strip().upper() or "KRW",
            markets=markets,
            include_ws_candle_1s=not bool(args.skip_ws_candle_1s),
            include_ws_candle_1m=not bool(args.skip_ws_candle_1m),
            include_second_candle_1s=not bool(args.skip_second_candle_1s),
        )
    )
    print(
        "[collect][raw-ws-trade-backfill] "
        f"dates={summary.dates_processed} raw_files={summary.raw_files} raw_rows={summary.raw_rows} "
        f"parsed_rows={summary.parsed_rows} filtered_rows={summary.filtered_rows} "
        f"rows_written_total={summary.rows_written_total} partitions={summary.market_tf_partitions_written}"
    )
    print(f"[collect][raw-ws-trade-backfill] report={summary.summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
