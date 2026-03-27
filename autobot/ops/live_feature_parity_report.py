"""Sampled offline/live feature parity report for features_v4."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import polars as pl

from autobot.features.pipeline_v4 import _load_feature_market
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
from autobot.strategy.micro_snapshot import OfflineMicroSnapshotProvider


LIVE_FEATURE_PARITY_REPORT_VERSION = 1
DEFAULT_REPORT_REL_PATH = Path("data") / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"


def build_live_feature_parity_report(
    *,
    project_root: str | Path,
    feature_set: str = "v4",
    tf: str = "5m",
    quote: str = "KRW",
    top_n: int = 20,
    samples_per_market: int = 1,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    dataset_root = root / "data" / "features" / f"features_{str(feature_set).strip().lower() or 'v4'}"
    meta_root = dataset_root / "_meta"
    feature_spec = _load_json(meta_root / "feature_spec.json")
    manifest = _load_manifest(meta_root / "manifest.parquet")
    feature_columns = tuple(
        str(item).strip()
        for item in (feature_spec.get("feature_columns") or [])
        if str(item).strip()
    )
    tf_value = str(tf).strip().lower() or "5m"
    quote_value = str(quote).strip().upper() or "KRW"
    sampling_window = _resolve_sampling_window(meta_root)
    context_markets = _select_markets(
        manifest=manifest,
        tf=tf_value,
        quote=quote_value,
        top_n=None,
    )
    selected_markets = _select_markets(
        manifest=manifest,
        tf=tf_value,
        quote=quote_value,
        top_n=top_n,
    )
    provider = _build_live_provider(
        root=root,
        feature_spec=feature_spec,
        feature_columns=feature_columns,
        tf=tf_value,
        quote=quote_value,
    )
    offline_market_frames: dict[str, pl.DataFrame] = {}
    sampled_rows_by_ts: dict[int, list[dict[str, Any]]] = {}
    rows: list[dict[str, Any]] = []
    mismatch_counts: dict[str, int] = {}
    missing_column_total = 0
    max_abs_diff_overall = 0.0
    compared_pairs = 0
    passing_pairs = 0
    hard_gate_fail_count = 0
    sampled_pairs = 0

    for market in selected_markets:
        offline_frame = _load_feature_market_window(
            dataset_root=dataset_root,
            tf=tf_value,
            market=market,
            sampling_window=sampling_window,
        )
        if offline_frame.height <= 0:
            continue
        offline_market_frames[market] = offline_frame
        sample_rows = offline_frame.tail(max(int(samples_per_market), 1)).to_dicts()
        for offline_row in sample_rows:
            ts_value = int(offline_row.get("ts_ms") or 0)
            sampled_rows_by_ts.setdefault(ts_value, []).append(dict(offline_row))

    for ts_value, offline_rows in sorted(sampled_rows_by_ts.items()):
        provider = _build_live_provider(
            root=root,
            feature_spec=feature_spec,
            feature_columns=feature_columns,
            tf=tf_value,
            quote=quote_value,
            bootstrap_end_ts_ms=ts_value,
        )
        live_frame = provider.build_frame(ts_ms=ts_value, markets=context_markets)
        live_stats = dict(provider.last_build_stats())
        missing_columns = list(live_stats.get("missing_feature_columns") or [])
        hard_gate_triggered = bool(live_stats.get("hard_gate_triggered", False))
        if hard_gate_triggered:
            hard_gate_fail_count += len(offline_rows)
        missing_column_total += len(missing_columns) * len(offline_rows)
        live_rows_by_market = {
            str(row.get("market") or "").strip().upper(): row
            for row in live_frame.to_dicts()
            if str(row.get("market") or "").strip()
        }
        compare_columns = ("ts_ms", "market", "close", *feature_columns)
        for offline_row in offline_rows:
            sampled_pairs += 1
            market = str(offline_row.get("market") or "").strip().upper()
            live_row = live_rows_by_market.get(market, {})
            comparison = _compare_rows(
                offline_row=offline_row,
                live_row=live_row,
                columns=compare_columns,
                tolerance=tolerance,
            )
            compared_pairs += 1 if comparison["compared_column_count"] > 0 else 0
            if comparison["pass"] and not hard_gate_triggered:
                passing_pairs += 1
            for column in comparison["mismatched_columns"]:
                mismatch_counts[column] = int(mismatch_counts.get(column, 0)) + 1
            max_abs_diff_overall = max(max_abs_diff_overall, float(comparison["max_abs_diff"]))
            rows.append(
                {
                    "market": market,
                    "ts_ms": ts_value,
                    "offline_row_hash": comparison["offline_row_hash"],
                    "live_row_hash": comparison["live_row_hash"],
                    "missing_feature_columns": missing_columns,
                    "hard_gate_triggered": hard_gate_triggered,
                    "compared_column_count": comparison["compared_column_count"],
                    "mismatched_columns": comparison["mismatched_columns"],
                    "max_abs_diff": comparison["max_abs_diff"],
                    "pass": bool(comparison["pass"] and not hard_gate_triggered),
                    "built_market_count": int(live_frame.height),
                    "built_market_samples": list(live_stats.get("built_market_samples") or []),
                    "skipped_market_samples": list(live_stats.get("skipped_market_samples") or []),
                }
            )

    acceptable = (
        sampled_pairs > 0
        and compared_pairs == sampled_pairs
        and hard_gate_fail_count == 0
        and missing_column_total == 0
        and passing_pairs == sampled_pairs
    )
    return {
        "artifact_version": LIVE_FEATURE_PARITY_REPORT_VERSION,
        "policy": "live_feature_parity_report_v1",
        "feature_set": str(feature_set).strip().lower() or "v4",
        "tf": tf_value,
        "quote": quote_value,
        "dataset_root": str(dataset_root),
        "report_path_default": str((root / DEFAULT_REPORT_REL_PATH).resolve()),
        "sampling_window": sampling_window,
        "sampled_pairs": sampled_pairs,
        "compared_pairs": compared_pairs,
        "passing_pairs": passing_pairs,
        "hard_gate_fail_count": hard_gate_fail_count,
        "missing_feature_columns_total": missing_column_total,
        "column_mismatch_counts": dict(sorted(mismatch_counts.items())),
        "max_abs_diff_overall": float(max_abs_diff_overall),
        "acceptable": bool(acceptable),
        "status": "PASS" if acceptable else "FAIL",
        "details": rows,
    }


def write_live_feature_parity_report(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
    feature_set: str = "v4",
    tf: str = "5m",
    quote: str = "KRW",
    top_n: int = 20,
    samples_per_market: int = 1,
    tolerance: float = 1e-6,
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_REPORT_REL_PATH)
    payload = build_live_feature_parity_report(
        project_root=root,
        feature_set=feature_set,
        tf=tf,
        quote=quote,
        top_n=top_n,
        samples_per_market=samples_per_market,
        tolerance=tolerance,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build_live_provider(
    *,
    root: Path,
    feature_spec: dict[str, Any],
    feature_columns: tuple[str, ...],
    tf: str,
    quote: str,
    bootstrap_end_ts_ms: int | None = None,
) -> LiveFeatureProviderV4:
    base_candles_root = _resolve_root(
        root=root,
        value=str(feature_spec.get("base_candles_root") or "data/parquet/candles_api_v1"),
    )
    micro_root = _resolve_root(
        root=root,
        value=str(feature_spec.get("micro_root") or "data/parquet/micro_v1"),
    )
    raw_ws_root = root / "data" / "raw_ws" / "upbit" / "public"
    micro_snapshot_provider = None
    if micro_root.exists():
        micro_snapshot_provider = OfflineMicroSnapshotProvider(
            micro_root=micro_root,
            tf=tf,
            raw_ws_root=(raw_ws_root if raw_ws_root.exists() else None),
        )
    return LiveFeatureProviderV4(
        feature_columns=feature_columns,
        tf=tf,
        quote=quote,
        parquet_root=base_candles_root.parent,
        candles_dataset_name=base_candles_root.name,
        micro_snapshot_provider=micro_snapshot_provider,
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=bootstrap_end_ts_ms,
        context_micro_required=True,
    )


def _select_markets(
    *,
    manifest: pl.DataFrame,
    tf: str,
    quote: str,
    top_n: int | None,
) -> list[str]:
    if manifest.height <= 0 or "market" not in manifest.columns:
        return []
    working = manifest
    if "tf" in working.columns:
        working = working.filter(pl.col("tf") == pl.lit(tf))
    markets = [
        str(value).strip().upper()
        for value in working.get_column("market").drop_nulls().unique(maintain_order=True).to_list()
        if str(value).strip().upper().startswith(f"{quote}-")
    ]
    if top_n is None:
        return markets
    return markets[: max(int(top_n), 1)]


def _load_feature_market_window(
    *,
    dataset_root: Path,
    tf: str,
    market: str,
    sampling_window: dict[str, Any],
) -> pl.DataFrame:
    start_date = str(sampling_window.get("effective_start") or "").strip()
    end_date = str(sampling_window.get("effective_end") or "").strip()
    market_dir = dataset_root / f"tf={str(tf).strip().lower()}" / f"market={str(market).strip().upper()}"
    files: list[Path] = []
    if market_dir.exists():
        for date_dir in sorted(market_dir.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_value = str(date_dir.name).split("=", 1)[-1].strip()
            if start_date and date_value < start_date:
                continue
            if end_date and date_value > end_date:
                continue
            files.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
        if not files:
            files = sorted(path for path in market_dir.rglob("*.parquet") if path.is_file())
    if not files:
        return pl.DataFrame()
    try:
        lazy = pl.scan_parquet([str(path) for path in files], extra_columns="ignore")
    except TypeError:
        lazy = pl.scan_parquet([str(path) for path in files])
    frame = lazy.collect()
    if "ts_ms" not in frame.columns:
        return pl.DataFrame()
    if start_date or end_date:
        start_ts_ms = _date_start_ts_ms(start_date) if start_date else None
        end_ts_ms = _date_end_ts_ms(end_date) if end_date else None
        if start_ts_ms is not None:
            frame = frame.filter(pl.col("ts_ms") >= pl.lit(start_ts_ms))
        if end_ts_ms is not None:
            frame = frame.filter(pl.col("ts_ms") <= pl.lit(end_ts_ms))
    return frame.sort("ts_ms").unique(subset=["ts_ms"], keep="last", maintain_order=True)


def _resolve_sampling_window(meta_root: Path) -> dict[str, Any]:
    build_report = _load_json(meta_root / "build_report.json")
    effective_start = str(
        build_report.get("effective_start")
        or build_report.get("requested_start")
        or ""
    ).strip()
    effective_end = str(
        build_report.get("effective_end")
        or build_report.get("requested_end")
        or ""
    ).strip()
    return {
        "effective_start": effective_start or None,
        "effective_end": effective_end or None,
    }


def _compare_rows(
    *,
    offline_row: dict[str, Any],
    live_row: dict[str, Any],
    columns: tuple[str, ...],
    tolerance: float,
) -> dict[str, Any]:
    offline_payload = {column: _normalize_value(offline_row.get(column)) for column in columns if column in offline_row}
    live_payload = {column: _normalize_value(live_row.get(column)) for column in columns if column in live_row}
    mismatched_columns: list[str] = []
    max_abs_diff = 0.0
    compared_column_count = 0
    for column in columns:
        if column not in offline_payload or column not in live_payload:
            mismatched_columns.append(str(column))
            continue
        compared_column_count += 1
        left = _normalize_column_value(str(column), offline_payload[column])
        right = _normalize_column_value(str(column), live_payload[column])
        if isinstance(left, float) and isinstance(right, float):
            if not math.isclose(left, right, rel_tol=0.0, abs_tol=float(tolerance)):
                mismatched_columns.append(str(column))
                max_abs_diff = max(max_abs_diff, abs(left - right))
        elif left != right:
            mismatched_columns.append(str(column))
    return {
        "offline_row_hash": _row_hash(offline_payload),
        "live_row_hash": _row_hash(live_payload),
        "compared_column_count": compared_column_count,
        "mismatched_columns": mismatched_columns,
        "max_abs_diff": float(max_abs_diff),
        "pass": compared_column_count > 0 and not mismatched_columns,
    }


def _normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            value = str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return round(float(value), 8)
    if isinstance(value, (int, str, bool)):
        return value
    return str(value)


def _normalize_column_value(column: str, value: Any) -> Any:
    name = str(column).strip()
    if name == "m_trade_source":
        text = str(value or "").strip().lower()
        if not text:
            return 0.0
        if text == "ws":
            return 2.0
        if text == "rest":
            return 1.0
        if text == "none":
            return 0.0
    return value


def _date_start_ts_ms(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        return None


def _date_end_ts_ms(value: str) -> int | None:
    start = _date_start_ts_ms(value)
    if start is None:
        return None
    return int(start + 86_400_000 - 1)


def _row_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _resolve_root(*, root: Path, value: str) -> Path:
    candidate = Path(str(value).strip())
    if candidate.is_absolute():
        return candidate
    return (root / candidate).resolve()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    try:
        return pl.read_parquet(path)
    except Exception:
        return pl.DataFrame()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build sampled offline/live feature parity report.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--feature-set", default="v4")
    parser.add_argument("--tf", default="5m")
    parser.add_argument("--quote", default="KRW")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--samples-per-market", type=int, default=1)
    parser.add_argument("--tolerance", type=float, default=1e-6)
    parser.add_argument("--out", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    output_path = Path(str(args.out)).resolve() if str(args.out).strip() else None
    path = write_live_feature_parity_report(
        project_root=Path(str(args.project_root)),
        output_path=output_path,
        feature_set=str(args.feature_set).strip().lower() or "v4",
        tf=str(args.tf).strip().lower() or "5m",
        quote=str(args.quote).strip().upper() or "KRW",
        top_n=max(int(args.top_n), 1),
        samples_per_market=max(int(args.samples_per_market), 1),
        tolerance=max(float(args.tolerance), 0.0),
    )
    print(f"[ops][live-feature-parity] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
