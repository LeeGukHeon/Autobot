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

from autobot.data import expected_interval_ms
from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
    attach_order_flow_panel_v1,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
)
from autobot.features.pipeline_v4 import _load_feature_market
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
from autobot.paper.live_features_v4_common import project_requested_v4_columns
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
    selected_markets = _select_markets(
        manifest=manifest,
        tf=tf_value,
        quote=quote_value,
        top_n=top_n,
    )
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
        sample_rows = offline_frame.tail(max(int(samples_per_market), 1)).to_dicts()
        for offline_row in sample_rows:
            ts_value = int(offline_row.get("ts_ms") or 0)
            sampled_rows_by_ts.setdefault(ts_value, []).append(dict(offline_row))

    provider: LiveFeatureProviderV4 | None = None
    live_frames_by_ts: dict[int, pl.DataFrame] = {}
    live_stats_by_ts: dict[int, dict[str, Any]] = {}
    if sampled_rows_by_ts:
        sampled_ts_values = sorted(sampled_rows_by_ts.keys())
        context_markets_by_ts = _resolve_context_markets_by_ts(
            dataset_root=dataset_root,
            tf=tf_value,
            quote=quote_value,
            sampling_window=sampling_window,
            feature_spec=feature_spec,
            requested_ts_values=sampled_ts_values,
            fallback_markets=_select_markets(
                manifest=manifest,
                tf=tf_value,
                quote=quote_value,
                top_n=None,
            ),
        )
        provider = _build_live_provider(
            root=root,
            feature_spec=feature_spec,
            feature_columns=feature_columns,
            tf=tf_value,
            quote=quote_value,
            bootstrap_end_ts_ms=sampled_ts_values[-1],
            bootstrap_1m_bars=_resolve_bootstrap_1m_bars(sampled_ts_values),
        )
        live_frames_by_ts, live_stats_by_ts = _build_live_frames_for_sampled_ts(
            provider=provider,
            sampled_ts_values=sampled_ts_values,
            markets_by_ts=context_markets_by_ts,
        )
    for ts_value, offline_rows in sorted(sampled_rows_by_ts.items()):
        if provider is None:
            break
        live_frame = live_frames_by_ts.get(int(ts_value), pl.DataFrame())
        live_stats = dict(live_stats_by_ts.get(int(ts_value), {}))
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
    bootstrap_1m_bars: int = 2000,
) -> LiveFeatureProviderV4:
    base_candles_root = _resolve_root(
        root=root,
        value=str(feature_spec.get("base_candles_root") or "data/parquet/candles_api_v1"),
    )
    micro_root = _resolve_root(
        root=root,
        value=str(feature_spec.get("micro_root") or "data/parquet/micro_v1"),
    )
    micro_snapshot_provider = None
    if micro_root.exists():
        micro_snapshot_provider = OfflineMicroSnapshotProvider(
            micro_root=micro_root,
            tf=tf,
            raw_ws_root=root / ".parity_no_raw_ws_overlay",
            allow_previous_fallback=False,
            enable_raw_ws_overlay=False,
        )
    return LiveFeatureProviderV4(
        feature_columns=feature_columns,
        tf=tf,
        quote=quote,
        parquet_root=base_candles_root.parent,
        candles_dataset_name=base_candles_root.name,
        micro_snapshot_provider=micro_snapshot_provider,
        bootstrap_1m_bars=max(int(bootstrap_1m_bars), 256),
        bootstrap_end_ts_ms=bootstrap_end_ts_ms,
        context_micro_required=True,
        context_history_bars=256,
    )


def _build_live_frames_for_sampled_ts(
    *,
    provider: LiveFeatureProviderV4,
    sampled_ts_values: list[int],
    markets_by_ts: dict[int, list[str]],
) -> tuple[dict[int, pl.DataFrame], dict[int, dict[str, Any]]]:
    if not sampled_ts_values:
        return {}, {}

    resolved_markets = sorted(
        {
            str(item).strip().upper()
            for items in markets_by_ts.values()
            for item in items
            if str(item).strip()
        }
    )
    if not resolved_markets:
        return {}, {}

    sampled_ts_unique = sorted({int(value) for value in sampled_ts_values})
    max_ts = sampled_ts_unique[-1]
    min_ts = sampled_ts_unique[0]
    interval_ms = max(int(expected_interval_ms(str(provider._tf).strip().lower() or "5m")), 1)  # noqa: SLF001
    span_bars = int(math.ceil(float(max_ts - min_ts) / float(interval_ms))) if max_ts > min_ts else 0
    history_bars = max(int(provider._context_history_bars) + span_bars, int(provider._context_history_bars), 1)  # noqa: SLF001

    base_frame, base_stats = provider._build_runtime_context_frame(  # noqa: SLF001
        ts_ms=max_ts,
        markets=resolved_markets,
        feature_columns=provider._base_feature_columns,  # noqa: SLF001
        extra_columns=provider._extra_columns,  # noqa: SLF001
        provider_name="LIVE_V4_PARITY_BASE",
        missing_feature_warn_ratio=1.0,
        missing_feature_skip_ratio=1.0,
        history_bars=history_bars,
    )
    base_frame, context_stats = provider._filter_context_for_micro_contract(base_frame)  # noqa: SLF001

    merged_base_stats = dict(base_stats)
    merged_base_stats.update(context_stats)

    if base_frame.height > 0:
        enriched = attach_spillover_breadth_features_v4(
            base_frame.sort(["ts_ms", "market"]),
            quote=str(provider._quote),  # noqa: SLF001
            float_dtype="float32",
        )
        enriched = attach_periodicity_features_v4(enriched, float_dtype="float32")
        enriched = attach_trend_volume_features_v4(enriched, float_dtype="float32")
        enriched = attach_order_flow_panel_v1(enriched, float_dtype="float32")
        enriched = attach_interaction_features_v4(enriched, float_dtype="float32")
        final_frame, missing_columns = project_requested_v4_columns(
            frame=enriched,
            feature_columns=provider._feature_columns,  # noqa: SLF001
            extra_columns=provider._extra_columns,  # noqa: SLF001
        )
        hard_gate_triggered = len(missing_columns) > 0
        if hard_gate_triggered:
            final_frame = final_frame.head(0)
        if final_frame.height > 0 and "ts_ms" in final_frame.columns:
            final_frame = final_frame.filter(pl.col("ts_ms").is_in(sampled_ts_unique)).sort(["ts_ms", "market"])
    else:
        final_frame = pl.DataFrame()
        missing_columns = ()
        hard_gate_triggered = False

    frames_by_ts: dict[int, pl.DataFrame] = {}
    stats_by_ts: dict[int, dict[str, Any]] = {}
    for ts_value in sampled_ts_unique:
        context_markets = [
            str(item).strip().upper()
            for item in markets_by_ts.get(int(ts_value), [])
            if str(item).strip()
        ]
        if not context_markets:
            context_markets = list(resolved_markets)
        if final_frame.height > 0 and "ts_ms" in final_frame.columns:
            ts_frame = final_frame.filter(
                (pl.col("ts_ms") == int(ts_value)) & (pl.col("market").is_in(context_markets))
            ).sort("market")
        else:
            ts_frame = final_frame
        built_markets = [
            str(item).strip().upper()
            for item in (ts_frame.get_column("market").to_list() if ts_frame.height > 0 and "market" in ts_frame.columns else [])
            if str(item).strip()
        ]
        built_market_set = set(built_markets)
        skipped_markets = [market for market in context_markets if market not in built_market_set]
        frames_by_ts[int(ts_value)] = ts_frame
        stats_by_ts[int(ts_value)] = {
            "provider": "LIVE_V4",
            "base_provider": "LIVE_V4_BASE",
            "requested_ts_ms": int(ts_value),
            "built_ts_ms": int(ts_value) if ts_frame.height > 0 else None,
            "built_rows": int(ts_frame.height),
            "requested_feature_count": int(len(provider._feature_columns)),  # noqa: SLF001
            "missing_feature_columns": list(missing_columns),
            "hard_gate_triggered": bool(hard_gate_triggered),
            "skip_reason": "MISSING_V4_FEATURE_COLUMNS" if hard_gate_triggered else "",
            "base_provider_stats": dict(merged_base_stats),
            "context_micro_required": bool(context_stats.get("context_micro_required", False)),
            "context_rows_before_micro_filter": int(context_stats.get("context_rows_before_micro_filter", 0)),
            "context_rows_after_micro_filter": int(context_stats.get("context_rows_after_micro_filter", 0)),
            "context_rows_dropped_no_micro": int(context_stats.get("context_rows_dropped_no_micro", 0)),
            "context_history_bars": int(history_bars),
            "built_market_samples": built_markets[:20],
            "skipped_market_samples": skipped_markets[:20],
        }
    return frames_by_ts, stats_by_ts


def _resolve_context_markets_by_ts(
    *,
    dataset_root: Path,
    tf: str,
    quote: str,
    sampling_window: dict[str, Any],
    feature_spec: dict[str, Any],
    requested_ts_values: list[int],
    fallback_markets: list[str],
) -> dict[int, list[str]]:
    requested = sorted({int(value) for value in requested_ts_values})
    if not requested:
        return {}
    policy = str(
        ((feature_spec.get("cross_sectional_context_policy") or {}).get("policy_id"))
        or "final_dataset_present_markets_at_ts_v1"
    ).strip()
    if policy != "final_dataset_present_markets_at_ts_v1":
        return {int(ts_value): list(fallback_markets) for ts_value in requested}

    membership = _load_context_market_membership_by_ts(
        dataset_root=dataset_root,
        tf=tf,
        quote=quote,
        sampling_window=sampling_window,
        requested_ts_values=requested,
    )
    resolved: dict[int, list[str]] = {}
    for ts_value in requested:
        markets = membership.get(int(ts_value), [])
        resolved[int(ts_value)] = markets if markets else list(fallback_markets)
    return resolved


def _load_context_market_membership_by_ts(
    *,
    dataset_root: Path,
    tf: str,
    quote: str,
    sampling_window: dict[str, Any],
    requested_ts_values: list[int],
) -> dict[int, list[str]]:
    tf_root = dataset_root / f"tf={str(tf).strip().lower()}"
    if not tf_root.exists():
        return {}
    start_date = str(sampling_window.get("effective_start") or "").strip()
    end_date = str(sampling_window.get("effective_end") or "").strip()
    files: list[Path] = []
    for market_dir in sorted(path for path in tf_root.glob("market=*") if path.is_dir()):
        market_value = market_dir.name.split("=", 1)[-1].strip().upper()
        if not market_value.startswith(f"{quote}-"):
            continue
        for date_dir in sorted(path for path in market_dir.glob("date=*") if path.is_dir()):
            date_value = date_dir.name.split("=", 1)[-1].strip()
            if start_date and date_value < start_date:
                continue
            if end_date and date_value > end_date:
                continue
            files.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
    if not files:
        return {}
    ts_set = {int(value) for value in requested_ts_values}
    try:
        lazy = pl.scan_parquet([str(path) for path in files], extra_columns="ignore")
        frame = lazy.select(["ts_ms", "market"]).filter(pl.col("ts_ms").is_in(sorted(ts_set))).collect()
    except Exception:
        parts: list[pl.DataFrame] = []
        for path in files:
            try:
                part = pl.read_parquet(path).select(["ts_ms", "market"])
            except Exception:
                continue
            sliced = part.filter(pl.col("ts_ms").is_in(sorted(ts_set)))
            if sliced.height > 0:
                parts.append(sliced)
        if not parts:
            return {}
        frame = pl.concat(parts, how="vertical_relaxed")
    membership: dict[int, list[str]] = {}
    if frame.height <= 0:
        return membership
    grouped = (
        frame.with_columns(
            pl.col("market").cast(pl.Utf8).str.to_uppercase().alias("market")
        )
        .unique(subset=["ts_ms", "market"], keep="first", maintain_order=True)
        .sort(["ts_ms", "market"])
        .group_by("ts_ms")
        .agg(pl.col("market"))
        .sort("ts_ms")
    )
    for row in grouped.iter_rows(named=True):
        ts_value = int(row.get("ts_ms") or 0)
        membership[ts_value] = [
            str(item).strip().upper()
            for item in (row.get("market") or [])
            if str(item).strip()
        ]
    return membership


def _resolve_bootstrap_1m_bars(ts_values: list[int]) -> int:
    if not ts_values:
        return 2000
    span_ms = max(int(ts_values[-1]) - int(ts_values[0]), 0)
    span_bars = int(math.ceil(float(span_ms) / 60_000.0)) + 512
    return max(span_bars, 2000)


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
