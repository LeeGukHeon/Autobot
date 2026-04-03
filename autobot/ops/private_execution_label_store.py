"""Build and validate the private execution supervision dataset."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import polars as pl

from autobot.live.state_store import LiveStateStore


DEFAULT_DATASET_REL_ROOT = Path("data") / "parquet" / "private_execution_v1"
DEFAULT_STATE_DB_CANDIDATES: tuple[tuple[str, Path], ...] = (
    ("live_main", Path("data/state/live/live_state.db")),
    ("live_main_legacy", Path("data/state/live_state.db")),
    ("live_canary", Path("data/state/live_canary/live_state.db")),
    ("live_candidate", Path("data/state/live_candidate/live_state.db")),
)


REQUIRED_COLUMNS: tuple[str, ...] = (
    "row_key",
    "market",
    "ts_ms",
    "decision_bucket_ts_ms",
    "runtime_model_family",
    "runtime_model_run_id",
    "action_code",
    "ord_type",
    "time_in_force",
    "requested_price",
    "requested_volume",
    "spread_bps",
    "depth_top5_notional_krw",
    "snapshot_age_ms",
    "expected_edge_bps",
    "score_mean",
    "score_std",
    "final_state",
    "first_fill_ts_ms",
    "full_fill_ts_ms",
    "fill_latency_ms",
    "shortfall_bps",
    "adverse_move_bps",
    "filled_within_deadline",
    "label_source",
    "y_tradeable",
    "y_fill_within_deadline",
    "y_shortfall_bps",
    "y_adverse_tolerance",
)

_PRIVATE_EXECUTION_SCHEMA: dict[str, pl.DataType] = {
    "row_key": pl.Utf8,
    "market": pl.Utf8,
    "ts_ms": pl.Int64,
    "decision_bucket_ts_ms": pl.Int64,
    "runtime_model_family": pl.Utf8,
    "runtime_model_run_id": pl.Utf8,
    "action_code": pl.Utf8,
    "ord_type": pl.Utf8,
    "time_in_force": pl.Utf8,
    "requested_price": pl.Float64,
    "requested_volume": pl.Float64,
    "spread_bps": pl.Float64,
    "depth_top5_notional_krw": pl.Float64,
    "snapshot_age_ms": pl.Int64,
    "expected_edge_bps": pl.Float64,
    "score_mean": pl.Float64,
    "score_std": pl.Float64,
    "final_state": pl.Utf8,
    "first_fill_ts_ms": pl.Int64,
    "full_fill_ts_ms": pl.Int64,
    "fill_latency_ms": pl.Int64,
    "shortfall_bps": pl.Float64,
    "adverse_move_bps": pl.Float64,
    "filled_within_deadline": pl.Boolean,
    "label_source": pl.Utf8,
    "y_tradeable": pl.Int64,
    "y_fill_within_deadline": pl.Int64,
    "y_shortfall_bps": pl.Float64,
    "y_adverse_tolerance": pl.Int64,
}


def build_private_execution_label_store(
    *,
    project_root: str | Path,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    dataset_root = Path(output_root).resolve() if output_root is not None else (root / DEFAULT_DATASET_REL_ROOT)
    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)

    rows, source_dbs = _collect_private_execution_rows(project_root=root)
    manifest_rows: list[dict[str, Any]] = []
    rows_written_total = 0
    for (market, date_value), part_rows in _group_rows_by_market_date(rows).items():
        part_dir = dataset_root / f"market={market}" / f"date={date_value}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_path = part_dir / "part-000.parquet"
        frame = _normalize_private_execution_frame(pl.DataFrame(part_rows))
        frame.write_parquet(part_path, compression="zstd")
        rows_written_total += int(frame.height)
        manifest_rows.append(
            {
                "dataset_name": "private_execution_v1",
                "market": market,
                "date": date_value,
                "rows": int(frame.height),
                "min_ts_ms": int(frame.get_column("ts_ms").min()) if frame.height > 0 else None,
                "max_ts_ms": int(frame.get_column("ts_ms").max()) if frame.height > 0 else None,
                "part_file": str(part_path),
                "status": "OK" if frame.height > 0 else "WARN",
                "reasons_json": json.dumps([] if frame.height > 0 else ["NO_ROWS"], ensure_ascii=False),
                "error_message": None,
            }
        )

    _write_manifest(meta_root / "manifest.parquet", manifest_rows)
    contract = _build_private_execution_label_contract(dataset_root=dataset_root)
    build_report = {
        "policy": "private_execution_build_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_name": "private_execution_v1",
        "dataset_root": str(dataset_root),
        "rows_written_total": int(rows_written_total),
        "source_state_dbs": source_dbs,
        "source_contract_ids": [
            "runtime:live_main",
            "runtime:live_candidate",
            "runtime:live_rollout_contract",
            "runtime:live_runtime_contract",
            "runtime:ws_public_contract",
            "runtime:live_runtime_health",
        ],
        "manifest_path": str(meta_root / "manifest.parquet"),
        "label_contract_path": str(meta_root / "private_execution_label_contract.json"),
        "status": "PASS" if rows_written_total > 0 else "FAIL",
    }
    validate_report = _validate_private_execution_rows(rows=rows, dataset_root=dataset_root)
    (meta_root / "private_execution_label_contract.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (meta_root / "build_report.json").write_text(
        json.dumps(build_report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (meta_root / "validate_report.json").write_text(
        json.dumps(validate_report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "dataset_root": str(dataset_root),
        "build_report_path": str(meta_root / "build_report.json"),
        "validate_report_path": str(meta_root / "validate_report.json"),
        "label_contract_path": str(meta_root / "private_execution_label_contract.json"),
        "rows_written_total": int(rows_written_total),
        "status": str(validate_report.get("status") or build_report.get("status") or "FAIL"),
    }


def write_private_execution_label_store(
    *,
    project_root: str | Path,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    return build_private_execution_label_store(
        project_root=project_root,
        output_root=output_root,
    )


def _normalize_private_execution_frame(frame: pl.DataFrame) -> pl.DataFrame:
    normalized = frame
    for column_name, dtype in _PRIVATE_EXECUTION_SCHEMA.items():
        if column_name not in normalized.columns:
            normalized = normalized.with_columns(pl.lit(None, dtype=dtype).alias(column_name))
        else:
            normalized = normalized.with_columns(pl.col(column_name).cast(dtype, strict=False))
    ordered = [name for name in _PRIVATE_EXECUTION_SCHEMA.keys() if name in normalized.columns]
    extras = [name for name in normalized.columns if name not in _PRIVATE_EXECUTION_SCHEMA]
    return normalized.select(ordered + extras)


def _collect_private_execution_rows(*, project_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    source_dbs: list[str] = []
    for lane_name, rel_path in DEFAULT_STATE_DB_CANDIDATES:
        db_path = project_root / rel_path
        if not db_path.exists():
            continue
        source_dbs.append(str(db_path))
        with LiveStateStore(db_path) as store:
            journals = list(store.list_trade_journal(limit=200_000))
            attempts = list(store.list_execution_attempts(limit=400_000))
            lineage = list(store.list_order_lineage())
            orders = list(store.list_orders(open_only=False))
            live_runtime_contract = dict(store.runtime_contract() or {})
            live_rollout_contract = dict(store.live_rollout_contract() or {})
            ws_public_contract = dict(store.ws_public_contract() or {})
            live_runtime_health = dict(store.live_runtime_health() or {})
        attempts_by_journal = _group_attempts_by_journal(attempts)
        attempts_by_intent = _group_attempts_by_intent(attempts)
        lineage_by_intent = _group_lineage_by_intent(lineage)
        orders_by_uuid = _group_orders_by_key(orders, key_name="uuid")
        orders_by_identifier = _group_orders_by_key(orders, key_name="identifier")
        orders_by_intent = _group_orders_by_key(orders, key_name="intent_id")
        for journal in journals:
            row = _build_private_execution_row(
                journal=journal,
                attempts_by_journal=attempts_by_journal,
                attempts_by_intent=attempts_by_intent,
                lineage_by_intent=lineage_by_intent,
                orders_by_uuid=orders_by_uuid,
                orders_by_identifier=orders_by_identifier,
                orders_by_intent=orders_by_intent,
                source_db_path=db_path,
                lane_name=lane_name,
                live_runtime_contract=live_runtime_contract,
                live_rollout_contract=live_rollout_contract,
                ws_public_contract=ws_public_contract,
                live_runtime_health=live_runtime_health,
            )
            if row:
                rows.append(row)
    rows.sort(key=lambda item: (int(item.get("ts_ms") or 0), str(item.get("market") or "").upper(), str(item.get("row_key") or "")))
    return rows, source_dbs


def _group_attempts_by_journal(attempts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in attempts:
        key = str(item.get("journal_id") or "").strip()
        if not key:
            continue
        grouped.setdefault(key, []).append(dict(item))
    for values in grouped.values():
        values.sort(key=lambda item: int(item.get("submitted_ts_ms") or 0))
    return grouped


def _group_attempts_by_intent(attempts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in attempts:
        key = str(item.get("intent_id") or "").strip()
        if not key:
            continue
        grouped.setdefault(key, []).append(dict(item))
    for values in grouped.values():
        values.sort(key=lambda item: int(item.get("submitted_ts_ms") or 0))
    return grouped


def _group_lineage_by_intent(edges: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in edges:
        key = str(item.get("intent_id") or "").strip()
        if not key:
            continue
        grouped.setdefault(key, []).append(dict(item))
    for values in grouped.values():
        values.sort(key=lambda item: int(item.get("ts_ms") or 0))
    return grouped


def _group_orders_by_key(orders: list[dict[str, Any]], *, key_name: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in orders:
        key = str(item.get(key_name) or "").strip()
        if not key:
            continue
        grouped.setdefault(key, []).append(dict(item))
    for values in grouped.values():
        values.sort(key=lambda item: int(item.get("updated_ts") or 0))
    return grouped


def _build_private_execution_row(
    *,
    journal: dict[str, Any],
    attempts_by_journal: dict[str, list[dict[str, Any]]],
    attempts_by_intent: dict[str, list[dict[str, Any]]],
    lineage_by_intent: dict[str, list[dict[str, Any]]],
    orders_by_uuid: dict[str, list[dict[str, Any]]],
    orders_by_identifier: dict[str, list[dict[str, Any]]],
    orders_by_intent: dict[str, list[dict[str, Any]]],
    source_db_path: Path,
    lane_name: str,
    live_runtime_contract: dict[str, Any],
    live_rollout_contract: dict[str, Any],
    ws_public_contract: dict[str, Any],
    live_runtime_health: dict[str, Any],
) -> dict[str, Any] | None:
    journal_id = str(journal.get("journal_id") or "").strip()
    intent_id = str(journal.get("entry_intent_id") or "").strip()
    market = str(journal.get("market") or "").strip().upper()
    ts_ms = _coalesce_int(
        journal.get("entry_submitted_ts_ms"),
        journal.get("entry_filled_ts_ms"),
        journal.get("updated_ts"),
    )
    if not market or ts_ms is None:
        return None
    attempts = list(attempts_by_journal.get(journal_id) or attempts_by_intent.get(intent_id) or [])
    first_attempt = dict(attempts[0]) if attempts else {}
    last_attempt = dict(attempts[-1]) if attempts else {}
    entry_meta = dict(journal.get("entry_meta") or {})
    exit_meta = dict(journal.get("exit_meta") or {})
    runtime_meta = dict(entry_meta.get("runtime") or {})
    strategy_meta = dict(((entry_meta.get("strategy") or {}).get("meta")) or {})
    execution_meta = dict(entry_meta.get("execution") or {})
    execution_policy = dict(entry_meta.get("execution_policy") or {})
    order_uuid = _coalesce_text(first_attempt.get("order_uuid"), journal.get("entry_order_uuid"), journal.get("exit_order_uuid"))
    order_identifier = _coalesce_text(first_attempt.get("order_identifier"))
    order_rows = list(
        orders_by_uuid.get(order_uuid or "", [])
        or orders_by_identifier.get(order_identifier or "", [])
        or orders_by_intent.get(intent_id or "", [])
    )
    order_row = dict(order_rows[-1]) if order_rows else {}
    adverse_move_bps = _coalesce_float(
        exit_meta.get("entry_realized_slippage_bps"),
        journal.get("expected_downside_bps"),
        last_attempt.get("expected_es_bps"),
        last_attempt.get("shortfall_bps"),
    )
    first_fill_ts_ms = _coalesce_int(last_attempt.get("first_fill_ts_ms"), journal.get("entry_filled_ts_ms"))
    full_fill_ts_ms = _coalesce_int(last_attempt.get("full_fill_ts_ms"), first_fill_ts_ms)
    fill_latency_ms = None
    if first_fill_ts_ms is not None:
        fill_latency_ms = max(int(first_fill_ts_ms) - int(ts_ms), 0)
    deadline_ms = _coalesce_int(
        execution_policy.get("deadline_ms"),
        execution_policy.get("selected_expected_time_to_first_fill_ms"),
    )
    if deadline_ms is None or deadline_ms <= 0:
        deadline_ms = 60_000
    filled_within_deadline = bool(first_fill_ts_ms is not None and fill_latency_ms is not None and int(fill_latency_ms) <= int(deadline_ms))
    expected_edge_bps = _coalesce_float(journal.get("expected_edge_bps"), last_attempt.get("expected_edge_bps"))
    expected_net_edge_bps = _coalesce_float(journal.get("expected_net_edge_bps"), last_attempt.get("expected_net_edge_bps"))
    adverse_tolerance_bps = max(abs(expected_edge_bps or 0.0), abs(expected_net_edge_bps or 0.0), 5.0)
    y_adverse_tolerance = 1 if adverse_move_bps is not None and float(abs(adverse_move_bps)) <= float(adverse_tolerance_bps) else 0
    realized_pnl_quote = _coalesce_float(journal.get("realized_pnl_quote"), 0.0) or 0.0
    y_tradeable = 1 if (realized_pnl_quote > 0.0 and filled_within_deadline and y_adverse_tolerance == 1) else 0
    lineage_rows = list(lineage_by_intent.get(intent_id) or [])
    row_key = "|".join(
        item
        for item in [
            journal_id or intent_id,
            market,
            str(ts_ms),
        ]
        if item
    )
    return {
        "row_key": row_key,
        "journal_id": journal_id or None,
        "intent_id": intent_id or None,
        "market": market,
        "ts_ms": int(ts_ms),
        "decision_bucket_ts_ms": int((int(ts_ms) // 300_000) * 300_000),
        "runtime_model_family": str(runtime_meta.get("model_family") or "").strip() or None,
        "runtime_model_run_id": str(runtime_meta.get("live_runtime_model_run_id") or "").strip() or None,
        "runtime_contract_version": _coalesce_int(live_runtime_contract.get("version")),
        "runtime_decision_contract_version": _coalesce_text(live_runtime_contract.get("decision_contract_version")),
        "source_state_db_path": str(source_db_path),
        "source_lane": str(lane_name).strip(),
        "action_code": _coalesce_text(first_attempt.get("action_code"), execution_policy.get("selected_action_code")),
        "ord_type": _coalesce_text(first_attempt.get("ord_type"), execution_policy.get("selected_ord_type")),
        "time_in_force": _coalesce_text(first_attempt.get("time_in_force"), execution_policy.get("selected_time_in_force")),
        "order_uuid": order_uuid,
        "order_identifier": order_identifier,
        "order_local_state": _coalesce_text(order_row.get("local_state")),
        "order_replace_seq": _coalesce_int(order_row.get("replace_seq")),
        "order_root_uuid": _coalesce_text(order_row.get("root_order_uuid")),
        "requested_price": _coalesce_float(first_attempt.get("requested_price"), execution_meta.get("requested_price")),
        "requested_volume": _coalesce_float(first_attempt.get("requested_volume"), execution_meta.get("requested_volume")),
        "requested_notional_quote": _coalesce_float(first_attempt.get("requested_notional_quote")),
        "spread_bps": _coalesce_float(first_attempt.get("spread_bps"), ((entry_meta.get("micro_state") or {}).get("spread_bps"))),
        "depth_top5_notional_krw": _coalesce_float(first_attempt.get("depth_top5_notional_krw"), ((entry_meta.get("micro_state") or {}).get("depth_top5_notional_krw"))),
        "snapshot_age_ms": _coalesce_int(first_attempt.get("snapshot_age_ms"), ((entry_meta.get("micro_state") or {}).get("snapshot_age_ms"))),
        "expected_edge_bps": expected_edge_bps,
        "expected_net_edge_bps": expected_net_edge_bps,
        "score_mean": _coalesce_float(((entry_meta.get("strategy") or {}).get("score")), strategy_meta.get("model_prob"), journal.get("model_prob")),
        "score_std": _coalesce_float(
            strategy_meta.get("model_prob_raw"),
            (((entry_meta.get("strategy") or {}).get("meta") or {}).get("final_uncertainty")),
            (((entry_meta.get("strategy") or {}).get("meta") or {}).get("score_std")),
        ),
        "model_prob": _coalesce_float(journal.get("model_prob"), strategy_meta.get("model_prob"), first_attempt.get("model_prob")),
        "final_state": _coalesce_text(last_attempt.get("final_state"), journal.get("status")),
        "order_executed_funds_quote": _coalesce_float(order_row.get("executed_funds")),
        "order_paid_fee_quote": _coalesce_float(order_row.get("paid_fee")),
        "order_remaining_fee_quote": _coalesce_float(order_row.get("remaining_fee")),
        "first_fill_ts_ms": first_fill_ts_ms,
        "full_fill_ts_ms": full_fill_ts_ms,
        "fill_latency_ms": fill_latency_ms,
        "shortfall_bps": _coalesce_float(last_attempt.get("shortfall_bps")),
        "adverse_move_bps": adverse_move_bps,
        "filled_within_deadline": bool(filled_within_deadline),
        "deadline_ms": int(deadline_ms),
        "rollout_mode": _coalesce_text(live_rollout_contract.get("mode"), live_rollout_contract.get("rollout_mode")),
        "rollout_lane_id": _coalesce_text(live_rollout_contract.get("lane_id")),
        "ws_public_stale": bool(ws_public_contract.get("ws_public_stale", False)),
        "live_runtime_health_status": _coalesce_text(live_runtime_health.get("status")),
        "label_source": "trade_journal_execution_attempts_v1",
        "lineage_event_count": len(lineage_rows),
        "y_tradeable": int(y_tradeable),
        "y_fill_within_deadline": 1 if filled_within_deadline else 0,
        "y_shortfall_bps": _coalesce_float(last_attempt.get("shortfall_bps"), 0.0) or 0.0,
        "y_adverse_tolerance": int(y_adverse_tolerance),
    }


def _group_rows_by_market_date(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        market = str(row.get("market") or "").strip().upper()
        ts_ms = int(row.get("ts_ms") or 0)
        if not market or ts_ms <= 0:
            continue
        date_value = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date().isoformat()
        grouped.setdefault((market, date_value), []).append(dict(row))
    return grouped


def _write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    schema = {
        "dataset_name": pl.Utf8,
        "market": pl.Utf8,
        "date": pl.Utf8,
        "rows": pl.Int64,
        "min_ts_ms": pl.Int64,
        "max_ts_ms": pl.Int64,
        "part_file": pl.Utf8,
        "status": pl.Utf8,
        "reasons_json": pl.Utf8,
        "error_message": pl.Utf8,
    }
    frame = pl.DataFrame(rows, schema=schema, orient="row") if rows else pl.DataFrame([], schema=schema, orient="row")
    frame.write_parquet(path, compression="zstd")


def _build_private_execution_label_contract(*, dataset_root: Path) -> dict[str, Any]:
    return {
        "policy": "private_execution_label_contract_v1",
        "dataset_name": "private_execution_v1",
        "dataset_root": str(dataset_root),
        "row_grain": "trade_journal_entry",
        "row_key": "journal_id_or_intent_id + market + entry_ts_ms",
        "required_columns": list(REQUIRED_COLUMNS),
        "label_columns": [
            "y_tradeable",
            "y_fill_within_deadline",
            "y_shortfall_bps",
            "y_adverse_tolerance",
        ],
        "inference_features": [
            "market",
            "decision_bucket_ts_ms",
            "runtime_model_family",
            "runtime_model_run_id",
            "runtime_decision_contract_version",
            "action_code",
            "ord_type",
            "time_in_force",
            "order_local_state",
            "order_replace_seq",
            "requested_price",
            "requested_volume",
            "requested_notional_quote",
            "spread_bps",
            "depth_top5_notional_krw",
            "snapshot_age_ms",
            "expected_edge_bps",
            "score_mean",
            "score_std",
            "rollout_mode",
            "ws_public_stale",
        ],
    }


def _validate_private_execution_rows(*, rows: list[dict[str, Any]], dataset_root: Path) -> dict[str, Any]:
    missing_columns = [name for name in REQUIRED_COLUMNS if not rows or any(name not in row for row in rows)]
    null_required = 0
    for row in rows:
        for name in ("row_key", "market", "ts_ms", "decision_bucket_ts_ms", "final_state", "label_source"):
            if row.get(name) in (None, ""):
                null_required += 1
    reasons: list[str] = []
    status = "PASS"
    if not rows:
        status = "FAIL"
        reasons.append("PRIVATE_EXECUTION_ROWS_EMPTY")
    if missing_columns:
        status = "FAIL"
        reasons.append("PRIVATE_EXECUTION_COLUMNS_MISSING")
    if null_required > 0:
        status = "FAIL"
        reasons.append("PRIVATE_EXECUTION_REQUIRED_NULLS_PRESENT")
    return {
        "policy": "private_execution_validate_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_root": str(dataset_root),
        "checked_rows": len(rows),
        "missing_columns": missing_columns,
        "required_null_count": int(null_required),
        "status": status,
        "pass": status == "PASS",
        "reasons": reasons,
    }


def _coalesce_int(*values: Any) -> int | None:
    for value in values:
        try:
            if value in (None, ""):
                continue
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _coalesce_float(*values: Any) -> float | None:
    for value in values:
        try:
            if value in (None, ""):
                continue
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _coalesce_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build private execution supervision dataset.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-root", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    payload = build_private_execution_label_store(
        project_root=Path(args.project_root),
        output_root=(Path(args.output_root) if str(args.output_root).strip() else None),
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
