from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sqlite3
from statistics import mean, median
from typing import Any


_KST = timezone(timedelta(hours=9), name="KST")


def _coerce_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_json_text(value: Any) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _is_synthetic_closed_row(item: dict[str, Any]) -> bool:
    journal_id = str(item.get("journal_id") or "").strip()
    status = str(item.get("status") or "").strip().upper()
    if status != "CLOSED":
        return False
    return journal_id.startswith("imported-") or journal_id.startswith("trade-")


def _row_priority(item: dict[str, Any]) -> tuple[int, int, int, int, str]:
    synthetic = 0 if not _is_synthetic_closed_row(item) else 1
    has_entry = int(bool(str(item.get("entry_intent_id") or "").strip() or str(item.get("entry_order_uuid") or "").strip()))
    verified = int(bool((_parse_json_text(item.get("exit_meta_json")) or {}).get("close_verified")))
    updated_ts = _coerce_int(item.get("updated_ts")) or 0
    return (
        -synthetic,
        has_entry,
        verified,
        updated_ts,
        str(item.get("journal_id") or ""),
    )


def _dedupe_closed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped_by_exit_uuid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    passthrough: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("status") or "").strip().upper() != "CLOSED":
            passthrough.append(row)
            continue
        exit_uuid = str(row.get("exit_order_uuid") or "").strip()
        if exit_uuid:
            grouped_by_exit_uuid[exit_uuid].append(row)
        else:
            passthrough.append(row)
    selected: list[dict[str, Any]] = []
    for items in grouped_by_exit_uuid.values():
        selected.append(max(items, key=_row_priority))
    selected.extend(passthrough)
    selected.sort(
        key=lambda item: (
            _coerce_int(item.get("exit_ts_ms"))
            or _coerce_int(item.get("entry_filled_ts_ms"))
            or _coerce_int(item.get("entry_submitted_ts_ms"))
            or _coerce_int(item.get("updated_ts"))
            or 0
        ),
        reverse=True,
    )
    return selected


def _fmt_ts_kst(ts_ms: int | None) -> str:
    if ts_ms is None:
        return "-"
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_KST).strftime("%Y-%m-%d %H:%M:%S KST")


def _profit_factor(values: list[float]) -> float | None:
    gross_profit = sum(value for value in values if value > 0.0)
    gross_loss = abs(sum(value for value in values if value < 0.0))
    if gross_loss <= 0.0:
        return None if gross_profit <= 0.0 else float("inf")
    return gross_profit / gross_loss


def build_candidate_canary_report(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(row) for row in conn.execute("SELECT * FROM trade_journal ORDER BY COALESCE(exit_ts_ms, entry_filled_ts_ms, entry_submitted_ts_ms, updated_ts) DESC")]
        positions_count = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        open_orders_count = conn.execute("SELECT COUNT(*) FROM orders WHERE UPPER(COALESCE(local_state,'')) IN ('OPEN','REPLACING')").fetchone()[0]
    finally:
        conn.close()

    deduped_rows = _dedupe_closed_rows(rows)
    closed_rows = [row for row in deduped_rows if str(row.get("status") or "").strip().upper() == "CLOSED"]
    open_rows = [row for row in deduped_rows if str(row.get("status") or "").strip().upper() == "OPEN"]
    cancelled_rows = [row for row in deduped_rows if str(row.get("status") or "").strip().upper() == "CANCELLED_ENTRY"]

    for row in closed_rows:
        row["exit_meta"] = _parse_json_text(row.get("exit_meta_json"))

    verified_rows = [row for row in closed_rows if (row.get("exit_meta") or {}).get("close_verified") is True]
    unverified_rows = [row for row in closed_rows if (row.get("exit_meta") or {}).get("close_verified") is False]

    realized_values = [float(row["realized_pnl_quote"]) for row in verified_rows if row.get("realized_pnl_quote") is not None]
    hold_minutes = []
    for row in closed_rows:
        entry_ts = _coerce_int(row.get("entry_filled_ts_ms")) or _coerce_int(row.get("entry_submitted_ts_ms"))
        exit_ts = _coerce_int(row.get("exit_ts_ms"))
        if entry_ts is not None and exit_ts is not None and exit_ts >= entry_ts:
            hold_minutes.append((exit_ts - entry_ts) / 60000.0)

    close_modes = Counter(str(row.get("close_mode") or "").strip() for row in closed_rows)
    close_reasons = Counter(str(row.get("close_reason_code") or "").strip() for row in closed_rows)
    verification_status = Counter(str((row.get("exit_meta") or {}).get("close_verification_status") or "").strip() for row in closed_rows)

    by_market: dict[str, dict[str, Any]] = defaultdict(lambda: {"closed": 0, "verified": 0, "realized_pnl_quote": 0.0, "wins": 0, "losses": 0})
    for row in closed_rows:
        market = str(row.get("market") or "").strip().upper()
        item = by_market[market]
        item["closed"] += 1
        if (row.get("exit_meta") or {}).get("close_verified") is True and row.get("realized_pnl_quote") is not None:
            realized = float(row["realized_pnl_quote"])
            item["verified"] += 1
            item["realized_pnl_quote"] += realized
            if realized > 0:
                item["wins"] += 1
            elif realized < 0:
                item["losses"] += 1

    report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "rows_total_raw": len(rows),
        "closed_total": len(closed_rows),
        "verified_closed_total": len(verified_rows),
        "unverified_closed_total": len(unverified_rows),
        "open_total": len(open_rows),
        "cancelled_entry_total": len(cancelled_rows),
        "positions_count": int(positions_count),
        "open_orders_count": int(open_orders_count),
        "realized_pnl_quote_total_verified": round(sum(realized_values), 6),
        "realized_pnl_quote_avg_verified": round(mean(realized_values), 6) if realized_values else None,
        "realized_pnl_quote_median_verified": round(median(realized_values), 6) if realized_values else None,
        "wins_verified": sum(1 for value in realized_values if value > 0.0),
        "losses_verified": sum(1 for value in realized_values if value < 0.0),
        "flats_verified": sum(1 for value in realized_values if value == 0.0),
        "win_rate_verified_pct": (sum(1 for value in realized_values if value > 0.0) / len(realized_values) * 100.0) if realized_values else None,
        "profit_factor_verified": _profit_factor(realized_values),
        "avg_hold_minutes_all_closed": round(mean(hold_minutes), 3) if hold_minutes else None,
        "median_hold_minutes_all_closed": round(median(hold_minutes), 3) if hold_minutes else None,
        "close_modes": dict(close_modes),
        "close_reasons_top": close_reasons.most_common(10),
        "verification_status": dict(verification_status),
        "markets_top": sorted(
            (
                {
                    "market": market,
                    "closed": item["closed"],
                    "verified": item["verified"],
                    "wins": item["wins"],
                    "losses": item["losses"],
                    "realized_pnl_quote": round(float(item["realized_pnl_quote"]), 6),
                }
                for market, item in by_market.items()
            ),
            key=lambda item: (item["closed"], item["realized_pnl_quote"]),
            reverse=True,
        )[:10],
        "latest_closed": [
            {
                "journal_id": row.get("journal_id"),
                "market": row.get("market"),
                "entry_ts_ms": _coerce_int(row.get("entry_filled_ts_ms")) or _coerce_int(row.get("entry_submitted_ts_ms")),
                "entry_ts_kst": _fmt_ts_kst(_coerce_int(row.get("entry_filled_ts_ms")) or _coerce_int(row.get("entry_submitted_ts_ms"))),
                "exit_ts_ms": _coerce_int(row.get("exit_ts_ms")),
                "exit_ts_kst": _fmt_ts_kst(_coerce_int(row.get("exit_ts_ms"))),
                "entry_price": _coerce_float(row.get("entry_price")),
                "exit_price": _coerce_float(row.get("exit_price")),
                "qty": _coerce_float(row.get("qty")),
                "realized_pnl_quote": _coerce_float(row.get("realized_pnl_quote")),
                "realized_pnl_pct": _coerce_float(row.get("realized_pnl_pct")),
                "close_mode": row.get("close_mode"),
                "close_reason_code": row.get("close_reason_code"),
                "close_verified": (row.get("exit_meta") or {}).get("close_verified"),
                "close_verification_status": (row.get("exit_meta") or {}).get("close_verification_status"),
            }
            for row in closed_rows[:10]
        ],
    }
    return report


def render_candidate_canary_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Candidate Canary Trade Report",
        "",
        "## Summary",
        f"- generated_at_utc: {report.get('generated_at_utc')}",
        f"- db_path: {report.get('db_path')}",
        f"- closed_total: {report.get('closed_total')}",
        f"- verified_closed_total: {report.get('verified_closed_total')}",
        f"- cancelled_entry_total: {report.get('cancelled_entry_total')}",
        f"- open_total: {report.get('open_total')}",
        f"- positions_count: {report.get('positions_count')}",
        f"- open_orders_count: {report.get('open_orders_count')}",
        f"- realized_pnl_quote_total_verified: {report.get('realized_pnl_quote_total_verified')}",
        f"- realized_pnl_quote_avg_verified: {report.get('realized_pnl_quote_avg_verified')}",
        f"- realized_pnl_quote_median_verified: {report.get('realized_pnl_quote_median_verified')}",
        f"- win_rate_verified_pct: {report.get('win_rate_verified_pct')}",
        f"- profit_factor_verified: {report.get('profit_factor_verified')}",
        f"- avg_hold_minutes_all_closed: {report.get('avg_hold_minutes_all_closed')}",
        f"- median_hold_minutes_all_closed: {report.get('median_hold_minutes_all_closed')}",
        "",
        "## Close Modes",
    ]
    for key, value in sorted((report.get("close_modes") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Close Reasons (Top 10)"])
    for key, value in report.get("close_reasons_top") or []:
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Markets"])
    lines.append("| Market | Closed | Verified | Wins | Losses | Realized PnL |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for item in report.get("markets_top") or []:
        lines.append(
            f"| {item['market']} | {item['closed']} | {item['verified']} | {item['wins']} | {item['losses']} | {item['realized_pnl_quote']:.6f} |"
        )
    lines.extend(["", "## Latest Closed Trades"])
    lines.append("| Market | Entry KST | Exit KST | Qty | Entry | Exit | PnL | Mode | Reason |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |")
    for item in report.get("latest_closed") or []:
        entry_price = item.get("entry_price")
        exit_price = item.get("exit_price")
        qty = item.get("qty")
        pnl = item.get("realized_pnl_quote")
        lines.append(
            f"| {item.get('market')} | {item.get('entry_ts_kst')} | {item.get('exit_ts_kst')} | "
            f"{('-' if qty is None else f'{qty:.8f}')} | "
            f"{('-' if entry_price is None else f'{entry_price:.6f}')} | "
            f"{('-' if exit_price is None else f'{exit_price:.6f}')} | "
            f"{('-' if pnl is None else f'{pnl:.6f}')} | "
            f"{item.get('close_mode') or '-'} | {item.get('close_reason_code') or '-'} |"
        )
    return "\n".join(lines) + "\n"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render cumulative candidate canary trade report.")
    parser.add_argument("--db-path", required=True, help="Path to live candidate live_state.db")
    parser.add_argument("--output-md", help="Markdown report output path")
    parser.add_argument("--output-json", help="JSON report output path")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = build_candidate_canary_report(Path(args.db_path))
    markdown = render_candidate_canary_markdown(report)
    if args.output_md:
        _write_text(Path(args.output_md), markdown)
    if args.output_json:
        _write_json(Path(args.output_json), report)
    print(markdown if not args.output_json else json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
