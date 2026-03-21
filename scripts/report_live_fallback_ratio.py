from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any


def _safe_load_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _summarize_by_run(*, db_path: Path, closed_limit: int) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    rows = cur.execute(
        """
        select market, realized_pnl_quote, entry_meta_json
        from trade_journal
        where status = 'CLOSED'
        order by updated_ts desc
        limit ?
        """,
        (int(closed_limit),),
    ).fetchall()
    conn.close()

    by_run: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "run_id": "unknown",
            "closed_trades": 0,
            "wins": 0,
            "losses": 0,
            "realized_pnl_quote": 0.0,
            "fallback_bin_count": 0,
            "continuous_count": 0,
            "support_reason_counts": defaultdict(int),
            "recommended_notional_multipliers": [],
            "markets": set(),
        }
    )
    for market, pnl_quote, entry_meta_json in rows:
        entry_meta = _safe_load_json(entry_meta_json)
        runtime = entry_meta.get("runtime") if isinstance(entry_meta.get("runtime"), dict) else {}
        strategy = entry_meta.get("strategy") if isinstance(entry_meta.get("strategy"), dict) else {}
        strategy_meta = strategy.get("meta") if isinstance(strategy.get("meta"), dict) else {}
        trade_action = strategy_meta.get("trade_action") if isinstance(strategy_meta.get("trade_action"), dict) else {}
        run_id = str(runtime.get("live_runtime_model_run_id") or "unknown").strip() or "unknown"
        info = by_run[run_id]
        info["run_id"] = run_id
        info["closed_trades"] += 1
        pnl_value = float(pnl_quote or 0.0)
        info["realized_pnl_quote"] += pnl_value
        if pnl_value > 0:
            info["wins"] += 1
        else:
            info["losses"] += 1
        info["markets"].add(str(market))
        decision_source = str(trade_action.get("decision_source") or trade_action.get("chosen_action_source") or "").strip()
        if decision_source == "bin_audit_fallback":
            info["fallback_bin_count"] += 1
        elif decision_source == "continuous_conditional_action_value":
            info["continuous_count"] += 1
        support_reason_code = str(trade_action.get("support_reason_code") or "").strip()
        if support_reason_code:
            info["support_reason_counts"][support_reason_code] += 1
        multiplier = _coerce_float(trade_action.get("recommended_notional_multiplier"))
        if multiplier is not None:
            info["recommended_notional_multipliers"].append(multiplier)

    output: list[dict[str, Any]] = []
    for run_id, info in sorted(by_run.items(), key=lambda item: item[0], reverse=True):
        values = list(info["recommended_notional_multipliers"])
        fallback_count = int(info["fallback_bin_count"])
        closed_trades = int(info["closed_trades"])
        output.append(
            {
                "run_id": run_id,
                "closed_trades": closed_trades,
                "wins": int(info["wins"]),
                "losses": int(info["losses"]),
                "realized_pnl_quote": round(float(info["realized_pnl_quote"]), 4),
                "fallback_bin_count": fallback_count,
                "fallback_bin_ratio": round((fallback_count / closed_trades), 4) if closed_trades > 0 else None,
                "continuous_count": int(info["continuous_count"]),
                "avg_recommended_notional_multiplier": round(sum(values) / len(values), 4) if values else None,
                "support_reason_counts": dict(sorted(info["support_reason_counts"].items())),
                "markets": sorted(info["markets"]),
            }
        )
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Report fallback_bin usage from live trade journal.")
    parser.add_argument(
        "--db-path",
        default="data/state/live_candidate/live_state.db",
        help="Path to live state SQLite DB",
    )
    parser.add_argument(
        "--closed-limit",
        type=int,
        default=200,
        help="How many recent closed trades to inspect",
    )
    args = parser.parse_args()

    rows = _summarize_by_run(
        db_path=Path(str(args.db_path)).expanduser(),
        closed_limit=max(int(args.closed_limit), 1),
    )
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
