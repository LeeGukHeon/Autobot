from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from typing import Any


_DELETE_CANDIDATE_QUERY = """
SELECT
    t.journal_id,
    t.market,
    t.status,
    t.exit_order_uuid,
    t.realized_pnl_quote,
    t.exit_ts_ms,
    t.updated_ts
FROM trade_journal AS t
WHERE t.journal_id LIKE 'imported-%'
  AND t.exit_order_uuid IS NOT NULL
  AND TRIM(t.exit_order_uuid) <> ''
  AND EXISTS (
      SELECT 1
      FROM trade_journal AS k
      WHERE k.exit_order_uuid = t.exit_order_uuid
        AND k.journal_id NOT LIKE 'imported-%'
  )
ORDER BY t.exit_order_uuid ASC, t.updated_ts ASC, t.journal_id ASC
"""


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {str(key): row[key] for key in row.keys()}


def find_imported_trade_journal_duplicates(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(_DELETE_CANDIDATE_QUERY).fetchall()
        return [_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def cleanup_imported_trade_journal_duplicates(db_path: Path, *, apply_changes: bool) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        candidates = [_row_to_dict(row) for row in conn.execute(_DELETE_CANDIDATE_QUERY).fetchall()]
        exit_order_uuids = sorted({str(item.get("exit_order_uuid") or "").strip() for item in candidates if item.get("exit_order_uuid")})
        groups: list[dict[str, Any]] = []
        for exit_order_uuid in exit_order_uuids:
            keep_rows = conn.execute(
                """
                SELECT journal_id, market, status, exit_order_uuid, realized_pnl_quote, exit_ts_ms, updated_ts
                FROM trade_journal
                WHERE exit_order_uuid = ?
                  AND journal_id NOT LIKE 'imported-%'
                ORDER BY updated_ts ASC, journal_id ASC
                """,
                (exit_order_uuid,),
            ).fetchall()
            delete_rows = [item for item in candidates if str(item.get("exit_order_uuid") or "").strip() == exit_order_uuid]
            groups.append(
                {
                    "exit_order_uuid": exit_order_uuid,
                    "keep_journal_ids": [str(row["journal_id"]) for row in keep_rows],
                    "delete_journal_ids": [str(item["journal_id"]) for item in delete_rows],
                    "deleted_realized_pnl_quote_total": float(
                        sum(float(item.get("realized_pnl_quote") or 0.0) for item in delete_rows)
                    ),
                }
            )

        if apply_changes and candidates:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    "DELETE FROM trade_journal WHERE journal_id = ?",
                    [(str(item["journal_id"]),) for item in candidates],
                )
            except Exception:
                conn.rollback()
                raise
            conn.commit()

        return {
            "db_path": str(db_path),
            "apply_changes": bool(apply_changes),
            "duplicate_row_count": len(candidates),
            "duplicate_exit_order_uuid_count": len(groups),
            "deleted_realized_pnl_quote_total": float(
                sum(float(item.get("realized_pnl_quote") or 0.0) for item in candidates)
            ),
            "groups": groups,
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean imported duplicate trade journal close rows.")
    parser.add_argument("--db-path", required=True, help="Path to live_state.db")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete duplicate imported rows. Without this flag, only report candidates.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = cleanup_imported_trade_journal_duplicates(Path(args.db_path), apply_changes=bool(args.apply))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
