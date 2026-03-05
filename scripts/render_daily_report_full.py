#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]

def run(cmd: list[str]) -> Tuple[int, str]:
    p = subprocess.run(cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode, p.stdout

def read_json(p: Path) -> Optional[Dict[str, Any]]:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def exists_ws_parts(raw_root: Path, date: str) -> bool:
    # count any *.zst under date partition
    for ch in ["trade", "orderbook"]:
        d = raw_root / ch / f"date={date}"
        if d.exists():
            # recurse check
            if any(d.rglob("*.zst")):
                return True
    return False

def pick_ws_date(batch_date: str, raw_root: Path) -> Tuple[str, str]:
    # 1) if batch_date has parts
    if exists_ws_parts(raw_root, batch_date):
        return batch_date, "MATCHED_BATCH_DATE_HAS_WS_PARTS"
    # 2) UTC today
    utc_today = datetime.utcnow().strftime("%Y-%m-%d")
    if exists_ws_parts(raw_root, utc_today):
        return utc_today, "FALLBACK_TO_UTC_TODAY_HAS_WS_PARTS"
    # 3) latest available partition
    dates = []
    for ch in ["trade", "orderbook"]:
        ch_root = raw_root / ch
        if not ch_root.exists():
            continue
        for p in ch_root.glob("date=*"):
            try:
                dates.append(p.name.split("date=")[1])
            except Exception:
                pass
    if dates:
        dates.sort()
        return dates[-1], "FALLBACK_TO_LATEST_AVAILABLE_PARTITION"
    return batch_date, "NO_WS_DATA_ANYWHERE"

def write_md(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main():
    # batch_date: KST yesterday unless overridden
    batch_date = os.environ.get("AUTOBOT_BATCH_DATE")
    if not batch_date:
        # KST yesterday
        kst_now = datetime.utcnow() + timedelta(hours=9)
        batch_date = (kst_now - timedelta(days=1)).strftime("%Y-%m-%d")
        batch_src = "DEFAULT_KST_YESTERDAY"
    else:
        batch_src = "ENV_OVERRIDE"

    raw_ws_root = ROOT / "data" / "raw_ws" / "upbit" / "public"
    meta_dir = ROOT / "data" / "raw_ws" / "upbit" / "_meta"
    raw_ticks_root = ROOT / "data" / "raw_ticks" / "upbit" / "trades"
    micro_root = ROOT / "data" / "parquet" / "micro_v1"

    ws_date, ws_reason = pick_ws_date(batch_date, raw_ws_root)

    # Run ws validate/stats for ws_date (HF2 behavior)
    # quarantine defaults: true, min-age-sec 300 (like your Windows pipeline)
    rc_val, out_val = run([
        str(ROOT/".venv/bin/python"), "-m", "autobot.cli",
        "collect", "ws-public", "validate",
        "--date", ws_date,
        "--raw-root", str(raw_ws_root),
        "--meta-dir", str(meta_dir),
        "--quarantine-corrupt", "true",
        "--min-age-sec", "300",
    ])
    rc_stats, out_stats = run([
        str(ROOT/".venv/bin/python"), "-m", "autobot.cli",
        "collect", "ws-public", "stats",
        "--date", ws_date,
        "--raw-root", str(raw_ws_root),
        "--meta-dir", str(meta_dir),
    ])

    ws_health = read_json(meta_dir / "ws_public_health.json")
    ws_validate_report = read_json(meta_dir / "ws_validate_report.json")

    micro_stats_rc, micro_stats_out = run([str(ROOT/".venv/bin/python"), "-m", "autobot.cli", "micro", "stats", "--out-root", str(micro_root)])
    micro_validate_rc, micro_validate_out = run([str(ROOT/".venv/bin/python"), "-m", "autobot.cli", "micro", "validate", "--out-root", str(micro_root)])
    micro_validate_report = read_json(micro_root / "_meta" / "validate_report.json")
    micro_aggregate_report = read_json(micro_root / "_meta" / "aggregate_report.json")

    # Paper smoke (10m) optional: skip if env says so
    smoke_enabled = os.environ.get("AUTOBOT_SMOKE", "1") == "1"
    smoke_json = None
    smoke_preview = ""
    if smoke_enabled:
        # run existing script if present, otherwise skip
        smoke_script = ROOT / "scripts" / "paper_micro_smoke.ps1"
        if smoke_script.exists():
            # NOTE: pwsh may not exist on Ubuntu; you likely won't have it.
            # We'll skip and just mark not attempted.
            pass
        # If you later install pwsh, you can wire it.
    # T15 gates:
    # policy gate uses last paper smoke in your Windows pipeline; on Linux we can only report "NOT_RUN" unless you add a Linux smoke.
    # We'll still compute a "policy gate" from last known JSON if exists.
    smoke_latest = read_json(ROOT / "logs" / "paper_micro_smoke" / "latest.json")
    if smoke_latest:
        # try extract
        micro_missing_ratio = smoke_latest.get("micro_missing_fallback_ratio")
        tier_unique = smoke_latest.get("tier_unique_count")
        rct = smoke_latest.get("replace_cancel_timeout_total")
        orders_sub = smoke_latest.get("orders_submitted")
    else:
        micro_missing_ratio = None
        tier_unique = None
        rct = None
        orders_sub = None

    policy_checks = []
    def chk(name, ok, detail):
        policy_checks.append((name, ok, detail))

    if micro_missing_ratio is None:
        chk("MICRO_MISSING_FALLBACK ratio < 10%", False, "NOT_AVAILABLE")
    else:
        chk("MICRO_MISSING_FALLBACK ratio < 10%", micro_missing_ratio < 0.10, f"actual={micro_missing_ratio}")
    if tier_unique is None:
        chk("tier_unique_count >= 2", False, "NOT_AVAILABLE")
    else:
        chk("tier_unique_count >= 2", tier_unique >= 2, f"actual={tier_unique}")
    if rct is None:
        chk("replace+cancel+timeout >= 1", False, "NOT_AVAILABLE")
    else:
        chk("replace+cancel+timeout >= 1", rct >= 1, f"actual={rct}")

    policy_pass = all(x[1] for x in policy_checks)
    policy_reason = "THRESHOLD_NOT_MET" if not policy_pass else "PASS"

    # data gate: use micro stats in aggregate_report if present, else DEFER if no ws parts for batch_date
    # We treat "has_ws_parts_for_batch_date" as existence check
    has_ws_parts_batch = exists_ws_parts(raw_ws_root, batch_date)

    # Try to pull ratios from micro stats output or reports if you already store them
    # We'll parse from validate_report if present; otherwise keep blank.
    book_ratio = None
    ws_ratio = None
    # if you store these in validate_report.json
    if micro_validate_report:
        book_ratio = micro_validate_report.get("book_available_ratio")
        ws_ratio = micro_validate_report.get("trade_source_ws_ratio")

    if not has_ws_parts_batch:
        data_gate_status = "DEFER"
        data_gate_reason = "NO_WS_PARTS_FOR_BATCH_DATE"
    else:
        # threshold
        ok_book = (book_ratio is not None and book_ratio >= 0.200)
        ok_ws = (ws_ratio is not None and ws_ratio >= 0.200)
        if ok_book and ok_ws:
            data_gate_status = "PASS"
            data_gate_reason = "PASS"
        else:
            data_gate_status = "FAIL"
            data_gate_reason = "THRESHOLD_NOT_MET"

    overall_pass = (policy_pass and data_gate_status == "PASS")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append(f"# DAILY_MICRO_REPORT_{batch_date}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- batch_date: {batch_date}")
    lines.append(f"- batch_date_source: {batch_src}")
    lines.append(f"- ws_date: {ws_date}")
    lines.append(f"- ws_date_reason: {ws_reason}")
    lines.append(f"- generated_at: {now}")
    lines.append("")
    # Orderbook verification summary
    lines.append("## Orderbook Verification (ws_date basis)")
    lines.append(f"- ws_date: {ws_date}")
    lines.append(f"- ws_date_reason: {ws_reason}")
    lines.append(f"- ws_validate_exit: {rc_val}")
    lines.append(f"- ws_stats_exit: {rc_stats}")
    if ws_validate_report:
        lines.append(f"- validate.checked={ws_validate_report.get('checked_files')} fail={ws_validate_report.get('fail_files')} parse_ok_ratio={ws_validate_report.get('parse_ok_ratio')}")
    if ws_health:
        lines.append(f"- health.connected={ws_health.get('connected')} subscribed_markets={ws_health.get('subscribed_markets_count')}")
    lines.append("")
    # Micro summary
    lines.append("## Micro (latest)")
    if micro_aggregate_report:
        lines.append(f"- micro.rows_written_total: {micro_aggregate_report.get('rows_written_total')}")
        lines.append(f"- micro.parts: {micro_aggregate_report.get('parts')}")
    if micro_validate_report:
        lines.append(f"- micro.validate ok={micro_validate_report.get('ok')} warn={micro_validate_report.get('warn')} fail={micro_validate_report.get('fail')}")
        lines.append(f"- book_available_ratio: {book_ratio}")
        lines.append(f"- trade_source_ws_ratio: {ws_ratio}")
    lines.append("")
    # T15 gates
    lines.append("## T15.1 Revalidation Gates (Policy / Data / Overall)")
    lines.append(f"- t15_policy_gate: {'PASS' if policy_pass else 'FAIL'} (reason={policy_reason})")
    for name, ok, detail in policy_checks:
        lines.append(f"  - {name}: {'PASS' if ok else 'FAIL'} ({detail})")
    lines.append(f"- t15_data_gate: {data_gate_status} (reason={data_gate_reason})")
    lines.append(f"  - batch_date_has_ws_parts: {has_ws_parts_batch}")
    lines.append(f"  - book_available_ratio >= 0.200: {'PASS' if (book_ratio is not None and book_ratio>=0.2) else 'FAIL'} (actual={book_ratio})")
    lines.append(f"  - trade_source_ws_ratio >= 0.200: {'PASS' if (ws_ratio is not None and ws_ratio>=0.2) else 'FAIL'} (actual={ws_ratio})")
    lines.append(f"- t15_overall_gate: {'PASS' if overall_pass else 'FAIL'}")
    lines.append("")
    # Write JSON gate report (like HF2)
    gate_report = {
        "batch_date": batch_date,
        "batch_date_source": batch_src,
        "ws_date": ws_date,
        "ws_date_reason": ws_reason,
        "policy_gate": {
            "pass": policy_pass,
            "reason": policy_reason,
            "checks": [{"name": n, "pass": ok, "detail": d} for n, ok, d in policy_checks],
        },
        "data_gate": {
            "status": data_gate_status,
            "reason": data_gate_reason,
            "has_ws_parts_for_batch_date": has_ws_parts_batch,
            "book_available_ratio": book_ratio,
            "trade_source_ws_ratio": ws_ratio,
        },
        "overall_gate": {"pass": overall_pass},
        "legacy": {"t15_1_revalidation_gate": overall_pass},
    }
    gate_json_path = micro_root / "_meta" / "daily_t15_gate_report.json"
    gate_json_path.write_text(json.dumps(gate_report, ensure_ascii=False, indent=2), encoding="utf-8")

    out_md = ROOT / "docs" / "reports" / f"DAILY_MICRO_REPORT_{batch_date}.md"
    write_md(out_md, lines)
    print(f"[daily-report] wrote: {out_md}")

if __name__ == "__main__":
    main()
