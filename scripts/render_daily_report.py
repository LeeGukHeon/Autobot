#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

def read_json(p: Path):
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def main():
    # KST 기준 batch_date는 daily_micro_pipeline.sh에서 환경변수로 넘겨줍니다.
    import os
    batch_date = os.environ.get("AUTOBOT_BATCH_DATE", "")
    if not batch_date:
        raise SystemExit("AUTOBOT_BATCH_DATE is required")

    reports_dir = ROOT / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_md = reports_dir / f"DAILY_MICRO_REPORT_{batch_date}.md"

    # 여러분 프로젝트에서 이미 생성되는 메타 파일들
    candles_collect = read_json(ROOT / "data" / "collect" / "_meta" / "candle_collect_report.json")
    candles_validate = read_json(ROOT / "data" / "collect" / "_meta" / "candle_validate_report.json")
    ticks_collect = read_json(ROOT / "data" / "raw_ticks" / "upbit" / "_meta" / "ticks_collect_report.json")
    micro_agg = read_json(ROOT / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json")
    micro_val = read_json(ROOT / "data" / "parquet" / "micro_v1" / "_meta" / "validate_report.json")
    ws_health = read_json(ROOT / "data" / "raw_ws" / "upbit" / "_meta" / "ws_public_health.json")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append(f"# DAILY_MICRO_REPORT_{batch_date}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- generated_at: {now}")
    lines.append(f"- batch_date: {batch_date}")
    lines.append("")

    # 최소 요약만이라도
    if ws_health:
        lines.append("## WS Health Snapshot")
        lines.append(f"- connected: {ws_health.get('connected')}")
        lines.append(f"- subscribed_markets_count: {ws_health.get('subscribed_markets_count')}")
        wr = ws_health.get("written_rows", {})
        lines.append(f"- written_rows.total: {wr.get('total')}")
        lines.append("")

    if candles_collect:
        lines.append("## Candles Daily Top-up (latest)")
        lines.append(f"- processed: {candles_collect.get('processed')}")
        lines.append(f"- ok/warn/fail: {candles_collect.get('ok')}/{candles_collect.get('warn')}/{candles_collect.get('fail')}")
        lines.append("")

    if ticks_collect:
        lines.append("## Ticks Daily (latest)")
        lines.append(f"- processed: {ticks_collect.get('processed')}")
        lines.append(f"- ok/warn/fail: {ticks_collect.get('ok')}/{ticks_collect.get('warn')}/{ticks_collect.get('fail')}")
        lines.append("")

    if micro_agg:
        lines.append("## Micro Aggregate (latest)")
        lines.append(f"- rows_written_total: {micro_agg.get('rows_written_total')}")
        lines.append(f"- parts: {micro_agg.get('parts')}")
        lines.append("")

    if micro_val:
        lines.append("## Micro Validate (latest)")
        lines.append(f"- ok/warn/fail: {micro_val.get('ok')}/{micro_val.get('warn')}/{micro_val.get('fail')}")
        lines.append("")

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[daily-report] wrote: {out_md}")

if __name__ == "__main__":
    main()
