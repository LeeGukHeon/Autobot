from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .paired_reporting import build_paired_paper_report, write_paired_paper_report


def run_paired_paper_harness(
    *,
    champion_run_dir: Path,
    challenger_run_dir: Path,
    output_path: Path | None = None,
) -> dict[str, Any]:
    report = build_paired_paper_report(
        champion_run_dir=Path(champion_run_dir),
        challenger_run_dir=Path(challenger_run_dir),
    )
    if output_path is not None:
        write_paired_paper_report(report=report, output_path=Path(output_path))
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a paired paper comparison report from two paper run directories.")
    parser.add_argument("--champion-run-dir", required=True)
    parser.add_argument("--challenger-run-dir", required=True)
    parser.add_argument("--output-path", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    output_path = Path(args.output_path) if str(args.output_path).strip() else None
    report = run_paired_paper_harness(
        champion_run_dir=Path(args.champion_run_dir),
        challenger_run_dir=Path(args.challenger_run_dir),
        output_path=output_path,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
