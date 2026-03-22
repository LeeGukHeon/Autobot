from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from autobot.ops.live_execution_override_audit import (
    DEFAULT_DB_PATH,
    DEFAULT_MODEL_FAMILY,
    DEFAULT_REGISTRY_ROOT,
    build_live_execution_override_audit,
    write_live_execution_override_audit,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a live execution override audit report from live_state.db and registry artifacts."
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--registry-root", default=str(DEFAULT_REGISTRY_ROOT))
    parser.add_argument("--model-family", default=DEFAULT_MODEL_FAMILY)
    parser.add_argument("--model-ref")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--attempt-limit", type=int, default=5000)
    parser.add_argument("--example-limit", type=int, default=12)
    parser.add_argument("--output-dir", default="logs/live_execution_override_audit")
    parser.add_argument("--print-json", action="store_true")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    payload = build_live_execution_override_audit(
        db_path=Path(str(args.db_path)),
        registry_root=Path(str(args.registry_root)),
        model_family=str(args.model_family).strip() or DEFAULT_MODEL_FAMILY,
        model_ref=(str(args.model_ref).strip() if args.model_ref else None),
        lookback_days=max(int(args.lookback_days), 1),
        attempt_limit=max(int(args.attempt_limit), 1),
        example_limit=max(int(args.example_limit), 1),
    )
    paths = write_live_execution_override_audit(
        payload=payload,
        output_dir=Path(str(args.output_dir)),
    )

    print(f"[live-exec-audit] json={paths['json_path']}")
    print(f"[live-exec-audit] md={paths['md_path']}")
    print(f"[live-exec-audit] latest_json={paths['latest_json_path']}")
    print(f"[live-exec-audit] latest_md={paths['latest_md_path']}")
    if args.print_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
