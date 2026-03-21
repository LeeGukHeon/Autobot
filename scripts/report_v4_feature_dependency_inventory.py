from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from autobot.features.feature_set_v4 import feature_columns_v4
from autobot.models.factor_block_selector import v4_factor_block_registry


@dataclass(frozen=True)
class BlockPolicy:
    producer: str
    live_inputs: tuple[str, ...]
    warmup_requirement: str
    needs_pre_3_4_history: bool
    legacy_code_dependency: bool
    notes: str


BLOCK_POLICIES: dict[str, BlockPolicy] = {
    "v3_base_core": BlockPolicy(
        producer="compute_base_features_v4_live_base",
        live_inputs=("candles_api_v1 1m bootstrap", "live/public ticker -> 1m rollup"),
        warmup_requirement="Post-3/4 in-window only; up to 37 base 5m bars for logret_36/vol_36, 36 bars for volume_z.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="Native v4 live-base contract. Data can come entirely from 3/4 onward if early-edge rows are allowed to warm up.",
    ),
    "v3_one_m_core": BlockPolicy(
        producer="aggregate_1m_for_base + join_1m_aggregate (v4 live base path)",
        live_inputs=("candles_api_v1 1m bootstrap", "live/public ticker -> 1m rollup"),
        warmup_requirement="Current base bucket plus dense 1m composition; required_bars=5 and effective 1m return moments need immediate prior 1m context.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="No old history requirement; depends on 1m continuity and synth-minute handling.",
    ),
    "v3_high_tf_core": BlockPolicy(
        producer="compute_high_tf_features + join_high_tf_asof (v4 live base path)",
        live_inputs=("base 5m candles from 1m rollup",),
        warmup_requirement="Post-3/4 in-window only; worst case is tf240m_trend_slope/regime needing 9 x 240m bars (~36h).",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="No pre-3/4 history required, but first ~36h of the window cannot fully populate every 240m feature.",
    ),
    "v3_micro_core": BlockPolicy(
        producer="MicroSnapshotProvider -> prefixed micro columns",
        live_inputs=("micro_v1 / live WS trade", "live WS orderbook", "current ref price"),
        warmup_requirement="Current snapshot only; effective quality depends on micro coverage accumulation within the live snapshot window.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="No pre-3/4 history requirement. Missing live micro is currently zero-filled upstream.",
    ),
    "v4_spillover_breadth": BlockPolicy(
        producer="attach_spillover_breadth_features_v4",
        live_inputs=("same-ts cross-sectional rows", "leader BTC/ETH rows", "base returns/turnover"),
        warmup_requirement="Same-timestamp only once base rows exist.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="No old-history pressure; depends on universe breadth and leader availability at the same timestamp.",
    ),
    "v4_periodicity": BlockPolicy(
        producer="attach_periodicity_features_v4",
        live_inputs=("ts_ms only",),
        warmup_requirement="None.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="Pure timestamp-derived features.",
    ),
    "v4_trend_volume": BlockPolicy(
        producer="attach_trend_volume_features_v4",
        live_inputs=("v4 live-base/high-tf features", "current and rolling volume stats", "leader basket features"),
        warmup_requirement="Post-3/4 in-window only; dominated by upstream 240m features (~36h) plus 12-bar rolling volume_z smoothing.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="Not old-data dependent and now fed by the native v4 live-base contract.",
    ),
    "v4_order_flow_panel_v1": BlockPolicy(
        producer="attach_order_flow_panel_v1",
        live_inputs=("micro trade/book snapshot columns",),
        warmup_requirement="Post-3/4 in-window only; 12 bars max for rolling persistence terms.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="No pre-3/4 history requirement. Warmup comes from accumulating enough recent micro snapshots.",
    ),
    "v4_ctrend_v1": BlockPolicy(
        producer="build_ctrend_v1_daily_feature_frame",
        live_inputs=("daily 5m candle history", "candles_api_v1 recent rows", "candles_v1 fallback warmup"),
        warmup_requirement="Requires long daily history; ctrend contract asks for 240-day lookback and includes 200-day MA/volume MA features.",
        needs_pre_3_4_history=True,
        legacy_code_dependency=False,
        notes="This is the only block that hard-requires substantial pre-3/4 market history if you want features available near the start of the 3/4 window.",
    ),
    "v4_interactions": BlockPolicy(
        producer="attach_interaction_features_v4",
        live_inputs=("current-row v4 live-base/micro/spillover/trend features",),
        warmup_requirement="No extra warmup beyond upstream inputs.",
        needs_pre_3_4_history=False,
        legacy_code_dependency=False,
        notes="No independent old-history requirement; now fed by native v4 live-base upstream features.",
    ),
}


def _load_feature_columns(args: argparse.Namespace) -> tuple[str, ...]:
    if args.train_config:
        path = Path(args.train_config)
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        cols = tuple(str(item).strip() for item in (raw.get("feature_columns") or []) if str(item).strip())
        if not cols:
            raise ValueError(f"feature_columns missing from {path}")
        return cols
    return feature_columns_v4(high_tfs=("15m", "60m", "240m"))


def _build_rows(feature_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    block_registry = v4_factor_block_registry(feature_columns=feature_columns, high_tfs=("15m", "60m", "240m"))
    feature_to_block: dict[str, str] = {}
    for block in block_registry:
        for col in block.feature_columns:
            if col in feature_to_block:
                raise ValueError(f"feature assigned twice: {col}")
            feature_to_block[col] = block.block_id
    missing = [col for col in feature_columns if col not in feature_to_block]
    if missing:
        raise ValueError(f"unclassified features: {missing}")

    rows: list[dict[str, Any]] = []
    for feature in feature_columns:
        block_id = feature_to_block[feature]
        block = next(item for item in block_registry if item.block_id == block_id)
        policy = BLOCK_POLICIES[block_id]
        rows.append(
            {
                "feature_name": feature,
                "block_id": block_id,
                "block_label": block.label,
                "protected_block": bool(block.protected),
                "source_contracts": list(block.source_contracts),
                "producer": policy.producer,
                "live_inputs": list(policy.live_inputs),
                "warmup_requirement": policy.warmup_requirement,
                "needs_pre_3_4_history": bool(policy.needs_pre_3_4_history),
                "legacy_code_dependency": bool(policy.legacy_code_dependency),
                "notes": policy.notes,
            }
        )
    return rows


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    pre_history = [row for row in rows if row["needs_pre_3_4_history"]]
    legacy_code = [row for row in rows if row["legacy_code_dependency"]]
    block_counts: dict[str, int] = {}
    for row in rows:
        block_counts[row["block_id"]] = int(block_counts.get(row["block_id"], 0)) + 1
    return {
        "feature_count_total": total,
        "needs_pre_3_4_history_count": len(pre_history),
        "post_3_4_only_or_in_window_warmup_count": total - len(pre_history),
        "legacy_code_dependency_count": len(legacy_code),
        "non_legacy_code_dependency_count": total - len(legacy_code),
        "block_counts": dict(sorted(block_counts.items())),
        "pre_3_4_history_blocks": sorted({row["block_id"] for row in pre_history}),
    }


def _write_markdown(path: Path, rows: list[dict[str, Any]], summary: dict[str, Any], source_label: str) -> None:
    lines: list[str] = []
    lines.append("# V4 Feature Dependency Inventory")
    lines.append("")
    lines.append(f"- Source: `{source_label}`")
    lines.append(f"- Total features: `{summary['feature_count_total']}`")
    lines.append(f"- Features that truly need pre-3/4 history: `{summary['needs_pre_3_4_history_count']}`")
    lines.append(f"- Features that can be built from 3/4 onward with only in-window warmup: `{summary['post_3_4_only_or_in_window_warmup_count']}`")
    lines.append(f"- Features still tied to legacy v3 code paths/contracts: `{summary['legacy_code_dependency_count']}`")
    lines.append("")
    lines.append("## Block Counts")
    lines.append("")
    for block_id, count in summary["block_counts"].items():
        lines.append(f"- `{block_id}`: `{count}`")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append("- The true pre-3/4 history blocker is `v4_ctrend_v1`.")
    lines.append("- The rest require only bounded in-window warmup after 2026-03-04.")
    lines.append("- So the clean migration path is:")
    lines.append("  1. remove/replace `ctrend_v1` if you want a strict 3/4-forward-only runtime contract")
    lines.append("  2. keep auditing bounded warmup behavior, especially 240m high-tf coverage and one_m continuity")
    lines.append("")
    lines.append("## Feature Table")
    lines.append("")
    lines.append("| feature | block | pre-3/4 history | legacy code dep | producer |")
    lines.append("|---|---|---:|---:|---|")
    for row in rows:
        lines.append(
            f"| `{row['feature_name']}` | `{row['block_id']}` | `{str(row['needs_pre_3_4_history']).lower()}` | `{str(row['legacy_code_dependency']).lower()}` | `{row['producer']}` |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Report V4 feature dependency inventory and legacy reliance.")
    parser.add_argument("--train-config", type=str, default="", help="Optional train_config.yaml path to read actual feature_columns from.")
    parser.add_argument("--json-out", type=str, default="", help="Optional JSON output path.")
    parser.add_argument("--md-out", type=str, default="", help="Optional Markdown output path.")
    args = parser.parse_args()

    feature_columns = _load_feature_columns(args)
    rows = _build_rows(feature_columns)
    summary = _build_summary(rows)
    payload = {
        "source": str(Path(args.train_config).resolve()) if args.train_config else "feature_columns_v4_contract",
        "summary": summary,
        "rows": rows,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.md_out:
        _write_markdown(Path(args.md_out), rows, summary, payload["source"])


if __name__ == "__main__":
    main()
