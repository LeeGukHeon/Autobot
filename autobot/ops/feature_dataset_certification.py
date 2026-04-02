"""Features dataset certification artifact builders."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autobot.common.data_quality_budget import (
    resolve_market_quality_budget,
    summarize_market_quality_budget,
)

from .data_contract_registry import build_data_contract_registry


DEFAULT_CERTIFICATION_REL_PATH = (
    Path("data") / "features" / "features_v4" / "_meta" / "feature_dataset_certification.json"
)


def build_feature_dataset_certification(
    *,
    project_root: str | Path,
    feature_set: str = "v4",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    feature_set_value = str(feature_set).strip().lower() or "v4"
    dataset_root = root / "data" / "features" / f"features_{feature_set_value}"
    meta_root = dataset_root / "_meta"
    build_report = _load_json(meta_root / "build_report.json")
    validate_report = _load_json(meta_root / "validate_report.json")
    parity_report = _load_json(meta_root / "live_feature_parity_report.json")
    micro_root = Path(str(build_report.get("micro_root") or "")).resolve() if str(build_report.get("micro_root") or "").strip() else None
    micro_validate_report = _load_json((micro_root / "_meta" / "validate_report.json")) if micro_root is not None else {}
    micro_aggregate_report = _load_json((micro_root / "_meta" / "aggregate_report.json")) if micro_root is not None else {}
    registry = build_data_contract_registry(project_root=root)
    feature_contract = _find_contract_entry(registry, contract_id=f"feature_dataset:{dataset_root.name}")
    live_contract = _find_contract_entry(registry, contract_id="live:features_v4_online")
    universe_selection = dict(build_report.get("universe_selection") or {}) if isinstance(build_report.get("universe_selection"), dict) else {}
    universe_candidates = _market_keyed_rows(universe_selection.get("candidates"))
    build_details = _market_keyed_rows(build_report.get("details"))
    validate_details = _market_keyed_rows(validate_report.get("details"))

    build_status = str(build_report.get("status") or "").strip().upper()
    build_present = bool(build_report)
    build_pass = build_present and build_status in {"PASS", "OK", "WARN"}

    validate_status = str(validate_report.get("status") or "").strip().upper()
    validate_present = bool(validate_report)
    validate_pass = validate_present and validate_status in {"PASS", "OK"} and _safe_int(validate_report.get("fail_files")) <= 0

    parity_status = str(parity_report.get("status") or "").strip().upper()
    parity_present = bool(parity_report)
    parity_pass = (
        parity_present
        and parity_status == "PASS"
        and bool(parity_report.get("acceptable", False))
        and _safe_int(parity_report.get("sampled_pairs")) > 0
    )

    reasons: list[str] = []
    if not build_present:
        reasons.append("FEATURE_BUILD_REPORT_MISSING")
    elif not build_pass:
        reasons.append("FEATURE_BUILD_REPORT_FAILED")
    if not validate_present:
        reasons.append("FEATURE_VALIDATE_REPORT_MISSING")
    elif not validate_pass:
        reasons.append("FEATURE_VALIDATE_REPORT_FAILED")
    if not parity_present:
        reasons.append("LIVE_FEATURE_PARITY_REPORT_MISSING")
    elif not parity_pass:
        reasons.append("LIVE_FEATURE_PARITY_REPORT_FAILED")
    if not feature_contract:
        reasons.append("FEATURE_CONTRACT_REGISTRY_ENTRY_MISSING")
    if not live_contract:
        reasons.append("LIVE_FEATURE_CONTRACT_REGISTRY_ENTRY_MISSING")

    pass_status = len(reasons) == 0
    quality_budget = {
        "rows_dropped_no_micro": _safe_int(build_report.get("rows_dropped_no_micro")),
        "rows_dropped_one_m_before_densify": _safe_int(build_report.get("rows_dropped_one_m_before_densify")),
        "rows_dropped_one_m": _safe_int(build_report.get("rows_dropped_one_m")),
        "rows_rescued_by_one_m_densify": _safe_int(build_report.get("rows_rescued_by_one_m_densify")),
        "one_m_synth_ratio_p50": _safe_optional_float(build_report.get("one_m_synth_ratio_p50")),
        "one_m_synth_ratio_p90": _safe_optional_float(build_report.get("one_m_synth_ratio_p90")),
        "validate_fail_files": _safe_int(validate_report.get("fail_files")),
        "validate_warn_files": _safe_int(validate_report.get("warn_files")),
        "leakage_smoke": str(validate_report.get("leakage_smoke") or "").strip(),
        "staleness_fail_rows": _safe_int(validate_report.get("staleness_fail_rows")),
        "dropped_rows_no_micro": _safe_int(validate_report.get("dropped_rows_no_micro")),
        "micro_validate_fail_files": _safe_int(micro_validate_report.get("fail_files")),
        "micro_validate_parse_ok_ratio": _safe_optional_float(micro_validate_report.get("parse_ok_ratio")),
        "micro_validate_short_trade_coverage_ratio": _safe_optional_float(micro_validate_report.get("short_trade_coverage_ratio")),
        "micro_validate_short_book_coverage_ratio": _safe_optional_float(micro_validate_report.get("short_book_coverage_ratio")),
        "micro_available_ratio": _safe_optional_float(micro_aggregate_report.get("micro_available_ratio")),
        "micro_trade_source_ws_ratio": _safe_optional_float(((micro_aggregate_report.get("trade_source_ratio") or {}).get("ws"))),
        "parity_sampled_pairs": _safe_int(parity_report.get("sampled_pairs")),
        "parity_hard_gate_fail_count": _safe_int(parity_report.get("hard_gate_fail_count")),
        "parity_missing_feature_columns_total": _safe_int(parity_report.get("missing_feature_columns_total")),
        "parity_max_abs_diff_overall": _safe_optional_float(parity_report.get("max_abs_diff_overall")),
    }
    market_quality_budget = _build_market_quality_budget(
        build_details=build_details,
        validate_details=validate_details,
        universe_candidates=universe_candidates,
    )
    quality_budget_summary = summarize_market_quality_budget(market_quality_budget)
    lineage = {
        "dataset_name": dataset_root.name,
        "feature_contract_id": str(feature_contract.get("contract_id") or "") if feature_contract else "",
        "feature_source_contract_ids": list(feature_contract.get("source_contract_ids") or []) if feature_contract else [],
        "feature_source_roots": list(feature_contract.get("source_roots") or []) if feature_contract else [],
        "live_contract_id": str(live_contract.get("contract_id") or "") if live_contract else "",
        "live_source_contract_ids": list(live_contract.get("source_contract_ids") or []) if live_contract else [],
        "base_candles_root": str(build_report.get("base_candles_root") or "").strip(),
        "micro_root": str(build_report.get("micro_root") or "").strip(),
        "micro_validate_report_path": str((micro_root / "_meta" / "validate_report.json")) if micro_root is not None else "",
        "micro_aggregate_report_path": str((micro_root / "_meta" / "aggregate_report.json")) if micro_root is not None else "",
    }
    coverage_window = {
        "start": str(build_report.get("effective_start") or build_report.get("requested_start") or "").strip(),
        "end": str(build_report.get("effective_end") or build_report.get("requested_end") or "").strip(),
        "requested_start": str(build_report.get("requested_start") or "").strip(),
        "requested_end": str(build_report.get("requested_end") or "").strip(),
    }
    return {
        "policy": "feature_dataset_certification_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project_root": str(root),
        "feature_set": feature_set_value,
        "dataset_root": str(dataset_root),
        "certification_path_default": str((root / DEFAULT_CERTIFICATION_REL_PATH).resolve()),
        "artifacts": {
            "build_report": str(meta_root / "build_report.json"),
            "validate_report": str(meta_root / "validate_report.json"),
            "live_feature_parity_report": str(meta_root / "live_feature_parity_report.json"),
            "data_contract_registry": str(root / "data" / "_meta" / "data_contract_registry.json"),
            "micro_validate_report": str((micro_root / "_meta" / "validate_report.json")) if micro_root is not None else "",
            "micro_aggregate_report": str((micro_root / "_meta" / "aggregate_report.json")) if micro_root is not None else "",
        },
        "checks": {
            "build_report_present": build_present,
            "build_report_pass": build_pass,
            "validate_report_present": validate_present,
            "validate_report_pass": validate_pass,
            "live_feature_parity_report_present": parity_present,
            "live_feature_parity_report_pass": parity_pass,
            "feature_contract_registry_entry_present": bool(feature_contract),
            "live_feature_contract_registry_entry_present": bool(live_contract),
        },
        "coverage_window": coverage_window,
        "lineage": lineage,
        "quality_budget": quality_budget,
        "market_quality_budget": market_quality_budget,
        "quality_budget_summary": quality_budget_summary,
        "reasons": reasons,
        "pass": pass_status,
        "status": "PASS" if pass_status else "FAIL",
    }


def write_feature_dataset_certification(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
    feature_set: str = "v4",
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_CERTIFICATION_REL_PATH)
    payload = build_feature_dataset_certification(project_root=root, feature_set=feature_set)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _find_contract_entry(registry: dict[str, Any], *, contract_id: str) -> dict[str, Any]:
    for item in registry.get("entries") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("contract_id") or "").strip() == str(contract_id).strip():
            return dict(item)
    return {}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _market_keyed_rows(rows: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(rows, list):
        return out
    for item in rows:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market") or "").strip().upper()
        if not market:
            continue
        out[market] = dict(item)
    return out


def _build_market_quality_budget(
    *,
    build_details: dict[str, dict[str, Any]],
    validate_details: dict[str, dict[str, Any]],
    universe_candidates: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    markets = sorted(set(build_details.keys()) | set(validate_details.keys()) | set(universe_candidates.keys()))
    rows: list[dict[str, Any]] = []
    for market in markets:
        build_item = dict(build_details.get(market) or {})
        validate_item = dict(validate_details.get(market) or {})
        universe_item = dict(universe_candidates.get(market) or {})
        rows.append(
            resolve_market_quality_budget(
                market=market,
                one_m_synth_ratio_mean=_safe_optional_float(build_item.get("one_m_synth_ratio_mean")),
                rows_dropped_no_micro=_safe_int(build_item.get("rows_dropped_no_micro")),
                validate_status=str(validate_item.get("status") or "").strip(),
                leakage_fail_rows=_safe_int(validate_item.get("leakage_fail_rows")),
                stale_rows=_safe_int(validate_item.get("stale_rows")),
                universe_quality_weight=_safe_optional_float(universe_item.get("quality_weight")),
                universe_quality_score=_safe_optional_float(universe_item.get("score")),
                selected_for_universe=bool(universe_item.get("selected", False)),
            )
        )
    return rows


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build feature dataset certification artifact.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--feature-set", default="v4")
    parser.add_argument("--output-path", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    path = write_feature_dataset_certification(
        project_root=Path(args.project_root),
        output_path=(Path(args.output_path) if str(args.output_path).strip() else None),
        feature_set=str(args.feature_set).strip().lower() or "v4",
    )
    print(f"[ops][feature-dataset-certification] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
