"""Paper/live divergence artifact builders."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PAPER_LIVE_DIVERGENCE_VERSION = 1


def paper_live_divergence_latest_path(*, project_root: Path, unit_name: str) -> Path:
    return Path(project_root) / "logs" / "paper_live_divergence" / _unit_slug(unit_name) / "latest.json"


def write_paper_live_divergence_report(*, latest_path: Path, payload: dict[str, Any]) -> Path:
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return latest_path


def build_paper_live_divergence_report(
    *,
    project_root: Path,
    unit_name: str,
    lane: str,
    run_id: str | None,
    ts_ms: int,
    min_matched_opportunities: int = 1,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    live_path = root / "logs" / "opportunity_log" / _unit_slug(unit_name) / "latest.jsonl"
    paired_report = _resolve_paired_paper_report(project_root=root)
    paper_source = _resolve_paper_source(
        project_root=root,
        paired_report=paired_report,
        lane=lane,
        run_id=run_id,
    )
    infra_parity = _load_live_feature_parity_diagnostic(project_root=root)
    live_rows = _load_jsonl(live_path)
    if not live_rows:
        return _insufficient_payload(
            unit_name=unit_name,
            lane=lane,
            run_id=run_id,
            ts_ms=ts_ms,
            live_path=live_path,
            paper_source=paper_source,
            infra_parity=infra_parity,
            reason_codes=["LIVE_OPPORTUNITY_LOG_MISSING"],
            status="insufficient_evidence",
        )
    if not paper_source.get("available"):
        return _insufficient_payload(
            unit_name=unit_name,
            lane=lane,
            run_id=run_id,
            ts_ms=ts_ms,
            live_path=live_path,
            paper_source=paper_source,
            infra_parity=infra_parity,
            reason_codes=list(paper_source.get("reason_codes") or ["PAPER_RUN_SOURCE_UNAVAILABLE"]),
            status="insufficient_evidence",
        )
    paper_rows = _load_jsonl(Path(str(paper_source.get("opportunity_log_path") or "")))
    if not paper_rows:
        return _insufficient_payload(
            unit_name=unit_name,
            lane=lane,
            run_id=run_id,
            ts_ms=ts_ms,
            live_path=live_path,
            paper_source=paper_source,
            infra_parity=infra_parity,
            reason_codes=["PAPER_OPPORTUNITY_LOG_MISSING"],
            status="insufficient_evidence",
        )

    live_index = _build_opportunity_index(live_rows)
    paper_index = _build_opportunity_index(paper_rows)
    matched_keys = sorted(
        set(live_index) & set(paper_index),
        key=lambda item: (
            int(_safe_int((live_index.get(item) or {}).get("ts_ms")) or _safe_int((paper_index.get(item) or {}).get("ts_ms")) or 0),
            str((live_index.get(item) or {}).get("market") or (paper_index.get(item) or {}).get("market") or ""),
            str(item),
        ),
    )
    min_required = max(int(min_matched_opportunities), 1)
    if len(matched_keys) < min_required:
        return _insufficient_payload(
            unit_name=unit_name,
            lane=lane,
            run_id=run_id,
            ts_ms=ts_ms,
            live_path=live_path,
            paper_source=paper_source,
            infra_parity=infra_parity,
            reason_codes=["INSUFFICIENT_MATCHED_OPPORTUNITIES"],
            status="insufficient_evidence",
            matching={
                "live_total": len(live_index),
                "paper_total": len(paper_index),
                "matched_opportunities": len(matched_keys),
                "min_matched_opportunities": min_required,
                "live_only_opportunities": max(len(live_index) - len(matched_keys), 0),
                "paper_only_opportunities": max(len(paper_index) - len(matched_keys), 0),
                "pair_ready": False,
            },
        )

    matched_records: list[dict[str, Any]] = []
    feature_match_count = 0
    feature_total = 0
    decision_match_count = 0
    decision_total = 0
    disagreement_examples: list[dict[str, Any]] = []
    for key in matched_keys:
        live_item = live_index[key]
        paper_item = paper_index[key]
        live_feature_hash = str(live_item.get("feature_hash") or "").strip()
        paper_feature_hash = str(paper_item.get("feature_hash") or "").strip()
        feature_hash_match = None
        if live_feature_hash or paper_feature_hash:
            feature_hash_match = live_feature_hash == paper_feature_hash
            feature_total += 1
            if feature_hash_match:
                feature_match_count += 1
        live_signature = _decision_signature(live_item)
        paper_signature = _decision_signature(paper_item)
        decision_match = live_signature == paper_signature
        decision_total += 1
        if decision_match:
            decision_match_count += 1
        record = {
            "opportunity_key": str(key),
            "opportunity_id": _safe_text(live_item.get("opportunity_id") or paper_item.get("opportunity_id")),
            "ts_ms": _safe_int(live_item.get("ts_ms")) or _safe_int(paper_item.get("ts_ms")),
            "market": _safe_text(live_item.get("market") or paper_item.get("market")).upper(),
            "side": _safe_text(live_item.get("side") or paper_item.get("side")).lower(),
            "feature_hash_match": feature_hash_match,
            "decision_match": bool(decision_match),
            "live": _record_projection(live_item),
            "paper": _record_projection(paper_item),
        }
        matched_records.append(record)
        if (feature_hash_match is False or not decision_match) and len(disagreement_examples) < 20:
            disagreement_examples.append(record)

    live_only = sorted(set(live_index) - set(matched_keys), key=str)
    paper_only = sorted(set(paper_index) - set(matched_keys), key=str)
    feature_hash_match_ratio = _safe_ratio(feature_match_count, feature_total)
    feature_divergence_rate = 1.0 - feature_hash_match_ratio if feature_total > 0 else None
    decision_match_ratio = _safe_ratio(decision_match_count, decision_total)
    decision_divergence_rate = 1.0 - decision_match_ratio if decision_total > 0 else None
    pair_ready = bool(matched_keys) and not live_only and not paper_only
    return {
        "artifact_version": PAPER_LIVE_DIVERGENCE_VERSION,
        "policy": "paper_live_divergence_v1",
        "status": "ready",
        "ts_ms": int(ts_ms),
        "unit_name": str(unit_name).strip(),
        "lane": str(lane).strip(),
        "run_id": _safe_text(run_id) or None,
        "live_source": {
            "available": True,
            "opportunity_log_path": str(live_path),
            "record_count": len(live_index),
        },
        "paper_source": paper_source,
        "infra_parity_diagnostic": infra_parity,
        "matching": {
            "live_total": len(live_index),
            "paper_total": len(paper_index),
            "matched_opportunities": len(matched_keys),
            "min_matched_opportunities": min_required,
            "live_only_opportunities": len(live_only),
            "paper_only_opportunities": len(paper_only),
            "pair_ready": bool(pair_ready),
        },
        "feature_divergence": {
            "available": bool(feature_total > 0),
            "matched_feature_rows": int(feature_total),
            "feature_hash_match_ratio": feature_hash_match_ratio if feature_total > 0 else None,
            "feature_divergence_rate": feature_divergence_rate,
        },
        "decision_divergence": {
            "available": bool(decision_total > 0),
            "matched_decision_rows": int(decision_total),
            "decision_match_ratio": decision_match_ratio if decision_total > 0 else None,
            "decision_divergence_rate": decision_divergence_rate,
        },
        "matched_records": matched_records,
        "reason_codes": [],
        "disagreement_examples": disagreement_examples,
    }


def load_paper_live_divergence_report(
    *,
    project_root: Path,
    unit_name: str,
) -> dict[str, Any]:
    latest_path = paper_live_divergence_latest_path(project_root=project_root, unit_name=unit_name)
    return _load_json(latest_path)


def _resolve_paper_source(
    *,
    project_root: Path,
    paired_report: dict[str, Any],
    lane: str,
    run_id: str | None,
) -> dict[str, Any]:
    paired_candidate = _paired_report_candidate_source(
        project_root=project_root,
        paired_report=paired_report,
        lane=lane,
        run_id=run_id,
    )
    if paired_candidate.get("available"):
        return paired_candidate
    scanned_candidate = _scan_paper_runs(project_root=project_root, lane=lane, run_id=run_id)
    if scanned_candidate.get("available"):
        return scanned_candidate
    reason_codes = list(dict.fromkeys(
        [*list(paired_candidate.get("reason_codes") or []), *list(scanned_candidate.get("reason_codes") or [])]
    ))
    return {
        "available": False,
        "source_kind": "unavailable",
        "reason_codes": reason_codes or ["PAPER_RUN_SOURCE_UNAVAILABLE"],
        "run_dir": None,
        "run_id": None,
        "paper_runtime_role": None,
        "paper_lane": None,
        "paper_runtime_model_run_id": None,
        "opportunity_log_path": None,
    }


def _paired_report_candidate_source(
    *,
    project_root: Path,
    paired_report: dict[str, Any],
    lane: str,
    run_id: str | None,
) -> dict[str, Any]:
    if not paired_report:
        return {"available": False, "reason_codes": ["PAIRED_PAPER_REPORT_MISSING"]}
    lane_value = str(lane).strip().lower()
    run_id_value = str(run_id or "").strip()
    scored: list[tuple[int, dict[str, Any]]] = []
    for role_name in ("challenger", "champion"):
        header = dict(paired_report.get(role_name) or {})
        run_dir_text = _safe_text(header.get("run_dir"))
        if not run_dir_text:
            continue
        score = 0
        model_run_id_matches = bool(run_id_value and _safe_text(header.get("paper_runtime_model_run_id")) == run_id_value)
        run_id_matches = bool(run_id_value and _safe_text(header.get("run_id")) == run_id_value)
        if run_id_value:
            if model_run_id_matches:
                score += 100
            if run_id_matches:
                score += 80
            if not model_run_id_matches and not run_id_matches:
                continue
        if "candidate" in lane_value and role_name == "challenger":
            score += 20
        if "champion" in lane_value and role_name == "champion":
            score += 20
        scored.append((score, header))
    if not scored:
        return {"available": False, "reason_codes": ["PAIRED_PAPER_REPORT_UNUSABLE"]}
    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, header = scored[0]
    if best_score <= 0:
        return {"available": False, "reason_codes": ["PAIRED_PAPER_REPORT_NO_MATCHING_RUN"]}
    run_dir = Path(_safe_text(header.get("run_dir")))
    if not run_dir.is_absolute():
        run_dir = (project_root / run_dir).resolve()
    else:
        run_dir = run_dir.resolve()
    opportunity_log_path = run_dir / "opportunity_log.jsonl"
    if not opportunity_log_path.exists():
        return {"available": False, "reason_codes": ["PAIRED_PAPER_OPPORTUNITY_LOG_MISSING"]}
    return {
        "available": True,
        "source_kind": "paired_paper",
        "run_dir": str(run_dir),
        "run_id": _safe_text(header.get("run_id")) or run_dir.name,
        "paper_runtime_role": _safe_text(header.get("paper_runtime_role")) or None,
        "paper_lane": _safe_text(header.get("paper_lane")) or None,
        "paper_runtime_model_run_id": _safe_text(header.get("paper_runtime_model_run_id")) or None,
        "opportunity_log_path": str(opportunity_log_path),
        "reason_codes": [],
    }


def _scan_paper_runs(*, project_root: Path, lane: str, run_id: str | None) -> dict[str, Any]:
    runs_root = Path(project_root) / "data" / "paper" / "runs"
    if not runs_root.exists():
        return {"available": False, "reason_codes": ["PAPER_RUNS_ROOT_MISSING"]}
    run_id_value = str(run_id or "").strip()
    lane_value = str(lane).strip().lower()
    candidates: list[tuple[float, dict[str, Any]]] = []
    for run_dir in sorted((path for path in runs_root.glob("paper-*") if path.is_dir()), key=lambda item: item.stat().st_mtime, reverse=True):
        metadata = _load_paper_run_metadata(run_dir)
        if not metadata:
            continue
        metadata_run_id = _safe_text(metadata.get("paper_runtime_model_run_id"))
        if run_id_value and metadata_run_id != run_id_value:
            continue
        metadata_lane = _safe_text(metadata.get("paper_lane")).lower()
        metadata_role = _safe_text(metadata.get("paper_runtime_role")).lower()
        if "candidate" in lane_value and metadata_role not in {"candidate", "challenger"} and metadata_lane not in {"paper_candidate", "candidate", "challenger"}:
            continue
        if "champion" in lane_value and metadata_role not in {"champion"} and metadata_lane not in {"paper_champion", "champion"}:
            continue
        opportunity_log_path = run_dir / "opportunity_log.jsonl"
        if not opportunity_log_path.exists():
            continue
        candidates.append(
            (
                float(run_dir.stat().st_mtime),
                {
                    "available": True,
                    "source_kind": "paper_run_scan",
                    "run_dir": str(run_dir.resolve()),
                    "run_id": _safe_text(metadata.get("run_id")) or run_dir.name,
                    "paper_runtime_role": _safe_text(metadata.get("paper_runtime_role")) or None,
                    "paper_lane": _safe_text(metadata.get("paper_lane")) or None,
                    "paper_runtime_model_run_id": metadata_run_id or None,
                    "opportunity_log_path": str(opportunity_log_path.resolve()),
                    "reason_codes": [],
                },
            )
        )
    if not candidates:
        return {"available": False, "reason_codes": ["PAPER_RUN_SCAN_NO_MATCH"]}
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _load_paper_run_metadata(run_dir: Path) -> dict[str, Any]:
    summary = _load_json(run_dir / "summary.json")
    if summary:
        metadata = {
            "run_id": _safe_text(summary.get("run_id")) or run_dir.name,
            "paper_runtime_role": _safe_text(summary.get("paper_runtime_role")),
            "paper_lane": _safe_text(summary.get("paper_lane")),
            "paper_runtime_model_run_id": _safe_text(summary.get("paper_runtime_model_run_id")),
        }
        if any(metadata.values()):
            return metadata
    for row in _load_jsonl(run_dir / "events.jsonl"):
        if _safe_text(row.get("event_type")).upper() != "RUN_STARTED":
            continue
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        return {
            "run_id": _safe_text(payload.get("run_id")) or run_dir.name,
            "paper_runtime_role": _safe_text(payload.get("paper_runtime_role")),
            "paper_lane": _safe_text(payload.get("paper_lane")),
            "paper_runtime_model_run_id": _safe_text(payload.get("paper_runtime_model_run_id")),
        }
    return {}


def _resolve_paired_paper_report(*, project_root: Path) -> dict[str, Any]:
    latest_path = Path(project_root) / "logs" / "paired_paper" / "latest.json"
    latest = _load_json(latest_path)
    if latest:
        return latest
    runs_root = Path(project_root) / "logs" / "paired_paper" / "runs"
    if not runs_root.exists():
        return {}
    candidates = sorted(
        (path / "paired_paper_report.json" for path in runs_root.iterdir() if path.is_dir()),
        key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
        reverse=True,
    )
    for path in candidates:
        payload = _load_json(path)
        if payload:
            return payload
    return {}


def _load_live_feature_parity_diagnostic(*, project_root: Path) -> dict[str, Any]:
    path = Path(project_root) / "data" / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"
    payload = _load_json(path)
    return {
        "available": bool(payload),
        "path": str(path),
        "status": _safe_text(payload.get("status")) or None,
        "acceptable": bool(payload.get("acceptable", False)) if payload else None,
        "sampled_pairs": _safe_int(payload.get("sampled_pairs")) if payload else None,
        "compared_pairs": _safe_int(payload.get("compared_pairs")) if payload else None,
        "passing_pairs": _safe_int(payload.get("passing_pairs")) if payload else None,
        "max_abs_diff_overall": _safe_optional_float(payload.get("max_abs_diff_overall")) if payload else None,
    }


def _build_opportunity_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _opportunity_key(row)
        if not key or key in index:
            continue
        index[key] = dict(row)
    return index


def _opportunity_key(row: dict[str, Any]) -> str:
    opportunity_id = _safe_text(row.get("opportunity_id"))
    if opportunity_id:
        return f"id:{opportunity_id}"
    ts_ms = _safe_int(row.get("ts_ms"))
    market = _safe_text(row.get("market")).upper()
    side = _safe_text(row.get("side")).lower()
    if ts_ms <= 0 or not market or not side:
        return ""
    return f"tuple:{ts_ms}:{market}:{side}"


def _decision_signature(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        _safe_text(row.get("chosen_action")),
        _safe_text(row.get("skip_reason_code")),
        _safe_text(_primary_decision_reason_code(row)),
        _safe_text(row.get("reason_code")).upper(),
    )


def _primary_decision_reason_code(row: dict[str, Any]) -> str:
    for code in _safety_veto_reason_codes(row):
        text = _safe_text(code)
        if text:
            return text
    for code in _entry_decision_reason_codes(row):
        text = _safe_text(code)
        if text:
            return text
    return _safe_text(row.get("skip_reason_code"))


def _entry_decision_reason_codes(row: dict[str, Any]) -> list[str]:
    meta = row.get("meta")
    if isinstance(meta, dict):
        entry_decision = meta.get("entry_decision")
        if isinstance(entry_decision, dict):
            return [_safe_text(code) for code in (entry_decision.get("reason_codes") or []) if _safe_text(code)]
        strategy = meta.get("strategy")
        if isinstance(strategy, dict):
            strategy_meta = strategy.get("meta")
            if isinstance(strategy_meta, dict):
                entry_decision = strategy_meta.get("entry_decision")
                if isinstance(entry_decision, dict):
                    return [_safe_text(code) for code in (entry_decision.get("reason_codes") or []) if _safe_text(code)]
    entry_decision = row.get("entry_decision")
    if isinstance(entry_decision, dict):
        return [_safe_text(code) for code in (entry_decision.get("reason_codes") or []) if _safe_text(code)]
    return []


def _safety_veto_reason_codes(row: dict[str, Any]) -> list[str]:
    def _extract(payload: dict[str, Any]) -> list[str]:
        safety_vetoes = payload.get("safety_vetoes")
        if not isinstance(safety_vetoes, dict):
            return []
        codes: list[str] = []
        for item in safety_vetoes.values():
            if not isinstance(item, dict):
                continue
            for code in item.get("reason_codes") or []:
                text = _safe_text(code)
                if text and text not in codes:
                    codes.append(text)
        return codes

    meta = row.get("meta")
    if isinstance(meta, dict):
        direct_codes = _extract(meta)
        if direct_codes:
            return direct_codes
        strategy = meta.get("strategy")
        if isinstance(strategy, dict):
            strategy_meta = strategy.get("meta")
            if isinstance(strategy_meta, dict):
                nested_codes = _extract(strategy_meta)
                if nested_codes:
                    return nested_codes
    direct_payload_codes = _extract(row if isinstance(row, dict) else {})
    return direct_payload_codes


def _record_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "chosen_action": _safe_text(row.get("chosen_action")),
        "skip_reason_code": _safe_text(row.get("skip_reason_code")) or None,
        "primary_decision_reason_code": _primary_decision_reason_code(row) or None,
        "feature_hash": _safe_text(row.get("feature_hash")) or None,
        "reason_code": _safe_text(row.get("reason_code")).upper() or None,
    }


def _insufficient_payload(
    *,
    unit_name: str,
    lane: str,
    run_id: str | None,
    ts_ms: int,
    live_path: Path,
    paper_source: dict[str, Any],
    infra_parity: dict[str, Any],
    reason_codes: list[str],
    status: str,
    matching: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_version": PAPER_LIVE_DIVERGENCE_VERSION,
        "policy": "paper_live_divergence_v1",
        "status": str(status).strip().lower() or "insufficient_evidence",
        "ts_ms": int(ts_ms),
        "unit_name": str(unit_name).strip(),
        "lane": str(lane).strip(),
        "run_id": _safe_text(run_id) or None,
        "live_source": {
            "available": bool(live_path.exists()),
            "opportunity_log_path": str(live_path),
            "record_count": 0,
        },
        "paper_source": paper_source,
        "infra_parity_diagnostic": infra_parity,
        "matching": matching
        or {
            "live_total": 0,
            "paper_total": 0,
            "matched_opportunities": 0,
            "min_matched_opportunities": 0,
            "live_only_opportunities": 0,
            "paper_only_opportunities": 0,
            "pair_ready": False,
        },
        "feature_divergence": {
            "available": False,
            "matched_feature_rows": 0,
            "feature_hash_match_ratio": None,
            "feature_divergence_rate": None,
        },
        "decision_divergence": {
            "available": False,
            "matched_decision_rows": 0,
            "decision_match_ratio": None,
            "decision_divergence_rate": None,
        },
        "matched_records": [],
        "reason_codes": sorted({str(code).strip() for code in reason_codes if str(code).strip()}),
        "disagreement_examples": [],
    }


def _unit_slug(unit_name: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(unit_name).strip().lower()).strip("_")
    slug = "_".join(part for part in slug.split("_") if part)
    return slug or "live"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


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


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
