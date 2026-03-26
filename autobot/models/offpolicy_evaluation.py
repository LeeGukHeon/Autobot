from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


DEFAULT_DR_OPE_WEIGHT_CLIP = 10.0
DEFAULT_DR_OPE_REPORT_NAME = "execution_ope_report.json"


def build_execution_dr_ope_report(
    *,
    run_dir: str | Path,
    execution_contract: dict[str, Any] | None,
    weight_clip: float = DEFAULT_DR_OPE_WEIGHT_CLIP,
) -> dict[str, Any]:
    root = Path(run_dir)
    opportunity_rows = _load_jsonl(root / "opportunity_log.jsonl")
    counterfactual_rows = _load_jsonl(root / "counterfactual_action_log.jsonl")
    actions_by_opportunity = _group_counterfactual_rows(counterfactual_rows)
    opportunities = []
    available_action_codes: set[str] = set()
    for row in opportunity_rows:
        opportunity = _build_opportunity_doc(
            opportunity_row=row,
            counterfactual_rows=actions_by_opportunity.get(str(row.get("opportunity_id") or "").strip(), []),
            execution_contract=execution_contract,
        )
        if opportunity is None:
            continue
        opportunities.append(opportunity)
        available_action_codes.update(opportunity["actions"].keys())

    policy_reports = {
        "logged_policy": _evaluate_policy(
            opportunities=opportunities,
            policy_name="logged_policy",
            weight_clip=weight_clip,
            policy_selector=lambda opportunity: str(opportunity.get("logged_action") or "").strip().upper(),
        ),
        "greedy_predicted_utility_policy": _evaluate_policy(
            opportunities=opportunities,
            policy_name="greedy_predicted_utility_policy",
            weight_clip=weight_clip,
            policy_selector=lambda opportunity: _greedy_action(opportunity=opportunity),
        ),
        "no_trade_policy": _evaluate_policy(
            opportunities=opportunities,
            policy_name="no_trade_policy",
            weight_clip=weight_clip,
            policy_selector=lambda opportunity: "NO_TRADE",
        ),
    }
    for action_code in sorted(available_action_codes):
        if action_code == "NO_TRADE":
            continue
        policy_reports[f"always_{action_code.lower()}"] = _evaluate_policy(
            opportunities=opportunities,
            policy_name=f"always_{action_code.lower()}",
            weight_clip=weight_clip,
            policy_selector=lambda opportunity, action=action_code: action,
        )

    return {
        "policy": "execution_dr_ope_v1",
        "run_dir": str(root),
        "weight_clip": float(max(float(weight_clip), 1.0)),
        "sample_count": int(len(opportunities)),
        "reward_metric": "realized_pnl_bps",
        "execution_contract_summary": {
            "policy": str((execution_contract or {}).get("policy") or "").strip() or None,
            "rows_total": int((execution_contract or {}).get("rows_total", 0) or 0),
            "execution_twin_policy": str((((execution_contract or {}).get("execution_twin") or {}).get("policy") or "")).strip() or None,
        },
        "policy_reports": policy_reports,
        "action_availability_counts": _action_availability_counts(opportunities=opportunities),
        "support_summary": {
            "deterministic_behavior_policy": True,
            "opportunities_with_realized_reward": int(sum(1 for item in opportunities if item.get("reward_bps") is not None)),
            "opportunities_with_counterfactual_actions": int(sum(1 for item in opportunities if item.get("actions"))),
        },
    }


def write_execution_dr_ope_report(
    *,
    run_dir: str | Path,
    execution_contract: dict[str, Any] | None,
    output_path: str | Path | None = None,
    weight_clip: float = DEFAULT_DR_OPE_WEIGHT_CLIP,
) -> Path:
    root = Path(run_dir)
    target = Path(output_path) if output_path is not None else (root / DEFAULT_DR_OPE_REPORT_NAME)
    payload = build_execution_dr_ope_report(
        run_dir=root,
        execution_contract=execution_contract,
        weight_clip=weight_clip,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target


def _build_opportunity_doc(
    *,
    opportunity_row: dict[str, Any],
    counterfactual_rows: list[dict[str, Any]],
    execution_contract: dict[str, Any] | None,
) -> dict[str, Any] | None:
    opportunity_id = str(opportunity_row.get("opportunity_id") or "").strip()
    if not opportunity_id:
        return None
    actions = _build_action_docs(
        opportunity_row=opportunity_row,
        counterfactual_rows=counterfactual_rows,
        execution_contract=execution_contract,
    )
    if not actions:
        return None
    logged_action = str(opportunity_row.get("chosen_action") or "").strip().upper()
    reward_bps = _resolve_realized_reward_bps(opportunity_row)
    if reward_bps is None and logged_action == "NO_TRADE":
        reward_bps = 0.0
    return {
        "opportunity_id": opportunity_id,
        "logged_action": logged_action,
        "logged_propensity": _resolve_action_propensity(
            action_doc=actions.get(logged_action),
            opportunity_row=opportunity_row,
            is_logged=True,
        ),
        "reward_bps": reward_bps,
        "actions": actions,
    }


def _build_action_docs(
    *,
    opportunity_row: dict[str, Any],
    counterfactual_rows: list[dict[str, Any]],
    execution_contract: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    action_docs: dict[str, dict[str, Any]] = {}
    for row in counterfactual_rows:
        action_payload = dict(row.get("action_payload") or {})
        action_code = str(
            action_payload.get("action_code")
            or row.get("chosen_action")
            or ""
        ).strip().upper()
        if not action_code:
            continue
        q_hat_bps = _resolve_q_hat_bps(
            opportunity_row=opportunity_row,
            action_payload=action_payload,
            action_code=action_code,
            execution_contract=execution_contract,
        )
        action_docs[action_code] = {
            "action_code": action_code,
            "propensity": _safe_optional_float(row.get("action_propensity")),
            "q_hat_bps": q_hat_bps,
        }
    if not action_docs:
        chosen_action = str(opportunity_row.get("chosen_action") or "").strip().upper()
        if chosen_action:
            action_docs[chosen_action] = {
                "action_code": chosen_action,
                "propensity": _safe_optional_float(opportunity_row.get("chosen_action_propensity")),
                "q_hat_bps": _resolve_q_hat_bps(
                    opportunity_row=opportunity_row,
                    action_payload={},
                    action_code=chosen_action,
                    execution_contract=execution_contract,
                ),
            }
    return action_docs


def _resolve_q_hat_bps(
    *,
    opportunity_row: dict[str, Any],
    action_payload: dict[str, Any],
    action_code: str,
    execution_contract: dict[str, Any] | None,
) -> float:
    predicted_utility = _safe_optional_float(action_payload.get("predicted_utility_bps"))
    if predicted_utility is not None:
        return float(predicted_utility)
    if str(action_code).strip().upper() == "NO_TRADE":
        return 0.0
    edge_bps = _safe_optional_float(opportunity_row.get("expected_edge_bps"))
    if edge_bps is None:
        edge_bps = _safe_optional_float(((opportunity_row.get("meta") or {}).get("trade_action") or {}).get("expected_edge_bps"))
    twin = dict(((execution_contract or {}).get("execution_twin") or {}))
    price_mode_stats = dict(twin.get("price_mode_stats") or {})
    stats = dict(price_mode_stats.get(str(action_code).strip().upper()) or {})
    fill_prob = _safe_optional_float(stats.get("full_fill_probability"))
    if fill_prob is None:
        fill_prob = _safe_optional_float(stats.get("first_fill_probability"))
    if fill_prob is None:
        fill_prob = 0.0
    expected_shortfall_bps = _safe_optional_float(stats.get("expected_shortfall_bps")) or 0.0
    if edge_bps is None:
        return -float(expected_shortfall_bps)
    return (float(edge_bps) * float(fill_prob)) - float(expected_shortfall_bps)


def _resolve_realized_reward_bps(opportunity_row: dict[str, Any]) -> float | None:
    realized = dict(opportunity_row.get("realized_outcome_json") or {})
    direct_bps = _safe_optional_float(realized.get("realized_pnl_bps"))
    if direct_bps is not None:
        return float(direct_bps)
    realized_quote = _safe_optional_float(realized.get("realized_pnl_quote"))
    entry_notional = _safe_optional_float(realized.get("entry_notional_quote"))
    if realized_quote is not None and entry_notional is not None and entry_notional > 0.0:
        return float(realized_quote) / float(entry_notional) * 10_000.0
    realized_pct = _safe_optional_float(realized.get("realized_pnl_pct"))
    if realized_pct is not None:
        return float(realized_pct) * 100.0
    return None


def _resolve_action_propensity(
    *,
    action_doc: dict[str, Any] | None,
    opportunity_row: dict[str, Any],
    is_logged: bool,
) -> float:
    if isinstance(action_doc, dict):
        resolved = _safe_optional_float(action_doc.get("propensity"))
        if resolved is not None:
            return float(resolved)
    if is_logged:
        resolved = _safe_optional_float(opportunity_row.get("chosen_action_propensity"))
        if resolved is not None:
            return float(resolved)
    return 0.0


def _evaluate_policy(
    *,
    opportunities: list[dict[str, Any]],
    policy_name: str,
    weight_clip: float,
    policy_selector: Any,
) -> dict[str, Any]:
    dm_values: list[float] = []
    ips_values: list[float] = []
    dr_values: list[float] = []
    weights: list[float] = []
    supported = 0
    selected_action_counts: dict[str, int] = {}
    q_source_missing = 0
    for opportunity in opportunities:
        target_action = str(policy_selector(opportunity) or "").strip().upper()
        action_doc = dict((opportunity.get("actions") or {}).get(target_action) or {})
        if not target_action or not action_doc:
            continue
        selected_action_counts[target_action] = int(selected_action_counts.get(target_action, 0)) + 1
        q_hat = _safe_optional_float(action_doc.get("q_hat_bps"))
        if q_hat is None:
            q_source_missing += 1
            q_hat = 0.0
        dm_values.append(float(q_hat))
        reward_bps = _safe_optional_float(opportunity.get("reward_bps"))
        logged_action = str(opportunity.get("logged_action") or "").strip().upper()
        logged_propensity = max(_safe_optional_float(opportunity.get("logged_propensity")) or 0.0, 0.0)
        ratio = 0.0
        ips_value = float(q_hat)
        dr_value = float(q_hat)
        if reward_bps is not None and target_action == logged_action and logged_propensity > 0.0:
            supported += 1
            ratio = min(1.0 / float(logged_propensity), float(weight_clip))
            ips_value = ratio * float(reward_bps)
            dr_value = float(q_hat) + (ratio * (float(reward_bps) - float(q_hat)))
            weights.append(float(ratio))
        ips_values.append(float(ips_value))
        dr_values.append(float(dr_value))
    sample_count = len(dm_values)
    effective_sample_size = (
        (sum(weights) ** 2) / max(sum(weight * weight for weight in weights), 1e-12)
        if weights
        else 0.0
    )
    return {
        "policy_name": policy_name,
        "sample_count": int(sample_count),
        "supported_count": int(supported),
        "support_rate": (float(supported) / float(sample_count)) if sample_count > 0 else 0.0,
        "effective_sample_size": float(effective_sample_size),
        "dm_estimate_bps": _mean(dm_values),
        "ips_estimate_bps": _mean(ips_values),
        "dr_estimate_bps": _mean(dr_values),
        "selected_action_counts": selected_action_counts,
        "q_source_missing_count": int(q_source_missing),
    }


def _greedy_action(*, opportunity: dict[str, Any]) -> str:
    actions = dict(opportunity.get("actions") or {})
    if not actions:
        return ""
    return max(
        actions.values(),
        key=lambda item: (
            _safe_optional_float((item or {}).get("q_hat_bps"))
            if _safe_optional_float((item or {}).get("q_hat_bps")) is not None
            else float("-inf")
        ),
    ).get("action_code", "")


def _action_availability_counts(*, opportunities: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for opportunity in opportunities:
        for action_code in (opportunity.get("actions") or {}).keys():
            text = str(action_code).strip().upper()
            if not text:
                continue
            counts[text] = int(counts.get(text, 0)) + 1
    return counts


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
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
    return rows


def _group_counterfactual_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("opportunity_id") or "").strip()
        if not key:
            continue
        grouped.setdefault(key, []).append(dict(row))
    return grouped


def _mean(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not clean:
        return None
    return float(sum(clean) / float(len(clean)))


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_execution_contract(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload.get("execution_contract"), dict):
        return dict(payload.get("execution_contract") or {})
    return payload if isinstance(payload, dict) else {}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build execution DR-OPE report from opportunity logs.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--execution-contract-artifact")
    parser.add_argument("--weight-clip", type=float, default=DEFAULT_DR_OPE_WEIGHT_CLIP)
    parser.add_argument("--out", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    run_dir = Path(str(args.run_dir))
    contract_path = Path(str(args.execution_contract_artifact)) if str(args.execution_contract_artifact or "").strip() else None
    output_path = Path(str(args.out)) if str(args.out).strip() else None
    path = write_execution_dr_ope_report(
        run_dir=run_dir,
        execution_contract=_load_execution_contract(contract_path),
        output_path=output_path,
        weight_clip=max(float(args.weight_clip), 1.0),
    )
    print(f"[model][ope-execution] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
