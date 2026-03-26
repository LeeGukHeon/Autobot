"""Machine-readable typed breaker taxonomy for live runtime safety."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


ACTION_WARN = "WARN"
ACTION_HALT_NEW_INTENTS = "HALT_NEW_INTENTS"
ACTION_HALT_AND_CANCEL_BOT_ORDERS = "HALT_AND_CANCEL_BOT_ORDERS"
ACTION_FULL_KILL_SWITCH = "FULL_KILL_SWITCH"

ACTION_SEVERITY = {
    ACTION_WARN: 0,
    ACTION_HALT_NEW_INTENTS: 1,
    ACTION_HALT_AND_CANCEL_BOT_ORDERS: 2,
    ACTION_FULL_KILL_SWITCH: 3,
}

BREAKER_TAXONOMY_VERSION = 1

BREAKER_TYPE_INFRA = "INFRA"
BREAKER_TYPE_STATE_INTEGRITY = "STATE_INTEGRITY"
BREAKER_TYPE_STATISTICAL_RISK = "STATISTICAL_RISK"
BREAKER_TYPE_OPERATIONAL_POLICY = "OPERATIONAL_POLICY"
BREAKER_TYPE_UNKNOWN = "UNKNOWN"

CLEAR_POLICY_AUTO_HEALTH_RECOVERY = "AUTO_HEALTH_RECOVERY"
CLEAR_POLICY_RECONCILE_RECOVERY = "RECONCILE_RECOVERY"
CLEAR_POLICY_RUNTIME_CONTRACT_RECOVERY = "RUNTIME_CONTRACT_RECOVERY"
CLEAR_POLICY_ROLLOUT_RECOVERY = "ROLLOUT_RECOVERY"
CLEAR_POLICY_COUNTER_RESET = "COUNTER_RESET"
CLEAR_POLICY_ONLINE_BASELINE_CLEAR = "ONLINE_BASELINE_CLEAR"
CLEAR_POLICY_STATE_MACHINE_RECOVERY = "STATE_MACHINE_RECOVERY"
CLEAR_POLICY_MANUAL = "MANUAL"
CLEAR_POLICY_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class BreakerReasonSpec:
    reason_code: str
    breaker_type: str
    subtype: str
    default_action: str
    clear_policy: str
    summary: str


_UNKNOWN_REASON_SPEC = BreakerReasonSpec(
    reason_code="UNKNOWN_BREAKER_REASON",
    breaker_type=BREAKER_TYPE_UNKNOWN,
    subtype="unknown",
    default_action=ACTION_WARN,
    clear_policy=CLEAR_POLICY_UNKNOWN,
    summary="Unknown breaker reason code",
)


_REASON_SPECS: dict[str, BreakerReasonSpec] = {
    "UNKNOWN_OPEN_ORDERS_DETECTED": BreakerReasonSpec(
        reason_code="UNKNOWN_OPEN_ORDERS_DETECTED",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="exchange_order_integrity",
        default_action=ACTION_HALT_AND_CANCEL_BOT_ORDERS,
        clear_policy=CLEAR_POLICY_RECONCILE_RECOVERY,
        summary="Unknown exchange open orders were detected during reconcile.",
    ),
    "UNKNOWN_POSITIONS_DETECTED": BreakerReasonSpec(
        reason_code="UNKNOWN_POSITIONS_DETECTED",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="position_integrity",
        default_action=ACTION_FULL_KILL_SWITCH,
        clear_policy=CLEAR_POLICY_RECONCILE_RECOVERY,
        summary="Unknown exchange positions were detected during reconcile.",
    ),
    "LOCAL_POSITION_MISSING_ON_EXCHANGE": BreakerReasonSpec(
        reason_code="LOCAL_POSITION_MISSING_ON_EXCHANGE",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="position_integrity",
        default_action=ACTION_HALT_AND_CANCEL_BOT_ORDERS,
        clear_policy=CLEAR_POLICY_RECONCILE_RECOVERY,
        summary="A managed local position disappeared from exchange state.",
    ),
    "LOCAL_OPEN_ORDER_NOT_FOUND_ON_EXCHANGE": BreakerReasonSpec(
        reason_code="LOCAL_OPEN_ORDER_NOT_FOUND_ON_EXCHANGE",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="exchange_order_integrity",
        default_action=ACTION_WARN,
        clear_policy=CLEAR_POLICY_RECONCILE_RECOVERY,
        summary="A local open order could not be found on the exchange snapshot.",
    ),
    "STALE_PRIVATE_WS_STREAM": BreakerReasonSpec(
        reason_code="STALE_PRIVATE_WS_STREAM",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="private_stream_health",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_AUTO_HEALTH_RECOVERY,
        summary="The private WS stream became stale.",
    ),
    "STALE_EXECUTOR_STREAM": BreakerReasonSpec(
        reason_code="STALE_EXECUTOR_STREAM",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="executor_stream_health",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_AUTO_HEALTH_RECOVERY,
        summary="The executor event stream became stale.",
    ),
    "WS_PUBLIC_STALE": BreakerReasonSpec(
        reason_code="WS_PUBLIC_STALE",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="public_stream_health",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_AUTO_HEALTH_RECOVERY,
        summary="The public WS health contract is stale.",
    ),
    "LIVE_PUBLIC_WS_STREAM_FAILED": BreakerReasonSpec(
        reason_code="LIVE_PUBLIC_WS_STREAM_FAILED",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="public_stream_health",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_AUTO_HEALTH_RECOVERY,
        summary="The live runtime observed a public WS failure path.",
    ),
    "LIVE_RUNTIME_LOOP_FAILED": BreakerReasonSpec(
        reason_code="LIVE_RUNTIME_LOOP_FAILED",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="runtime_loop_health",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_AUTO_HEALTH_RECOVERY,
        summary="The live runtime loop failed outside the public WS-specific path.",
    ),
    "MODEL_POINTER_DIVERGENCE": BreakerReasonSpec(
        reason_code="MODEL_POINTER_DIVERGENCE",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="runtime_pointer_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_RUNTIME_CONTRACT_RECOVERY,
        summary="Pinned runtime model state diverged from the current pointer contract.",
    ),
    "MODEL_POINTER_UNRESOLVED": BreakerReasonSpec(
        reason_code="MODEL_POINTER_UNRESOLVED",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="runtime_pointer_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_RUNTIME_CONTRACT_RECOVERY,
        summary="The requested runtime model pointer could not be resolved.",
    ),
    "LIVE_ROLLOUT_NOT_ARMED": BreakerReasonSpec(
        reason_code="LIVE_ROLLOUT_NOT_ARMED",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="rollout_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="Live rollout requires an explicit arm contract before starting order emission.",
    ),
    "LIVE_ROLLOUT_UNIT_MISMATCH": BreakerReasonSpec(
        reason_code="LIVE_ROLLOUT_UNIT_MISMATCH",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="rollout_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="The live rollout target unit does not match the current runtime unit.",
    ),
    "LIVE_ROLLOUT_MODE_MISMATCH": BreakerReasonSpec(
        reason_code="LIVE_ROLLOUT_MODE_MISMATCH",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="rollout_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="The rollout mode contract does not match the current runtime mode.",
    ),
    "LIVE_TEST_ORDER_REQUIRED": BreakerReasonSpec(
        reason_code="LIVE_TEST_ORDER_REQUIRED",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="rollout_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="The rollout contract requires a successful live test order.",
    ),
    "LIVE_TEST_ORDER_STALE": BreakerReasonSpec(
        reason_code="LIVE_TEST_ORDER_STALE",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="rollout_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="The rollout test-order evidence is stale.",
    ),
    "LIVE_BREAKER_ACTIVE": BreakerReasonSpec(
        reason_code="LIVE_BREAKER_ACTIVE",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="rollout_contract",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="The rollout gate is blocked because a live breaker is active.",
    ),
    "LIVE_CANARY_REQUIRES_SINGLE_SLOT": BreakerReasonSpec(
        reason_code="LIVE_CANARY_REQUIRES_SINGLE_SLOT",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="canary_slot_policy",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ROLLOUT_RECOVERY,
        summary="Canary mode requires explicit single-slot small-account constraints.",
    ),
    "REPEATED_CANCEL_REJECTS": BreakerReasonSpec(
        reason_code="REPEATED_CANCEL_REJECTS",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="execution_reject_counter",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_COUNTER_RESET,
        summary="Cancel rejects exceeded the configured breaker counter budget.",
    ),
    "REPEATED_REPLACE_REJECTS": BreakerReasonSpec(
        reason_code="REPEATED_REPLACE_REJECTS",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="execution_reject_counter",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_COUNTER_RESET,
        summary="Replace rejects exceeded the configured breaker counter budget.",
    ),
    "REPEATED_RATE_LIMIT_ERRORS": BreakerReasonSpec(
        reason_code="REPEATED_RATE_LIMIT_ERRORS",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="exchange_auth_and_limits",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_COUNTER_RESET,
        summary="Exchange rate-limit errors exceeded the configured breaker counter budget.",
    ),
    "RISK_CONTROL_ONLINE_BREACH_STREAK": BreakerReasonSpec(
        reason_code="RISK_CONTROL_ONLINE_BREACH_STREAK",
        breaker_type=BREAKER_TYPE_STATISTICAL_RISK,
        subtype="online_risk_monitor",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ONLINE_BASELINE_CLEAR,
        summary="The online risk monitor observed a breach streak above threshold.",
    ),
    "RISK_CONTROL_MARTINGALE_EVIDENCE": BreakerReasonSpec(
        reason_code="RISK_CONTROL_MARTINGALE_EVIDENCE",
        breaker_type=BREAKER_TYPE_STATISTICAL_RISK,
        subtype="online_risk_monitor",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_ONLINE_BASELINE_CLEAR,
        summary="Martingale evidence crossed the live statistical risk threshold.",
    ),
    "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE": BreakerReasonSpec(
        reason_code="RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE",
        breaker_type=BREAKER_TYPE_STATISTICAL_RISK,
        subtype="online_risk_monitor",
        default_action=ACTION_HALT_AND_CANCEL_BOT_ORDERS,
        clear_policy=CLEAR_POLICY_ONLINE_BASELINE_CLEAR,
        summary="Critical martingale evidence crossed the strongest live statistical risk threshold.",
    ),
    "REPEATED_AUTH_ERRORS": BreakerReasonSpec(
        reason_code="REPEATED_AUTH_ERRORS",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="exchange_auth_and_limits",
        default_action=ACTION_FULL_KILL_SWITCH,
        clear_policy=CLEAR_POLICY_COUNTER_RESET,
        summary="Exchange auth errors exceeded the configured breaker counter budget.",
    ),
    "REPEATED_NONCE_ERRORS": BreakerReasonSpec(
        reason_code="REPEATED_NONCE_ERRORS",
        breaker_type=BREAKER_TYPE_INFRA,
        subtype="exchange_auth_and_limits",
        default_action=ACTION_FULL_KILL_SWITCH,
        clear_policy=CLEAR_POLICY_COUNTER_RESET,
        summary="Exchange nonce errors exceeded the configured breaker counter budget.",
    ),
    "EXECUTOR_REPLACE_PERSIST_FAILED": BreakerReasonSpec(
        reason_code="EXECUTOR_REPLACE_PERSIST_FAILED",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="order_lineage_persist",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_STATE_MACHINE_RECOVERY,
        summary="Executor replace lineage could not be persisted safely.",
    ),
    "RISK_EXIT_STUCK_MAX_REPLACES": BreakerReasonSpec(
        reason_code="RISK_EXIT_STUCK_MAX_REPLACES",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="protective_exit_state_machine",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_STATE_MACHINE_RECOVERY,
        summary="Protective exit hit replace exhaustion and needs explicit recovery evidence.",
    ),
    "RISK_EXIT_REPLACE_PERSIST_FAILED": BreakerReasonSpec(
        reason_code="RISK_EXIT_REPLACE_PERSIST_FAILED",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="order_lineage_persist",
        default_action=ACTION_HALT_NEW_INTENTS,
        clear_policy=CLEAR_POLICY_STATE_MACHINE_RECOVERY,
        summary="Protective exit replace lineage could not be persisted safely.",
    ),
    "IDENTIFIER_COLLISION": BreakerReasonSpec(
        reason_code="IDENTIFIER_COLLISION",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="identifier_integrity",
        default_action=ACTION_FULL_KILL_SWITCH,
        clear_policy=CLEAR_POLICY_MANUAL,
        summary="An order identifier collision was detected.",
    ),
    "MANUAL_KILL_SWITCH": BreakerReasonSpec(
        reason_code="MANUAL_KILL_SWITCH",
        breaker_type=BREAKER_TYPE_OPERATIONAL_POLICY,
        subtype="manual_override",
        default_action=ACTION_FULL_KILL_SWITCH,
        clear_policy=CLEAR_POLICY_MANUAL,
        summary="A manual kill switch was armed by the operator.",
    ),
    "SUPERVISOR_REPLACE_PERSIST_FAILED": BreakerReasonSpec(
        reason_code="SUPERVISOR_REPLACE_PERSIST_FAILED",
        breaker_type=BREAKER_TYPE_STATE_INTEGRITY,
        subtype="order_lineage_persist",
        default_action=ACTION_WARN,
        clear_policy=CLEAR_POLICY_STATE_MACHINE_RECOVERY,
        summary="Order supervisor replace lineage could not be persisted safely.",
    ),
}


def normalize_reason_code(value: str | None) -> str:
    return str(value or "").strip().upper()


def normalize_reason_codes(values: Iterable[str | None]) -> list[str]:
    unique: list[str] = []
    for value in values:
        reason_code = normalize_reason_code(value)
        if reason_code and reason_code not in unique:
            unique.append(reason_code)
    return unique


def breaker_reason_spec(reason_code: str | None) -> BreakerReasonSpec:
    normalized = normalize_reason_code(reason_code)
    if not normalized:
        return _UNKNOWN_REASON_SPEC
    return _REASON_SPECS.get(
        normalized,
        BreakerReasonSpec(
            reason_code=normalized,
            breaker_type=_UNKNOWN_REASON_SPEC.breaker_type,
            subtype=_UNKNOWN_REASON_SPEC.subtype,
            default_action=_UNKNOWN_REASON_SPEC.default_action,
            clear_policy=_UNKNOWN_REASON_SPEC.clear_policy,
            summary=f"Unknown breaker reason code: {normalized}",
        ),
    )


def action_for_reason(reason_code: str | None) -> str:
    return breaker_reason_spec(reason_code).default_action


def choose_action(reason_codes: Iterable[str | None]) -> str:
    normalized = normalize_reason_codes(reason_codes)
    if not normalized:
        return ACTION_WARN
    return max((action_for_reason(reason_code) for reason_code in normalized), key=lambda item: ACTION_SEVERITY[item])


def typed_reason_payloads(reason_codes: Iterable[str | None]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for reason_code in normalize_reason_codes(reason_codes):
        spec = breaker_reason_spec(reason_code)
        payloads.append(
            {
                "reason_code": spec.reason_code,
                "breaker_type": spec.breaker_type,
                "subtype": spec.subtype,
                "default_action": spec.default_action,
                "clear_policy": spec.clear_policy,
                "summary": spec.summary,
            }
        )
    return payloads


def breaker_taxonomy_summary(reason_codes: Iterable[str | None]) -> dict[str, Any]:
    typed_reasons = typed_reason_payloads(reason_codes)
    type_counts: dict[str, int] = {}
    ordered_types: list[str] = []
    clear_policies: list[str] = []
    primary_reason_type: str | None = None
    primary_reason_severity = -1
    for payload in typed_reasons:
        reason_type = str(payload.get("breaker_type") or BREAKER_TYPE_UNKNOWN).strip().upper() or BREAKER_TYPE_UNKNOWN
        type_counts[reason_type] = int(type_counts.get(reason_type, 0)) + 1
        if reason_type not in ordered_types:
            ordered_types.append(reason_type)
        clear_policy = str(payload.get("clear_policy") or "").strip().upper()
        if clear_policy and clear_policy not in clear_policies:
            clear_policies.append(clear_policy)
        severity = int(ACTION_SEVERITY.get(str(payload.get("default_action") or "").strip().upper(), -1))
        if severity > primary_reason_severity:
            primary_reason_severity = severity
            primary_reason_type = reason_type
    return {
        "taxonomy_version": BREAKER_TAXONOMY_VERSION,
        "typed_reason_codes": typed_reasons,
        "reason_types": ordered_types,
        "primary_reason_type": primary_reason_type,
        "reason_type_counts": type_counts,
        "clear_policies": clear_policies,
    }


def annotate_reason_payload(
    payload: dict[str, Any] | None,
    *,
    reason_codes: Iterable[str | None],
) -> dict[str, Any]:
    annotated = dict(payload or {})
    annotated.update(breaker_taxonomy_summary(reason_codes))
    return annotated
