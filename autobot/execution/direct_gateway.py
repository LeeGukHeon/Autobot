"""Direct REST execution gateway for poll-based live runtime."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..upbit.exceptions import UpbitError, ValidationError
from ..upbit.private import UpbitPrivateClient
from .grpc_gateway import ExecutorEvent, ExecutorReplaceResult, ExecutorSubmitResult
from .intent import OrderIntent, new_order_intent


def _format_decimal(value: float) -> str:
    try:
        decimal_value = Decimal(str(float(value)))
    except (ValueError, InvalidOperation):
        decimal_value = Decimal(0)
    normalized = format(decimal_value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _first_nonempty(mapping: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _optional_str(mapping.get(key))
        if value:
            return value
    return None


def _extract_replace_result(
    payload: Any,
    *,
    cancelled_order_uuid: str | None,
    requested_new_identifier: str,
) -> tuple[str | None, str | None, str | None]:
    if not isinstance(payload, dict):
        return cancelled_order_uuid, None, requested_new_identifier

    cancelled_uuid = _first_nonempty(
        payload,
        ("cancelled_order_uuid", "canceled_order_uuid", "prev_order_uuid", "cancel_uuid"),
    ) or cancelled_order_uuid

    new_uuid = _first_nonempty(payload, ("new_order_uuid", "new_uuid", "order_uuid"))
    new_identifier = _first_nonempty(payload, ("new_identifier", "new_order_identifier"))

    for nested_key in ("new_order", "order", "data", "result"):
        nested = payload.get(nested_key)
        if not isinstance(nested, dict):
            continue
        if not new_uuid:
            new_uuid = _first_nonempty(nested, ("uuid", "new_order_uuid", "new_uuid", "order_uuid"))
        if not new_identifier:
            new_identifier = _first_nonempty(nested, ("identifier", "new_identifier", "new_order_identifier"))

    if not new_uuid:
        direct_uuid = _optional_str(payload.get("uuid"))
        if direct_uuid and direct_uuid != cancelled_uuid:
            new_uuid = direct_uuid
    if not new_identifier:
        direct_identifier = _optional_str(payload.get("identifier"))
        if direct_identifier and direct_identifier != requested_new_identifier:
            new_identifier = direct_identifier

    return cancelled_uuid, new_uuid, new_identifier or requested_new_identifier


@dataclass(frozen=True)
class DirectRestExecutionGateway:
    """Thin execution adapter that submits directly via Upbit private REST."""

    client: UpbitPrivateClient

    def close(self) -> None:
        return

    def __enter__(self) -> "DirectRestExecutionGateway":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def ping(self) -> dict[str, Any]:
        accounts = self.client.accounts()
        return {
            "ok": True,
            "message": "direct_rest",
            "accounts_count": len(accounts) if isinstance(accounts, list) else 0,
        }

    def submit_intent(
        self,
        *,
        intent: OrderIntent,
        identifier: str,
        meta_json: str | None = None,
    ) -> ExecutorSubmitResult:
        _ = meta_json
        try:
            payload = self.client.create_order(
                market=intent.market,
                side=intent.side,
                ord_type=intent.ord_type,
                price=_format_decimal(intent.price),
                volume=_format_decimal(intent.volume),
                time_in_force=intent.time_in_force,
                identifier=identifier,
            )
        except UpbitError as exc:
            return ExecutorSubmitResult(
                accepted=False,
                reason=str(exc),
                upbit_uuid=None,
                identifier=identifier,
                intent_id=intent.intent_id,
            )
        return ExecutorSubmitResult(
            accepted=True,
            reason="",
            upbit_uuid=_optional_str(payload.get("uuid") if isinstance(payload, dict) else None),
            identifier=_optional_str(payload.get("identifier") if isinstance(payload, dict) else None) or identifier,
            intent_id=intent.intent_id,
        )

    def submit_test(
        self,
        *,
        market: str,
        side: str,
        price: float,
        volume: float,
        identifier: str | None = None,
    ) -> ExecutorSubmitResult:
        intent = new_order_intent(
            market=market,
            side=side,
            price=float(price),
            volume=float(volume),
            reason_code="EXEC_SUBMIT_TEST",
            ord_type="limit",
            time_in_force="gtc",
            meta={"submit_mode": "order_test"},
        )
        identifier_value = _optional_str(identifier) or f"AUTOBOT-EXEC-TEST-{intent.intent_id[:12]}"
        try:
            payload = self.client.order_test(
                market=intent.market,
                side=intent.side,
                ord_type=intent.ord_type,
                price=_format_decimal(intent.price),
                volume=_format_decimal(intent.volume),
                time_in_force=intent.time_in_force,
                identifier=identifier_value,
            )
        except UpbitError as exc:
            return ExecutorSubmitResult(
                accepted=False,
                reason=str(exc),
                upbit_uuid=None,
                identifier=identifier_value,
                intent_id=intent.intent_id,
            )
        return ExecutorSubmitResult(
            accepted=True,
            reason="",
            upbit_uuid=_optional_str(payload.get("uuid") if isinstance(payload, dict) else None),
            identifier=_optional_str(payload.get("identifier") if isinstance(payload, dict) else None) or identifier_value,
            intent_id=intent.intent_id,
        )

    def cancel(
        self,
        *,
        upbit_uuid: str | None = None,
        identifier: str | None = None,
    ) -> ExecutorSubmitResult:
        try:
            payload = self.client.cancel_order(uuid=upbit_uuid, identifier=identifier)
        except UpbitError as exc:
            return ExecutorSubmitResult(
                accepted=False,
                reason=str(exc),
                upbit_uuid=_optional_str(upbit_uuid),
                identifier=_optional_str(identifier),
                intent_id=None,
            )
        return ExecutorSubmitResult(
            accepted=True,
            reason="",
            upbit_uuid=_optional_str(payload.get("uuid") if isinstance(payload, dict) else upbit_uuid),
            identifier=_optional_str(payload.get("identifier") if isinstance(payload, dict) else identifier),
            intent_id=None,
        )

    def replace_order(
        self,
        *,
        intent_id: str,
        prev_order_uuid: str | None = None,
        prev_order_identifier: str | None = None,
        new_identifier: str,
        new_price_str: str,
        new_volume_str: str,
        new_time_in_force: str | None = None,
    ) -> ExecutorReplaceResult:
        try:
            resolved_new_volume = self._resolve_replace_volume(
                prev_order_uuid=prev_order_uuid,
                prev_order_identifier=prev_order_identifier,
                new_volume_str=new_volume_str,
            )
        except UpbitError as exc:
            return ExecutorReplaceResult(
                accepted=False,
                reason=str(exc),
                cancelled_order_uuid=_optional_str(prev_order_uuid),
                new_order_uuid=None,
                new_identifier=new_identifier,
            )
        except ValueError as exc:
            return ExecutorReplaceResult(
                accepted=False,
                reason=str(exc),
                cancelled_order_uuid=_optional_str(prev_order_uuid),
                new_order_uuid=None,
                new_identifier=new_identifier,
            )
        try:
            payload = self.client.cancel_and_new_order(
                prev_order_uuid=prev_order_uuid,
                prev_order_identifier=prev_order_identifier,
                new_identifier=new_identifier,
                new_price=new_price_str,
                new_volume=resolved_new_volume,
                new_time_in_force=new_time_in_force,
            )
        except UpbitError as exc:
            return ExecutorReplaceResult(
                accepted=False,
                reason=str(exc),
                cancelled_order_uuid=_optional_str(prev_order_uuid),
                new_order_uuid=None,
                new_identifier=new_identifier,
            )
        _ = intent_id
        cancelled_uuid, new_uuid, resolved_new_identifier = _extract_replace_result(
            payload,
            cancelled_order_uuid=_optional_str(prev_order_uuid),
            requested_new_identifier=new_identifier,
        )
        if new_uuid is None:
            try:
                lookup = self.client.order(uuid=None, identifier=resolved_new_identifier)
            except UpbitError:
                lookup = None
            if isinstance(lookup, dict):
                looked_up_uuid = _optional_str(lookup.get("uuid"))
                if looked_up_uuid and looked_up_uuid != cancelled_uuid:
                    new_uuid = looked_up_uuid
                    resolved_new_identifier = _optional_str(lookup.get("identifier")) or resolved_new_identifier
        accepted = new_uuid is not None or bool(resolved_new_identifier)
        return ExecutorReplaceResult(
            accepted=accepted,
            reason="" if new_uuid is not None else ("replace_accepted_new_order_pending_lookup" if accepted else "replace_accepted_new_order_unconfirmed"),
            cancelled_order_uuid=cancelled_uuid,
            new_order_uuid=new_uuid,
            new_identifier=resolved_new_identifier,
        )

    def _resolve_replace_volume(
        self,
        *,
        prev_order_uuid: str | None,
        prev_order_identifier: str | None,
        new_volume_str: str,
    ) -> str:
        requested = str(new_volume_str or "").strip()
        if not requested:
            raise ValidationError("new_volume_str is required")
        if requested.lower() != "remain_only":
            return requested
        payload = self.client.order(uuid=prev_order_uuid, identifier=prev_order_identifier)
        if not isinstance(payload, dict):
            raise ValidationError("remain_only could not be resolved")
        remaining = _optional_str(payload.get("remaining_volume")) or _optional_str(payload.get("remaining_volume_str"))
        if remaining:
            return remaining
        volume = _optional_str(payload.get("volume"))
        executed = _optional_str(payload.get("executed_volume"))
        if volume is None:
            raise ValidationError("remain_only could not be resolved")
        try:
            remaining_value = Decimal(volume) - Decimal(executed or "0")
        except (InvalidOperation, ValueError):
            raise ValidationError("remain_only could not be resolved")
        if remaining_value <= 0:
            raise ValidationError("remain_only could not be resolved")
        normalized = format(remaining_value.normalize(), "f")
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        return normalized or "0"

    def get_snapshot(self) -> ExecutorEvent:
        return ExecutorEvent(
            event_type="SNAPSHOT_UNAVAILABLE",
            ts_ms=0,
            payload_json="{}",
            payload={},
        )

    def stream_events(self) -> Iterator[ExecutorEvent]:
        if False:
            yield ExecutorEvent(event_type="", ts_ms=0, payload_json="{}", payload={})
        return
