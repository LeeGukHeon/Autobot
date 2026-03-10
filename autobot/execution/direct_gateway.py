"""Direct REST execution gateway for poll-based live runtime."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..upbit.exceptions import UpbitError
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
            payload = self.client.cancel_and_new_order(
                prev_order_uuid=prev_order_uuid,
                prev_order_identifier=prev_order_identifier,
                new_identifier=new_identifier,
                new_price=new_price_str,
                new_volume=new_volume_str,
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
        return ExecutorReplaceResult(
            accepted=True,
            reason="",
            cancelled_order_uuid=_optional_str(prev_order_uuid),
            new_order_uuid=_optional_str(payload.get("uuid") if isinstance(payload, dict) else None),
            new_identifier=_optional_str(payload.get("identifier") if isinstance(payload, dict) else None) or new_identifier,
        )

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
