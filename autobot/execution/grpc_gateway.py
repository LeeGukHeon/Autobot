"""gRPC execution gateway that routes order intents to external executor."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import json
import time
from typing import Any

from .intent import OrderIntent, new_order_intent


@dataclass(frozen=True)
class ExecutorSubmitResult:
    accepted: bool
    reason: str
    upbit_uuid: str | None
    identifier: str | None
    intent_id: str | None


@dataclass(frozen=True)
class ExecutorReplaceResult:
    accepted: bool
    reason: str
    cancelled_order_uuid: str | None
    new_order_uuid: str | None
    new_identifier: str | None


@dataclass(frozen=True)
class ExecutorEvent:
    event_type: str
    ts_ms: int
    payload_json: str
    payload: dict[str, Any]


class GrpcExecutionGateway:
    """Thin Python client for `ExecutionService` gRPC contract."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        timeout_sec: float = 5.0,
        insecure: bool = True,
    ) -> None:
        runtime = _load_runtime()
        if not insecure:
            raise ValueError("secure channel mode is not implemented in MVP; use insecure=true")

        target = f"{str(host).strip()}:{int(port)}"
        if not target or target.startswith(":"):
            raise ValueError("host and port are required")

        self._grpc = runtime["grpc"]
        self._pb2 = runtime["pb2"]
        self._timeout_sec = max(float(timeout_sec), 0.1)
        self._channel = self._grpc.insecure_channel(target)
        self._stub = runtime["pb2_grpc"].ExecutionServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> GrpcExecutionGateway:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def ping(self) -> dict[str, Any]:
        try:
            response = self._stub.Health(self._pb2.HealthRequest(), timeout=self._timeout_sec)
        except self._grpc.RpcError as exc:
            raise RuntimeError(_rpc_error_message("Health", exc)) from exc
        return {
            "ok": bool(getattr(response, "ok", False)),
            "message": str(getattr(response, "message", "")),
            "ts_ms": int(getattr(response, "ts_ms", 0)),
        }

    def submit_intent(
        self,
        *,
        intent: OrderIntent,
        identifier: str,
        meta_json: str | None = None,
    ) -> ExecutorSubmitResult:
        identifier_value = str(identifier).strip()
        if not identifier_value:
            raise ValueError("identifier is required")
        if str(intent.ord_type).strip().lower() != "limit":
            raise ValueError("gRPC executor currently supports limit intents only")
        if str(intent.time_in_force).strip().lower() == "post_only":
            raise ValueError("gRPC executor currently does not support post_only")
        if intent.price is None or intent.volume is None:
            raise ValueError("gRPC executor requires both price and volume")

        meta_value = meta_json if meta_json is not None else _build_meta_json(intent)
        request = self._pb2.OrderIntent(
            intent_id=str(intent.intent_id),
            identifier=identifier_value,
            market=str(intent.market).strip().upper(),
            side=_to_side_enum(intent.side, self._pb2),
            ord_type=_to_ord_type_enum(intent.ord_type, self._pb2),
            price=float(intent.price),
            volume=float(intent.volume),
            tif=_to_tif_enum(intent.time_in_force, self._pb2),
            ts_ms=int(intent.ts_ms),
            meta_json=meta_value,
        )
        try:
            response = self._stub.SubmitIntent(request, timeout=self._timeout_sec)
        except self._grpc.RpcError as exc:
            raise RuntimeError(_rpc_error_message("SubmitIntent", exc)) from exc
        return ExecutorSubmitResult(
            accepted=bool(getattr(response, "accepted", False)),
            reason=str(getattr(response, "reason", "")),
            upbit_uuid=_as_optional_str(getattr(response, "upbit_uuid", "")),
            identifier=_as_optional_str(getattr(response, "identifier", "")),
            intent_id=_as_optional_str(getattr(response, "intent_id", "")),
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
        now_ms = int(time.time() * 1000)
        intent = new_order_intent(
            market=market,
            side=side,
            price=float(price),
            volume=float(volume),
            reason_code="EXEC_SUBMIT_TEST",
            ord_type="limit",
            time_in_force="gtc",
            meta={"submit_mode": "order_test"},
            ts_ms=now_ms,
        )
        identifier_value = _as_optional_str(identifier) or f"AUTOBOT-EXEC-TEST-{intent.intent_id[:12]}-{now_ms}"
        return self.submit_intent(intent=intent, identifier=identifier_value)

    def cancel(
        self,
        *,
        upbit_uuid: str | None = None,
        identifier: str | None = None,
    ) -> ExecutorSubmitResult:
        uuid_value = _as_optional_str(upbit_uuid)
        identifier_value = _as_optional_str(identifier)
        if uuid_value is None and identifier_value is None:
            raise ValueError("upbit_uuid or identifier is required")

        request = self._pb2.CancelRequest(
            upbit_uuid=uuid_value or "",
            identifier=identifier_value or "",
        )
        try:
            response = self._stub.Cancel(request, timeout=self._timeout_sec)
        except self._grpc.RpcError as exc:
            raise RuntimeError(_rpc_error_message("Cancel", exc)) from exc
        return ExecutorSubmitResult(
            accepted=bool(getattr(response, "accepted", False)),
            reason=str(getattr(response, "reason", "")),
            upbit_uuid=_as_optional_str(getattr(response, "upbit_uuid", "")),
            identifier=_as_optional_str(getattr(response, "identifier", "")),
            intent_id=_as_optional_str(getattr(response, "intent_id", "")),
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
        prev_uuid_value = _as_optional_str(prev_order_uuid)
        prev_identifier_value = _as_optional_str(prev_order_identifier)
        if prev_uuid_value is None and prev_identifier_value is None:
            raise ValueError("prev_order_uuid or prev_order_identifier is required")

        new_identifier_value = str(new_identifier).strip()
        new_price_value = str(new_price_str).strip()
        new_volume_value = str(new_volume_str).strip()
        if not new_identifier_value:
            raise ValueError("new_identifier is required")
        if not new_price_value:
            raise ValueError("new_price_str is required")
        if not new_volume_value:
            raise ValueError("new_volume_str is required")

        request = self._pb2.ReplaceRequest(
            intent_id=str(intent_id).strip(),
            prev_order_uuid=prev_uuid_value or "",
            prev_order_identifier=prev_identifier_value or "",
            new_identifier=new_identifier_value,
            new_price_str=new_price_value,
            new_volume_str=new_volume_value,
            new_time_in_force=str(new_time_in_force or "").strip().lower(),
        )
        try:
            response = self._stub.ReplaceOrder(request, timeout=self._timeout_sec)
        except self._grpc.RpcError as exc:
            raise RuntimeError(_rpc_error_message("ReplaceOrder", exc)) from exc
        return ExecutorReplaceResult(
            accepted=bool(getattr(response, "accepted", False)),
            reason=str(getattr(response, "reason", "")),
            cancelled_order_uuid=_as_optional_str(getattr(response, "cancelled_order_uuid", "")),
            new_order_uuid=_as_optional_str(getattr(response, "new_order_uuid", "")),
            new_identifier=_as_optional_str(getattr(response, "new_identifier", "")),
        )

    def get_snapshot(self) -> ExecutorEvent:
        try:
            raw_event = self._stub.GetSnapshot(self._pb2.HealthRequest(), timeout=self._timeout_sec)
        except self._grpc.RpcError as exc:
            raise RuntimeError(_rpc_error_message("GetSnapshot", exc)) from exc
        return _decode_event(raw_event, pb2=self._pb2)

    def stream_events(self) -> Iterator[ExecutorEvent]:
        try:
            stream = self._stub.StreamEvents(self._pb2.HealthRequest(), timeout=None)
            for raw_event in stream:
                yield _decode_event(raw_event, pb2=self._pb2)
        except self._grpc.RpcError as exc:
            raise RuntimeError(_rpc_error_message("StreamEvents", exc)) from exc


def _load_runtime() -> dict[str, Any]:
    try:
        import grpc  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("grpc runtime not installed. add grpcio to python dependencies.") from exc

    try:
        from . import autobot_pb2, autobot_pb2_grpc  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "gRPC stubs not generated. run: "
            "python -m grpc_tools.protoc -I proto --python_out=autobot/execution "
            "--grpc_python_out=autobot/execution proto/autobot.proto"
        ) from exc

    return {
        "grpc": grpc,
        "pb2": autobot_pb2,
        "pb2_grpc": autobot_pb2_grpc,
    }


def _decode_event(raw_event: Any, *, pb2: Any) -> ExecutorEvent:
    event_type_value = int(getattr(raw_event, "event_type", 0))
    event_type = str(pb2.EventType.Name(event_type_value))
    payload_json = str(getattr(raw_event, "payload_json", "") or "{}")
    payload = _parse_json_dict(payload_json)
    return ExecutorEvent(
        event_type=event_type,
        ts_ms=int(getattr(raw_event, "ts_ms", 0)),
        payload_json=payload_json,
        payload=payload,
    )


def _to_side_enum(side: str, pb2: Any) -> int:
    side_value = str(side).strip().lower()
    if side_value == "bid":
        return int(pb2.BID)
    if side_value == "ask":
        return int(pb2.ASK)
    raise ValueError("side must be bid or ask")


def _to_ord_type_enum(ord_type: str, pb2: Any) -> int:
    ord_type_value = str(ord_type).strip().lower()
    if ord_type_value == "limit":
        return int(pb2.LIMIT)
    raise ValueError("only limit ord_type is supported in MVP")


def _to_tif_enum(tif: str, pb2: Any) -> int:
    value = str(tif).strip().lower()
    if value == "gtc":
        return int(pb2.GTC)
    if value == "ioc":
        return int(pb2.IOC)
    if value == "fok":
        return int(pb2.FOK)
    raise ValueError("time_in_force must be one of: gtc, ioc, fok")


def _build_meta_json(intent: OrderIntent) -> str:
    payload = {
        "reason_code": intent.reason_code,
        "meta": intent.meta,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _parse_json_dict(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _rpc_error_message(method: str, exc: Any) -> str:
    code_fn = getattr(exc, "code", None)
    details_fn = getattr(exc, "details", None)
    code_name = "UNKNOWN"
    details = str(exc)
    if callable(code_fn):
        try:
            code = code_fn()
            code_name = getattr(code, "name", str(code))
        except Exception:  # pragma: no cover - defensive
            code_name = "UNKNOWN"
    if callable(details_fn):
        try:
            details = str(details_fn())
        except Exception:  # pragma: no cover - defensive
            details = str(exc)
    return f"executor rpc {method} failed: {code_name} {details}".strip()
