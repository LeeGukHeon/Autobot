from __future__ import annotations

import base64
import hashlib
import json

from autobot.upbit.auth_jwt import UpbitJwtSigner


def test_jwt_payload_includes_query_hash_when_query_exists() -> None:
    query = "market=KRW-BTC&states[]=wait&states[]=watch"
    signer = UpbitJwtSigner(access_key="access", secret_key="secret")

    token = signer.build_token(query_string=query)
    payload = _decode_payload(token)

    assert payload["access_key"] == "access"
    assert payload["query_hash_alg"] == "SHA512"
    assert payload["query_hash"] == hashlib.sha512(query.encode("utf-8")).hexdigest()


def test_jwt_payload_excludes_query_hash_without_query() -> None:
    signer = UpbitJwtSigner(access_key="access", secret_key="secret")

    token = signer.build_token()
    payload = _decode_payload(token)

    assert payload["access_key"] == "access"
    assert "query_hash" not in payload
    assert "query_hash_alg" not in payload


def test_jwt_nonce_changes_every_call() -> None:
    signer = UpbitJwtSigner(access_key="access", secret_key="secret")
    first = _decode_payload(signer.build_token())
    second = _decode_payload(signer.build_token())
    assert first["nonce"] != second["nonce"]


def _decode_payload(token: str) -> dict[str, str]:
    payload_segment = token.split(".")[1]
    padded = payload_segment + ("=" * (-len(payload_segment) % 4))
    decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
    return json.loads(decoded.decode("utf-8"))
