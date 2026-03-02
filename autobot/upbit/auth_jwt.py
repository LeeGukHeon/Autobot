"""JWT generation for Upbit private REST requests."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import uuid
from collections.abc import Callable

from .exceptions import AuthError


class UpbitJwtSigner:
    """Creates HS512 JWT bearer tokens with optional query hash."""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        *,
        nonce_factory: Callable[[], uuid.UUID] | None = None,
    ) -> None:
        if not access_key:
            raise AuthError("UPBIT access key is empty")
        if not secret_key:
            raise AuthError("UPBIT secret key is empty")
        self._access_key = access_key
        self._secret_key = secret_key
        self._nonce_factory = nonce_factory or uuid.uuid4

    def build_authorization_header(self, query_string: str | None = None) -> str:
        token = self.build_token(query_string=query_string)
        return f"Bearer {token}"

    def build_token(self, query_string: str | None = None) -> str:
        payload: dict[str, str] = {
            "access_key": self._access_key,
            "nonce": str(self._nonce_factory()),
        }

        canonical_query = (query_string or "").strip()
        if canonical_query:
            payload["query_hash"] = hash_query_string(canonical_query)
            payload["query_hash_alg"] = "SHA512"

        header = {"alg": "HS512", "typ": "JWT"}
        encoded_header = _base64url(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        encoded_payload = _base64url(
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        )
        signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
        signature = hmac.new(self._secret_key.encode("utf-8"), signing_input, hashlib.sha512).digest()
        return f"{encoded_header}.{encoded_payload}.{_base64url(signature)}"


def hash_query_string(query_string: str) -> str:
    return hashlib.sha512(query_string.encode("utf-8")).hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")
