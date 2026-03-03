# ADR 0007 - Executor Request Signing Contract

## Status
Accepted (2026-03-03)

## Context
- Upbit private auth requires `query_hash` to be computed from the exact request query/body key-value sequence.
- Reordering keys, hashing encoded text, or serializing body and hash-source differently creates intermittent auth failures.
- Order endpoints are non-idempotent when `identifier` is reused, so request retries are a footgun.

## Decision
- Introduce `upbit/request_builder.*` as the single source for:
  - URL query string (encoded transport form)
  - `query_hash` source string (unencoded form)
  - JSON body (`application/json; charset=utf-8`)
- Builder uses `OrderedParams` (`vector<pair<string,string>>`) and preserves insertion order.
- Array keys keep bracket notation in URL query (`states[]`, `uuids[]`).
- For `POST` order creation:
  - executor sends exactly one HTTP attempt (`allow_retry=false`)
  - timeout/network/5xx result is resolved by `GET /v1/order?identifier=...` (no duplicate POST with same identifier)

## Consequences
### Positive
- Hash/source mismatch class of bugs is removed from executor call sites.
- Duplicate-order risk from automatic POST retry is reduced.
- Endpoint-specific validation can run before network call (`orders/uuids` constraints, uuid-vs-identifier precedence).

### Trade-offs
- Builder enforces string-based body values; callers must format numbers explicitly.
- Additional pre-validation returns local 400-style errors for invalid combinations.

