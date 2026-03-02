# ADR 0003 - Upbit REST Auth and Rate-Limit Architecture

## Status
Accepted (2026-03-03)

## Context
- Upbit Exchange APIs require JWT with per-request nonce and query hash semantics.
- Wrong query-string ordering/encoding can cause auth failures.
- Rate-limit behavior is group-based and exposed via `Remaining-Req`.
- Repeated 429 can escalate to 418 temporary bans.

## Decision
- Implement a dedicated `autobot.upbit` package with:
  - canonical query builder (`unquote(urlencode(..., doseq=True))`) with input-order preservation
  - JWT HS512 signer with `access_key`, `nonce`, and optional `query_hash` fields
  - centralized sync `httpx` client with:
    - structured status/error mapping
    - bounded retries for network/timeout/5xx
    - dedicated handling for 429 and 418 via limiter cooldown
  - group-aware token-bucket limiter synchronized with `Remaining-Req`
- Add CLI smoke commands for both public and private endpoints.
- Use `POST /v1/orders/test` for non-destructive private API verification.

## Consequences
### Positive
- Auth correctness is centralized and testable.
- Rate-limit violations are less likely to cause escalation bans.
- CLI can verify API connectivity quickly without placing real orders.
- Error handling behavior is deterministic and bounded.

### Trade-offs
- Sync client keeps implementation simpler, but async scaling is deferred.
- Limiter defaults are conservative and may under-utilize burst capacity.
- 418 cooldown parsing is best-effort when API does not expose clear wait fields.

## Alternatives Considered
- Direct ad-hoc requests from each command:
  - rejected due to duplicated auth/retry/rate-limit logic.
- Async-only client from the start:
  - rejected for this ticket to keep operational complexity low.
