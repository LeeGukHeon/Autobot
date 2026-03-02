# API Notes (Upbit)

## Rate Limit
- Read `Remaining-Req` on every response.
- Header format example: `group=default; min=1800; sec=29`
- `min` is deprecated for practical throttling decisions.
- Use `sec` as the active per-second remaining budget signal.

## 429 and 418
- `429 Too Many Requests`:
  - stop immediate burst for the affected group
  - apply cooldown/backoff before next attempt
- `418 I'm a teapot`:
  - treat as temporary ban
  - enforce cooldown from response hints (`Retry-After` or message parsing) when available
  - fallback to configured cooldown when hints are missing

## JWT Auth
- Always include:
  - `access_key`
  - per-request `nonce` (UUID)
- Include only when query/body exists:
  - `query_hash`
  - `query_hash_alg=SHA512`

## Query String / Hash Canonicalization
- Preserve parameter order exactly as input.
- Keep repeated array key form for bracket notation keys:
  - `states[]=wait&states[]=watch`
- Canonical builder policy:
  - `unquote(urlencode(params, doseq=True))`
- POST body:
  - request transport is JSON (`application/json; charset=utf-8`)
  - hash source is the body represented as query-string key-value pairs

## Logging Policy
- Record:
  - endpoint, method, status, latency, `Remaining-Req`, request-id, error metadata
- Never log:
  - API keys, bearer tokens, secrets, full sensitive payloads

## Credential Loading
- Private API credentials are read from env vars.
- `.env` at project root is auto-loaded when settings are loaded.
- Existing shell env vars have priority over `.env` values.

## Safe Private Smoke Test
- Use `POST /v1/orders/test` for order validation without creating a real order.
- Returned UUID/identifier from test endpoint is not for real-order query/cancel flows.
