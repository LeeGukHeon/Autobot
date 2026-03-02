# T02 - Upbit REST Client v1

## Goal
- Build a production-safe Upbit REST client with correct JWT auth and request throttling.
- Provide CLI smoke commands for public and private endpoints.
- Enforce non-destructive validation flow via `POST /v1/orders/test`.

## Scope Implemented
- New package: `autobot/upbit/`
  - `config.py`: `base.yaml + upbit.yaml` merge and env-key loading
  - `querystring.py`: order-preserving canonical query string builder
  - `auth_jwt.py`: HS512 JWT with `nonce`, `query_hash`, `query_hash_alg`
  - `remaining_req.py`: parser for `Remaining-Req`
  - `rate_limiter.py`: group limiter + 429/418 cooldown handling
  - `http_client.py`: httpx wrapper, retries, status mapping, structured logging
  - `public.py`: Quotation API wrappers (`markets`, `ticker`, `candles`)
  - `private.py`: Exchange API wrappers (`accounts`, `chance`, `order_test`)
- CLI expansion in `autobot/cli.py`:
  - `python -m autobot.cli upbit public markets`
  - `python -m autobot.cli upbit public ticker --markets KRW-BTC,KRW-ETH`
  - `python -m autobot.cli upbit public candles --market KRW-BTC --tf-min 1 --count 10`
  - `python -m autobot.cli upbit private accounts`
  - `python -m autobot.cli upbit private chance --market KRW-BTC`
  - `python -m autobot.cli upbit private order-test --market KRW-BTC --side bid --ord-type limit --price 10000 --volume 0.0001`

## Auth / Query Hash Rules
- GET/DELETE query hash source:
  - canonical query string from the same params used for request
  - preserves parameter order and repeated array keys like `states[]=wait&states[]=watch`
- POST query hash source:
  - JSON body converted to query-string form for hash generation
  - actual request body is still JSON with `application/json; charset=utf-8`
- Per-request nonce:
  - always regenerated UUID for each call

## Rate Limit Policy
- Parses `Remaining-Req` header (`group`, `sec`)
- Applies group-level token bucket defaults from config
- `429`:
  - immediate group cooldown and bounded retry
- `418`:
  - forced cooldown by header/body extraction when available
  - fallback to `upbit.ratelimit.ban_cooldown_sec`

## Logging Policy
- Structured request log fields:
  - `method`, `endpoint`, `status`, `latency_ms`, `remaining_req`, `request_id`, `error_name`, `error_message`
- Sensitive info is not logged:
  - no access key, no secret, no bearer token, no full request body dump

## Tests Added
- `tests/test_upbit_querystring.py`
- `tests/test_upbit_remaining_req.py`
- `tests/test_upbit_auth_jwt.py`
- `tests/test_upbit_error_mapping.py`

## Notes
- `config/secrets.example.env` now uses placeholders (`...`) only.
- Real API keys must be provided via local `.env` or shell env vars and never committed.
- `.env` is auto-loaded by `autobot.upbit.config.load_upbit_settings()` (non-override mode).

## T02-Pre (Package SSOT)
- SSOT package fixed to root: `autobot/`
- `python/autobot/` minimized as deprecated mirror guidance
- Added sync helper script: `scripts/sync_python_autobot.ps1`
- Architectural decision recorded in ADR 0004
