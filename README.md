# Payment Cascading Gateway

A payment gateway that creates deposit/withdraw payments, routes them through mock
payment providers, and **cascades** to the next provider on a retryable failure —
while being careful never to double-charge when a provider's response is ambiguous
(timeout, 5xx) or arrives late.

Design doc: [`docs/superpowers/specs/2026-11-11-payment-cascading-design.md`](docs/superpowers/specs/2026-11-11-payment-cascading-design.md)
Implementation plan: [`docs/superpowers/plans/2026-11-11-payment-cascading-implementation.md`](docs/superpowers/plans/2026-11-11-payment-cascading-implementation.md)

## Architecture

```mermaid
flowchart TD
    A[POST /payments] -->|Idempotency-Key| B[Payment created, status=pending]
    B --> C[asyncio background task: run_cascade]
    C --> D{order_providers}
    D -->|manual priority first, then adaptive score| E[Try next provider]
    E --> F[adapter.submit]
    F -->|success| G[finalize_success]
    F -->|declined_final| H[finalize_failed — stop, no more providers]
    F -->|declined_retryable| E
    F -->|ambiguous: timeout / 5xx / pending-webhook| I[resolve_ambiguous]
    I -->|resolved in time| F
    I -->|deadline exceeded| J[mark attempt timed_out, stop cascading — outcome is unknown, not failed]
    J --> R[payment.status=review_required, no further providers tried]
    G --> K[send_merchant_webhook: payment.succeeded]
    H --> L[send_merchant_webhook: payment.failed]

    M[POST /webhooks/providers/id] --> N{waiter registered?}
    N -->|yes, within deadline| I
    N -->|no — late or duplicate| O[handle_late_resolution]
    O -->|attempt already succeeded| P[no-op]
    O -->|payment is review_required| S[log outcome on the attempt only — status untouched, human resolves]
    O -->|contradicts an already-finalized outcome| Q[payment.status=conflict + ReconciliationFlag + payment.conflict_detected webhook]
```

**Core pieces** (`app/`):

- `providers/` — three mock adapters (`fast_sync`, `webhook_async`, `flaky`), each
  using a *different* response convention (HTTP status code, `200` + body, or a
  mix), and each normalizing its own raw response into one shared `AttemptOutcome`
  shape. Nothing outside `app/providers/*` ever looks at a raw HTTP status.
- `routing.py` / `stats.py` — provider ordering: manual `priority` wins if set on
  any eligible provider; otherwise providers rank by a Laplace-smoothed adaptive
  success score, `(success+1)/(success+failure+2)`.
- `cascade.py` — the orchestrator. A `declined_final` outcome (fraud, insufficient
  funds) stops the whole cascade immediately — retrying elsewhere would just fail
  again everywhere. A `declined_retryable` outcome moves to the next provider. An
  `ambiguous` outcome is never treated as a failure outright: `reconciliation.py`
  either polls the provider's status-check endpoint or waits for its webhook, up to
  a deadline, before the cascade is allowed to continue. **If the deadline passes
  with no answer, the cascade stops entirely** — `payment.status = review_required`,
  no further provider is tried, and nothing resumes automatically. The provider's
  real outcome is genuinely unknown at that point (not "probably failed"), and
  trying another provider on an unconfirmed attempt is exactly how a double charge
  happens. A human has to reconcile directly with the provider; there is no
  background retry and no automatic resolution path (see Limitations).
- `webhooks.py` — outgoing merchant webhooks (HMAC-signed, retried with backoff) and
  the in-process event bus that wakes up a waiting `reconciliation.resolve_ambiguous`
  call when a provider's webhook arrives in time.
- `idempotency.py` — `Idempotency-Key` enforcement: same key + same payload replays
  the original payment; same key + a different payload is a `409`.
- `handle_late_resolution` (in `cascade.py`) — the late/duplicate webhook path.
  - If the payment is already `review_required`, the webhook's outcome is logged
    onto the `ProviderAttempt` (so the on-call engineer can see what the provider
    actually said) but `payment.status` is never touched automatically — it stays
    a human decision.
  - Otherwise, if this late `SUCCESS` contradicts an outcome the cascade already
    finalized (`succeeded`/`failed` via a *different* attempt), that's a genuine
    conflict: it flags `payment.status = conflict`, writes a `ReconciliationFlag`,
    and fires a dedicated `payment.conflict_detected` webhook instead of touching
    the already-sent `payment.succeeded`/`payment.failed` event.
- **Optimistic locking on `Payment`** — `finalize_success`, `finalize_failed`,
  `mark_review_required`, and the conflict-flagging branch of
  `handle_late_resolution` all read-then-write the same `Payment` row from
  *different* sessions (the cascade's background task vs. an incoming provider
  webhook request). `Payment.version` (SQLAlchemy `version_id_col`) makes a stale
  write raise `StaleDataError` instead of silently overwriting a concurrent change;
  `cascade._retry_on_stale_payment` catches it, re-reads the row, and re-decides
  with fresh data instead of retrying blindly.

## Running it

```bash
docker-compose up -d          # postgres on localhost:5432
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/alembic upgrade head
.venv/bin/python scripts/seed_providers.py   # inserts the 3 mock providers
.venv/bin/uvicorn app.main:app --reload
```

## Running the tests

Tests run against an in-memory SQLite engine (see `tests/conftest.py`), not
Postgres, so no running database is required:

```bash
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -v
```

## Demoing each corner case

All of these are implemented as automated tests in `tests/test_corner_cases.py`;
the curl examples below assume a running server with providers seeded via
`scripts/seed_providers.py` (the test suite seeds them separately, in-memory,
via the `seed_providers` fixture).

**Manual priority overrides adaptive history** — a provider with a long success
history still loses to a manually-prioritized provider:

```bash
curl -X POST localhost:8000/payments \
  -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" \
  -d '{"direction":"deposit","amount":1000,"currency":"USD","card_bin":"411111",
       "card_type":"visa","callback_url":"https://example.test/cb",
       "provider_scenarios":{"fast_sync":"insufficient_funds"}}'
```

**Final decline stops the cascade** (no further providers tried):

```bash
curl -X POST localhost:8000/payments \
  -H "Idempotency-Key: demo-2" -H "Content-Type: application/json" \
  -d '{"direction":"deposit","amount":1000,"currency":"USD","card_bin":"411111",
       "card_type":"visa","callback_url":"https://example.test/cb",
       "provider_scenarios":{"fast_sync":"fraud_suspected"}}'
```

**Retryable decline cascades to the next provider:**

```bash
curl -X POST localhost:8000/payments \
  -H "Idempotency-Key: demo-3" -H "Content-Type: application/json" \
  -d '{"direction":"deposit","amount":1000,"currency":"USD","card_bin":"411111",
       "card_type":"visa","callback_url":"https://example.test/cb",
       "provider_scenarios":{"fast_sync":"processing_error"}}'
```

**All providers exhausted:**

```bash
curl -X POST localhost:8000/payments \
  -H "Idempotency-Key: demo-4" -H "Content-Type: application/json" \
  -d '{"direction":"deposit","amount":1000,"currency":"USD","card_bin":"411111",
       "card_type":"visa","callback_url":"https://example.test/cb",
       "provider_scenarios":{"fast_sync":"processing_error","flaky":"declined_via_body"}}'
```

**Duplicate POST with the same Idempotency-Key is a no-op** (returns the original
payment, `200` instead of `202`):

```bash
curl -X POST localhost:8000/payments -H "Idempotency-Key: demo-5" \
  -H "Content-Type: application/json" -d '{...}'
curl -X POST localhost:8000/payments -H "Idempotency-Key: demo-5" \
  -H "Content-Type: application/json" -d '{...}'   # same body -> 200, same id
```

**Duplicate webhook delivery is a no-op:**

```bash
curl -X POST localhost:8000/webhooks/providers/webhook_async \
  -d '{"attempt_id":"<id>","status":"approved"}'
curl -X POST localhost:8000/webhooks/providers/webhook_async \
  -d '{"attempt_id":"<id>","status":"approved"}'   # repeat -> still 200, no second webhook
```

**Ambiguous timeout stops the cascade for manual review — a late webhook only
logs, it never auto-resolves:**

```bash
# webhook_async never answers within the deadline: the cascade stops here,
# fast_sync (next in line) is never tried, and the payment sits as review_required.
# GET /payments/<id> shows status: "review_required", exactly one attempt.
curl -X POST localhost:8000/webhooks/providers/webhook_async \
  -d '{"attempt_id":"<timed-out-attempt-id>","status":"approved"}'
# GET /payments/<id> still shows status: "review_required" — the webhook is
# recorded on the attempt (decline_code/provider_reference), but nothing
# auto-finalizes; resolving it is a manual tool outside this API.
```

**Late webhook that genuinely contradicts an already-finalized payment still
flags a conflict** (this is now the narrower case: the cascade finished via one
provider, and a *different* attempt's late webhook disagrees with that):

```bash
curl -X POST localhost:8000/webhooks/providers/webhook_async \
  -d '{"attempt_id":"<a-different-already-timed-out-attempt-id>","status":"approved"}'
# GET /payments/<id> now shows status: "conflict"
```

**Adaptive routing prefers the provider with the better success history** (no
manual priority set on either):

```bash
curl -X POST localhost:8000/payments \
  -H "Idempotency-Key: demo-8" -H "Content-Type: application/json" \
  -d '{"direction":"deposit","amount":1000,"currency":"USD","card_bin":"411111",
       "card_type":"visa","callback_url":"https://example.test/cb",
       "provider_scenarios":{"fast_sync":"success","flaky":"declined_via_http"}}'
```

## Limitations / what I'd do differently with more time

- **No manual-resolution tool.** A payment parked as `review_required` (or
  `conflict`) stays there until someone changes it — there is deliberately no
  endpoint for that in this API. Resolving it means talking to the provider
  directly and then writing the correct status by hand (direct DB access/admin
  tool, not built here); automating "what do we do once we know the truth" is a
  business decision outside a 24h/short-cycle scope, same reasoning as the next
  point.
- **No automatic reversal on conflict.** When a late webhook reveals two providers
  both reported success for the same payment, the system flags `status=conflict`
  and stops — it does not attempt to automatically refund or reverse the duplicate.
  That's a deliberate choice: reversal (especially for withdrawals, where money
  already left to the end user) is a business/compliance decision, not something
  safe to automate in a 24-hour scope.
- **Real payment-domain gaps this project doesn't touch at all — named explicitly
  here instead of silently assumed away:**
  - **No PAN/token handling.** Only `card_bin` is stored. Routing a payment to a
    *second* PSP on cascade requires either the raw PAN or a network/vault token
    for that PSP — this project has neither, which means the cascade as built
    couldn't actually retry on real providers without a tokenization vault and
    PCI DSS scope (SAQ D at minimum) in front of it.
  - **No 3DS.** Cascading after a 3DS challenge, liability-shift handling, and
    soft-decline codes (e.g. `1A`) aren't modeled. A real second attempt after a
    3DS-involved decline has different rules than a plain retry.
  - **Decline codes are invented for the mocks**, not mapped to real scheme
    taxonomies (Mastercard MAC, Visa's excessive-retry penalties/limits). The
    `final` vs `retryable` classification is illustrative, not a real
    scheme-compliant decision table.
  - **Payouts (`withdraw`) run through the identical cascade as deposits.** This
    release's fix (stop on unresolved-ambiguous instead of cascading) applies
    equally to both directions, which does close the specific double-charge path
    for payouts too — but a payout's risk profile is still otherwise unexamined:
    money that already left to the end user is harder to claw back than a card
    charge, and that asymmetry isn't reflected anywhere in routing or retry policy.
  - **`card_bin` is stored but never read.** No BIN/geo-based routing, no
    cost/fee-aware provider selection, no circuit breaker for a provider that's
    currently failing, and `ProviderStats` has no time window or decay — a
    provider's history from months ago weighs the same as yesterday's.
- **No external task broker.** The cascade runs as an in-process `asyncio` task,
  not a Celery/RQ job. If the process restarts mid-cascade, that payment is stuck
  `pending`/`processing` forever — there's no crash recovery (an unexpected error
  *within* a running cascade is handled — it finalizes the payment as `failed`
  and logs — but a killed process loses the in-flight task entirely). In
  production this would be a durable queue (Celery/RQ + Redis, or a
  Postgres-backed job table with a poller that can resume orphaned payments).
- **No auth on the API, and the provider webhook endpoint specifically trusts its
  caller.** There's no API key / merchant identity on `POST`/`GET /payments`
  (every payment belongs to a single implicit merchant, see below), and
  `POST /webhooks/providers/{id}` has no signature verification either — anything
  that can reach it can address any `attempt_id` and flip a finalized payment to
  `conflict`, or bump a provider's adaptive-routing stats. A real deployment would
  need per-provider webhook secrets (HMAC, mirroring what this gateway already
  does for its *outgoing* merchant webhook) before this endpoint is internet-facing.
- **No multi-tenancy.** The schema has no merchant/account concept — one logical
  merchant, one `callback_url` per payment. A real system would scope providers,
  routing rules, and stats per merchant.
- **No automatic reversal on conflict.** When a late webhook reveals two providers
  both reported success for the same payment, the system flags `status=conflict`
  and stops — it does not attempt to automatically refund or reverse the duplicate.
  That's a deliberate choice: reversal (especially for withdrawals, where money
  already left to the end user) is a business/compliance decision, not something
  safe to automate in a 24-hour scope.
- **`provider_scenarios` is a testing/demo hook.** Real providers wouldn't need a
  payload field telling them what to do — this exists purely so the three mock
  providers' behavior is deterministic and demoable through the real API instead of
  only through internal unit tests.
- **Reconciliation deadline is a single global default (10s).** A real system would
  likely tune this per provider (a provider with a known slow settlement window
  needs a longer deadline than one that's normally synchronous).
- **The merchant webhook's HMAC signature has no timestamp/nonce**, so a captured
  request is technically replayable. The design only called for "signed"; the
  merchant side is expected to be idempotent on `(payment_id, status)` regardless,
  which bounds the damage of a replay to "redundant", not "harmful".
- **No rate limiting, request size limits, or CORS configuration** — out of scope
  for this exercise, but real deployment-facing concerns.
- **`amount` is a bare `int` in minor units** (e.g. cents) with no currency-specific
  exponent handling — fine for USD, wrong for a currency like JPY (0 decimal
  places) or BHD (3). A real system would look up the exponent per ISO 4217 code.
