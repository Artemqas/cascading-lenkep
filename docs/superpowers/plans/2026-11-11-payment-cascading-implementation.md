# Payment Cascading Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI payment gateway that creates deposit/withdraw payments, cascades across mock providers on retryable failure, resolves ambiguous provider responses before cascading, and detects late-arriving conflicts — per the approved design doc.

**Architecture:** Async FastAPI app over PostgreSQL (SQLAlchemy 2.0 async + Alembic). A single in-process cascade orchestrator walks an ordered provider list (manual priority first, else Laplace-smoothed adaptive score) per payment, delegating response normalization to per-provider adapters and ambiguous-outcome resolution to a reconciliation module backed by an in-process webhook event bus. No external broker — background work runs as asyncio tasks, state lives in Postgres.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), asyncpg, Alembic, pytest + pytest-asyncio + httpx (tests run against SQLite/aiosqlite for speed — see Task 1), docker-compose (Postgres).

**Spec:** `docs/superpowers/specs/2026-11-11-payment-cascading-design.md` — executors should read both documents; this plan does not repeat the design's rationale, only the exact interfaces and tests needed to build it.

## Global Constraints

- Idempotency-Key required on `POST /payments`; same key + same payload → return original payment; same key + different payload → `409`.
- Decline classification governs cascade continuation: `declined_final` **stops the whole cascade** (payment fails immediately, does not try remaining providers); `declined_retryable` moves to the next provider; unresolved-by-deadline `ambiguous` is treated as retryable for cascade purposes but recorded as `timed_out`, never silently overwritten.
- Manual `priority` (if set on any eligible provider) ranks those providers first, in `priority` order; providers without a manual priority are ranked among themselves by adaptive score `(success_count + 1) / (success_count + failure_count + 2)`, computed per `(provider_id, direction, card_type)`.
- No external broker/queue — cascade runs as an asyncio task per payment; all state transitions are persisted to Postgres so the audit trail survives.
- Provider adapters own 100% of response-shape normalization (HTTP status code vs. `200` + error-in-body); nothing outside `app/providers/*` ever branches on raw HTTP status.
- Conflict (late success contradicting an already-finalized payment) sets `Payment.status = conflict`, writes a `ReconciliationFlag`, and fires a dedicated `payment.conflict_detected` merchant webhook — no automatic reversal.
- Merchant webhooks are HMAC-signed and retried with backoff on non-2xx.
- Tests run against SQLite (`aiosqlite`) for speed; runtime uses Postgres. All ORM columns must use types valid on both (`JSON`, not Postgres `ARRAY`) — this is a plan-level call, not in the design doc, made for 24h-deadline simplicity.

## Review Focus

- **Final decline must stop the cascade, not just skip that provider.** A reasonable implementer under time pressure writes "on any decline, try next provider" — the spec explicitly forbids this for `declined_final`. Test owned by Task 12 (cascade orchestrator).
- **Idempotent replay must not re-run the cascade or create a second payment.** Posting the same `Idempotency-Key` + body twice while the first is still `processing` must not spawn a second cascade task. Test owned by Task 13 (payments API).
- **A duplicate webhook delivery (matches the already-recorded outcome) must be a no-op**, not a second merchant webhook or a double stats increment. Test owned by Task 14 (incoming provider webhook).
- **HTTP-status-coded declines and body-coded declines must normalize to the identical `AttemptOutcome` shape** — this is the corner case the user raised explicitly. Tests owned by Tasks 4–6 (each adapter), asserting the same assertion helper against different raw-response shapes.
- **A `timed_out` attempt must remain visible in the audit trail after the cascade moves on** — not deleted, not silently relabeled as the next attempt's status. Test owned by Task 10 (reconciliation) and Task 12 (cascade).

---

## File Structure

```
pyproject.toml
docker-compose.yml
alembic.ini
alembic/versions/
app/
  config.py            # Settings (env-driven)
  db.py                # async engine/session factory, get_session dependency
  main.py              # FastAPI app, router mounting, /health
  enums.py             # Direction, CardType, PaymentStatus, AttemptStatus
  models.py            # Payment, Provider, ProviderAttempt, ProviderStats, ReconciliationFlag
  providers/
    base.py            # OutcomeKind, AttemptOutcome, ProviderAttemptRequest, WebhookResolution, ProviderAdapter Protocol
    fast_sync.py        # provider_fast_sync adapter
    webhook_async.py     # provider_webhook_async adapter
    flaky.py             # provider_flaky adapter
    registry.py          # provider_id -> adapter instance, eligible_providers()
  stats.py             # laplace_score, record_result, correct_result, get_score
  routing.py           # order_providers()
  webhooks.py          # WebhookEventBus, send_merchant_webhook()
  reconciliation.py    # resolve_ambiguous()
  cascade.py           # run_cascade(), finalize_success(), finalize_failed(), handle_late_resolution()
  idempotency.py       # get_or_create_payment(), IdempotencyConflict
  schemas.py           # Pydantic request/response models
  api/
    payments.py         # POST /payments, GET /payments/{id}
    provider_webhooks.py # POST /webhooks/providers/{provider_id}
tests/
  conftest.py
  providers/test_fast_sync.py
  providers/test_webhook_async.py
  providers/test_flaky.py
  test_stats.py
  test_routing.py
  test_reconciliation.py
  test_cascade.py
  test_idempotency.py
  test_payments_api.py
  test_provider_webhooks_api.py
  test_corner_cases.py
README.md
```

---

### Task 1: Project scaffolding, config, DB session, health check

**Files:**
- Create: `pyproject.toml`, `docker-compose.yml`, `app/config.py`, `app/db.py`, `app/main.py`, `tests/conftest.py`
- Test: `tests/test_health.py`

**Interfaces:**
- Produces: `app.config.Settings` (pydantic-settings) with fields `database_url: str`, `max_cascade_attempts: int = 10`, `reconciliation_deadline_seconds: float = 10.0`, `merchant_webhook_secret: str`, loaded via `get_settings() -> Settings`. (Idempotency keys never expire in this scope — no window field; see Task 13.)
- Produces: `app.db.get_session() -> AsyncIterator[AsyncSession]` (FastAPI dependency), `app.db.engine`, `app.db.Base` (declarative base).
- Produces: `app.main.app` (FastAPI instance) with `GET /health` returning `{"status": "ok"}`.
- Produces test fixtures in `conftest.py`: `db_session` (fresh SQLite in-memory `AsyncSession` per test, schema created from `Base.metadata`), `client` (httpx `AsyncClient` wrapping `app` with `get_session` overridden to yield `db_session`).

- [ ] **Step 1: Write the failing test**

```python
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_health.py -v`
Expected: FAIL (no app / no fixtures)

- [ ] **Step 3: Implement scaffolding**

`docker-compose.yml` with a single `postgres:16` service. `config.py` Settings class per above, reading from env with the stated defaults. `db.py` using `create_async_engine` + `async_sessionmaker`. `main.py` instantiating `FastAPI()` and the health route. `conftest.py` builds the SQLite engine (`sqlite+aiosqlite:///:memory:`) with `StaticPool` so the in-memory DB persists across connections within a test, runs `Base.metadata.create_all` before yielding, drops after.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_health.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml docker-compose.yml app/config.py app/db.py app/main.py tests/conftest.py tests/test_health.py
git commit -m "feat: project scaffolding, config, db session, health check"
```

---

### Task 2: Domain enums and provider outcome types

**Files:**
- Create: `app/enums.py`, `app/providers/base.py`
- Test: `tests/providers/test_base.py`

**Interfaces:**
- Produces (`app/enums.py`): `Direction(str, Enum)` = `DEPOSIT="deposit"`, `WITHDRAW="withdraw"`; `CardType(str, Enum)` = `VISA="visa"`, `MASTERCARD="mastercard"`, `OTHER="other"`; `PaymentStatus(str, Enum)` = `PENDING`, `PROCESSING`, `SUCCEEDED`, `FAILED`, `CONFLICT`; `AttemptStatus(str, Enum)` = `PENDING`, `SUCCEEDED`, `DECLINED_FINAL`, `DECLINED_RETRYABLE`, `AMBIGUOUS`, `TIMED_OUT`.
- Produces (`app/providers/base.py`):
  ```python
  class OutcomeKind(str, Enum):
      SUCCESS = "success"
      DECLINED_FINAL = "declined_final"
      DECLINED_RETRYABLE = "declined_retryable"
      AMBIGUOUS = "ambiguous"

  @dataclass(frozen=True)
  class AttemptOutcome:
      kind: OutcomeKind
      decline_code: str | None = None
      provider_reference: str | None = None

  @dataclass(frozen=True)
  class ProviderAttemptRequest:
      attempt_id: str
      direction: Direction
      amount: int
      currency: str
      card_type: CardType
      scenario: str | None = None

  @dataclass(frozen=True)
  class WebhookResolution:
      attempt_id: str
      outcome: AttemptOutcome

  class ProviderAdapter(Protocol):
      provider_id: str
      supports_status_check: bool
      async def submit(self, request: ProviderAttemptRequest) -> AttemptOutcome: ...
      async def check_status(self, provider_reference: str) -> AttemptOutcome: ...
      def parse_webhook(self, payload: dict) -> WebhookResolution: ...
  ```

- [ ] **Step 1: Write the failing test**

```python
def test_attempt_outcome_is_frozen_and_typed():
    outcome = AttemptOutcome(kind=OutcomeKind.DECLINED_FINAL, decline_code="insufficient_funds")
    assert outcome.kind == OutcomeKind.DECLINED_FINAL
    assert outcome.decline_code == "insufficient_funds"
    with pytest.raises(FrozenInstanceError):
        outcome.decline_code = "other"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/providers/test_base.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement the types above exactly as specified**

No algorithmic choice here — straight dataclasses/enums/Protocol as given.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/providers/test_base.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/enums.py app/providers/base.py tests/providers/test_base.py
git commit -m "feat: domain enums and provider outcome types"
```

---

### Task 3: SQLAlchemy models and initial migration

**Files:**
- Create: `app/models.py`, `alembic.ini`, `alembic/env.py`, `alembic/versions/0001_initial.py`
- Test: `tests/test_models.py`

**Interfaces:**
- Consumes: `app.db.Base`, enums from Task 2.
- Produces:
  ```python
  class Payment(Base):
      __tablename__ = "payments"
      id: Mapped[uuid.UUID]              # pk, default uuid4
      direction: Mapped[Direction]
      amount: Mapped[int]                # minor units
      currency: Mapped[str]
      card_bin: Mapped[str | None]
      card_type: Mapped[CardType]
      status: Mapped[PaymentStatus]      # default PENDING
      idempotency_key: Mapped[str]       # unique
      idempotency_payload_hash: Mapped[str]
      callback_url: Mapped[str]
      created_at: Mapped[datetime]
      updated_at: Mapped[datetime]

  class Provider(Base):
      __tablename__ = "providers"
      id: Mapped[str]                    # pk, e.g. "fast_sync"
      name: Mapped[str]
      priority: Mapped[int | None]
      supported_directions: Mapped[list[str]]   # JSON
      supported_card_types: Mapped[list[str]]   # JSON
      min_amount: Mapped[int]
      max_amount: Mapped[int]
      enabled: Mapped[bool]               # default True

  class ProviderAttempt(Base):
      __tablename__ = "provider_attempts"
      id: Mapped[uuid.UUID]               # pk
      payment_id: Mapped[uuid.UUID]        # FK payments.id
      provider_id: Mapped[str]             # FK providers.id
      status: Mapped[AttemptStatus]
      decline_code: Mapped[str | None]
      provider_reference: Mapped[str | None]
      requested_at: Mapped[datetime]
      resolved_at: Mapped[datetime | None]

  class ProviderStats(Base):
      __tablename__ = "provider_stats"
      provider_id: Mapped[str]             # pk part, FK providers.id
      direction: Mapped[Direction]         # pk part
      card_type: Mapped[CardType]          # pk part
      success_count: Mapped[int]           # default 0
      failure_count: Mapped[int]           # default 0
      updated_at: Mapped[datetime]

  class ReconciliationFlag(Base):
      __tablename__ = "reconciliation_flags"
      id: Mapped[uuid.UUID]                # pk
      payment_id: Mapped[uuid.UUID]        # FK payments.id
      reason: Mapped[str]
      attempt_ids: Mapped[list[str]]       # JSON
      created_at: Mapped[datetime]
      resolved_at: Mapped[datetime | None]
  ```
  `ProviderStats` primary key is the composite `(provider_id, direction, card_type)`.

- [ ] **Step 1: Write the failing test**

```python
async def test_can_persist_and_load_payment(db_session):
    payment = Payment(direction=Direction.DEPOSIT, amount=1000, currency="USD",
                       card_type=CardType.VISA, idempotency_key="k1",
                       idempotency_payload_hash="h1", callback_url="https://merchant.test/cb")
    db_session.add(payment)
    await db_session.commit()
    loaded = await db_session.get(Payment, payment.id)
    assert loaded.status == PaymentStatus.PENDING
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_models.py -v`
Expected: FAIL (`app.models` not found)

- [ ] **Step 3: Implement the models exactly as specified, and the initial Alembic migration**

Use `JSON` (not Postgres `ARRAY`) for list columns per Global Constraints, so the schema works on SQLite in tests and Postgres at runtime.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_models.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/models.py alembic.ini alembic/env.py alembic/versions/0001_initial.py tests/test_models.py
git commit -m "feat: SQLAlchemy models and initial migration"
```

---

### Task 4: `provider_fast_sync` adapter (HTTP-status-code convention)

**Files:**
- Create: `app/providers/fast_sync.py`
- Test: `tests/providers/test_fast_sync.py`

**Interfaces:**
- Consumes: `ProviderAdapter`, `ProviderAttemptRequest`, `AttemptOutcome`, `OutcomeKind` from Task 2.
- Produces: `FastSyncAdapter` implementing `ProviderAdapter`, `provider_id = "fast_sync"`, `supports_status_check = False`. Internally builds a raw `(status_code: int, body: dict)` response from `request.scenario` and normalizes it — this exercises the normalization responsibility stated in the design doc.
  - `scenario="success"` → raw `(200, {})` → `AttemptOutcome(SUCCESS)`
  - `scenario="insufficient_funds"` → raw `(402, {"code": "insufficient_funds"})` → `AttemptOutcome(DECLINED_FINAL, "insufficient_funds")`
  - `scenario="fraud_suspected"` → raw `(403, {"code": "fraud_suspected"})` → `AttemptOutcome(DECLINED_FINAL, "fraud_suspected")`
  - `scenario="processing_error"` → raw `(503, {"code": "processing_error"})` → `AttemptOutcome(DECLINED_RETRYABLE, "processing_error")`
  - unspecified `scenario` → weighted-random between the above (90% success) — used only outside tests.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.parametrize("scenario,kind,code", [
    ("success", OutcomeKind.SUCCESS, None),
    ("insufficient_funds", OutcomeKind.DECLINED_FINAL, "insufficient_funds"),
    ("fraud_suspected", OutcomeKind.DECLINED_FINAL, "fraud_suspected"),
    ("processing_error", OutcomeKind.DECLINED_RETRYABLE, "processing_error"),
])
async def test_fast_sync_normalizes_http_status_declines(scenario, kind, code):
    adapter = FastSyncAdapter()
    outcome = await adapter.submit(make_request(scenario=scenario))
    assert outcome.kind == kind
    assert outcome.decline_code == code
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/providers/test_fast_sync.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `FastSyncAdapter.submit` and a private `_raw_response`/`_normalize` pair**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/providers/test_fast_sync.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/providers/fast_sync.py tests/providers/test_fast_sync.py
git commit -m "feat: provider_fast_sync adapter"
```

---

### Task 5: `provider_webhook_async` adapter (200 + body convention, async resolution)

**Files:**
- Create: `app/providers/webhook_async.py`
- Test: `tests/providers/test_webhook_async.py`

**Interfaces:**
- Produces: `WebhookAsyncAdapter`, `provider_id = "webhook_async"`, `supports_status_check = False`.
  - `submit()` always returns raw `(200, {"status": "pending"})` → normalizes to `AttemptOutcome(AMBIGUOUS)`, regardless of `scenario` (the real result only ever arrives via webhook).
  - `parse_webhook(payload)`: payload is always HTTP `200`-shaped JSON like `{"attempt_id": "...", "status": "approved"}` or `{"attempt_id": "...", "status": "declined", "code": "fraud_suspected"}`; maps `"approved"` → `SUCCESS`, `"declined"` + code in `{"insufficient_funds","fraud_suspected"}` → `DECLINED_FINAL`, `"declined"` + code `"processing_error"` → `DECLINED_RETRYABLE`. Returns `WebhookResolution(attempt_id, outcome)`.

- [ ] **Step 1: Write the failing test**

```python
async def test_webhook_async_submit_is_always_ambiguous():
    adapter = WebhookAsyncAdapter()
    outcome = await adapter.submit(make_request(scenario="success"))
    assert outcome.kind == OutcomeKind.AMBIGUOUS

@pytest.mark.parametrize("payload,kind,code", [
    ({"attempt_id": "a1", "status": "approved"}, OutcomeKind.SUCCESS, None),
    ({"attempt_id": "a1", "status": "declined", "code": "fraud_suspected"}, OutcomeKind.DECLINED_FINAL, "fraud_suspected"),
    ({"attempt_id": "a1", "status": "declined", "code": "processing_error"}, OutcomeKind.DECLINED_RETRYABLE, "processing_error"),
])
def test_webhook_async_parse_webhook_normalizes_body_coded_result(payload, kind, code):
    adapter = WebhookAsyncAdapter()
    resolution = adapter.parse_webhook(payload)
    assert resolution.attempt_id == "a1"
    assert resolution.outcome.kind == kind
    assert resolution.outcome.decline_code == code
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/providers/test_webhook_async.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `WebhookAsyncAdapter.submit` and `.parse_webhook` per the mapping above**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/providers/test_webhook_async.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/providers/webhook_async.py tests/providers/test_webhook_async.py
git commit -m "feat: provider_webhook_async adapter"
```

---

### Task 6: `provider_flaky` adapter (mixed convention, timeouts, status-check)

**Files:**
- Create: `app/providers/flaky.py`
- Test: `tests/providers/test_flaky.py`

**Interfaces:**
- Produces: `FlakyAdapter`, `provider_id = "flaky"`, `supports_status_check = True`.
  - `scenario="timeout"` → `submit()` raises `asyncio.TimeoutError` (caller/cascade.py is responsible for catching this and treating it as `AMBIGUOUS` — see Task 12).
  - `scenario="http_5xx"` → raw `(502, {})` → `AttemptOutcome(AMBIGUOUS)`.
  - `scenario="declined_via_http"` → raw `(403, {"code": "fraud_suspected"})` → `AttemptOutcome(DECLINED_FINAL, "fraud_suspected")`.
  - `scenario="declined_via_body"` → raw `(200, {"status": "declined", "code": "processing_error"})` → `AttemptOutcome(DECLINED_RETRYABLE, "processing_error")`.
  - `check_status(provider_reference)`: looks up a module-level in-memory dict seeded by tests (`FlakyAdapter.STATUS_RESOLUTIONS: dict[str, AttemptOutcome]`) keyed by `provider_reference`; returns the stored outcome, or re-raises `asyncio.TimeoutError` if the reference isn't present yet (simulating "still not resolved").

- [ ] **Step 1: Write the failing test**

```python
async def test_flaky_timeout_scenario_raises():
    adapter = FlakyAdapter()
    with pytest.raises(asyncio.TimeoutError):
        await adapter.submit(make_request(scenario="timeout"))

async def test_flaky_normalizes_both_conventions():
    adapter = FlakyAdapter()
    http_outcome = await adapter.submit(make_request(scenario="declined_via_http"))
    body_outcome = await adapter.submit(make_request(scenario="declined_via_body"))
    assert http_outcome.kind == OutcomeKind.DECLINED_FINAL
    assert body_outcome.kind == OutcomeKind.DECLINED_RETRYABLE

async def test_flaky_check_status_returns_seeded_outcome():
    adapter = FlakyAdapter()
    adapter.STATUS_RESOLUTIONS["ref-1"] = AttemptOutcome(kind=OutcomeKind.SUCCESS)
    outcome = await adapter.check_status("ref-1")
    assert outcome.kind == OutcomeKind.SUCCESS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/providers/test_flaky.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `FlakyAdapter` per the mapping above**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/providers/test_flaky.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/providers/flaky.py tests/providers/test_flaky.py
git commit -m "feat: provider_flaky adapter"
```

---

### Task 7: Provider registry and eligibility filter

**Files:**
- Create: `app/providers/registry.py`
- Test: `tests/providers/test_registry.py`

**Interfaces:**
- Consumes: `Provider` model (Task 3), the three adapters (Tasks 4–6).
- Produces:
  ```python
  ADAPTERS: dict[str, ProviderAdapter] = {
      "fast_sync": FastSyncAdapter(),
      "webhook_async": WebhookAsyncAdapter(),
      "flaky": FlakyAdapter(),
  }
  def get_adapter(provider_id: str) -> ProviderAdapter: ...
  async def eligible_providers(session: AsyncSession, direction: Direction, card_type: CardType, amount: int) -> list[Provider]: ...
  ```
  `eligible_providers` returns enabled `Provider` rows where `direction.value in supported_directions`, `card_type.value in supported_card_types`, and `min_amount <= amount <= max_amount`.

- [ ] **Step 1: Write the failing test**

```python
async def test_eligible_providers_filters_by_direction_card_type_and_amount(db_session):
    await seed_provider(db_session, id="a", supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=100, max_amount=10000)
    await seed_provider(db_session, id="b", supported_directions=["withdraw"],
                         supported_card_types=["visa"], min_amount=100, max_amount=10000)
    result = await eligible_providers(db_session, Direction.DEPOSIT, CardType.VISA, 500)
    assert [p.id for p in result] == ["a"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/providers/test_registry.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `eligible_providers` and `get_adapter` as specified**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/providers/test_registry.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/providers/registry.py tests/providers/test_registry.py
git commit -m "feat: provider registry and eligibility filter"
```

---

### Task 8: Adaptive stats (Laplace score, record, correction)

**Files:**
- Create: `app/stats.py`
- Test: `tests/test_stats.py`

**Interfaces:**
- Consumes: `ProviderStats` model (Task 3).
- Produces:
  ```python
  def laplace_score(success_count: int, failure_count: int) -> float:
      return (success_count + 1) / (success_count + failure_count + 2)

  async def record_result(session: AsyncSession, provider_id: str, direction: Direction, card_type: CardType, success: bool) -> None: ...
  async def correct_result(session: AsyncSession, provider_id: str, direction: Direction, card_type: CardType, old_success: bool, new_success: bool) -> None: ...
  async def get_score(session: AsyncSession, provider_id: str, direction: Direction, card_type: CardType) -> float: ...
  ```
  `record_result` upserts the `(provider_id, direction, card_type)` row, incrementing `success_count` or `failure_count`. `correct_result` decrements the counter matching `old_success` and increments the counter matching `new_success` (no-op if `old_success == new_success`).

- [ ] **Step 1: Write the failing test**

```python
def test_laplace_score_smooths_small_samples():
    assert laplace_score(0, 0) == 0.5
    assert laplace_score(1, 0) == pytest.approx(2/3)

async def test_record_and_correct_result(db_session):
    await record_result(db_session, "p1", Direction.DEPOSIT, CardType.VISA, success=False)
    assert await get_score(db_session, "p1", Direction.DEPOSIT, CardType.VISA) == pytest.approx(1/3)
    await correct_result(db_session, "p1", Direction.DEPOSIT, CardType.VISA, old_success=False, new_success=True)
    assert await get_score(db_session, "p1", Direction.DEPOSIT, CardType.VISA) == pytest.approx(2/3)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_stats.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `laplace_score`, `record_result`, `correct_result`, `get_score`**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_stats.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/stats.py tests/test_stats.py
git commit -m "feat: adaptive provider stats with laplace smoothing"
```

---

### Task 9: Routing engine (manual priority first, then adaptive)

**Files:**
- Create: `app/routing.py`
- Test: `tests/test_routing.py`

**Interfaces:**
- Consumes: `Provider` model, `stats.get_score` (Task 8).
- Produces: `async def order_providers(session: AsyncSession, eligible: list[Provider], direction: Direction, card_type: CardType) -> list[Provider]`. Providers with `priority is not None` come first, sorted ascending by `priority`; providers with `priority is None` follow, sorted descending by `stats.get_score(...)`.

- [ ] **Step 1: Write the failing test**

```python
async def test_manual_priority_providers_always_rank_before_adaptive(db_session):
    p_low_priority = make_provider(id="x", priority=2)
    p_high_priority = make_provider(id="y", priority=1)
    p_adaptive_strong = make_provider(id="z", priority=None)
    await record_result(db_session, "z", Direction.DEPOSIT, CardType.VISA, success=True)
    ordered = await order_providers(db_session, [p_low_priority, p_high_priority, p_adaptive_strong],
                                     Direction.DEPOSIT, CardType.VISA)
    assert [p.id for p in ordered] == ["y", "x", "z"]

async def test_adaptive_only_providers_rank_by_score(db_session):
    p_weak = make_provider(id="weak", priority=None)
    p_strong = make_provider(id="strong", priority=None)
    await record_result(db_session, "strong", Direction.DEPOSIT, CardType.VISA, success=True)
    await record_result(db_session, "weak", Direction.DEPOSIT, CardType.VISA, success=False)
    ordered = await order_providers(db_session, [p_weak, p_strong], Direction.DEPOSIT, CardType.VISA)
    assert [p.id for p in ordered] == ["strong", "weak"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_routing.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `order_providers` per the split above**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_routing.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/routing.py tests/test_routing.py
git commit -m "feat: routing engine with manual-priority-then-adaptive ordering"
```

---

### Task 10: Webhook event bus and ambiguous-outcome reconciliation

**Files:**
- Create: `app/webhooks.py` (event bus only in this task — outgoing dispatch is Task 11), `app/reconciliation.py`
- Test: `tests/test_reconciliation.py`

**Interfaces:**
- Produces (`app/webhooks.py`):
  ```python
  class WebhookEventBus:
      def register_wait(self, attempt_id: str) -> asyncio.Future: ...
      def resolve(self, attempt_id: str, outcome: AttemptOutcome) -> bool:  # True if a waiter existed
          ...
  event_bus = WebhookEventBus()
  ```
- Produces (`app/reconciliation.py`):
  ```python
  async def resolve_ambiguous(attempt_id: str, provider_reference: str | None, adapter: ProviderAdapter, deadline_seconds: float) -> AttemptOutcome | None: ...
  ```
  If `adapter.supports_status_check`, poll `adapter.check_status(provider_reference)` every 0.1s (catching `asyncio.TimeoutError` as "not yet") until `deadline_seconds` elapses. Otherwise call `event_bus.register_wait(attempt_id)` and `await asyncio.wait_for(future, timeout=deadline_seconds)`. Returns `None` on timeout in either branch (never raises).

- [ ] **Step 1: Write the failing test**

```python
async def test_resolve_ambiguous_via_status_check_polling():
    adapter = FlakyAdapter()
    async def seed_after_delay():
        await asyncio.sleep(0.05)
        adapter.STATUS_RESOLUTIONS["ref-1"] = AttemptOutcome(kind=OutcomeKind.SUCCESS)
    asyncio.create_task(seed_after_delay())
    outcome = await resolve_ambiguous("attempt-1", "ref-1", adapter, deadline_seconds=1.0)
    assert outcome.kind == OutcomeKind.SUCCESS

async def test_resolve_ambiguous_via_webhook_wait():
    adapter = WebhookAsyncAdapter()
    async def deliver_after_delay():
        await asyncio.sleep(0.05)
        event_bus.resolve("attempt-2", AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code="processing_error"))
    asyncio.create_task(deliver_after_delay())
    outcome = await resolve_ambiguous("attempt-2", None, adapter, deadline_seconds=1.0)
    assert outcome.decline_code == "processing_error"

async def test_resolve_ambiguous_returns_none_on_timeout():
    adapter = WebhookAsyncAdapter()
    outcome = await resolve_ambiguous("attempt-3", None, adapter, deadline_seconds=0.05)
    assert outcome is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_reconciliation.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `WebhookEventBus` and `resolve_ambiguous`**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_reconciliation.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/webhooks.py app/reconciliation.py tests/test_reconciliation.py
git commit -m "feat: webhook event bus and ambiguous-outcome reconciliation"
```

---

### Task 11: Outgoing merchant webhook dispatch

**Files:**
- Modify: `app/webhooks.py` (add dispatch function)
- Test: `tests/test_merchant_webhook.py`

**Interfaces:**
- Consumes: `Payment` model, `Settings.merchant_webhook_secret`.
- Produces:
  ```python
  async def send_merchant_webhook(payment: Payment, event_type: str, extra: dict | None = None, http_client: httpx.AsyncClient | None = None) -> None: ...
  ```
  Builds `{"event": event_type, "payment_id": str(payment.id), "status": payment.status.value, **(extra or {})}`, signs the JSON body with HMAC-SHA256 using `merchant_webhook_secret`, sends as `X-Signature` header to `payment.callback_url`, retries on non-2xx with exponential backoff (3 attempts: 0.1s, 0.2s, 0.4s — demo-scaled, configurable later).

- [ ] **Step 1: Write the failing test**

```python
async def test_send_merchant_webhook_signs_and_posts(respx_mock):
    route = respx_mock.post("https://merchant.test/cb").mock(return_value=httpx.Response(200))
    payment = make_payment(callback_url="https://merchant.test/cb", status=PaymentStatus.SUCCEEDED)
    await send_merchant_webhook(payment, "payment.succeeded")
    assert route.called
    sent_body = json.loads(route.calls[0].request.content)
    assert sent_body["event"] == "payment.succeeded"
    assert "X-Signature" in route.calls[0].request.headers

async def test_send_merchant_webhook_retries_on_failure(respx_mock):
    route = respx_mock.post("https://merchant.test/cb").mock(
        side_effect=[httpx.Response(500), httpx.Response(200)])
    payment = make_payment(callback_url="https://merchant.test/cb", status=PaymentStatus.FAILED)
    await send_merchant_webhook(payment, "payment.failed")
    assert route.call_count == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_merchant_webhook.py -v`
Expected: FAIL (function not defined)

- [ ] **Step 3: Implement `send_merchant_webhook`**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_merchant_webhook.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/webhooks.py tests/test_merchant_webhook.py
git commit -m "feat: signed, retrying merchant webhook dispatch"
```

---

### Task 12: Cascade orchestrator and payment finalization

**Files:**
- Create: `app/cascade.py`
- Test: `tests/test_cascade.py`

**Interfaces:**
- Consumes: `eligible_providers`, `get_adapter` (Task 7), `order_providers` (Task 9), `record_result` (Task 8), `resolve_ambiguous` (Task 10), `send_merchant_webhook` (Task 11), `ProviderAttempt`/`Payment` models (Task 3), `Settings.max_cascade_attempts`, `Settings.reconciliation_deadline_seconds`.
- Produces:
  ```python
  async def run_cascade(session: AsyncSession, payment_id: str) -> None: ...
  async def finalize_success(session: AsyncSession, payment: Payment, attempt: ProviderAttempt) -> None: ...
  async def finalize_failed(session: AsyncSession, payment: Payment, attempts: list[ProviderAttempt]) -> None: ...
  ```
  `run_cascade` loads the payment, computes `order_providers(eligible_providers(...))`, and for each provider in order (capped at `max_cascade_attempts`): creates a `ProviderAttempt(status=PENDING)`, calls `adapter.submit(...)` catching `asyncio.TimeoutError` as `AttemptOutcome(AMBIGUOUS)`; if the outcome is `AMBIGUOUS`, calls `resolve_ambiguous(...)` — `None` means mark the attempt `TIMED_OUT`, record a failure in stats, and continue to the next provider; a resolved outcome is handled the same as an immediate one. `SUCCESS` → mark attempt `SUCCEEDED`, record success, call `finalize_success`, stop. `DECLINED_FINAL` → mark attempt `DECLINED_FINAL`, record failure, call `finalize_failed` **immediately** (no further providers tried), stop. `DECLINED_RETRYABLE` → mark attempt `DECLINED_RETRYABLE`, record failure, continue to next provider. If the provider list is exhausted without success, call `finalize_failed` with all attempts. `finalize_success`/`finalize_failed` set `payment.status`, commit, and call `send_merchant_webhook` with `event_type="payment.succeeded"` / `"payment.failed"`.

- [ ] **Step 1: Write the failing test**

```python
async def test_final_decline_stops_cascade_without_trying_next_provider(db_session, monkeypatch):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    await seed_provider(db_session, id="flaky", priority=2, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.DECLINED_FINAL, decline_code="insufficient_funds")))
    flaky_submit = AsyncMock()
    monkeypatch.setattr(FlakyAdapter, "submit", flaky_submit)

    await run_cascade(db_session, str(payment.id))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.FAILED
    flaky_submit.assert_not_called()

async def test_retryable_decline_cascades_to_next_provider(db_session, monkeypatch):
    # fast_sync -> DECLINED_RETRYABLE, flaky -> SUCCESS; assert payment.status == SUCCEEDED
    # and both providers have a ProviderAttempt row
    ...

async def test_ambiguous_timeout_is_recorded_as_timed_out_and_cascade_continues(db_session, monkeypatch):
    # fast_sync -> AMBIGUOUS, resolve_ambiguous returns None (patched), flaky -> SUCCESS
    # assert the fast_sync attempt row has status == TIMED_OUT and is still present after success
    ...

async def test_all_providers_exhausted_marks_payment_failed(db_session, monkeypatch):
    # both providers -> DECLINED_RETRYABLE; assert payment.status == FAILED, two attempts recorded
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_cascade.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `run_cascade`, `finalize_success`, `finalize_failed` per the state machine above**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_cascade.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/cascade.py tests/test_cascade.py
git commit -m "feat: cascade orchestrator with final-decline-stops, retryable-cascades, timeout-handling"
```

---

### Task 13: Payments API (create with idempotency, get status)

**Files:**
- Create: `app/idempotency.py`, `app/schemas.py`, `app/api/payments.py`
- Modify: `app/main.py` (mount router)
- Test: `tests/test_idempotency.py`, `tests/test_payments_api.py`

**Interfaces:**
- Consumes: `Payment`, `ProviderAttempt` models, `run_cascade` (Task 12).
- Produces (`app/idempotency.py`):
  ```python
  async def get_or_create_payment(session: AsyncSession, idempotency_key: str, payload_hash: str, build: Callable[[], Payment]) -> tuple[Payment, bool]: ...
  ```
  Raises `IdempotencyConflict` (new exception class, defined in `app/idempotency.py`) if an existing row with that key has a different `idempotency_payload_hash`. Keys never expire in this scope.
- Produces (`app/schemas.py`): `PaymentCreateRequest(direction, amount, currency, card_bin, card_type, callback_url)`, `PaymentResponse(id, direction, amount, currency, status, created_at)`, `PaymentDetailResponse(PaymentResponse fields + attempts: list[AttemptResponse])`, `AttemptResponse(provider_id, status, decline_code, requested_at, resolved_at)`.
- Produces (`app/api/payments.py`): `POST /payments` (reads `Idempotency-Key` header, required; on new payment, persists it, schedules `run_cascade` as a background asyncio task, returns `202` + `PaymentResponse`; on replay of same key+payload, returns `200` + the existing `PaymentResponse`; on key+different payload, returns `409`); `GET /payments/{id}` → `200` + `PaymentDetailResponse` or `404`.

- [ ] **Step 1: Write the failing test**

```python
async def test_same_idempotency_key_and_payload_returns_original(db_session):
    build = lambda: Payment(direction=Direction.DEPOSIT, amount=100, currency="USD",
                             card_type=CardType.VISA, idempotency_key="k", idempotency_payload_hash="h",
                             callback_url="https://x.test/cb")
    first, created_first = await get_or_create_payment(db_session, "k", "h", build)
    second, created_second = await get_or_create_payment(db_session, "k", "h", build)
    assert created_first is True
    assert created_second is False
    assert first.id == second.id

async def test_same_key_different_payload_raises_conflict(db_session):
    build = lambda: Payment(direction=Direction.DEPOSIT, amount=100, currency="USD",
                             card_type=CardType.VISA, idempotency_key="k", idempotency_payload_hash="h1",
                             callback_url="https://x.test/cb")
    await get_or_create_payment(db_session, "k", "h1", build)
    with pytest.raises(IdempotencyConflict):
        await get_or_create_payment(db_session, "k", "h2", build)
```

```python
async def test_post_payments_requires_idempotency_key_header(client):
    resp = await client.post("/payments", json=valid_payment_payload())
    assert resp.status_code == 422

async def test_post_payments_replay_returns_same_payment(client):
    headers = {"Idempotency-Key": "abc"}
    first = await client.post("/payments", json=valid_payment_payload(), headers=headers)
    second = await client.post("/payments", json=valid_payment_payload(), headers=headers)
    assert first.json()["id"] == second.json()["id"]
    assert second.status_code == 200

async def test_post_payments_same_key_different_body_returns_409(client):
    headers = {"Idempotency-Key": "abc2"}
    await client.post("/payments", json=valid_payment_payload(amount=100), headers=headers)
    resp = await client.post("/payments", json=valid_payment_payload(amount=200), headers=headers)
    assert resp.status_code == 409

async def test_get_payment_returns_attempts(client):
    headers = {"Idempotency-Key": "abc3"}
    created = await client.post("/payments", json=valid_payment_payload(), headers=headers)
    payment_id = created.json()["id"]
    resp = await client.get(f"/payments/{payment_id}")
    assert resp.status_code == 200
    assert "attempts" in resp.json()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_idempotency.py tests/test_payments_api.py -v`
Expected: FAIL (modules not found)

- [ ] **Step 3: Implement `get_or_create_payment`, the Pydantic schemas, and the two routes**

`payload_hash` is `sha256` of the canonical JSON request body, computed in the route handler before calling `get_or_create_payment`. Scheduling the cascade: `asyncio.create_task(run_cascade(session_factory(), str(payment.id)))` — use a fresh session from `app.db`'s session factory inside the task, not the request-scoped session, since the task outlives the request.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_idempotency.py tests/test_payments_api.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/idempotency.py app/schemas.py app/api/payments.py app/main.py tests/test_idempotency.py tests/test_payments_api.py
git commit -m "feat: payments API with idempotent creation and status lookup"
```

---

### Task 14: Incoming provider webhook endpoint, late resolution and conflict detection

**Files:**
- Modify: `app/cascade.py` (add `handle_late_resolution`), `app/main.py` (mount router)
- Create: `app/api/provider_webhooks.py`
- Test: `tests/test_provider_webhooks_api.py`

**Interfaces:**
- Consumes: `get_adapter` (Task 7), `event_bus.resolve` (Task 10), `correct_result` (Task 8), `send_merchant_webhook` (Task 11), `ReconciliationFlag` model (Task 3).
- Produces (`app/cascade.py`):
  ```python
  async def handle_late_resolution(session: AsyncSession, attempt: ProviderAttempt, outcome: AttemptOutcome) -> None: ...
  ```
  If `attempt.status` is still awaited (payment not yet finalized past this attempt) — this is the in-deadline path, already covered by `resolve_ambiguous`'s waiter; `handle_late_resolution` only runs for attempts the cascade already moved past (`status == TIMED_OUT`, `DECLINED_RETRYABLE`, or `DECLINED_FINAL`). If the new `outcome.kind` matches what's already recorded for the payment's finalized state (e.g. payment already `FAILED` and this is a late final/retryable decline confirmation) → no-op. If `outcome.kind == SUCCESS` and `payment.status == SUCCEEDED` via a *different* attempt → set `payment.status = CONFLICT`, create a `ReconciliationFlag(payment_id, reason="late_success_after_finalization", attempt_ids=[...])`, call `correct_result(..., old_success=False, new_success=True)` for this provider, and `send_merchant_webhook(payment, "payment.conflict_detected")`. If `outcome.kind == SUCCESS` and `payment.status == SUCCEEDED` via **this same** attempt (duplicate webhook delivery) → no-op.
- Produces (`app/api/provider_webhooks.py`): `POST /webhooks/providers/{provider_id}` — looks up the attempt via `adapter.parse_webhook(body)`, then: if `event_bus.resolve(attempt_id, outcome)` returns `True` (someone was waiting inside the reconciliation deadline), done — `resolve_ambiguous`'s caller handles the rest. If it returns `False` (no one was waiting — either already resolved, or deadline passed), load the `ProviderAttempt` and call `handle_late_resolution`. Always returns `200` (webhook receivers must not fail the delivery).

- [ ] **Step 1: Write the failing test**

```python
async def test_duplicate_webhook_matching_recorded_outcome_is_noop(db_session, monkeypatch):
    payment = await seed_succeeded_payment(db_session, via_provider="webhook_async")
    attempt = await get_succeeded_attempt(db_session, payment.id)
    send_webhook_spy = AsyncMock()
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", send_webhook_spy)

    await handle_late_resolution(db_session, attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED
    send_webhook_spy.assert_not_called()

async def test_late_success_on_different_attempt_after_finalization_is_conflict(db_session, monkeypatch):
    payment = await seed_succeeded_payment(db_session, via_provider="flaky")
    timed_out_attempt = await seed_timed_out_attempt(db_session, payment.id, provider_id="webhook_async")
    send_webhook_spy = AsyncMock()
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", send_webhook_spy)

    await handle_late_resolution(db_session, timed_out_attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.CONFLICT
    flags = await load_reconciliation_flags(db_session, payment.id)
    assert len(flags) == 1
    send_webhook_spy.assert_called_once_with(payment, "payment.conflict_detected")
```

```python
async def test_provider_webhook_endpoint_returns_200_even_for_unknown_attempt(client):
    resp = await client.post("/webhooks/providers/webhook_async",
                              json={"attempt_id": "does-not-exist", "status": "approved"})
    assert resp.status_code == 200
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_provider_webhooks_api.py -v`
Expected: FAIL (modules not found)

- [ ] **Step 3: Implement `handle_late_resolution` and the webhook route**

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_provider_webhooks_api.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/cascade.py app/api/provider_webhooks.py app/main.py tests/test_provider_webhooks_api.py
git commit -m "feat: provider webhook endpoint with late-resolution and conflict detection"
```

---

### Task 15: End-to-end corner-case suite and README

**Files:**
- Create: `tests/test_corner_cases.py`, `README.md`

**Interfaces:**
- Consumes: the full `POST /payments` / `GET /payments/{id}` / `POST /webhooks/providers/{id}` surface from Tasks 13–14, all three adapters, seeded `Provider` rows (one per adapter, via a `seed_providers` fixture added to `conftest.py` in this task).

- [ ] **Step 1: Write the failing end-to-end tests**, one per corner case listed in the design doc §5, each driving only the HTTP API (no internal imports beyond test helpers):

```python
async def test_e2e_manual_priority_overrides_adaptive_history(client, db_session): ...
async def test_e2e_final_decline_does_not_cascade(client, db_session): ...
async def test_e2e_retryable_decline_cascades_to_next_provider(client, db_session): ...
async def test_e2e_all_providers_exhausted_returns_failed(client, db_session): ...
async def test_e2e_duplicate_post_with_same_idempotency_key_is_noop(client): ...
async def test_e2e_duplicate_webhook_delivery_is_noop(client): ...
async def test_e2e_late_webhook_after_cascade_moved_on_flags_conflict(client, db_session): ...
async def test_e2e_adaptive_routing_prefers_provider_with_better_history(client, db_session): ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_corner_cases.py -v`
Expected: FAIL (fixtures/scenarios not wired yet)

- [ ] **Step 3: Add the `seed_providers` fixture and any scenario glue needed to drive each test purely through the API**

No new production code should be needed here — if a test can't be made to pass without changing `app/*`, that's a bug surfaced by this task, fix it in the owning module.

- [ ] **Step 4: Run the full suite**

Run: `pytest -v`
Expected: PASS (all tests from Tasks 1–15)

- [ ] **Step 5: Write `README.md`**

Cover: architecture summary + mermaid diagram of the cascade flow, how to run (`docker-compose up`, `alembic upgrade head`, `uvicorn app.main:app`), how to run tests, one curl example per corner case in `test_corner_cases.py`, and an explicit "Limitations / what I'd do differently with more time" section listing: no external task broker (asyncio in-process), no auth, no multi-tenancy, no automatic reversal on conflict — matching the design doc's Non-goals.

- [ ] **Step 6: Commit**

```bash
git add tests/test_corner_cases.py tests/conftest.py README.md
git commit -m "test: end-to-end corner-case suite, add README"
```
