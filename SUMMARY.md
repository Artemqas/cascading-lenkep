# SUMMARY: как система обрабатывает платёж — три сценария

Ниже — пошаговая трассировка реального кода для трёх конкретных запросов.
Формат: *что пришло* → *какая функция вызывается, с какими аргументами и
откуда они берутся* → ... до конца обработки.

Везде участвуют одни и те же модули:

- `app/api/payments.py` — HTTP-слой (`POST /payments`, `GET /payments/{id}`)
- `app/idempotency.py` — проверка `Idempotency-Key`
- `app/cascade.py` — оркестратор каскада (`run_cascade`, `finalize_success`, `finalize_failed`)
- `app/providers/registry.py` — реестр адаптеров провайдеров и фильтр совместимости
- `app/routing.py` — порядок обхода провайдеров
- `app/stats.py` — адаптивная статистика успешности
- `app/webhooks.py` — исходящий вебхук мерчанту

---

## Сценарий 1. Платёж создался и сразу завершился успехом (без каскада)

**Вход:** `POST /payments`, заголовок `Idempotency-Key: demo-1`, тело:

```json
{
  "direction": "deposit", "amount": 1000, "currency": "USD",
  "card_bin": "411111", "card_type": "visa",
  "callback_url": "https://merchant.test/cb",
  "provider_scenarios": {"fast_sync": "success"}
}
```

В таблице `providers` заведены все три провайдера (как в `scripts/seed_providers.py`):
`fast_sync` с `priority=1` (ручной приоритет), `webhook_async` и `flaky` —
без приоритета (`priority=None`, адаптивный выбор по умолчанию). Все три
подходят под `deposit`/`visa`/`1000` по диапазонам.

1. FastAPI валидирует тело запроса в `PaymentCreateRequest` (`app/schemas.py`) —
   проверяет `amount > 0`, `currency` на паттерн `^[A-Z]{3}$`, `callback_url` как
   настоящий URL — и вызывает `create_payment(request, response, idempotency_key, session)`
   (`app/api/payments.py:60`). `idempotency_key` берётся из заголовка `Idempotency-Key`
   (объявлен как обязательный параметр `Header(alias="Idempotency-Key")`), `session`
   приходит через DI-зависимость `get_session` (`app/db.py`).

2. Считается `payload_hash = sha256(json.dumps(request.model_dump(mode="json"), sort_keys=True))`
   (`payments.py:66-68`) — хэш канонического JSON тела запроса. Он нужен, чтобы
   отличить повтор того же запроса от повтора с другим телом под тем же ключом.

3. Определяется локальная функция `build()` (`payments.py:70-81`), которая при
   вызове создаёт объект `Payment(direction=request.direction, amount=request.amount,
   ..., idempotency_key=idempotency_key, idempotency_payload_hash=payload_hash,
   callback_url=str(request.callback_url), provider_scenarios=request.provider_scenarios)`
   — все аргументы берутся напрямую из уже провалидированного `request` и из
   вычисленных на шаге 2 `idempotency_key`/`payload_hash`.

4. Вызывается `get_or_create_payment(session, idempotency_key, payload_hash, build)`
   (`app/idempotency.py:19`). Внутри:
   - `_load_existing(session, idempotency_key)` делает `SELECT ... WHERE idempotency_key = :key`
     — строки нет (это первый запрос с этим ключом).
   - вызывается `payment = build()` (функция с шага 3), `session.add(payment)`,
     `await session.commit()` — платёж с `status=PENDING` (значение по умолчанию
     в модели `Payment`) попадает в БД.
   - возвращается `(payment, created=True)`.

5. Так как `created is True`, вызывается `_schedule_cascade(str(payment.id))`
   (`payments.py:89`, аргумент — `id` только что созданного платежа). Внутри:
   `task = asyncio.create_task(_run_cascade_background(payment_id))` — запускает
   фоновую корутину не дожидаясь её завершения; `task` кладётся в множество
   `_background_tasks`, чтобы его не собрал GC, и регистрируется
   `task.add_done_callback(_on_cascade_task_done)` — на случай необработанного
   исключения, оно будет залогировано, а не потеряно молча.

6. HTTP-ответ уходит клиенту немедленно: `202 Accepted` с телом
   `PaymentResponse(id=str(payment.id), direction, amount, currency, status="pending", created_at)`
   (`_to_payment_response`, `payments.py:43`). Дальше всё происходит в фоне,
   параллельно с тем, что клиент уже получил ответ.

7. Фоновая задача выполняет `_run_cascade_background(payment_id)` (`payments.py:54`):
   открывает **новую** сессию БД через `db_module.session_factory()` (не ту, что
   была у исходного HTTP-запроса — та уже закрылась) и вызывает
   `run_cascade(session, payment_id)` (`app/cascade.py:35`).

8. `run_cascade`: `payment = await session.get(Payment, uuid.UUID(payment_id))` —
   перезагружает платёж из БД по `id`, который passed как аргумент. Берёт
   `settings = get_settings()` (глобальные настройки — `max_cascade_attempts=10`,
   `reconciliation_deadline_seconds=10.0` и т.д., `app/config.py`). Ставит
   `payment.status = PROCESSING` и коммитит — теперь клиент, сделав `GET`, увидит
   не "pending", а "идёт обработка".

9. Вызывается `_run_cascade_loop(session, payment, settings, attempts=[])`
   (`cascade.py:44`). Внутри:
   - `candidates = await eligible_providers(session, payment.direction, payment.card_type, payment.amount)`
     (`app/providers/registry.py:22`) — `direction`, `card_type`, `amount` взяты
     прямо из объекта `payment`, загруженного на шаге 8. Функция делает
     `SELECT * FROM providers WHERE enabled`, затем в Python отфильтровывает те,
     у кого `direction.value` входит в `supported_directions`, `card_type.value` —
     в `supported_card_types`, и `min_amount <= amount <= max_amount`. Для нашего
     запроса (`deposit`, `visa`, `1000`) все три провайдера проходят фильтр →
     `candidates = [fast_sync, webhook_async, flaky]` (в каком-то порядке — фильтр
     порядок не гарантирует).
   - `ordered = await order_providers(session, candidates, payment.direction, payment.card_type)`
     (`app/routing.py:8`) — сначала идут провайдеры с выставленным `priority`,
     отсортированные по его значению (у нас такой только `fast_sync`, `priority=1`),
     затем — все остальные (`webhook_async`, `flaky`), отсортированные по
     адаптивному score. Итог: `ordered = [fast_sync, webhook_async, flaky]`
     — `fast_sync` первый просто потому, что у него явный приоритет, а не
     потому, что остальных отфильтровали.

10. Цикл `for provider in ordered` — **первая итерация**, `provider = fast_sync`
    (до `webhook_async` и `flaky` дело дойдёт только если `fast_sync` не
    завершит платёж сам — см. ниже, почему в этом сценарии так и не доходит):
    - `adapter = get_adapter(provider.id)` (`registry.py:18`) — `provider.id` =
      `"fast_sync"`, функция просто берёт `ADAPTERS["fast_sync"]` — заранее
      созданный экземпляр `FastSyncAdapter()`.
    - Создаётся `attempt = ProviderAttempt(id=uuid4(), payment_id=payment.id,
      provider_id="fast_sync", status=PENDING, requested_at=now())`, кладётся в
      БД (`session.add` + `commit`) и добавляется в список `attempts`.
    - `request = _build_request(attempt, payment)` (`cascade.py:23`) — собирает
      `ProviderAttemptRequest(attempt_id=str(attempt.id), direction=payment.direction,
      amount=payment.amount, currency=payment.currency, card_type=payment.card_type,
      scenario=payment.provider_scenarios.get("fast_sync"))`. Поле `scenario`
      берётся из словаря `provider_scenarios`, который пришёл в исходном JSON
      запроса (шаг 1) и был сохранён в `Payment.provider_scenarios` на шаге 4 —
      для `"fast_sync"` это строка `"success"`.
    - `outcome = await adapter.submit(request)` (`app/providers/fast_sync.py:17`).
      Внутри `FastSyncAdapter.submit`: `scenario = request.scenario or self._random_scenario()`
      — так как `scenario="success"` явно задан, случайный путь не используется.
      `_raw_response("success")` (`fast_sync.py:27`) отдаёт сырой ответ `(200, {})`
      — это имитация настоящего HTTP-ответа провайдера. `_normalize(200, {})`
      (`fast_sync.py:30`) видит `status_code == 200` и возвращает
      `AttemptOutcome(kind=SUCCESS)`.

11. `outcome.kind == SUCCESS` → `attempt.status = SUCCEEDED`;
    `await record_result(session, "fast_sync", payment.direction, payment.card_type, success=True)`
    (`app/stats.py:30`) — находит или создаёt строку `ProviderStats` по составному
    ключу `(provider_id, direction, card_type)` и увеличивает `success_count`
    на 1 — это та самая адаптивная статистика, по которой выбираются провайдеры
    без ручного приоритета. Коммит попытки.

12. Вызывается `finalize_success(session, payment, attempt)` (`cascade.py:119`):
    `await session.refresh(payment)` — перечитывает платёж из БД (защита от
    гонки с "поздним" конфликтом, см. сценарий ниже); статус не `CONFLICT`,
    значит ставим `payment.status = SUCCEEDED`, коммитим.
    `await send_merchant_webhook(payment, "payment.succeeded")` (`app/webhooks.py:35`):
    собирает тело `{"event": "payment.succeeded", "payment_id": str(payment.id),
    "status": "succeeded"}`, подписывает HMAC-SHA256 секретом
    `settings.merchant_webhook_secret`, шлёт `POST` на `payment.callback_url`
    (тот самый `"https://merchant.test/cb"` из исходного запроса) с заголовком
    `X-Signature`. При не-2xx ответе — до 3 повторов с задержками 0.1/0.2/0.4с.

    Сразу после `finalize_success` в коде стоит `return` (`cascade.py:103`) —
    функция `_run_cascade_loop` завершается, не доходя до второй и третьей
    итерации `for provider in ordered`. Именно поэтому `webhook_async` и `flaky`,
    хотя и присутствовали в `ordered`, ни разу не вызываются: цикл не "знает"
    заранее, что первый провайдер окажется успешным — он просто выходит, как
    только это становится известно.

13. Клиент делает `GET /payments/{payment.id}` → `get_payment(payment_id, session)`
    (`payments.py:96`): загружает `Payment` по `id` из URL, подгружает все
    `ProviderAttempt` с этим `payment_id`, возвращает `PaymentDetailResponse`:
    `status: "succeeded"`, `attempts: [{provider_id: "fast_sync", status: "succeeded", decline_code: null, ...}]`.

**Итог:** один провайдер, один успешный вызов, каскад не потребовался.

---

## Сценарий 2. Каскад триггернулся из-за ошибки и завершился успехом

**Вход:** то же тело, но `provider_scenarios: {"fast_sync": "processing_error"}`,
`flaky` не указан. В таблице `providers` всё те же три провайдера, но теперь
`fast_sync` — `priority=1`, `flaky` — `priority=2` (у обоих выставлен ручной
приоритет), `webhook_async` — без приоритета, как и раньше.

Шаги 1–9 идентичны сценарию 1, с двумя отличиями:

- на шаге 9 `ordered = [fast_sync, flaky, webhook_async]` — сначала оба
  провайдера с ручным `priority` (в порядке 1, 2), затем всё остальное
  адаптивным порядком; `webhook_async` в этом сценарии так и не будет вызван —
  до него просто не дойдёт очередь, см. ниже;
  идут в порядке `priority` (1, затем 2), в обход адаптивной статистики;
- `payment.provider_scenarios = {"fast_sync": "processing_error"}`.

10. **Первая итерация цикла (provider = fast_sync).** Как и раньше, создаётся
    `attempt_1`, собирается `request` со `scenario = "processing_error"`
    (взят из `provider_scenarios["fast_sync"]`). Вызывается `adapter.submit(request)`:
    `_raw_response("processing_error")` отдаёт `(503, {"code": "processing_error"})`
    — имитация 503 от провайдера. `_normalize(503, {...})` (`fast_sync.py:30-35`):
    код не `200`, не в `(402, 403)` → возвращает
    `AttemptOutcome(kind=DECLINED_RETRYABLE, decline_code="processing_error")`.

11. `outcome.kind == DECLINED_RETRYABLE` (ветка `else` в `cascade.py:110-114`):
    `attempt_1.status = DECLINED_RETRYABLE`; `decline_code = "processing_error"`;
    `await record_result(session, "fast_sync", deposit, visa, success=False)` —
    у `fast_sync` в статистике `failure_count += 1`. Коммит, `continue` — цикл
    переходит к следующему провайдеру **без финализации платежа**: это и есть
    момент "каскадирования" — ключевое правило (`cascade.py`), что retryable-отказ
    не останавливает весь каскад, в отличие от финального отказа.

12. **Вторая итерация (provider = flaky).** Создаётся `attempt_2`. Собирается
    `request` со `scenario = payment.provider_scenarios.get("flaky")` — в словаре
    такого ключа нет, значит `scenario = None`. `adapter = get_adapter("flaky")`
    → экземпляр `FlakyAdapter`. `outcome = await adapter.submit(request)`
    (`app/providers/flaky.py`): `scenario is None`, это не `"timeout"`, значит
    идём в `_raw_response(None)` — ни один из явных кейсов (`http_5xx`,
    `declined_via_http`, `declined_via_body`) не совпал → возвращается
    дефолтный `(200, {})`. `_normalize(200, {})`: `status_code < 500`, не в
    `(402,403)`, `body.get("status") != "declined"` → `AttemptOutcome(kind=SUCCESS)`.

13. `outcome.kind == SUCCESS` → `attempt_2.status = SUCCEEDED`;
    `record_result(session, "flaky", deposit, visa, success=True)`; коммит;
    `finalize_success(session, payment, attempt_2)` — платёж получает
    `status = SUCCEEDED`, отправляется вебхук `payment.succeeded` мерчанту
    (как в шаге 12 сценария 1). Функция `run_cascade` завершается через `return`
    сразу после `finalize_success` — до `flaky` в этом примере других провайдеров
    в `ordered` и не было.

14. `GET /payments/{id}` вернёт `status: "succeeded"` и **два** объекта в
    `attempts`: `{provider_id: "fast_sync", status: "declined_retryable",
    decline_code: "processing_error"}` и `{provider_id: "flaky", status: "succeeded",
    decline_code: null}` — полная история попыток сохранена, ничего не стёрто.

**Итог:** первый провайдер вернул устранимую ошибку → каскад перешёл ко
второму → тот отработал успешно → платёж завершён успехом.

---

## Сценарий 3. Каскад триггернулся, но успехом не завершился

**Вход:** то же тело, `provider_scenarios: {"fast_sync": "processing_error",
"flaky": "declined_via_body"}`. В таблице `providers` на этот раз доступны
только `fast_sync` (`priority=1`) и `flaky` (`priority=2`) — третий провайдер
`webhook_async` для этого примера выключен (`enabled=False`), чтобы каскад
не растягивался на провайдера, который отвечает асинхронно через вебхук.

Шаги 1–9 как в сценарии 1/2; `ordered = [fast_sync, flaky]`.

10. **Первая итерация (fast_sync).** Как в сценарии 2, шаг 10: `scenario =
    "processing_error"` → `outcome = AttemptOutcome(DECLINED_RETRYABLE,
    "processing_error")`. `attempt_1.status = DECLINED_RETRYABLE`,
    `record_result(..., success=False)`, `continue`.

11. **Вторая итерация (flaky).** `request.scenario = payment.provider_scenarios.get("flaky")
    = "declined_via_body"`. `adapter.submit(request)` (`app/providers/flaky.py`):
    `_raw_response("declined_via_body")` отдаёт `(200, {"status": "declined",
    "code": "processing_error"})` — провайдер имитирует отказ не HTTP-кодом, а
    полем в теле ответа при коде `200` (ровно та "разная конвенция ответа",
    из-за которой и появился протокол `AttemptOutcome` — нормализация целиком
    внутри адаптера). `_normalize(200, {...})`: код `200`, не `402/403`, но
    `body.get("status") == "declined"` → код `"processing_error"` не входит в
    `_FINAL_CODES = {"insufficient_funds", "fraud_suspected"}` → возвращается
    `AttemptOutcome(kind=DECLINED_RETRYABLE, decline_code="processing_error")`.

12. `outcome.kind == DECLINED_RETRYABLE` → `attempt_2.status = DECLINED_RETRYABLE`;
    `record_result(session, "flaky", deposit, visa, success=False)`; коммит;
    `continue`. Это была последняя итерация — `ordered` исчерпан.

13. Цикл `for provider in ordered` заканчивается без `return` внутри →
    выполняется строка после цикла: `await finalize_failed(session, payment, attempts)`
    (`cascade.py:116`, `attempts` — список из двух объектов `ProviderAttempt`,
    накопленный на шагах 10 и 11). Внутри `finalize_failed` (`cascade.py:131`):
    `await session.refresh(payment)` — статус не `CONFLICT`; `payment.status = FAILED`,
    коммит. `reasons = [a.decline_code for a in attempts if a.decline_code]`
    → `["processing_error", "processing_error"]`. `await send_merchant_webhook(
    payment, "payment.failed", extra={"reasons": reasons})` — мерчанту уходит
    вебхук `event: "payment.failed"` с перечнем причин отказа по каждой попытке.

14. `GET /payments/{id}` вернёт `status: "failed"` и оба объекта в `attempts`
    со статусом `"declined_retryable"` и `decline_code: "processing_error"` —
    видно, что система честно попробовала обоих доступных провайдеров и
    только после этого сдалась, вместо того чтобы остановиться после первого
    отказа.

**Итог:** оба доступных провайдера вернули устранимую ошибку, каскад
перепробовал всех — список провайдеров исчерпан → платёж завершён отказом
(`failed`), а не "висит" и не притворяется успешным.

---

### Для сравнения: когда каскад *не* продолжается после ошибки

Если бы на шаге 10 любого сценария `fast_sync` получил `provider_scenarios:
{"fast_sync": "fraud_suspected"}`, то `_normalize` вернул бы
`AttemptOutcome(kind=DECLINED_FINAL, decline_code="fraud_suspected")` —
и вместо `continue` сработала бы ветка `elif outcome.kind == DECLINED_FINAL`
(`cascade.py:104-109`): сразу `finalize_failed(session, payment, attempts)`
с одной-единственной попыткой и немедленный `return` — каскад даже не
пытается спросить `flaky`, потому что "недостаточно средств"/"подозрение
в мошенничестве" провалятся у любого провайдера одинаково.
