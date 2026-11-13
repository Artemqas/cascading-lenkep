# WEBHOOK_ASYNC_FLOW: как система ждёт и обрабатывает вебхук от `webhook_async`

Этот файл — то же самое, что `SUMMARY.md`, но детально про один конкретный
механизм: асинхронное подтверждение через вебхук (провайдер `webhook_async`).
Формат тот же: *что происходит* → *какая функция вызывается, с какими
аргументами и откуда они берутся* → ...

Участвующие файлы:

- `app/providers/webhook_async.py` — адаптер провайдера
- `app/cascade.py` — оркестратор каскада (`run_cascade`, `handle_late_resolution`)
- `app/reconciliation.py` — механизм ожидания/опроса (`resolve_ambiguous`)
- `app/webhooks.py` — шина событий `WebhookEventBus` + исходящий вебхук мерчанту
- `app/api/provider_webhooks.py` — HTTP-эндпоинт, на который стучится провайдер
- `app/api/payments.py` — создание платежа, запуск каскада в фоне

В таблице `providers` на всех сценариях: `webhook_async` с `priority=1`
(идёт первым гарантированно).

---

## Сценарий A. Вебхук приходит вовремя, `status: "approved"`

**Вход:** `POST /payments`, `direction=deposit, amount=1000, currency=USD,
card_type=visa, callback_url=https://merchant.test/cb`. Провайдер `webhook_async`
не нуждается в `provider_scenarios` — он игнорирует `scenario` в принципе.

1. `create_payment(...)` (`app/api/payments.py:60`) создаёт `Payment(status=PENDING)`
   через `get_or_create_payment` (как в `SUMMARY.md`), вызывает
   `_schedule_cascade(str(payment.id))` (`payments.py:89`) и **сразу** отдаёт
   клиенту `202` — дальше всё происходит в фоне, пока клиент уже получил ответ.

2. Фоновая задача: `_run_cascade_background(payment_id)` (`payments.py:54`)
   открывает новую сессию и зовёт `run_cascade(session, payment_id)`
   (`cascade.py:35`). Внутри — `payment.status = PROCESSING`, коммит, затем
   `_run_cascade_loop(...)` (`cascade.py:44`).

3. `eligible_providers(...)` + `order_providers(...)` (`cascade.py:57-58`)
   возвращают `ordered = [webhook_async, ...]` — он первый, так как у него
   `priority=1`.

4. **Первая (и единственная в этом сценарии) итерация цикла**, `provider =
   webhook_async`:
   - `adapter = get_adapter("webhook_async")` (`cascade.py:62`) — берёт
     экземпляр `WebhookAsyncAdapter()` из реестра.
   - Создаётся `attempt = ProviderAttempt(id=uuid4(), provider_id="webhook_async",
     status=PENDING, ...)` (`cascade.py:63-69`), коммитится, кладётся в
     `attempts`.
   - `request = _build_request(attempt, payment)` (`cascade.py:23`, вызов на
     строке 74) — собирает `ProviderAttemptRequest(attempt_id=str(attempt.id),
     ...)`. `attempt_id` отсюда — это **ключ**, по которому вебхук потом найдёт
     эту конкретную попытку.
   - `outcome = await adapter.submit(request)` (`cascade.py:76` →
     `webhook_async.py:10-12`): `submit()` **не смотрит** ни на `request`, ни
     на `scenario` — просто всегда возвращает `AttemptOutcome(kind=AMBIGUOUS)`.
     Это имитация того, что провайдер принял запрос, но результат узнает позже.

5. `outcome.kind == AMBIGUOUS` (`cascade.py:80`): `attempt.status = AMBIGUOUS`,
   коммит (строки 81-82). Теперь `GET /payments/{id}` показал бы этот attempt
   как `"ambiguous"`, а платёж — как `"processing"`.

6. Вызывается `resolved = await resolve_ambiguous(str(attempt.id),
   outcome.provider_reference, adapter, settings.reconciliation_deadline_seconds)`
   (`cascade.py:83-85`). Аргументы:
   - `str(attempt.id)` — тот же id попытки, что положили в `request.attempt_id`
     на шаге 4;
   - `outcome.provider_reference` — у `webhook_async` оно всегда `None`
     (`submit()` его не выставляет);
   - `adapter` — тот же `WebhookAsyncAdapter()`;
   - `settings.reconciliation_deadline_seconds` — глобальный дефолт `10.0`
     (`app/config.py`).

7. Внутри `resolve_ambiguous` (`app/reconciliation.py:7-12`): проверяется
   `adapter.supports_status_check` — у `webhook_async` это `False` → идём в
   `_wait_for_webhook(attempt_id, deadline_seconds)` (`reconciliation.py:29-34`),
   **не** в `_poll_status`.

8. `_wait_for_webhook`:
   - `future = event_bus.register_wait(attempt_id)` (`reconciliation.py:30` →
     `app/webhooks.py:22-25`) — создаёт пустой `asyncio.Future()` и кладёт его
     в словарь `event_bus._waiters[attempt_id] = future`. `event_bus` — это
     один общий на весь процесс singleton (`webhooks.py:35`, создаётся при
     импорте модуля).
   - `return await asyncio.wait_for(future, timeout=deadline_seconds)`
     (`reconciliation.py:32`) — **вот здесь корутина реально "засыпает"**.
     `run_cascade` стоит на этой строке и не продолжится, пока `future` не
     получит результат (или не пройдёт 10 секунд).

   На этом моменте весь фоновый `asyncio.Task` из шага 2 просто ждёт — других
   действий не происходит, пока не случится шаг 9.

9. **Где-то снаружи**, в любой момент до истечения дедлайна, реальный провайдер
   (или в нашем случае — тестовый клиент/curl) делает:

   ```
   POST /webhooks/providers/webhook_async
   {"attempt_id": "<id из шага 4>", "status": "approved"}
   ```

10. Роут `receive_provider_webhook(provider_id, payload, session)`
    (`app/api/provider_webhooks.py:16-45`):
    - `adapter = get_adapter("webhook_async")` (строка 24) — `provider_id`
      взят из URL.
    - `resolution = adapter.parse_webhook(payload)` (строка 29 →
      `webhook_async.py:17-26`): `payload["attempt_id"]` и `payload["status"]`
      читаются из тела запроса (шаг 9). `status == "approved"` →
      `outcome = AttemptOutcome(kind=SUCCESS)`. Возвращается
      `WebhookResolution(attempt_id=payload["attempt_id"], outcome=outcome)`.
    - `event_bus.resolve(resolution.attempt_id, resolution.outcome)`
      (строка 33 → `webhooks.py:27-32`): `self._waiters.pop(attempt_id, None)`
      находит **тот самый** `future` из шага 8 (ключ совпадает — это тот же
      `attempt_id`, пришедший в вебхуке и положенный туда на шаге 4).
      `future.set_result(outcome)` — вот это и "пробуждает" `await asyncio.wait_for`
      из шага 8. `resolve()` возвращает `True`.
    - Так как `resolve()` вернул `True`, роут сразу отдаёт `{"status": "ok"}`
      (строка 34) — дальнейшая обработка (финализация платежа) происходит не
      здесь, а в том фоновом `asyncio.Task`, который только что проснулся.

11. Назад в `_wait_for_webhook` (шаг 8): `asyncio.wait_for` возвращает
    `outcome = AttemptOutcome(kind=SUCCESS)` — без `TimeoutError`.
    `resolve_ambiguous` отдаёт этот `outcome` обратно в `run_cascade`.

12. В `cascade.py:86`: `resolved is not None` → `outcome = resolved` (строка 92).
    Дальше — обычная ветка `outcome.kind == SUCCESS` (строка 98):
    `attempt.status = SUCCEEDED`, `record_result(session, "webhook_async",
    deposit, visa, success=True)` (`app/stats.py:30`), коммит,
    `finalize_success(session, payment, attempt)` (строка 102).

13. `finalize_success` (`cascade.py:119-128`): `session.refresh(payment)` —
    статус ещё не `CONFLICT`; `payment.status = SUCCEEDED`, коммит;
    `send_merchant_webhook(payment, "payment.succeeded")` — отдельный, не
    связанный с `event_bus` исходящий HTTP-вызов мерчанту (см. `SUMMARY.md`).

14. `GET /payments/{id}` → `status: "succeeded"`, один attempt:
    `{provider_id: "webhook_async", status: "succeeded"}`.

---

## Сценарий B. Вебхук приходит вовремя, но с отказом

Шаги 1-9 как в сценарии A, только тело вебхука другое:

```
POST /webhooks/providers/webhook_async
{"attempt_id": "<id>", "status": "declined", "code": "processing_error"}
```

10. `parse_webhook(payload)` (`webhook_async.py:17-26`): `status != "approved"`
    → идём в `else`: `code = payload["code"] = "processing_error"`.
    `kind = DECLINED_FINAL if code in _FINAL_CODES else DECLINED_RETRYABLE`
    (строка 24) — `_FINAL_CODES = {"insufficient_funds", "fraud_suspected"}`
    (строка 3), `"processing_error"` туда не входит → `kind = DECLINED_RETRYABLE`.
    Если бы `code` был `"fraud_suspected"` — получили бы `DECLINED_FINAL`.

11. `event_bus.resolve(...)` пробуждает тот же `future`, как в шаге 10 сценария A.

12. `run_cascade` получает `outcome.kind == DECLINED_RETRYABLE` — ветка
    `else` (`cascade.py:110-114`): `attempt.status = DECLINED_RETRYABLE`,
    `record_result(..., success=False)`, `continue` — каскад идёт к
    **следующему** провайдеру в `ordered` (если есть). Если бы код был
    `"fraud_suspected"` → `DECLINED_FINAL` (`cascade.py:104-109`) — немедленный
    `finalize_failed` и `return`, без попыток следующего провайдера.

---

## Сценарий C. Вебхук не приходит вовремя — таймаут → каскад останавливается

Шаги 1-8 как в сценарии A. Дальше **никто не вызывает** `POST /webhooks/providers/webhook_async`.

9. Через `deadline_seconds` (10с по умолчанию) `asyncio.wait_for(future, timeout=10.0)`
   (`reconciliation.py:32`) сам кидает `asyncio.TimeoutError`, пойманный тут же
   (строка 33-34): `return None`.

10. `resolve_ambiguous` отдаёт `None`. В `cascade.py:89`: `resolved is None` —
    и вот здесь ключевое отличие от более раннего поведения этой системы:
    раньше отсюда шли дальше по каскаду, **считая неопределённость провалом**.
    Это неверно — провайдер ничего не подтвердил и не отклонил, то есть
    попытка вполне могла реально пройти. Пойти дальше означало бы предложить
    списание **второму** провайдеру, не зная, не списал ли уже первый — именно
    так получается задвоение. Поэтому сейчас: `attempt.status = TIMED_OUT`,
    коммит, `await mark_review_required(session, payment)` (строка 99),
    `return` — **без** `record_result(success=False)` (мы не знаем исхода,
    значит не портим адаптивную статистику provider'а угадыванием), и **без**
    перехода к `fast_sync`.

11. `mark_review_required` (`cascade.py:172-183`) через тот же оптимистичный
    retry-хелпер (`_retry_on_stale_payment`, см. ниже) ставит
    `payment.status = REVIEW_REQUIRED` и пишет `logger.warning(...)` — платёж
    дальше не трогается автоматически никем. `GET /payments/{id}` в этот
    момент покажет `status: "review_required"` и **один** attempt
    (`webhook_async`, `status: "timed_out"`) — `fast_sync` так и не был вызван.

Важный побочный эффект (как и раньше): `future`, который лежал в
`event_bus._waiters[attempt_id]`, не удаляется автоматически при таймауте.
Это уже не влияет на то, продолжится ли каскад (он не продолжится в любом
случае), но делает возможным следующий сценарий — поздний вебхук всё ещё может
прийти и должен быть корректно обработан.

---

## Сценарий D. "Поздний" вебхук — приходит после того, как платёж уже ушёл в ручную проверку

Продолжение сценария C: `webhook_async` протаймаутился, платёж —
`status=REVIEW_REQUIRED`, `fast_sync` ни разу не вызывался. Теперь, с
опозданием, реальный провайдер всё-таки присылает вебхук по **старому**
`attempt_id`:

```
POST /webhooks/providers/webhook_async
{"attempt_id": "<тот же id из сценария C>", "status": "approved"}
```

10. `receive_provider_webhook` (`provider_webhooks.py:16`): `parse_webhook(payload)`
    снова даёт `AttemptOutcome(kind=SUCCESS)`.

11. `event_bus.resolve(attempt_id, outcome)` (строка 33 → `webhooks.py:27-32`):
    `self._waiters.pop(attempt_id, None)` не находит живого `future` (он уже
    был отменён таймаутом в сценарии C) → `resolve()` возвращает `False`.

12. Роут идёт дальше: `attempt = await session.get(ProviderAttempt, attempt_id)`
    (строка 41, `status = TIMED_OUT` из сценария C),
    `await handle_late_resolution(session, attempt, resolution.outcome)` (строка 43).

13. `handle_late_resolution` (`cascade.py:186-253`):
    - `attempt.status == SUCCEEDED`? Нет (`TIMED_OUT`) → не выходим на строке 190.
    - `payment = await session.get(Payment, attempt.payment_id)`, `session.refresh(payment)`
      (строки 192-193) — читает актуальный статус, `REVIEW_REQUIRED`.
    - Ключевая проверка (строки 195-198): `attempt.status` в
      `{AMBIGUOUS, TIMED_OUT}` **и** `payment.status` в `{PROCESSING, REVIEW_REQUIRED}` —
      оба условия выполняются → это именно та попытка, на которой платёж
      остановился и ждёт человека, а не новое противоречие.
    - Пишется `attempt.decline_code`, `attempt.provider_reference`,
      `attempt.resolved_at` (строки 204-206) — то, что реально ответил
      провайдер, остаётся в истории попытки. `logger.info(...)` (строка 207) —
      это и есть лог, по которому тех.поддержка увидит, что провайдер всё-таки
      ответил, и дальше вручную сверится с провайдером и проставит финальный
      статус платежа сама, если это нужно.
    - `await session.commit()`, `return` (строки 211-212). **`payment.status`
      не трогается** — не `succeeded`, не `conflict`, остаётся `review_required`.
      Никакой вебхук мерчанту не уходит.

14. `GET /payments/{id}` → `status: "review_required"` — как и до вебхука.
    В `attempts` виден один attempt `webhook_async` с `status: "timed_out"`,
    но уже с заполненным `decline_code`/`provider_reference` из пришедшего
    вебхука — информация не потеряна, просто ничего не решено автоматически.

### Когда это всё-таки `conflict`, а не просто лог

Если бы вместо `fast_sync` каскад **уже успешно завершился через другого
провайдера раньше, чем webhook_async протаймаутился** (т.е. платёж уже
`SUCCEEDED`/`FAILED`, а не `REVIEW_REQUIRED`) — условие на строках 195-198 не
выполнилось бы (`payment.status` не в `{PROCESSING, REVIEW_REQUIRED}`), и код
ушёл бы в старую ветку ниже (строки 219-253): `payment.status = CONFLICT` +
`ReconciliationFlag` + `payment.conflict_detected`. Это теперь куда более узкий
случай, чем раньше — он требует, чтобы противоречие пришло по **другому**
attempt'у, не тому, из-за которого остановился сам каскад (см.
`tests/test_provider_webhooks_api.py::test_late_success_on_different_attempt_after_finalization_is_conflict`).

### Гонка между каскадом и вебхуком за одну и ту же строку `Payment`

`mark_review_required` и ветка `CONFLICT` в `handle_late_resolution` обе пишут
`payment.status` из **разных** сессий (фоновая задача каскада vs. HTTP-запрос
вебхука) — без защиты одна могла бы тихо затереть решение другой. Обе идут
через `_retry_on_stale_payment` (`cascade.py:128-143`): читают `payment`
заново, решают, что писать, и коммитят через `Payment.version`
(`version_id_col`, `app/models.py`) — если версия строки успела измениться
между чтением и записью, `StaleDataError` летит наружу, перехватывается, и
весь цикл "перечитать → решить → записать" повторяется (до 3 раз) с уже
актуальными данными, а не вслепую.

---

### Куда смотреть, если непонятно "кто кого ждёт"

```
cascade.py: await resolve_ambiguous(...)          — здесь ЖДЁМ
      │
      ▼
reconciliation.py: await asyncio.wait_for(future, timeout=...)   — здесь СПИМ на future
      │
      ▼ (future создан в event_bus._waiters[attempt_id])
      │
      │   ...время идёт, HTTP-запрос каскада давно всё отдал клиенту...
      │
      ▼
provider_webhooks.py: POST /webhooks/providers/{id} приходит снаружи
      │
      ▼
webhooks.py: event_bus.resolve(attempt_id, outcome)
      │  future.set_result(outcome)  — здесь БУДИМ
      ▼
reconciliation.py: asyncio.wait_for(...) возвращает outcome
      │
      ▼
cascade.py: resolved = outcome — продолжаем run_cascade с того же места
```
