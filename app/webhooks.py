import asyncio

from app.providers.base import AttemptOutcome


class WebhookEventBus:
    def __init__(self) -> None:
        self._waiters: dict[str, asyncio.Future] = {}

    def register_wait(self, attempt_id: str) -> asyncio.Future:
        future = asyncio.get_event_loop().create_future()
        self._waiters[attempt_id] = future
        return future

    def resolve(self, attempt_id: str, outcome: AttemptOutcome) -> bool:
        future = self._waiters.pop(attempt_id, None)
        if future is None or future.done():
            return False
        future.set_result(outcome)
        return True


event_bus = WebhookEventBus()
