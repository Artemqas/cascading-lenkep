import uuid

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.cascade import handle_late_resolution
from app.db import get_session
from app.models import ProviderAttempt
from app.providers.registry import get_adapter
from app.webhooks import event_bus

router = APIRouter()


@router.post("/webhooks/providers/{provider_id}")
async def receive_provider_webhook(
    provider_id: str, payload: dict, session: AsyncSession = Depends(get_session)
) -> dict:
    adapter = get_adapter(provider_id)
    resolution = adapter.parse_webhook(payload)

    if event_bus.resolve(resolution.attempt_id, resolution.outcome):
        return {"status": "ok"}

    try:
        attempt_id = uuid.UUID(resolution.attempt_id)
    except ValueError:
        return {"status": "ok"}

    attempt = await session.get(ProviderAttempt, attempt_id)
    if attempt is not None:
        await handle_late_resolution(session, attempt, resolution.outcome)

    return {"status": "ok"}
