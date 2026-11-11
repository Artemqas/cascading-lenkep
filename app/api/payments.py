import asyncio
import hashlib
import json
import logging
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import app.db as db_module
from app.cascade import run_cascade
from app.db import get_session
from app.idempotency import IdempotencyConflict, get_or_create_payment
from app.models import Payment, ProviderAttempt
from app.schemas import AttemptResponse, PaymentCreateRequest, PaymentDetailResponse, PaymentResponse

logger = logging.getLogger(__name__)

router = APIRouter()

# Holds a strong reference to every scheduled cascade task so it can't be
# garbage-collected mid-flight, and so a failure that somehow escapes
# run_cascade's own handling still gets logged instead of silently vanishing.
_background_tasks: set[asyncio.Task] = set()


def _schedule_cascade(payment_id: str) -> None:
    task = asyncio.create_task(_run_cascade_background(payment_id))
    _background_tasks.add(task)
    task.add_done_callback(_on_cascade_task_done)


def _on_cascade_task_done(task: asyncio.Task) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("background cascade task failed", exc_info=exc)


def _to_payment_response(payment: Payment) -> PaymentResponse:
    return PaymentResponse(
        id=str(payment.id),
        direction=payment.direction,
        amount=payment.amount,
        currency=payment.currency,
        status=payment.status,
        created_at=payment.created_at,
    )


async def _run_cascade_background(payment_id: str) -> None:
    async with db_module.session_factory() as session:
        await run_cascade(session, payment_id)


@router.post("/payments", status_code=202, response_model=PaymentResponse)
async def create_payment(
    request: PaymentCreateRequest,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:
    payload_hash = hashlib.sha256(
        json.dumps(request.model_dump(mode="json"), sort_keys=True).encode()
    ).hexdigest()

    def build() -> Payment:
        return Payment(
            direction=request.direction,
            amount=request.amount,
            currency=request.currency,
            card_bin=request.card_bin,
            card_type=request.card_type,
            idempotency_key=idempotency_key,
            idempotency_payload_hash=payload_hash,
            callback_url=str(request.callback_url),
            provider_scenarios=request.provider_scenarios,
        )

    try:
        payment, created = await get_or_create_payment(session, idempotency_key, payload_hash, build)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail="Idempotency-Key reused with a different payload") from exc

    if created:
        _schedule_cascade(str(payment.id))
    else:
        response.status_code = 200

    return _to_payment_response(payment)


@router.get("/payments/{payment_id}", response_model=PaymentDetailResponse)
async def get_payment(payment_id: str, session: AsyncSession = Depends(get_session)) -> PaymentDetailResponse:
    try:
        payment_uuid = uuid.UUID(payment_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Payment not found") from exc

    payment = await session.get(Payment, payment_uuid)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")

    result = await session.execute(
        select(ProviderAttempt).where(ProviderAttempt.payment_id == payment_uuid)
    )
    attempts = result.scalars().all()

    return PaymentDetailResponse(
        **_to_payment_response(payment).model_dump(),
        attempts=[
            AttemptResponse(
                id=str(a.id),
                provider_id=a.provider_id,
                status=a.status,
                decline_code=a.decline_code,
                requested_at=a.requested_at,
                resolved_at=a.resolved_at,
            )
            for a in attempts
        ],
    )
