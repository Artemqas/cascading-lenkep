from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.api.provider_webhooks import router as provider_webhooks_router

app = FastAPI(title="Payment Cascading Gateway")
app.include_router(payments_router)
app.include_router(provider_webhooks_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
