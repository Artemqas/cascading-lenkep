from fastapi import FastAPI

from app.api.payments import router as payments_router

app = FastAPI(title="Payment Cascading Gateway")
app.include_router(payments_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
