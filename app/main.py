from fastapi import FastAPI

app = FastAPI(title="Payment Cascading Gateway")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
