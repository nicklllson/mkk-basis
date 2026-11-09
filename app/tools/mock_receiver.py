import logging

from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("receiver")
app = FastAPI(title="Mock webhook receiver")


@app.post("/hook")
async def hook(request: Request) -> dict:
    log.info("WEBHOOK RECEIVED: %s", await request.json())
    return {"ok": True}


@app.post("/hook-fail")
async def hook_fail(request: Request) -> dict:
    log.info("WEBHOOK (will fail with 500): %s", await request.json())
    raise HTTPException(500, "simulated receiver failure")
