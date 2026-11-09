import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.payments import router as payments_router
from app.core.config import settings
from app.db.session import SessionFactory, engine
from app.infrastructure.messaging.broker import broker
from app.infrastructure.messaging.publisher import RabbitEventPublisher
from app.services.outbox_relay import OutboxRelay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = RabbitEventPublisher(broker)
    relay = OutboxRelay(
        SessionFactory,
        publisher,
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval_seconds,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    try:
        yield
    finally:
        stop.set()
        await task
        try:
            await publisher.close()
        except Exception:
            pass
        await engine.dispose()


app = FastAPI(title="Payment Processing Service", version="1.0.0", lifespan=lifespan)
register_exception_handlers(app)
app.include_router(payments_router)
