import logging

from faststream import FastStream
from faststream.rabbit.annotations import RabbitMessage
import httpx

from typing import Any


from app.db.models import DeadLetter
from app.core.config import settings
from app.db.session import SessionFactory
from app.domain.events import PaymentCreated

from app.infrastructure.gateway import EmulatedPaymentGateway
from app.infrastructure.messaging.broker import broker
from app.infrastructure.messaging.publisher import RabbitEventPublisher
from app.infrastructure.messaging.topology import (
    ATTEMPT_HEADER,
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_topology,
    DLQ,
    DLX_EXCHANGE
)
from app.infrastructure.webhook import HttpWebhookSender
from app.services.payment_processor import PaymentProcessor
from app.domain.exceptions import PaymentNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("consumer")

app = FastStream(broker)
publisher = RabbitEventPublisher(broker)
processor = PaymentProcessor(
    SessionFactory,
    gateway=EmulatedPaymentGateway(
        settings.processing_min_seconds, settings.processing_max_seconds, settings.processing_success_rate
    ),
    webhook=HttpWebhookSender(settings.webhook_timeout_seconds),
)


@app.after_startup
async def _declare_topology() -> None:
    await declare_topology(broker)

def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, PaymentNotFound):
        return False
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code in (408, 429)
    return True

@broker.subscriber(NEW_QUEUE, PAYMENTS_EXCHANGE)
async def handle_payment_created(event: PaymentCreated, msg: RabbitMessage) -> None:
    attempt = int((msg.headers or {}).get(ATTEMPT_HEADER, 1))
    try:
        await processor.process(event.payment_id)
    except Exception as exc:
        await publisher.retry_or_dead_letter(
            event.model_dump(mode="json"), attempt, exc, retryable=is_retryable(exc)
        )

@broker.subscriber(DLQ, DLX_EXCHANGE, retry=True)
async def handle_dead_letter(body: Any, msg: RabbitMessage) -> None:
    headers = msg.headers or {}
    log.error("Dead letter: body=%s headers=%s", body, headers)
    async with SessionFactory.begin() as session:
        session.add(
            DeadLetter(
                payload=body if isinstance(body, (dict, list)) else {"raw": str(body)},
                attempts=headers.get(ATTEMPT_HEADER),
                error=headers.get("x-error"),
            )
        )