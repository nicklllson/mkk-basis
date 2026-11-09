import logging

from faststream import FastStream
from faststream.rabbit.annotations import RabbitMessage

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
)
from app.infrastructure.webhook import HttpWebhookSender
from app.services.payment_processor import PaymentProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

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


@broker.subscriber(NEW_QUEUE, PAYMENTS_EXCHANGE)
async def handle_payment_created(event: PaymentCreated, msg: RabbitMessage) -> None:
    attempt = int((msg.headers or {}).get(ATTEMPT_HEADER, 1))
    try:
        await processor.process(event.payment_id)
    except Exception as exc:
        await publisher.retry_or_dead_letter(event.model_dump(mode="json"), attempt, exc)
