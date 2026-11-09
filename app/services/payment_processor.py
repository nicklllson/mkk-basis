import logging
import uuid

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.db.models import Payment
from app.domain.enums import PaymentStatus
from app.domain.exceptions import PaymentNotFound
from app.repositories.payments import PaymentRepository
from app.services.ports import PaymentGateway, WebhookSender

log = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(self, session_factory: async_sessionmaker, gateway: PaymentGateway, webhook: WebhookSender) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._load(payment_id)

        if payment.status == PaymentStatus.PENDING:
            new_status = await self._gateway.charge(payment)
            async with self._session_factory.begin() as session:
                await PaymentRepository(session).complete_if_pending(payment_id, new_status)
            payment = await self._load(payment_id)
            log.info("Payment %s -> %s", payment_id, payment.status.value)

        await self._webhook.send(payment)
        log.info("Webhook delivered for payment %s (%s)", payment_id, payment.status.value)

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        return payment
