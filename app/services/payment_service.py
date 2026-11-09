import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.domain.enums import Currency
from app.domain.events import EVENT_PAYMENT_CREATED
from app.domain.exceptions import IdempotencyConflict, PaymentNotFound
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository


@dataclass(frozen=True)
class CreatePayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


class PaymentService:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, cmd: CreatePayment, idempotency_key: str) -> Payment:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)

        if existing is None:
            payment = Payment(
                id=uuid.uuid4(),
                amount=cmd.amount,
                currency=cmd.currency,
                description=cmd.description,
                metadata_=cmd.metadata,
                idempotency_key=idempotency_key,
                webhook_url=cmd.webhook_url,
            )
            self._payments.add(payment)
            self._outbox.add(EVENT_PAYMENT_CREATED, {"payment_id": str(payment.id)})
            try:
                await self._session.commit()
                return payment
            except IntegrityError:
                await self._session.rollback()
                existing = await self._payments.get_by_idempotency_key(idempotency_key)
                if existing is None:
                    raise

        if not self._same(existing, cmd):
            raise IdempotencyConflict(idempotency_key)
        return existing

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        return payment

    @staticmethod
    def _same(p: Payment, cmd: CreatePayment) -> bool:
        return (
            p.amount == cmd.amount
            and p.currency == cmd.currency
            and p.description == cmd.description
            and p.metadata_ == cmd.metadata
            and p.webhook_url == cmd.webhook_url
        )
