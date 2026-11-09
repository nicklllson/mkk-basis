import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.domain.enums import PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self._session.scalar(select(Payment).where(Payment.idempotency_key == key))

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def complete_if_pending(self, payment_id: uuid.UUID, status: PaymentStatus) -> bool:
        result = await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=datetime.now(timezone.utc))
        )
        return result.rowcount == 1
