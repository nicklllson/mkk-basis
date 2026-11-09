import asyncio
import random

from app.db.models import Payment
from app.domain.enums import PaymentStatus


class EmulatedPaymentGateway:
    def __init__(self, min_seconds: float, max_seconds: float, success_rate: float) -> None:
        self._min = min_seconds
        self._max = max_seconds
        self._success_rate = success_rate

    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(random.uniform(self._min, self._max))
        return PaymentStatus.SUCCEEDED if random.random() < self._success_rate else PaymentStatus.FAILED
