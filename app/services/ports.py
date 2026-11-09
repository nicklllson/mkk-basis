from typing import Any, Protocol
import uuid

from app.db.models import Payment
from app.domain.enums import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None:
        """Бросает исключение, если доставить не удалось."""


class EventPublisher(Protocol):
    async def connect(self) -> None: ...

    async def publish_event(self, *, event_id: uuid.UUID, event_type: str, payload: dict[str, Any]) -> None: ...
