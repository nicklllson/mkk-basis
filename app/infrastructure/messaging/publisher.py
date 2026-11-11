import logging
import uuid
from typing import Any

from faststream.rabbit import RabbitBroker

from app.core.config import settings
from app.infrastructure.messaging.topology import (
    ATTEMPT_HEADER,
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE,
    NEW_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    declare_topology,
    retry_queue,
)

log = logging.getLogger(__name__)


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def connect(self) -> None:
        await self._broker.start()
        await declare_topology(self._broker)

    async def close(self) -> None:
        await self._broker.close()

    async def publish_event(self, *, event_id: uuid.UUID, event_type: str, payload: dict[str, Any]) -> None:
        await self._broker.publish(
            payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=NEW_ROUTING_KEY,
            persist=True,
            message_id=str(event_id),
            headers={"event_type": event_type},
        )

    async def retry_or_dead_letter(
        self,
        body: dict[str, Any],
        attempt: int,
        exc: Exception,
        *,
        retryable: bool = True
    ) -> None:
        error = f"{type(exc).__name__}: {exc}"[:500]
        if not retryable or attempt >= settings.max_attempts:
            log.error("Attempt %d/%d failed (retryable=%s) -> DLQ: %s", attempt, settings.max_attempts, retryable, error)
            await self._broker.publish(
                body,
                exchange=DLX_EXCHANGE,
                routing_key=DLQ_ROUTING_KEY,
                persist=True,
                headers={ATTEMPT_HEADER: attempt, "x-error": error},
            )
        else:
            queue = retry_queue(attempt)
            log.warning("Attempt %d/%d failed -> %s: %s", attempt, settings.max_attempts, queue.name, error)
            await self._broker.publish(
                body,
                queue=queue.name,
                persist=True,
                headers={ATTEMPT_HEADER: attempt + 1, "x-error": error},
            )