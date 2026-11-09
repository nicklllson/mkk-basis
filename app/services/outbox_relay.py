import asyncio
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.repositories.outbox import OutboxRepository
from app.services.ports import EventPublisher

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        publisher: EventPublisher,
        *,
        batch_size: int,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def publish_batch(self) -> int:
        async with self._session_factory.begin() as session:
            repo = OutboxRepository(session)
            published = 0
            for event in await repo.lock_unpublished(self._batch_size):
                try:
                    await self._publisher.publish_event(
                        event_id=event.id, event_type=event.event_type, payload=event.payload
                    )
                except Exception:
                    log.exception("Failed to publish outbox event %s", event.id)
                    break
                repo.mark_published(event)
                published += 1
            return published

    async def run(self, stop: asyncio.Event) -> None:
        connected = False
        while not stop.is_set():
            published = 0
            try:
                if not connected:
                    await self._publisher.connect()
                    connected = True
                    log.info("Outbox relay connected to broker")
                published = await self.publish_batch()
            except Exception:
                log.exception("Outbox relay iteration failed")
            if published == 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
                except asyncio.TimeoutError:
                    pass
