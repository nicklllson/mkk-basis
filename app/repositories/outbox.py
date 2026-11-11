from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event_type: str, payload: dict[str, Any]) -> OutboxEvent:
        event = OutboxEvent(event_type=event_type, payload=payload)
        self._session.add(event)
        return event

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        result = await self._session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return result.all()

    async def delete_published_before(self, cutoff: datetime) -> int:
        result = await self._session.execute(
            delete(OutboxEvent).where(
                OutboxEvent.published_at.is_not(None),
                OutboxEvent.published_at < cutoff,
            )
        )
        return result.rowcount

    @staticmethod
    def mark_published(event: OutboxEvent) -> None:
        event.published_at = datetime.now(timezone.utc)
