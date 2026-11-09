import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import DateTime, Enum, Index, Numeric, String, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.domain.enums import Currency, PaymentStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


def _str_enum(cls: type, name: str) -> Enum:
    return Enum(
        cls,
        name=name,
        native_enum=False,
        length=16,
        create_constraint=False,
        values_callable=lambda e: [m.value for m in e],
    )


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_str_enum(Currency, "currency"))
    description: Mapped[str] = mapped_column(String(500))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    status: Mapped[PaymentStatus] = mapped_column(
        _str_enum(PaymentStatus, "payment_status"), default=PaymentStatus.PENDING
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OutboxEvent(Base):
    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_outbox_unpublished", "created_at", postgresql_where=text("published_at IS NULL")),
    )
