import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import SessionFactory
from app.services.payment_service import PaymentService


async def verify_api_key(x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None) -> None:
    if x_api_key is None or not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session


def get_payment_service(session: Annotated[AsyncSession, Depends(get_session)]) -> PaymentService:
    return PaymentService(session)
