import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, status

from app.api.deps import get_payment_service, verify_api_key
from app.api.schemas import PaymentAccepted, PaymentCreateRequest, PaymentResponse
from app.db.models import Payment
from app.services.payment_service import CreatePayment, PaymentService

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(verify_api_key)])

Service = Annotated[PaymentService, Depends(get_payment_service)]


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreateRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
    service: Service,
) -> Payment:
    cmd = CreatePayment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
    )
    return await service.create(cmd, idempotency_key)


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(payment_id: uuid.UUID, service: Service) -> Payment:
    return await service.get(payment_id)
