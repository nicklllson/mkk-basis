import uuid

from pydantic import BaseModel

EVENT_PAYMENT_CREATED = "payment.created"


class PaymentCreated(BaseModel):
    payment_id: uuid.UUID
