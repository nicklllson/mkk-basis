import httpx

from app.db.models import Payment


def build_payload(p: Payment) -> dict:
    return {
        "payment_id": str(p.id),
        "status": p.status.value,
        "amount": str(p.amount),
        "currency": p.currency.value,
        "description": p.description,
        "metadata": p.metadata_,
        "created_at": p.created_at.isoformat(),
        "processed_at": p.processed_at.isoformat() if p.processed_at else None,
    }


class HttpWebhookSender:
    def __init__(self, timeout: float) -> None:
        self._timeout = timeout

    async def send(self, payment: Payment) -> None:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                payment.webhook_url,
                json=build_payload(payment),
                headers={"X-Payment-Id": str(payment.id)},
            )
            resp.raise_for_status()
