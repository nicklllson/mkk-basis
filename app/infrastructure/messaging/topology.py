from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import settings

ATTEMPT_HEADER = "x-attempt"
NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.dlq"
QUORUM = { "x-queue-type": "quorum" }

def _dead_letter_args(exchange: str, routing_key: str) -> dict:
    return {
        **QUORUM,
        "x-dead-letter-exchange": exchange,
        "x-dead-letter-routing-key": routing_key,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        **_dead_letter_args(DLX_EXCHANGE.name, DLQ_ROUTING_KEY),
        "x-delivery-limit": 5,
    },
)
DLQ = RabbitQueue("payments.dlq", durable=True, routing_key=DLQ_ROUTING_KEY, arguments=QUORUM)


def retry_delay_seconds(attempt: int) -> int:
    return settings.retry_base_delay_seconds * 2 ** (attempt - 1)


def retry_queue(attempt: int) -> RabbitQueue:
    return RabbitQueue(
        f"payments.new.retry.{attempt}",
        durable=True,
        arguments={
            **_dead_letter_args(PAYMENTS_EXCHANGE.name, NEW_ROUTING_KEY),
            "x-message-ttl": retry_delay_seconds(attempt) * 1000,
        },
    )


async def declare_topology(broker: RabbitBroker) -> None:
    payments_ex = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx_ex = await broker.declare_exchange(DLX_EXCHANGE)

    new_q = await broker.declare_queue(NEW_QUEUE)
    await new_q.bind(payments_ex, routing_key=NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(DLQ)
    await dlq.bind(dlx_ex, routing_key=DLQ_ROUTING_KEY)

    for attempt in range(1, settings.max_attempts):
        await broker.declare_queue(retry_queue(attempt))
