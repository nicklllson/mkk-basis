from faststream.rabbit import RabbitBroker

from app.core.config import settings

broker = RabbitBroker(settings.rabbitmq_url, max_consumers=settings.consumer_concurrency)
