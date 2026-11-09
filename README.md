# Payment Processing Service

Асинхронный сервис обработки платежей: FastAPI + SQLAlchemy 2.0 (async) + PostgreSQL + RabbitMQ (FastStream) + Alembic. Всё запускается через Docker Compose.

## Быстрый старт

```bash
cp .env.example .env                       # необязательно: есть значения по умолчанию
docker compose --profile demo up --build   # postgres, rabbitmq, migrate, api, consumer, webhook-receiver
```

- API: http://localhost:8000 (Swagger: `/docs`)
- RabbitMQ UI: http://localhost:15672 (guest / guest)
- Демо-приёмник webhook'ов: логи `docker compose logs -f webhook-receiver`

Без демо-приёмника: `docker compose up --build`.

## Примеры

```bash
# 1. Создать платёж (API-ключ по умолчанию: secret-api-key)
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.50", "currency": "RUB", "description": "Заказ №1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://webhook-receiver:9000/hook"}'
# -> 202 {"payment_id": "...", "status": "pending", "created_at": "..."}

# Повтор с тем же ключом и телом -> тот же payment_id (дубля нет).
# Тот же ключ с другим телом -> 409.

# 2. Получить платёж
curl -H "X-API-Key: secret-api-key" http://localhost:8000/api/v1/payments/<payment_id>
```

Через 2–5 секунд статус станет `succeeded` (90%) или `failed` (10%), на `webhook_url` придёт POST.

### Проверка retry и DLQ

Webhook на `http://webhook-receiver:9000/hook-fail` всегда отвечает 500:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" -H "Idempotency-Key: fail-1" -H "Content-Type: application/json" \
  -d '{"amount": "10", "currency": "USD", "description": "retry demo",
       "webhook_url": "http://webhook-receiver:9000/hook-fail"}'
```

В логах consumer: попытка 1 → retry через 5 с → попытка 2 → retry через 10 с → попытка 3 → DLQ.
Сообщение появится в очереди `payments.dlq` (RabbitMQ UI → Queues; в заголовках `x-attempt`, `x-error`).
Платёж при этом уже имеет финальный статус — в DLQ уходит только недоставленное уведомление.

## Архитектура (слои)

```
app/
├── api/              HTTP-слой: роуты, схемы Pydantic, auth (X-API-Key), маппинг ошибок
├── services/         бизнес-логика и use-cases + порты (Protocol)
│   ├── payment_service.py    создание платежа (идемпотентность + outbox), чтение
│   ├── payment_processor.py  шлюз -> статус -> webhook (идемпотентно)
│   ├── outbox_relay.py       outbox -> брокер
│   └── ports.py              PaymentGateway, WebhookSender, EventPublisher
├── repositories/     доступ к данным (SQLAlchemy-запросы)
├── db/               engine/session, ORM-модели
├── domain/           enum'ы, события, доменные исключения (без зависимостей от фреймворков)
├── infrastructure/   реализации портов: RabbitMQ (broker, topology, publisher), HTTP webhook, эмулятор шлюза
├── workers/          consumer (composition root воркера)
├── core/             конфигурация
└── main.py           composition root API (lifespan запускает outbox relay)
```

Зависимости идут сверху вниз: `api/workers → services → repositories → db → domain`. Сервисы знают только порты, реализации подставляются в composition root (`main.py`, `workers/consumer.py`).

## Как это работает

1. `POST /payments` в **одной транзакции** пишет `payments` и `outbox` (`payment.created`). Уникальный индекс на `idempotency_key` + обработка гонки (`IntegrityError`) защищают от дублей.
2. **Outbox relay** (фоновая задача в API-процессе) забирает неопубликованные события (`FOR UPDATE SKIP LOCKED`), публикует в exchange `payments` с routing key `payments.new` (persistent, с publisher confirms) и только после подтверждения ставит `published_at`. Если RabbitMQ недоступен, API продолжает принимать платежи, события копятся.
3. **Consumer** читает `payments.new`: эмулирует шлюз (2–5 с, 90/10), условным `UPDATE ... WHERE status='pending'` обновляет статус, шлёт webhook.
4. **Retry**: любая техническая ошибка (БД, сеть, не-2xx от webhook) → сообщение в `payments.new.retry.N` (TTL 5 с, 10 с, …, экспонента), по истечении TTL через DLX возвращается в основную очередь. Счётчик попыток — заголовок `x-attempt`. На повторной попытке платёж не обрабатывается заново, повторяется только webhook.
5. **DLQ**: после 3-й неудачной попытки (`MAX_ATTEMPTS`) сообщение уходит в `payments.dlx` → `payments.dlq`. Туда же попадают reject'нутые сообщения (например, невалидное тело).

Топология RabbitMQ: `payments` (direct) → `payments.new`; `payments.new.retry.1/2` (queue TTL → DLX обратно в `payments`); `payments.dlx` (direct) → `payments.dlq`.

## Гарантии и ограничения

- Доставка событий и webhook'ов — **at-least-once**: получатель webhook'а может получить дубль (идентифицируйте по `payment_id` / заголовку `X-Payment-Id`).
- Статус платежа меняется атомарно и ровно один раз, даже при повторной доставке сообщения.
- Настройки (`MAX_ATTEMPTS`, `RETRY_BASE_DELAY_SECONDS`, `OUTBOX_*`, параметры эмуляции) — в `app/core/config.py`, переопределяются переменными окружения.
- В production стоит ограничить `webhook_url` (защита от SSRF: запрет приватных сетей), добавить метрики и алерты на размер DLQ.

## Миграции

Применяются автоматически сервисом `migrate`. Вручную: `docker compose run --rm migrate alembic upgrade head`.
