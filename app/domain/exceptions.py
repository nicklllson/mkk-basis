class DomainError(Exception):
    pass


class PaymentNotFound(DomainError):
    pass


class IdempotencyConflict(DomainError):
    """Ключ идемпотентности уже использован с другим запросом."""
