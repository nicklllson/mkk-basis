from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.domain.exceptions import IdempotencyConflict, PaymentNotFound


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFound)
    async def _not_found(_: Request, __: PaymentNotFound) -> JSONResponse:
        return JSONResponse({"detail": "Payment not found"}, status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(IdempotencyConflict)
    async def _conflict(_: Request, __: IdempotencyConflict) -> JSONResponse:
        return JSONResponse(
            {"detail": "Idempotency-Key was already used with a different request body"},
            status_code=status.HTTP_409_CONFLICT,
        )
