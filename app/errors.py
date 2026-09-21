"""统一的 API 错误类型与异常处理器。"""

from fastapi import Request
from fastapi.responses import JSONResponse


class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str, **details: object):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)


async def api_error_handler(_: Request, exc: APIError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, **exc.details}},
    )
