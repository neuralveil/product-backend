from fastapi import HTTPException


class NotFoundError(Exception):
    pass


class BadRequestError(Exception):
    pass


class BackendError(Exception):
    pass


def to_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, BadRequestError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))
