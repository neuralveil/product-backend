from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from app.config import settings
from app.errors import to_http_error
from app.schemas import (
    ErrorResponse,
    EventsFeedResponse,
    HealthResponse,
    TaxonomyCatalogResponse,
    TickerSnapshotResponse,
    TickerTimelineResponse,
)
from app.service import ProductService


app = FastAPI(title=settings.api_title, version=settings.api_version)
service = ProductService()


@app.get("/health", response_model=HealthResponse)
def get_health() -> HealthResponse:
    return HealthResponse()


@app.get("/v1/taxonomy/catalog", response_model=TaxonomyCatalogResponse, responses={500: {"model": ErrorResponse}})
def get_taxonomy_catalog():
    try:
        return service.get_taxonomy_catalog()
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/tickers/{ticker}/snapshot",
    response_model=TickerSnapshotResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_ticker_snapshot(ticker: str):
    try:
        return service.get_ticker_snapshot(ticker)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/tickers/{ticker}/timeline",
    response_model=TickerTimelineResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_ticker_timeline(
    ticker: str,
    limit: int = Query(default=settings.default_timeline_limit, ge=1, le=24),
):
    try:
        return service.get_ticker_timeline(ticker=ticker, limit=limit)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/events",
    response_model=EventsFeedResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_events(
    tickers: str = Query(..., description="Comma-separated ticker list, e.g. AAPL,MSFT,NVDA"),
    min_severity: str = Query(default="medium", pattern="^(low|medium|high)$"),
    limit: int = Query(default=settings.default_events_limit, ge=1, le=200),
):
    try:
        parsed = [item.strip().upper() for item in tickers.split(",") if item.strip()]
        return service.get_events_feed(tickers=parsed, min_severity=min_severity, limit=limit)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})
