from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.errors import to_http_error
from app.schemas import (
    ClientStrategyResponseLinksResponse,
    ClientStrategySignalsResponse,
    ClientStrategySnapshotResponse,
    ClientStrategyTrendsResponse,
    ErrorResponse,
    FeedbackCreateRequest,
    FeedbackCreateResponse,
    HealthResponse,
    TaxonomyCatalogResponse,
)
from app.service import ProductService


app = FastAPI(title=settings.api_title, version=settings.api_version)
service = ProductService()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def get_health() -> HealthResponse:
    return HealthResponse()


@app.get("/taxonomy-catalog", response_model=TaxonomyCatalogResponse, responses={500: {"model": ErrorResponse}})
def get_taxonomy_catalog_legacy():
    try:
        return service.get_taxonomy_catalog()
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get("/v1/taxonomy/catalog", response_model=TaxonomyCatalogResponse, responses={500: {"model": ErrorResponse}})
def get_taxonomy_catalog_v1():
    try:
        return service.get_taxonomy_catalog()
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/strategy/snapshot",
    response_model=ClientStrategySnapshotResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_strategy_snapshot(ticker: str):
    try:
        return service.get_strategy_snapshot(ticker=ticker)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/strategy/trends",
    response_model=ClientStrategyTrendsResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_strategy_trends(
    ticker: str,
    theme_key: str | None = None,
    limit: int = Query(default=400, ge=1, le=1200),
):
    try:
        return service.get_strategy_trends(ticker=ticker, theme_key=theme_key, limit=limit)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/strategy/signals",
    response_model=ClientStrategySignalsResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_strategy_signals(
    ticker: str,
    limit: int = Query(default=50, ge=1, le=500),
    latest_only: bool = False,
):
    try:
        return service.get_strategy_signals(ticker=ticker, limit=limit, latest_only=latest_only)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.get(
    "/v1/strategy/response-links",
    response_model=ClientStrategyResponseLinksResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_strategy_response_links(
    ticker: str,
    limit: int = Query(default=50, ge=1, le=500),
    latest_only: bool = False,
):
    try:
        return service.get_strategy_response_links(ticker=ticker, limit=limit, latest_only=latest_only)
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})


@app.post(
    "/v1/feedback",
    response_model=FeedbackCreateResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def post_feedback(payload: FeedbackCreateRequest, request: Request):
    try:
        return service.create_feedback(
            payload,
            user_agent=request.headers.get("user-agent"),
        )
    except Exception as exc:
        http_exc = to_http_error(exc)
        return JSONResponse(status_code=http_exc.status_code, content={"detail": str(http_exc.detail)})
