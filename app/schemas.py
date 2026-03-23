from typing import Any

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    detail: str


class HealthResponse(BaseModel):
    status: str = "ok"


class TaxonomyCatalogResponse(BaseModel):
    catalog: dict[str, dict[str, dict[str, str]]]


class CompanySearchResult(BaseModel):
    ticker: str
    name: str


class CompanySearchResponse(BaseModel):
    items: list[CompanySearchResult]


class ClientStrategyThemeLabel(BaseModel):
    key: str
    label: str
    dimension_key: str | None = None
    score: float | None = None
    evidence_count: int | None = None
    evidence_quotes: list[str] | None = None
    persistence_count: int | None = None
    persistence_score: float | None = None
    score_components: dict[str, Any] | None = None


class ClientStrategySnapshotResponse(BaseModel):
    ticker: str
    filing_date: str
    filing_type: str
    dominant_themes: list[ClientStrategyThemeLabel]
    emerging_themes: list[ClientStrategyThemeLabel]
    declining_themes: list[ClientStrategyThemeLabel]


class ClientStrategyTrendPoint(BaseModel):
    quarter: str
    score: float


class ClientStrategyTrendSeries(BaseModel):
    theme_key: str
    dimension_key: str
    label: str
    series: list[ClientStrategyTrendPoint]


class ClientStrategyTrendsResponse(BaseModel):
    ticker: str
    theme_key: str | None = None
    series: list[ClientStrategyTrendPoint] = []
    themes: list[ClientStrategyTrendSeries] = []


class ClientStrategySignal(BaseModel):
    type: str
    theme_key: str
    theme_label: str
    dimension_key: str | None = None
    filing_id: int | None = None
    direction: str | None = None
    confidence: float
    title: str
    description: str
    evidence_summary: str | None = None
    filing_date: str
    filing_type: str
    evidence_quote: str | None = None
    current_score: float | None = None
    previous_score: float | None = None
    delta: float | None = None
    delta_severity: str | None = None
    comparison_basis: str | None = None
    persistence_count: int | None = None
    persistence_score: float | None = None
    score_components: dict[str, Any] | None = None


class ClientStrategySignalsResponse(BaseModel):
    ticker: str
    signals: list[ClientStrategySignal]


class ClientStrategyResponseLink(BaseModel):
    risk: str
    response: str
    direction: str
    quarter: str
    confidence: float
    link_strength: float | None = None
    summary: str
    filing_date: str
    filing_type: str
    risk_score: float
    response_score: float
    risk_delta: float
    response_delta: float
    evidence_quote_risk: str | None = None
    evidence_quote_response: str | None = None
    confidence_reason: str | None = None


class ClientStrategyResponseLinksResponse(BaseModel):
    ticker: str
    links: list[ClientStrategyResponseLink]


class ClientDominantTheme(BaseModel):
    key: str
    label: str
    dimension_key: str | None = None
    score: float
    strength: str
    evidence_quote: str | None = None
    evidence_quotes: list[str] | None = None
    why_selected: str | None = None
    persistence_count: int | None = None
    persistence_score: float | None = None
    score_components: dict[str, Any] | None = None


class ClientDominantThemesResponse(BaseModel):
    ticker: str
    filing_date: str
    filing_type: str
    dominant_themes: list[ClientDominantTheme]


class FeedbackCreateRequest(BaseModel):
    rating: str | None = Field(default=None, pattern="^(positive|neutral|negative)$")
    tags: list[str] = Field(default_factory=list, max_length=10)
    note: str | None = Field(default=None, max_length=2000)
    path: str | None = Field(default=None, max_length=500)
    submitted_at: str | None = Field(default=None, max_length=100)
    source: str | None = Field(default=None, max_length=100)


class FeedbackCreateResponse(BaseModel):
    status: str = "ok"
    feedback_id: int
