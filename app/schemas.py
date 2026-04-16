from typing import Any, Literal, Optional

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


class UiThemeEvidence(BaseModel):
    quote: str
    filing_date: str | None = None
    filing_type: str | None = None
    source_kind: str = "quote"


class UiTheme(BaseModel):
    id: str
    theme_key: str
    label: str
    dimension_key: str | None = None
    score: float | None = None
    direction: str | None = None
    source_insight: str | None = None
    evidence_count: int | None = None
    evidence: list[UiThemeEvidence] = []
    delta: float | None = None
    delta_severity: str | None = None
    comparison_basis: str | None = None
    persistence_count: int | None = None
    persistence_score: float | None = None


class UiCurrentSignal(BaseModel):
    title: str
    role: str
    direction_label: str
    confidence_label: str
    interpretation: str
    why_it_matters: str
    evidence_snippet: str
    filing_type: str
    filing_date: str
    theme_key: str | None = None
    dimension_key: str | None = None
    score: float | None = None
    delta: float | None = None
    direction: str | None = None


class UiHistoryPoint(BaseModel):
    filing_date: str | None = None
    filing_type: str | None = None
    top_themes: list[str] = []
    takeaway: str | None = None
    evidence_snippet: str | None = None


class UiStrategyEvolutionCard(BaseModel):
    filing_type: str
    filing_date: str
    short_summary: str
    themes: list[str] = []
    evidence_snippet: str | None = None


class UiSnapshotMetadata(BaseModel):
    confidence_label: str
    coverage_note: str
    signal_set_note: str
    filing_basis: str
    durable_themes: list[str] = []


class UiTickerIntelligenceResponse(BaseModel):
    ticker: str
    filing_date: str | None = None
    filing_type: str | None = None
    narrative: str
    summary: str | None = None
    overall_strategy_story: str | None = None
    strategic_arc: str | None = None
    current_focus: str | None = None
    history_timeline: list[UiHistoryPoint] = []
    top_current_signals: list[UiCurrentSignal] = []
    what_changed: list[str] = []
    strategy_evolution: list[UiStrategyEvolutionCard] = []
    snapshot_metadata: UiSnapshotMetadata | None = None
    supporting_context: list[str] = []
    changes: list[str] = []
    evolution_points: list[str] = []
    durable_themes: list[str] = []
    implications: list[str] = []
    strategic_implications: list[str] = []
    confidence_coverage: Optional["TickerConfidenceCoverage"] = None
    themes: list[UiTheme]
    key_moves: list[UiTheme]
    risk_pairs: list[ClientStrategyResponseLink]


class TickerSignalFiling(BaseModel):
    type: str
    date: str


class TickerTopSignal(BaseModel):
    title: str
    direction: Literal["increasing", "stable", "decreasing", "new"]
    why_it_matters: str
    confidence_label: Literal["Strong", "Moderate", "Emerging"]
    evidence_snippet: str
    filing: TickerSignalFiling


class TickerChange(BaseModel):
    description: str


class TickerRiskResponse(BaseModel):
    risk: str
    response: str


class TickerConfidenceCoverage(BaseModel):
    confidence: Literal["Strong", "Moderate", "Emerging"]
    coverage: Literal["High", "Medium", "Low"]


class TickerIntelligenceResponse(BaseModel):
    summary: str
    strategic_arc: str | None = None
    current_focus: str | None = None
    evolution_points: list[str] = []
    durable_themes: list[str] = []
    top_signals: list[TickerTopSignal]
    changes: list[TickerChange]
    risk_response: list[TickerRiskResponse]
    confidence_coverage: TickerConfidenceCoverage


class ClientDominantTheme(BaseModel):
    key: str
    label: str
    dimension_key: str | None = None
    score: float
    strength: str
    evidence_quote: str | None = None
    evidence_quotes: list[str] | None = None
    evidence_source: str | None = None
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
