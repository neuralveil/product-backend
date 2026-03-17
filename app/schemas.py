from typing import Literal

from pydantic import BaseModel, Field


Severity = Literal["low", "medium", "high"]
ToneChange = Literal["accelerating", "reevaluating", "exited", "stable"]
NarrativeAlignment = Literal["confirms", "contradicts", "extends"]


class HealthResponse(BaseModel):
    status: str = "ok"


class TaxonomyLabelMeta(BaseModel):
    key: str
    display_name: str
    description: str


class TaxonomyDimensionMeta(BaseModel):
    key: str
    display_name: str
    description: str
    labels: list[TaxonomyLabelMeta]


class TaxonomyCatalogResponse(BaseModel):
    dimensions: list[TaxonomyDimensionMeta]


class TaxonomySignal(BaseModel):
    dimension_key: str
    label_key: str
    score: float = Field(..., ge=0.0, le=1.0)
    source: str


class DriftSignal(BaseModel):
    section_name: str
    score: float = Field(..., ge=0.0, le=1.0)
    severity: Severity
    tone_change: ToneChange
    narrative_alignment: NarrativeAlignment
    pivot_within_90d: bool
    weakness_count: int = Field(..., ge=0)
    confidence: float = Field(..., ge=0.0, le=1.0)
    short_narrative: str
    key_changes: list[str]
    evidence_quotes: list[str]


class QuarterState(BaseModel):
    filing_id: int
    filing_type: str
    filing_date: str
    shift_level: Severity | None = None
    net_direction: ToneChange | None = None
    contradictions_count: int = Field(..., ge=0)
    pivots_count: int = Field(..., ge=0)
    changed_sections_count: int = Field(..., ge=0)


class TickerSnapshotResponse(BaseModel):
    ticker: str
    company_id: int
    latest_quarter: QuarterState | None = None
    taxonomy_top_signals: list[TaxonomySignal]
    top_drift_signals: list[DriftSignal]


class QuarterNarrativePoint(BaseModel):
    quarter: QuarterState
    top_drift: DriftSignal | None = None
    top_taxonomy_signals: list[TaxonomySignal]


class TickerTimelineResponse(BaseModel):
    ticker: str
    company_id: int
    points: list[QuarterNarrativePoint]


class CrossTickerEvent(BaseModel):
    ticker: str
    company_id: int
    filing_date: str
    filing_type: str
    drift: DriftSignal


class EventsFeedResponse(BaseModel):
    events: list[CrossTickerEvent]


class ErrorResponse(BaseModel):
    detail: str
