from __future__ import annotations

from collections import defaultdict
from typing import Any

from app.config import settings
from app.errors import BadRequestError
from app.repository import ProductRepository
from app.schemas import (
    CrossTickerEvent,
    DriftSignal,
    EventsFeedResponse,
    QuarterNarrativePoint,
    QuarterState,
    TaxonomyCatalogResponse,
    TaxonomyDimensionMeta,
    TaxonomyLabelMeta,
    TaxonomySignal,
    TickerSnapshotResponse,
    TickerTimelineResponse,
)
from app.taxonomy import get_catalog_rows


SEVERITY_RANK = {"low": 1, "medium": 2, "high": 3}


class ProductService:
    def __init__(self, repository: ProductRepository | None = None) -> None:
        self.repo = repository or ProductRepository()

    def get_taxonomy_catalog(self) -> TaxonomyCatalogResponse:
        dimensions: list[TaxonomyDimensionMeta] = []
        for row in get_catalog_rows():
            labels = [TaxonomyLabelMeta(**label) for label in row["labels"]]
            dimensions.append(
                TaxonomyDimensionMeta(
                    key=row["key"],
                    display_name=row["display_name"],
                    description=row["description"],
                    labels=labels,
                )
            )
        return TaxonomyCatalogResponse(dimensions=dimensions)

    def get_ticker_snapshot(self, ticker: str) -> TickerSnapshotResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])

        states = self.repo.list_quarter_states(company_id, limit=1)
        latest_quarter = self._to_quarter_state(states[0]) if states else None

        extractions = self.repo.list_strategy_extractions(company_id, limit=400)
        if not extractions:
            return TickerSnapshotResponse(
                ticker=company["ticker"],
                company_id=company_id,
                latest_quarter=latest_quarter,
                taxonomy_top_signals=[],
                top_drift_signals=[],
            )

        latest_filing_id = self._latest_filing_id(extractions)
        latest_rows = [row for row in extractions if self._filing_id(row) == latest_filing_id]
        taxonomy_signals = self._build_top_taxonomy_signals(latest_rows)
        drift_signals = self._build_drift_signals(latest_rows, min_severity="low")[:12]

        return TickerSnapshotResponse(
            ticker=company["ticker"],
            company_id=company_id,
            latest_quarter=latest_quarter,
            taxonomy_top_signals=taxonomy_signals,
            top_drift_signals=drift_signals,
        )

    def get_ticker_timeline(self, ticker: str, limit: int) -> TickerTimelineResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])
        quarter_states = self.repo.list_quarter_states(company_id, limit=limit)
        if not quarter_states:
            return TickerTimelineResponse(ticker=company["ticker"], company_id=company_id, points=[])

        extractions = self.repo.list_strategy_extractions(company_id, limit=900)
        rows_by_filing: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in extractions:
            filing_id = self._filing_id(row)
            if filing_id:
                rows_by_filing[filing_id].append(row)

        points: list[QuarterNarrativePoint] = []
        for state_row in quarter_states:
            filing_id = int(state_row.get("filing_id", 0) or 0)
            filing_rows = rows_by_filing.get(filing_id, [])
            point = QuarterNarrativePoint(
                quarter=self._to_quarter_state(state_row),
                top_drift=(self._build_drift_signals(filing_rows, min_severity="low") or [None])[0],
                top_taxonomy_signals=self._build_top_taxonomy_signals(filing_rows),
            )
            points.append(point)

        points.sort(key=lambda p: p.quarter.filing_date, reverse=True)
        return TickerTimelineResponse(ticker=company["ticker"], company_id=company_id, points=points)

    def get_events_feed(
        self,
        tickers: list[str],
        min_severity: str,
        limit: int,
    ) -> EventsFeedResponse:
        severity_floor = SEVERITY_RANK.get(min_severity.lower())
        if not severity_floor:
            raise BadRequestError("min_severity must be one of: low, medium, high")

        companies = self.repo.list_companies_by_tickers(tickers)
        events: list[CrossTickerEvent] = []

        for company in companies:
            company_id = int(company["id"])
            ticker = str(company["ticker"])
            rows = self.repo.list_strategy_extractions(company_id, settings.max_events_lookback_rows)
            drift_rows = self._build_drift_signals(rows, min_severity=min_severity)
            for drift in drift_rows:
                filing_row = drift.__dict__.get("_filing_row", {})
                events.append(
                    CrossTickerEvent(
                        ticker=ticker,
                        company_id=company_id,
                        filing_date=str(filing_row.get("filing_date", "")),
                        filing_type=str(filing_row.get("filing_type", "")),
                        drift=drift,
                    )
                )

        events.sort(
            key=lambda event: (
                SEVERITY_RANK.get(event.drift.severity, 0),
                event.drift.score,
                event.filing_date,
            ),
            reverse=True,
        )
        return EventsFeedResponse(events=events[:limit])

    def _latest_filing_id(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        ordered = sorted(rows, key=lambda row: self._filing_date(row), reverse=True)
        return self._filing_id(ordered[0])

    def _filing_date(self, row: dict[str, Any]) -> str:
        section = row.get("sections") or {}
        filing = section.get("filings") or {}
        return str(filing.get("filing_date", ""))

    def _filing_id(self, row: dict[str, Any]) -> int:
        section = row.get("sections") or {}
        filing = section.get("filings") or {}
        return int(filing.get("id", 0) or 0)

    def _to_quarter_state(self, row: dict[str, Any]) -> QuarterState:
        return QuarterState(
            filing_id=int(row.get("filing_id", 0) or 0),
            filing_type=str(row.get("filing_type", "")),
            filing_date=str(row.get("filing_date", "")),
            shift_level=row.get("shift_level"),
            net_direction=row.get("net_direction"),
            contradictions_count=int(row.get("contradictions_count", 0) or 0),
            pivots_count=int(row.get("pivots_count", 0) or 0),
            changed_sections_count=int(row.get("changed_sections_count", 0) or 0),
        )

    def _build_top_taxonomy_signals(self, rows: list[dict[str, Any]]) -> list[TaxonomySignal]:
        extraction_ids = [int(row.get("id", 0) or 0) for row in rows if row.get("id") is not None]
        score_rows = self.repo.list_taxonomy_scores(extraction_ids)
        by_dimension_label: dict[tuple[str, str], float] = {}
        source_by_key: dict[tuple[str, str], str] = {}

        for row in score_rows:
            key = (str(row.get("dimension_key", "")), str(row.get("label_key", "")))
            score = float(row.get("score", 0) or 0)
            if key not in by_dimension_label or score > by_dimension_label[key]:
                by_dimension_label[key] = score
                source_by_key[key] = str(row.get("source", "unknown"))

        # Keep strongest two labels per dimension for compact, high-engagement payloads.
        grouped: dict[str, list[TaxonomySignal]] = defaultdict(list)
        for (dimension_key, label_key), score in by_dimension_label.items():
            grouped[dimension_key].append(
                TaxonomySignal(
                    dimension_key=dimension_key,
                    label_key=label_key,
                    score=round(score, 3),
                    source=source_by_key[(dimension_key, label_key)],
                )
            )

        compact: list[TaxonomySignal] = []
        for dimension_key, dimension_rows in grouped.items():
            dimension_rows.sort(key=lambda item: item.score, reverse=True)
            compact.extend(dimension_rows[:2])

        compact.sort(key=lambda item: item.score, reverse=True)
        return compact[:12]

    def _build_drift_signals(self, rows: list[dict[str, Any]], min_severity: str) -> list[DriftSignal]:
        floor = SEVERITY_RANK.get(min_severity.lower(), 1)
        result: list[DriftSignal] = []
        for row in rows:
            extracted = row.get("extracted_data") or {}
            delta = extracted.get("quarterly_delta") or {}
            if not isinstance(delta, dict) or not delta:
                continue
            scoring = delta.get("scoring") or {}
            severity = str(scoring.get("severity", "low"))
            if SEVERITY_RANK.get(severity, 0) < floor:
                continue

            score = float(scoring.get("score", 0) or 0)
            confidence = float(row.get("confidence", 0) or 0)
            section = row.get("sections") or {}

            signal = DriftSignal(
                section_name=str(section.get("section_name", "")),
                score=round(score, 3),
                severity=severity,
                tone_change=str(delta.get("tone_change", "stable")),
                narrative_alignment=str(delta.get("narrative_alignment", "extends")),
                pivot_within_90d=bool(delta.get("pivot_within_90d", False)),
                weakness_count=len(delta.get("segment_weakness_explanations", []) or []),
                confidence=round(confidence, 3),
                short_narrative=str(delta.get("short_narrative", "")),
                key_changes=list(delta.get("key_changes", []) or []),
                evidence_quotes=list(delta.get("evidence_quotes", []) or []),
            )
            signal.__dict__["_filing_row"] = section.get("filings") or {}
            result.append(signal)

        result.sort(key=lambda item: (SEVERITY_RANK.get(item.severity, 0), item.score), reverse=True)
        return result
