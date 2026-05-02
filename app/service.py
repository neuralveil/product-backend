from __future__ import annotations

from collections import defaultdict
import time
from typing import Any

from app.errors import NotFoundError
from app.repository import ProductRepository
from app.schemas import (
    ClientDominantTheme,
    ClientDominantThemesResponse,
    CompanySearchResponse,
    CompanySearchResult,
    FeedbackCreateRequest,
    FeedbackCreateResponse,
    ClientStrategyResponseLinksResponse,
    ClientStrategySignal,
    ClientStrategySignalsResponse,
    ClientStrategySnapshotResponse,
    ClientStrategyThemeLabel,
    ClientStrategyTrendPoint,
    ClientStrategyTrendSeries,
    ClientStrategyTrendsResponse,
    TickerChange,
    TickerConfidenceCoverage,
    TickerIntelligenceResponse,
    TickerRiskResponse,
    TickerSignalFiling,
    TickerTopSignal,
    TaxonomyCatalogResponse,
    UiShareableTicker,
    UiShareableTickersResponse,
    UiTheme,
    UiThemeEvidence,
    UiTickerIntelligenceResponse,
)
from app.taxonomy import (
    ACTION_KEYWORDS,
    DIRECTION_KEYWORDS,
    RISK_POSTURE_KEYWORDS,
    TAXONOMY_CATALOG,
    coerce_theme_row,
    flatten_allowed_dimensions,
    flatten_allowed_theme_keys,
    get_taxonomy_catalog_detailed,
    label_display_name,
    label_signal_description,
    quarter_from_date,
)


RISK_RESPONSE_MAP: dict[str, list[str]] = {
    "regulatory_exposure": ["regulatory_remediation_program", "strategic_partnership"],
    "supply_chain_fragility": ["supply_chain_relocation", "strategic_partnership", "large_capex_program"],
    "competitive_intensity": [
        "product_category_launch",
        "subscription_pricing_shift",
        "ai_infrastructure_investment",
        "strategic_partnership",
    ],
}

GENERIC_THEME_KEYS = {"international_expansion", "cost_efficiency"}
HIGH_SIGNAL_THEME_BOOSTS: dict[str, float] = {
    "ai_automation": 1.15,
    "ai_infrastructure_investment": 1.12,
    "platform_ecosystem": 1.1,
    "product_expansion": 1.08,
}


class ProductService:
    def __init__(self, repository: ProductRepository | None = None) -> None:
        self.repo = repository or ProductRepository()
        self._ticker_intelligence_ttl_seconds = 120
        self._ticker_intelligence_cache: dict[str, tuple[float, TickerIntelligenceResponse]] = {}

    def _get_precomputed_intelligence_row(self, ticker: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
        company = self.repo.get_company_by_ticker(ticker)
        intelligence = self.repo.get_latest_strategy_intelligence(int(company["id"]))
        return company, intelligence

    def _validate_model(self, model_cls: Any, payload: dict[str, Any]) -> Any:
        if hasattr(model_cls, "model_validate"):
            return model_cls.model_validate(payload)
        return model_cls(**payload)

    def _load_precomputed_core_intelligence(self, ticker: str) -> TickerIntelligenceResponse | None:
        try:
            _, intelligence = self._get_precomputed_intelligence_row(ticker)
        except Exception:
            return None
        if not intelligence:
            return None
        payload = intelligence.get("core_response_json")
        if not isinstance(payload, dict) or not payload:
            return None
        try:
            return self._validate_model(TickerIntelligenceResponse, payload)
        except Exception:
            return None

    def _load_precomputed_ui_intelligence(self, ticker: str) -> UiTickerIntelligenceResponse | None:
        try:
            company, intelligence = self._get_precomputed_intelligence_row(ticker)
        except Exception:
            return None
        if not intelligence:
            return None
        payload = intelligence.get("ui_response_json")
        if not isinstance(payload, dict) or not payload:
            return None
        payload = self._normalize_precomputed_ui_payload(
            payload=dict(payload),
            ticker=str(company.get("ticker", ticker.upper())),
            filing_date=str(intelligence.get("filing_date") or ""),
            filing_type=str(intelligence.get("filing_type") or ""),
        )
        try:
            return self._validate_model(UiTickerIntelligenceResponse, payload)
        except Exception:
            return None

    def get_taxonomy_catalog(self) -> TaxonomyCatalogResponse:
        return TaxonomyCatalogResponse(catalog=get_taxonomy_catalog_detailed())

    def get_shareable_tickers(self, limit: int = 6) -> UiShareableTickersResponse:
        rows = self.repo.list_recent_strategy_intelligence(limit=max(limit * 8, 80))
        companies = self.repo.list_companies_by_ids(
            [int(row.get("company_id", 0) or 0) for row in rows if row.get("company_id")]
        )
        best_by_ticker: dict[str, UiShareableTicker] = {}

        for row in rows:
            company = companies.get(int(row.get("company_id", 0) or 0))
            if not company:
                continue
            ticker = str(company.get("ticker") or "").upper().strip()
            if not ticker:
                continue
            payload = row.get("ui_response_json")
            if not isinstance(payload, dict) or not payload:
                payload = row.get("core_response_json")
            if not isinstance(payload, dict) or not payload:
                continue

            candidate = self._shareable_ticker_from_payload(
                ticker=ticker,
                name=str(company.get("name") or ticker),
                payload=payload,
                filing_date=str(row.get("filing_date") or ""),
                filing_type=str(row.get("filing_type") or ""),
                updated_at=str(row.get("updated_at") or "") or None,
            )
            if not candidate:
                continue
            existing = best_by_ticker.get(ticker)
            if existing is None or candidate.readiness_score > existing.readiness_score:
                best_by_ticker[ticker] = candidate

        items = sorted(
            best_by_ticker.values(),
            key=lambda item: (item.readiness_score, str(item.filing_date or ""), item.ticker),
            reverse=True,
        )[:limit]
        return UiShareableTickersResponse(items=items)

    def get_ticker_intelligence(self, ticker: str) -> TickerIntelligenceResponse:
        normalized_ticker = ticker.upper().strip()
        now = time.time()
        cached = self._ticker_intelligence_cache.get(normalized_ticker)
        if cached and cached[0] > now:
            return cached[1]

        precomputed = self._load_precomputed_core_intelligence(normalized_ticker)
        if precomputed is not None:
            self._ticker_intelligence_cache[normalized_ticker] = (
                now + self._ticker_intelligence_ttl_seconds,
                precomputed,
            )
            return precomputed

        snapshot = self.get_strategy_snapshot(normalized_ticker)
        signals = self.get_strategy_signals(
            normalized_ticker,
            limit=120,
            latest_only=True,
            include_score_components=False,
        )
        links = self.get_strategy_response_links(normalized_ticker, limit=50, latest_only=True)

        deduped: dict[str, dict[str, Any]] = {}
        for row in signals.signals:
            theme_key = str(row.theme_key or "")
            if not theme_key:
                continue
            key = self._canonical_theme_key(theme_key)
            confidence_value = float(
                row.current_score
                if row.current_score is not None
                else (row.confidence if row.confidence is not None else 0.0)
            )
            direction = str(row.direction or "stable").lower()
            if row.previous_score is None and row.delta is None:
                direction = "new"
            if direction not in {"increasing", "stable", "decreasing", "new"}:
                direction = "stable"

            title = str(row.theme_label or label_display_name(theme_key) or theme_key.replace("_", " ").title())
            delta_mag = abs(float(row.delta or 0.0))
            impact_rank = self._impact_rank(theme_key, row.dimension_key)
            direction_rank = self._direction_rank(direction)

            evidence = self._evidence_snippet(row.evidence_quote)
            filing_type = str(row.filing_type or snapshot.filing_type or "")
            filing_date = str(row.filing_date or snapshot.filing_date or "")
            candidate = {
                "title": title,
                "direction": direction,
                "confidence": confidence_value,
                "confidence_label": self._confidence_label(confidence_value),
                "why_it_matters": self._investor_why(theme_key),
                "evidence_snippet": evidence or "No direct filing quote available in current snapshot.",
                "filing_type": filing_type,
                "filing_date": filing_date,
                "impact_rank": impact_rank,
                "direction_rank": direction_rank,
                "delta_mag": delta_mag,
            }
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = candidate
                continue
            if (
                direction_rank,
                impact_rank,
                confidence_value,
                delta_mag,
            ) > (
                existing["direction_rank"],
                existing["impact_rank"],
                existing["confidence"],
                existing["delta_mag"],
            ):
                deduped[key] = candidate

        # Supplement sparse signal rows with snapshot themes so we do not collapse to a
        # single-theme narrative when latest drift/signals are thin.
        snapshot_theme_rows = list(snapshot.dominant_themes or []) + list(snapshot.emerging_themes or [])
        for row in snapshot_theme_rows:
            theme_key = str(row.key or "")
            canonical = self._canonical_theme_key(theme_key)
            if not canonical or canonical in deduped:
                continue
            confidence_value = float(row.score or 0.45)
            evidence_quotes = list(row.evidence_quotes or [])
            evidence = self._evidence_snippet(evidence_quotes[0] if evidence_quotes else None)
            deduped[canonical] = {
                "title": str(row.label or label_display_name(theme_key) or theme_key.replace("_", " ").title()),
                "direction": "stable",
                "confidence": confidence_value,
                "confidence_label": self._confidence_label(confidence_value),
                "why_it_matters": self._investor_why(theme_key),
                "evidence_snippet": evidence or "No direct filing quote available in current snapshot.",
                "filing_type": str(snapshot.filing_type or ""),
                "filing_date": str(snapshot.filing_date or ""),
                "impact_rank": self._impact_rank(theme_key, row.dimension_key),
                "direction_rank": self._direction_rank("stable"),
                "delta_mag": 0.0,
            }

        ranked = list(deduped.values())
        ranked.sort(
            key=lambda item: (
                -item["direction_rank"],
                -item["impact_rank"],
                -float(item["confidence"]),
                -float(item["delta_mag"]),
                item["title"].lower(),
            )
        )
        ranked = ranked[:3]

        if not ranked:
            fallback_rows: list[dict[str, Any]] = []
            fallback_seen: set[str] = set()
            snapshot_themes = list(snapshot.dominant_themes or []) + list(snapshot.emerging_themes or [])
            for row in snapshot_themes:
                theme_key = str(row.key or "")
                canonical = self._canonical_theme_key(theme_key)
                if not canonical or canonical in fallback_seen:
                    continue
                fallback_seen.add(canonical)
                confidence_value = float(row.score or 0.45)
                evidence_quotes = list(row.evidence_quotes or [])
                evidence = self._evidence_snippet(evidence_quotes[0] if evidence_quotes else None)
                fallback_rows.append(
                    {
                        "title": str(row.label or label_display_name(theme_key) or theme_key.replace("_", " ").title()),
                        "direction": "stable",
                        "confidence": confidence_value,
                        "confidence_label": self._confidence_label(confidence_value),
                        "why_it_matters": self._investor_why(theme_key),
                        "evidence_snippet": evidence or "No direct filing quote available in current snapshot.",
                        "filing_type": str(snapshot.filing_type or ""),
                        "filing_date": str(snapshot.filing_date or ""),
                        "impact_rank": self._impact_rank(theme_key, row.dimension_key),
                        "direction_rank": self._direction_rank("stable"),
                        "delta_mag": 0.0,
                    }
                )

            fallback_rows.sort(
                key=lambda item: (
                    -float(item["impact_rank"]),
                    -float(item["confidence"]),
                    item["title"].lower(),
                )
            )
            ranked = fallback_rows[:3]

        top_signals = [
            TickerTopSignal(
                title=item["title"],
                direction=item["direction"],
                why_it_matters=item["why_it_matters"],
                confidence_label=item["confidence_label"],
                evidence_snippet=item["evidence_snippet"],
                filing=TickerSignalFiling(type=item["filing_type"], date=item["filing_date"]),
            )
            for item in ranked
        ]

        change_candidates: list[tuple[float, str]] = []
        for item in ranked:
            direction = item["direction"]
            title = item["title"]
            delta_mag = float(item["delta_mag"])
            if direction == "new":
                change_candidates.append((1.0, f"New strategic signal: {title}."))
            elif direction == "increasing" and delta_mag >= 0.08:
                change_candidates.append((delta_mag, f"Increasing emphasis on {title}."))
            elif direction == "decreasing" and delta_mag >= 0.08:
                change_candidates.append((delta_mag, f"Decreasing emphasis on {title}."))

        seen_changes: set[str] = set()
        changes: list[TickerChange] = []
        for _, description in sorted(change_candidates, key=lambda row: (-row[0], row[1].lower())):
            if description in seen_changes:
                continue
            seen_changes.add(description)
            changes.append(TickerChange(description=description))
            if len(changes) == 3:
                break
        if not changes:
            changes = [TickerChange(description="No material strategic shift this period")]

        risk_response: list[TickerRiskResponse] = []
        seen_pairs: set[tuple[str, str]] = set()
        sorted_links = sorted(
            links.links,
            key=lambda row: (
                -float(row.confidence if row.confidence is not None else 0.0),
                -float(row.link_strength if row.link_strength is not None else 0.0),
                str(row.risk).lower(),
                str(row.response).lower(),
            ),
        )
        for row in sorted_links:
            risk = label_display_name(str(row.risk or "")) or str(row.risk or "").replace("_", " ").title()
            response = label_display_name(str(row.response or "")) or str(row.response or "").replace("_", " ").title()
            if not risk or not response:
                continue
            pair = (risk.lower(), response.lower())
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            risk_response.append(TickerRiskResponse(risk=risk, response=response))
            if len(risk_response) == 3:
                break

        summary = self._build_ticker_summary(
            ticker=normalized_ticker,
            top_signals=top_signals,
            risk_response=risk_response,
        )

        confidence_values = [float(item["confidence"]) for item in ranked]
        avg_confidence = (sum(confidence_values) / len(confidence_values)) if confidence_values else 0.0
        if avg_confidence >= 0.75 and len(ranked) >= 2:
            confidence_label = "Strong"
        elif avg_confidence >= 0.5 and len(ranked) >= 1:
            confidence_label = "Moderate"
        else:
            confidence_label = "Emerging"

        breadth = len(deduped) if deduped else len(ranked)
        if breadth >= 5:
            coverage_label = "High"
        elif breadth >= 3:
            coverage_label = "Medium"
        else:
            coverage_label = "Low"

        response = TickerIntelligenceResponse(
            summary=summary,
            top_signals=top_signals,
            changes=changes,
            risk_response=risk_response,
            confidence_coverage=TickerConfidenceCoverage(
                confidence=confidence_label,
                coverage=coverage_label,
            ),
        )
        self._ticker_intelligence_cache[normalized_ticker] = (
            now + self._ticker_intelligence_ttl_seconds,
            response,
        )
        return response

    def get_ui_ticker_intelligence(self, ticker: str) -> UiTickerIntelligenceResponse:
        normalized_ticker = ticker.upper().strip()
        precomputed = self._load_precomputed_ui_intelligence(normalized_ticker)
        if precomputed is not None:
            # Keep precomputed payloads fast, but patch in live risk-response links
            # when historical stored JSON is missing them.
            if not precomputed.risk_pairs:
                try:
                    links = self.get_strategy_response_links(normalized_ticker, limit=40, latest_only=True)
                    if not links.links:
                        links = self.get_strategy_response_links(normalized_ticker, limit=40, latest_only=False)
                    if links.links:
                        precomputed.risk_pairs = sorted(
                            links.links,
                            key=lambda row: (float(row.confidence), float(row.link_strength or 0)),
                            reverse=True,
                        )[:3]
                except Exception:
                    pass
            return precomputed

        snapshot = self.get_strategy_snapshot(ticker)
        dominant = self.get_dominant_themes(ticker, limit=5)
        # Keep this endpoint lightweight for UI latency and reliability.
        signals = self.get_strategy_signals(ticker, limit=80, latest_only=True, include_score_components=False)
        links = self.get_strategy_response_links(ticker, limit=40, latest_only=True)
        if not links.links:
            links = self.get_strategy_response_links(ticker, limit=40, latest_only=False)
        dominant_rows = list(dominant.dominant_themes or [])[:5]

        aggregated: dict[str, UiTheme] = {}

        def ensure_theme(theme_key: str, label: str | None = None, dimension_key: str | None = None) -> UiTheme:
            key = self._canonical_theme_key(theme_key)
            existing = aggregated.get(key)
            if existing is None:
                existing = UiTheme(
                    id=key,
                    theme_key=theme_key,
                    label=label or label_display_name(theme_key) or theme_key.replace("_", " ").title(),
                    dimension_key=dimension_key,
                    evidence=[],
                )
                aggregated[key] = existing
            return existing

        def add_evidence(theme: UiTheme, evidence: UiThemeEvidence) -> None:
            quote = (evidence.quote or "").strip()
            if not quote:
                return
            if not self._is_reasonable_evidence_quote(quote):
                return
            if not self._quote_matches_theme(theme.theme_key, quote):
                return
            if any(existing.quote.strip().lower() == quote.lower() for existing in theme.evidence):
                return
            theme.evidence.append(evidence)

        for row in dominant_rows:
            theme = ensure_theme(row.key, row.label, row.dimension_key)
            theme.score = max(theme.score or 0.0, float(row.score or 0.0))
            theme.dimension_key = theme.dimension_key or row.dimension_key
            if row.why_selected:
                cleaned_why = str(row.why_selected).strip()
                if cleaned_why and not self._is_generic_ui_text(cleaned_why):
                    if not theme.source_insight or len(cleaned_why) > len(theme.source_insight):
                        theme.source_insight = cleaned_why
            for quote in row.evidence_quotes or ([] if not row.evidence_quote else [row.evidence_quote]):
                cleaned = str(quote).strip()
                if not cleaned:
                    continue
                add_evidence(
                    theme,
                    UiThemeEvidence(
                        quote=cleaned,
                        filing_date=dominant.filing_date,
                        filing_type=dominant.filing_type,
                        source_kind="inferred" if row.evidence_source == "inferred_extraction" else "quote",
                    ),
                )
            if row.persistence_count is not None:
                theme.persistence_count = row.persistence_count
            if row.persistence_score is not None:
                theme.persistence_score = row.persistence_score

        for row in signals.signals:
            theme = ensure_theme(row.theme_key, row.theme_label, row.dimension_key)
            if row.current_score is not None:
                theme.score = max(theme.score or 0.0, float(row.current_score))
            elif row.confidence is not None:
                theme.score = max(theme.score or 0.0, float(row.confidence))
            theme.dimension_key = theme.dimension_key or row.dimension_key
            if row.direction and (theme.direction is None or (row.current_score or row.confidence or 0) >= (theme.score or 0)):
                theme.direction = row.direction
            if row.description:
                cleaned_description = str(row.description).strip()
                if cleaned_description and not self._is_generic_ui_text(cleaned_description):
                    if not theme.source_insight or len(cleaned_description) > len(theme.source_insight):
                        theme.source_insight = cleaned_description
            if row.delta is not None and (theme.delta is None or abs(row.delta) > abs(theme.delta)):
                theme.delta = row.delta
                theme.delta_severity = row.delta_severity
                theme.comparison_basis = row.comparison_basis
            if row.persistence_count is not None:
                theme.persistence_count = row.persistence_count
            if row.persistence_score is not None:
                theme.persistence_score = row.persistence_score
            if row.evidence_quote:
                quote = row.evidence_quote.strip()
                add_evidence(
                    theme,
                    UiThemeEvidence(
                        quote=quote,
                        filing_date=row.filing_date,
                        filing_type=row.filing_type,
                        source_kind="quote",
                    ),
                )

        for row in links.links:
            risk_key = self._canonical_theme_key(row.risk)
            response_key = self._canonical_theme_key(row.response)
            risk_theme = aggregated.get(risk_key)
            response_theme = aggregated.get(response_key)
            if risk_theme and row.evidence_quote_risk and not any(item.source_kind == "quote" for item in risk_theme.evidence):
                add_evidence(
                    risk_theme,
                    UiThemeEvidence(
                        quote=row.evidence_quote_risk,
                        filing_date=row.filing_date,
                        filing_type=row.filing_type,
                        source_kind="quote",
                    ),
                )
            if response_theme and row.evidence_quote_response and not any(item.source_kind == "quote" for item in response_theme.evidence):
                add_evidence(
                    response_theme,
                    UiThemeEvidence(
                        quote=row.evidence_quote_response,
                        filing_date=row.filing_date,
                        filing_type=row.filing_type,
                        source_kind="quote",
                    ),
                )

        themes = list(aggregated.values())
        qualified_themes: list[UiTheme] = []
        for theme in themes:
            # If evidence is missing or inferred-only, enrich with most recent direct quotes from history.
            has_direct_quote = any(item.source_kind == "quote" for item in theme.evidence)
            self._sort_theme_evidence(theme)
            if theme.evidence_count is None:
                theme.evidence_count = len(theme.evidence) if theme.evidence else None
            if self._is_displayable_ui_theme(theme):
                qualified_themes.append(theme)

        themes = qualified_themes
        themes.sort(key=lambda item: (float(item.score or 0.0), item.label.lower()), reverse=True)
        themes = themes[:7]

        if not themes and aggregated:
            fallback_themes = [
                row
                for row in aggregated.values()
                if float(row.score or 0.0) >= 0.45
                or (row.source_insight and not self._is_generic_ui_text(row.source_insight))
            ]
            for row in fallback_themes:
                self._sort_theme_evidence(row)
                if row.evidence_count is None:
                    row.evidence_count = len(row.evidence) if row.evidence else None
            fallback_themes.sort(key=lambda item: (float(item.score or 0.0), item.label.lower()), reverse=True)
            themes = fallback_themes[:5]

        if len(themes) < 2:
            present = {self._canonical_theme_key(row.theme_key) for row in themes}
            supplemental_rows = list(snapshot.dominant_themes or []) + list(snapshot.emerging_themes or [])
            for row in supplemental_rows:
                theme_key = str(row.key or "")
                canonical = self._canonical_theme_key(theme_key)
                if not canonical or canonical in present:
                    continue
                new_theme = UiTheme(
                    id=canonical,
                    theme_key=theme_key,
                    label=str(row.label or label_display_name(theme_key) or theme_key.replace("_", " ").title()),
                    dimension_key=row.dimension_key,
                    score=float(row.score or 0.45),
                    direction="stable",
                    source_insight=self._investor_why(theme_key),
                    evidence=[],
                    evidence_count=int(row.evidence_count or 0) if row.evidence_count is not None else None,
                    persistence_count=row.persistence_count,
                    persistence_score=row.persistence_score,
                )
                for quote in list(row.evidence_quotes or [])[:2]:
                    cleaned = self._sanitize_quote_for_theme(theme_key, str(quote))
                    if not cleaned:
                        continue
                    new_theme.evidence.append(
                        UiThemeEvidence(
                            quote=cleaned,
                            filing_date=snapshot.filing_date,
                            filing_type=snapshot.filing_type,
                            source_kind="quote",
                        )
                    )
                self._sort_theme_evidence(new_theme)
                if new_theme.evidence_count is None:
                    new_theme.evidence_count = len(new_theme.evidence) if new_theme.evidence else None
                themes.append(new_theme)
                present.add(canonical)
                if len(themes) >= 3:
                    break
            themes.sort(key=lambda item: (float(item.score or 0.0), item.label.lower()), reverse=True)
            themes = themes[:7]

        theme_index = {row.id: row for row in themes}
        key_moves: list[UiTheme] = []
        for dominant_theme in dominant_rows:
            key = self._canonical_theme_key(dominant_theme.key)
            row = theme_index.get(key)
            if row:
                key_moves.append(row)
        if not key_moves:
            key_moves = themes[:5]
        else:
            key_moves = key_moves[:5]

        risk_pairs = sorted(links.links, key=lambda row: (float(row.confidence), float(row.link_strength or 0)), reverse=True)[:3]
        filing_date = str(snapshot.filing_date or dominant.filing_date or "")
        filing_type = str(snapshot.filing_type or dominant.filing_type or "")
        narrative = self._build_ui_narrative(ticker=snapshot.ticker, themes=themes, risk_pairs=risk_pairs)
        payload = self._normalize_precomputed_ui_payload(
            payload={
                "ticker": snapshot.ticker,
                "filing_date": filing_date,
                "filing_type": filing_type,
                "narrative": narrative,
                "summary": narrative,
                "strategic_arc": narrative,
                "current_focus": narrative,
                "themes": [theme.model_dump() if hasattr(theme, "model_dump") else dict(theme) for theme in themes],
                "key_moves": [theme.model_dump() if hasattr(theme, "model_dump") else dict(theme) for theme in key_moves],
                "risk_pairs": [pair.model_dump() if hasattr(pair, "model_dump") else dict(pair) for pair in risk_pairs],
            },
            ticker=snapshot.ticker,
            filing_date=filing_date,
            filing_type=filing_type,
        )
        return self._validate_model(UiTickerIntelligenceResponse, payload)

    def search_companies(self, query: str, limit: int) -> CompanySearchResponse:
        rows = self.repo.search_companies(query, limit=limit)
        items = [
            CompanySearchResult(
                ticker=str(row.get("ticker", "")).upper(),
                name=str(row.get("name", "")),
            )
            for row in rows
            if row.get("ticker")
        ]
        return CompanySearchResponse(items=items)

    def get_strategy_snapshot(self, ticker: str) -> ClientStrategySnapshotResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])

        snapshot = self.repo.get_latest_strategy_snapshot(company_id)
        score_rows: list[dict[str, Any]] = []
        score_components_map: dict[tuple[str, str], dict[str, Any]] = {}

        if snapshot and snapshot.get("filing_id"):
            filing_id = int(snapshot["filing_id"])
            score_rows = self.repo.list_company_strategy_scores_for_filing(company_id, filing_id)
            extraction_rows = self.repo.list_strategy_extractions_for_filing(filing_id)
            score_components_map = self._build_score_component_map(extraction_rows)

        if not snapshot:
            snapshot, score_rows, score_components_map = self._build_fallback_snapshot(company_id)

        if not snapshot:
            raise NotFoundError(f"No strategy snapshot for {ticker.upper()}")

        score_map = {str(row.get("theme_key", "")): row for row in score_rows}
        persistence_map = self._build_persistence_map(company_id)

        def build_theme_list(keys: list[str]) -> list[ClientStrategyThemeLabel]:
            out: list[ClientStrategyThemeLabel] = []
            for key in keys:
                if not key:
                    continue
                meta = score_map.get(key, {})
                dimension_key = meta.get("dimension_key")
                persistence = persistence_map.get((str(dimension_key or ""), key), {})
                out.append(
                    ClientStrategyThemeLabel(
                        key=key,
                        label=label_display_name(key),
                        dimension_key=str(dimension_key) if dimension_key else None,
                        score=float(meta.get("score", 0) or 0) if meta.get("score") is not None else None,
                        evidence_count=int(meta.get("evidence_count", 0) or 0),
                        evidence_quotes=list(meta.get("evidence_quotes") or []),
                        persistence_count=persistence.get("persistence_count"),
                        persistence_score=persistence.get("persistence_score"),
                        score_components=None,
                    )
                )
            return out

        return ClientStrategySnapshotResponse(
            ticker=str(company.get("ticker", ticker.upper())),
            filing_date=str(snapshot.get("filing_date", "")),
            filing_type=str(snapshot.get("filing_type", "")),
            dominant_themes=build_theme_list(list(snapshot.get("dominant_themes") or [])),
            emerging_themes=build_theme_list(list(snapshot.get("emerging_themes") or [])),
            declining_themes=build_theme_list(list(snapshot.get("declining_themes") or [])),
        )

    def get_dominant_themes(self, ticker: str, limit: int) -> ClientDominantThemesResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])

        snapshot = self.repo.get_latest_strategy_snapshot(company_id)
        if not snapshot:
            raise NotFoundError(f"No strategy snapshot for {ticker.upper()}")
        filing_id = int(snapshot.get("filing_id", 0) or 0)
        if not filing_id:
            raise NotFoundError(f"No filing found for latest snapshot of {ticker.upper()}")

        score_rows = self.repo.list_company_strategy_scores_for_filing(company_id, filing_id)
        if not score_rows:
            raise NotFoundError(f"No strategy scores for latest snapshot of {ticker.upper()}")
        extraction_rows = self.repo.list_strategy_extractions_for_filing(filing_id)
        taxonomy_decision_map = self._build_taxonomy_decision_map(extraction_rows)

        previous_scores = self.repo.list_company_strategy_scores_all(company_id, limit=1200)
        previous_scores = [
            row
            for row in previous_scores
            if str(row.get("filing_date", "")) < str(snapshot.get("filing_date", ""))
        ]
        persistence_map = self._build_persistence_map(company_id)
        ranked = self._rank_dominant_themes(score_rows=score_rows, previous_scores=previous_scores)[:limit]

        themes: list[ClientDominantTheme] = []
        for row in ranked:
            dimension_key = row.get("dimension_key")
            theme_key = str(row.get("theme_key", ""))
            persistence = persistence_map.get((str(dimension_key or ""), theme_key), {})
            taxonomy_decision = taxonomy_decision_map.get((str(dimension_key or ""), theme_key), {})
            themes.append(
                ClientDominantTheme(
                    key=theme_key,
                    label=label_display_name(theme_key),
                    dimension_key=str(dimension_key) if dimension_key else None,
                    score=float(row.get("dominant_score", 0) or 0),
                    strength=self._dominant_strength(float(row.get("dominant_score", 0) or 0)),
                    evidence_quote=taxonomy_decision.get("evidence_quote"),
                    evidence_quotes=taxonomy_decision.get("evidence_quotes"),
                    evidence_source=taxonomy_decision.get("evidence_source"),
                    why_selected=self._public_justification(
                        theme_label=label_display_name(theme_key),
                        reason=taxonomy_decision.get("why_selected"),
                    ),
                    persistence_count=persistence.get("persistence_count"),
                    persistence_score=persistence.get("persistence_score"),
                    score_components=None,
                )
            )

        return ClientDominantThemesResponse(
            ticker=str(company.get("ticker", ticker.upper())),
            filing_date=str(snapshot.get("filing_date", "")),
            filing_type=str(snapshot.get("filing_type", "")),
            dominant_themes=themes,
        )

    def get_strategy_trends(self, ticker: str, theme_key: str | None, limit: int) -> ClientStrategyTrendsResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])

        rows = self.repo.list_strategy_trends(company_id, theme_key=theme_key, limit=limit)
        if not rows:
            rows = self._build_fallback_trends(company_id, limit=limit)

        allowed_themes = flatten_allowed_theme_keys()
        allowed_dimensions = flatten_allowed_dimensions()

        if theme_key:
            series = [
                ClientStrategyTrendPoint(
                    quarter=str(row.get("quarter", "")),
                    score=float(row.get("score", 0) or 0),
                )
                for row in rows
                if str(row.get("theme_key", "")) in allowed_themes
                and str(row.get("dimension_key", "")) in allowed_dimensions
            ]
            return ClientStrategyTrendsResponse(
                ticker=str(company.get("ticker", ticker.upper())),
                theme_key=theme_key,
                series=self._smooth_series(series),
                themes=[],
            )

        theme_map: dict[tuple[str, str], list[ClientStrategyTrendPoint]] = {}
        for row in rows:
            theme = str(row.get("theme_key", ""))
            dimension = str(row.get("dimension_key", ""))
            if theme not in allowed_themes or dimension not in allowed_dimensions:
                continue
            key = (dimension, theme)
            theme_map.setdefault(key, []).append(
                ClientStrategyTrendPoint(
                    quarter=str(row.get("quarter", "")),
                    score=float(row.get("smoothed_score", row.get("score", 0)) or 0),
                )
            )

        themes = [
            ClientStrategyTrendSeries(
                theme_key=theme,
                dimension_key=dimension,
                label=label_display_name(theme),
                series=self._smooth_series(series),
            )
            for (dimension, theme), series in theme_map.items()
        ]
        return ClientStrategyTrendsResponse(
            ticker=str(company.get("ticker", ticker.upper())),
            theme_key=None,
            series=[],
            themes=themes,
        )

    def get_strategy_signals(
        self,
        ticker: str,
        limit: int,
        latest_only: bool,
        include_score_components: bool = True,
    ) -> ClientStrategySignalsResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])

        rows = self.repo.list_company_strategy_signals(company_id, limit=limit)
        if latest_only and rows:
            latest_date = max(str(row.get("filing_date", "")) for row in rows)
            rows = [row for row in rows if str(row.get("filing_date", "")) == latest_date]

        drift_rows = self.repo.list_strategy_drift_events(company_id, limit=max(200, limit * 4))
        if not drift_rows:
            drift_rows = self._build_fallback_drift_rows(company_id)

        if not rows:
            rows = self._build_fallback_signal_rows(company_id, drift_rows, limit=limit)

        drift_map: dict[tuple[int, str, str], dict[str, Any]] = {}
        previous_filing_ids: set[int] = set()
        for row in drift_rows:
            filing_id = int(row.get("filing_id", 0) or 0)
            dimension_key = str(row.get("dimension_key", ""))
            theme = str(row.get("theme_key", ""))
            if not filing_id or not dimension_key or not theme:
                continue
            drift_map[(filing_id, dimension_key, theme)] = row
            previous_filing_id = row.get("previous_filing_id")
            if previous_filing_id is not None:
                previous_filing_ids.add(int(previous_filing_id))

        previous_filings = self.repo.list_filings_by_ids(sorted(previous_filing_ids))
        persistence_map = self._build_persistence_map(company_id)
        score_component_map_by_filing: dict[int, dict[tuple[str, str], dict[str, Any]]] = {}
        if include_score_components:
            extraction_map = self._company_extractions_by_filing(company_id, limit=3000)
            filing_ids = sorted({int(row.get("filing_id", 0) or 0) for row in rows if row.get("filing_id")})
            for filing_id in filing_ids:
                extraction_rows = extraction_map.get(filing_id, [])
                score_component_map_by_filing[filing_id] = self._build_score_component_map(extraction_rows)

        signals: list[ClientStrategySignal] = []
        for row in rows:
            theme_key = str(row.get("theme_key", ""))
            filing_id = int(row.get("filing_id", 0) or 0) or None
            dimension_key = str(row.get("dimension_key", "")) or None
            label = label_display_name(theme_key)
            direction = str(row.get("direction", ""))
            title = str(row.get("signal_title", "")) or f"{label} {direction}".strip()
            description = str(row.get("signal_description", "")) or f"Recent filings indicate {label} is {direction}."

            drift_row = drift_map.get((filing_id or 0, dimension_key or "", theme_key))
            delta = float(drift_row.get("delta", 0) or 0) if drift_row else None
            previous_score = float(drift_row.get("previous_score", 0) or 0) if drift_row else None
            current_score = float(drift_row.get("current_score", 0) or 0) if drift_row else None

            previous_filing_type = None
            if drift_row and drift_row.get("previous_filing_id") is not None:
                prev_filing = previous_filings.get(int(drift_row["previous_filing_id"]))
                previous_filing_type = str((prev_filing or {}).get("filing_type", "")) or None

            persistence = persistence_map.get((dimension_key or "", theme_key), {})
            score_components = None
            if include_score_components and filing_id:
                score_components = score_component_map_by_filing.get(filing_id or 0, {}).get((dimension_key or "", theme_key))

            signals.append(
                ClientStrategySignal(
                    type="drift",
                    theme_key=theme_key,
                    theme_label=label,
                    dimension_key=dimension_key,
                    filing_id=filing_id,
                    direction=direction or None,
                    confidence=float(row.get("confidence", 0) or 0),
                    title=title,
                    description=description,
                    evidence_summary=row.get("evidence_summary"),
                    filing_date=str(row.get("filing_date", "")),
                    filing_type=str(row.get("filing_type", "")),
                    evidence_quote=row.get("evidence_quote"),
                    current_score=current_score,
                    previous_score=previous_score,
                    delta=delta,
                    delta_severity=self._delta_severity(delta),
                    comparison_basis=self._comparison_basis(previous_filing_type),
                    persistence_count=persistence.get("persistence_count"),
                    persistence_score=persistence.get("persistence_score"),
                    score_components=None,
                )
            )

        return ClientStrategySignalsResponse(
            ticker=str(company.get("ticker", ticker.upper())),
            signals=signals,
        )

    def get_strategy_response_links(self, ticker: str, limit: int, latest_only: bool) -> ClientStrategyResponseLinksResponse:
        company = self.repo.get_company_by_ticker(ticker)
        company_id = int(company["id"])

        rows = self.repo.list_strategy_response_links(company_id, limit=limit)
        if not rows:
            rows = self._build_fallback_response_links(company_id)

        if latest_only and rows:
            latest_date = max(str(row.get("filing_date", "")) for row in rows)
            rows = [row for row in rows if str(row.get("filing_date", "")) == latest_date]

        links = []
        for row in rows:
            risk_key = str(row.get("risk_theme_key", ""))
            response_key = str(row.get("response_theme_key", ""))
            risk_label = label_display_name(risk_key)
            response_label = label_display_name(response_key)
            risk_delta = float(row.get("risk_delta", 0) or 0)
            response_delta = float(row.get("response_delta", 0) or 0)
            confidence = float(row.get("confidence", 0) or 0)
            link_strength = float(row.get("link_strength", 0) or 0)

            links.append(
                {
                    "risk": risk_key,
                    "response": response_key,
                    "direction": "mitigation",
                    "quarter": str(row.get("quarter", "")),
                    "confidence": confidence,
                    "link_strength": link_strength,
                    "summary": self._response_link_summary(
                        risk_label=risk_label,
                        response_label=response_label,
                        risk_delta=risk_delta,
                        response_delta=response_delta,
                        link_strength=link_strength,
                    ),
                    "filing_date": str(row.get("filing_date", "")),
                    "filing_type": str(row.get("filing_type", "")),
                    "risk_score": float(row.get("risk_score", 0) or 0),
                    "response_score": float(row.get("response_score", 0) or 0),
                    "risk_delta": risk_delta,
                    "response_delta": response_delta,
                    "evidence_quote_risk": row.get("evidence_quote_risk"),
                    "evidence_quote_response": row.get("evidence_quote_response"),
                    "confidence_reason": self._response_link_confidence_reason(
                        confidence=confidence,
                        risk_delta=risk_delta,
                        response_delta=response_delta,
                        link_strength=link_strength,
                    ),
                }
            )

        return ClientStrategyResponseLinksResponse(
            ticker=str(company.get("ticker", ticker.upper())),
            links=links,
        )

    def _shareable_ticker_from_payload(
        self,
        *,
        ticker: str,
        name: str,
        payload: dict[str, Any],
        filing_date: str,
        filing_type: str,
        updated_at: str | None,
    ) -> UiShareableTicker | None:
        confidence_coverage = dict(payload.get("confidence_coverage") or {})
        confidence_label = str(
            confidence_coverage.get("confidence")
            or (payload.get("snapshot_metadata") or {}).get("confidence_label")
            or "Emerging"
        )
        if confidence_label not in {"Strong", "Moderate", "Emerging"}:
            confidence_label = "Emerging"

        coverage_label = str(confidence_coverage.get("coverage") or "")
        if coverage_label not in {"High", "Medium", "Low"}:
            coverage_label = "Low"

        top_signals = list(payload.get("top_current_signals") or payload.get("top_signals") or [])
        themes = list(payload.get("key_moves") or payload.get("themes") or [])
        evolution = list(payload.get("strategy_evolution") or payload.get("history_timeline") or [])
        risk_pairs = list(payload.get("risk_pairs") or payload.get("risk_response") or [])
        narrative = str(
            payload.get("summary")
            or payload.get("overall_strategy_story")
            or payload.get("strategic_arc")
            or payload.get("narrative")
            or ""
        ).strip()

        signal_labels: list[str] = []
        for row in top_signals[:3]:
            if isinstance(row, dict):
                title = str(row.get("title") or row.get("theme_label") or "").strip()
                if title and title not in signal_labels:
                    signal_labels.append(title)
        if not signal_labels:
            for row in themes[:3]:
                if isinstance(row, dict):
                    label = str(row.get("label") or row.get("theme_key") or row.get("id") or "").strip()
                    if label and label not in signal_labels:
                        signal_labels.append(label_display_name(label) if "_" in label else label)

        if not narrative and not signal_labels:
            return None

        confidence_points = {"Strong": 34.0, "Moderate": 24.0, "Emerging": 12.0}[confidence_label]
        coverage_points = {"High": 24.0, "Medium": 16.0, "Low": 7.0}[coverage_label]
        signal_points = min(len(signal_labels), 3) * 8.0
        evidence_points = min(len(evolution), 4) * 3.0 + min(len(risk_pairs), 2) * 3.0
        narrative_points = min(len(narrative.split()), 45) / 45.0 * 14.0 if narrative else 0.0
        freshness_points = 8.0 if filing_date else 0.0
        readiness = round(
            min(100.0, confidence_points + coverage_points + signal_points + evidence_points + narrative_points + freshness_points),
            1,
        )

        if readiness < 38.0:
            return None

        headline = narrative or f"{ticker} has a shareable strategy read built from recent filing signals."
        headline = self._evidence_snippet(headline)
        if len(headline) > 170:
            headline = headline[:167].rstrip(" ,;:.") + "..."

        if confidence_label == "Strong" and len(signal_labels) >= 2:
            reason = "High-confidence read with multiple client-friendly signals."
        elif risk_pairs:
            reason = "Useful now because the risk and response pattern is explicit."
        elif len(evolution) >= 2:
            reason = "Good client share because the filing story has enough history."
        else:
            reason = "Shareable as a focused current-strategy snapshot."

        return UiShareableTicker(
            ticker=ticker,
            name=name,
            filing_date=filing_date or None,
            filing_type=filing_type or None,
            readiness_score=readiness,
            confidence_label=confidence_label,
            coverage_label=coverage_label,
            headline=headline,
            reason=reason,
            signals=signal_labels[:3],
            updated_at=updated_at,
        )

    def _response_link_summary(
        self,
        *,
        risk_label: str,
        response_label: str,
        risk_delta: float,
        response_delta: float,
        link_strength: float,
    ) -> str:
        risk_text = risk_label or "Risk pressure"
        response_text = response_label or "management action"

        if risk_delta >= 0.08 and response_delta >= 0.08:
            return f"{risk_text} is becoming more visible, and the company appears to be leaning further into {response_text} rather than stepping back."
        if risk_delta >= 0.08 and response_delta > 0:
            return f"{risk_text} is rising, with the company still responding through {response_text}."
        if risk_delta >= 0.08 and response_delta <= 0:
            return f"{risk_text} is rising, but the response is not strengthening alongside it."
        if risk_delta > 0 and response_delta >= 0.08:
            return f"The company still appears to be using {response_text} to absorb pressure from {risk_text}."
        if response_delta >= 0.08:
            return f"{response_text} is becoming more explicit as the company’s answer to {risk_text}."
        if link_strength >= 0.6:
            return f"{risk_text} and {response_text} are appearing together as a visible pressure-and-response pattern."
        return f"{response_text} looks like the clearest company response to {risk_text} in the current filing window."

    def create_feedback(
        self,
        payload: FeedbackCreateRequest,
        *,
        user_agent: str | None = None,
    ) -> FeedbackCreateResponse:
        row = self.repo.create_feedback(
            rating=payload.rating,
            tags=payload.tags,
            note=payload.note.strip() if isinstance(payload.note, str) else None,
            path=payload.path.strip() if isinstance(payload.path, str) else None,
            source=payload.source.strip() if isinstance(payload.source, str) else None,
            submitted_at=payload.submitted_at,
            user_agent=user_agent,
        )
        feedback_id = int(row.get("id", 0) or 0)
        return FeedbackCreateResponse(status="ok", feedback_id=feedback_id)

    def _build_fallback_snapshot(self, company_id: int) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
        history_rows = self.repo.list_company_strategy_scores_all(company_id, limit=1200)
        if history_rows:
            history_rows.sort(key=lambda row: str(row.get("filing_date", "")), reverse=True)
            latest_filing_id = int(history_rows[0].get("filing_id", 0) or 0)
            latest_rows = [coerce_theme_row(row) | {"dimension_key": row.get("dimension_key"), "theme_key": row.get("theme_key")} for row in history_rows if int(row.get("filing_id", 0) or 0) == latest_filing_id]
            latest_date = str(history_rows[0].get("filing_date", ""))
            latest_type = str(history_rows[0].get("filing_type", ""))

            previous_rows = [row for row in history_rows if int(row.get("filing_id", 0) or 0) != latest_filing_id]
            previous_by_theme: dict[tuple[str, str], float] = {}
            for row in previous_rows:
                key = (str(row.get("dimension_key", "")), str(row.get("theme_key", "")))
                if key in previous_by_theme:
                    continue
                previous_by_theme[key] = float(row.get("score", 0) or 0)

            score_rows = [
                {
                    "dimension_key": row.get("dimension_key"),
                    "theme_key": row.get("theme_key"),
                    "score": float(row.get("score", 0) or 0),
                    "evidence_count": int(row.get("evidence_count", 0) or 0),
                    "evidence_quotes": list(row.get("evidence_quotes") or []),
                }
                for row in latest_rows
            ]
            snapshot = self._snapshot_from_scores(
                company_id=company_id,
                filing_id=latest_filing_id,
                filing_type=latest_type,
                filing_date=latest_date,
                current_rows=score_rows,
                previous_by_theme=previous_by_theme,
            )
            return snapshot, score_rows, {}

        # Final fallback uses section-level taxonomy for latest filing.
        extractions = self.repo.list_company_extractions(company_id, limit=1200)
        if not extractions:
            return None, [], {}

        latest = sorted(extractions, key=lambda row: str((row.get("sections") or {}).get("filings", {}).get("filing_date", "")), reverse=True)[0]
        latest_filing = (latest.get("sections") or {}).get("filings") or {}
        latest_filing_id = int(latest_filing.get("id", 0) or 0)
        latest_rows = [row for row in extractions if int(((row.get("sections") or {}).get("filings") or {}).get("id", 0) or 0) == latest_filing_id]
        score_rows = self._scores_from_section_taxonomy(latest_rows)
        snapshot = self._snapshot_from_scores(
            company_id=company_id,
            filing_id=latest_filing_id,
            filing_type=str(latest_filing.get("filing_type", "")),
            filing_date=str(latest_filing.get("filing_date", "")),
            current_rows=score_rows,
            previous_by_theme={},
        )
        return snapshot, score_rows, self._build_score_component_map(latest_rows)

    def _scores_from_section_taxonomy(self, extraction_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        extraction_ids = [int(row.get("id", 0) or 0) for row in extraction_rows if row.get("id") is not None]
        score_rows = self.repo.list_section_taxonomy_scores(extraction_ids)
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        evidence_map: dict[tuple[str, str], list[str]] = defaultdict(list)

        for row in extraction_rows:
            extracted = row.get("extracted_data") or {}
            taxonomy = extracted.get("taxonomy") or {}
            if not isinstance(taxonomy, dict):
                continue
            for dimension, labels in taxonomy.items():
                if not isinstance(labels, list):
                    continue
                for item in labels:
                    if not isinstance(item, dict):
                        continue
                    label = str(item.get("label", ""))
                    if not label:
                        continue
                    quotes = item.get("evidence_quotes") or []
                    if isinstance(quotes, list):
                        evidence_map[(dimension, label)].extend([str(q) for q in quotes if isinstance(q, str)])
                    quote = item.get("evidence_quote")
                    if isinstance(quote, str) and quote:
                        evidence_map[(dimension, label)].append(quote)

        for row in score_rows:
            dimension = str(row.get("dimension_key", ""))
            theme = str(row.get("label_key", ""))
            key = (dimension, theme)
            score = float(row.get("score", 0) or 0)
            existing = grouped.get(key)
            if existing is None or score > float(existing.get("score", 0) or 0):
                grouped[key] = {
                    "dimension_key": dimension,
                    "theme_key": theme,
                    "score": score,
                    "evidence_count": 0,
                    "evidence_quotes": [],
                }

        for key, payload in grouped.items():
            quotes = []
            for quote in evidence_map.get(key, []):
                if quote and quote not in quotes:
                    quotes.append(quote)
            payload["evidence_quotes"] = quotes[:5]
            payload["evidence_count"] = len(payload["evidence_quotes"])

        rows = list(grouped.values())
        rows.sort(key=lambda row: float(row.get("score", 0) or 0), reverse=True)
        return rows

    def _snapshot_from_scores(
        self,
        company_id: int,
        filing_id: int,
        filing_type: str,
        filing_date: str,
        current_rows: list[dict[str, Any]],
        previous_by_theme: dict[tuple[str, str], float],
    ) -> dict[str, Any]:
        sorted_scores = sorted(current_rows, key=lambda row: float(row.get("score", 0) or 0), reverse=True)
        dominant = [str(row.get("theme_key", "")) for row in sorted_scores[:3] if row.get("theme_key")]

        emerging: list[str] = []
        declining: list[str] = []
        for row in current_rows:
            dimension = str(row.get("dimension_key", ""))
            theme = str(row.get("theme_key", ""))
            if not theme:
                continue
            prev_score = previous_by_theme.get((dimension, theme))
            if prev_score is None:
                continue
            delta = float(row.get("score", 0) or 0) - float(prev_score)
            if delta >= 0.08:
                emerging.append(theme)
            elif delta <= -0.08:
                declining.append(theme)

        return {
            "company_id": company_id,
            "filing_id": filing_id,
            "filing_type": filing_type,
            "filing_date": filing_date,
            "dominant_themes": dominant,
            "emerging_themes": emerging[:3],
            "declining_themes": declining[:3],
        }

    def _build_fallback_trends(self, company_id: int, limit: int) -> list[dict[str, Any]]:
        rows = self.repo.list_company_strategy_scores_all(company_id, limit=limit)
        out = []
        for row in rows:
            filing_date = str(row.get("filing_date", ""))
            out.append(
                {
                    "filing_id": row.get("filing_id"),
                    "filing_date": filing_date,
                    "quarter": str(row.get("quarter") or quarter_from_date(filing_date)),
                    "score": float(row.get("score", 0) or 0),
                    "smoothed_score": float(row.get("score", 0) or 0),
                    "theme_key": str(row.get("theme_key", "")),
                    "dimension_key": str(row.get("dimension_key", "")),
                }
            )
        out.sort(key=lambda row: str(row.get("quarter", "")))
        return out

    def _rank_dominant_themes(
        self,
        *,
        score_rows: list[dict[str, Any]],
        previous_scores: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        history: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in previous_scores:
            key = (str(row.get("dimension_key", "")), str(row.get("theme_key", "")))
            if not key[0] or not key[1]:
                continue
            history[key].append(row)
        for key in history:
            history[key].sort(key=lambda r: str(r.get("filing_date", "")), reverse=True)

        out: list[dict[str, Any]] = []
        for row in score_rows:
            dimension = str(row.get("dimension_key", ""))
            theme = str(row.get("theme_key", ""))
            if not dimension or not theme:
                continue
            base_score = float(row.get("score", 0) or 0)
            quotes = [str(q) for q in list(row.get("evidence_quotes") or []) if isinstance(q, str)]
            prior_rows = history.get((dimension, theme), [])
            prior_above = sum(1 for pr in prior_rows if float(pr.get("score", 0) or 0) >= 0.2)
            frequency_score = min(1.0, (prior_above + (1 if base_score >= 0.2 else 0)) / 6.0)

            persistence_count = 1 if base_score >= 0.2 else 0
            for pr in prior_rows[:3]:
                if float(pr.get("score", 0) or 0) >= 0.2:
                    persistence_count += 1
                else:
                    break
            persistence_score = min(1.0, persistence_count / 4.0)

            boost = HIGH_SIGNAL_THEME_BOOSTS.get(theme, 1.0)
            penalty = 1.0
            if theme in GENERIC_THEME_KEYS and not self._is_concrete_generic_theme(theme, quotes):
                penalty = 0.72

            raw = (0.55 * base_score) + (0.25 * persistence_score) + (0.2 * frequency_score)
            dominant_score = max(0.0, min(1.0, raw * boost * penalty))
            out.append(
                {
                    "dimension_key": dimension,
                    "theme_key": theme,
                    "dominant_score": round(dominant_score, 3),
                    "base_score": round(base_score, 3),
                    "persistence_score": round(persistence_score, 3),
                    "frequency_score": round(frequency_score, 3),
                    "theme_boost": round(boost, 3),
                    "generic_penalty": round(penalty, 3),
                }
            )
        out.sort(key=lambda r: float(r.get("dominant_score", 0) or 0), reverse=True)
        return out

    def _is_concrete_generic_theme(self, theme_key: str, quotes: list[str]) -> bool:
        text = " ".join(q.lower() for q in quotes)
        if not text:
            return False
        if any(ch.isdigit() for ch in text):
            return True
        if theme_key == "international_expansion":
            geo_terms = [
                "international",
                "global",
                "europe",
                "asia",
                "apac",
                "emea",
                "latin america",
                "united states",
                "u.s.",
                "china",
                "india",
                "japan",
            ]
            action_terms = ["expand", "expansion", "enter", "launch", "open", "build", "invest", "scale", "grow"]
            return any(t in text for t in geo_terms) and any(t in text for t in action_terms)
        if theme_key == "cost_efficiency":
            action_terms = ["reduce", "optimization", "optimiz", "restructure", "automation", "efficien", "productivity"]
            object_terms = ["cost", "expense", "headcount", "fulfillment", "logistics", "operations", "procurement"]
            return any(t in text for t in action_terms) and any(t in text for t in object_terms)
        return True

    def _dominant_strength(self, score: float) -> str:
        if score >= 0.8:
            return "strong"
        if score >= 0.55:
            return "moderate"
        return "emerging"

    def _public_justification(self, *, theme_label: str, reason: str | None) -> str:
        raw = (reason or "").strip()
        if not raw:
            return f"Recent filing language consistently supports emphasis on {theme_label.lower()}."
        lowered = raw.lower()
        blocked_terms = [
            "taxonomy",
            "label",
            "classifier",
            "model",
            "score",
            "threshold",
            "keyword",
            "gate",
            "prompt",
            "schema",
            "heuristic strategy fallback",
            "retrieved/field evidence",
            "pass a",
            "pass b",
        ]
        if any(term in lowered for term in blocked_terms):
            return f"Recent filing language consistently supports emphasis on {theme_label.lower()}."
        return raw

    def _build_taxonomy_decision_map(self, extraction_rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
        out: dict[tuple[str, str], dict[str, Any]] = {}
        keyword_maps = {
            "strategy_direction": DIRECTION_KEYWORDS,
            "strategy_action": ACTION_KEYWORDS,
            "risk_posture": RISK_POSTURE_KEYWORDS,
        }

        def split_sentences(text: str) -> list[str]:
            raw = (text or "").replace("\n", " ")
            chunks = [part.strip() for part in raw.replace("?", ".").replace("!", ".").split(".")]
            return [chunk for chunk in chunks if len(chunk.split()) >= 5]

        def best_sentence_for_theme(theme: str, dimension: str, final_payload: dict[str, Any]) -> str | None:
            allowed_fields = (
                ["risk_signals"]
                if dimension == "risk_posture"
                else ["growth_strategy", "cost_strategy", "innovation_strategy", "geographic_expansion", "capital_allocation"]
            )
            terms = [t.lower() for t in keyword_maps.get(dimension, {}).get(theme, [])]
            candidates: list[str] = []
            for field in allowed_fields:
                value = final_payload.get(field)
                if not isinstance(value, str) or not value.strip():
                    continue
                candidates.extend(split_sentences(value))
            if not candidates:
                return None
            if terms:
                ranked = sorted(
                    candidates,
                    key=lambda sentence: sum(1 for term in terms if term in sentence.lower()),
                    reverse=True,
                )
                if ranked and sum(1 for term in terms if term in ranked[0].lower()) > 0:
                    return ranked[0]
            return max(candidates, key=len)

        for row in extraction_rows:
            extracted = row.get("extracted_data") or {}
            taxonomy = extracted.get("taxonomy") or {}
            final_payload = extracted.get("final") or {}
            if not isinstance(taxonomy, dict):
                continue
            for dimension, labels in taxonomy.items():
                if not isinstance(labels, list):
                    continue
                for label_row in labels:
                    if not isinstance(label_row, dict):
                        continue
                    theme_key = str(label_row.get("label", "")).strip()
                    if not theme_key:
                        continue
                    key = (str(dimension), theme_key)
                    try:
                        score = float(label_row.get("score", 0) or 0)
                    except Exception:
                        score = 0.0
                    existing = out.get(key)
                    if existing is not None and score <= float(existing.get("_score", 0) or 0):
                        continue

                    evidence_quote = str(label_row.get("evidence_quote", "")).strip() or None
                    evidence_quotes = [
                        str(q).strip()
                        for q in (label_row.get("evidence_quotes") or [])
                        if isinstance(q, str) and str(q).strip()
                    ]
                    evidence_quotes = [
                        q for q in evidence_quotes
                        if self._sanitize_quote_for_theme(theme_key, q) is not None
                    ]
                    evidence_quote = self._sanitize_quote_for_theme(theme_key, evidence_quote)
                    if evidence_quote and evidence_quote not in evidence_quotes:
                        evidence_quotes = [evidence_quote, *evidence_quotes]

                    out[key] = {
                        "evidence_quote": evidence_quote,
                        "evidence_quotes": evidence_quotes[:3],
                        "evidence_source": "taxonomy_quote" if evidence_quote else None,
                        "why_selected": str(label_row.get("why_selected", "")).strip() or None,
                        "_score": score,
                    }

        for value in out.values():
            value.pop("_score", None)
        return out

    def _sanitize_quote_for_theme(self, theme_key: str, quote: str | None) -> str | None:
        text = " ".join((quote or "").split()).strip()
        if not text:
            return None

        words = text.split()
        if len(words) < 6 or len(words) > 90:
            return None

        lowered = text.lower()
        boilerplate_markers = [
            "item 1a. risk factors",
            "these risk factors do not identify all risks",
            "investment in our securities",
            "material adverse effect on our business",
            "events and consequences discussed in these risk factors",
        ]
        if any(marker in lowered for marker in boilerplate_markers):
            return None

        canonical = self._canonical_theme_key(theme_key)
        terms = (
            DIRECTION_KEYWORDS.get(canonical, [])
            or ACTION_KEYWORDS.get(canonical, [])
            or RISK_POSTURE_KEYWORDS.get(canonical, [])
        )
        if terms and not any(term.lower() in lowered for term in terms):
            return None

        return text

    def _build_fallback_drift_rows(self, company_id: int) -> list[dict[str, Any]]:
        rows = self.repo.list_company_strategy_scores_all(company_id, limit=1200)
        if not rows:
            return []

        rows.sort(key=lambda row: str(row.get("filing_date", "")), reverse=True)
        by_filing: dict[int, list[dict[str, Any]]] = defaultdict(list)
        filing_order: list[int] = []
        for row in rows:
            filing_id = int(row.get("filing_id", 0) or 0)
            if filing_id not in by_filing:
                filing_order.append(filing_id)
            by_filing[filing_id].append(row)

        output: list[dict[str, Any]] = []
        for idx in range(len(filing_order) - 1):
            current_filing_id = filing_order[idx]
            previous_filing_id = filing_order[idx + 1]
            current_rows = by_filing[current_filing_id]
            previous_rows = by_filing[previous_filing_id]

            prev_map = {
                (str(row.get("dimension_key", "")), str(row.get("theme_key", ""))): row
                for row in previous_rows
            }
            for current in current_rows:
                key = (str(current.get("dimension_key", "")), str(current.get("theme_key", "")))
                prev = prev_map.get(key)
                if not prev:
                    continue
                prev_score = float(prev.get("score", 0) or 0)
                curr_score = float(current.get("score", 0) or 0)
                delta = round(curr_score - prev_score, 3)
                if delta >= 0.08:
                    direction = "increasing"
                elif delta <= -0.08:
                    direction = "decreasing"
                else:
                    direction = "stable"
                output.append(
                    {
                        "filing_id": current_filing_id,
                        "previous_filing_id": previous_filing_id,
                        "filing_date": str(current.get("filing_date", "")),
                        "filing_type": str(current.get("filing_type", "")),
                        "dimension_key": key[0],
                        "theme_key": key[1],
                        "previous_score": prev_score,
                        "current_score": curr_score,
                        "delta": delta,
                        "direction": direction,
                    }
                )
        return output

    def _build_fallback_signal_rows(self, company_id: int, drift_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        signals: list[dict[str, Any]] = []
        score_history = self.repo.list_company_strategy_scores_all(company_id, limit=1200)
        if not score_history:
            return []

        score_history.sort(key=lambda row: str(row.get("filing_date", "")), reverse=True)
        latest_filing_id = int(score_history[0].get("filing_id", 0) or 0)
        latest_rows = [row for row in score_history if int(row.get("filing_id", 0) or 0) == latest_filing_id]
        score_lookup = {
            (str(row.get("dimension_key", "")), str(row.get("theme_key", ""))): row
            for row in latest_rows
        }

        for row in drift_rows:
            if str(row.get("direction", "stable")) == "stable":
                continue
            dimension = str(row.get("dimension_key", ""))
            theme = str(row.get("theme_key", ""))
            score_row = score_lookup.get((dimension, theme), {})
            quotes = list(score_row.get("evidence_quotes") or [])
            signals.append(
                {
                    "filing_id": row.get("filing_id"),
                    "theme_key": theme,
                    "dimension_key": dimension,
                    "direction": row.get("direction"),
                    "confidence": float(score_row.get("score", row.get("current_score", 0)) or 0),
                    "signal_title": f"{label_display_name(theme)} {row.get('direction', '')}".strip(),
                    "signal_description": f"Strategic emphasis is {row.get('direction', 'stable')} for {label_display_name(theme).lower()}.",
                    "filing_date": row.get("filing_date"),
                    "filing_type": row.get("filing_type"),
                    "evidence_quote": quotes[0] if quotes else None,
                    "evidence_summary": None,
                }
            )

        latest_rows_sorted = sorted(latest_rows, key=lambda row: float(row.get("score", 0) or 0), reverse=True)
        for row in latest_rows_sorted[:3]:
            theme = str(row.get("theme_key", ""))
            quotes = list(row.get("evidence_quotes") or [])
            signals.append(
                {
                    "filing_id": row.get("filing_id"),
                    "theme_key": theme,
                    "dimension_key": row.get("dimension_key"),
                    "direction": "stable",
                    "confidence": float(row.get("score", 0) or 0),
                    "signal_title": label_display_name(theme),
                    "signal_description": label_signal_description(theme),
                    "filing_date": row.get("filing_date"),
                    "filing_type": row.get("filing_type"),
                    "evidence_quote": quotes[0] if quotes else None,
                    "evidence_summary": None,
                }
            )

        signals.sort(key=lambda row: (str(row.get("filing_date", "")), float(row.get("confidence", 0) or 0)), reverse=True)
        return signals[:limit]

    def _build_fallback_response_links(self, company_id: int) -> list[dict[str, Any]]:
        drift_rows = self._build_fallback_drift_rows(company_id)
        if not drift_rows:
            return []
        scores = self.repo.list_company_strategy_scores_all(company_id, limit=1200)
        if not scores:
            return []
        scores.sort(key=lambda row: str(row.get("filing_date", "")), reverse=True)
        latest_filing_id = int(scores[0].get("filing_id", 0) or 0)
        current_rows = [row for row in scores if int(row.get("filing_id", 0) or 0) == latest_filing_id]
        previous_rows = [row for row in scores if int(row.get("filing_id", 0) or 0) != latest_filing_id]
        if not previous_rows:
            return []

        prev_map: dict[tuple[str, str], dict[str, Any]] = {}
        for row in previous_rows:
            key = (str(row.get("dimension_key", "")), str(row.get("theme_key", "")))
            if key not in prev_map:
                prev_map[key] = row

        curr_map = {(str(row.get("dimension_key", "")), str(row.get("theme_key", ""))): row for row in current_rows}
        filing_date = str(current_rows[0].get("filing_date", "")) if current_rows else ""
        filing_type = str(current_rows[0].get("filing_type", "")) if current_rows else ""
        quarter = quarter_from_date(filing_date)

        out: list[dict[str, Any]] = []
        for risk_theme, response_themes in RISK_RESPONSE_MAP.items():
            risk_curr = curr_map.get(("risk_posture", risk_theme))
            risk_prev = prev_map.get(("risk_posture", risk_theme))
            if not risk_curr or not risk_prev:
                continue
            risk_score = float(risk_curr.get("score", 0) or 0)
            risk_prev_score = float(risk_prev.get("score", 0) or 0)
            risk_delta = risk_score - risk_prev_score
            if risk_delta < 0.05:
                continue

            for response_theme in response_themes:
                response_curr = curr_map.get(("strategy_action", response_theme))
                response_prev = prev_map.get(("strategy_action", response_theme))
                if not response_curr or not response_prev:
                    continue
                response_score = float(response_curr.get("score", 0) or 0)
                response_prev_score = float(response_prev.get("score", 0) or 0)
                response_delta = response_score - response_prev_score
                if response_delta < 0.05:
                    continue

                risk_quotes = list(risk_curr.get("evidence_quotes") or [])
                response_quotes = list(response_curr.get("evidence_quotes") or [])
                link_strength = min(risk_score, response_score)
                out.append(
                    {
                        "risk_theme_key": risk_theme,
                        "response_theme_key": response_theme,
                        "risk_score": round(risk_score, 3),
                        "response_score": round(response_score, 3),
                        "risk_delta": round(risk_delta, 3),
                        "response_delta": round(response_delta, 3),
                        "link_strength": round(link_strength, 3),
                        "confidence": round(link_strength, 3),
                        "evidence_quote_risk": risk_quotes[0] if risk_quotes else None,
                        "evidence_quote_response": response_quotes[0] if response_quotes else None,
                        "filing_date": filing_date,
                        "filing_type": filing_type,
                        "quarter": quarter,
                    }
                )
        return out

    def _build_persistence_map(self, company_id: int) -> dict[tuple[str, str], dict[str, Any]]:
        latest_timeseries = self.repo.list_theme_timeseries_latest(company_id, limit=600)
        persistence_map: dict[tuple[str, str], dict[str, Any]] = {}
        seen: set[tuple[str, str]] = set()
        for row in latest_timeseries:
            key = (str(row.get("dimension_key", "")), str(row.get("theme_key", "")))
            if key in seen:
                continue
            seen.add(key)
            persistence_map[key] = {
                "persistence_count": int(row.get("persistence_count", 0) or 0),
                "persistence_score": float(row.get("persistence_score", 0) or 0),
            }
        return persistence_map

    def _build_score_component_map(self, extraction_rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
        buckets: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: {"scores": [], "evidence_count": 0})
        for row in extraction_rows:
            extracted = row.get("extracted_data") or {}
            taxonomy = extracted.get("taxonomy") or {}
            if not isinstance(taxonomy, dict):
                continue
            for dimension, labels in taxonomy.items():
                if not isinstance(labels, list):
                    continue
                for label_row in labels:
                    if not isinstance(label_row, dict):
                        continue
                    theme = str(label_row.get("label", ""))
                    if not theme:
                        continue
                    try:
                        score = float(label_row.get("score", 0) or 0)
                    except Exception:
                        score = 0.0
                    quotes = list(label_row.get("evidence_quotes") or [])
                    key = (str(dimension), theme)
                    buckets[key]["scores"].append(max(0.0, min(1.0, score)))
                    buckets[key]["evidence_count"] += len([q for q in quotes if isinstance(q, str) and q])

        out: dict[tuple[str, str], dict[str, Any]] = {}
        for key, payload in buckets.items():
            scores = payload["scores"]
            if not scores:
                continue
            avg_score = sum(scores) / len(scores)
            out[key] = {
                "avg_model_score": round(avg_score, 3),
                "evidence_count": int(payload["evidence_count"]),
            }
        return out

    def _smooth_series(self, series: list[ClientStrategyTrendPoint]) -> list[ClientStrategyTrendPoint]:
        if len(series) <= 1:
            return series
        smoothed: list[ClientStrategyTrendPoint] = []
        prev = float(series[0].score)
        smoothed.append(ClientStrategyTrendPoint(quarter=series[0].quarter, score=prev))
        for point in series[1:]:
            score = float(point.score)
            avg = round((prev + score) / 2.0, 3)
            smoothed.append(ClientStrategyTrendPoint(quarter=point.quarter, score=avg))
            prev = avg
        return smoothed

    def _delta_severity(self, delta: float | None) -> str | None:
        if delta is None:
            return None
        magnitude = abs(float(delta))
        if magnitude >= 0.2:
            return "strong"
        if magnitude >= 0.1:
            return "moderate"
        if magnitude >= 0.04:
            return "minor"
        return "minimal"

    def _comparison_basis(self, previous_filing_type: str | None) -> str | None:
        if not previous_filing_type:
            return None
        filing_type = previous_filing_type.upper()
        if filing_type == "10-Q":
            return "vs prior 10-Q"
        if filing_type == "10-K":
            return "vs baseline 10-K"
        return "vs prior filing"

    def _response_link_confidence_reason(
        self,
        *,
        confidence: float,
        risk_delta: float,
        response_delta: float,
        link_strength: float,
    ) -> str:
        level = "high" if confidence >= 0.8 else "medium" if confidence >= 0.6 else "moderate"
        return (
            f"{level.title()} confidence: risk delta {risk_delta:+.2f}, "
            f"response delta {response_delta:+.2f}, link strength {link_strength:.2f}."
        )

    def _company_extractions_by_filing(self, company_id: int, limit: int = 3000) -> dict[int, list[dict[str, Any]]]:
        rows = self.repo.list_company_extractions(company_id, limit=limit)
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            filing = ((row.get("sections") or {}).get("filings") or {})
            filing_id = int(filing.get("id", 0) or 0)
            if filing_id:
                grouped[filing_id].append(row)
        for filing_rows in grouped.values():
            filing_rows.sort(key=lambda item: int(item.get("id", 0) or 0))
        return grouped

    def _sort_theme_evidence(self, theme: UiTheme) -> None:
        if not theme.evidence:
            return
        theme.evidence.sort(key=lambda item: len((item.quote or "").strip()), reverse=True)
        theme.evidence.sort(key=lambda item: str(item.filing_date or ""), reverse=True)
        theme.evidence.sort(key=lambda item: 0 if item.source_kind == "quote" else 1)

        deduped: list[UiThemeEvidence] = []
        seen: set[str] = set()
        for item in theme.evidence:
            quote = (item.quote or "").strip()
            if not quote:
                continue
            key = quote.lower()
            if key in seen:
                continue
            seen.add(key)
            item.quote = quote
            deduped.append(item)
        theme.evidence = deduped[:4]

    def _is_generic_ui_text(self, text: str | None) -> bool:
        raw = (text or "").strip()
        if not raw:
            return True
        if self._looks_like_template_text(raw):
            return True
        if len(raw.split()) < 6:
            return True
        return False

    def _looks_like_template_text(self, text: str | None) -> bool:
        lowered = (text or "").strip().lower()
        if not lowered:
            return True
        generic_markers = [
            "recent filing language consistently supports emphasis on",
            "current relevance score",
            "directionality currently limited",
            "material strategic focus worth active monitoring",
            "emerging relevance",
            "derived from filing signals",
            "heuristic strategy fallback",
            "retrieved/field evidence",
            "taxonomy",
            "classifier",
            "threshold",
            "model score",
            "prompt",
            "schema",
        ]
        return any(marker in lowered for marker in generic_markers)

    def _is_displayable_ui_theme(self, theme: UiTheme) -> bool:
        score = float(theme.score or 0.0)
        direct_evidence = [
            item for item in theme.evidence if item.source_kind == "quote" and not self._looks_like_template_text(item.quote)
        ]
        inferred_evidence = [
            item for item in theme.evidence if item.source_kind != "quote" and not self._is_generic_ui_text(item.quote)
        ]
        if direct_evidence:
            return True
        if inferred_evidence and score >= 0.7:
            return True
        if inferred_evidence and float(theme.persistence_score or 0.0) >= 0.75 and score >= 0.6:
            return True
        if score >= 0.78 and theme.source_insight and not self._is_generic_ui_text(theme.source_insight):
            return True
        return False

    def _canonical_theme_key(self, raw: str) -> str:
        cleaned = (raw or "").lower().strip()
        if not cleaned:
            return ""
        if "ai" in cleaned and any(token in cleaned for token in ["infrastructure", "compute", "capex", "investment"]):
            return "ai_infrastructure_investment"
        if any(token in cleaned for token in ["international", "global", "geographic"]) and any(token in cleaned for token in ["expansion", "growth", "market"]):
            return "international_expansion"
        if any(token in cleaned for token in ["cost", "efficiency", "discipline", "optimization", "productivity"]):
            return "cost_efficiency"
        if any(token in cleaned for token in ["competitive", "competition", "rivalry"]) and any(token in cleaned for token in ["intensity", "pressure", "dynamics"]):
            return "competitive_intensity"
        if "automation" in cleaned and ("ai" in cleaned or "machine" in cleaned or "software" in cleaned):
            return "ai_automation"
        return cleaned.replace(" ", "_")

    def _is_reasonable_evidence_quote(self, quote: str) -> bool:
        text = " ".join((quote or "").split()).strip()
        if not text:
            return False
        words = text.split()
        if len(words) < 6:
            return False
        if len(words) > 90:
            return False

        lowered = text.lower()
        boilerplate_markers = [
            "item 1a. risk factors",
            "these risk factors do not identify all risks",
            "investment in our securities",
            "material adverse effect on our business",
        ]
        if any(marker in lowered for marker in boilerplate_markers):
            return False
        return True

    def _quote_matches_theme(self, theme_key: str, quote: str) -> bool:
        canonical = self._canonical_theme_key(theme_key)
        terms = (
            DIRECTION_KEYWORDS.get(canonical, [])
            or ACTION_KEYWORDS.get(canonical, [])
            or RISK_POSTURE_KEYWORDS.get(canonical, [])
        )
        if not terms:
            return True
        lowered = quote.lower()
        return any(term.lower() in lowered for term in terms)

    def _direction_rank(self, direction: str) -> int:
        order = {"new": 4, "increasing": 3, "stable": 2, "decreasing": 1}
        return order.get(direction, 0)

    def _impact_rank(self, theme_key: str, dimension_key: str | None) -> int:
        canonical = self._canonical_theme_key(theme_key)
        growth_themes = {
            "product_expansion",
            "product_category_launch",
            "international_expansion",
            "ai_infrastructure_investment",
            "platform_ecosystem",
            "strategic_partnership",
        }
        margin_themes = {"cost_efficiency", "ai_automation", "subscription_pricing_shift"}
        risk_themes = {"competitive_intensity", "regulatory_exposure", "supply_chain_fragility", "cybersecurity_emphasis"}

        if canonical in growth_themes:
            return 3
        if canonical in margin_themes:
            return 2
        if canonical in risk_themes or str(dimension_key or "") == "risk_posture":
            return 1
        return 0

    def _confidence_label(self, value: float) -> str:
        if value >= 0.75:
            return "Strong"
        if value >= 0.5:
            return "Moderate"
        return "Emerging"

    def _evidence_snippet(self, quote: str | None) -> str:
        raw = " ".join((quote or "").split()).strip()
        if not raw:
            return ""
        max_chars = 220
        if len(raw) <= max_chars:
            return raw
        short = raw[:max_chars]
        cut = max(short.rfind(". "), short.rfind("; "), short.rfind(", "))
        if cut < 120:
            cut = max_chars
        return short[:cut].rstrip(" ,;:.") + "..."

    def _investor_why(self, theme_key: str) -> str:
        canonical = self._canonical_theme_key(theme_key)
        if canonical == "ai_infrastructure_investment":
            return "Signals investment in long-term AI capacity and future product competitiveness."
        if canonical == "product_expansion":
            return "Expands revenue opportunities and supports growth beyond existing product lines."
        if canonical == "international_expansion":
            return "Diversifies growth across geographies and reduces reliance on one region."
        if canonical == "cost_efficiency":
            return "Can support margins and free capital for reinvestment."
        if canonical == "ai_automation":
            return "May improve productivity and operating leverage over time."
        if canonical == "competitive_intensity":
            return "Indicates pressure that can affect pricing power and execution risk."
        if canonical == "regulatory_exposure":
            return "Raises compliance and operational risk that can influence earnings stability."
        return "This theme is currently relevant to execution, growth, or risk management."

    def _build_ticker_summary(
        self,
        *,
        ticker: str,
        top_signals: list[TickerTopSignal],
        risk_response: list[TickerRiskResponse],
    ) -> str:
        if not top_signals:
            return f"{ticker} currently has limited strategic signal coverage from the latest filing set."

        first = top_signals[0].title
        second = top_signals[1].title if len(top_signals) > 1 else None
        third = top_signals[2].title if len(top_signals) > 2 else None

        if second and third:
            summary = f"{ticker} is currently focused on {first}, {second}, and {third}."
        elif second:
            summary = f"{ticker} is currently focused on {first} and {second}."
        else:
            summary = f"{ticker} is currently focused on {first}."

        direction_notes = [item.title for item in top_signals if item.direction in {"new", "increasing"}]
        if direction_notes:
            summary += f" The strongest momentum is in {', '.join(direction_notes[:2])}."
        else:
            summary += " Strategic positioning appears broadly stable versus the prior period."

        if risk_response:
            pair = risk_response[0]
            summary += f" Key risk-response pattern: {pair.risk} -> {pair.response}."
        return summary

    def _build_ui_narrative(self, *, ticker: str, themes: list[UiTheme], risk_pairs: list[Any]) -> str:
        if not themes:
            return f"{ticker} has limited strategy signal coverage in the latest dataset."
        lead = themes[0].label if len(themes) > 0 else None
        second = themes[1].label if len(themes) > 1 else None
        third = themes[2].label if len(themes) > 2 else None
        if lead and second and third:
            text = f"{ticker} is prioritizing {lead} and {second}, while sustaining focus on {third}."
        elif lead and second:
            text = f"{ticker} is prioritizing {lead} and {second}."
        else:
            text = f"{ticker} is primarily prioritizing {lead}."

        if risk_pairs:
            top = risk_pairs[0]
            risk = label_display_name(str(top.risk or "")) or str(top.risk or "")
            response = label_display_name(str(top.response or "")) or str(top.response or "")
            text += f" Main pressure point: {risk}, with response concentrated in {response}."
        return text

    def _ui_direction_label(self, direction: str, role: str) -> str:
        normalized_direction = str(direction or "stable").lower()
        normalized_role = str(role or "supporting_driver")
        if normalized_role == "fading_theme":
            return "Fading from prominence"
        if normalized_direction == "new":
            return "Emerging more clearly"
        if normalized_direction == "increasing":
            return "Gaining emphasis"
        if normalized_direction == "decreasing":
            return "Losing emphasis"
        if normalized_role == "core_driver":
            return "Continuing as a core pillar"
        if normalized_role == "constraint":
            return "Persistent execution pressure"
        return "Persistent but secondary"

    def _ui_signal_role_from_theme(self, row: dict[str, Any]) -> str:
        direction = str(row.get("direction") or "stable").lower()
        dimension_key = str(row.get("dimension_key") or "")
        score = float(row.get("score", 0.0) or 0.0)
        if dimension_key == "risk_posture":
            return "constraint"
        if direction == "decreasing":
            return "fading_theme"
        if direction in {"new", "increasing"} and score < 0.75:
            return "emerging_theme"
        if score >= 0.75:
            return "core_driver"
        return "supporting_driver"

    def _ui_signal_from_theme_row(self, row: dict[str, Any], *, filing_type: str, filing_date: str) -> dict[str, Any]:
        theme_key = str(row.get("theme_key") or "")
        direction = str(row.get("direction") or "stable").lower()
        if direction not in {"increasing", "stable", "decreasing", "new"}:
            direction = "stable"
        role = self._ui_signal_role_from_theme(row)
        evidence_rows = list(row.get("evidence") or [])
        first_evidence = next(
            (
                item
                for item in evidence_rows
                if isinstance(item, dict) and str(item.get("quote") or "").strip()
            ),
            {},
        )
        score = float(row.get("score", 0.0) or 0.0)
        return {
            "title": str(row.get("label") or label_display_name(theme_key) or theme_key.replace("_", " ").title() or "Strategic signal"),
            "role": role,
            "direction": direction,
            "direction_label": self._ui_direction_label(direction, role),
            "confidence_label": self._confidence_label(score),
            "interpretation": str(row.get("source_insight") or self._investor_why(theme_key)),
            "why_it_matters": self._investor_why(theme_key),
            "evidence_snippet": self._evidence_snippet(str(first_evidence.get("quote") or "")),
            "filing_type": str(first_evidence.get("filing_type") or filing_type),
            "filing_date": str(first_evidence.get("filing_date") or filing_date),
            "theme_key": theme_key or None,
            "dimension_key": str(row.get("dimension_key") or "") or None,
            "score": round(score, 3) if score else None,
            "delta": float(row.get("delta", 0.0) or 0.0) if row.get("delta") is not None else None,
        }

    def _normalize_date_key(self, value: Any) -> str:
        text = str(value or "").strip()
        return text[:10] if len(text) >= 10 else text

    def _build_evolution_evidence_pool(
        self,
        *,
        top_current_signals: list[dict[str, Any]],
        themes: list[dict[str, Any]],
        risk_pairs: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        pool: list[dict[str, str]] = []
        seen: set[str] = set()

        def append_item(quote: str, filing_date: Any, filing_type: Any, theme_key: Any = "") -> None:
            cleaned = self._evidence_snippet(str(quote or ""))
            if not cleaned:
                return
            item = {
                "quote": cleaned,
                "filing_date": self._normalize_date_key(filing_date),
                "filing_type": str(filing_type or "").upper().strip(),
                "theme_key": self._canonical_theme_key(str(theme_key or "")),
            }
            dedupe = f'{item["quote"].lower()}|{item["filing_date"]}|{item["filing_type"]}|{item["theme_key"]}'
            if dedupe in seen:
                return
            seen.add(dedupe)
            pool.append(item)

        for row in top_current_signals:
            if not isinstance(row, dict):
                continue
            append_item(
                quote=row.get("evidence_snippet"),
                filing_date=row.get("filing_date"),
                filing_type=row.get("filing_type"),
                theme_key=row.get("theme_key") or row.get("title"),
            )

        for row in themes:
            if not isinstance(row, dict):
                continue
            theme_key = row.get("theme_key") or row.get("id") or row.get("label")
            for evidence in list(row.get("evidence") or [])[:2]:
                if not isinstance(evidence, dict):
                    continue
                append_item(
                    quote=evidence.get("quote"),
                    filing_date=evidence.get("filing_date"),
                    filing_type=evidence.get("filing_type"),
                    theme_key=theme_key,
                )

        for row in risk_pairs:
            if not isinstance(row, dict):
                continue
            append_item(
                quote=row.get("evidence_quote_response") or row.get("evidence_quote_risk"),
                filing_date=row.get("filing_date"),
                filing_type=row.get("filing_type"),
                theme_key=row.get("response") or row.get("risk"),
            )

        return pool

    def _enrich_strategy_evolution_evidence(
        self,
        *,
        strategy_evolution: list[dict[str, Any]],
        top_current_signals: list[dict[str, Any]],
        themes: list[dict[str, Any]],
        risk_pairs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not strategy_evolution:
            return strategy_evolution
        pool = self._build_evolution_evidence_pool(
            top_current_signals=top_current_signals,
            themes=themes,
            risk_pairs=risk_pairs,
        )
        if not pool:
            return strategy_evolution

        used_keys: set[str] = set()
        out: list[dict[str, Any]] = []
        for row in strategy_evolution:
            if not isinstance(row, dict):
                continue
            current = dict(row)
            date_key = self._normalize_date_key(current.get("filing_date"))
            type_key = str(current.get("filing_type") or "").upper().strip()
            existing_items: list[dict[str, Any]] = []
            for existing in list(current.get("evidence_items") or []):
                if not isinstance(existing, dict):
                    continue
                quote = str(existing.get("quote") or "").strip()
                if not quote:
                    continue
                existing_date = self._normalize_date_key(existing.get("filing_date"))
                existing_type = str(existing.get("filing_type") or "").upper().strip()
                if date_key and existing_date and existing_date != date_key:
                    continue
                if type_key and existing_type and existing_type != type_key:
                    continue
                existing_items.append(
                    {
                        "quote": quote,
                        "filing_date": existing_date or date_key or None,
                        "filing_type": existing_type or type_key or None,
                        "source_kind": "quote",
                    }
                )
            current["evidence_items"] = existing_items[:3]
            current["evidence_snippet"] = existing_items[0]["quote"] if existing_items else ""
            theme_keys = {
                self._canonical_theme_key(str(item or ""))
                for item in list(current.get("themes") or [])[:3]
                if str(item or "").strip()
            }

            ranked: list[tuple[int, dict[str, str]]] = []
            for item in pool:
                dedupe_key = f'{item["quote"].lower()}|{item["filing_date"]}|{item["filing_type"]}'
                score = 0
                if date_key and item["filing_date"] == date_key:
                    score += 6
                if type_key and item["filing_type"] == type_key:
                    score += 3
                if item["theme_key"] and item["theme_key"] in theme_keys:
                    score += 5
                if dedupe_key not in used_keys:
                    score += 1
                ranked.append((score, item))

            ranked.sort(key=lambda x: x[0], reverse=True)
            selected: list[dict[str, str]] = []
            selected_seen: set[str] = set()

            def add_selected(item: dict[str, str], *, skip_if_used: bool) -> bool:
                dedupe = f'{item["quote"].lower()}|{item["filing_date"]}|{item["filing_type"]}'
                if dedupe in selected_seen:
                    return False
                if skip_if_used and dedupe in used_keys:
                    return False
                selected_seen.add(dedupe)
                selected.append(item)
                return True

            # First pass: strict date match for this evolution row.
            for _, item in ranked:
                if len(selected) >= 3:
                    break
                if date_key and item["filing_date"] != date_key:
                    continue
                add_selected(
                    {
                        "quote": item["quote"],
                        "filing_date": item["filing_date"],
                        "filing_type": item["filing_type"],
                    },
                    skip_if_used=True,
                )

            if selected:
                current["evidence_items"] = [
                    {
                        "quote": item["quote"],
                        "filing_date": item["filing_date"] or None,
                        "filing_type": item["filing_type"] or None,
                        "source_kind": "quote",
                    }
                    for item in selected[:3]
                ]
                current["evidence_snippet"] = selected[0]["quote"]
                for item in selected[:3]:
                    used_keys.add(f'{item["quote"].lower()}|{item["filing_date"]}|{item["filing_type"]}')
            out.append(current)

        return out

    def _normalize_precomputed_ui_payload(
        self,
        *,
        payload: dict[str, Any],
        ticker: str,
        filing_date: str,
        filing_type: str,
    ) -> dict[str, Any]:
        normalized = dict(payload)
        normalized["ticker"] = str(normalized.get("ticker") or ticker.upper())
        normalized["filing_date"] = str(normalized.get("filing_date") or filing_date or "")
        normalized["filing_type"] = str(normalized.get("filing_type") or filing_type or "")

        overall_strategy_story = str(
            normalized.get("overall_strategy_story")
            or normalized.get("strategic_arc")
            or normalized.get("summary")
            or normalized.get("narrative")
            or ""
        ).strip()
        narrative = str(normalized.get("narrative") or overall_strategy_story).strip()
        current_focus = str(
            normalized.get("current_focus")
            or normalized.get("summary")
            or normalized.get("narrative")
            or overall_strategy_story
            or ""
        ).strip()

        risk_pairs = list(normalized.get("risk_pairs") or [])
        if not risk_pairs:
            risk_pairs = []
            source_pairs = (
                list(normalized.get("risk_response_pairs") or [])
                or list(normalized.get("risk_response_links") or [])
                or list(normalized.get("risk_response") or [])
            )
            for pair in source_pairs[:3]:
                if not isinstance(pair, dict):
                    continue
                risk_pairs.append(
                    {
                        "risk": str(pair.get("risk") or pair.get("risk_label") or ""),
                        "response": str(pair.get("response") or pair.get("response_label") or ""),
                        "direction": str(pair.get("direction") or "mitigation"),
                        "quarter": str(pair.get("quarter") or ""),
                        "confidence": float(pair.get("confidence", 0.0) or 0.0),
                        "link_strength": float(pair.get("link_strength", 0.0) or 0.0) if pair.get("link_strength") is not None else None,
                        "summary": str(pair.get("summary") or ""),
                        "filing_date": str(pair.get("filing_date") or normalized["filing_date"]),
                        "filing_type": str(pair.get("filing_type") or normalized["filing_type"]),
                        "risk_score": float(pair.get("risk_score", 0.0) or 0.0),
                        "response_score": float(pair.get("response_score", 0.0) or 0.0),
                        "risk_delta": float(pair.get("risk_delta", 0.0) or 0.0),
                        "response_delta": float(pair.get("response_delta", 0.0) or 0.0),
                        "evidence_quote_risk": pair.get("evidence_quote_risk"),
                        "evidence_quote_response": pair.get("evidence_quote_response"),
                        "confidence_reason": pair.get("confidence_reason"),
                    }
                )

        themes = list(normalized.get("themes") or [])
        key_moves = list(normalized.get("key_moves") or themes[:3])
        top_current_signals = list(normalized.get("top_current_signals") or [])
        if not top_current_signals:
            top_current_signals = [
                self._ui_signal_from_theme_row(row, filing_type=normalized["filing_type"], filing_date=normalized["filing_date"])
                for row in key_moves[:3]
                if isinstance(row, dict)
            ]

        strategy_evolution = list(normalized.get("strategy_evolution") or [])
        if not strategy_evolution:
            strategy_evolution = []
            for row in list(normalized.get("history_timeline") or [])[:6]:
                if not isinstance(row, dict):
                    continue
                strategy_evolution.append(
                    {
                        "filing_type": str(row.get("filing_type") or ""),
                        "filing_date": str(row.get("filing_date") or ""),
                        "short_summary": str(row.get("takeaway") or "Strategic visibility was limited in this filing."),
                        "themes": [str(item) for item in list(row.get("top_themes") or [])[:3] if str(item or "").strip()],
                        "evidence_snippet": self._evidence_snippet(str(row.get("evidence_snippet") or "")),
                        "evidence_items": list(row.get("evidence_items") or []),
                    }
                )
        strategy_evolution = self._enrich_strategy_evolution_evidence(
            strategy_evolution=[row for row in strategy_evolution if isinstance(row, dict)],
            top_current_signals=[row for row in top_current_signals if isinstance(row, dict)],
            themes=[row for row in themes if isinstance(row, dict)],
            risk_pairs=[row for row in risk_pairs if isinstance(row, dict)],
        )

        confidence_coverage = dict(normalized.get("confidence_coverage") or {})
        confidence_label = str(confidence_coverage.get("confidence") or "Emerging")
        if confidence_label not in {"Strong", "Moderate", "Emerging"}:
            confidence_label = self._confidence_label(
                max(
                    [float(row.get("score", 0.0) or 0.0) for row in top_current_signals if isinstance(row, dict)],
                    default=0.0,
                )
            )
        coverage_label = str(confidence_coverage.get("coverage") or "")
        if coverage_label not in {"High", "Medium", "Low"}:
            if len(top_current_signals) >= 3:
                coverage_label = "Medium"
            elif len(top_current_signals) >= 1:
                coverage_label = "Low"
            else:
                coverage_label = "Low"
        confidence_coverage = {"confidence": confidence_label, "coverage": coverage_label}

        durable_themes = [
            str(item).strip()
            for item in list(
                (normalized.get("snapshot_metadata") or {}).get("durable_themes")
                or normalized.get("durable_themes")
                or []
            )[:3]
            if str(item or "").strip()
        ]
        filing_basis = " • ".join(part for part in [normalized["filing_type"], normalized["filing_date"]] if part)
        snapshot_metadata = dict(normalized.get("snapshot_metadata") or {})
        snapshot_metadata = {
            "confidence_label": str(snapshot_metadata.get("confidence_label") or confidence_label),
            "coverage_note": str(
                snapshot_metadata.get("coverage_note")
                or (
                    "Broad visibility across filings"
                    if coverage_label == "High"
                    else "Moderate visibility across filings"
                    if coverage_label == "Medium"
                    else "Limited visibility; current conclusions should remain provisional"
                )
            ),
            "signal_set_note": str(
                snapshot_metadata.get("signal_set_note")
                or ("Concentrated around a few high-signal themes" if len(top_current_signals) >= 2 else "Sparse signal set")
            ),
            "filing_basis": str(snapshot_metadata.get("filing_basis") or filing_basis or "Latest filing"),
            "durable_themes": durable_themes,
        }

        supporting_context = [
            str(item).strip()
            for item in list(normalized.get("supporting_context") or [])
            if str(item or "").strip()
        ][:3]
        if not supporting_context:
            supporting_context = [snapshot_metadata["coverage_note"], snapshot_metadata["signal_set_note"]]
            if not risk_pairs:
                supporting_context.append("No strong risk-response pair stands out clearly in the current filing window.")
            supporting_context = supporting_context[:3]

        what_changed = [
            str(item).strip()
            for item in list(normalized.get("what_changed") or normalized.get("changes") or [])
            if str(item or "").strip()
        ][:3]
        strategic_implications = [
            str(item).strip()
            for item in list(normalized.get("strategic_implications") or normalized.get("implications") or [])
            if str(item or "").strip()
        ][:3]

        normalized.update(
            {
                "narrative": narrative or overall_strategy_story,
                "summary": overall_strategy_story or narrative,
                "overall_strategy_story": overall_strategy_story or narrative,
                "strategic_arc": overall_strategy_story or narrative,
                "current_focus": current_focus or overall_strategy_story or narrative,
                "top_current_signals": top_current_signals[:3],
                "what_changed": what_changed,
                "strategy_evolution": strategy_evolution[:6],
                "snapshot_metadata": snapshot_metadata,
                "supporting_context": supporting_context,
                "changes": what_changed,
                "durable_themes": durable_themes,
                "strategic_implications": strategic_implications,
                "implications": strategic_implications,
                "confidence_coverage": confidence_coverage,
                "strategy_evidence_map": dict(normalized.get("strategy_evidence_map") or {}),
                "strategy_conclusion": dict(normalized.get("strategy_conclusion") or {}),
                "themes": themes,
                "key_moves": key_moves,
                "risk_pairs": risk_pairs,
            }
        )
        return normalized
