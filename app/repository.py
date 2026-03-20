from __future__ import annotations

from typing import Any

from supabase import Client, create_client

from app.config import settings
from app.errors import BackendError, NotFoundError


def _normalize_single_relation(value: Any) -> dict[str, Any]:
    if isinstance(value, list):
        return value[0] if value else {}
    if isinstance(value, dict):
        return value
    return {}


class ProductRepository:
    def __init__(self) -> None:
        try:
            self.client: Client = create_client(settings.supabase_url, settings.supabase_key)
        except Exception as exc:
            raise BackendError(f"Failed to initialize Supabase client: {exc}") from exc

    def get_company_by_ticker(self, ticker: str) -> dict[str, Any]:
        normalized_ticker = ticker.upper().strip()
        try:
            response = (
                self.client.table("companies")
                .select("id,name,ticker")
                .eq("ticker", normalized_ticker)
                .limit(1)
                .execute()
            )
            if not response.data:
                raise NotFoundError(f"Ticker {normalized_ticker} not found")
            return response.data[0]
        except NotFoundError:
            raise
        except Exception as exc:
            raise BackendError(f"Failed to load company for ticker {normalized_ticker}: {exc}") from exc

    def get_latest_strategy_snapshot(self, company_id: int) -> dict[str, Any] | None:
        try:
            response = (
                self.client.table("company_strategy_snapshots")
                .select("filing_id,filing_date,filing_type,dominant_themes,emerging_themes,declining_themes")
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(1)
                .execute()
            )
            return (response.data or [None])[0]
        except Exception:
            return None

    def list_company_strategy_scores_for_filing(self, company_id: int, filing_id: int) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("company_strategy_scores")
                .select("dimension_key,theme_key,score,evidence_count,evidence_quotes")
                .eq("company_id", company_id)
                .eq("filing_id", filing_id)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_company_strategy_scores_all(self, company_id: int, limit: int = 1200) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("company_strategy_scores")
                .select("filing_id,filing_date,filing_type,quarter,dimension_key,theme_key,score,evidence_count,evidence_quotes")
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_theme_timeseries_latest(self, company_id: int, limit: int = 600) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("strategy_theme_timeseries")
                .select("filing_id,filing_date,dimension_key,theme_key,persistence_count,persistence_score")
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_strategy_trends(self, company_id: int, theme_key: str | None = None, limit: int = 400) -> list[dict[str, Any]]:
        try:
            query = (
                self.client.table("strategy_theme_timeseries")
                .select("filing_id,filing_date,quarter,score,smoothed_score,persistence_count,persistence_score,theme_key,dimension_key")
                .eq("company_id", company_id)
                .order("quarter", desc=False)
                .limit(limit)
            )
            if theme_key:
                query = query.eq("theme_key", theme_key)
            response = query.execute()
            return response.data or []
        except Exception:
            return []

    def list_company_strategy_signals(self, company_id: int, limit: int = 50) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("company_strategy_signals")
                .select(
                    "filing_id,theme_key,dimension_key,direction,confidence,signal_title,signal_description,filing_date,filing_type,evidence_quote,evidence_summary"
                )
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_strategy_response_links(self, company_id: int, limit: int = 50) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("strategy_response_links")
                .select(
                    "risk_theme_key,response_theme_key,risk_score,response_score,risk_delta,response_delta,link_strength,confidence,evidence_quote_risk,evidence_quote_response,filing_date,filing_type,quarter"
                )
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_strategy_drift_events(self, company_id: int, limit: int = 200) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("strategy_drift_events")
                .select("filing_id,previous_filing_id,filing_date,filing_type,dimension_key,theme_key,previous_score,current_score,delta,direction")
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_filings_by_ids(self, filing_ids: list[int]) -> dict[int, dict[str, Any]]:
        if not filing_ids:
            return {}
        try:
            response = self.client.table("filings").select("id,filing_type,filing_date").in_("id", filing_ids).execute()
            rows = response.data or []
            return {int(row["id"]): row for row in rows if row.get("id") is not None}
        except Exception:
            return {}

    def list_strategy_extractions_for_filing(self, filing_id: int) -> list[dict[str, Any]]:
        try:
            section_rows = self.client.table("sections").select("id").eq("filing_id", filing_id).execute().data or []
            section_ids = [row.get("id") for row in section_rows if row.get("id") is not None]
            if not section_ids:
                return []
            response = (
                self.client.table("strategy_extractions")
                .select("id,extracted_data,confidence,section_id")
                .in_("section_id", section_ids)
                .order("id", desc=False)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def list_company_extractions(self, company_id: int, limit: int = 1200) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("strategy_extractions")
                .select(
                    """
                    id,
                    confidence,
                    extracted_data,
                    section_id,
                    sections(
                        id,
                        section_name,
                        filings(
                            id,
                            filing_type,
                            filing_date,
                            companies(id,ticker,name)
                        )
                    )
                    """
                )
                .order("id", desc=True)
                .limit(limit)
                .execute()
            )
            rows = response.data or []
            filtered: list[dict[str, Any]] = []
            for row in rows:
                section = _normalize_single_relation(row.get("sections"))
                filing = _normalize_single_relation(section.get("filings"))
                company = _normalize_single_relation(filing.get("companies"))
                if int(company.get("id", 0) or 0) != int(company_id):
                    continue
                row["sections"] = section
                section["filings"] = filing
                filing["companies"] = company
                filtered.append(row)
            return filtered
        except Exception as exc:
            raise BackendError(f"Failed to load strategy extractions for company_id={company_id}: {exc}") from exc

    def list_section_taxonomy_scores(self, extraction_ids: list[int]) -> list[dict[str, Any]]:
        if not extraction_ids:
            return []
        try:
            response = (
                self.client.table("section_taxonomy_scores")
                .select("strategy_extraction_id,dimension_key,label_key,score,source")
                .in_("strategy_extraction_id", extraction_ids)
                .execute()
            )
            return response.data or []
        except Exception:
            return []

    def create_feedback(
        self,
        *,
        rating: str | None,
        tags: list[str],
        note: str | None,
        path: str | None,
        source: str | None,
        submitted_at: str | None,
        user_agent: str | None,
    ) -> dict[str, Any]:
        payload = {
            "rating": rating,
            "tags": tags,
            "note": note,
            "path": path,
            "source": source,
            "submitted_at": submitted_at,
            "user_agent": user_agent,
        }
        try:
            response = self.client.table("product_feedback").insert(payload).execute()
            return (response.data or [{}])[0]
        except Exception as exc:
            raise BackendError(f"Failed to create feedback row: {exc}") from exc
