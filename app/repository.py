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

    def list_quarter_states(self, company_id: int, limit: int) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("company_quarter_strategy_states")
                .select(
                    "filing_id,filing_type,filing_date,shift_level,net_direction,contradictions_count,pivots_count,changed_sections_count"
                )
                .eq("company_id", company_id)
                .order("filing_date", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception as exc:
            raise BackendError(f"Failed to load quarter states for company_id={company_id}: {exc}") from exc

    def list_strategy_extractions(self, company_id: int, limit: int) -> list[dict[str, Any]]:
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
                            accession_number,
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

    def list_taxonomy_scores(self, strategy_extraction_ids: list[int]) -> list[dict[str, Any]]:
        if not strategy_extraction_ids:
            return []
        try:
            response = (
                self.client.table("section_taxonomy_scores")
                .select("strategy_extraction_id,dimension_key,label_key,score,source")
                .in_("strategy_extraction_id", strategy_extraction_ids)
                .execute()
            )
            return response.data or []
        except Exception as exc:
            raise BackendError("Failed to load taxonomy scores") from exc

    def list_companies_by_tickers(self, tickers: list[str]) -> list[dict[str, Any]]:
        if not tickers:
            return []
        normalized = sorted({ticker.upper().strip() for ticker in tickers if ticker and ticker.strip()})
        if not normalized:
            return []
        try:
            response = (
                self.client.table("companies")
                .select("id,name,ticker")
                .in_("ticker", normalized)
                .execute()
            )
            return response.data or []
        except Exception as exc:
            raise BackendError(f"Failed to load companies for tickers={normalized}: {exc}") from exc
