from __future__ import annotations

from typing import Any


DIRECTION_KEYWORDS = {
    "ai_automation": ["ai", "machine learning", "automation", "model", "genai"],
    "international_expansion": ["international", "global", "region", "geograph", "outside the u.s"],
    "cost_efficiency": ["cost", "efficien", "productivity", "margin", "optimiz", "restructur"],
    "product_expansion": ["new product", "product launch", "new service", "category", "portfolio"],
    "platform_ecosystem": ["platform", "ecosystem", "developer", "marketplace", "partner network"],
    "vertical_integration": ["vertical integration", "in-house", "own manufacturing", "internal production"],
}

ACTION_KEYWORDS = {
    "ai_infrastructure_investment": [
        "data center",
        "infrastructure",
        "compute",
        "gpu",
        "server",
        "ai infrastructure",
    ],
    "ai_acquisition": ["acquire", "acquisition", "acquired", "purchase", "merger"],
    "geographic_market_entry": ["entered", "entry", "launch in", "opened", "new market", "market entry"],
    "workforce_reduction": ["layoff", "workforce reduction", "headcount reduction", "reduction in force"],
    "product_category_launch": ["new product", "product launch", "introduced", "new category"],
    "subscription_pricing_shift": ["subscription", "pricing model", "price increase", "tier", "recurring revenue"],
    "supply_chain_relocation": ["relocate", "relocation", "move production", "shifting production", "diversify suppliers"],
    "large_capex_program": ["capex", "capital expenditure", "capital investment", "build-out", "capacity expansion"],
    "strategic_partnership": ["partnership", "partner", "collaboration", "joint venture", "alliance"],
    "divestiture_exit": ["divest", "divestiture", "exit", "disposed", "sale of"],
    "manufacturing_inhouse": ["in-house manufacturing", "internal production", "own manufacturing"],
    "regulatory_remediation_program": ["remediation", "compliance program", "consent decree", "settlement"],
}

RISK_POSTURE_KEYWORDS = {
    "regulatory_exposure": [
        "regulatory",
        "regulation",
        "regulator",
        "government",
        "law",
        "legal",
        "antitrust",
        "compliance",
        "privacy",
        "data protection",
        "gdpr",
        "dma",
        "consent decree",
        "investigation",
        "enforcement",
    ],
    "competitive_intensity": [
        "competition",
        "competitive",
        "pricing pressure",
        "rival",
        "rivalry",
        "market share",
        "price war",
        "intense competition",
    ],
    "supply_chain_fragility": [
        "supply chain",
        "supplier",
        "suppliers",
        "single-source",
        "component",
        "component shortage",
        "semiconductor",
        "inventory",
        "logistics",
        "shipping",
        "port",
        "tariff",
        "trade restriction",
    ],
    "cybersecurity_emphasis": [
        "cyber",
        "cybersecurity",
        "security",
        "information security",
        "data security",
        "privacy breach",
        "ransomware",
        "breach",
        "incident",
    ],
    "geopolitical_exposure": [
        "geopolitical",
        "political",
        "trade",
        "trade policy",
        "sanction",
        "conflict",
        "war",
        "tariff",
    ],
}

TAXONOMY_CATALOG: dict[str, list[str]] = {
    "strategy_direction": list(DIRECTION_KEYWORDS.keys()),
    "strategy_action": list(ACTION_KEYWORDS.keys()),
    "risk_posture": list(RISK_POSTURE_KEYWORDS.keys()),
}

TAXONOMY_LABEL_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "strategy_direction": {
        "ai_automation": "Narrative emphasizes AI, ML, or automation adoption.",
        "international_expansion": "Narrative emphasizes expansion into new regions or geographies.",
        "cost_efficiency": "Narrative emphasizes efficiency and cost discipline.",
        "product_expansion": "Narrative emphasizes new products or category expansion.",
        "platform_ecosystem": "Narrative emphasizes platforms, partners, or ecosystem growth.",
        "vertical_integration": "Narrative emphasizes in-house production or tighter supply control.",
    },
    "strategy_action": {
        "ai_infrastructure_investment": "Concrete investment in AI compute or infrastructure.",
        "ai_acquisition": "Acquisition of AI/ML-related assets or companies.",
        "geographic_market_entry": "Concrete entry into a new geographic market.",
        "workforce_reduction": "Workforce reduction or restructuring program.",
        "product_category_launch": "Launch of a new product category.",
        "subscription_pricing_shift": "Shift in pricing model or subscription structure.",
        "supply_chain_relocation": "Relocation or diversification of supply chain footprint.",
        "large_capex_program": "Large capital expenditure or capacity build-out program.",
        "strategic_partnership": "Strategic partnership or alliance announced.",
        "divestiture_exit": "Divestiture or exit from a business line.",
        "manufacturing_inhouse": "Move toward in-house manufacturing/production.",
        "regulatory_remediation_program": "Formal remediation/compliance program announced.",
    },
    "risk_posture": {
        "regulatory_exposure": "Risk narrative emphasizes legal/regulatory pressures.",
        "competitive_intensity": "Risk narrative emphasizes competitive pressure or market rivalry.",
        "supply_chain_fragility": "Risk narrative emphasizes supplier, logistics, or tariff fragility.",
        "cybersecurity_emphasis": "Risk narrative emphasizes cyber/data security threats.",
        "geopolitical_exposure": "Risk narrative emphasizes geopolitical/trade conflict exposure.",
    },
}

LABEL_DISPLAY_OVERRIDES = {
    "competitive_intensity": "Market competition",
    "large_capex_program": "Heavy infrastructure investment",
    "regulatory_exposure": "Regulatory pressure",
    "ai_infrastructure_investment": "AI infrastructure investment",
    "geopolitical_exposure": "Geopolitical exposure",
    "supply_chain_fragility": "Supply chain fragility",
    "product_expansion": "Product expansion",
    "cost_efficiency": "Cost efficiency",
    "international_expansion": "International expansion",
    "platform_ecosystem": "Platform ecosystem",
    "vertical_integration": "Vertical integration",
}

DOMINANT_SIGNAL_DESCRIPTIONS = {
    "competitive_intensity": "Recent filings emphasize intense market competition.",
    "large_capex_program": "Recent filings emphasize large capital expenditure programs.",
    "regulatory_exposure": "Recent filings emphasize regulatory pressure.",
    "ai_infrastructure_investment": "Recent filings emphasize AI infrastructure investment.",
    "geopolitical_exposure": "Recent filings emphasize geopolitical exposure.",
    "supply_chain_fragility": "Recent filings emphasize supply chain fragility.",
    "product_expansion": "Recent filings emphasize product expansion.",
    "cost_efficiency": "Recent filings emphasize cost efficiency.",
    "international_expansion": "Recent filings emphasize international expansion.",
    "platform_ecosystem": "Recent filings emphasize platform ecosystem growth.",
    "vertical_integration": "Recent filings emphasize vertical integration.",
}


def _title_from_key(key: str) -> str:
    return key.replace("_", " ").title()


def label_display_name(label_key: str) -> str:
    if not label_key:
        return ""
    if label_key in LABEL_DISPLAY_OVERRIDES:
        return LABEL_DISPLAY_OVERRIDES[label_key]
    return _title_from_key(label_key)


def label_signal_description(label_key: str) -> str:
    if not label_key:
        return ""
    if label_key in DOMINANT_SIGNAL_DESCRIPTIONS:
        return DOMINANT_SIGNAL_DESCRIPTIONS[label_key]
    return f"Recent filings emphasize {label_display_name(label_key).lower()}."


def get_taxonomy_catalog_detailed() -> dict[str, dict[str, dict[str, str]]]:
    detailed: dict[str, dict[str, dict[str, str]]] = {}
    for dimension, labels in TAXONOMY_CATALOG.items():
        detailed[dimension] = {}
        for label_key in labels:
            detailed[dimension][label_key] = {
                "display_name": label_display_name(label_key),
                "description": TAXONOMY_LABEL_DESCRIPTIONS.get(dimension, {}).get(
                    label_key,
                    f"{label_display_name(label_key)} strategy signal.",
                ),
            }
    return detailed


def quarter_from_date(value: str) -> str:
    try:
        year, month, _ = value.split("-")
        month_int = int(month)
        quarter = (month_int - 1) // 3 + 1
        return f"{year}Q{quarter}"
    except Exception:
        return "unknown"


def flatten_allowed_theme_keys() -> set[str]:
    return {label for labels in TAXONOMY_CATALOG.values() for label in labels}


def flatten_allowed_dimensions() -> set[str]:
    return set(TAXONOMY_CATALOG.keys())


def coerce_theme_row(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "dimension_key": str(raw.get("dimension_key", "")),
        "theme_key": str(raw.get("theme_key", "")),
        "score": float(raw.get("score", 0) or 0),
        "evidence_count": int(raw.get("evidence_count", 0) or 0),
        "evidence_quotes": list(raw.get("evidence_quotes") or []),
    }
