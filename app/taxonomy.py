from typing import Any

TAXONOMY_DIMENSIONS: dict[str, dict[str, str]] = {
    "strategic_orientation": {
        "display_name": "Strategic Orientation",
        "description": "Primary strategic direction inferred from filing language.",
    },
    "growth_vector": {
        "display_name": "Growth Vector",
        "description": "Main growth mechanisms highlighted by management.",
    },
    "capital_deployment": {
        "display_name": "Capital Deployment",
        "description": "How resources are emphasized across investment and return priorities.",
    },
    "risk_posture": {
        "display_name": "Risk Posture",
        "description": "Dominant risk themes and pressure points reflected in disclosures.",
    },
}

TAXONOMY_LABEL_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "strategic_orientation": {
        "growth_expansion": "Narrative emphasizes expansion across revenue, products, or markets.",
        "margin_optimization": "Narrative emphasizes profitability, efficiency, and margin improvement.",
        "innovation_led": "Narrative prioritizes innovation and technology differentiation.",
        "defensive_stabilization": "Narrative emphasizes stability under uncertainty and downside protection.",
        "capital_return_priority": "Narrative emphasizes shareholder returns via dividends or buybacks.",
    },
    "growth_vector": {
        "new_product": "Growth is driven by new product or service launches.",
        "ai_automation": "Growth is tied to AI, ML, or automation adoption.",
        "geographic_expansion": "Growth comes from expansion across regions.",
        "pricing_power": "Growth relies on pricing and mix improvement.",
        "mna_led": "Growth relies on acquisitions or inorganic expansion.",
    },
    "capital_deployment": {
        "r_and_d_intensification": "Capital is concentrated in R&D and product development.",
        "shareholder_yield_focus": "Capital is concentrated in dividends or buybacks.",
        "capacity_expansion": "Capital is concentrated in infrastructure and capacity build-out.",
        "cost_restructuring": "Capital is concentrated in restructuring and cost efficiency programs.",
    },
    "risk_posture": {
        "regulatory_exposure": "Risk narrative emphasizes legal or regulatory pressure.",
        "competitive_intensity": "Risk narrative emphasizes competitive pressure.",
        "supply_chain_fragility": "Risk narrative emphasizes supplier, logistics, or tariff fragility.",
        "cybersecurity_emphasis": "Risk narrative emphasizes cyber or data security threats.",
        "geopolitical_exposure": "Risk narrative emphasizes geopolitical and trade conflict exposure.",
    },
}


def _title_from_key(key: str) -> str:
    return key.replace("_", " ").title()


def get_catalog_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dim_key, dim_meta in TAXONOMY_DIMENSIONS.items():
        labels = TAXONOMY_LABEL_DESCRIPTIONS.get(dim_key, {})
        rows.append(
            {
                "key": dim_key,
                "display_name": dim_meta["display_name"],
                "description": dim_meta["description"],
                "labels": [
                    {
                        "key": label_key,
                        "display_name": _title_from_key(label_key),
                        "description": label_desc,
                    }
                    for label_key, label_desc in labels.items()
                ],
            }
        )
    return rows
