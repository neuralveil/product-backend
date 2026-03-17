# product-backend

Ticker-centric API for packaging taxonomy extraction into partner-ready signal feeds.

## Why this design

Platforms like eToro usually need compact, frequent, explainable updates that can power cards, feeds, and alerts. This API is built around that:

- `snapshot`: latest high-value signal state for one ticker.
- `timeline`: quarter-by-quarter narrative/drift evolution for one ticker.
- `events`: cross-ticker high-signal drift events for feed ranking.
- `taxonomy/catalog`: stable metadata to render labels consistently.

The payload is intentionally **not** a full analytical dump. It focuses on drift, signals, and narrative changes with confidence and severity.

## Data model assumptions

This service reads from the existing extraction DB tables already used by `extract-a`:

- `companies`
- `filings`
- `strategy_extractions`
- `section_taxonomy_scores`
- `company_quarter_strategy_states`

## Endpoints

### `GET /health`

Health probe.

### `GET /v1/taxonomy/catalog`

Stable metadata for dimensions and labels.

### `GET /v1/tickers/{ticker}/snapshot`

Returns the latest quarter state, top taxonomy signals, and top drift signals.

### `GET /v1/tickers/{ticker}/timeline?limit=8`

Returns quarter points for a ticker with representative drift+narrative and top taxonomy signals per quarter.

### `GET /v1/events?tickers=AAPL,MSFT,NVDA&min_severity=medium&limit=50`

Returns cross-ticker drift events sorted by severity and score.

## Setup

1. Create env file:

```bash
cp .env.example .env
```

2. Set values in `.env`:

- `SUPABASE_URL`
- `SUPABASE_KEY`

3. Install and run:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8010
```

## Product contract guidance

For partner engagement products, these fields should be treated as primary:

- Drift intensity: `score`, `severity`, `tone_change`, `narrative_alignment`
- Narrative payload: `short_narrative`, `key_changes`, `evidence_quotes`
- Taxonomy payload: `dimension_key`, `label_key`, `score`
- Reliability context: `confidence`
- Temporal anchor: `filing_date`, `filing_type`

This keeps the payload actionable for ranking, alerts, and concise UI cards.
