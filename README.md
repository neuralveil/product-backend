# product-backend

Product API backend for `product-ui`, compatible with the public strategy endpoints previously served by `extract-a`.

## Goal

This project is the read-focused backend for product surfaces (ticker pages, strategy cards, feed-style narratives), while `extract-a` remains the extraction/ingestion pipeline.

## API compatibility for `product-ui`

Implemented endpoints:

- `GET /health`
- `GET /taxonomy-catalog`
- `GET /v1/taxonomy/catalog`
- `GET /v1/strategy/snapshot?ticker=NVDA`
- `GET /v1/strategy/trends?ticker=NVDA&theme_key=ai_automation`
- `GET /v1/strategy/signals?ticker=NVDA&limit=50&latest_only=true`
- `GET /v1/strategy/response-links?ticker=NVDA&limit=50&latest_only=true`

Response contracts are aligned with the `extract-a` public product API models used by `product-ui`.

## Data source strategy

`product-backend` reads from Supabase tables. It uses this order:

1. Pre-aggregated strategy tables (`company_strategy_snapshots`, `company_strategy_signals`, `strategy_response_links`, `strategy_theme_timeseries`, `strategy_drift_events`, `company_strategy_scores`) when available.
2. Fallback computation from lower-level extraction tables (`strategy_extractions`, `section_taxonomy_scores`, `sections`, `filings`) when aggregate tables are absent or empty.

This keeps the API functional even when full aggregation jobs have not run yet.

## Setup

1. Create env file:

```bash
cp .env.example .env
```

2. Set required env vars in `.env`:

- `SUPABASE_URL`
- `SUPABASE_KEY`

3. Install and run:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8010
```

## Deploy to Render

### Option A: Blueprint (`render.yaml`)

1. Push this repo to GitHub.
2. In Render: `New +` -> `Blueprint`.
3. Select this repo and deploy.
4. Set secret env vars in Render service settings:
- `SUPABASE_URL`
- `SUPABASE_KEY`

Render uses:
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Health check: `/health`

### Option B: Manual Web Service

Use these values in Render:
- Runtime: `Python`
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Health Check Path: `/health`

Required env vars:
- `SUPABASE_URL`
- `SUPABASE_KEY`
