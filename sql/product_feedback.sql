CREATE TABLE IF NOT EXISTS public.product_feedback (
    id BIGSERIAL PRIMARY KEY,
    rating TEXT CHECK (rating IN ('positive', 'neutral', 'negative')),
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    note TEXT,
    path TEXT,
    source TEXT,
    submitted_at TIMESTAMPTZ,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_product_feedback_created_at
    ON public.product_feedback (created_at DESC);
