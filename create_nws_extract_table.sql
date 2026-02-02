CREATE TABLE IF NOT EXISTS public.nws_hourly_forecast_extract (
    id BIGSERIAL PRIMARY KEY,
    run_ts_utc TIMESTAMPTZ NOT NULL,
    location TEXT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    source_url TEXT,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_nws_extract_run_ts
    ON public.nws_hourly_forecast_extract (run_ts_utc);

CREATE INDEX IF NOT EXISTS ix_nws_extract_payload_gin
    ON public.nws_hourly_forecast_extract
    USING GIN (payload);