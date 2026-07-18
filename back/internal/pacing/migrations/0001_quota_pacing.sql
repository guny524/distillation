-- Quota pacing state tables (spec sec 2). Applied idempotently via
-- pacing.Migrate; CREATE ... IF NOT EXISTS keeps re-apply safe (backward-compat:
-- DB is migration-managed). Separate from distillation_pairs.

-- Per-run decision/status log.
CREATE TABLE IF NOT EXISTS quota_pacing_run_logs (
    id                    BIGSERIAL PRIMARY KEY,
    provider              TEXT NOT NULL,
    source_tag            TEXT NOT NULL,
    status                TEXT NOT NULL,
    mode                  TEXT NOT NULL DEFAULT '',
    started_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    planned_count         INT NOT NULL DEFAULT 0,
    generated_count       INT NOT NULL DEFAULT 0,
    primary_used_before   DOUBLE PRECISION,
    primary_used_after    DOUBLE PRECISION,
    secondary_used_before DOUBLE PRECISION,
    secondary_used_after  DOUBLE PRECISION,
    allowance_pct         DOUBLE PRECISION,
    error_msg             TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_qp_run_logs_key_started
    ON quota_pacing_run_logs (provider, source_tag, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_qp_run_logs_status_started
    ON quota_pacing_run_logs (status, started_at DESC);

-- Learned per-item cost EMA per (provider, source_tag, window_kind).
CREATE TABLE IF NOT EXISTS quota_cost_ema (
    provider               TEXT NOT NULL,
    source_tag             TEXT NOT NULL,
    window_kind            TEXT NOT NULL,
    cost_pct_per_item      DOUBLE PRECISION NOT NULL DEFAULT 0,
    alpha                  DOUBLE PRECISION NOT NULL DEFAULT 0.2,
    sample_count           BIGINT NOT NULL DEFAULT 0,
    attribution_mode       TEXT NOT NULL DEFAULT '',
    pending_zero_generated INT NOT NULL DEFAULT 0,
    pending_zero_delta     DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, source_tag, window_kind)
);

-- Before/after quota observations with outlier classification.
CREATE TABLE IF NOT EXISTS quota_observations (
    id               BIGSERIAL PRIMARY KEY,
    run_log_id       BIGINT NOT NULL REFERENCES quota_pacing_run_logs (id),
    provider         TEXT NOT NULL,
    source_tag       TEXT NOT NULL,
    window_kind      TEXT NOT NULL,
    used_before      DOUBLE PRECISION NOT NULL,
    used_after       DOUBLE PRECISION NOT NULL,
    resets_at_before TIMESTAMPTZ,
    resets_at_after  TIMESTAMPTZ,
    delta            DOUBLE PRECISION NOT NULL,
    generated_count  INT NOT NULL,
    is_outlier       BOOLEAN NOT NULL DEFAULT FALSE,
    outlier_reason   TEXT NOT NULL DEFAULT '',
    used_for_ema     BOOLEAN NOT NULL DEFAULT FALSE,
    observed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_qp_observations_run_log
    ON quota_observations (run_log_id);
CREATE INDEX IF NOT EXISTS idx_qp_observations_key_observed
    ON quota_observations (provider, source_tag, window_kind, observed_at DESC);

-- Per-hour planned/consumed allowance (leaky bucket state).
CREATE TABLE IF NOT EXISTS quota_hourly_budget_consumption (
    provider        TEXT NOT NULL,
    source_tag      TEXT NOT NULL,
    budget_hour     TIMESTAMPTZ NOT NULL,
    window_kind     TEXT NOT NULL,
    allowance_pct   DOUBLE PRECISION NOT NULL DEFAULT 0,
    planned_pct     DOUBLE PRECISION NOT NULL DEFAULT 0,
    consumed_pct    DOUBLE PRECISION NOT NULL DEFAULT 0,
    generated_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, source_tag, budget_hour, window_kind)
);
