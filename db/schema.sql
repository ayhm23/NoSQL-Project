-- db/schema.sql
-- NASA Log ETL — Result Tables Schema
-- PostgreSQL version

-- ─── Pipeline Run Registry ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id              VARCHAR(64)  PRIMARY KEY,
    pipeline            VARCHAR(32)  NOT NULL,
    batch_size          INT          NOT NULL,
    total_records       BIGINT       NOT NULL,
    malformed_records   BIGINT       NOT NULL,
    num_batches         INT          NOT NULL,
    avg_batch_size      NUMERIC(12, 2) NOT NULL,
    runtime_seconds     NUMERIC(12, 3) NOT NULL,
    started_at          TIMESTAMPTZ  NOT NULL,
    completed_at        TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_pipeline
    ON etl_runs (pipeline);

-- ─── Q1 — Daily Traffic Summary ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id              BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(64)  NOT NULL REFERENCES etl_runs(run_id),
    pipeline        VARCHAR(32)  NOT NULL,
    log_date        DATE         NOT NULL,
    status_code     SMALLINT     NOT NULL,
    request_count   BIGINT       NOT NULL,
    total_bytes     BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q1_run_id
    ON q1_daily_traffic (run_id);

CREATE INDEX IF NOT EXISTS idx_q1_date
    ON q1_daily_traffic (log_date);

-- ─── Q2 — Top 20 Resources ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS q2_top_resources (
    id              BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(64)  NOT NULL REFERENCES etl_runs(run_id),
    pipeline        VARCHAR(32)  NOT NULL,
    resource_path   VARCHAR(2048) NOT NULL,
    request_count   BIGINT        NOT NULL,
    total_bytes     BIGINT        NOT NULL,
    distinct_hosts  INT           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q2_run_id
    ON q2_top_resources (run_id);

CREATE INDEX IF NOT EXISTS idx_q2_request_count
    ON q2_top_resources (request_count DESC);

-- ─── Q3 — Hourly Error Analysis ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               VARCHAR(64) NOT NULL REFERENCES etl_runs(run_id),
    pipeline             VARCHAR(32) NOT NULL,
    log_date             DATE        NOT NULL,
    log_hour             SMALLINT    NOT NULL,
    error_count          BIGINT      NOT NULL,
    total_requests       BIGINT      NOT NULL,
    error_rate           NUMERIC(10, 6) NOT NULL,
    distinct_error_hosts INT         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q3_run_id
    ON q3_hourly_errors (run_id);

CREATE INDEX IF NOT EXISTS idx_q3_date_hour
    ON q3_hourly_errors (log_date, log_hour);