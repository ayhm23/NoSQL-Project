-- db/schema.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- NASA Log ETL — Result Tables Schema
-- Compatible with both MySQL 8+ and PostgreSQL 13+
--
-- Every result table shares the same four metadata columns:
--   pipeline    : "pig" | "mapreduce" | "mongodb" | "hive"
--   run_id      : UUID string identifying a single pipeline execution
--   batch_id    : Batch number (1-based); 0 means aggregated over all batches
--   executed_at : ISO-8601 UTC timestamp when the pipeline run started
-- ─────────────────────────────────────────────────────────────────────────────

-- ─── Q1 — Daily Traffic Summary ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id              BIGSERIAL PRIMARY KEY,          -- auto PK (use BIGINT AUTO_INCREMENT for MySQL)
    pipeline        VARCHAR(32)  NOT NULL,
    run_id          VARCHAR(64)  NOT NULL,
    batch_id        INT          NOT NULL DEFAULT 0,
    executed_at     TIMESTAMPTZ  NOT NULL,          -- use DATETIME for MySQL
    log_date        DATE         NOT NULL,
    status_code     SMALLINT     NOT NULL,
    request_count   BIGINT       NOT NULL,
    total_bytes     BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q1_pipeline_run
    ON q1_daily_traffic (pipeline, run_id);

CREATE INDEX IF NOT EXISTS idx_q1_date
    ON q1_daily_traffic (log_date);

-- ─── Q2 — Top 20 Resources ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS q2_top_resources (
    id              BIGSERIAL PRIMARY KEY,
    pipeline        VARCHAR(32)   NOT NULL,
    run_id          VARCHAR(64)   NOT NULL,
    batch_id        INT           NOT NULL DEFAULT 0,
    executed_at     TIMESTAMPTZ   NOT NULL,
    resource_path   VARCHAR(2048) NOT NULL,
    request_count   BIGINT        NOT NULL,
    total_bytes     BIGINT        NOT NULL,
    distinct_hosts  INT           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q2_pipeline_run
    ON q2_top_resources (pipeline, run_id);

CREATE INDEX IF NOT EXISTS idx_q2_request_count
    ON q2_top_resources (request_count DESC);

-- ─── Q3 — Hourly Error Analysis ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   BIGSERIAL PRIMARY KEY,
    pipeline             VARCHAR(32) NOT NULL,
    run_id               VARCHAR(64) NOT NULL,
    batch_id             INT         NOT NULL DEFAULT 0,
    executed_at          TIMESTAMPTZ NOT NULL,
    log_date             DATE        NOT NULL,
    log_hour             SMALLINT    NOT NULL,
    error_count          BIGINT      NOT NULL,
    total_requests       BIGINT      NOT NULL,
    error_rate           NUMERIC(10, 6) NOT NULL,
    distinct_error_hosts INT         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q3_pipeline_run
    ON q3_hourly_errors (pipeline, run_id);

CREATE INDEX IF NOT EXISTS idx_q3_date_hour
    ON q3_hourly_errors (log_date, log_hour);

-- ─── Pipeline Run Registry ───────────────────────────────────────────────────
-- One row per pipeline execution; useful for the comparison report.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           VARCHAR(64)  PRIMARY KEY,
    pipeline         VARCHAR(32)  NOT NULL,
    executed_at      TIMESTAMPTZ  NOT NULL,
    runtime_s        NUMERIC(12, 3),
    total_lines      BIGINT,
    parsed_ok        BIGINT,
    malformed        BIGINT,
    total_batches    INT,
    non_empty_batches INT,
    avg_batch_size   NUMERIC(12, 2),
    notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline
    ON pipeline_runs (pipeline);


-- ─────────────────────────────────────────────────────────────────────────────
-- MySQL-compatible version (comment out the PostgreSQL block above and use this)
-- ─────────────────────────────────────────────────────────────────────────────
-- If using MySQL, replace:
--   BIGSERIAL      → BIGINT AUTO_INCREMENT
--   TIMESTAMPTZ    → DATETIME
--   SMALLINT       → TINYINT / SMALLINT
--   NUMERIC(10,6)  → DECIMAL(10,6)
--   CREATE INDEX IF NOT EXISTS → CREATE INDEX  (IF NOT EXISTS added in MySQL 8.0.22+)
