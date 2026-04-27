-- =============================================================================
-- DAS 839 – NoSQL Systems End Semester Project
-- Database Schema: Multi-Pipeline ETL Results Store
-- Compatible with: MySQL 8.0+ and PostgreSQL 13+
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DROP (safe teardown for re-runs)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS q3_hourly_errors CASCADE;
DROP TABLE IF EXISTS q2_top_resources CASCADE;
DROP TABLE IF EXISTS q1_daily_traffic CASCADE;
DROP TABLE IF EXISTS etl_runs CASCADE;

-- =============================================================================
-- TABLE 1: etl_runs
-- One row per pipeline execution run.
-- All query result tables FK reference this.
-- =============================================================================
CREATE TABLE etl_runs (
    run_id              VARCHAR(36)     PRIMARY KEY,       -- UUID e.g. "550e8400-e29b-41d4-a716-446655440000"
    pipeline            VARCHAR(20)     NOT NULL,          -- 'pig' | 'mapreduce' | 'mongodb' | 'hive'
    batch_size          INT             NOT NULL,          -- configured batch size (records per batch)
    total_records       BIGINT          NOT NULL DEFAULT 0,-- total input log lines processed
    malformed_records   BIGINT          NOT NULL DEFAULT 0,-- lines that failed parsing
    num_batches         INT             NOT NULL DEFAULT 0,-- total number of batches executed
    avg_batch_size      FLOAT           NOT NULL DEFAULT 0,-- total_records / num_batches
    runtime_seconds     FLOAT           NOT NULL DEFAULT 0,-- wall-clock from first read → last DB write
    started_at          TIMESTAMP       NOT NULL,          -- when pipeline execution began
    completed_at        TIMESTAMP       NOT NULL           -- when final results were written to DB
);

-- =============================================================================
-- TABLE 2: q1_daily_traffic
-- Query 1 – Daily Traffic Summary
-- For each (log_date, status_code): total requests and bytes.
-- =============================================================================
CREATE TABLE q1_daily_traffic (
    id              BIGSERIAL       PRIMARY KEY,
    run_id          VARCHAR(36)     NOT NULL,
    log_date        DATE            NOT NULL,              -- e.g. 1995-07-01
    status_code     INT             NOT NULL,              -- HTTP status code e.g. 200, 404, 500
    request_count   BIGINT          NOT NULL DEFAULT 0,
    total_bytes     BIGINT          NOT NULL DEFAULT 0,

    CONSTRAINT fk_q1_run FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX idx_q1_run_date ON q1_daily_traffic (run_id, log_date);
CREATE INDEX idx_q1_status   ON q1_daily_traffic (status_code);

-- =============================================================================
-- TABLE 3: q2_top_resources
-- Query 2 – Top 20 Requested Resources
-- By request count: path, total requests, bytes, distinct hosts.
-- =============================================================================
CREATE TABLE q2_top_resources (
    id                  BIGSERIAL       PRIMARY KEY,
    run_id              VARCHAR(36)     NOT NULL,
    resource_path       TEXT            NOT NULL,          -- e.g. /shuttle/missions/sts-71/images/...
    request_count       BIGINT          NOT NULL DEFAULT 0,
    total_bytes         BIGINT          NOT NULL DEFAULT 0,
    distinct_host_count INT             NOT NULL DEFAULT 0,-- number of unique hosts that requested this path
    rank_position       INT             NOT NULL DEFAULT 0, -- 1 = most requested, up to 20

    CONSTRAINT fk_q2_run FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX idx_q2_run      ON q2_top_resources (run_id);
CREATE INDEX idx_q2_rank     ON q2_top_resources (run_id, rank_position);

-- =============================================================================
-- TABLE 4: q3_hourly_errors
-- Query 3 – Hourly Error Analysis
-- For each (log_date, log_hour): error counts, total requests, error rate,
-- and number of distinct hosts generating error requests (status 400-599).
-- =============================================================================
CREATE TABLE q3_hourly_errors (
    id                      BIGSERIAL       PRIMARY KEY,
    run_id                  VARCHAR(36)     NOT NULL,
    log_date                DATE            NOT NULL,
    log_hour                INT             NOT NULL,      -- 0–23
    error_request_count     BIGINT          NOT NULL DEFAULT 0, -- requests with status 400–599
    total_request_count     BIGINT          NOT NULL DEFAULT 0, -- all requests in that hour
    error_rate              FLOAT           NOT NULL DEFAULT 0,  -- error_request_count / total_request_count
    distinct_error_hosts    INT             NOT NULL DEFAULT 0,  -- unique hosts that triggered errors

    CONSTRAINT fk_q3_run FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX idx_q3_run_date ON q3_hourly_errors (run_id, log_date);
CREATE INDEX idx_q3_hour     ON q3_hourly_errors (log_date, log_hour);

-- =============================================================================
-- VIEWS (optional helpers for quick inspection)
-- =============================================================================

-- Latest run summary across pipelines
CREATE OR REPLACE VIEW v_latest_runs AS
SELECT
    pipeline,
    run_id,
    batch_size,
    total_records,
    malformed_records,
    num_batches,
    ROUND(avg_batch_size::NUMERIC, 2)    AS avg_batch_size,
    ROUND(runtime_seconds::NUMERIC, 3)   AS runtime_seconds,
    started_at,
    completed_at
FROM etl_runs
ORDER BY started_at DESC;

-- Quick Q1 pivot: requests by date across all runs
CREATE OR REPLACE VIEW v_q1_summary AS
SELECT
    r.pipeline,
    q.log_date,
    q.status_code,
    q.request_count,
    q.total_bytes
FROM q1_daily_traffic q
JOIN etl_runs r ON r.run_id = q.run_id
ORDER BY r.pipeline, q.log_date, q.status_code;

-- Quick Q2 view: top resources across all runs
CREATE OR REPLACE VIEW v_q2_summary AS
SELECT
    r.pipeline,
    q.rank_position,
    q.resource_path,
    q.request_count,
    q.total_bytes,
    q.distinct_host_count
FROM q2_top_resources q
JOIN etl_runs r ON r.run_id = q.run_id
ORDER BY r.pipeline, q.rank_position;

-- Quick Q3 view: error rates by pipeline
CREATE OR REPLACE VIEW v_q3_summary AS
SELECT
    r.pipeline,
    q.log_date,
    q.log_hour,
    q.error_request_count,
    q.total_request_count,
    ROUND((q.error_rate * 100)::NUMERIC, 2) AS error_rate_pct,
    q.distinct_error_hosts
FROM q3_hourly_errors q
JOIN etl_runs r ON r.run_id = q.run_id
ORDER BY r.pipeline, q.log_date, q.log_hour;
