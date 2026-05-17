-- TARGET DIALECT: PostgreSQL 13+
-- This schema supersedes the previous version and changes column names for spec compliance.

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id              VARCHAR(64)   PRIMARY KEY,
    pipeline            VARCHAR(32)   NOT NULL,
    batch_size          INT           NOT NULL,
    total_records       BIGINT        NOT NULL,
    malformed_records   BIGINT        NOT NULL,
    num_batches         INT           NOT NULL,
    avg_batch_size      NUMERIC(12,2) NOT NULL,
    runtime_seconds     NUMERIC(12,3) NOT NULL,
    started_at          TIMESTAMPTZ   NOT NULL,
    completed_at        TIMESTAMPTZ   NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_metadata (
    id                  BIGSERIAL     PRIMARY KEY,
    run_id              VARCHAR(64)   NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    pipeline            VARCHAR(32)   NOT NULL,
    batch_id            INT           NOT NULL,
    batch_size_config   INT           NOT NULL,
    records_in_batch    INT           NOT NULL,
    started_at          TIMESTAMPTZ   NULL,
    completed_at        TIMESTAMPTZ   NULL
);
CREATE INDEX IF NOT EXISTS idx_bm_run_id ON batch_metadata (run_id);
CREATE INDEX IF NOT EXISTS idx_bm_batch_id ON batch_metadata (run_id, batch_id);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id              BIGSERIAL    PRIMARY KEY,
    run_id          VARCHAR(64)  NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    pipeline        VARCHAR(32)  NOT NULL,
    query_name      VARCHAR(16)  NOT NULL DEFAULT 'q1',
    log_date        DATE         NOT NULL,
    status_code     SMALLINT     NOT NULL,
    request_count   BIGINT       NOT NULL,
    total_bytes     BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_q1_run_id ON q1_daily_traffic (run_id);
CREATE INDEX IF NOT EXISTS idx_q1_date ON q1_daily_traffic (log_date);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                    BIGSERIAL     PRIMARY KEY,
    run_id                VARCHAR(64)   NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    pipeline              VARCHAR(32)   NOT NULL,
    query_name            VARCHAR(16)   NOT NULL DEFAULT 'q2',
    resource_path         VARCHAR(2048) NOT NULL,
    request_count         BIGINT        NOT NULL,
    total_bytes           BIGINT        NOT NULL,
    distinct_host_count   INT           NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_q2_run_id ON q2_top_resources (run_id);
CREATE INDEX IF NOT EXISTS idx_q2_request_count ON q2_top_resources (request_count DESC);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                    BIGSERIAL     PRIMARY KEY,
    run_id                VARCHAR(64)   NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    pipeline              VARCHAR(32)   NOT NULL,
    query_name            VARCHAR(16)   NOT NULL DEFAULT 'q3',
    log_date              DATE          NOT NULL,
    log_hour              SMALLINT      NOT NULL,
    error_request_count   BIGINT        NOT NULL,
    total_request_count   BIGINT        NOT NULL,
    error_rate            NUMERIC(10,6) NOT NULL,
    distinct_error_hosts  INT           NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_q3_run_id ON q3_hourly_errors (run_id);
CREATE INDEX IF NOT EXISTS idx_q3_date_hour ON q3_hourly_errors (log_date, log_hour);

CREATE TABLE IF NOT EXISTS malformed_record_summary (
    id                      BIGSERIAL    PRIMARY KEY,
    run_id                  VARCHAR(64)  NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    pipeline                VARCHAR(32)  NOT NULL,
    total_malformed         BIGINT       NOT NULL,
    empty_line_count        BIGINT       NOT NULL DEFAULT 0,
    missing_brackets_count  BIGINT       NOT NULL DEFAULT 0,
    missing_quotes_count    BIGINT       NOT NULL DEFAULT 0,
    bad_timestamp_count     BIGINT       NOT NULL DEFAULT 0,
    bad_status_count        BIGINT       NOT NULL DEFAULT 0,
    bad_bytes_count         BIGINT       NOT NULL DEFAULT 0,
    truncated_count         BIGINT       NOT NULL DEFAULT 0,
    unknown_count           BIGINT       NOT NULL DEFAULT 0,
    sample_lines            TEXT
);
CREATE INDEX IF NOT EXISTS idx_mal_run_id ON malformed_record_summary (run_id);