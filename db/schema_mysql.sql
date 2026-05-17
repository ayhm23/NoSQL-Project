-- TARGET DIALECT: MySQL 8.0+
-- This schema supersedes the previous version and changes column names for spec compliance.

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id              VARCHAR(64)   PRIMARY KEY,
    pipeline            VARCHAR(32)   NOT NULL,
    batch_size          INT           NOT NULL,
    total_records       BIGINT        NOT NULL,
    malformed_records   BIGINT        NOT NULL,
    num_batches         INT           NOT NULL,
    avg_batch_size      DECIMAL(12,2) NOT NULL,
    runtime_seconds     DECIMAL(12,3) NOT NULL,
    started_at          DATETIME(3)   NOT NULL,
    completed_at        DATETIME(3)   NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS batch_metadata (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(64)   NOT NULL,
    pipeline            VARCHAR(32)   NOT NULL,
    batch_id            INT           NOT NULL,
    batch_size_config   INT           NOT NULL,
    records_in_batch    INT           NOT NULL,
    started_at          DATETIME(3)   NULL,
    completed_at        DATETIME(3)   NULL,
    CONSTRAINT fk_bm_run_id FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    INDEX idx_bm_run_id (run_id),
    INDEX idx_bm_batch_id (run_id, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id          VARCHAR(64)  NOT NULL,
    pipeline        VARCHAR(32)  NOT NULL,
    query_name      VARCHAR(16)  NOT NULL DEFAULT 'q1',
    log_date        DATE         NOT NULL,
    status_code     SMALLINT     NOT NULL,
    request_count   BIGINT       NOT NULL,
    total_bytes     BIGINT       NOT NULL,
    CONSTRAINT fk_q1_run_id FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    INDEX idx_q1_run_id (run_id),
    INDEX idx_q1_date   (log_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id                VARCHAR(64)   NOT NULL,
    pipeline              VARCHAR(32)   NOT NULL,
    query_name            VARCHAR(16)   NOT NULL DEFAULT 'q2',
    resource_path         VARCHAR(2048) NOT NULL,
    request_count         BIGINT        NOT NULL,
    total_bytes           BIGINT        NOT NULL,
    distinct_host_count   INT           NOT NULL,
    CONSTRAINT fk_q2_run_id FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    INDEX idx_q2_run_id       (run_id),
    INDEX idx_q2_request_count (request_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id                VARCHAR(64)   NOT NULL,
    pipeline              VARCHAR(32)   NOT NULL,
    query_name            VARCHAR(16)   NOT NULL DEFAULT 'q3',
    log_date              DATE          NOT NULL,
    log_hour              TINYINT       NOT NULL,
    error_request_count   BIGINT        NOT NULL,
    total_request_count   BIGINT        NOT NULL,
    error_rate            DECIMAL(10,6) NOT NULL,
    distinct_error_hosts  INT           NOT NULL,
    CONSTRAINT fk_q3_run_id FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    INDEX idx_q3_run_id   (run_id),
    INDEX idx_q3_date_hour (log_date, log_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS malformed_record_summary (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id                  VARCHAR(64)  NOT NULL,
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
    sample_lines            TEXT,
    CONSTRAINT fk_mal_run_id FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    INDEX idx_mal_run_id (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
