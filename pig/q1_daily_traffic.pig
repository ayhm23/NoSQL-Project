-- =============================================================================
-- Q1: Daily Traffic Summary
-- =============================================================================
-- Input columns (TSV, produced by pig_pipeline.py _load_data):
--   host, log_date, log_hour, http_method, resource_path,
--   protocol, status_code, bytes_transferred
--
-- Output columns (TSV, written to $output_dir):
--   log_date, status_code, request_count, total_bytes
--
-- Parameters (passed via `pig -param`):
--   input_dir   — HDFS path to the batch TSV files
--   output_dir  — HDFS path for this query's output
-- =============================================================================

-- ── Load ─────────────────────────────────────────────────────────────────────
raw = LOAD '$input_dir' USING PigStorage('\t') AS (
    host:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol:chararray,
    status_code:int,
    bytes_transferred:long
);

-- ── Clean: drop rows with null date or status (true parse failures) ──────────
clean = FILTER raw BY log_date IS NOT NULL
                  AND log_date != ''
                  AND status_code IS NOT NULL;

-- ── Group by (log_date, status_code) ────────────────────────────────────────
grouped = GROUP clean BY (log_date, status_code);

-- ── Aggregate ────────────────────────────────────────────────────────────────
q1_result = FOREACH grouped GENERATE
    FLATTEN(group)                           AS (log_date:chararray,
                                                 status_code:int),
    (long)COUNT(clean)                       AS request_count:long,
    (long)SUM(clean.bytes_transferred)       AS total_bytes:long;

-- ── Sort for deterministic output ────────────────────────────────────────────
sorted = ORDER q1_result BY log_date ASC, status_code ASC;

-- ── Store ────────────────────────────────────────────────────────────────────
STORE sorted INTO '$output_dir' USING PigStorage('\t');
