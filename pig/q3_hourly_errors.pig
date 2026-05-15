-- =============================================================================
-- Q3: Hourly Error Analysis
-- =============================================================================
-- Input columns (TSV):
--   host, log_date, log_hour, http_method, resource_path,
--   protocol, status_code, bytes_transferred
--
-- Output columns (TSV):
--   log_date, log_hour, error_count, total_requests,
--   error_rate, distinct_error_hosts
--
-- "Errors" are defined as status_code in [400, 599] inclusive.
-- error_rate = error_count / total_requests  (per date+hour bucket)
--
-- The three-way join strategy:
--   1. totals       — total requests per (date, hour)
--   2. err_agg      — error request count per (date, hour)
--   3. dist_err_hosts — distinct hosts with errors per (date, hour)
-- All three are joined on (log_date, log_hour).
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

-- ── Clean ────────────────────────────────────────────────────────────────────
clean = FILTER raw BY log_date IS NOT NULL
                  AND log_date != ''
                  AND status_code IS NOT NULL;

-- ── Relation 1: Total requests per (log_date, log_hour) ──────────────────────
grp_total  = GROUP clean BY (log_date, log_hour);
totals     = FOREACH grp_total GENERATE
    FLATTEN(group)            AS (log_date:chararray, log_hour:int),
    (long)COUNT(clean)        AS total_requests:long;

-- ── Filter to errors only ────────────────────────────────────────────────────
errors = FILTER clean BY status_code >= 400 AND status_code <= 599;

-- ── Relation 2: Error count per (log_date, log_hour) ─────────────────────────
grp_err = GROUP errors BY (log_date, log_hour);
err_agg = FOREACH grp_err GENERATE
    FLATTEN(group)            AS (log_date:chararray, log_hour:int),
    (long)COUNT(errors)       AS error_count:long;

-- ── Relation 3: Distinct error hosts per (log_date, log_hour) ────────────────
--   Project, deduplicate, then count — same pattern as Q2 distinct hosts.
err_host_dt    = FOREACH errors GENERATE log_date, log_hour, host;
dist_eh        = DISTINCT err_host_dt;
grp_deh        = GROUP dist_eh BY (log_date, log_hour);
dist_err_hosts = FOREACH grp_deh GENERATE
    FLATTEN(group)            AS (log_date:chararray, log_hour:int),
    (long)COUNT(dist_eh)      AS distinct_error_hosts:long;

-- ── Three-way join ────────────────────────────────────────────────────────────
--   LEFT OUTER on totals ensures we keep all (date, hour) buckets that have
--   errors even if the distinct-host relation is unexpectedly sparse.
j1 = JOIN err_agg      BY (log_date, log_hour),
          totals        BY (log_date, log_hour);

j2 = JOIN j1            BY (err_agg::log_date, err_agg::log_hour),
          dist_err_hosts BY (log_date,           log_hour);

-- ── Compute error_rate and project final columns ──────────────────────────────
q3_raw = FOREACH j2 GENERATE
    err_agg::log_date                                AS log_date:chararray,
    err_agg::log_hour                                AS log_hour:int,
    err_agg::error_count                             AS error_count:long,
    totals::total_requests                           AS total_requests:long,
    (double)err_agg::error_count /
    (double)totals::total_requests                   AS error_rate:double,
    dist_err_hosts::distinct_error_hosts             AS distinct_error_hosts:long;

-- Safety: drop any rows where join produced a null date
q3_result = FILTER q3_raw BY log_date IS NOT NULL;

-- ── Sort ─────────────────────────────────────────────────────────────────────
sorted = ORDER q3_result BY log_date ASC, log_hour ASC;

-- ── Store ────────────────────────────────────────────────────────────────────
STORE sorted INTO '$output_dir' USING PigStorage('\t');
