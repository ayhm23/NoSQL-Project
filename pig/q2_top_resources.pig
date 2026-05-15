-- =============================================================================
-- Q2: Top 20 Requested Resources (Fixed with Safe Nested Distinct)
-- =============================================================================

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

clean = FILTER raw BY resource_path IS NOT NULL
                  AND resource_path != ''
                  AND host IS NOT NULL;

-- ── Safely calculate all metrics in a single pass ────────────────────────────
grp_main = GROUP clean BY resource_path;

agg_main = FOREACH grp_main {
    dist_hosts = DISTINCT clean.host;
    -- SUM can return null if all values are null, which crashes ORDER BY serialization
    sum_bytes = SUM(clean.bytes_transferred);
    GENERATE
        group AS resource_path:chararray,
        COUNT(clean) AS request_count:long,
        (sum_bytes is null ? 0L : (long)sum_bytes) AS total_bytes:long,
        COUNT(dist_hosts) AS distinct_hosts:long;
};

-- ── Sort and Limit ───────────────────────────────────────────────────────────
q2_result = ORDER agg_main BY request_count DESC;
final_sorted = LIMIT q2_result 20;

STORE final_sorted INTO '$output_dir' USING PigStorage('\t');
