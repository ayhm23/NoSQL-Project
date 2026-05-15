raw = LOAD '/Users/sanyam/NoSQL-Project/data' USING PigStorage('\t') AS (
    host:chararray, log_date:chararray, log_hour:int, http_method:chararray,
    resource_path:chararray, protocol:chararray, status_code:int, bytes_transferred:long
);

clean = FILTER raw BY resource_path IS NOT NULL AND resource_path != '' AND host IS NOT NULL;

grp_main = GROUP clean BY resource_path;
agg_main = FOREACH grp_main {
    dist_hosts = DISTINCT clean.host;
    GENERATE
        group AS resource_path:chararray,
        (long)COUNT(clean) AS request_count:long,
        (long)SUM(clean.bytes_transferred) AS total_bytes:long,
        (long)COUNT(dist_hosts) AS distinct_hosts:long;
};

sorted = ORDER agg_main BY request_count DESC;
top20 = LIMIT sorted 20;
STORE top20 INTO '/tmp/q2_nested_out' USING PigStorage('\t');
