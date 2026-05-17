"""
pipelines/mongo_pipeline.py — Full MongoDB ETL Pipeline.

Architecture
────────────
1. LOAD  : Stream NASA log files through the shared parser → insert LogRecord
           documents into MongoDB in configurable batch sizes.

2. QUERY : Run three MongoDB Aggregation Pipeline queries directly against
           the raw_logs collection — no intermediate transformations stored.

   Q1 — Daily Traffic Summary
        Group by (log_date, status_code)
        → request_count, total_bytes

   Q2 — Top 20 Resources
        Group by resource_path
        → request_count, total_bytes, distinct_hosts   (top 20 by request_count)

   Q3 — Hourly Error Analysis
        Filter status_code 400–599, group by (log_date, log_hour)
        → error_count, total_requests (from outer join), error_rate,
          distinct_error_hosts

3. WRITE : Persist results to MySQL / PostgreSQL via db/loader.py,
           tagging every row with pipeline, run_id, batch_id, executed_at.

Design notes
────────────
• Indexes are created before ingestion so MongoDB can build them incrementally.
• Bulk inserts use insert_many with ordered=False for maximum throughput.
• Aggregation pipelines use $group → $sort → $limit patterns that MongoDB
  can optimise with the indexes defined.
• Q3 error_rate requires knowing total_requests per (date, hour) bucket, so
  we use a $lookup / $facet approach to keep it in one pass.
• All aggregation pipelines are defined as module-level constants (QUERY_*)
  so they can be unit-tested independently of a live MongoDB instance.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

import config
from parser.batcher import BatcherStats, generate_batches
from pipelines.base_pipeline import BasePipeline
from db.loader import ResultLoader

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation pipeline definitions (module-level for testability)
# ─────────────────────────────────────────────────────────────────────────────

#: Q1 — Daily Traffic Summary
#: Per (log_date, status_code) → request_count, total_bytes
QUERY_Q1_DAILY_TRAFFIC: List[Dict] = [
    {
        "$group": {
            "_id": {
                "log_date":    "$log_date",
                "status_code": "$status_code",
            },
            "request_count": {"$sum": 1},
            "total_bytes":   {"$sum": "$bytes_transferred"},
        }
    },
    {
        "$project": {
            "_id":           0,
            "log_date":      "$_id.log_date",
            "status_code":   "$_id.status_code",
            "request_count": 1,
            "total_bytes":   1,
        }
    },
    {
        "$sort": {"log_date": ASCENDING, "status_code": ASCENDING}
    },
]

#: Q2 — Top 20 Resources by hit count
#: Per resource_path → request_count, total_bytes, distinct_hosts
QUERY_Q2_TOP_RESOURCES: List[Dict] = [
    {
        "$group": {
            "_id": "$resource_path",
            "request_count":  {"$sum": 1},
            "total_bytes":    {"$sum": "$bytes_transferred"},
            "distinct_hosts": {"$addToSet": "$host"},
        }
    },
    {
        "$project": {
            "_id":            0,
            "resource_path":  "$_id",
            "request_count":  1,
            "total_bytes":    1,
            "distinct_hosts": {"$size": "$distinct_hosts"},
        }
    },
    {
        "$sort": {"request_count": DESCENDING}
    },
    {
        "$limit": 20
    },
]

#: Q3 — Hourly Error Analysis (status 400–599)
#: Per (log_date, log_hour) → error_count, total_requests, error_rate,
#:                              distinct_error_hosts
#:
#: Strategy: two $facet branches in a single aggregation pass:
#:   "errors"  — filter 400–599, group by (date, hour)
#:   "totals"  — group ALL records by (date, hour)
#: Then $unwind + $lookup-style $map to join them and compute error_rate.
#:
#: Because $lookup inside an aggregation pipeline requires a collection
#: reference, we use a two-stage Python-side join instead, which is simpler
#: and avoids writing an intermediate collection.  The two sub-pipelines run
#: sequentially; each is O(N) over the already-indexed collection.

QUERY_Q3_ERRORS_STAGE: List[Dict] = [
    {
        "$match": {
            "status_code": {"$gte": 400, "$lte": 599}
        }
    },
    {
        "$group": {
            "_id": {
                "log_date": "$log_date",
                "log_hour": "$log_hour",
            },
            "error_count":        {"$sum": 1},
            "distinct_error_hosts": {"$addToSet": "$host"},
        }
    },
    {
        "$project": {
            "_id":                 0,
            "log_date":            "$_id.log_date",
            "log_hour":            "$_id.log_hour",
            "error_count":         1,
            "distinct_error_hosts": {"$size": "$distinct_error_hosts"},
        }
    },
    {
        "$sort": {"log_date": ASCENDING, "log_hour": ASCENDING}
    },
]

QUERY_Q3_TOTALS_STAGE: List[Dict] = [
    {
        "$group": {
            "_id": {
                "log_date": "$log_date",
                "log_hour": "$log_hour",
            },
            "total_requests": {"$sum": 1},
        }
    },
    {
        "$project": {
            "_id":            0,
            "log_date":       "$_id.log_date",
            "log_hour":       "$_id.log_hour",
            "total_requests": 1,
        }
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline class
# ─────────────────────────────────────────────────────────────────────────────

class MongoPipeline(BasePipeline):
    """
    MongoDB ETL pipeline implementation.

    Full lifecycle:
        1. Connect to MongoDB (lazy, validated on first use)
        2. Drop + recreate the raw_logs collection
        3. Create indexes for fast aggregation
        4. Stream + insert parsed records in batches
        5. Run Q1, Q2, Q3 aggregation pipelines
        6. Write results to MySQL / PostgreSQL via ResultLoader
        7. Drop the raw_logs collection to free memory (optional, see config)
    """

    PIPELINE_NAME = "mongodb"

    def __init__(
        self,
        log_files=None,
        batch_size: int = None,
        mongo_uri:   str = None,
        mongo_db:    str = None,
        mongo_coll:  str = None,
        drop_after:  bool = True,
        **kwargs,
    ):
        super().__init__(
            log_files  = log_files  or config.LOG_FILES,
            batch_size = batch_size or config.BATCH_SIZE,
            **kwargs,
        )
        self._mongo_uri  = mongo_uri  or config.MONGO_URI
        self._mongo_db   = mongo_db   or config.MONGO_DB
        self._mongo_coll = mongo_coll or config.MONGO_COLL
        self._drop_after = drop_after

        self._client: Optional[MongoClient] = None
        self._db     = None
        self._coll   = None

        # Populated by _load_data; used by _write_results for batch tagging
        self._total_batches_loaded: int = 0

    # ── Connection helpers ────────────────────────────────────────────────────

    def _connect(self) -> None:
        """Establish MongoDB connection and pin db/collection references."""
        if self._client is not None:
            return
        logger.info("Connecting to MongoDB at %s", self._mongo_uri)
        self._client = MongoClient(
            self._mongo_uri,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
        )
        # Force immediate connection check
        self._client.admin.command("ping")
        self._db   = self._client[self._mongo_db]
        self._coll = self._db[self._mongo_coll]
        logger.info(
            "Connected — db=%s  collection=%s",
            self._mongo_db, self._mongo_coll,
        )

    def _disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB connection closed")

    def _create_indexes(self) -> None:
        """
        Create compound indexes that accelerate all three aggregation pipelines.
        Created before bulk insert so MongoDB builds them incrementally.
        """
        indexes = [
            # Q1 — group by (log_date, status_code)
            [("log_date", ASCENDING), ("status_code", ASCENDING)],
            # Q2 — group by resource_path, sort by count (scan index)
            [("resource_path", ASCENDING)],
            # Q3 — filter status_code range, group by (log_date, log_hour)
            [("status_code", ASCENDING), ("log_date", ASCENDING), ("log_hour", ASCENDING)],
            # General sort / filter support
            [("host", ASCENDING)],
        ]
        for key_spec in indexes:
            self._coll.create_index(key_spec, background=True)
            logger.debug("Index created: %s", key_spec)

    # ── Phase 1 — Load ────────────────────────────────────────────────────────

    def _load_data(self, batch_size: int) -> None:
        """
        Stream all log files → parse → insert into MongoDB in batches.
        Each batch is inserted atomically with ordered=False (continues on
        duplicate-key errors if any).
        """
        self._connect()

        # Fresh collection for this run
        logger.info("Dropping existing collection '%s'", self._mongo_coll)
        self._coll.drop()
        self._create_indexes()

        batcher_stats = BatcherStats()
        self._batcher_stats = batcher_stats

        total_inserted = 0
        for batch_id, records in generate_batches(
            self.log_files, batch_size, batcher_stats
        ):
            docs = [r.to_dict() for r in records]
            docs_count = len(docs)

            try:
                result = self._coll.insert_many(docs, ordered=False)
                n_inserted = len(result.inserted_ids)
            except BulkWriteError as bwe:
                # ordered=False: some docs inserted, some failed
                n_inserted = bwe.details.get("nInserted", 0)
                logger.warning(
                    "Batch %d: BulkWriteError — %d inserted, %d errors",
                    batch_id, n_inserted, len(bwe.details.get("writeErrors", [])),
                )

            total_inserted += n_inserted
            logger.info(
                "Batch %d: %d/%d records inserted (total so far: %d)",
                batch_id, n_inserted, docs_count, total_inserted,
            )

        self._total_batches_loaded = batcher_stats.total_batches
        logger.info(
            "Load complete — %d records in %d batches (malformed=%d)",
            total_inserted,
            batcher_stats.total_batches,
            batcher_stats.parse_stats.malformed,
        )

    # ── Phase 2 — Queries ─────────────────────────────────────────────────────

    def _run_queries(self) -> Dict[str, List[Dict]]:
        """Execute all three aggregation pipelines and return raw result lists."""
        results: Dict[str, List[Dict]] = {}

        # Q1
        if "q1" in self.selected_queries:
            logger.info("Running Q1 — Daily Traffic Summary")
            results["q1_daily_traffic"] = list(
                self._coll.aggregate(
                    QUERY_Q1_DAILY_TRAFFIC,
                    allowDiskUse=True,
                )
            )
            logger.info("Q1 returned %d rows", len(results["q1_daily_traffic"]))
        else:
            results["q1_daily_traffic"] = None

        # Q2
        if "q2" in self.selected_queries:
            logger.info("Running Q2 — Top 20 Resources")
            results["q2_top_resources"] = list(
                self._coll.aggregate(
                    QUERY_Q2_TOP_RESOURCES,
                    allowDiskUse=True,
                )
            )
            logger.info("Q2 returned %d rows", len(results["q2_top_resources"]))

            # debug
            if results["q2_top_resources"]:
                logger.info("Q2 sample row keys: %s", list(results["q2_top_resources"][0].keys()))
        else:
            results["q2_top_resources"] = None

        # Q3 — two-pass, Python-side join
        if "q3" in self.selected_queries:
            logger.info("Running Q3 — Hourly Error Analysis")
            results["q3_hourly_errors"] = self._run_q3()
            logger.info("Q3 returned %d rows", len(results["q3_hourly_errors"]))
        else:
            results["q3_hourly_errors"] = None

        return results

    def _run_q3(self) -> List[Dict]:
        """
        Q3 requires joining error-bucket counts with total-request counts.
        We run two lightweight aggregations and merge in Python — this avoids
        writing an intermediate collection while remaining correct.
        """
        # Pass A: error counts per (date, hour)
        errors: Dict[tuple, Dict] = {}
        for row in self._coll.aggregate(QUERY_Q3_ERRORS_STAGE, allowDiskUse=True):
            key = (row["log_date"], row["log_hour"])
            errors[key] = row

        # Pass B: total request counts per (date, hour) — all status codes
        totals: Dict[tuple, int] = {}
        for row in self._coll.aggregate(QUERY_Q3_TOTALS_STAGE, allowDiskUse=True):
            key = (row["log_date"], row["log_hour"])
            totals[key] = row["total_requests"]

        # Merge: only emit (date, hour) buckets that had at least one error
        merged: List[Dict] = []
        for key, err_row in sorted(errors.items()):
            total = totals.get(key, err_row["error_count"])
            error_rate = err_row["error_count"] / total if total else 0.0
            merged.append({
                "log_date":            err_row["log_date"],
                "log_hour":            err_row["log_hour"],
                "error_count":         err_row["error_count"],
                "total_requests":      total,
                "error_rate":          round(error_rate, 6),
                "distinct_error_hosts": err_row["distinct_error_hosts"],
            })
        return merged

    # ── Phase 3 — Write Results ───────────────────────────────────────────────

    def _write_results(self, results: Dict[str, List[Dict]]) -> None:
        """
        Persist aggregated ETL results to MySQL/PostgreSQL using the
        compliant ResultLoader save_* methods.
        """
        # Calculate final runtime
        completed_at = datetime.now(timezone.utc)
        runtime_s = (completed_at - self.execution_ts).total_seconds()

        # Prepare metadata for etl_runs
        # stats are stored in self._batcher_stats
        meta = {
            "run_id":            self.run_id,
            "pipeline":          self.PIPELINE_NAME,
            "batch_size":        self.batch_size,
            "total_records":     self._batcher_stats.parse_stats.total_lines,
            "malformed_records": self._batcher_stats.parse_stats.malformed,
            "num_batches":       self._batcher_stats.total_batches,
            "avg_batch_size":    self._batcher_stats.avg_batch_size,
            "runtime_seconds":   runtime_s,
            "started_at":        self.execution_ts,
            "completed_at":      completed_at
        }

        with ResultLoader() as loader:
            # 1. Save run metadata first (FK constraint)
            loader.save_run(meta)

            # 2. Save Q1 (Daily Traffic)
            # Keys match exactly: log_date, status_code, request_count, total_bytes
            if results.get("q1_daily_traffic") is not None:
                loader.save_q1(self.run_id, self.PIPELINE_NAME, results["q1_daily_traffic"])

            # 3. Save Q2 (Top Resources)
            # Need to rename 'distinct_hosts' -> 'distinct_host_count'
            if results.get("q2_top_resources") is not None:
                q2_rows = []
                for r in results["q2_top_resources"]:
                    q2_rows.append({
                        "resource_path":       r["resource_path"],
                        "request_count":       r["request_count"],
                        "total_bytes":         r["total_bytes"],
                        "distinct_host_count": r["distinct_hosts"]
                    })
                loader.save_q2(self.run_id, self.PIPELINE_NAME, q2_rows)

            # 4. Save Q3 (Hourly Errors)
            # Need to rename keys to match standard
            if results.get("q3_hourly_errors") is not None:
                q3_rows = []
                for r in results["q3_hourly_errors"]:
                    q3_rows.append({
                        "log_date":            r["log_date"],
                        "log_hour":            r["log_hour"],
                        "error_request_count": r["error_count"],
                        "total_request_count": r["total_requests"],
                        "error_rate":          r["error_rate"],
                        "distinct_error_hosts": r["distinct_error_hosts"]
                    })
                loader.save_q3(self.run_id, self.PIPELINE_NAME, q3_rows)

        logger.info("[%s] Results persisted to relational DB", self.PIPELINE_NAME)

        # Optionally drop the raw collection to free disk space
        if self._drop_after:
            logger.info(
                "Dropping raw_logs collection to free space "
                "(set drop_after=False to retain)"
            )
            self._coll.drop()

        self._disconnect()
