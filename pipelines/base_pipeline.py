"""
pipelines/base_pipeline.py — Abstract base class for all four ETL pipelines.

Every pipeline (Pig, MapReduce, MongoDB, Hive) must subclass BasePipeline
and implement the three abstract methods below.  The orchestration contract
(run, timing, result writing) is enforced here so each subclass only needs
to worry about its technology-specific logic.
"""

import abc
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BasePipeline(abc.ABC):
    """
    Abstract ETL pipeline base class.

    Subclasses must implement:
        _load_data(batch_size)      — parse + ingest raw logs
        _run_queries()              — execute Q1, Q2, Q3 and return raw results
        _write_results(results)     — persist results to MySQL/PostgreSQL

    The `run()` method orchestrates all three steps and measures wall-clock
    runtime from first file read to last DB write (exclusive of download,
    install, and report-rendering time as per the spec).
    """

    #: Human-readable name used as the `pipeline` column in result tables.
    PIPELINE_NAME: str = "base"

    def __init__(self, log_files: List[Path], batch_size: int):
        self.log_files  = log_files
        self.batch_size = batch_size
        self.run_id     = str(uuid.uuid4())
        self._start_ts: Optional[float] = None
        self._end_ts:   Optional[float] = None
        self._batcher_stats = None   # set by subclass after _load_data

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """
        Full pipeline execution.  Returns a summary dict with runtime and
        batch stats.  Raises on any unrecoverable error.
        """
        logger.info(
            "[%s] Starting pipeline run_id=%s", self.PIPELINE_NAME, self.run_id
        )
        self._start_ts = time.perf_counter()

        logger.info("[%s] Phase 1 — Load data", self.PIPELINE_NAME)
        self._load_data(self.batch_size)

        logger.info("[%s] Phase 2 — Run queries", self.PIPELINE_NAME)
        results = self._run_queries()

        logger.info("[%s] Phase 3 — Write results to DB", self.PIPELINE_NAME)
        self._write_results(results)

        self._end_ts = time.perf_counter()
        elapsed = self.get_runtime()

        summary = {
            "pipeline":  self.PIPELINE_NAME,
            "run_id":    self.run_id,
            "runtime_s": round(elapsed, 3),
        }
        if self._batcher_stats is not None:
            summary.update(self._batcher_stats.summary())

        logger.info(
            "[%s] Pipeline complete in %.2fs  %s",
            self.PIPELINE_NAME, elapsed, summary,
        )
        return summary

    def get_runtime(self) -> float:
        """Wall-clock seconds from first file read to last DB write."""
        if self._start_ts is None or self._end_ts is None:
            return 0.0
        return self._end_ts - self._start_ts

    @property
    def execution_ts(self) -> str:
        """ISO-8601 timestamp of when the run started (UTC)."""
        if self._start_ts is None:
            return ""
        # Convert perf_counter offset to wall time
        now_wall  = datetime.now(timezone.utc)
        now_perf  = time.perf_counter()
        delta     = now_perf - self._start_ts
        run_start = datetime.fromtimestamp(
            now_wall.timestamp() - delta, tz=timezone.utc
        )
        return run_start.isoformat()

    # ── Abstract methods ──────────────────────────────────────────────────────

    @abc.abstractmethod
    def _load_data(self, batch_size: int) -> None:
        """
        Parse log files and ingest into the pipeline's native storage.
        Must set self._batcher_stats when done.
        """

    @abc.abstractmethod
    def _run_queries(self) -> Dict[str, List[Dict]]:
        """
        Execute Q1 (daily traffic), Q2 (top-20 resources), Q3 (hourly errors).
        Return a dict keyed by query name, each value a list of result rows.

        Expected keys: "q1_daily_traffic", "q2_top_resources", "q3_hourly_errors"
        """

    @abc.abstractmethod
    def _write_results(self, results: Dict[str, List[Dict]]) -> None:
        """
        Persist query results to MySQL/PostgreSQL using db/loader.py.
        Must include pipeline, run_id, batch_id, and execution timestamp
        in every row.
        """
