"""
pipelines/mapreduce_pipeline.py
================================
Hadoop MapReduce ETL Pipeline (Hadoop Streaming).

Architecture
────────────
1. LOAD  : Decompress .gz log files and upload plain-text data to HDFS.
           Simultaneously runs the shared Python batcher to compute
           batch statistics (total_batches, avg_batch_size, malformed count)
           that are stored in the run metadata.

2. QUERY : Run three independent Hadoop Streaming jobs, one per query.
           Each job uses a Python mapper + reducer shipped to the cluster
           via the -files flag so log_parser.py is available on every task node.

           Q1 — Daily Traffic Summary
           Q2 — Top 20 Resources (reducer outputs all; Python takes top 20)
           Q3 — Hourly Error Analysis

3. WRITE : Read tab-separated output from HDFS, parse into dicts, and
           persist to PostgreSQL via the shared ResultLoader.

Batching note
─────────────
The spec defines batch size as the number of input log records per batch.
Batching is applied during the LOAD phase: records are counted in fixed-size
batches using the shared batcher, and batch metadata is stored in etl_runs.
Hadoop processes the full uploaded dataset in a single distributed pass;
query results are tagged batch_id=0 (aggregated across all batches),
consistent with the MongoDB pipeline's convention.
"""

import glob
import gzip
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import config
from db.loader import ResultLoader
from parser.batcher import BatcherStats, generate_batches
from pipelines.base_pipeline import BasePipeline

logger = logging.getLogger(__name__)

# ── Paths inside this file (absolute so subprocess can find them) ──────────
_MR_DIR        = Path(__file__).resolve().parent.parent / "mapreduce"
_LOG_PARSER_PY = Path(__file__).resolve().parent.parent / "parser" / "log_parser.py"

# ── HDFS paths ─────────────────────────────────────────────────────────────
_HDFS_INPUT  = "/nasa_etl/input"
_HDFS_OUTPUT = "/nasa_etl/output"


def _find_streaming_jar() -> str:
    """
    Locate the Hadoop Streaming JAR.
    Checks HADOOP_STREAMING_JAR env var first, then walks HADOOP_HOME.
    """
    jar = os.getenv("HADOOP_STREAMING_JAR", "")
    if jar and Path(jar).exists():
        return jar

    hadoop_home = os.getenv("HADOOP_HOME", "")
    if hadoop_home:
        matches = glob.glob(
            f"{hadoop_home}/share/hadoop/tools/lib/hadoop-streaming*.jar"
        )
        if matches:
            return matches[0]

    raise FileNotFoundError(
        "Hadoop Streaming JAR not found. "
        "Set HADOOP_STREAMING_JAR=/path/to/hadoop-streaming-*.jar in your environment."
    )


def _run(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command, stream output to logger, raise on failure."""
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',  # ← ADD THIS LINE: replace invalid UTF-8 with U+FFFD
    )
    for line in result.stdout.splitlines():
        logger.debug("  %s", line)
    if check and result.returncode != 0:
        logger.error("Command failed (exit %d):\n%s", result.returncode, result.stdout)
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}"
        )
    return result


def _hdfs(*args) -> subprocess.CompletedProcess:
    """Convenience wrapper for hdfs dfs commands."""
    return _run(["hdfs", "dfs"] + list(args))


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

class MapReducePipeline(BasePipeline):
    """
    Hadoop Streaming MapReduce pipeline.

    Runs three Hadoop Streaming jobs (Q1, Q2, Q3) over NASA HTTP log data
    uploaded to HDFS, then persists results to PostgreSQL.
    """

    PIPELINE_NAME = "mapreduce"

    def __init__(
        self,
        log_files=None,
        batch_size: int = None,
        streaming_jar: str = None,
        hdfs_input: str = _HDFS_INPUT,
        hdfs_output: str = _HDFS_OUTPUT,
        keep_hdfs: bool = False,
    ):
        super().__init__(
            log_files  = log_files  or config.LOG_FILES,
            batch_size = batch_size or config.BATCH_SIZE,
        )
        self._streaming_jar = streaming_jar or _find_streaming_jar()
        self._hdfs_input    = hdfs_input
        self._hdfs_output   = hdfs_output
        self._keep_hdfs     = keep_hdfs   # set True to inspect HDFS output after run

    # ── Phase 1 — Load ────────────────────────────────────────────────────────

    def _load_data(self, batch_size: int) -> None:
        """
        Decompress log files, count batches using the shared batcher,
        then upload the decompressed text to HDFS.
        """
        batcher_stats = BatcherStats()
        self._batcher_stats = batcher_stats

        # ── Step 1: Count batches using the shared batcher (for metadata) ──
        logger.info("Counting batches for metadata (batch_size=%d)...", batch_size)
        for _batch_id, _records in generate_batches(
            self.log_files, batch_size, batcher_stats
        ):
            pass   # we only need the stats, not the records

        logger.info(
            "Batch count complete: %d batches, %d parsed, %d malformed",
            batcher_stats.total_batches,
            batcher_stats.parse_stats.parsed_ok,
            batcher_stats.parse_stats.malformed,
        )

        # ── Step 2: Prepare HDFS ────────────────────────────────────────────
        logger.info("Preparing HDFS input directory: %s", self._hdfs_input)
        _hdfs("-rm", "-r", "-f", self._hdfs_input)
        _hdfs("-mkdir", "-p", self._hdfs_input)

        # ── Step 3: Decompress and upload each log file ─────────────────────
        with tempfile.TemporaryDirectory() as tmp_dir:
            for gz_path in self.log_files:
                gz_path = Path(gz_path)
                if not gz_path.exists():
                    raise FileNotFoundError(f"Log file not found: {gz_path}")

                # Decompress to a temp plain-text file
                plain_name = gz_path.stem   # e.g. NASA_access_log_Jul95
                plain_path = Path(tmp_dir) / plain_name
                logger.info("Decompressing %s → %s", gz_path.name, plain_path)

                with gzip.open(gz_path, "rb") as f_in, \
                     open(plain_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

                # Upload to HDFS
                hdfs_dest = f"{self._hdfs_input}/{plain_name}"
                logger.info("Uploading to HDFS: %s", hdfs_dest)
                _hdfs("-put", str(plain_path), hdfs_dest)

        logger.info("HDFS upload complete. Input: %s", self._hdfs_input)

    # ── Phase 2 — Queries ─────────────────────────────────────────────────────

    def _run_queries(self) -> Dict[str, List[Dict]]:
        """Run Q1, Q2, Q3 as separate Hadoop Streaming jobs."""
        results: Dict[str, List[Dict]] = {}

        results["q1_daily_traffic"]  = self._run_q1()
        results["q2_top_resources"]  = self._run_q2()
        results["q3_hourly_errors"]  = self._run_q3()

        return results

    def _run_streaming_job(
        self,
        job_name: str,
        mapper: str,
        reducer: str,
        output_subdir: str,
        num_reducers: int = 1,
    ) -> str:
        """
        Submit a Hadoop Streaming job and return the HDFS output path.

        Ships log_parser.py alongside the mapper/reducer scripts
        so parse_line() is available on every task node.
        """
        output_path = f"{self._hdfs_output}/{output_subdir}"

        # Remove any previous output for this job
        _hdfs("-rm", "-r", "-f", output_path)

        mapper_path  = str(_MR_DIR / mapper)
        reducer_path = str(_MR_DIR / reducer)

        cmd = [
            "hadoop", "jar", self._streaming_jar,
            "-D", f"mapreduce.job.name=NasaETL_{job_name}_{self.run_id[:8]}",
            "-D", f"mapreduce.job.reduces={num_reducers}",
            # Ship log_parser.py and the mapper/reducer to every task node
            "-files", f"{mapper_path},{reducer_path},{str(_LOG_PARSER_PY)}",
            "-input",   self._hdfs_input,
            "-output",  output_path,
            "-mapper",  f"python3 {mapper}",
            "-reducer", f"python3 {reducer}",
        ]

        logger.info("Submitting Hadoop job: %s", job_name)
        _run(cmd)
        logger.info("Job %s complete. Output: %s", job_name, output_path)
        return output_path

    def _read_hdfs_output(self, hdfs_path: str) -> List[str]:
        """Read all part-* output files from an HDFS path, return lines."""
        result = _run(["hdfs", "dfs", "-cat", f"{hdfs_path}/part-*"])
        return [l for l in result.stdout.splitlines() if l.strip()]

    # ── Q1 ─────────────────────────────────────────────────────────────────

    def _run_q1(self) -> List[Dict]:
        """
        Q1 — Daily Traffic Summary
        Output columns: log_date, status_code, request_count, total_bytes
        """
        out = self._run_streaming_job(
            job_name="Q1_DailyTraffic",
            mapper="q1_mapper.py",
            reducer="q1_reducer.py",
            output_subdir="q1",
        )

        rows = []
        for line in self._read_hdfs_output(out):
            parts = line.split('\t')
            if len(parts) != 4:
                logger.warning("Q1: unexpected output line: %s", line)
                continue
            log_date, status_code, request_count, total_bytes = parts
            try:
                rows.append({
                    "log_date":      log_date,
                    "status_code":   int(status_code),
                    "request_count": int(request_count),
                    "total_bytes":   int(total_bytes),
                })
            except ValueError as e:
                logger.warning("Q1: parse error on line %r: %s", line, e)

        logger.info("Q1 returned %d rows", len(rows))
        return rows

    # ── Q2 ─────────────────────────────────────────────────────────────────

    def _run_q2(self) -> List[Dict]:
        """
        Q2 — Top 20 Requested Resources
        Output columns: resource_path, request_count, total_bytes, distinct_hosts
        """
        out = self._run_streaming_job(
            job_name="Q2_TopResources",
            mapper="q2_mapper.py",
            reducer="q2_reducer.py",
            output_subdir="q2",
        )

        all_rows = []
        for line in self._read_hdfs_output(out):
            parts = line.split('\t')
            if len(parts) != 4:
                logger.warning("Q2: unexpected output line: %s", line)
                continue
            resource_path, request_count, total_bytes, distinct_hosts = parts
            try:
                all_rows.append({
                    "resource_path":  resource_path,
                    "request_count":  int(request_count),
                    "total_bytes":    int(total_bytes),
                    "distinct_hosts": int(distinct_hosts),
                })
            except ValueError as e:
                logger.warning("Q2: parse error on line %r: %s", line, e)

        # Sort by request_count DESC and take top 20
        all_rows.sort(key=lambda r: r["request_count"], reverse=True)
        top20 = all_rows[:20]

        logger.info("Q2 returned %d rows (from %d total resources)", len(top20), len(all_rows))
        return top20

    # ── Q3 ─────────────────────────────────────────────────────────────────

    def _run_q3(self) -> List[Dict]:
        """
        Q3 — Hourly Error Analysis
        Output columns: log_date, log_hour, error_count, total_requests,
                        error_rate, distinct_error_hosts
        """
        out = self._run_streaming_job(
            job_name="Q3_HourlyErrors",
            mapper="q3_mapper.py",
            reducer="q3_reducer.py",
            output_subdir="q3",
        )

        rows = []
        for line in self._read_hdfs_output(out):
            parts = line.split('\t')
            if len(parts) != 6:
                logger.warning("Q3: unexpected output line: %s", line)
                continue
            log_date, log_hour, error_count, total_requests, error_rate, distinct_error_hosts = parts
            try:
                rows.append({
                    "log_date":             log_date,
                    "log_hour":             int(log_hour),
                    "error_count":          int(error_count),
                    "total_requests":       int(total_requests),
                    "error_rate":           float(error_rate),
                    "distinct_error_hosts": int(distinct_error_hosts),
                })
            except ValueError as e:
                logger.warning("Q3: parse error on line %r: %s", line, e)

        # Sort by date, hour for consistent output
        rows.sort(key=lambda r: (r["log_date"], r["log_hour"]))
        logger.info("Q3 returned %d rows", len(rows))
        return rows

    # ── Phase 3 — Write Results ────────────────────────────────────────────

    def _write_results(self, results: Dict[str, List[Dict]]) -> None:
        """Persist Q1, Q2, Q3 results to PostgreSQL via ResultLoader."""
        exec_ts = self.execution_ts

        # Build run metadata
        bs = self._batcher_stats
        meta = {
            "run_id":            self.run_id,
            "pipeline":          self.PIPELINE_NAME,
            "batch_size":        self.batch_size,
            "total_records":     bs.parse_stats.total_lines,
            "malformed_records": bs.parse_stats.malformed,
            "num_batches":       bs.total_batches,
            "avg_batch_size":    bs.avg_batch_size,
            "runtime_seconds":   self.get_runtime(),
            "started_at":        exec_ts,
            "completed_at":      exec_ts,   # updated below after write
        }

        with ResultLoader() as loader:
            loader.save_run(meta)
            logger.info("Run metadata saved  run_id=%s", self.run_id)

            loader.save_q1(self.run_id, self.PIPELINE_NAME, results["q1_daily_traffic"])
            logger.info("Q1 saved: %d rows", len(results["q1_daily_traffic"]))

            loader.save_q2(self.run_id, self.PIPELINE_NAME, results["q2_top_resources"])
            logger.info("Q2 saved: %d rows", len(results["q2_top_resources"]))

            loader.save_q3(self.run_id, self.PIPELINE_NAME, results["q3_hourly_errors"])
            logger.info("Q3 saved: %d rows", len(results["q3_hourly_errors"]))

        logger.info("[mapreduce] Results persisted to relational DB")

        # Optionally clean up HDFS output
        if not self._keep_hdfs:
            logger.info("Cleaning up HDFS output directory: %s", self._hdfs_output)
            _hdfs("-rm", "-r", "-f", self._hdfs_output)