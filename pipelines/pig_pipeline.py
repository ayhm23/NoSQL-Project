"""
pipelines/pig_pipeline.py
=========================
Apache Pig ETL pipeline for the NASA Log Analytics project.

Changes vs original
-------------------
v1 → v2 (from changes.md):
  • parse_line() returns (LogRecord, malformed_flag) tuple — unpacked correctly.
  • Binary paths use config.HADOOP_HOME/bin/hdfs and config.PIG_HOME/bin/pig
    so subprocess doesn't rely on .zshrc PATH.
  • HADOOP_CONF_DIR injected into the pig subprocess environment so Pig reads
    core-site.xml and connects to HDFS instead of local fs.

v2 → v3 (this file — fixes 0-row output bug):
  • _record_to_row: handles both 'ip'/'host' field name variants, and parses
    timestamp when it arrives as a string instead of a datetime object.
    This was the root cause of all 0-row output — log_date was always "" so
    every record was dropped by Pig's FILTER clause.
  • _read_hdfs_output: lists the output directory first so a missing or empty
    result set produces a clear WARNING instead of a silent empty string.
  • _diagnose_hdfs_input: called once before Phase 2 — samples 2 rows from
    the first HDFS batch file so you can visually verify the TSV schema.
"""

import gzip
import io
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import config
from db.loader import ResultLoader
from parser.log_parser import parse_line
from pipelines.base_pipeline import BasePipeline

logger = logging.getLogger(__name__)

# NASA Common Log Format timestamp, e.g. "01/Jul/1995:00:00:01 -0400"
_NASA_TS_FORMAT = "%d/%b/%Y:%H:%M:%S"


# ─────────────────────────────────────────────────────────────────────────────
# Batch statistics
# ─────────────────────────────────────────────────────────────────────────────

class _PigBatchStatsSnapshot:
    def __init__(self, batch_id: int, record_count: int):
        self.batch_id = batch_id
        self.record_count = record_count


class _PigBatcherStats:
    def __init__(self, batch_size: int) -> None:
        self.batch_size        = batch_size
        self.total_records     = 0
        self.malformed_records = 0
        self.num_batches       = 0
        self.batch_stats       = []

    @property
    def parsed_ok(self) -> int:
        return self.total_records - self.malformed_records

    @property
    def avg_batch_size(self) -> float:
        return self.total_records / self.num_batches if self.num_batches else 0.0

    def summary(self) -> Dict[str, Any]:
        return {
            "parsed_ok":         self.parsed_ok,
            "malformed":         self.malformed_records,
            "total_batches":     self.num_batches,
            "avg_batch_size":    round(self.avg_batch_size, 2),
            # ResultLoader.save_run() keys
            "total_records":     self.total_records,
            "malformed_records": self.malformed_records,
            "num_batches":       self.num_batches,
        }


# ─────────────────────────────────────────────────────────────────────────────
# PigPipeline
# ─────────────────────────────────────────────────────────────────────────────

class PigPipeline(BasePipeline):
    PIPELINE_NAME = "pig"

    _TSV_COLUMNS = (
        "host", "log_date", "log_hour", "http_method",
        "resource_path", "protocol", "status_code", "bytes_transferred",
    )

    def __init__(self, log_files, batch_size: int, **kwargs) -> None:
        super().__init__(log_files, batch_size, **kwargs)

        _run_tag          = self.run_id[:8]
        self._hdfs_base   = f"{config.PIG_HDFS_BASE}/run_{_run_tag}"
        self._hdfs_input  = f"{self._hdfs_base}/input"
        self._hdfs_output = f"{self._hdfs_base}/output"
        self._pig_scripts  = Path(config.PIG_SCRIPTS_DIR)

        # Full binary paths — avoids subprocess not finding them without .zshrc
        self._hdfs_bin = str(Path(config.HADOOP_HOME) / "bin" / "hdfs")
        self._pig_bin  = str(Path(config.PIG_HOME)    / "bin" / "pig")

        # Environment passed to pig subprocess — must include HADOOP_CONF_DIR
        # so Pig reads core-site.xml and targets HDFS, not the local filesystem.
        self._pig_env = {
            **os.environ,
            "HADOOP_CONF_DIR": str(Path(config.HADOOP_HOME) / "etc" / "hadoop"),
            "JAVA_HOME":        os.environ.get("JAVA_HOME", ""),
        }

        self._batcher_stats = _PigBatcherStats(batch_size)

    # =========================================================================
    # Subprocess helpers
    # =========================================================================

    def _hdfs(self, *args: str) -> str:
        cmd = [self._hdfs_bin, "dfs"] + list(args)
        logger.debug("HDFS ▶ %s", " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"HDFS command failed (exit {proc.returncode}):\n"
                f"  cmd : {' '.join(cmd)}\n"
                f"  err : {proc.stderr.strip()}"
            )
        return proc.stdout

    def _run_pig(self, script_name: str, params: Dict[str, str]) -> None:
        script_path = str(self._pig_scripts / script_name)
        param_flags: List[str] = []
        for k, v in params.items():
            param_flags += ["-param", f"{k}={v}"]

        cmd = [self._pig_bin, "-x", "mapreduce"] + param_flags + ["-f", script_path]
        logger.info("Pig  ▶  %s", " ".join(cmd))

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=self._pig_env,   # injects HADOOP_CONF_DIR
        )
        for line in proc.stderr.splitlines():
            logger.debug("pig| %s", line)

        if proc.returncode != 0:
            stderr_text = proc.stderr
            tail = "\n".join(proc.stderr.splitlines()[-40:])
            
            # Exit code 3 with "FinalApplicationStatus=SUCCEEDED" means the actual
            # MapReduce jobs completed successfully, but Pig couldn't connect to the
            # Job History Server (port 10020) to fetch final metrics. This is a known
            # Pig 0.17.0 + Hadoop 2.10.2/3.x compatibility issue. Treat as a warning.
            if proc.returncode == 3 and "FinalApplicationStatus=SUCCEEDED" in stderr_text:
                logger.warning(
                    "Pig script '%s' exited with code 3 (history server unreachable), "
                    "but MapReduce jobs report SUCCEEDED. Treating as success and continuing...",
                    script_name
                )
            else:
                raise RuntimeError(
                    f"Pig script '{script_name}' failed (exit {proc.returncode}).\n"
                    f"Last Pig log lines:\n{tail}"
                )
        else:
            logger.info("Pig  ✓  %s", script_name)

    # =========================================================================
    # Phase 1 — _load_data
    # =========================================================================

    def _load_data(self, batch_size: int) -> None:
        stats = self._batcher_stats

        self._hdfs("-mkdir", "-p", self._hdfs_input)
        logger.info("HDFS input directory: %s", self._hdfs_input)

        batch: List[List] = []
        batch_num: int = 0

        def _flush() -> None:
            nonlocal batch_num
            if not batch:
                return
            batch_num += 1
            stats.num_batches += 1
            stats.batch_stats.append(_PigBatchStatsSnapshot(batch_num, len(batch)))
            self._upload_batch(batch, batch_num)
            logger.info("Batch %04d: %d records → HDFS", batch_num, len(batch))
            batch.clear()

        for log_file in self.log_files:
            log_file = Path(log_file)
            logger.info("Parsing %s …", log_file.name)
            opener = gzip.open if log_file.suffix == ".gz" else open

            with opener(log_file, "rt", encoding="latin-1", errors="replace") as fh:
                for raw_line in fh:
                    raw_line = raw_line.rstrip("\n")
                    stats.total_records += 1

                    # parse_line() return shape varies by parser version:
                    #   tuple → (LogRecord, malformed_bool)
                    #   dict  → direct record dict
                    result = parse_line(raw_line)
                    if result is None:
                        stats.malformed_records += 1
                        continue

                    if isinstance(result, tuple):
                        record_obj, malformed = result
                        if malformed or record_obj is None:
                            stats.malformed_records += 1
                            continue
                        record = record_obj.to_dict()
                    elif isinstance(result, dict):
                        record = result
                    else:
                        stats.malformed_records += 1
                        continue

                    batch.append(self._record_to_row(record))

                    if len(batch) >= batch_size:
                        _flush()

        _flush()

        logger.info(
            "Load complete — records: %d  malformed: %d  batches: %d  avg: %.0f",
            stats.total_records,
            stats.malformed_records,
            stats.num_batches,
            stats.avg_batch_size,
        )

    @staticmethod
    def _record_to_row(record: Dict) -> List:
        """
        Convert a parsed-record dict to an ordered TSV row.

        FIX (v3): Handles two root-cause bugs that produced all-empty log_date
        columns, causing Pig's FILTER to silently drop every record:

        1. Field-name variants across parser versions:
              host / ip
              method / http_method
              url / resource_path
              status / status_code
              size / bytes / bytes_transferred

        2. Timestamp as string instead of datetime object:
           parse_line() may return the raw NASA log timestamp string
           "01/Jul/1995:00:00:01 -0400" rather than a datetime.
           We parse it here with strptime before deriving log_date / log_hour.
        """
        # ── host ──────────────────────────────────────────────────────────────
        host = record.get("host") or record.get("ip") or ""

        # ── timestamp → log_date + log_hour ───────────────────────────────────
        ts = record.get("timestamp")
        log_date: str = ""
        log_hour: int = -1

        if isinstance(ts, datetime):
            log_date = ts.strftime("%Y-%m-%d")
            log_hour = ts.hour
        elif isinstance(ts, str) and ts:
            # Strip timezone offset — keep first 20 chars: "01/Jul/1995:00:00:01"
            ts_clean = ts[:20].strip()
            try:
                parsed = datetime.strptime(ts_clean, _NASA_TS_FORMAT)
                log_date = parsed.strftime("%Y-%m-%d")
                log_hour = parsed.hour
            except ValueError:
                logger.debug("Unparseable timestamp string: %r", ts)

        # ── other fields — accept both naming conventions ──────────────────────
        method   = record.get("method")   or record.get("http_method")   or ""
        url      = record.get("url")      or record.get("resource_path") or ""
        protocol = record.get("protocol") or ""
        status   = int(record.get("status") or record.get("status_code") or 0)
        size     = int(
            record.get("size") or record.get("bytes")
            or record.get("bytes_transferred") or 0
        )

        return [host, log_date, log_hour, method, url, protocol, status, size]

    def _upload_batch(self, batch: List[List], batch_num: int) -> None:
        buf = io.StringIO()
        for row in batch:
            buf.write("\t".join(str(c) for c in row) + "\n")

        tsv_bytes = buf.getvalue().encode("utf-8")
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=f"_pig_batch_{batch_num:06d}.tsv")
        try:
            with os.fdopen(tmp_fd, "wb") as tmp:
                tmp.write(tsv_bytes)
            hdfs_dest = f"{self._hdfs_input}/batch_{batch_num:06d}.tsv"
            self._hdfs("-put", tmp_path, hdfs_dest)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # =========================================================================
    # Diagnosis helper — runs before Phase 2, saves hours of debugging
    # =========================================================================

    def _diagnose_hdfs_input(self) -> None:
        """
        Samples 2 rows from the first uploaded batch file and logs each column
        with its label. Lets you confirm the TSV schema is correct before
        spending 8+ minutes on a Pig job that will silently produce 0 rows.

        What to check in the log output:
          log_date  → must be "1995-07-xx" or "1995-08-xx", NOT ""
          log_hour  → must be 0–23, NOT "-1"
          host      → must be a hostname/IP, NOT ""
          status    → must be a 3-digit code, NOT "0"
        """
        try:
            sample = self._hdfs("-cat", f"{self._hdfs_input}/batch_000001.tsv")
            lines  = [l for l in sample.splitlines() if l.strip()][:2]
            logger.info("──── TSV schema check (2 sample rows from batch_000001) ────")
            labels = ["host", "log_date", "log_hour", "method",
                      "resource_path", "protocol", "status", "bytes"]
            for i, line in enumerate(lines, 1):
                cols = line.split("\t")
                for label, val in zip(labels, cols):
                    logger.info("  row%d  %-15s = %r", i, label, val)
            if not lines:
                logger.warning("batch_000001.tsv is empty — upload may have failed.")
        except RuntimeError as exc:
            logger.warning("TSV diagnosis skipped: %s", exc)

    # =========================================================================
    # Phase 2 — _run_queries
    # =========================================================================

    def _run_queries(self) -> Dict[str, List[Dict]]:
        # Diagnose TSV schema before any Pig job runs
        self._diagnose_hdfs_input()

        results: Dict[str, List[Dict]] = {}
        query_specs = [
            ("q1_daily_traffic.pig", f"{self._hdfs_output}/q1",
             "q1_daily_traffic",     self._parse_q1, "q1"),
            ("q2_top_resources.pig", f"{self._hdfs_output}/q2",
             "q2_top_resources",     self._parse_q2, "q2"),
            ("q3_hourly_errors.pig", f"{self._hdfs_output}/q3",
             "q3_hourly_errors",     self._parse_q3, "q3"),
        ]

        for script, out_dir, result_key, parser_fn, q_name in query_specs:
            if q_name not in self.selected_queries:
                results[result_key] = None
                continue

            logger.info("─── Running %s ───", script)
            self._hdfs("-rm", "-r", "-f", out_dir)
            self._run_pig(
                script,
                {"input_dir": self._hdfs_input, "output_dir": out_dir},
            )
            raw_tsv = self._read_hdfs_output(out_dir)
            rows    = parser_fn(raw_tsv)
            results[result_key] = rows
            logger.info("%s → %d rows", result_key, len(rows))

        return results

    def _read_hdfs_output(self, hdfs_dir: str) -> str:
        """
        Read all part-* files from an HDFS output directory.

        FIX (v3): Lists the directory first and counts part files so we emit
        a clear WARNING when Pig produced an empty-but-successful result, instead
        of silently returning "" and logging "0 rows".
        """
        # List the output directory
        try:
            listing = self._hdfs("-ls", hdfs_dir)
        except RuntimeError:
            logger.warning("Output directory does not exist: %s", hdfs_dir)
            return ""

        part_files = [
            l for l in listing.splitlines()
            if "/part-" in l or "/part_" in l
        ]
        logger.info(
            "HDFS output %s — %d part file(s) found", hdfs_dir, len(part_files)
        )
        if not part_files:
            logger.warning(
                "No part-* files in %s. Pig job ran but produced empty output. "
                "Most likely cause: FILTER dropped all records (e.g. log_date = ''). "
                "Check the TSV schema diagnosis above. "
                "Also check YARN job history: http://localhost:8088",
                hdfs_dir,
            )
            return ""

        try:
            return self._hdfs("-cat", f"{hdfs_dir}/part-*")
        except RuntimeError as exc:
            logger.error("Failed to read output from %s: %s", hdfs_dir, exc)
            return ""

    # =========================================================================
    # TSV → dict parsers
    # =========================================================================

    @staticmethod
    def _parse_q1(raw: str) -> List[Dict]:
        rows: List[Dict] = []
        for line in raw.splitlines():
            parts = line.strip().split("\t")
            if len(parts) != 4:
                continue
            log_date, status_code, request_count, total_bytes = parts
            rows.append({
                "log_date":      log_date,
                "status_code":   int(status_code),
                "request_count": int(request_count),
                "total_bytes":   int(total_bytes),
            })
        return rows

    @staticmethod
    def _parse_q2(raw: str) -> List[Dict]:
        rows: List[Dict] = []
        for line in raw.splitlines():
            parts = line.strip().split("\t")
            if len(parts) != 4:
                continue
            resource_path, request_count, total_bytes, distinct_hosts = parts
            rows.append({
                "resource_path":  resource_path,
                "request_count":  int(request_count),
                "total_bytes":    int(total_bytes),
                "distinct_hosts": int(distinct_hosts),
            })
        return rows

    @staticmethod
    def _parse_q3(raw: str) -> List[Dict]:
        rows: List[Dict] = []
        for line in raw.splitlines():
            parts = line.strip().split("\t")
            if len(parts) != 6:
                continue
            log_date, log_hour, error_count, total_requests, error_rate, dist = parts
            rows.append({
                "log_date":             log_date,
                "log_hour":             int(log_hour),
                "error_count":          int(error_count),
                "total_requests":       int(total_requests),
                "error_rate":           float(error_rate),
                "distinct_error_hosts": int(dist),
            })
        return rows

    # =========================================================================
    # Phase 3 — _write_results
    # =========================================================================

    def _write_results(self, results: Dict[str, List[Dict]]) -> None:
        stats = self._batcher_stats
        now   = datetime.now(timezone.utc)

        run_meta = {
            "run_id":            self.run_id,
            "pipeline":          self.PIPELINE_NAME,
            "batch_size":        self.batch_size,
            "total_records":     stats.total_records,
            "malformed_records": stats.malformed_records,
            "num_batches":       stats.num_batches,
            "avg_batch_size":    stats.avg_batch_size,
            "runtime_seconds":   self.get_runtime(),
            "started_at":        self.execution_ts,
            "completed_at":      now,
        }

        with ResultLoader() as loader:
            loader.save_run(run_meta)

            # Save batch metadata
            batches = [
                {
                    "batch_id": b.batch_id,
                    "batch_size_config": self.batch_size,
                    "records_in_batch": b.record_count,
                    "started_at": None,
                    "completed_at": None
                }
                for b in stats.batch_stats
            ]
            loader.save_batch_metadata(self.run_id, self.PIPELINE_NAME, batches)

            if results.get("q1_daily_traffic") is not None:
                q1_rows = []
                for r in results["q1_daily_traffic"]:
                    q1_rows.append({
                        "query_name":    "q1",
                        "log_date":      r["log_date"],
                        "status_code":   r["status_code"],
                        "request_count": r["request_count"],
                        "total_bytes":   r["total_bytes"]
                      })
                loader.save_q1(self.run_id, self.PIPELINE_NAME, q1_rows)

            if results.get("q2_top_resources") is not None:
                q2_rows = []
                for r in results["q2_top_resources"]:
                    q2_rows.append({
                        "query_name":          "q2",
                        "resource_path":       r["resource_path"],
                        "request_count":       r["request_count"],
                        "total_bytes":         r["total_bytes"],
                        "distinct_host_count": r["distinct_hosts"]
                    })
                loader.save_q2(self.run_id, self.PIPELINE_NAME, q2_rows)

            if results.get("q3_hourly_errors") is not None:
                q3_rows = []
                for r in results["q3_hourly_errors"]:
                    q3_rows.append({
                        "query_name":           "q3",
                        "log_date":             r["log_date"],
                        "log_hour":             r["log_hour"],
                        "error_request_count":  r["error_count"],
                        "total_request_count":  r["total_requests"],
                        "error_rate":           r["error_rate"],
                        "distinct_error_hosts": r["distinct_error_hosts"]
                    })
                loader.save_q3(self.run_id, self.PIPELINE_NAME, q3_rows)

            loader.save_malformed(self.run_id, self.PIPELINE_NAME, {
                "run_id": self.run_id,
                "pipeline": self.PIPELINE_NAME,
                "total_malformed": stats.malformed_records,
                "empty_line_count": 0,
                "missing_brackets_count": 0,
                "missing_quotes_count": 0,
                "bad_timestamp_count": 0,
                "bad_status_count": 0,
                "bad_bytes_count": 0,
                "truncated_count": 0,
                "unknown_count": stats.malformed_records,
                "sample_lines": "[]"
            })

        logger.info("All results committed to DB  run_id=%s", self.run_id)

    # =========================================================================
    # Optional cleanup
    # =========================================================================

    def cleanup_hdfs(self) -> None:
        try:
            self._hdfs("-rm", "-r", "-skipTrash", self._hdfs_base)
            logger.info("HDFS cleanup done: %s", self._hdfs_base)
        except Exception as exc:
            logger.warning("HDFS cleanup failed (non-fatal): %s", exc)