"""
db/loader.py
============
ResultLoader – writes aggregated ETL results and run metadata
to MySQL or PostgreSQL after a pipeline completes.

Usage (called by each pipeline at the end of its run):

    from db.loader import ResultLoader

    loader = ResultLoader()
    loader.save_run(run_meta)
    loader.save_q1(run_id, pipeline, q1_rows)
    loader.save_q2(run_id, pipeline, q2_rows)
    loader.save_q3(run_id, pipeline, q3_rows)
    loader.close()
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Any

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB connection helper — supports both MySQL and PostgreSQL
# ---------------------------------------------------------------------------

def _get_connection():
    """
    Returns a DB-API 2.0 connection using environment variables,
    falling back to config.py values.

    DB_DIALECT : "mysql" or "postgresql"
    DB_HOST    : hostname
    DB_PORT    : port
    DB_NAME    : database name
    DB_USER    : username
    DB_PASSWORD: password
    """
    dialect  = os.getenv("DB_DIALECT", config.RDBMS_TYPE).lower()
    host     = os.getenv("DB_HOST",     config.PG_HOST     if dialect == "postgresql" else config.MYSQL_HOST)
    db_name  = os.getenv("DB_NAME",     config.PG_DB       if dialect == "postgresql" else config.MYSQL_DB)
    user     = os.getenv("DB_USER",     config.PG_USER     if dialect == "postgresql" else config.MYSQL_USER)
    password = os.getenv("DB_PASSWORD", config.PG_PASSWORD if dialect == "postgresql" else config.MYSQL_PASSWORD)

    if dialect == "mysql":
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("Install mysql-connector-python: pip install mysql-connector-python")
        port = int(os.getenv("DB_PORT", "3306"))
        conn = mysql.connector.connect(
            host=host, port=port, database=db_name,
            user=user, password=password,
            autocommit=False,
        )
        return conn, "%s"

    elif dialect == "postgresql":
        try:
            import psycopg2
        except ImportError:
            raise ImportError("Install psycopg2: pip install psycopg2-binary")
        port = int(os.getenv("DB_PORT", "5432"))
        conn = psycopg2.connect(
            host=host, port=port, dbname=db_name,
            user=user, password=password,
        )
        conn.autocommit = False
        return conn, "%s"

    else:
        raise ValueError(f"Unsupported DB_DIALECT: '{dialect}'. Use 'mysql' or 'postgresql'.")


# ---------------------------------------------------------------------------
# ResultLoader
# ---------------------------------------------------------------------------

class ResultLoader:
    """
    Handles all DB writes for one or more pipeline runs.
    Create once, use for all save_* calls, then call close().

    Column reference (must match db/schema.sql exactly):

    etl_runs:
        run_id, pipeline, batch_size, total_records, malformed_records,
        num_batches, avg_batch_size, runtime_seconds, started_at, completed_at

    q1_daily_traffic:
        run_id, pipeline, query_name, log_date, status_code, request_count, total_bytes

    q2_top_resources:
        run_id, pipeline, query_name, resource_path, request_count, total_bytes, distinct_host_count

    q3_hourly_errors:
        run_id, pipeline, query_name, log_date, log_hour,
        error_request_count, total_request_count, error_rate, distinct_error_hosts
    """

    def __init__(self):
        self._conn, self._ph = _get_connection()
        self._cursor = self._conn.cursor()
        logger.info("ResultLoader connected to DB.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_run(self, meta: Dict[str, Any]) -> None:
        """
        Insert one row into etl_runs.
        Call this FIRST before saving query results (FK constraint).

        Required keys in meta:
            run_id, pipeline, batch_size, total_records, malformed_records,
            num_batches, avg_batch_size, runtime_seconds, started_at, completed_at
        """
        ph = self._ph
        sql = f"""
            INSERT INTO etl_runs
                (run_id, pipeline, batch_size, total_records, malformed_records,
                 num_batches, avg_batch_size, runtime_seconds, started_at, completed_at)
            VALUES
                ({ph},{ph},{ph},{ph},{ph},
                 {ph},{ph},{ph},{ph},{ph})
        """
        values = (
            meta["run_id"],
            meta["pipeline"],
            meta["batch_size"],
            meta["total_records"],
            meta["malformed_records"],
            meta["num_batches"],
            round(meta["avg_batch_size"],   4),
            round(meta["runtime_seconds"],  4),
            _to_ts(meta["started_at"]),
            _to_ts(meta["completed_at"]),
        )
        self._execute(sql, values)
        self._conn.commit()
        logger.info(f"[{meta['pipeline']}] Run metadata saved  run_id={meta['run_id']}")

    def save_q1(self, run_id: str, pipeline: str, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert Q1 Daily Traffic Summary results.

        Required keys per row:
            log_date, status_code, request_count, total_bytes
        """
        if not rows:
            logger.warning("save_q1 called with empty rows list.")
            return

        ph = self._ph
        sql = f"""
            INSERT INTO q1_daily_traffic
                (run_id, pipeline, query_name, log_date, status_code, request_count, total_bytes)
            VALUES
                ({ph},{ph},{ph},{ph},{ph},{ph},{ph})
        """
        data = [
            (
                run_id,
                pipeline,
                r.get("query_name", "q1"),
                r["log_date"],
                r["status_code"],
                r["request_count"],
                r["total_bytes"],
            )
            for r in rows
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Q1 saved: {len(rows)} rows  run_id={run_id}")

    def save_q2(self, run_id: str, pipeline: str, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert Q2 Top Requested Resources results.

        Required keys per row:
            resource_path, request_count, total_bytes, distinct_host_count
        """
        if not rows:
            logger.warning("save_q2 called with empty rows list.")
            return

        ph = self._ph
        sql = f"""
            INSERT INTO q2_top_resources
                (run_id, pipeline, query_name, resource_path, request_count, total_bytes, distinct_host_count)
            VALUES
                ({ph},{ph},{ph},{ph},{ph},{ph},{ph})
        """
        data = [
            (
                run_id,
                pipeline,
                r.get("query_name", "q2"),
                r["resource_path"],
                r["request_count"],
                r["total_bytes"],
                r.get("distinct_host_count", r.get("distinct_hosts", 0)),
            )
            for r in rows
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Q2 saved: {len(rows)} rows  run_id={run_id}")

    def save_q3(self, run_id: str, pipeline: str, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert Q3 Hourly Error Analysis results.

        Required keys per row:
            log_date, log_hour, error_request_count, total_request_count,
            error_rate, distinct_error_hosts
        """
        if not rows:
            logger.warning("save_q3 called with empty rows list.")
            return

        ph = self._ph
        sql = f"""
            INSERT INTO q3_hourly_errors
                (run_id, pipeline, query_name, log_date, log_hour,
                 error_request_count, total_request_count, error_rate, distinct_error_hosts)
            VALUES
                ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
        """
        data = [
            (
                run_id,
                pipeline,
                r.get("query_name", "q3"),
                r["log_date"],
                r["log_hour"],
                r.get("error_request_count", r.get("error_count", 0)),
                r.get("total_request_count", r.get("total_requests", 0)),
                round(r["error_rate"], 6),
                r["distinct_error_hosts"],
            )
            for r in rows
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Q3 saved: {len(rows)} rows  run_id={run_id}")

    def save_batch_metadata(self, run_id: str, pipeline: str, batches: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert one row per batch into batch_metadata.
        Each dict must have: batch_id, batch_size_config, records_in_batch.
        Optional: started_at, completed_at.
        """
        if not batches:
            logger.warning("save_batch_metadata called with empty list.")
            return

        ph = self._ph
        sql = f"""
            INSERT INTO batch_metadata
                (run_id, pipeline, batch_id, batch_size_config, records_in_batch, started_at, completed_at)
            VALUES
                ({ph},{ph},{ph},{ph},{ph},{ph},{ph})
        """
        data = [
            (
                run_id,
                pipeline,
                b["batch_id"],
                b["batch_size_config"],
                b["records_in_batch"],
                _to_ts(b["started_at"]) if b.get("started_at") else None,
                _to_ts(b["completed_at"]) if b.get("completed_at") else None,
            )
            for b in batches
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Batch metadata saved: {len(batches)} batches for run_id={run_id}")

    def save_malformed(self, run_id: str, pipeline: str, summary: Dict[str, Any]) -> None:
        """
        Insert one row into malformed_record_summary.
        The summary dict comes from MalformedHandler.to_db_summary().
        Required keys: all keys from to_db_summary() above.
        """
        ph = self._ph
        sql = f"""
            INSERT INTO malformed_record_summary
                (run_id, pipeline, total_malformed, empty_line_count,
                 missing_brackets_count, missing_quotes_count,
                 bad_timestamp_count, bad_status_count, bad_bytes_count,
                 truncated_count, unknown_count, sample_lines)
            VALUES
                ({ph},{ph},{ph},{ph},
                 {ph},{ph},
                 {ph},{ph},{ph},
                 {ph},{ph},{ph})
        """
        values = (
            run_id,
            pipeline,
            summary["total_malformed"],
            summary["empty_line_count"],
            summary["missing_brackets_count"],
            summary["missing_quotes_count"],
            summary["bad_timestamp_count"],
            summary["bad_status_count"],
            summary["bad_bytes_count"],
            summary["truncated_count"],
            summary["unknown_count"],
            summary["sample_lines"]
        )
        self._execute(sql, values)
        self._conn.commit()
        logger.info(f"[{pipeline}] Malformed record summary saved  run_id={run_id}")

    def health_check(self) -> bool:
        """Run SELECT 1. Return True on success, False on any exception."""
        try:
            self._cursor.execute("SELECT 1")
            self._cursor.fetchone()
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def close(self) -> None:
        """Close cursor and connection cleanly."""
        try:
            self._cursor.close()
            self._conn.close()
            logger.info("ResultLoader connection closed.")
        except Exception as e:
            logger.warning(f"Error closing DB connection: {e}")

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            try:
                self._conn.rollback()
                logger.error(f"Transaction rolled back due to: {exc_val}")
            except Exception:
                pass
        self.close()
        return False   # do not suppress exceptions

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str, values: tuple) -> None:
        try:
            self._cursor.execute(sql, values)
        except Exception as e:
            self._conn.rollback()
            logger.error(
                f"DB execute failed: {e}\nSQL: {sql}\nValues: {values}"
            )
            raise

    def _executemany(self, sql: str, data: list) -> None:
        try:
            self._cursor.executemany(sql, data)
        except Exception as e:
            self._conn.rollback()
            logger.error(
                f"DB executemany failed: {e}\nSQL: {sql}\n"
                f"First row: {data[0] if data else 'N/A'}"
            )
            raise


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _to_ts(value) -> str:
    """Normalise datetime or string to 'YYYY-MM-DD HH:MM:SS' string."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)