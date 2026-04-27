"""
db/loader.py
============
ResultLoader – writes aggregated ETL results and run metadata
to MySQL or PostgreSQL after a pipeline completes.

Usage (called by each pipeline at the end of its run):

    from db.loader import ResultLoader

    loader = ResultLoader()
    loader.save_run(run_meta)
    loader.save_q1(run_id, q1_rows)
    loader.save_q2(run_id, q2_rows)
    loader.save_q3(run_id, q3_rows)
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
# Set DB_DIALECT = "mysql" or "postgresql" in environment / config.py
# ---------------------------------------------------------------------------

def _get_connection():
    """
    Returns a DB-API 2.0 connection using environment variables.

    Required env vars:
        DB_DIALECT   : "mysql" or "postgresql"
        DB_HOST      : hostname (default: localhost)
        DB_PORT      : port (default: 3306 / 5432)
        DB_NAME      : database name
        DB_USER      : username
        DB_PASSWORD  : password
    """
    dialect  = os.getenv("DB_DIALECT", config.RDBMS_TYPE).lower()
    host     = os.getenv("DB_HOST", config.PG_HOST if dialect == "postgresql" else config.MYSQL_HOST)
    db_name  = os.getenv("DB_NAME", config.PG_DB if dialect == "postgresql" else config.MYSQL_DB)
    user     = os.getenv("DB_USER", config.PG_USER if dialect == "postgresql" else config.MYSQL_USER)
    password = os.getenv("DB_PASSWORD", config.PG_PASSWORD if dialect == "postgresql" else config.MYSQL_PASSWORD)

    if dialect == "mysql":
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("Install mysql-connector-python:  pip install mysql-connector-python")

        port = int(os.getenv("DB_PORT", "3306"))
        conn = mysql.connector.connect(
            host=host, port=port, database=db_name,
            user=user, password=password,
            autocommit=False
        )
        return conn, "%s"          # MySQL placeholder

    elif dialect == "postgresql":
        try:
            import psycopg2
        except ImportError:
            raise ImportError("Install psycopg2:  pip install psycopg2-binary")

        port = int(os.getenv("DB_PORT", "5432"))
        conn = psycopg2.connect(
            host=host, port=port, dbname=db_name,
            user=user, password=password
        )
        conn.autocommit = False
        return conn, "%s"          # psycopg2 also uses %s

    else:
        raise ValueError(f"Unsupported DB_DIALECT: '{dialect}'. Use 'mysql' or 'postgresql'.")


# ---------------------------------------------------------------------------
# Data classes (plain dicts with documented keys)
# ---------------------------------------------------------------------------

"""
RunMeta dict keys:
    run_id              str     UUID string
    pipeline            str     'pig' | 'mapreduce' | 'mongodb' | 'hive'
    batch_size          int     configured records per batch
    total_records       int     total lines read from input files
    malformed_records   int     lines that failed parsing
    num_batches         int     how many batches were processed
    avg_batch_size      float   total_records / num_batches
    runtime_seconds     float   wall-clock seconds from start to DB write
    started_at          datetime
    completed_at        datetime

Q1Row dict keys:
    log_date            str | date    e.g. "1995-07-01"
    status_code         int
    request_count       int
    total_bytes         int

Q2Row dict keys:
    resource_path       str
    request_count       int
    total_bytes         int
    distinct_host_count int
    rank_position       int           1-based rank (1 = most requested)

Q3Row dict keys:
    log_date            str | date
    log_hour            int           0-23
    error_request_count int
    total_request_count int
    error_rate          float         0.0 – 1.0
    distinct_error_hosts int
"""


# ---------------------------------------------------------------------------
# ResultLoader
# ---------------------------------------------------------------------------

class ResultLoader:
    """
    Handles all DB writes for one or more pipeline runs.
    Create once, use for all save_* calls, then call close().
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
        Call this FIRST before saving query results.
        """
        sql = f"""
            INSERT INTO etl_runs
                (run_id, pipeline, batch_size, total_records, malformed_records,
                 num_batches, avg_batch_size, runtime_seconds, started_at, completed_at)
            VALUES
                ({self._ph},{self._ph},{self._ph},{self._ph},{self._ph},
                 {self._ph},{self._ph},{self._ph},{self._ph},{self._ph})
        """
        values = (
            meta["run_id"],
            meta["pipeline"],
            meta["batch_size"],
            meta["total_records"],
            meta["malformed_records"],
            meta["num_batches"],
            round(meta["avg_batch_size"], 4),
            round(meta["runtime_seconds"], 4),
            _to_ts(meta["started_at"]),
            _to_ts(meta["completed_at"]),
        )
        self._execute(sql, values)
        self._conn.commit()
        logger.info(f"[{meta['pipeline']}] Run metadata saved  run_id={meta['run_id']}")

    def save_q1(self, run_id: str, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert Q1 Daily Traffic Summary results.
        rows: list of Q1Row dicts.
        """
        if not rows:
            logger.warning("save_q1 called with empty rows list.")
            return

        sql = f"""
            INSERT INTO q1_daily_traffic
                (run_id, log_date, status_code, request_count, total_bytes)
            VALUES
                ({self._ph},{self._ph},{self._ph},{self._ph},{self._ph})
        """
        data = [
            (run_id, r["log_date"], r["status_code"],
             r["request_count"], r["total_bytes"])
            for r in rows
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Q1 saved: {len(rows)} rows  run_id={run_id}")

    def save_q2(self, run_id: str, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert Q2 Top Requested Resources results.
        rows: list of Q2Row dicts, already ranked (rank_position 1–20).
        """
        if not rows:
            logger.warning("save_q2 called with empty rows list.")
            return

        # Assign rank if not already present
        for i, r in enumerate(rows, start=1):
            r.setdefault("rank_position", i)

        sql = f"""
            INSERT INTO q2_top_resources
                (run_id, resource_path, request_count, total_bytes,
                 distinct_host_count, rank_position)
            VALUES
                ({self._ph},{self._ph},{self._ph},{self._ph},{self._ph},{self._ph})
        """
        data = [
            (run_id, r["resource_path"], r["request_count"],
             r["total_bytes"], r["distinct_host_count"], r["rank_position"])
            for r in rows
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Q2 saved: {len(rows)} rows  run_id={run_id}")

    def save_q3(self, run_id: str, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk-insert Q3 Hourly Error Analysis results.
        rows: list of Q3Row dicts.
        """
        if not rows:
            logger.warning("save_q3 called with empty rows list.")
            return

        sql = f"""
            INSERT INTO q3_hourly_errors
                (run_id, log_date, log_hour, error_request_count,
                 total_request_count, error_rate, distinct_error_hosts)
            VALUES
                ({self._ph},{self._ph},{self._ph},{self._ph},{self._ph},{self._ph},{self._ph})
        """
        data = [
            (run_id, r["log_date"], r["log_hour"],
             r["error_request_count"], r["total_request_count"],
             round(r["error_rate"], 6), r["distinct_error_hosts"])
            for r in rows
        ]
        self._executemany(sql, data)
        self._conn.commit()
        logger.info(f"Q3 saved: {len(rows)} rows  run_id={run_id}")

    def close(self) -> None:
        """Close cursor and connection cleanly."""
        try:
            self._cursor.close()
            self._conn.close()
            logger.info("ResultLoader connection closed.")
        except Exception as e:
            logger.warning(f"Error closing DB connection: {e}")

    # ------------------------------------------------------------------
    # Context manager support  (with ResultLoader() as loader: ...)
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
        return False   # don't suppress exceptions

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str, values: tuple) -> None:
        try:
            self._cursor.execute(sql, values)
        except Exception as e:
            self._conn.rollback()
            logger.error(f"DB execute failed: {e}\nSQL: {sql}\nValues: {values}")
            raise

    def _executemany(self, sql: str, data: list) -> None:
        try:
            self._cursor.executemany(sql, data)
        except Exception as e:
            self._conn.rollback()
            logger.error(f"DB executemany failed: {e}\nSQL: {sql}\nFirst row: {data[0] if data else 'N/A'}")
            raise


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _to_ts(value) -> str:
    """Normalise datetime or string to 'YYYY-MM-DD HH:MM:SS' string."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)
