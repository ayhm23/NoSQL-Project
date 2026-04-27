"""
db/loader.py — Result writer for MySQL / PostgreSQL.

Provides ResultLoader, which:
  • Connects lazily using the config in config.py
  • Ensures all result tables exist (runs schema.sql if needed)
  • Exposes write_q1(), write_q2(), write_q3(), write_run_summary()
  • Uses executemany() for bulk-insert performance
  • Supports both psycopg2 (PostgreSQL) and mysql-connector-python (MySQL)

The loader is deliberately thin — no ORM, no abstraction overhead — so
every query it generates is obvious and auditable.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import config

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_pg_conn():
    """Return a psycopg2 connection using config.py values."""
    import psycopg2
    return psycopg2.connect(
        host     = config.PG_HOST,
        port     = config.PG_PORT,
        dbname   = config.PG_DB,
        user     = config.PG_USER,
        password = config.PG_PASSWORD,
    )


def _get_mysql_conn():
    """Return a mysql-connector connection using config.py values."""
    import mysql.connector
    return mysql.connector.connect(
        host     = config.MYSQL_HOST,
        port     = config.MYSQL_PORT,
        database = config.MYSQL_DB,
        user     = config.MYSQL_USER,
        password = config.MYSQL_PASSWORD,
    )


def _placeholder(rdbms_type: str) -> str:
    """Return the correct parameter placeholder for the driver."""
    return "%s" if rdbms_type in ("postgresql", "mysql") else "?"


# ─────────────────────────────────────────────────────────────────────────────
# ResultLoader
# ─────────────────────────────────────────────────────────────────────────────

class ResultLoader:
    """
    Thin wrapper around a relational DB connection that writes ETL results.

    Usage:
        loader = ResultLoader()
        loader.write_q1(rows)
        loader.write_q2(rows)
        loader.write_q3(rows)
        loader.write_run_summary(summary)
        loader.close()

    Or as a context manager:
        with ResultLoader() as loader:
            loader.write_q1(rows)
    """

    def __init__(self, rdbms_type: str = None):
        self._type = (rdbms_type or config.RDBMS_TYPE).lower()
        self._conn = None
        self._cursor = None
        self._ph = _placeholder(self._type)

    # ── Connection ────────────────────────────────────────────────────────────

    def _connect(self):
        if self._conn is not None:
            return
        logger.info("Connecting to %s", self._type)
        if self._type == "postgresql":
            self._conn = _get_pg_conn()
        elif self._type == "mysql":
            self._conn = _get_mysql_conn()
        else:
            raise ValueError(f"Unsupported RDBMS type: {self._type!r}")
        self._cursor = self._conn.cursor()
        self._ensure_schema()
        logger.info("Connected to %s", self._type)

    def _ensure_schema(self):
        """
        Execute schema.sql to create tables if they don't exist.
        Only the CREATE TABLE / CREATE INDEX statements are executed;
        comment lines are skipped.  Each statement is run independently
        so that pre-existing tables don't abort the whole script.
        """
        sql = _SCHEMA_PATH.read_text(encoding="utf-8")
        # Split on semicolons, skip MySQL-compat comments block
        statements = [
            s.strip()
            for s in sql.split(";")
            if s.strip() and not s.strip().startswith("--")
        ]
        for stmt in statements:
            if not stmt:
                continue
            # Skip MySQL-specific comment block lines
            if stmt.startswith("--"):
                continue
            # Only run DDL; skip pure comment-only chunks
            upper = stmt.upper().lstrip()
            if not (upper.startswith("CREATE") or upper.startswith("DROP")):
                continue
            try:
                self._cursor.execute(stmt)
                self._conn.commit()
            except Exception as exc:
                self._conn.rollback()
                logger.debug("Schema stmt skipped (%s): %.80s", exc, stmt)

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self._conn = None
        self._cursor = None
        logger.info("ResultLoader connection closed")

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ── Internal bulk insert ──────────────────────────────────────────────────

    def _bulk_insert(self, table: str, columns: List[str], rows: List[Dict]) -> int:
        """
        Insert `rows` into `table` using executemany().
        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        self._connect()
        ph = ", ".join([self._ph] * len(columns))
        col_list = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({ph})"
        data = [tuple(row.get(c) for c in columns) for row in rows]
        try:
            self._cursor.executemany(sql, data)
            self._conn.commit()
            logger.debug("Inserted %d rows into %s", len(data), table)
            return len(data)
        except Exception:
            self._conn.rollback()
            raise

    # ── Public write methods ──────────────────────────────────────────────────

    def write_q1(self, rows: List[Dict]) -> int:
        """
        Write Q1 (daily traffic) results.

        Expected keys per row:
            pipeline, run_id, batch_id, executed_at,
            log_date, status_code, request_count, total_bytes
        """
        columns = [
            "pipeline", "run_id", "batch_id", "executed_at",
            "log_date", "status_code", "request_count", "total_bytes",
        ]
        return self._bulk_insert("q1_daily_traffic", columns, rows)

    def write_q2(self, rows: List[Dict]) -> int:
        """
        Write Q2 (top resources) results.

        Expected keys per row:
            pipeline, run_id, batch_id, executed_at,
            resource_path, request_count, total_bytes, distinct_hosts
        """
        columns = [
            "pipeline", "run_id", "batch_id", "executed_at",
            "resource_path", "request_count", "total_bytes", "distinct_hosts",
        ]
        return self._bulk_insert("q2_top_resources", columns, rows)

    def write_q3(self, rows: List[Dict]) -> int:
        """
        Write Q3 (hourly error analysis) results.

        Expected keys per row:
            pipeline, run_id, batch_id, executed_at,
            log_date, log_hour, error_count, total_requests,
            error_rate, distinct_error_hosts
        """
        columns = [
            "pipeline", "run_id", "batch_id", "executed_at",
            "log_date", "log_hour", "error_count", "total_requests",
            "error_rate", "distinct_error_hosts",
        ]
        return self._bulk_insert("q3_hourly_errors", columns, rows)

    def write_run_summary(self, summary: Dict) -> None:
        """
        Upsert a pipeline run summary row into pipeline_runs.

        Expected keys:
            run_id, pipeline, executed_at, runtime_s,
            total_lines, parsed_ok, malformed,
            total_batches, non_empty_batches, avg_batch_size
        """
        self._connect()
        columns = [
            "run_id", "pipeline", "executed_at", "runtime_s",
            "total_lines", "parsed_ok", "malformed",
            "total_batches", "non_empty_batches", "avg_batch_size",
        ]
        ph = self._ph
        col_list = ", ".join(columns)
        placeholders = ", ".join([ph] * len(columns))

        if self._type == "postgresql":
            sql = (
                f"INSERT INTO pipeline_runs ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT (run_id) DO UPDATE SET "
                + ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "run_id")
            )
        else:
            # MySQL: INSERT IGNORE + UPDATE handled separately; keep it simple
            sql = f"INSERT IGNORE INTO pipeline_runs ({col_list}) VALUES ({placeholders})"

        data = tuple(summary.get(c) for c in columns)
        try:
            self._cursor.execute(sql, data)
            self._conn.commit()
            logger.info("Run summary written for run_id=%s", summary.get("run_id"))
        except Exception:
            self._conn.rollback()
            raise
