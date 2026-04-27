"""
reporting/report.py
====================
Reads ETL run results from MySQL / PostgreSQL and prints a formatted
terminal report showing:
  - Run metadata (pipeline, timing, batch stats, malformed records)
  - Q1: Daily Traffic Summary
  - Q2: Top 20 Requested Resources
  - Q3: Hourly Error Analysis

Usage:
    # Report the most recent run for any pipeline:
    python -m reporting.report

    # Report a specific run by ID:
    python -m reporting.report --run-id 550e8400-e29b-41d4-a716-446655440000

    # Report all runs for a specific pipeline:
    python -m reporting.report --pipeline mongodb

    # Compare two pipelines side-by-side (summary only):
    python -m reporting.report --compare pig mongodb
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ANSI colour codes (disabled automatically when not a TTY)
# ---------------------------------------------------------------------------
_USE_COLOUR = sys.stdout.isatty()

class C:
    RESET  = "\033[0m"   if _USE_COLOUR else ""
    BOLD   = "\033[1m"   if _USE_COLOUR else ""
    CYAN   = "\033[96m"  if _USE_COLOUR else ""
    GREEN  = "\033[92m"  if _USE_COLOUR else ""
    YELLOW = "\033[93m"  if _USE_COLOUR else ""
    RED    = "\033[91m"  if _USE_COLOUR else ""
    DIM    = "\033[2m"   if _USE_COLOUR else ""
    BLUE   = "\033[94m"  if _USE_COLOUR else ""


# ---------------------------------------------------------------------------
# DB connection (reuses same helper as loader.py)
# ---------------------------------------------------------------------------

def _get_connection():
    dialect  = os.getenv("DB_DIALECT", config.RDBMS_TYPE).lower()
    host     = os.getenv("DB_HOST", config.PG_HOST if dialect == "postgresql" else config.MYSQL_HOST)
    db_name  = os.getenv("DB_NAME", config.PG_DB if dialect == "postgresql" else config.MYSQL_DB)
    user     = os.getenv("DB_USER", config.PG_USER if dialect == "postgresql" else config.MYSQL_USER)
    password = os.getenv("DB_PASSWORD", config.PG_PASSWORD if dialect == "postgresql" else config.MYSQL_PASSWORD)

    if dialect == "mysql":
        import mysql.connector
        port = int(os.getenv("DB_PORT", "3306"))
        conn = mysql.connector.connect(
            host=host, port=port, database=db_name,
            user=user, password=password, autocommit=True
        )
        return conn, "%s"

    elif dialect == "postgresql":
        import psycopg2
        port = int(os.getenv("DB_PORT", "5432"))
        conn = psycopg2.connect(host=host, port=port, dbname=db_name,
                                user=user, password=password)
        conn.autocommit = True
        return conn, "%s"

    raise ValueError(f"Unsupported DB_DIALECT: '{dialect}'")


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _fetchall(cursor, sql: str, params: tuple = ()) -> List[Dict]:
    cursor.execute(sql, params)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def fetch_run(cursor, run_id: str) -> Optional[Dict]:
    rows = _fetchall(cursor,
        "SELECT * FROM etl_runs WHERE run_id = %s", (run_id,))
    return rows[0] if rows else None


def fetch_latest_run(cursor, pipeline: Optional[str] = None) -> Optional[Dict]:
    if pipeline:
        rows = _fetchall(cursor,
            "SELECT * FROM etl_runs WHERE pipeline = %s ORDER BY started_at DESC LIMIT 1",
            (pipeline,))
    else:
        rows = _fetchall(cursor,
            "SELECT * FROM etl_runs ORDER BY started_at DESC LIMIT 1")
    return rows[0] if rows else None


def fetch_all_runs(cursor, pipeline: Optional[str] = None) -> List[Dict]:
    if pipeline:
        return _fetchall(cursor,
            "SELECT * FROM etl_runs WHERE pipeline = %s ORDER BY started_at DESC",
            (pipeline,))
    return _fetchall(cursor,
        "SELECT * FROM etl_runs ORDER BY started_at DESC")


def fetch_q1(cursor, run_id: str) -> List[Dict]:
    return _fetchall(cursor,
        """SELECT log_date, status_code, request_count, total_bytes
           FROM q1_daily_traffic WHERE run_id = %s
           ORDER BY log_date, status_code""",
        (run_id,))


def fetch_q2(cursor, run_id: str) -> List[Dict]:
    return _fetchall(cursor,
        """SELECT resource_path, request_count, total_bytes, distinct_hosts
           FROM q2_top_resources WHERE run_id = %s
           ORDER BY request_count DESC""",
        (run_id,))


def fetch_q3(cursor, run_id: str) -> List[Dict]:
    return _fetchall(cursor,
        """SELECT log_date, log_hour, error_count, total_requests,
                  error_rate, distinct_error_hosts
           FROM q3_hourly_errors WHERE run_id = %s
           ORDER BY log_date, log_hour""",
        (run_id,))


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _hr(char: str = "─", width: int = 90) -> str:
    return char * width

def _fmt_bytes(n: int) -> str:
    """Human-readable bytes: 1.23 GB etc."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

def _fmt_float(v, decimals: int = 4) -> str:
    return f"{v:.{decimals}f}"

def _col(text: str, width: int, align: str = "<") -> str:
    """Left/right-pad a string to fixed width, truncating if needed."""
    s = str(text)
    if len(s) > width:
        s = s[:width - 1] + "…"
    return f"{s:{align}{width}}"


def _table(headers: List[str], rows: List[List[str]], col_widths: List[int]) -> str:
    lines = []
    header = "  ".join(_col(h, w) for h, w in zip(headers, col_widths))
    lines.append(f"{C.BOLD}{C.CYAN}{header}{C.RESET}")
    lines.append(_hr("─", sum(col_widths) + 2 * (len(col_widths) - 1)))
    for i, row in enumerate(rows):
        line = "  ".join(_col(cell, w) for cell, w in zip(row, col_widths))
        colour = C.DIM if i % 2 == 1 else ""
        lines.append(f"{colour}{line}{C.RESET}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section printers
# ---------------------------------------------------------------------------

def print_banner():
    print(f"\n{C.BOLD}{C.BLUE}{'═' * 90}")
    print("  DAS 839 – NoSQL ETL Framework  │  Run Report")
    print(f"{'═' * 90}{C.RESET}")
    print(f"{C.DIM}  Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{C.RESET}\n")


def print_run_meta(run: Dict):
    p = run["pipeline"].upper()
    colour = {
        "PIG": C.YELLOW, "MAPREDUCE": C.RED,
        "MONGODB": C.GREEN, "HIVE": C.CYAN
    }.get(p, C.BOLD)

    print(f"{C.BOLD}{'─' * 90}")
    print(f"  EXECUTION METADATA   {colour}[{p}]{C.RESET}")
    print(f"{'─' * 90}{C.RESET}")
    print(f"  {'Run ID':<25} {run['run_id']}")
    print(f"  {'Pipeline':<25} {colour}{run['pipeline']}{C.RESET}")
    print(f"  {'Started at':<25} {run['started_at']}")
    print(f"  {'Completed at':<25} {run['completed_at']}")
    print(f"  {'Runtime':<25} {C.BOLD}{_fmt_float(run['runtime_seconds'], 3)} seconds{C.RESET}")
    print()
    print(f"  {'Batch size (config)':<25} {run['batch_size']:,} records")
    print(f"  {'Total records':<25} {run['total_records']:,}")
    print(f"  {'Malformed records':<25} {C.RED if run['malformed_records'] > 0 else ''}"
          f"{run['malformed_records']:,}{C.RESET}")
    print(f"  {'Number of batches':<25} {run['num_batches']:,}")
    print(f"  {'Avg batch size':<25} {_fmt_float(run['avg_batch_size'], 2)} records/batch")
    print()


def print_q1(rows: List[Dict]):
    print(f"\n{C.BOLD}{'─' * 90}")
    print("  QUERY 1 – Daily Traffic Summary")
    print(f"  log_date  ╳  status_code  →  request_count, total_bytes")
    print(f"{'─' * 90}{C.RESET}\n")

    if not rows:
        print("  (no data)\n")
        return

    table_rows = [
        [str(r["log_date"]), str(r["status_code"]),
         f"{r['request_count']:,}", _fmt_bytes(r["total_bytes"])]
        for r in rows
    ]
    print(_table(
        ["Log Date", "Status", "Requests", "Total Bytes"],
        table_rows,
        [14, 8, 14, 16]
    ))

    total_req   = sum(r["request_count"] for r in rows)
    total_bytes = sum(r["total_bytes"] for r in rows)
    print(f"\n  {C.DIM}Totals: {total_req:,} requests │ {_fmt_bytes(total_bytes)} transferred{C.RESET}\n")


def print_q2(rows: List[Dict]):
    print(f"\n{C.BOLD}{'─' * 90}")
    print("  QUERY 2 – Top 20 Requested Resources")
    print(f"  Ranked by request count")
    print(f"{'─' * 90}{C.RESET}\n")

    if not rows:
        print("  (no data)\n")
        return

    # FIX: rank derived from loop position (rows already ordered by request_count DESC)
    # FIX: distinct_hosts matches schema column name (not distinct_host_count)
    table_rows = [
        [str(i),
         str(r["resource_path"]),
         f"{r['request_count']:,}",
         _fmt_bytes(r["total_bytes"]),
         f"{r['distinct_hosts']:,}"]
        for i, r in enumerate(rows, start=1)
    ]
    print(_table(
        ["#", "Resource Path", "Requests", "Bytes", "Unique Hosts"],
        table_rows,
        [4, 44, 12, 12, 12]
    ))
    print()


def print_q3(rows: List[Dict]):
    print(f"\n{C.BOLD}{'─' * 90}")
    print("  QUERY 3 – Hourly Error Analysis  (status 400–599)")
    print(f"  log_date  ╳  log_hour  →  error stats")
    print(f"{'─' * 90}{C.RESET}\n")

    if not rows:
        print("  (no data)\n")
        return

    # FIX: error_count matches schema column name (not error_request_count)
    # FIX: total_requests matches schema column name (not total_request_count)
    table_rows = [
        [str(r["log_date"]),
         f"{int(r['log_hour']):02d}:00",
         f"{r['error_count']:,}",
         f"{r['total_requests']:,}",
         f"{r['error_rate'] * 100:.2f}%",
         f"{r['distinct_error_hosts']:,}"]
        for r in rows
    ]
    print(_table(
        ["Date", "Hour", "Errors", "Total Req", "Error Rate", "Error Hosts"],
        table_rows,
        [14, 8, 12, 12, 12, 12]
    ))

    # FIX: use corrected column names in footer totals too
    total_err    = sum(r["error_count"] for r in rows)
    total_all    = sum(r["total_requests"] for r in rows)
    overall_rate = (total_err / total_all * 100) if total_all > 0 else 0
    print(f"\n  {C.DIM}Overall error rate: {overall_rate:.2f}% "
          f"({total_err:,} errors / {total_all:,} total){C.RESET}\n")


def print_comparison(runs: List[Dict]):
    """Side-by-side runtime comparison across pipeline runs."""
    print(f"\n{C.BOLD}{'═' * 90}")
    print("  PIPELINE COMPARISON SUMMARY")
    print(f"{'═' * 90}{C.RESET}\n")
    headers = ["Pipeline", "Run ID", "Records", "Batches", "Avg Batch", "Runtime (s)", "Malformed"]
    widths  = [12, 38, 12, 10, 12, 14, 12]
    rows = [
        [r["pipeline"], r["run_id"],
         f"{r['total_records']:,}",
         f"{r['num_batches']:,}",
         f"{r['avg_batch_size']:.1f}",
         f"{r['runtime_seconds']:.3f}",
         f"{r['malformed_records']:,}"]
        for r in runs
    ]
    print(_table(headers, rows, widths))
    print()


# ---------------------------------------------------------------------------
# Main report function
# ---------------------------------------------------------------------------

def generate_report(run_id: Optional[str] = None,
                    pipeline: Optional[str] = None,
                    compare: Optional[List[str]] = None):
    """
    Entry point. Connects to DB and prints the appropriate report.

    Args:
        run_id   : show a specific run
        pipeline : show latest run for this pipeline
        compare  : list of pipeline names to compare side-by-side
    """
    conn, _ = _get_connection()
    cursor = conn.cursor()

    print_banner()

    try:
        # ── Comparison mode ──────────────────────────────────────────────────
        if compare:
            runs = []
            for p in compare:
                run = fetch_latest_run(cursor, p)
                if run:
                    runs.append(run)
                else:
                    print(f"  {C.RED}Warning: no runs found for pipeline '{p}'{C.RESET}")
            if runs:
                print_comparison(runs)
            return

        # ── Single run mode ──────────────────────────────────────────────────
        if run_id:
            run = fetch_run(cursor, run_id)
            if not run:
                print(f"  {C.RED}Error: run_id '{run_id}' not found in DB.{C.RESET}\n")
                return
        else:
            run = fetch_latest_run(cursor, pipeline)
            if not run:
                msg = f"pipeline='{pipeline}'" if pipeline else "any pipeline"
                print(f"  {C.RED}No runs found for {msg}.{C.RESET}\n")
                return

        print_run_meta(run)
        rid = run["run_id"]

        q1 = fetch_q1(cursor, rid)
        q2 = fetch_q2(cursor, rid)
        q3 = fetch_q3(cursor, rid)

        print_q1(q1)
        print_q2(q2)
        print_q3(q3)

        print(f"{C.DIM}{'─' * 90}{C.RESET}\n")

    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="DAS 839 – ETL Run Reporter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m reporting.report
  python -m reporting.report --pipeline mongodb
  python -m reporting.report --run-id 550e8400-e29b-41d4-a716-446655440000
  python -m reporting.report --compare pig mapreduce mongodb hive
        """
    )
    parser.add_argument("--run-id",   type=str, help="Show a specific run by UUID")
    parser.add_argument("--pipeline", type=str,
                        choices=["pig", "mapreduce", "mongodb", "hive"],
                        help="Show latest run for this pipeline")
    parser.add_argument("--compare",  nargs="+",
                        choices=["pig", "mapreduce", "mongodb", "hive"],
                        help="Compare latest runs across pipelines side-by-side")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING,
                        format="%(levelname)s  %(name)s  %(message)s")
    args = _parse_args()
    generate_report(
        run_id=args.run_id,
        pipeline=args.pipeline,
        compare=args.compare
    )