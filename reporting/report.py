"""
reporting/report.py — Query the relational DB and render a formatted comparison report.

Reads from MySQL/PostgreSQL (written by all four pipelines) and produces
a terminal-friendly report using tabulate.  Can also export to CSV.

Usage:
    python -m reporting.report [--pipeline PIPELINE] [--run_id RUN_ID] [--csv]
    python reporting/report.py
"""

import argparse
import csv
import io
import logging
import sys
from pathlib import Path
from typing import List, Dict, Optional

try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except ImportError:
    _HAS_TABULATE = False

import config
from db.loader import ResultLoader

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_table(rows: List[Dict], headers: List[str]) -> str:
    """Render a list-of-dicts as an ASCII table."""
    data = [[row.get(h, "") for h in headers] for row in rows]
    if _HAS_TABULATE:
        return tabulate(data, headers=headers, tablefmt="rounded_outline",
                        floatfmt=".4f", intfmt=",")
    # Fallback: basic TSV
    lines = ["\t".join(str(h) for h in headers)]
    for row in data:
        lines.append("\t".join(str(v) for v in row))
    return "\n".join(lines)


def _divider(title: str = "", width: int = 80) -> str:
    if title:
        pad = (width - len(title) - 2) // 2
        return "─" * pad + f" {title} " + "─" * (width - pad - len(title) - 2)
    return "─" * width


# ─────────────────────────────────────────────────────────────────────────────
# Query helpers (read from relational DB)
# ─────────────────────────────────────────────────────────────────────────────

class ReportReader:
    """Reads result tables and pipeline_runs from the relational DB."""

    def __init__(self, loader: ResultLoader):
        self._loader = loader
        self._loader._connect()
        self._cur = self._loader._cursor

    def _fetchall_as_dicts(self, sql: str, params: tuple = ()) -> List[Dict]:
        self._cur.execute(sql, params)
        cols = [d[0] for d in self._cur.description]
        return [dict(zip(cols, row)) for row in self._cur.fetchall()]

    def pipeline_runs(self, pipeline: str = None) -> List[Dict]:
        if pipeline:
            return self._fetchall_as_dicts(
                "SELECT * FROM pipeline_runs WHERE pipeline = %s ORDER BY executed_at DESC",
                (pipeline,),
            )
        return self._fetchall_as_dicts(
            "SELECT * FROM pipeline_runs ORDER BY executed_at DESC"
        )

    def q1(self, run_id: str = None, pipeline: str = None) -> List[Dict]:
        filters, params = self._build_filter(run_id, pipeline)
        return self._fetchall_as_dicts(
            f"SELECT * FROM q1_daily_traffic{filters} ORDER BY log_date, status_code",
            tuple(params),
        )

    def q2(self, run_id: str = None, pipeline: str = None) -> List[Dict]:
        filters, params = self._build_filter(run_id, pipeline)
        return self._fetchall_as_dicts(
            f"SELECT * FROM q2_top_resources{filters} ORDER BY request_count DESC",
            tuple(params),
        )

    def q3(self, run_id: str = None, pipeline: str = None) -> List[Dict]:
        filters, params = self._build_filter(run_id, pipeline)
        return self._fetchall_as_dicts(
            f"SELECT * FROM q3_hourly_errors{filters} ORDER BY log_date, log_hour",
            tuple(params),
        )

    @staticmethod
    def _build_filter(run_id, pipeline):
        clauses, params = [], []
        if run_id:
            clauses.append("run_id = %s"); params.append(run_id)
        if pipeline:
            clauses.append("pipeline = %s"); params.append(pipeline)
        filters = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return filters, params


# ─────────────────────────────────────────────────────────────────────────────
# Report renderer
# ─────────────────────────────────────────────────────────────────────────────

def render_report(
    pipeline: str = None,
    run_id: str = None,
    csv_out: bool = False,
    output=None,
) -> None:
    """
    Print (or write) a formatted report for the given filters.
    If `csv_out` is True, write CSV to `output` (default: stdout).
    """
    out = output or sys.stdout
    loader = ResultLoader()
    reader = ReportReader(loader)

    # ── Pipeline Runs Summary ─────────────────────────────────────────────────
    runs = reader.pipeline_runs(pipeline=pipeline)
    if not runs:
        print("No pipeline runs found in the database.", file=out)
        loader.close()
        return

    print("\n" + _divider("PIPELINE RUNS SUMMARY"), file=out)
    run_cols = [
        "pipeline", "run_id", "executed_at", "runtime_s",
        "parsed_ok", "malformed", "total_batches", "avg_batch_size",
    ]
    print(_fmt_table(runs, run_cols), file=out)

    # Filter to the latest (or specified) run per pipeline
    target_run_id = run_id or runs[0]["run_id"]

    # ── Q1 ────────────────────────────────────────────────────────────────────
    q1_rows = reader.q1(run_id=target_run_id, pipeline=pipeline)
    print(f"\n{_divider('Q1 — DAILY TRAFFIC SUMMARY')}", file=out)
    print(f"  Run: {target_run_id}  |  Rows: {len(q1_rows)}", file=out)
    q1_cols = ["log_date", "status_code", "request_count", "total_bytes", "pipeline"]
    print(_fmt_table(q1_rows[:50], q1_cols), file=out)
    if len(q1_rows) > 50:
        print(f"  … {len(q1_rows) - 50} more rows (use --csv to export all)", file=out)

    # ── Q2 ────────────────────────────────────────────────────────────────────
    q2_rows = reader.q2(run_id=target_run_id, pipeline=pipeline)
    print(f"\n{_divider('Q2 — TOP 20 RESOURCES BY HITS')}", file=out)
    print(f"  Run: {target_run_id}  |  Rows: {len(q2_rows)}", file=out)
    q2_cols = ["resource_path", "request_count", "total_bytes", "distinct_hosts", "pipeline"]
    print(_fmt_table(q2_rows, q2_cols), file=out)

    # ── Q3 ────────────────────────────────────────────────────────────────────
    q3_rows = reader.q3(run_id=target_run_id, pipeline=pipeline)
    print(f"\n{_divider('Q3 — HOURLY ERROR ANALYSIS')}", file=out)
    print(f"  Run: {target_run_id}  |  Rows: {len(q3_rows)}", file=out)
    q3_cols = [
        "log_date", "log_hour", "error_count", "total_requests",
        "error_rate", "distinct_error_hosts", "pipeline",
    ]
    print(_fmt_table(q3_rows[:50], q3_cols), file=out)
    if len(q3_rows) > 50:
        print(f"  … {len(q3_rows) - 50} more rows", file=out)

    # ── CSV export ────────────────────────────────────────────────────────────
    if csv_out:
        for name, rows, cols in [
            ("q1_daily_traffic",  q1_rows, q1_cols),
            ("q2_top_resources",  q2_rows, q2_cols),
            ("q3_hourly_errors",  q3_rows, q3_cols),
        ]:
            path = Path(f"{name}_{target_run_id[:8]}.csv")
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            print(f"  CSV written: {path}", file=out)

    loader.close()
    print(f"\n{_divider()}\n", file=out)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Render the NASA Log ETL pipeline comparison report."
    )
    parser.add_argument(
        "--pipeline", "-p",
        choices=["pig", "mapreduce", "mongodb", "hive"],
        help="Filter report to a specific pipeline",
    )
    parser.add_argument(
        "--run_id", "-r",
        help="Filter report to a specific run UUID",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Export result tables to CSV files",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        help="Python logging level (default: WARNING)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(levelname)s  %(name)s  %(message)s",
    )

    render_report(pipeline=args.pipeline, run_id=args.run_id, csv_out=args.csv)


if __name__ == "__main__":
    main()
