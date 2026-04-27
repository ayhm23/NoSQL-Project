"""
main.py — NASA Log ETL Pipeline — CLI entry point.

Usage:
    python main.py --pipeline mongodb [--batch-size 50000] [--report] [--no-drop]
    python main.py --pipeline mongodb --report-only
    python main.py --pipeline mongodb --report --csv

Pipelines available:
    mongodb     ← Implemented (this project)
    pig         ← Stub (to be implemented)
    mapreduce   ← Stub (to be implemented)
    hive        ← Stub (to be implemented)
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import config
from reporting.report import render_report


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline registry — add new pipelines here
# ─────────────────────────────────────────────────────────────────────────────

def _get_pipeline_class(name: str):
    """Lazy-import the pipeline class to avoid loading all dependencies at startup."""
    name = name.lower()
    if name == "mongodb":
        from pipelines.mongo_pipeline import MongoPipeline
        return MongoPipeline
    elif name == "pig":
        raise NotImplementedError("Pig pipeline not yet implemented")
    elif name == "mapreduce":
        raise NotImplementedError("MapReduce pipeline not yet implemented")
    elif name == "hive":
        raise NotImplementedError("Hive pipeline not yet implemented")
    else:
        raise ValueError(f"Unknown pipeline: {name!r}")


# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _check_data_files(log_files) -> bool:
    missing = [str(f) for f in log_files if not Path(f).exists()]
    if missing:
        print("ERROR: The following data files are missing:", file=sys.stderr)
        for f in missing:
            print(f"  {f}", file=sys.stderr)
        print(
            "\nDownload from the ITA archive:\n"
            "  https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html\n"
            "Place both .gz files in the data/ directory.",
            file=sys.stderr,
        )
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="nasa-etl",
        description=(
            "NASA HTTP Log ETL Pipeline — DAS 839 Project\n"
            "Processes NASA web server logs through one of four big-data pipelines."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--pipeline", "-p",
        required=True,
        choices=["mongodb", "pig", "mapreduce", "hive"],
        help="Pipeline engine to use",
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=config.BATCH_SIZE,
        metavar="N",
        help=f"Records per batch (default: {config.BATCH_SIZE:,})",
    )
    parser.add_argument(
        "--report", "-R",
        action="store_true",
        help="Print the comparison report after the pipeline finishes",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Skip ETL; only render the report from existing DB data",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Export report tables to CSV files (used with --report or --report-only)",
    )
    parser.add_argument(
        "--no-drop",
        action="store_true",
        help="[MongoDB] Keep the raw_logs collection after queries (default: drop it)",
    )
    parser.add_argument(
        "--log-level",
        default=config.LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help=f"Logging verbosity (default: {config.LOG_LEVEL})",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    # ── Report-only mode ──────────────────────────────────────────────────────
    if args.report_only:
        print(f"\n{'═'*80}")
        print(f"  NASA LOG ETL — Report  |  Pipeline filter: {args.pipeline}")
        print(f"{'═'*80}")
        render_report(pipeline=args.pipeline, csv_out=args.csv)
        return 0

    # ── ETL mode ──────────────────────────────────────────────────────────────
    if not _check_data_files(config.LOG_FILES):
        return 1

    PipelineClass = _get_pipeline_class(args.pipeline)

    print(f"\n{'═'*80}")
    print(f"  NASA LOG ETL   Pipeline: {args.pipeline.upper()}   Batch size: {args.batch_size:,}")
    print(f"{'═'*80}\n")

    # Pipeline-specific kwargs
    kwargs = {}
    if args.pipeline == "mongodb":
        kwargs["drop_after"] = not args.no_drop

    pipeline = PipelineClass(
        log_files=config.LOG_FILES,
        batch_size=args.batch_size,
        **kwargs,
    )

    t0 = time.perf_counter()
    summary = pipeline.run()
    elapsed = time.perf_counter() - t0

    print(f"\n{'─'*80}")
    print(f"  ✓ Pipeline finished in {elapsed:.2f}s")
    print(f"  Parsed:    {summary.get('parsed_ok', 0):,} records")
    print(f"  Malformed: {summary.get('malformed', 0):,} records")
    print(f"  Batches:   {summary.get('total_batches', 0):,}  (avg size: {summary.get('avg_batch_size', 0):,.0f})")
    print(f"  Run ID:    {summary.get('run_id', '')}")
    print(f"{'─'*80}\n")

    # Write run summary to DB
    try:
        from db.loader import ResultLoader
        with ResultLoader() as loader:
            loader.write_run_summary({
                "run_id":            summary["run_id"],
                "pipeline":          summary["pipeline"],
                "executed_at":       pipeline.execution_ts,
                "runtime_s":         summary.get("runtime_s"),
                "total_lines":       summary.get("total_lines"),
                "parsed_ok":         summary.get("parsed_ok"),
                "malformed":         summary.get("malformed"),
                "total_batches":     summary.get("total_batches"),
                "non_empty_batches": summary.get("non_empty_batches"),
                "avg_batch_size":    summary.get("avg_batch_size"),
            })
    except Exception as e:
        logging.getLogger(__name__).warning("Could not write run summary: %s", e)

    if args.report:
        render_report(pipeline=args.pipeline, run_id=summary["run_id"], csv_out=args.csv)

    return 0


if __name__ == "__main__":
    sys.exit(main())
