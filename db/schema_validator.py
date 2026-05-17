"""
db/schema_validator.py
=======================
Standalone validation script to check that the database schema is compliant.

Usage:
    python -m db.schema_validator [--dialect mysql|postgresql]
"""

import os
import sys
import argparse
import logging
from typing import Dict, List, Set

import config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("schema_validator")

# Define the expected schema
EXPECTED_TABLES = {
    "etl_runs": {
        "run_id", "pipeline", "batch_size", "total_records", "malformed_records",
        "num_batches", "avg_batch_size", "runtime_seconds", "started_at", "completed_at"
    },
    "batch_metadata": {
        "id", "run_id", "pipeline", "batch_id", "batch_size_config", "records_in_batch",
        "started_at", "completed_at"
    },
    "q1_daily_traffic": {
        "id", "run_id", "pipeline", "query_name", "log_date", "status_code", "request_count", "total_bytes"
    },
    "q2_top_resources": {
        "id", "run_id", "pipeline", "query_name", "resource_path", "request_count", "total_bytes", "distinct_host_count"
    },
    "q3_hourly_errors": {
        "id", "run_id", "pipeline", "query_name", "log_date", "log_hour", "error_request_count", "total_request_count", "error_rate", "distinct_error_hosts"
    },
    "malformed_record_summary": {
        "id", "run_id", "pipeline", "total_malformed", "empty_line_count", "missing_brackets_count", "missing_quotes_count", "bad_timestamp_count", "bad_status_count", "bad_bytes_count", "truncated_count", "unknown_count", "sample_lines"
    }
}

EXPECTED_INDEXES = {
    "batch_metadata": {"idx_bm_run_id", "idx_bm_batch_id"},
    "q1_daily_traffic": {"idx_q1_run_id", "idx_q1_date"},
    "q2_top_resources": {"idx_q2_run_id", "idx_q2_request_count"},
    "q3_hourly_errors": {"idx_q3_run_id", "idx_q3_date_hour"},
    "malformed_record_summary": {"idx_mal_run_id"}
}


def get_connection(dialect: str):
    host     = os.getenv("DB_HOST", config.PG_HOST if dialect == "postgresql" else config.MYSQL_HOST)
    db_name  = os.getenv("DB_NAME", config.PG_DB if dialect == "postgresql" else config.MYSQL_DB)
    user     = os.getenv("DB_USER", config.PG_USER if dialect == "postgresql" else config.MYSQL_USER)
    password = os.getenv("DB_PASSWORD", config.PG_PASSWORD if dialect == "postgresql" else config.MYSQL_PASSWORD)

    if dialect == "mysql":
        import mysql.connector
        port = int(os.getenv("DB_PORT", "3306"))
        return mysql.connector.connect(
            host=host, port=port, database=db_name,
            user=user, password=password, autocommit=True
        )
    elif dialect == "postgresql":
        import psycopg2
        port = int(os.getenv("DB_PORT", "5432"))
        conn = psycopg2.connect(
            host=host, port=port, dbname=db_name,
            user=user, password=password
        )
        conn.autocommit = True
        return conn
    else:
        raise ValueError(f"Unsupported DB dialect: {dialect}")


def validate_schema(dialect: str) -> bool:
    try:
        conn = get_connection(dialect)
    except Exception as e:
        print(f"Error: Connection to DB failed: {e}")
        return False

    cursor = conn.cursor()
    all_passed = True

    print("\n" + "=" * 80)
    print(f" SCHEMA VALIDATION REPORT ({dialect.upper()})")
    print("=" * 80)

    db_name = os.getenv("DB_NAME", config.PG_DB if dialect == "postgresql" else config.MYSQL_DB)

    if dialect == "mysql":
        cursor.execute("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s
        """, (db_name,))
    else:
        cursor.execute("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public'
        """)

    db_columns: Dict[str, Set[str]] = {}
    for t_name, c_name in cursor.fetchall():
        t_name = t_name.lower()
        if t_name not in db_columns:
            db_columns[t_name] = set()
        db_columns[t_name].add(c_name.lower())

    # Fetch existing indexes
    db_indexes: Dict[str, Set[str]] = {}

    for table in EXPECTED_TABLES.keys():
        db_indexes[table] = set()
        try:
            if dialect == "mysql":
                cursor.execute(f"SHOW INDEX FROM `{table}`")
                for row in cursor.fetchall():
                    db_indexes[table].add(row[2].lower())
            else:
                cursor.execute("""
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE schemaname = 'public' AND tablename = %s
                """, (table,))
                for row in cursor.fetchall():
                    db_indexes[table].add(row[0].lower())
        except Exception:
            pass

    # Print Results Table
    print(f"{'Table / Component':<30} | {'Status':<6} | {'Issues'}")
    print("-" * 80)

    for table, exp_cols in EXPECTED_TABLES.items():
        if table not in db_columns:
            print(f"{table:<30} | {C_RED('✗ FAIL')} | Table does not exist in DB")
            all_passed = False
            continue

        actual_cols = db_columns[table]
        missing_cols = exp_cols - actual_cols

        # Check indexes
        exp_idx = EXPECTED_INDEXES.get(table, set())
        actual_idx = db_indexes[table]
        missing_idx = exp_idx - actual_idx

        table_ok = True
        issues = []

        if missing_cols:
            table_ok = False
            issues.append(f"Missing columns: {', '.join(missing_cols)}")
        if missing_idx:
            table_ok = False
            issues.append(f"Missing indexes: {', '.join(missing_idx)}")

        if table_ok:
            print(f"{table:<30} | {C_GREEN('✓ PASS')} | All columns and indexes OK")
        else:
            issue_str = "; ".join(issues)
            print(f"{table:<30} | {C_RED('✗ FAIL')} | {issue_str}")
            all_passed = False

    print("=" * 80)
    cursor.close()
    conn.close()
    return all_passed


def C_GREEN(s: str) -> str:
    return f"\033[92m{s}\033[0m" if sys.stdout.isatty() else s


def C_RED(s: str) -> str:
    return f"\033[91m{s}\033[0m" if sys.stdout.isatty() else s


def main():
    dialect_default = os.getenv("DB_DIALECT", config.RDBMS_TYPE or "mysql").lower()
    parser = argparse.ArgumentParser(description="Validate DB Schema.")
    parser.add_argument("--dialect", choices=["mysql", "postgresql"], default=dialect_default)
    args = parser.parse_args()

    passed = validate_schema(args.dialect)
    if passed:
        print("Schema Validation Success! ✓ All components compliant.")
        sys.exit(0)
    else:
        print("Schema Validation FAILED! ✗ Fix issues before running pipelines.")
        sys.exit(1)


if __name__ == "__main__":
    main()
