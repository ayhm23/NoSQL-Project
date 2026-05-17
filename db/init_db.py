"""
db/init_db.py
=============
Run this ONCE to create all tables in MySQL / PostgreSQL.

Usage:
    python -m db.init_db [--dialect mysql|postgresql] [--dry-run]
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any

import config

logger = logging.getLogger(__name__)


class SchemaInitialiser:
    def __init__(self, dialect: str):
        # dialect: "mysql" or "postgresql"
        self.dialect = dialect.lower()
        if self.dialect not in ("mysql", "postgresql"):
            raise ValueError(f"Unsupported DB dialect: {dialect}")

    def connect(self):
        """Return connected DB-API 2 connection. Raises on failure."""
        dialect = self.dialect
        host     = os.getenv("DB_HOST", config.PG_HOST if dialect == "postgresql" else config.MYSQL_HOST)
        db_name  = os.getenv("DB_NAME", config.PG_DB if dialect == "postgresql" else config.MYSQL_DB)
        user     = os.getenv("DB_USER", config.PG_USER if dialect == "postgresql" else config.MYSQL_USER)
        password = os.getenv("DB_PASSWORD", config.PG_PASSWORD if dialect == "postgresql" else config.MYSQL_PASSWORD)

        if dialect == "mysql":
            import mysql.connector
            port = int(os.getenv("DB_PORT", str(config.MYSQL_PORT)))
            conn = mysql.connector.connect(
                host=host, port=port, database=db_name,
                user=user, password=password, autocommit=True
            )
            return conn

        elif dialect == "postgresql":
            import psycopg2
            port = int(os.getenv("DB_PORT", str(config.PG_PORT)))
            conn = psycopg2.connect(
                host=host, port=port, dbname=db_name,
                user=user, password=password
            )
            conn.autocommit = True
            return conn

    def schema_path(self) -> Path:
        """Return Path to db/schema_mysql.sql or db/schema.sql."""
        db_dir = Path(__file__).resolve().parent
        if self.dialect == "mysql":
            return db_dir / "schema_mysql.sql"
        else:
            return db_dir / "schema.sql"

    def run(self) -> dict:
        """
        Execute all statements. Returns:
          {"statements_ok": int, "statements_failed": int, "errors": list[str]}
        Never raises. Idempotent (safe to run multiple times).
        """
        results = {"statements_ok": 0, "statements_failed": 0, "errors": []}
        try:
            conn = self.connect()
        except Exception as e:
            msg = f"Connection failed: {e}"
            logger.error(msg)
            results["errors"].append(msg)
            return results

        try:
            s_path = self.schema_path()
            if not s_path.exists():
                msg = f"Schema file not found: {s_path}"
                logger.error(msg)
                results["errors"].append(msg)
                conn.close()
                return results

            with open(s_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            statements = self.split_statements(sql_content)
            cursor = conn.cursor()

            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    results["statements_ok"] += 1
                except Exception as ex:
                    results["statements_failed"] += 1
                    err_msg = f"Failed statement: {stmt.splitlines()[0][:60]} -> {ex}"
                    logger.error(err_msg)
                    results["errors"].append(err_msg)

            cursor.close()
            conn.close()
        except Exception as e:
            msg = f"Execution error: {e}"
            logger.error(msg)
            results["errors"].append(msg)

        return results

    @staticmethod
    def split_statements(sql: str) -> list[str]:
        """Split SQL into individual statements, skipping comments."""
        statements = []
        current = []

        for line in sql.splitlines():
            stripped = line.strip()
            # Skip comment lines and blank lines
            if stripped.startswith("--") or stripped.startswith("#") or stripped == "":
                continue
            current.append(line)
            if stripped.endswith(";"):
                stmt = "\n".join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []

        if current:
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)

        return statements


def main():
    dialect_default = os.getenv("DB_DIALECT", config.RDBMS_TYPE or "mysql").lower()
    parser = argparse.ArgumentParser(description="Initialise database schema.")
    parser.add_argument("--dialect", choices=["mysql", "postgresql"], default=dialect_default)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Initialising dialect={args.dialect} ...")
    if args.dry_run:
        init = SchemaInitialiser(args.dialect)
        path = init.schema_path()
        print(f"[DRY-RUN] Would execute SQL from: {path}")
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                statements = SchemaInitialiser.split_statements(f.read())
            print(f"[DRY-RUN] Found {len(statements)} statements to execute.")
        else:
            print(f"[DRY-RUN] Schema file does not exist!")
        return

    init = SchemaInitialiser(args.dialect)
    res = init.run()
    print(f"Result: {res['statements_ok']} OK, {res['statements_failed']} failed.")
    if res["errors"]:
        print("Errors encountered:")
        for err in res["errors"]:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("Schema initialised successfully.")
        sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
