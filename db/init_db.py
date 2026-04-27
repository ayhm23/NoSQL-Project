"""
db/init_db.py
=============
Run this ONCE to create all tables in MySQL / PostgreSQL.

Usage:
    python -m db.init_db

Required env vars (or set in .env):
    DB_DIALECT   mysql | postgresql
    DB_HOST      localhost
    DB_PORT      3306 | 5432
    DB_NAME      etl_results
    DB_USER      root
    DB_PASSWORD  your_password
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def _split_statements(sql: str):
    """Split a SQL file into individual statements, skipping comments."""
    statements = []
    current = []

    for line in sql.splitlines():
        stripped = line.strip()
        # Skip comment lines and blank lines
        if stripped.startswith("--") or stripped == "":
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

    return statements


def init_db():
    dialect  = os.getenv("DB_DIALECT", "mysql").lower()
    host     = os.getenv("DB_HOST", "localhost")
    db_name  = os.getenv("DB_NAME", "etl_results")
    user     = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")

    print(f"[init_db] Connecting to {dialect}://{user}@{host}/{db_name} ...")

    if dialect == "mysql":
        import mysql.connector
        port = int(os.getenv("DB_PORT", "3306"))
        conn = mysql.connector.connect(
            host=host, port=port, database=db_name,
            user=user, password=password, autocommit=True
        )
    elif dialect == "postgresql":
        import psycopg2
        port = int(os.getenv("DB_PORT", "5432"))
        conn = psycopg2.connect(host=host, port=port, dbname=db_name,
                                user=user, password=password)
        conn.autocommit = True
    else:
        print(f"[init_db] Unsupported DB_DIALECT: '{dialect}'")
        sys.exit(1)

    with open(SCHEMA_PATH, "r") as f:
        sql = f.read()

    # PostgreSQL uses SERIAL / BIGSERIAL, not AUTO_INCREMENT
    if dialect == "postgresql":
        sql = sql.replace("BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
                          "BIGSERIAL PRIMARY KEY")
        sql = sql.replace("AUTO_INCREMENT", "")
        sql = sql.replace("CREATE OR REPLACE VIEW", "CREATE OR REPLACE VIEW")

    cursor = conn.cursor()
    statements = _split_statements(sql)

    print(f"[init_db] Executing {len(statements)} statements...")
    for i, stmt in enumerate(statements, 1):
        try:
            cursor.execute(stmt)
            first_line = stmt.splitlines()[0][:60]
            print(f"  [{i:02d}] OK  {first_line}")
        except Exception as e:
            print(f"  [{i:02d}] FAIL  {e}")
            print(f"        Statement: {stmt[:120]}")

    cursor.close()
    conn.close()
    print("\n[init_db] Schema initialised successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
