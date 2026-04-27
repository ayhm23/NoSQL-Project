"""
config.py — Central configuration for the NASA Log ETL Pipeline.

All tunable parameters live here. Pipelines import from this file
so there is exactly one source of truth.
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent
DATA_DIR   = BASE_DIR / "data"
LOG_FILES  = [
    DATA_DIR / "NASA_access_log_Jul95.gz",
    DATA_DIR / "NASA_access_log_Aug95.gz",
]

# ─────────────────────────────────────────────
# Batching
# ─────────────────────────────────────────────
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50_000))   # records per batch

# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────
MONGO_URI  = os.getenv("MONGO_URI",  "mongodb://localhost:27017/")
MONGO_DB   = os.getenv("MONGO_DB",   "nasa_logs")
MONGO_COLL = os.getenv("MONGO_COLL", "raw_logs")

# ─────────────────────────────────────────────
# Relational DB  (MySQL or PostgreSQL)
# ─────────────────────────────────────────────
RDBMS_TYPE = os.getenv("RDBMS_TYPE", "postgresql")   # "mysql" or "postgresql"

# PostgreSQL defaults
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB",       "etl_results")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# MySQL defaults
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB       = os.getenv("MYSQL_DB",       "etl_results")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
