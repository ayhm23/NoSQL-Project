import pymongo
import config
from pathlib import Path

PIPELINE_STATUS = {
    "pig":       False,
    "mapreduce": True,
    "mongodb":   True,
    "hive":      False,
}

def validate_queries(raw: list[str]) -> list[str]:
    """Normalise to lowercase, expand "all", deduplicate, validate."""
    if not raw:
        return ["q1", "q2", "q3"]
    
    cleaned = set()
    for q in raw:
        q_lower = q.lower()
        if q_lower == "all":
            cleaned.update(["q1", "q2", "q3"])
        elif q_lower in ("q1", "q2", "q3"):
            cleaned.add(q_lower)
        else:
            raise ValueError(f"Unknown query: {q}")
    return sorted(list(cleaned))

def validate_pipeline(name: str) -> bool:
    """Returns True if pipeline is implemented, False if stub."""
    return PIPELINE_STATUS.get(name.lower(), False)

def check_data_files(paths: list) -> list[Path]:
    """Returns list of missing paths. Empty = all present."""
    return [Path(p) for p in paths if not Path(p).exists()]

def ping_mysql() -> bool:
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            database=config.MYSQL_DB,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            connection_timeout=2
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return True
    except Exception:
        return False

def ping_postgres() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=config.PG_HOST,
            port=config.PG_PORT,
            dbname=config.PG_DB,
            user=config.PG_USER,
            password=config.PG_PASSWORD,
            connect_timeout=2
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return True
    except Exception:
        return False

def ping_mongo() -> bool:
    try:
        client = pymongo.MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
        return True
    except Exception:
        return False

def ping_db() -> bool:
    if config.RDBMS_TYPE.lower() == "mysql":
        return ping_mysql()
    return ping_postgres()
