import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from db.init_db import SchemaInitialiser
from db.loader import ResultLoader


# 1. split_statements() handles multi-line SQL correctly
def test_split_statements_multiline():
    sql = """
    CREATE TABLE t1 (
        a INT
    );
    CREATE TABLE t2 (
        b INT
    );
    """
    stmts = SchemaInitialiser.split_statements(sql)
    assert len(stmts) == 2
    assert "CREATE TABLE t1" in stmts[0]
    assert "CREATE TABLE t2" in stmts[1]


# 2. split_statements() skips comment-only and blank lines
def test_split_statements_skips_comments():
    sql = """
    -- This is a comment
    # Another comment
    
    CREATE TABLE t1 (a INT);
    
    -- trailing comment
    """
    stmts = SchemaInitialiser.split_statements(sql)
    assert len(stmts) == 1
    assert stmts[0] == "CREATE TABLE t1 (a INT);"


# 3. SchemaInitialiser("mysql").schema_path() returns schema_mysql.sql
def test_schema_path_mysql():
    init = SchemaInitialiser("mysql")
    path = init.schema_path()
    assert path.name == "schema_mysql.sql"


# 4. SchemaInitialiser("postgresql").schema_path() returns schema.sql
def test_schema_path_postgres():
    init = SchemaInitialiser("postgresql")
    path = init.schema_path()
    assert path.name == "schema.sql"


# 5. health_check() returns False when connection fails (mock connector)
@patch("db.loader._get_connection")
def test_health_check_fail(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("DB connection down")
    mock_get_conn.return_value = (mock_conn, "%s")

    loader = ResultLoader()
    loader._cursor = mock_cursor

    assert loader.health_check() is False


# 6. save_batch_metadata() inserts correct column values (mock cursor)
@patch("db.loader._get_connection")
def test_save_batch_metadata(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = (mock_conn, "%s")

    loader = ResultLoader()
    loader._cursor = mock_cursor

    batches = [
        {
            "batch_id": 1,
            "batch_size_config": 50000,
            "records_in_batch": 48200,
            "started_at": None,
            "completed_at": None
        }
    ]
    loader.save_batch_metadata("run_123", "mongodb", batches)

    mock_cursor.executemany.assert_called_once()
    sql, data = mock_cursor.executemany.call_args[0]
    assert "INSERT INTO batch_metadata" in sql
    assert "batch_size_config" in sql
    assert "records_in_batch" in sql
    assert data[0] == ("run_123", "mongodb", 1, 50000, 48200, None, None)


# 7. save_q2() uses "distinct_host_count" column name (verify SQL string)
@patch("db.loader._get_connection")
def test_save_q2_column_name(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = (mock_conn, "%s")

    loader = ResultLoader()
    loader._cursor = mock_cursor

    rows = [{
        "resource_path": "/index.html",
        "request_count": 100,
        "total_bytes": 5000,
        "distinct_host_count": 25
    }]
    loader.save_q2("run_123", "mongodb", rows)

    mock_cursor.executemany.assert_called_once()
    sql, data = mock_cursor.executemany.call_args[0]
    assert "distinct_host_count" in sql
    assert "distinct_hosts" not in sql
    assert "query_name" in sql


# 8. save_q3() uses "error_request_count" and "total_request_count" (verify SQL)
@patch("db.loader._get_connection")
def test_save_q3_column_names(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = (mock_conn, "%s")

    loader = ResultLoader()
    loader._cursor = mock_cursor

    rows = [{
        "log_date": "1995-07-01",
        "log_hour": 12,
        "error_request_count": 10,
        "total_request_count": 200,
        "error_rate": 0.05,
        "distinct_error_hosts": 8
    }]
    loader.save_q3("run_123", "mongodb", rows)

    mock_cursor.executemany.assert_called_once()
    sql, data = mock_cursor.executemany.call_args[0]
    assert "error_request_count" in sql
    assert "total_request_count" in sql
    assert "error_count" not in sql
    assert "total_requests" not in sql
    assert "query_name" in sql
