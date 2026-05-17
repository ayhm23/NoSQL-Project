import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from cli.validators import (
    validate_queries,
    validate_pipeline,
    check_data_files,
    ping_mysql,
    ping_mongo,
)

def test_validate_queries_all():
    assert validate_queries(["ALL"]) == ["q1", "q2", "q3"]

def test_validate_queries_dedup_and_lower():
    assert validate_queries(["q1", "Q2", "q1"]) == ["q1", "q2"]

def test_validate_queries_unknown():
    with pytest.raises(ValueError):
        validate_queries(["q9"])

def test_validate_pipeline():
    assert validate_pipeline("mongodb") is True
    assert validate_pipeline("pig") is False
    assert validate_pipeline("unknown") is False

def test_check_data_files():
    # Providing a non-existent path should return it in the list
    missing = check_data_files([Path("/nonexistent_path_for_test")])
    assert len(missing) == 1
    assert missing[0] == Path("/nonexistent_path_for_test")

@patch.dict('sys.modules', {'mysql': MagicMock(), 'mysql.connector': MagicMock()})
def test_ping_mysql():
    import sys
    # Wire the mock connector to the mock mysql module
    sys.modules['mysql'].connector = sys.modules['mysql.connector']
    
    # Setup mock
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)
    
    mock_connector = sys.modules['mysql.connector']
    mock_connector.connect.return_value = mock_conn

    assert ping_mysql() is True
    mock_connector.connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT 1")

@patch('pymongo.MongoClient')
def test_ping_mongo(mock_mongo_client):
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    assert ping_mongo() is True
    mock_client_instance.admin.command.assert_called_once_with('ping')
