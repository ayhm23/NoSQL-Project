"""
tests/test_malformed_handler.py — Unit tests for MalformedHandler.
"""

import json
import os
import sys
from pathlib import Path
import pytest

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parser.malformed_handler import MalformedHandler, MalformedRecord

# Sample malformed lines representing each of the 8 categories
CLASSIFICATION_TEST_CASES = [
    # 1. empty_line
    ("", "empty_line"),
    ("   ", "empty_line"),
    # 2. missing_brackets
    ('199.72.81.55 - - 01/Jul/1995:00:00:01 -0400 "GET /index.html" 200 1234', "missing_brackets"),
    # 3. missing_quotes
    ('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] GET /index.html 200 1234', "missing_quotes"),
    # 4. bad_timestamp
    ('199.72.81.55 - - [01/Jul/1995:00:00:01] "GET /index.html" 200 1234', "bad_timestamp"),
    ('199.72.81.55 - - [01/BadMonth/1995:00:00:01 -0400] "GET /index.html" 200 1234', "bad_timestamp"),
    # 5. truncated
    ('199.72.81.55 [01/Jul/1995:00:00:01 -0400] "GET"', "truncated"),
    # 6. bad_status_code
    ('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html" ABC 1234', "bad_status_code"),
    # 7. bad_bytes_field
    ('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html" 200 XYZ', "bad_bytes_field"),
    # 8. unknown (let's assume any line that successfully parses status and bytes but has other weird syntax)
    # Wait, the unknown category: if status and bytes are valid, but somehow it's not a valid standard log record?
    # Actually, a line that has status and bytes valid but doesn't match standard regex. E.g.
    # '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html" 200 1234' is a completely valid record!
    # Wait, if parse_line parses it successfully, it's not malformed. But if parse_line treats it as malformed (e.g. status code is correct, but something else fails, or we just call handler.record directly on it),
    # then our handler will classify it as "unknown" since status/bytes are ok!
    ('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html" 200 1234', "unknown")
]


class TestMalformedHandlerClassification:
    @pytest.mark.parametrize("line, expected_reason", CLASSIFICATION_TEST_CASES)
    def test_classification_categories(self, line, expected_reason):
        handler = MalformedHandler()
        record = handler.record(line, 1, "test.log")
        assert record.reason == expected_reason


class TestMalformedHandlerInMemoryCap:
    def test_max_in_memory_cap(self):
        # Max capacity is 5 records
        handler = MalformedHandler(max_in_memory=5)
        for i in range(12):
            handler.record(f"line_{i}", i + 1, "test.log")

        # Total count should be 12
        assert handler.total == 12
        # But only 5 records stored in the in-memory list
        assert len(handler.records) == 5

        # Check in-memory contents are the first 5 records
        assert handler.records[0].raw_line == "line_0"
        assert handler.records[4].raw_line == "line_4"


class TestMalformedHandlerFlushToFile:
    def test_flush_to_file_writes_valid_jsonl(self, tmp_path):
        q_path = tmp_path / "quarantine.jsonl"
        handler = MalformedHandler(max_in_memory=10, quarantine_path=str(q_path))

        handler.record("line 1", 1, "test.log", batch_id=101)
        handler.record("line 2", 2, "test.log", batch_id=102)

        written = handler.flush_to_file()
        assert written == 2

        # Verify the file contents
        assert q_path.exists()
        with open(q_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 2
        
        # Parse the JSONL lines
        data0 = json.loads(lines[0])
        assert data0["raw_line"] == "line 1"
        assert data0["line_number"] == 1
        assert data0["file"] == "test.log"
        assert data0["batch_id"] == 101
        assert data0["reason"] == "missing_brackets"

        data1 = json.loads(lines[1])
        assert data1["raw_line"] == "line 2"
        assert data1["line_number"] == 2
        assert data1["file"] == "test.log"
        assert data1["batch_id"] == 102
        assert data1["reason"] == "missing_brackets"

    def test_flush_to_file_no_op_when_path_is_none(self):
        handler = MalformedHandler(quarantine_path=None)
        handler.record("line 1", 1, "test.log")
        written = handler.flush_to_file()
        assert written == 0

    def test_flush_to_file_does_not_raise_on_ioerror(self):
        # Using a directory path as the file path to cause an IOError/PermissionError
        invalid_path = "/" if sys.platform != "win32" else "C:\\Windows\\System32\\config"
        handler = MalformedHandler(quarantine_path=invalid_path, emit_warnings=False)
        handler.record("line 1", 1, "test.log")
        
        # This should return 0 and not raise any exception
        written = handler.flush_to_file()
        assert written == 0


class TestMalformedHandlerDBSummary:
    def test_to_db_summary_keys_and_values(self):
        handler = MalformedHandler()
        # Add various types of records
        handler.record("", 1, "test.log")  # empty_line
        handler.record("line no brackets", 2, "test.log")  # missing_brackets
        handler.record('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html" ABC 1234', 3, "test.log")  # bad_status_code

        summary = handler.to_db_summary(run_id="run123", pipeline="mongodb")

        # Required keys check
        required_keys = {
            "run_id", "pipeline", "total_malformed", "empty_line_count",
            "missing_brackets_count", "missing_quotes_count",
            "bad_timestamp_count", "bad_status_count", "bad_bytes_count",
            "truncated_count", "unknown_count", "sample_lines"
        }
        assert set(summary.keys()) == required_keys

        # Check counts
        assert summary["total_malformed"] == 3
        assert summary["empty_line_count"] == 1
        assert summary["missing_brackets_count"] == 1
        assert summary["bad_status_count"] == 1
        
        # Zero-filled check
        assert summary["missing_quotes_count"] == 0
        assert summary["bad_timestamp_count"] == 0
        assert summary["bad_bytes_count"] == 0
        assert summary["truncated_count"] == 0
        assert summary["unknown_count"] == 0

        # Sample lines check (JSON list of up to 5)
        samples = json.loads(summary["sample_lines"])
        assert len(samples) == 3
        assert samples[0] == ""
        assert samples[1] == "line no brackets"
        assert "ABC" in samples[2]

    def test_to_db_summary_zero_fills_missing_categories_on_empty_handler(self):
        handler = MalformedHandler()
        summary = handler.to_db_summary(run_id="run_empty", pipeline="mongodb")
        
        assert summary["total_malformed"] == 0
        assert summary["empty_line_count"] == 0
        assert summary["missing_brackets_count"] == 0
        assert summary["missing_quotes_count"] == 0
        assert summary["bad_timestamp_count"] == 0
        assert summary["bad_status_count"] == 0
        assert summary["bad_bytes_count"] == 0
        assert summary["truncated_count"] == 0
        assert summary["unknown_count"] == 0
        assert summary["sample_lines"] == "[]"


class TestMalformedHandlerSummaryAndTopReasons:
    def test_summary_is_always_safe(self):
        handler = MalformedHandler()
        res = handler.summary()
        assert isinstance(res, dict)
        assert res["total"] == 0
        assert isinstance(res["categories"], dict)
        assert res["categories"]["empty_line"] == 0

    def test_top_reasons_ranking(self):
        handler = MalformedHandler()
        
        # Add 3 empty_line
        for i in range(3):
            handler.record("", i + 1, "test.log")
        # Add 1 missing_brackets
        handler.record("brackets", 4, "test.log")
        # Add 5 bad_status_code
        for i in range(5):
            handler.record('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /" ABC 1', i + 5, "test.log")

        top = handler.top_reasons(n=3)
        assert len(top) == 3
        # Should be sorted descending by count
        assert top[0] == ("bad_status_code", 5)
        assert top[1] == ("empty_line", 3)
        assert top[2] == ("missing_brackets", 1)
