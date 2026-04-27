"""
tests/test_log_parser.py — Comprehensive unit tests for parser/log_parser.py

Tests cover:
  • Valid log lines (standard NASA format)
  • Edge cases (bytes="-", missing protocol, empty request string)
  • Malformed lines (truncated, bad status, bad timestamp)
  • Timestamp parsing month mapping
  • ParseStats accumulation
  • stream_records() with in-memory gzip data
"""

import gzip
import io
import sys
import os
import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parser.log_parser import (
    parse_line,
    LogRecord,
    ParseStats,
    stream_records,
    _parse_timestamp,
)


# ─────────────────────────────────────────────────────────────────────────────
# Test data
# ─────────────────────────────────────────────────────────────────────────────

VALID_LINES = [
    # Standard GET with bytes
    (
        '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
        LogRecord(
            host="199.72.81.55",
            timestamp="01/Jul/1995:00:00:01 -0400",
            log_date="1995-07-01",
            log_hour=0,
            http_method="GET",
            resource_path="/history/apollo/",
            protocol_version="HTTP/1.0",
            status_code=200,
            bytes_transferred=6245,
        ),
    ),
    # Named host
    (
        'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985',
        LogRecord(
            host="unicomp6.unicomp.net",
            timestamp="01/Jul/1995:00:00:06 -0400",
            log_date="1995-07-01",
            log_hour=0,
            http_method="GET",
            resource_path="/shuttle/countdown/",
            protocol_version="HTTP/1.0",
            status_code=200,
            bytes_transferred=3985,
        ),
    ),
    # Bytes = "-" → 0
    (
        '199.72.81.55 - - [01/Jul/1995:12:34:56 -0400] "GET /missing.html HTTP/1.0" 404 -',
        LogRecord(
            host="199.72.81.55",
            timestamp="01/Jul/1995:12:34:56 -0400",
            log_date="1995-07-01",
            log_hour=12,
            http_method="GET",
            resource_path="/missing.html",
            protocol_version="HTTP/1.0",
            status_code=404,
            bytes_transferred=0,
        ),
    ),
    # POST request
    (
        'example.com - - [15/Aug/1995:23:59:59 -0400] "POST /cgi-bin/login HTTP/1.0" 302 512',
        LogRecord(
            host="example.com",
            timestamp="15/Aug/1995:23:59:59 -0400",
            log_date="1995-08-15",
            log_hour=23,
            http_method="POST",
            resource_path="/cgi-bin/login",
            protocol_version="HTTP/1.0",
            status_code=302,
            bytes_transferred=512,
        ),
    ),
    # HEAD request, 304
    (
        'crawler.bot - - [10/Jul/1995:08:00:00 -0400] "HEAD /index.html HTTP/1.0" 304 -',
        LogRecord(
            host="crawler.bot",
            timestamp="10/Jul/1995:08:00:00 -0400",
            log_date="1995-07-10",
            log_hour=8,
            http_method="HEAD",
            resource_path="/index.html",
            protocol_version="HTTP/1.0",
            status_code=304,
            bytes_transferred=0,
        ),
    ),
    # 500 error with bytes
    (
        '10.0.0.1 - - [31/Jul/1995:11:22:33 -0400] "GET /crash HTTP/1.0" 500 1024',
        LogRecord(
            host="10.0.0.1",
            timestamp="31/Jul/1995:11:22:33 -0400",
            log_date="1995-07-31",
            log_hour=11,
            http_method="GET",
            resource_path="/crash",
            protocol_version="HTTP/1.0",
            status_code=500,
            bytes_transferred=1024,
        ),
    ),
]

MALFORMED_LINES = [
    "",                          # empty
    "   ",                       # whitespace only
    "this is not a log line",
    "199.72.81.55 - - BADTIME GET /foo HTTP/1.0 200 123",  # no brackets
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /foo HTTP/1.0" abc 100',  # bad status
    '199.72.81.55 - - [01/BadMonth/1995:00:00:01 -0400] "GET /foo HTTP/1.0" 200 100',  # bad month
    '199.72.81.55 - -',          # truncated
]


# ─────────────────────────────────────────────────────────────────────────────
# Tests: parse_line
# ─────────────────────────────────────────────────────────────────────────────

class TestParseLine:
    @pytest.mark.parametrize("line, expected", VALID_LINES)
    def test_valid_line(self, line, expected):
        record, malformed = parse_line(line)
        assert not malformed
        assert record is not None
        assert record.host             == expected.host
        assert record.timestamp        == expected.timestamp
        assert record.log_date         == expected.log_date
        assert record.log_hour         == expected.log_hour
        assert record.http_method      == expected.http_method
        assert record.resource_path    == expected.resource_path
        assert record.protocol_version == expected.protocol_version
        assert record.status_code      == expected.status_code
        assert record.bytes_transferred == expected.bytes_transferred

    @pytest.mark.parametrize("line", MALFORMED_LINES)
    def test_malformed_line(self, line):
        record, malformed = parse_line(line)
        assert malformed is True
        assert record is None

    def test_bytes_dash_becomes_zero(self):
        line = '1.2.3.4 - - [01/Jul/1995:00:00:01 -0400] "GET /foo HTTP/1.0" 200 -'
        record, malformed = parse_line(line)
        assert not malformed
        assert record.bytes_transferred == 0

    def test_large_bytes(self):
        line = '1.2.3.4 - - [01/Jul/1995:00:00:01 -0400] "GET /large HTTP/1.0" 200 999999999'
        record, malformed = parse_line(line)
        assert not malformed
        assert record.bytes_transferred == 999_999_999

    def test_status_code_range(self):
        for status in [100, 200, 301, 404, 500, 599]:
            line = f'1.2.3.4 - - [01/Jul/1995:00:00:01 -0400] "GET /x HTTP/1.0" {status} 0'
            record, malformed = parse_line(line)
            assert not malformed
            assert record.status_code == status

    def test_to_dict_has_all_fields(self):
        line, expected = VALID_LINES[0]
        record, _ = parse_line(line)
        d = record.to_dict()
        required = {
            "host", "timestamp", "log_date", "log_hour",
            "http_method", "resource_path", "protocol_version",
            "status_code", "bytes_transferred",
        }
        assert required.issubset(set(d.keys()))

    def test_line_with_trailing_newline(self):
        line = '1.2.3.4 - - [01/Jul/1995:00:00:01 -0400] "GET /foo HTTP/1.0" 200 100\n'
        record, malformed = parse_line(line)
        assert not malformed
        assert record is not None

    def test_request_without_protocol(self):
        # Some log entries lack the HTTP/x.x part
        line = '1.2.3.4 - - [01/Jul/1995:00:00:01 -0400] "GET /foo" 200 100'
        record, malformed = parse_line(line)
        # Should parse with empty protocol_version
        assert not malformed
        assert record.protocol_version == ""
        assert record.resource_path == "/foo"


# ─────────────────────────────────────────────────────────────────────────────
# Tests: _parse_timestamp
# ─────────────────────────────────────────────────────────────────────────────

class TestParseTimestamp:
    @pytest.mark.parametrize("ts, expected_date, expected_hour", [
        ("01/Jan/1995:00:00:00 -0400", "1995-01-01", 0),
        ("28/Feb/1995:12:30:00 -0400", "1995-02-28", 12),
        ("31/Dec/1995:23:59:59 -0400", "1995-12-31", 23),
        ("01/Jul/1995:08:15:30 -0400", "1995-07-01", 8),
        ("15/Aug/1995:16:45:00 -0400", "1995-08-15", 16),
    ])
    def test_valid_timestamps(self, ts, expected_date, expected_hour):
        log_date, log_hour = _parse_timestamp(ts)
        assert log_date == expected_date
        assert log_hour == expected_hour

    def test_all_months_parsed(self):
        months = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        for mon_str, mon_int in months.items():
            ts = f"01/{mon_str}/1995:00:00:00 -0400"
            log_date, log_hour = _parse_timestamp(ts)
            assert log_date is not None
            assert log_date == f"1995-{mon_int:02d}-01"

    def test_invalid_month(self):
        log_date, log_hour = _parse_timestamp("01/Xyz/1995:00:00:00 -0400")
        assert log_date is None
        assert log_hour is None

    def test_badly_formatted_timestamp(self):
        log_date, log_hour = _parse_timestamp("not-a-timestamp")
        assert log_date is None


# ─────────────────────────────────────────────────────────────────────────────
# Tests: ParseStats
# ─────────────────────────────────────────────────────────────────────────────

class TestParseStats:
    def test_initial_state(self):
        s = ParseStats()
        assert s.total_lines == 0
        assert s.parsed_ok == 0
        assert s.malformed == 0
        assert s.parse_rate == 0.0

    def test_record_ok(self):
        s = ParseStats()
        s.record(True)
        s.record(True)
        assert s.total_lines == 2
        assert s.parsed_ok == 2
        assert s.malformed == 0
        assert s.parse_rate == 1.0

    def test_record_malformed(self):
        s = ParseStats()
        s.record(False, "bad line")
        assert s.malformed == 1
        assert s.malformed_examples == ["bad line"]

    def test_malformed_examples_capped_at_10(self):
        s = ParseStats()
        for i in range(20):
            s.record(False, f"line {i}")
        assert len(s.malformed_examples) == 10
        assert s.malformed == 20

    def test_parse_rate(self):
        s = ParseStats()
        for _ in range(8):
            s.record(True)
        for _ in range(2):
            s.record(False)
        assert abs(s.parse_rate - 0.8) < 1e-9

    def test_repr(self):
        s = ParseStats()
        s.record(True)
        r = repr(s)
        assert "ParseStats" in r
        assert "total=1" in r


# ─────────────────────────────────────────────────────────────────────────────
# Tests: stream_records (integration-style, in-memory files)
# ─────────────────────────────────────────────────────────────────────────────

def _make_gz_file(lines: list, tmp_path: Path) -> Path:
    """Write lines to a gzip file and return its path."""
    gz_path = tmp_path / "test.gz"
    with gzip.open(gz_path, "wt", encoding="latin-1") as f:
        for line in lines:
            f.write(line + "\n")
    return gz_path


class TestStreamRecords:
    def test_yields_parsed_records(self, tmp_path):
        lines = [line for line, _ in VALID_LINES]
        gz = _make_gz_file(lines, tmp_path)
        stats = ParseStats()
        records = list(stream_records([gz], stats))
        assert len(records) == len(VALID_LINES)
        assert stats.parsed_ok == len(VALID_LINES)
        assert stats.malformed == 0

    def test_skips_malformed(self, tmp_path):
        lines = [VALID_LINES[0][0]] + MALFORMED_LINES + [VALID_LINES[1][0]]
        gz = _make_gz_file(lines, tmp_path)
        stats = ParseStats()
        records = list(stream_records([gz], stats))
        assert len(records) == 2
        assert stats.malformed == len(MALFORMED_LINES)

    def test_multiple_files(self, tmp_path):
        gz1 = _make_gz_file([VALID_LINES[0][0], VALID_LINES[1][0]], tmp_path / "a.gz" if False else tmp_path)
        # Use two separate tmp dirs via sub-paths
        f1 = tmp_path / "f1.gz"
        f2 = tmp_path / "f2.gz"
        with gzip.open(f1, "wt") as fh:
            fh.write(VALID_LINES[0][0] + "\n")
            fh.write(VALID_LINES[1][0] + "\n")
        with gzip.open(f2, "wt") as fh:
            fh.write(VALID_LINES[2][0] + "\n")
        stats = ParseStats()
        records = list(stream_records([f1, f2], stats))
        assert len(records) == 3

    def test_empty_file(self, tmp_path):
        gz = _make_gz_file([], tmp_path)
        records = list(stream_records([gz]))
        assert records == []

    def test_creates_stats_if_none(self, tmp_path):
        lines = [VALID_LINES[0][0]]
        gz = _make_gz_file(lines, tmp_path)
        # Should not raise even without passing stats
        records = list(stream_records([gz]))
        assert len(records) == 1
