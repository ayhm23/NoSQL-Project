"""
parser/log_parser.py — Shared NASA HTTPD log line parser.

This module is the single source of truth for log parsing.
ALL four pipelines (Pig, MapReduce, MongoDB, Hive) must ultimately
produce logically equivalent results; this parser enforces that contract
for the Python-side pipelines.

NASA Log Format (Combined Log Format, slight variation):
  host - - [DD/Mon/YYYY:HH:MM:SS -TZOFF] "METHOD /path HTTP/x.x" status bytes

Examples:
  199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
  unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
  (malformed lines exist — they are counted, not silently dropped)
"""

import re
import gzip
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Regex — compiled once at import time for maximum throughput
# ─────────────────────────────────────────────────────────────────────────────

# Outer pattern captures the 5 top-level CLF fields
_CLF_RE = re.compile(
    r'^(\S+)'           # group 1 : host
    r' \S+ \S+'         # ident & authuser (always "- -")
    r' \[([^\]]+)\]'    # group 2 : timestamp string (inside brackets)
    r' "([^"]*)"'       # group 3 : request string (inside quotes)
    r' (\S+)'           # group 4 : status code
    r' (\S+)$'          # group 5 : bytes (may be "-")
)

# Split the request string into METHOD, PATH, PROTOCOL
_REQUEST_RE = re.compile(
    r'^(\S+)'           # group 1 : HTTP method
    r' (\S+)'           # group 2 : resource path
    r'(?: (\S+))?$'     # group 3 : protocol version (optional)
)

# Timestamp: "01/Jul/1995:00:00:01 -0400"
_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

_TS_RE = re.compile(
    r'^(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})$'
)


# ─────────────────────────────────────────────────────────────────────────────
# Parsed record dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class LogRecord:
    """One fully-parsed NASA HTTP log entry."""
    host:             str
    timestamp:        str        # raw timestamp string from log
    log_date:         str        # YYYY-MM-DD
    log_hour:         int        # 0–23
    http_method:      str
    resource_path:    str
    protocol_version: str        # e.g. "HTTP/1.0" or "" if absent
    status_code:      int
    bytes_transferred: int       # "-" → 0

    def to_dict(self) -> dict:
        return {
            "host":             self.host,
            "timestamp":        self.timestamp,
            "log_date":         self.log_date,
            "log_hour":         self.log_hour,
            "http_method":      self.http_method,
            "resource_path":    self.resource_path,
            "protocol_version": self.protocol_version,
            "status_code":      self.status_code,
            "bytes_transferred": self.bytes_transferred,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_timestamp(ts_str: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Parse "01/Jul/1995:00:00:01 -0400" into (log_date, log_hour).
    Returns (None, None) on failure.
    """
    m = _TS_RE.match(ts_str)
    if not m:
        return None, None
    day, mon_str, year, hour = m.group(1), m.group(2), m.group(3), m.group(4)
    month = _MONTHS.get(mon_str)
    if month is None:
        return None, None
    log_date = f"{year}-{month:02d}-{int(day):02d}"
    return log_date, int(hour)


def parse_line(raw: str) -> Tuple[Optional[LogRecord], bool]:
    """
    Parse one raw log line.

    Returns:
        (LogRecord, False)  — successful parse
        (None, True)        — malformed line
    """
    line = raw.strip()
    m = _CLF_RE.match(line)
    if not m:
        return None, True   # malformed

    host      = m.group(1)
    ts_str    = m.group(2)
    req_str   = m.group(3)
    status_s  = m.group(4)
    bytes_s   = m.group(5)

    # Parse timestamp
    log_date, log_hour = _parse_timestamp(ts_str)
    if log_date is None:
        return None, True

    # Parse request
    rm = _REQUEST_RE.match(req_str)
    if rm:
        http_method      = rm.group(1)
        resource_path    = rm.group(2)
        protocol_version = rm.group(3) or ""
    else:
        # Treat the entire string as the path when the request is malformed
        http_method      = "UNKNOWN"
        resource_path    = req_str
        protocol_version = ""

    # Status code
    try:
        status_code = int(status_s)
    except ValueError:
        return None, True

    # Bytes
    try:
        bytes_transferred = 0 if bytes_s == "-" else int(bytes_s)
    except ValueError:
        return None, True

    record = LogRecord(
        host=host,
        timestamp=ts_str,
        log_date=log_date,
        log_hour=log_hour,
        http_method=http_method,
        resource_path=resource_path,
        protocol_version=protocol_version,
        status_code=status_code,
        bytes_transferred=bytes_transferred,
    )
    return record, False


# ─────────────────────────────────────────────────────────────────────────────
# File reader — handles .gz transparently
# ─────────────────────────────────────────────────────────────────────────────

def _open_log(path: Path):
    """Open a log file, transparently decompressing .gz files."""
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="latin-1", errors="replace")
    return open(path, "r", encoding="latin-1", errors="replace")


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

class ParseStats:
    """Accumulates parsing statistics across a full pipeline run."""

    def __init__(self):
        self.total_lines:    int = 0
        self.parsed_ok:      int = 0
        self.malformed:      int = 0
        self.malformed_examples: List[str] = []  # up to 10 examples

    def record(self, ok: bool, line: str = ""):
        self.total_lines += 1
        if ok:
            self.parsed_ok += 1
        else:
            self.malformed += 1
            if len(self.malformed_examples) < 10:
                self.malformed_examples.append(line[:120])

    @property
    def parse_rate(self) -> float:
        return self.parsed_ok / self.total_lines if self.total_lines else 0.0

    def __repr__(self) -> str:
        return (
            f"ParseStats(total={self.total_lines:,}, "
            f"ok={self.parsed_ok:,}, "
            f"malformed={self.malformed:,}, "
            f"rate={self.parse_rate:.2%})"
        )


def stream_records(
    log_files: List[Path],
    stats: Optional[ParseStats] = None,
) -> Generator[LogRecord, None, None]:
    """
    Yield LogRecord objects from one or more NASA log files in order.
    Malformed lines are counted in `stats` and skipped.

    Args:
        log_files: List of Path objects (plain text or .gz).
        stats:     Optional ParseStats instance; updated in-place.
    """
    if stats is None:
        stats = ParseStats()

    for path in log_files:
        logger.info("Opening log file: %s", path)
        with _open_log(path) as fh:
            for raw in fh:
                record, malformed = parse_line(raw)
                stats.record(not malformed, raw)
                if record is not None:
                    yield record
