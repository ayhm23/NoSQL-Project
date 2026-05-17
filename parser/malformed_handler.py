"""
parser/malformed_handler.py — Handler for malformed log lines.
Handles classification, in-memory buffering, and atomic JSONL quarantine logging.
"""

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any

logger = logging.getLogger(__name__)

@dataclass
class MalformedRecord:
    raw_line:    str
    line_number: int
    file_name:   str
    reason:      str    # one of the 8 categories
    batch_id:    int | None

class MalformedHandler:
    _MONTHS = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
        "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
        "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    _TS_RE = re.compile(
        r'^(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})$'
    )

    def __init__(
        self,
        max_in_memory: int = 1000,
        quarantine_path: str | None = None,   # JSONL file; None = no file
        emit_warnings:   bool = True,
    ):
        self.max_in_memory = max_in_memory
        self.quarantine_path = quarantine_path
        self.emit_warnings = emit_warnings

        self.records: List[MalformedRecord] = []
        self._total = 0
        self._flushed_count = 0
        self._sample_lines: List[str] = []

        # Initialize all 8 categories
        self._category_counts = {
            "empty_line": 0,
            "missing_brackets": 0,
            "missing_quotes": 0,
            "bad_timestamp": 0,
            "bad_status_code": 0,
            "bad_bytes_field": 0,
            "truncated": 0,
            "unknown": 0,
        }

    def _parse_timestamp(self, ts_str: str) -> Tuple[Optional[str], Optional[int]]:
        """Parse timestamp to check validity, matching log_parser.py."""
        m = self._TS_RE.match(ts_str)
        if not m:
            return None, None
        day, mon_str, year, hour = m.group(1), m.group(2), m.group(3), m.group(4)
        month = self._MONTHS.get(mon_str)
        if month is None:
            return None, None
        return f"{year}-{month:02d}-{int(day):02d}", int(hour)

    def _classify(self, raw_line: str) -> str:
        """Classify a raw log line into one of the 8 categories."""
        # 1. empty_line
        if not raw_line.strip():
            return "empty_line"

        # 2. missing_brackets
        if '[' not in raw_line or ']' not in raw_line:
            return "missing_brackets"

        # 3. missing_quotes
        if '"' not in raw_line:
            return "missing_quotes"

        # 4. bad_timestamp
        start_bracket = raw_line.find('[')
        end_bracket = raw_line.find(']')
        ts_str = raw_line[start_bracket + 1:end_bracket]
        log_date, _ = self._parse_timestamp(ts_str)
        if log_date is None:
            return "bad_timestamp"

        # 5. truncated — fewer than 7 whitespace-separated tokens
        if len(raw_line.split()) < 7:
            return "truncated"

        # Extract status and bytes
        last_quote = raw_line.rfind('"')
        suffix = raw_line[last_quote + 1:].strip()
        suffix_tokens = suffix.split()

        # 6. bad_status_code
        status_val = suffix_tokens[0] if suffix_tokens else None
        is_status_ok = False
        if status_val is not None:
            try:
                int(status_val)
                is_status_ok = True
            except ValueError:
                pass

        if not is_status_ok:
            return "bad_status_code"

        # 7. bad_bytes_field
        bytes_val = suffix_tokens[1] if len(suffix_tokens) >= 2 else None
        is_bytes_ok = False
        if bytes_val is not None:
            if bytes_val == "-":
                is_bytes_ok = True
            else:
                try:
                    int(bytes_val)
                    is_bytes_ok = True
                except ValueError:
                    pass

        if not is_bytes_ok:
            return "bad_bytes_field"

        # 8. unknown
        return "unknown"

    def record(
        self,
        raw_line: str,
        line_number: int,
        file_name: str,
        batch_id: int | None = None,
    ) -> MalformedRecord:
        """Classify the line, store (up to max_in_memory), return record."""
        self._total += 1
        reason = self._classify(raw_line)
        self._category_counts[reason] += 1

        if len(self._sample_lines) < 5:
            self._sample_lines.append(raw_line)

        record = MalformedRecord(
            raw_line=raw_line,
            line_number=line_number,
            file_name=file_name,
            reason=reason,
            batch_id=batch_id,
        )

        if len(self.records) < self.max_in_memory:
            self.records.append(record)

        return record

    def flush_to_file(self) -> int:
        """Write buffered records to quarantine_path (append, JSONL).
        Returns lines written. No-op if quarantine_path is None.
        On IOError: log warning, return 0. Never raise.
        flush_to_file() must be atomic (write to .tmp then rename)."""
        if self.quarantine_path is None:
            return 0

        to_write = self.records[self._flushed_count:]
        if not to_write:
            return 0

        try:
            path = Path(self.quarantine_path)
            existing_lines = []
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            existing_lines.append(line.strip())

            for rec in to_write:
                data = {
                    "file": rec.file_name,
                    "line_number": rec.line_number,
                    "batch_id": rec.batch_id,
                    "reason": rec.reason,
                    "raw_line": rec.raw_line,
                }
                existing_lines.append(json.dumps(data))

            # Atomic write using temporary file in same directory
            tmp_path = path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                for line in existing_lines:
                    f.write(line + "\n")

            tmp_path.replace(path)

            written = len(to_write)
            self._flushed_count += written
            return written
        except Exception as e:
            if self.emit_warnings:
                logger.warning("Failed to atomically flush malformed records to %s: %s", self.quarantine_path, e)
            return 0

    def to_db_summary(self, run_id: str, pipeline: str) -> dict:
        """
        Return a dict ready for save_malformed() in ResultLoader.
        Keys:
          run_id, pipeline, total_malformed, empty_line_count,
          missing_brackets_count, missing_quotes_count,
          bad_timestamp_count, bad_status_count, bad_bytes_count,
          truncated_count, unknown_count,
          sample_lines (JSON string of up to 5 example raw lines)
        """
        return {
            "run_id": run_id,
            "pipeline": pipeline,
            "total_malformed": self.total,
            "empty_line_count": self._category_counts["empty_line"],
            "missing_brackets_count": self._category_counts["missing_brackets"],
            "missing_quotes_count": self._category_counts["missing_quotes"],
            "bad_timestamp_count": self._category_counts["bad_timestamp"],
            "bad_status_count": self._category_counts["bad_status_code"],
            "bad_bytes_count": self._category_counts["bad_bytes_field"],
            "truncated_count": self._category_counts["truncated"],
            "unknown_count": self._category_counts["unknown"],
            "sample_lines": json.dumps(self._sample_lines),
        }

    def summary(self) -> dict:
        """Always safe, even on empty handler."""
        return {
            "total": self.total,
            "categories": self._category_counts.copy(),
        }

    def top_reasons(self, n: int = 5) -> List[Tuple[str, int]]:
        """Returns the top categories sorted by count DESC, then name ASC."""
        items = list(self._category_counts.items())
        items.sort(key=lambda x: (-x[1], x[0]))
        return items[:n]

    @property
    def total(self) -> int:
        return self._total
