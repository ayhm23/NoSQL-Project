"""
tests/test_batcher.py — Unit tests for parser/batcher.py

Tests cover:
  • Batch size enforcement (exact multiple, remainder, single-record batches)
  • Batch ID sequencing (1-indexed, monotonically increasing)
  • BatcherStats: total_batches, non_empty_batches, avg_batch_size
  • Empty input → no batches
  • batch_size=1 edge case
  • Malformed lines counted correctly in batcher stats
"""

import gzip
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parser.batcher import generate_batches, BatcherStats
from parser.log_parser import LogRecord

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

_GOOD_LINE = '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /foo HTTP/1.0" 200 100'
_BAD_LINE  = "this is not a log line"


def _make_gz(lines, path: Path) -> Path:
    with gzip.open(path, "wt", encoding="latin-1") as f:
        for line in lines:
            f.write(line + "\n")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateBatches:
    def test_exact_multiple(self, tmp_path):
        """10 records with batch_size=5 → 2 full batches."""
        gz = _make_gz([_GOOD_LINE] * 10, tmp_path / "t.gz")
        stats = BatcherStats()
        batches = list(generate_batches([gz], 5, stats))
        assert len(batches) == 2
        assert all(len(b) == 5 for _, b in batches)

    def test_remainder_batch(self, tmp_path):
        """7 records with batch_size=3 → batches of 3, 3, 1."""
        gz = _make_gz([_GOOD_LINE] * 7, tmp_path / "t.gz")
        stats = BatcherStats()
        batches = list(generate_batches([gz], 3, stats))
        assert len(batches) == 3
        counts = [len(b) for _, b in batches]
        assert counts == [3, 3, 1]

    def test_batch_ids_are_sequential(self, tmp_path):
        gz = _make_gz([_GOOD_LINE] * 5, tmp_path / "t.gz")
        stats = BatcherStats()
        ids = [bid for bid, _ in generate_batches([gz], 2, stats)]
        assert ids == [1, 2, 3]

    def test_batch_id_starts_at_1(self, tmp_path):
        gz = _make_gz([_GOOD_LINE], tmp_path / "t.gz")
        batches = list(generate_batches([gz], 10))
        assert batches[0][0] == 1

    def test_empty_file_no_batches(self, tmp_path):
        gz = _make_gz([], tmp_path / "t.gz")
        batches = list(generate_batches([gz], 100))
        assert batches == []

    def test_batch_size_1(self, tmp_path):
        gz = _make_gz([_GOOD_LINE] * 4, tmp_path / "t.gz")
        stats = BatcherStats()
        batches = list(generate_batches([gz], 1, stats))
        assert len(batches) == 4
        for _, records in batches:
            assert len(records) == 1

    def test_batch_size_larger_than_input(self, tmp_path):
        gz = _make_gz([_GOOD_LINE] * 3, tmp_path / "t.gz")
        batches = list(generate_batches([gz], 1000))
        assert len(batches) == 1
        assert len(batches[0][1]) == 3

    def test_batch_size_zero_raises(self, tmp_path):
        gz = _make_gz([_GOOD_LINE], tmp_path / "t.gz")
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            list(generate_batches([gz], 0))

    def test_yields_log_record_objects(self, tmp_path):
        gz = _make_gz([_GOOD_LINE], tmp_path / "t.gz")
        batches = list(generate_batches([gz], 10))
        _, records = batches[0]
        assert isinstance(records[0], LogRecord)

    def test_malformed_lines_not_in_batches(self, tmp_path):
        lines = [_GOOD_LINE, _BAD_LINE, _GOOD_LINE, _BAD_LINE, _GOOD_LINE]
        gz = _make_gz(lines, tmp_path / "t.gz")
        stats = BatcherStats()
        batches = list(generate_batches([gz], 100, stats))
        total_records = sum(len(b) for _, b in batches)
        assert total_records == 3
        assert stats.parse_stats.malformed == 2

    def test_multiple_files(self, tmp_path):
        f1 = _make_gz([_GOOD_LINE] * 3, tmp_path / "f1.gz")
        f2 = _make_gz([_GOOD_LINE] * 2, tmp_path / "f2.gz")
        batches = list(generate_batches([f1, f2], 10))
        total = sum(len(b) for _, b in batches)
        assert total == 5


class TestBatcherStats:
    def test_avg_batch_size_formula(self, tmp_path):
        """avg_batch_size = total_parsed / non_empty_batches."""
        gz = _make_gz([_GOOD_LINE] * 9, tmp_path / "t.gz")
        stats = BatcherStats()
        list(generate_batches([gz], 4, stats))  # batches: 4, 4, 1
        # 9 records / 3 non-empty batches = 3.0
        assert stats.non_empty_batches == 3
        assert abs(stats.avg_batch_size - 3.0) < 1e-6

    def test_summary_keys(self, tmp_path):
        gz = _make_gz([_GOOD_LINE] * 5, tmp_path / "t.gz")
        stats = BatcherStats()
        list(generate_batches([gz], 5, stats))
        s = stats.summary()
        for key in [
            "total_lines", "parsed_ok", "malformed", "parse_rate",
            "total_batches", "non_empty_batches", "avg_batch_size",
        ]:
            assert key in s, f"Missing key: {key}"

    def test_empty_input_stats(self, tmp_path):
        gz = _make_gz([], tmp_path / "t.gz")
        stats = BatcherStats()
        list(generate_batches([gz], 10, stats))
        assert stats.total_batches == 0
        assert stats.non_empty_batches == 0
        assert stats.avg_batch_size == 0.0
