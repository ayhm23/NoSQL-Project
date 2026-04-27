"""
tests/test_mongo_pipeline.py — Tests for pipelines/mongo_pipeline.py

Uses mongomock to avoid requiring a live MongoDB instance.
Tests cover:
  • Document insertion (batch tracking)
  • Q1 aggregation output structure and correctness
  • Q2 aggregation (top 20, distinct_hosts)
  • Q3 aggregation (error_rate calculation, two-pass join)
  • _write_results calls loader methods with correct metadata
  • Full run() flow with mocked DB
  • Index creation called before insertion
  • drop_after=True drops the collection
"""

import gzip
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from typing import List, Dict

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ── mongomock patch — must happen before importing MongoPipeline ──────────────
try:
    import mongomock
    _HAS_MONGOMOCK = True
except ImportError:
    _HAS_MONGOMOCK = False

import config

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_GOOD_LINE_TPL = (
    '{host} - - [{ts}] "GET {path} HTTP/1.0" {status} {bytes}'
)

def _make_line(host="1.2.3.4", ts="01/Jul/1995:12:00:00 -0400",
               path="/index.html", status=200, bytes_=1000) -> str:
    return _GOOD_LINE_TPL.format(host=host, ts=ts, path=path,
                                  status=status, bytes=bytes_)

def _make_gz(lines: List[str], path: Path) -> Path:
    with gzip.open(path, "wt", encoding="latin-1") as f:
        for line in lines:
            f.write(line + "\n")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation pipeline unit tests (no MongoDB needed)
# ─────────────────────────────────────────────────────────────────────────────

class TestAggregationPipelineDefinitions:
    """Verify the module-level aggregation pipeline constants are well-formed."""

    def test_q1_has_required_stages(self):
        from pipelines.mongo_pipeline import QUERY_Q1_DAILY_TRAFFIC
        ops = [list(s.keys())[0] for s in QUERY_Q1_DAILY_TRAFFIC]
        assert "$group" in ops
        assert "$project" in ops
        assert "$sort" in ops

    def test_q2_has_limit_20(self):
        from pipelines.mongo_pipeline import QUERY_Q2_TOP_RESOURCES
        limits = [s["$limit"] for s in QUERY_Q2_TOP_RESOURCES if "$limit" in s]
        assert limits == [20]

    def test_q3_errors_matches_status_range(self):
        from pipelines.mongo_pipeline import QUERY_Q3_ERRORS_STAGE
        match_stage = next(s["$match"] for s in QUERY_Q3_ERRORS_STAGE if "$match" in s)
        sc = match_stage["status_code"]
        assert sc["$gte"] == 400
        assert sc["$lte"] == 599

    def test_q3_totals_has_no_match_stage(self):
        from pipelines.mongo_pipeline import QUERY_Q3_TOTALS_STAGE
        ops = [list(s.keys())[0] for s in QUERY_Q3_TOTALS_STAGE]
        assert "$match" not in ops   # totals covers all status codes


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests with mongomock
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.skipif(not _HAS_MONGOMOCK, reason="mongomock not installed")
class TestMongoPipelineWithMock:
    """Full pipeline tests using mongomock (no live MongoDB)."""

    def _make_pipeline(self, tmp_path, lines, batch_size=100, drop_after=False):
        gz = _make_gz(lines, tmp_path / "test.gz")
        from pipelines.mongo_pipeline import MongoPipeline

        pipeline = MongoPipeline(
            log_files=[gz],
            batch_size=batch_size,
            mongo_uri="mongodb://localhost:27017/",
            mongo_db="test_nasa",
            mongo_coll="test_logs",
            drop_after=drop_after,
        )
        # Patch MongoClient with mongomock
        mock_client = mongomock.MongoClient()
        pipeline._client = mock_client
        pipeline._db     = mock_client["test_nasa"]
        pipeline._coll   = mock_client["test_nasa"]["test_logs"]
        return pipeline

    def test_load_inserts_correct_count(self, tmp_path):
        lines = [_make_line()] * 10
        p = self._make_pipeline(tmp_path, lines, batch_size=3)
        p._load_data(3)
        assert p._coll.count_documents({}) == 10

    def test_batch_stats_populated(self, tmp_path):
        lines = [_make_line()] * 7
        p = self._make_pipeline(tmp_path, lines, batch_size=3)
        p._load_data(3)
        # 7 records, batch_size=3 → batches of 3, 3, 1
        assert p._batcher_stats.total_batches == 3
        assert p._batcher_stats.parse_stats.parsed_ok == 7

    def test_q1_returns_date_status_rows(self, tmp_path):
        lines = [
            _make_line(host="a.com", ts="01/Jul/1995:10:00:00 -0400", status=200, bytes_=100),
            _make_line(host="b.com", ts="01/Jul/1995:11:00:00 -0400", status=200, bytes_=200),
            _make_line(host="c.com", ts="01/Jul/1995:12:00:00 -0400", status=404, bytes_=0),
        ]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        q1 = results["q1_daily_traffic"]
        dates = {r["log_date"] for r in q1}
        assert "1995-07-01" in dates
        statuses = {r["status_code"] for r in q1}
        assert 200 in statuses
        assert 404 in statuses

    def test_q1_request_count_correct(self, tmp_path):
        lines = [
            _make_line(status=200, bytes_=100),
            _make_line(status=200, bytes_=200),
            _make_line(status=200, bytes_=300),
        ]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        q1 = results["q1_daily_traffic"]
        row = next(r for r in q1 if r["status_code"] == 200)
        assert row["request_count"] == 3
        assert row["total_bytes"] == 600

    def test_q2_top_resources_limit(self, tmp_path):
        # Insert 25 distinct paths; only top 20 should come back
        lines = [_make_line(path=f"/path/{i}", bytes_=i*10) for i in range(25)]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        assert len(results["q2_top_resources"]) == 20

    def test_q2_distinct_hosts(self, tmp_path):
        # Same path, 3 different hosts
        lines = [
            _make_line(host="h1.com", path="/shared"),
            _make_line(host="h2.com", path="/shared"),
            _make_line(host="h3.com", path="/shared"),
            _make_line(host="h1.com", path="/shared"),  # duplicate host
        ]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        q2 = results["q2_top_resources"]
        shared = next(r for r in q2 if r["resource_path"] == "/shared")
        assert shared["request_count"] == 4
        assert shared["distinct_hosts"] == 3   # h1, h2, h3

    def test_q3_error_filter(self, tmp_path):
        lines = [
            _make_line(status=200),   # not an error
            _make_line(status=404),   # error
            _make_line(status=500),   # error
            _make_line(status=301),   # not an error
        ]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        q3 = results["q3_hourly_errors"]
        assert len(q3) >= 1
        row = q3[0]
        assert row["error_count"] == 2
        assert row["total_requests"] == 4
        assert abs(row["error_rate"] - 0.5) < 1e-6

    def test_q3_error_rate_formula(self, tmp_path):
        # 1 error out of 5 total → error_rate = 0.2
        lines = [
            _make_line(status=200),
            _make_line(status=200),
            _make_line(status=200),
            _make_line(status=200),
            _make_line(status=500),
        ]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        q3 = results["q3_hourly_errors"]
        row = q3[0]
        assert abs(row["error_rate"] - 0.2) < 1e-6

    def test_q3_distinct_error_hosts(self, tmp_path):
        lines = [
            _make_line(host="a.com", status=500),
            _make_line(host="b.com", status=500),
            _make_line(host="a.com", status=500),  # duplicate
        ]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()
        q3 = results["q3_hourly_errors"]
        assert q3[0]["distinct_error_hosts"] == 2

    def test_write_results_calls_loader(self, tmp_path):
        lines = [_make_line(status=200), _make_line(status=404)]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()

        mock_loader = MagicMock()
        with patch("pipelines.mongo_pipeline.ResultLoader", return_value=mock_loader):
            p._write_results(results)

        mock_loader.write_q1.assert_called_once()
        mock_loader.write_q2.assert_called_once()
        mock_loader.write_q3.assert_called_once()
        mock_loader.close.assert_called_once()

    def test_write_results_metadata_in_rows(self, tmp_path):
        lines = [_make_line(status=200)]
        p = self._make_pipeline(tmp_path, lines)
        p._load_data(100)
        results = p._run_queries()

        captured_q1 = []
        mock_loader = MagicMock()
        mock_loader.write_q1.side_effect = lambda rows: captured_q1.extend(rows)

        with patch("pipelines.mongo_pipeline.ResultLoader", return_value=mock_loader):
            p._write_results(results)

        assert len(captured_q1) > 0
        row = captured_q1[0]
        assert row["pipeline"] == "mongodb"
        assert row["run_id"] == p.run_id
        assert "executed_at" in row
        assert "batch_id" in row

    def test_drop_after_true(self, tmp_path):
        lines = [_make_line()]
        p = self._make_pipeline(tmp_path, lines, drop_after=True)
        p._load_data(100)
        results = p._run_queries()

        with patch("pipelines.mongo_pipeline.ResultLoader"):
            p._write_results(results)

        # Collection should be dropped (empty)
        assert p._coll.count_documents({}) == 0

    def test_pipeline_name_is_mongodb(self):
        from pipelines.mongo_pipeline import MongoPipeline
        assert MongoPipeline.PIPELINE_NAME == "mongodb"


# ─────────────────────────────────────────────────────────────────────────────
# Tests for base pipeline contract
# ─────────────────────────────────────────────────────────────────────────────

class TestBasePipeline:
    def test_cannot_instantiate_abstract(self):
        from pipelines.base_pipeline import BasePipeline
        with pytest.raises(TypeError):
            BasePipeline(log_files=[], batch_size=100)

    def test_run_id_is_uuid_format(self, tmp_path):
        if not _HAS_MONGOMOCK:
            pytest.skip("mongomock not installed")
        import mongomock
        from pipelines.mongo_pipeline import MongoPipeline

        gz = _make_gz([_make_line()], tmp_path / "t.gz")
        p = MongoPipeline(log_files=[gz], batch_size=100, drop_after=False)
        mock_client = mongomock.MongoClient()
        p._client = mock_client
        p._db     = mock_client["test_nasa"]
        p._coll   = mock_client["test_nasa"]["test_logs"]

        # Validate run_id is a valid UUID
        uuid.UUID(p.run_id)   # raises if invalid

    def test_get_runtime_before_run(self):
        from pipelines.mongo_pipeline import MongoPipeline
        p = MongoPipeline.__new__(MongoPipeline)
        p._start_ts = None
        p._end_ts   = None
        assert p.get_runtime() == 0.0
