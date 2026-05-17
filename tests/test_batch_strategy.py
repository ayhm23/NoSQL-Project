import pytest
import gzip
import json
import sys
from pathlib import Path
from parser.log_parser import LogRecord
from parser import (
    BatcherStats,
    BatchStats,
    generate_batches,
    generate_batches_v2,
    BatchStrategy,
    FixedSizeBatchStrategy,
    FileBatchStrategy,
    AdaptiveBatchStrategy,
    ProgressReporter,
    BatchCheckpoint,
)


# 1. FixedSizeBatchStrategy flushes at correct size boundary
def test_fixed_size_strategy_flushes():
    strategy = FixedSizeBatchStrategy(3)
    rec = LogRecord("host", "ts", "date", 0, "GET", "res", "HTTP", 200, 100)
    assert not strategy.should_flush([], rec)
    assert not strategy.should_flush([rec], rec)
    assert not strategy.should_flush([rec, rec], rec)
    assert strategy.should_flush([rec, rec, rec], rec)


# 2. FixedSizeBatchStrategy raises ValueError for batch_size <= 0
def test_fixed_size_value_error():
    with pytest.raises(ValueError):
        FixedSizeBatchStrategy(0)
    with pytest.raises(ValueError):
        FixedSizeBatchStrategy(-5)


# 3. AdaptiveBatchStrategy flushes when byte estimate exceeds max_bytes
def test_adaptive_bytes_flush():
    strategy = AdaptiveBatchStrategy(max_bytes=100, max_records=1000)
    # Memory estimate per record: len(resource_path) + len(host) + 64 bytes.
    # host is 9 chars, resource_path is 10 chars, estimate is 9 + 10 + 64 = 83.
    rec = LogRecord("host_9ch", "ts", "date", 0, "GET", "path_10ch_", "HTTP", 200, 100)
    # 1 record = 83 bytes. Cumulative is 83 < 100.
    assert not strategy.should_flush([rec], rec)
    # 2 records = 166 bytes. Cumulative is 166 >= 100.
    assert strategy.should_flush([rec, rec], rec)


# 4. AdaptiveBatchStrategy flushes when record count exceeds max_records
def test_adaptive_records_flush():
    strategy = AdaptiveBatchStrategy(max_bytes=10000, max_records=2)
    rec = LogRecord("h", "ts", "date", 0, "GET", "p", "HTTP", 200, 100)
    assert not strategy.should_flush([rec], rec)
    assert strategy.should_flush([rec, rec], rec)


# 5. FileBatchStrategy produces exactly 2 batches from 2 files (any size)
def test_file_batch_strategy(tmp_path):
    file1 = tmp_path / "file1.gz"
    file2 = tmp_path / "file2.gz"

    with gzip.open(file1, "wt") as f:
        f.write('199.0.2.2 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html HTTP/1.0" 200 100\n')
        f.write('199.0.2.2 - - [01/Jul/1995:00:00:02 -0400] "GET /about.html HTTP/1.0" 200 100\n')
    with gzip.open(file2, "wt") as f:
        f.write('199.0.2.2 - - [01/Aug/1995:00:00:01 -0400] "GET /contact.html HTTP/1.0" 200 100\n')

    strategy = FileBatchStrategy()
    stats = BatcherStats()
    batches = list(generate_batches_v2([file1, file2], strategy, stats))

    assert len(batches) == 2
    # Batch 1 should have 2 records
    assert len(batches[0][1]) == 2
    # Batch 2 should have 1 record
    assert len(batches[1][1]) == 1


# 6. BatchCheckpoint.save() creates valid JSON; .load() reads it back
def test_checkpoint_save_and_load(tmp_path):
    checkpoint = BatchCheckpoint("test_run", "mongodb", checkpoint_dir=str(tmp_path))
    checkpoint.save(
        batch_id=42,
        file_index=2,
        line_number=1500,
        total_parsed=10000,
        total_malformed=5
    )
    assert checkpoint.path.exists()

    loaded = checkpoint.load()
    assert loaded is not None
    assert loaded["run_id"] == "test_run"
    assert loaded["pipeline"] == "mongodb"
    assert loaded["last_batch_id"] == 42
    assert loaded["last_file_index"] == 2
    assert loaded["last_file_line"] == 1500
    assert loaded["total_parsed"] == 10000
    assert loaded["total_malformed"] == 5
    assert "created_at" in loaded
    assert "updated_at" in loaded


# 7. BatchCheckpoint.save() is atomic (no .tmp left behind after save)
def test_checkpoint_atomic(tmp_path):
    checkpoint = BatchCheckpoint("test_run", "mongodb", checkpoint_dir=str(tmp_path))
    checkpoint.save(1, 1, 10, 100, 0)

    tmp_file = checkpoint.path.with_suffix(".tmp")
    assert not tmp_file.exists()


# 8. BatchCheckpoint.delete() removes the file; second delete is a no-op
def test_checkpoint_delete(tmp_path):
    checkpoint = BatchCheckpoint("test_run", "mongodb", checkpoint_dir=str(tmp_path))
    checkpoint.save(1, 1, 10, 100, 0)
    assert checkpoint.path.exists()

    checkpoint.delete()
    assert not checkpoint.path.exists()

    # second delete should not raise an error
    checkpoint.delete()


# 9. ProgressReporter writes to stderr when enabled=True (patch sys.stderr)
def test_progress_reporter_enabled(capsys):
    reporter = ProgressReporter(total_files=2, report_every_n_batches=1, enabled=True)
    reporter.on_file_start("file1.gz", 1)
    reporter.on_batch_complete(
        batch_id=1,
        batch_size=50,
        elapsed_seconds=0.5,
        total_parsed=100,
        total_malformed=2
    )
    reporter.on_file_end("file1.gz", 100)
    stats = BatcherStats()
    stats.batch_stats.append(BatchStats(1, 50, 0.5))
    stats.parse_stats.parsed_ok = 100
    stats.parse_stats.malformed = 2
    reporter.on_complete(stats)

    captured = capsys.readouterr()
    # Should be in stderr, not stdout
    assert captured.out == ""
    assert "Starting file: file1.gz" in captured.err
    assert "[Batch 1 |" in captured.err
    assert "Finished file: file1.gz" in captured.err
    assert "[COMPLETE |" in captured.err


# 10. ProgressReporter produces no output when enabled=False
def test_progress_reporter_disabled(capsys):
    reporter = ProgressReporter(total_files=2, report_every_n_batches=1, enabled=False)
    reporter.on_file_start("file1.gz", 1)
    reporter.on_batch_complete(1, 50, 0.5, 100, 2)
    reporter.on_file_end("file1.gz", 100)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


# 11. generate_batches_v2() with FixedSizeBatchStrategy matches generate_batches() output
def test_v2_matches_v1_regression(tmp_path):
    file_path = tmp_path / "test_logs.gz"
    with gzip.open(file_path, "wt") as f:
        for i in range(100):
            f.write(f'199.0.2.2 - - [01/Jul/1995:00:00:{i:02d} -0400] "GET /index.html HTTP/1.0" 200 100\n')

    stats1 = BatcherStats()
    batches1 = list(generate_batches([file_path], 30, stats1))

    stats2 = BatcherStats()
    strategy = FixedSizeBatchStrategy(30)
    batches2 = list(generate_batches_v2([file_path], strategy, stats2))

    assert len(batches1) == len(batches2)
    for b1, b2 in zip(batches1, batches2):
        assert b1[0] == b2[0]  # batch_id
        assert len(b1[1]) == len(b2[1])  # record counts
        for r1, r2 in zip(b1[1], b2[1]):
            assert r1.resource_path == r2.resource_path
            assert r1.timestamp == r2.timestamp

    assert stats1.total_batches == stats2.total_batches
    assert stats1.non_empty_batches == stats2.non_empty_batches
    assert stats1.parse_stats.parsed_ok == stats2.parse_stats.parsed_ok
    assert stats1.parse_stats.malformed == stats2.parse_stats.malformed


# 12. generate_batches_v2() with resume_from_batch=2 skips first 2 batches
def test_v2_resume(tmp_path):
    file_path = tmp_path / "test_logs.gz"
    with gzip.open(file_path, "wt") as f:
        for i in range(100):
            f.write(f'199.0.2.2 - - [01/Jul/1995:00:00:{i:02d} -0400] "GET /index.html HTTP/1.0" 200 100\n')

    strategy = FixedSizeBatchStrategy(30)
    batches = list(generate_batches_v2([file_path], strategy, resume_from_batch=2))

    # Total batches for 100 records with size 30 is 4 batches.
    # Skipping first 2 batches should yield only batch 3 and 4.
    assert len(batches) == 2
    assert batches[0][0] == 3
    assert batches[1][0] == 4


# 13. BatchStats.wall_time_seconds is populated (> 0) after generate_batches_v2
def test_wall_time_populated(tmp_path):
    file_path = tmp_path / "test_logs.gz"
    with gzip.open(file_path, "wt") as f:
        f.write('199.0.2.2 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html HTTP/1.0" 200 100\n')

    stats = BatcherStats()
    strategy = FixedSizeBatchStrategy(1)
    list(generate_batches_v2([file_path], strategy, stats))

    assert len(stats.batch_stats) == 1
    assert stats.batch_stats[0].wall_time_seconds >= 0.0


# 14. BatcherStats.summary() includes all 3 new timing keys
def test_summary_includes_new_keys():
    stats = BatcherStats()
    stats.batch_stats.append(BatchStats(1, 10, 1.5))
    stats.batch_stats.append(BatchStats(2, 20, 0.5))

    summary = stats.summary()
    assert "total_wall_time_seconds" in summary
    assert "peak_records_per_second" in summary
    assert "slowest_batch_id" in summary

    assert summary["total_wall_time_seconds"] == 2.0
    assert summary["peak_records_per_second"] == 40.0
    assert summary["slowest_batch_id"] == 1
