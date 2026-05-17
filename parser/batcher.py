"""
parser/batcher.py — Generic batch generator used by all four pipelines.

Wraps stream_records() and emits fixed-size lists of LogRecord objects,
also computing the batch statistics required by the project specification.
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from parser.log_parser import (
    LogRecord,
    ParseStats,
    stream_records,
    parse_line,
    _open_log,
    stream_records_with_file_info,
)
from parser.malformed_handler import MalformedHandler
from parser.batch_strategy import BatchStrategy

logger = logging.getLogger(__name__)


@dataclass
class BatchStats:
    """Statistics for a single batch."""
    batch_id:    int
    record_count: int
    wall_time_seconds: float = 0.0

    @property
    def records_per_second(self) -> float:
        return self.record_count / self.wall_time_seconds if self.wall_time_seconds > 0 else 0.0


@dataclass
class BatcherStats:
    """Aggregate statistics for an entire batcher run."""
    parse_stats:   ParseStats  = field(default_factory=ParseStats)
    batch_stats:   List[BatchStats] = field(default_factory=list)

    @property
    def total_batches(self) -> int:
        return len(self.batch_stats)

    @property
    def non_empty_batches(self) -> int:
        return sum(1 for b in self.batch_stats if b.record_count > 0)

    @property
    def avg_batch_size(self) -> float:
        """
        avg_batch_size = total_parsed_records / number_of_non_empty_batches
        (as specified in the project rubric)
        """
        n = self.non_empty_batches
        return self.parse_stats.parsed_ok / n if n else 0.0

    def summary(self) -> dict:
        total_wall_time = sum(b.wall_time_seconds for b in self.batch_stats)
        peak_rps = max((b.records_per_second for b in self.batch_stats), default=0.0)

        valid_batches = [b for b in self.batch_stats if b.wall_time_seconds > 0]
        if valid_batches:
            slowest = min(valid_batches, key=lambda b: b.records_per_second)
            slowest_batch_id = slowest.batch_id
        else:
            slowest_batch_id = 0

        return {
            "total_lines":     self.parse_stats.total_lines,
            "parsed_ok":       self.parse_stats.parsed_ok,
            "malformed":       self.parse_stats.malformed,
            "parse_rate":      round(self.parse_stats.parse_rate, 6),
            "total_batches":   self.total_batches,
            "non_empty_batches": self.non_empty_batches,
            "avg_batch_size":  round(self.avg_batch_size, 2),
            "total_wall_time_seconds": round(total_wall_time, 4),
            "peak_records_per_second": round(peak_rps, 2),
            "slowest_batch_id": slowest_batch_id,
        }


def generate_batches(
    log_files: List[Path],
    batch_size: int,
    stats: Optional[BatcherStats] = None,
    malformed_handler: Optional[MalformedHandler] = None,
) -> Generator[Tuple[int, List[LogRecord]], None, None]:
    """
    Yield (batch_id, List[LogRecord]) tuples.

    Batch IDs start at 1 and increment by 1.
    The final batch may be smaller than `batch_size`.
    Empty files produce no batches.

    Args:
        log_files:  Ordered list of log file paths.
        batch_size: Maximum records per batch (> 0).
        stats:      Optional BatcherStats; updated in-place.
        malformed_handler: Optional MalformedHandler; updated in-place.

    Yields:
        (batch_id: int, records: List[LogRecord])
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    if stats is None:
        stats = BatcherStats()

    batch:    List[LogRecord] = []
    batch_id: int = 0

    for record in stream_records(log_files, stats.parse_stats, malformed_handler):
        batch.append(record)
        if len(batch) >= batch_size:
            batch_id += 1
            bs = BatchStats(batch_id=batch_id, record_count=len(batch))
            stats.batch_stats.append(bs)
            logger.debug("Emitting batch %d (%d records)", batch_id, len(batch))
            yield batch_id, batch
            batch = []

    # Flush the final partial batch
    if batch:
        batch_id += 1
        bs = BatchStats(batch_id=batch_id, record_count=len(batch))
        stats.batch_stats.append(bs)
        logger.debug("Emitting final batch %d (%d records)", batch_id, len(batch))
        yield batch_id, batch

    logger.info(
        "Batching complete: %d batches, %d records parsed, %d malformed",
        stats.total_batches,
        stats.parse_stats.parsed_ok,
        stats.parse_stats.malformed,
    )


def generate_batches_v2(
    log_files: List[Path],
    strategy: "BatchStrategy",
    stats: Optional[BatcherStats] = None,
    malformed_handler=None,
    progress=None,
    checkpoint=None,
    resume_from_batch: int = 0,
) -> Generator[Tuple[int, List[LogRecord]], None, None]:
    """
    Enhanced batch generator.
    - resume_from_batch: skip batches with batch_id <= this value.
    - Records per-batch timing in BatchStats.wall_time_seconds.
    - Calls progress hooks on each batch, file start/end, and completion.
    - Calls checkpoint.save() after each successfully yielded batch.
    """
    if stats is None:
        stats = BatcherStats()

    batch_id = 0
    current_batch = []
    current_file_idx = 0
    current_file_name = ""
    line_number = 0

    batch_start_time = time.perf_counter()

    for event in stream_records_with_file_info(log_files, stats.parse_stats, malformed_handler):
        ev_type, file_name, file_idx, record, line_or_records = event
        if ev_type == "file_start":
            current_file_idx = file_idx
            current_file_name = file_name
            if progress:
                progress.on_file_start(file_name, file_idx)

        elif ev_type == "file_end":
            if progress:
                progress.on_file_end(file_name, line_or_records)

        elif ev_type == "record":
            record._file_name = file_name
            line_number = line_or_records

            if current_batch and strategy.should_flush(current_batch, record):
                batch_id += 1
                elapsed = time.perf_counter() - batch_start_time

                bs = BatchStats(
                    batch_id=batch_id,
                    record_count=len(current_batch),
                    wall_time_seconds=elapsed
                )
                stats.batch_stats.append(bs)

                if batch_id > resume_from_batch:
                    if progress:
                        progress.on_batch_complete(
                            batch_id=batch_id,
                            batch_size=len(current_batch),
                            elapsed_seconds=elapsed,
                            total_parsed=stats.parse_stats.parsed_ok,
                            total_malformed=stats.parse_stats.malformed,
                            current_file=file_name
                        )
                    if checkpoint:
                        checkpoint.save(
                            batch_id=batch_id,
                            file_index=file_idx,
                            line_number=line_number,
                            total_parsed=stats.parse_stats.parsed_ok,
                            total_malformed=stats.parse_stats.malformed
                        )
                    yield batch_id, current_batch

                current_batch = []
                batch_start_time = time.perf_counter()

            current_batch.append(record)

    # Flush final partial batch
    if current_batch:
        batch_id += 1
        elapsed = time.perf_counter() - batch_start_time
        bs = BatchStats(
            batch_id=batch_id,
            record_count=len(current_batch),
            wall_time_seconds=elapsed
        )
        stats.batch_stats.append(bs)

        if batch_id > resume_from_batch:
            if progress:
                progress.on_batch_complete(
                    batch_id=batch_id,
                    batch_size=len(current_batch),
                    elapsed_seconds=elapsed,
                    total_parsed=stats.parse_stats.parsed_ok,
                    total_malformed=stats.parse_stats.malformed,
                    current_file=current_file_name
                )
            if checkpoint:
                checkpoint.save(
                    batch_id=batch_id,
                    file_index=current_file_idx,
                    line_number=line_number,
                    total_parsed=stats.parse_stats.parsed_ok,
                    total_malformed=stats.parse_stats.malformed
                )
            yield batch_id, current_batch

    if progress:
        progress.on_complete(stats)
