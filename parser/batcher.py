"""
parser/batcher.py — Generic batch generator used by all four pipelines.

Wraps stream_records() and emits fixed-size lists of LogRecord objects,
also computing the batch statistics required by the project specification.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional

from parser.log_parser import LogRecord, ParseStats, stream_records

logger = logging.getLogger(__name__)


@dataclass
class BatchStats:
    """Statistics for a single batch."""
    batch_id:    int
    record_count: int


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
        return {
            "total_lines":     self.parse_stats.total_lines,
            "parsed_ok":       self.parse_stats.parsed_ok,
            "malformed":       self.parse_stats.malformed,
            "parse_rate":      round(self.parse_stats.parse_rate, 6),
            "total_batches":   self.total_batches,
            "non_empty_batches": self.non_empty_batches,
            "avg_batch_size":  round(self.avg_batch_size, 2),
        }


def generate_batches(
    log_files: List[Path],
    batch_size: int,
    stats: Optional[BatcherStats] = None,
) -> Generator[tuple, None, None]:
    """
    Yield (batch_id, List[LogRecord]) tuples.

    Batch IDs start at 1 and increment by 1.
    The final batch may be smaller than `batch_size`.
    Empty files produce no batches.

    Args:
        log_files:  Ordered list of log file paths.
        batch_size: Maximum records per batch (> 0).
        stats:      Optional BatcherStats; updated in-place.

    Yields:
        (batch_id: int, records: List[LogRecord])
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    if stats is None:
        stats = BatcherStats()

    batch:    List[LogRecord] = []
    batch_id: int = 0

    for record in stream_records(log_files, stats.parse_stats):
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
