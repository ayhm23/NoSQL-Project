import sys
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parser.batcher import BatcherStats


class ProgressReporter:
    """
    Thread-safe progress printer. Always writes to stderr (not stdout).
    Reason: MapReduce streaming uses stdout for key-value pairs.
    """

    def __init__(
        self,
        total_files: int,
        report_every_n_batches: int = 10,
        enabled: bool = True,  # False in tests and non-TTY
    ):
        self.total_files = total_files
        self.report_every_n_batches = report_every_n_batches
        self.enabled = enabled
        self._lock = threading.Lock()
        self._current_file_index = 0

    def on_file_start(self, file_name: str, file_index: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._current_file_index = file_index
            sys.stderr.write(
                f"Starting file: {file_name} (File {file_index}/{self.total_files})\n"
            )
            sys.stderr.flush()

    def on_file_end(self, file_name: str, records_in_file: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            sys.stderr.write(
                f"Finished file: {file_name} ({records_in_file:,} records parsed)\n"
            )
            sys.stderr.flush()

    def on_batch_complete(
        self,
        batch_id: int,
        batch_size: int,
        elapsed_seconds: float,
        total_parsed: int,
        total_malformed: int,
        current_file: str = "",
    ) -> None:
        """Print if batch_id % report_every_n_batches == 0."""
        if not self.enabled:
            return
        if batch_id % self.report_every_n_batches != 0:
            return

        with self._lock:
            speed = batch_size / elapsed_seconds if elapsed_seconds > 0 else 0
            # Line format: [Batch 42 | 2,100,000 records | 1,842 rec/s | 2,001 malformed | File 2/2]
            sys.stderr.write(
                f"[Batch {batch_id} | {total_parsed:,} records | {int(speed):,} rec/s | "
                f"{total_malformed:,} malformed | File {self._current_file_index}/{self.total_files}]\n"
            )
            sys.stderr.flush()

    def on_complete(self, batcher_stats: "BatcherStats") -> None:
        """Always prints the final summary line, regardless of interval."""
        if not self.enabled:
            return
        with self._lock:
            total_parsed = batcher_stats.parse_stats.parsed_ok
            total_malformed = batcher_stats.parse_stats.malformed
            sys.stderr.write(
                f"[COMPLETE | Total Batches: {batcher_stats.total_batches} | "
                f"Total Records: {total_parsed:,} | Total Malformed: {total_malformed:,}]\n"
            )
            sys.stderr.flush()
