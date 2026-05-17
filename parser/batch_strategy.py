from abc import ABC, abstractmethod
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from parser.log_parser import LogRecord


class BatchStrategy(ABC):
    @abstractmethod
    def should_flush(self, current_batch: list, record: "LogRecord") -> bool:
        """Return True when current_batch should be emitted now."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the batching strategy."""


class FixedSizeBatchStrategy(BatchStrategy):
    """Flush when len(current_batch) >= batch_size. Spec-compliant default."""

    def __init__(self, batch_size: int):
        if batch_size <= 0:
            raise ValueError(f"batch_size must be > 0, got {batch_size}")
        self.batch_size = batch_size

    def should_flush(self, current_batch: list, record: "LogRecord") -> bool:
        return len(current_batch) >= self.batch_size

    @property
    def name(self) -> str:
        return "fixed-size"


class FileBatchStrategy(BatchStrategy):
    """
    Treats each input file as one batch.
    Flush when the file changes (i.e., all records from one .gz file form
    one batch). This implements the spec's alternative:
      Batch 1 = Jul95, Batch 2 = Aug95.
    batch_size is still tracked and reported as the file's record count.
    """

    def __init__(self):
        self._current_file = None

    def should_flush(self, current_batch: list, record: "LogRecord") -> bool:
        file_name = getattr(record, "_file_name", "")
        if self._current_file is None:
            self._current_file = file_name
            return False
        if file_name != self._current_file:
            self._current_file = file_name
            return True
        return False

    @property
    def name(self) -> str:
        return "file-per-batch"


class AdaptiveBatchStrategy(BatchStrategy):
    """
    Flush when estimated memory >= max_bytes OR records >= max_records.
    Memory estimate per record: len(resource_path) + len(host) + 64 bytes.
    """

    def __init__(self, max_bytes: int = 50 * 1024 * 1024, max_records: int = 200_000):
        self.max_bytes = max_bytes
        self.max_records = max_records

    def should_flush(self, current_batch: list, record: "LogRecord") -> bool:
        if len(current_batch) >= self.max_records:
            return True
        # Estimate total memory for current batch
        current_mem = sum(len(r.resource_path) + len(r.host) + 64 for r in current_batch)
        if current_mem >= self.max_bytes:
            return True
        return False

    @property
    def name(self) -> str:
        return "adaptive"
