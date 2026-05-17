import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


class BatchCheckpoint:
    """
    Atomic checkpoint for resuming crashed runs.
    Checkpoint file: .checkpoints/<run_id>.json
    """

    def __init__(
        self,
        run_id: str,
        pipeline: str,
        checkpoint_dir: str = ".checkpoints",
    ):
        self.run_id = run_id
        self.pipeline = pipeline
        self.checkpoint_dir = Path(checkpoint_dir)
        self._created_at = None

    @property
    def path(self) -> Path:
        return self.checkpoint_dir / f"{self.run_id}.json"

    def save(
        self,
        batch_id: int,
        file_index: int,
        line_number: int,
        total_parsed: int,
        total_malformed: int,
    ) -> None:
        """
        Write checkpoint atomically: write to .tmp file then os.replace().
        Never leaves a corrupt file behind.
        Checkpoint JSON keys:
          run_id, pipeline, last_batch_id, last_file_index,
          last_file_line, total_parsed, total_malformed,
          created_at (ISO str, set once), updated_at (ISO str, updated each call)
        """
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        now_str = datetime.utcnow().isoformat() + "Z"
        if self._created_at is None:
            existing = self.load()
            if existing and "created_at" in existing:
                self._created_at = existing["created_at"]
            else:
                self._created_at = now_str

        data = {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "last_batch_id": batch_id,
            "last_file_index": file_index,
            "last_file_line": line_number,
            "total_parsed": total_parsed,
            "total_malformed": total_malformed,
            "created_at": self._created_at,
            "updated_at": now_str,
        }

        tmp_path = self.path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        os.replace(str(tmp_path), str(self.path))

    def load(self) -> Optional[dict]:
        """Return checkpoint dict or None if file does not exist."""
        p = self.path
        if not p.exists():
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "created_at" in data:
                    self._created_at = data["created_at"]
                return data
        except Exception:
            return None

    def delete(self) -> None:
        """Remove checkpoint file on successful run completion."""
        try:
            if self.path.exists():
                self.path.unlink()
        except Exception:
            pass
