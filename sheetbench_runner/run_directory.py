"""Run directory management for SpreadsheetBench results."""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .entities import RunMetadata, TaskResult, TaskStatus
from .solve_profile import SolveConfiguration


class RunMetadataError(ValueError):
    """run.json could not be decoded as a supported format."""


class LegacyRunMetadata(BaseModel):
    """A released-format run.json awaiting migration to the canonical schema."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    model: str
    git_hash: str = "unknown"
    test_set: int | None = None
    notes: str = ""
    dataset_path: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)

    def to_canonical(self, solve_configuration: SolveConfiguration) -> RunMetadata:
        """Build canonical metadata, keeping the historical run's own record."""
        return RunMetadata(**self.model_dump(), solve_configuration=solve_configuration)


class RunDirectory:
    """
    Manages a run directory for SpreadsheetBench results.

    Handles:
    - Creating run directories and run.json metadata
    - Loading/saving results.json
    - Tracking completed tasks for resumability
    """

    def __init__(self, path: Path):
        """
        Initialize a run directory.

        Args:
            path: Path to the run directory (created if it doesn't exist)
        """
        self.path = path
        self._completed_task_ids: set[str] = set()
        self._results: dict[str, dict[str, Any]] = {}

    @property
    def results_path(self) -> Path:
        return self.path / "results.json"

    @property
    def run_json_path(self) -> Path:
        return self.path / "run.json"

    def exists(self) -> bool:
        """Check if the run directory already exists."""
        return self.path.exists()

    def create(self, metadata: RunMetadata) -> None:
        """
        Create the run directory and initialize run.json.

        Args:
            metadata: Metadata to write to run.json
        """
        self.path.mkdir(parents=True, exist_ok=True)
        self._replace_run_json(metadata.model_dump(mode="json"))

        # Initialize results.json only if it doesn't exist
        if not self.results_path.exists():
            with open(self.results_path, "w") as f:
                json.dump([], f)

    def load(self) -> None:
        """
        Load existing run state from results.json.

        Call this when resuming a run to know which tasks are already completed.
        """
        if not self.results_path.exists():
            return

        with open(self.results_path) as f:
            results_list = json.load(f)

        self._results = {}
        self._completed_task_ids = set()

        for result in results_list:
            task_id = result["task_id"]
            self._results[task_id] = result
            # A task is "completed" if it has a result (pass/fail) and no error
            if result.get("result") and not result.get("error"):
                self._completed_task_ids.add(task_id)

    def is_completed(self, task_id: str) -> bool:
        """Check if a task has already been completed."""
        return task_id in self._completed_task_ids

    def get_completed_count(self) -> int:
        """Get the number of completed tasks."""
        return len(self._completed_task_ids)

    def get_result(self, task_id: str) -> dict[str, Any] | None:
        """Get the result dict for a task, or None if not found."""
        return self._results.get(task_id)

    def record_result(self, result: TaskResult) -> None:
        """
        Record a task result to results.json.

        Only records if the task was successfully evaluated (not transient failures).
        Writes to disk immediately for crash safety.
        """
        if result.status not in (TaskStatus.COMPLETED, TaskStatus.EVALUATED):
            # Don't record transient failures - they should be retried
            return

        result_dict = result.to_results_dict()
        self._results[result.task_id] = result_dict
        self._completed_task_ids.add(result.task_id)
        self._save_results()

    def _save_results(self) -> None:
        """Save results to disk, sorted by task_id for consistency."""
        results_list = sorted(self._results.values(), key=lambda x: x["task_id"])
        with open(self.results_path, "w") as f:
            json.dump(results_list, f, indent=2)

    def _replace_run_json(self, document: dict[str, Any]) -> None:
        """Write run.json through a same-directory temporary file and an atomic replace."""
        handle, temporary_name = tempfile.mkstemp(dir=self.path, prefix="run.json.", suffix=".tmp")
        try:
            with os.fdopen(handle, "w") as f:
                json.dump(document, f, indent=2)
            os.replace(temporary_name, self.run_json_path)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise

    def write_metadata(self, metadata: RunMetadata) -> None:
        """Serialize canonical metadata over run.json without a partial write."""
        self._replace_run_json(metadata.model_dump(mode="json"))

    def read_metadata(self) -> RunMetadata | LegacyRunMetadata | None:
        """Decode run.json as canonical or released metadata, or raise RunMetadataError."""
        if not self.run_json_path.exists():
            return None
        try:
            data: object = json.loads(self.run_json_path.read_text())
        except (OSError, json.JSONDecodeError) as e:
            raise RunMetadataError(f"Could not read {self.run_json_path}") from e
        if not isinstance(data, dict):
            raise RunMetadataError(f"{self.run_json_path} is not a JSON object")

        if "solve_configuration" in data:
            try:
                return RunMetadata.model_validate(data)
            except ValidationError as e:
                raise RunMetadataError(
                    f"{self.run_json_path} is not valid canonical metadata"
                ) from e
        try:
            return LegacyRunMetadata.model_validate(data)
        except ValidationError as e:
            raise RunMetadataError(f"{self.run_json_path} is not valid released metadata") from e

    def migrate_released_metadata(
        self, legacy: LegacyRunMetadata, solve_configuration: SolveConfiguration
    ) -> RunMetadata:
        """Rewrite a released-format run.json as canonical metadata."""
        metadata = legacy.to_canonical(solve_configuration)
        self.write_metadata(metadata)
        return metadata
