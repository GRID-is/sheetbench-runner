"""Run directory management for SpreadsheetBench results."""

import json
import shutil
from pathlib import Path
from typing import Any

from .entities import RunMetadata, TaskResult, TaskStatus


class RunDirectory:
    """
    Manages a run directory for SpreadsheetBench results.

    Handles:
    - Creating run directories and run.json metadata
    - Loading/saving results.json
    - Tracking completed tasks for resumability
    - Copying task artifacts (output xlsx, transcript json)
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

        # Write run.json
        with open(self.run_json_path, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)

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

    def copy_artifacts(
        self,
        task_id: str,
        output_path: Path | str | None,
        transcript_path: Path | str | None,
    ) -> tuple[str | None, str | None]:
        """
        Copy task artifacts to the run directory.

        Args:
            task_id: The task ID
            output_path: Path to the output xlsx file (from infuser response)
            transcript_path: Path to the transcript json file (from infuser response)

        Returns:
            Tuple of (output_filename, transcript_filename) or None if not copied
        """
        output_file = None
        transcript_file = None

        if output_path:
            output_path = Path(output_path)
            if output_path.exists():
                output_file = f"{task_id}-output.xlsx"
                shutil.copy2(output_path, self.path / output_file)

        if transcript_path:
            transcript_path = Path(transcript_path)
            if transcript_path.exists():
                transcript_file = f"{task_id}-transcript.json"
                shutil.copy2(transcript_path, self.path / transcript_file)

        return output_file, transcript_file

    def get_output_path(self, task_id: str) -> Path:
        """Get the expected output path for a task (where infuser should write)."""
        return self.path / f"{task_id}-output.xlsx"

    def load_metadata(self) -> RunMetadata | None:
        """Load run metadata from run.json if it exists."""
        if not self.run_json_path.exists():
            return None
        with open(self.run_json_path) as f:
            data = json.load(f)
        return RunMetadata.from_dict(data)
