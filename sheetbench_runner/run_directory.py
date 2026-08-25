"""Run directory management for SpreadsheetBench results."""

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .entities import SCHEMA_VERSION, RunMetadata, TaskResult, TaskStatus
from .solve_profile import SolveProfileError, validate_sanitized_configuration

# Released sheetbench-runner master wrote the solve server /status response under
# this run.json key. Legacy run.json files carry it, but its contents are never
# read: it is not a canonical schema and holds nothing the migration needs.
LEGACY_SOLVE_CONFIGURATION_KEY = "infuser_config"
_CANONICAL_METADATA_KEYS = {
    "schema_version",
    "model",
    "git_hash",
    "solve_configuration",
    "test_set",
    "notes",
    "created_at",
}


def _legacy_created_at(value: object) -> datetime:
    """Parse a legacy created_at, defaulting to now for anything unusable."""
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            parsed = None
        if parsed is not None and parsed.isoformat() == value:
            return parsed
    return datetime.now()


class RunMetadataError(ValueError):
    """run.json could not be decoded as a supported format."""


@dataclass(frozen=True)
class LegacyRunMetadata:
    """A released-format run.json awaiting migration to the canonical schema."""

    model: str
    git_hash: str
    test_set: int | None
    notes: str
    created_at: datetime

    def to_canonical(self, solve_configuration: dict[str, Any]) -> RunMetadata:
        """Build canonical metadata, keeping the historical run's own record."""
        return RunMetadata(
            model=self.model,
            git_hash=self.git_hash,
            solve_configuration=solve_configuration,
            test_set=self.test_set,
            notes=self.notes,
            created_at=self.created_at,
        )


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
        document = metadata.to_dict()
        self.path.mkdir(parents=True, exist_ok=True)
        self._replace_run_json(document)

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
        document = metadata.to_dict()
        self._replace_run_json(document)

    def read_metadata(self) -> RunMetadata | LegacyRunMetadata | None:
        """Decode run.json as canonical or released metadata, failing closed otherwise."""
        if not self.run_json_path.exists():
            return None
        try:
            data: object = json.loads(self.run_json_path.read_text())
        except (OSError, json.JSONDecodeError) as e:
            raise RunMetadataError(f"Could not read {self.run_json_path}") from e
        if not isinstance(data, dict):
            raise RunMetadataError(f"{self.run_json_path} is not a JSON object")

        canonical = "solve_configuration" in data
        if not canonical:
            # A run.json without canonical solve_configuration is legacy. The only
            # historical value the resume decision needs is a usable model;
            # everything else is copied when valid and defaulted otherwise.
            # LEGACY_SOLVE_CONFIGURATION_KEY, if present, is ignored unread.
            model = data.get("model")
            if not isinstance(model, str) or not model or model == "unknown":
                raise RunMetadataError(f"{self.run_json_path} is not valid released metadata")

            git_hash = data.get("git_hash")
            if not isinstance(git_hash, str) or not git_hash:
                git_hash = "unknown"

            test_set = data.get("test_set")
            if not (
                test_set is None or (isinstance(test_set, int) and not isinstance(test_set, bool))
            ):
                test_set = None

            notes = data.get("notes")
            if not isinstance(notes, str):
                notes = ""

            return LegacyRunMetadata(
                model=model,
                git_hash=git_hash,
                test_set=test_set,
                notes=notes,
                created_at=_legacy_created_at(data.get("created_at")),
            )

        if set(data) != _CANONICAL_METADATA_KEYS:
            raise RunMetadataError(f"{self.run_json_path} is not valid canonical metadata")
        if data.get("schema_version") != SCHEMA_VERSION:
            raise RunMetadataError(f"{self.run_json_path} schema_version must be {SCHEMA_VERSION}")
        model = data.get("model")
        git_hash = data.get("git_hash")
        solve_configuration = data["solve_configuration"]
        created_at = data.get("created_at")
        test_set = data.get("test_set")
        notes = data.get("notes", "")
        if (
            not isinstance(model, str)
            or not model
            or not isinstance(git_hash, str)
            or not isinstance(solve_configuration, dict)
            or not isinstance(created_at, str)
            or not (
                test_set is None or (isinstance(test_set, int) and not isinstance(test_set, bool))
            )
            or not isinstance(notes, str)
        ):
            raise RunMetadataError(f"{self.run_json_path} is not valid canonical metadata")
        try:
            parsed_created_at = datetime.fromisoformat(created_at)
        except ValueError as e:
            raise RunMetadataError(f"{self.run_json_path} has an invalid created_at") from e
        try:
            validated_configuration = validate_sanitized_configuration(solve_configuration)
        except SolveProfileError as e:
            raise RunMetadataError(
                f"{self.run_json_path} has an invalid solve_configuration"
            ) from e
        return RunMetadata(
            model=model,
            git_hash=git_hash,
            solve_configuration=validated_configuration,
            test_set=test_set,
            notes=notes,
            created_at=parsed_created_at,
        )

    def migrate_released_metadata(
        self, legacy: LegacyRunMetadata, solve_configuration: dict[str, Any]
    ) -> RunMetadata:
        """Rewrite a released-format run.json as canonical metadata."""
        metadata = legacy.to_canonical(solve_configuration)
        self.write_metadata(metadata)
        return metadata
