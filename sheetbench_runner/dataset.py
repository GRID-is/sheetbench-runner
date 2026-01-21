"""Dataset loading and filtering for SpreadsheetBench."""

import json
from pathlib import Path

from .entities import Task


class Dataset:
    """Loads and filters SpreadsheetBench tasks."""

    def __init__(self, dataset_path: Path):
        """
        Initialize dataset from a SpreadsheetBench dataset directory.

        Args:
            dataset_path: Path to the dataset directory containing dataset.json
                          (e.g., data/spreadsheetbench_verified_400)
        """
        self.dataset_path = dataset_path
        self._tasks: list[Task] = []
        self._load()

    def _load(self) -> None:
        """Load tasks from dataset.json."""
        dataset_file = self.dataset_path / "dataset.json"
        if not dataset_file.exists():
            raise FileNotFoundError(f"Dataset file not found: {dataset_file}")

        with open(dataset_file) as f:
            raw_tasks = json.load(f)

        self._tasks = [Task.from_dict(t) for t in raw_tasks]

    @property
    def all_tasks(self) -> list[Task]:
        """Return all tasks in the dataset."""
        return list(self._tasks)

    def get_excluded_task_ids(self, exclude_file: Path | None = None) -> set[str]:
        """
        Load task IDs to exclude from a file.

        Default: looks for excluded_vba_tasks.txt in the dataset directory.
        """
        if exclude_file is None:
            exclude_file = self.dataset_path / "excluded_vba_tasks.txt"

        if not exclude_file.exists():
            return set()

        excluded: set[str] = set()
        with open(exclude_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    excluded.add(line)
        return excluded

    def filter_tasks(
        self,
        task_ids: set[str] | None = None,
        exclude_ids: set[str] | None = None,
    ) -> list[Task]:
        """
        Filter tasks based on criteria.

        Args:
            task_ids: If provided, only include these specific task IDs
            exclude_ids: Task IDs to exclude (typically VBA tasks)

        Returns:
            Filtered list of tasks
        """
        tasks = self._tasks

        # Apply exclusions first
        if exclude_ids:
            tasks = [t for t in tasks if t.id not in exclude_ids]

        # Then apply inclusion filter
        if task_ids:
            tasks = [t for t in tasks if t.id in task_ids]

        return tasks

    def get_input_path(self, task: Task) -> Path:
        """Get the path to the input spreadsheet for a task."""
        # verified_400 uses _init.xlsx naming: 1_{task_id}_init.xlsx
        filename = f"1_{task.id}_init.xlsx"
        return self.dataset_path / task.spreadsheet_path / filename

    def get_golden_path(self, task: Task) -> Path:
        """Get the path to the golden (expected) spreadsheet for a task."""
        # Golden files: spreadsheet/{task_id}/1_{task_id}_golden.xlsx
        filename = f"1_{task.id}_golden.xlsx"
        return self.dataset_path / "spreadsheet" / task.id / filename
