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

    def filter_tasks(self, task_ids: set[str] | None = None) -> list[Task]:
        """
        Filter tasks to only include specific task IDs.

        Args:
            task_ids: If provided, only include these specific task IDs

        Returns:
            Filtered list of tasks (or all tasks if no filter provided)
        """
        if task_ids:
            return [t for t in self._tasks if t.id in task_ids]
        return list(self._tasks)

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
