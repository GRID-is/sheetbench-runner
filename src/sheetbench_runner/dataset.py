"""Dataset loading and filtering for SpreadsheetBench."""

import json
from pathlib import Path

from .entities import Task

# Test sets from SpreadsheetBench docs/TEST_SET_TRACKING.md
TEST_SET_1: frozenset[str] = frozenset({
    "13284", "14240", "15387", "178-22", "267-21", "31011", "32093", "32612",
    "325-44", "34033", "37456", "38985", "399-14", "39903", "438-18", "440-24",
    "44017", "44296", "44389", "44628", "45063", "45300", "45738", "45896",
    "45937", "463-17", "46646", "48365", "48745", "48982", "49036", "49801",
    "49945", "50324", "50486", "50682", "53449", "53647", "55049", "55427",
    "55965", "57262", "57693", "58723", "59160", "59595", "59794", "59884",
    "59902", "9448",
})

TEST_SET_2: frozenset[str] = frozenset({
    "13-1", "17-35", "22-47", "23-24", "24-23", "28-7", "41-47", "60-7",
    "267-18", "280-17", "304-35", "433-47", "493-18", "61-4", "66-24", "84-40",
    "146-49", "170-13", "142-12", "183-8", "188-39", "227-40", "302-1", "341-14",
    "379-36", "395-36", "402-43", "448-11", "560-12", "585-41", "599-9", "12307",
    "15380", "30930", "31202", "33722", "37900", "38074", "38462", "38537",
    "38703", "38823", "38969", "39046", "39432", "39515", "39667", "39931",
    "40478", "41410",
})


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
        test_set: int | None = None,
        exclude_ids: set[str] | None = None,
    ) -> list[Task]:
        """
        Filter tasks based on criteria.

        Args:
            task_ids: If provided, only include these specific task IDs
            test_set: If provided (1 or 2), use the predefined test set
            exclude_ids: Task IDs to exclude (typically VBA tasks)

        Returns:
            Filtered list of tasks
        """
        tasks = self._tasks

        # Apply exclusions first
        if exclude_ids:
            tasks = [t for t in tasks if t.id not in exclude_ids]

        # Then apply inclusion filter
        if test_set == 1:
            tasks = [t for t in tasks if t.id in TEST_SET_1]
        elif test_set == 2:
            tasks = [t for t in tasks if t.id in TEST_SET_2]
        elif task_ids:
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
