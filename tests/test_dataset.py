"""Tests for dataset loading and filtering."""

from pathlib import Path

import pytest

from sheetbench_runner.dataset import Dataset


def test_load_dataset(sample_dataset_dir: Path):
    """Test loading tasks from dataset.json."""
    # Act
    dataset = Dataset(sample_dataset_dir)

    # Assert
    tasks = dataset.all_tasks
    assert len(tasks) == 3
    assert tasks[0].id == "13-1"
    assert tasks[0].instruction == "Combine data"
    assert tasks[0].answer_sheet == "Sheet1"


def test_load_dataset_missing_file(temp_dir: Path):
    """Test error when dataset.json is missing."""
    # Act & Assert
    with pytest.raises(FileNotFoundError):
        Dataset(temp_dir)


def test_filter_by_task_ids(sample_dataset_dir: Path):
    """Test filtering by specific task IDs."""
    # Arrange
    dataset = Dataset(sample_dataset_dir)

    # Act
    tasks = dataset.filter_tasks(task_ids={"13-1", "17-35"})

    # Assert
    assert len(tasks) == 2
    assert {t.id for t in tasks} == {"13-1", "17-35"}


def test_get_input_path(sample_dataset_dir: Path):
    """Test getting input file path for a task."""
    # Arrange
    dataset = Dataset(sample_dataset_dir)
    task = dataset.all_tasks[0]

    # Act
    input_path = dataset.get_input_path(task)

    # Assert
    assert input_path == sample_dataset_dir / "spreadsheet/13-1/1_13-1_init.xlsx"


def test_get_golden_path(sample_dataset_dir: Path):
    """Test getting golden file path for a task."""
    # Arrange
    dataset = Dataset(sample_dataset_dir)
    task = dataset.all_tasks[0]

    # Act
    golden_path = dataset.get_golden_path(task)

    # Assert
    assert golden_path == sample_dataset_dir / "spreadsheet/13-1/1_13-1_golden.xlsx"


class TestV2Dataset:
    """v2 datasets give explicit input/golden paths and omit instruction_type."""

    def test_load_dataset_without_instruction_type(self, sample_dataset_dir_v2: Path):
        """v2 entries have no instruction_type; loading must not fail."""
        # Act
        dataset = Dataset(sample_dataset_dir_v2)

        # Assert
        tasks = dataset.all_tasks
        assert len(tasks) == 2
        assert tasks[0].id == "01_01"
        assert tasks[0].instruction_type is None
        assert tasks[0].answer_sheet is None
        assert tasks[0].data_position is None

    def test_golden_response_path_is_loaded(self, sample_dataset_dir_v2: Path):
        """The explicit golden_response_path is carried onto the task."""
        # Act
        task = Dataset(sample_dataset_dir_v2).all_tasks[0]

        # Assert
        assert task.golden_response_path == "spreadsheet/01_bond_accounting/01_01_golden.xlsx"

    def test_get_input_path_uses_spreadsheet_path_directly(self, sample_dataset_dir_v2: Path):
        """v2 spreadsheet_path is the input workbook itself, not a directory."""
        # Arrange
        dataset = Dataset(sample_dataset_dir_v2)
        task = dataset.all_tasks[0]

        # Act
        input_path = dataset.get_input_path(task)

        # Assert
        expected = sample_dataset_dir_v2 / "spreadsheet/01_bond_accounting/01_01_input.xlsx"
        assert input_path == expected

    def test_get_golden_path_uses_golden_response_path(self, sample_dataset_dir_v2: Path):
        """v2 golden files are named by the dataset, not derived from the task id."""
        # Arrange
        dataset = Dataset(sample_dataset_dir_v2)
        task = dataset.all_tasks[1]

        # Act
        golden_path = dataset.get_golden_path(task)

        # Assert
        assert golden_path == sample_dataset_dir_v2 / "spreadsheet/02_Debugging/02_golden.xlsx"

    def test_shared_golden_layout(self, sample_dataset_dir_v2: Path):
        """Debugging inputs live under input_files/ with the golden beside that folder."""
        # Arrange
        dataset = Dataset(sample_dataset_dir_v2)
        task = dataset.all_tasks[1]

        # Act
        input_path = dataset.get_input_path(task)
        golden_path = dataset.get_golden_path(task)

        # Assert
        assert input_path.parent.name == "input_files"
        assert golden_path.parent.name == "02_Debugging"
