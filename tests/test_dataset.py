"""Tests for dataset loading and filtering."""

from pathlib import Path

import pytest

from sheetbench_runner.dataset import TEST_SET_1, TEST_SET_2, Dataset


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


def test_filter_by_exclusion(sample_dataset_dir: Path):
    """Test filtering with exclusion list."""
    # Arrange
    dataset = Dataset(sample_dataset_dir)
    exclude_ids = dataset.get_excluded_task_ids()

    # Act
    tasks = dataset.filter_tasks(exclude_ids=exclude_ids)

    # Assert
    assert len(tasks) == 2
    assert "vba-task" not in {t.id for t in tasks}


def test_filter_by_test_set_1(sample_dataset_dir: Path):
    """Test filtering by test set 1."""
    # Arrange
    dataset = Dataset(sample_dataset_dir)

    # Act
    tasks = dataset.filter_tasks(test_set=1)

    # Assert - only tasks that are in TEST_SET_1 should be included
    for task in tasks:
        assert task.id in TEST_SET_1


def test_filter_by_test_set_2(sample_dataset_dir: Path):
    """Test filtering by test set 2."""
    # Arrange
    dataset = Dataset(sample_dataset_dir)

    # Act
    tasks = dataset.filter_tasks(test_set=2)

    # Assert
    for task in tasks:
        assert task.id in TEST_SET_2


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


def test_test_sets_are_disjoint():
    """Verify test set 1 and 2 don't overlap."""
    # Assert
    overlap = TEST_SET_1 & TEST_SET_2
    assert len(overlap) == 0, f"Test sets overlap: {overlap}"


def test_test_sets_have_expected_size():
    """Verify test sets have 50 tasks each."""
    # Assert
    assert len(TEST_SET_1) == 50
    assert len(TEST_SET_2) == 50
