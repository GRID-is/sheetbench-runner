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
