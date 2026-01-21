"""Pytest fixtures for SheetBench Runner tests."""

import json
import tempfile
from pathlib import Path

import pytest

from sheetbench_runner.entities import Task


@pytest.fixture
def sample_task() -> Task:
    """A sample task for testing."""
    return Task(
        id="13-1",
        instruction="Combine the data from columns A and B into column C",
        spreadsheet_path="spreadsheet/13-1",
        instruction_type="Sheet-Level Manipulation",
        answer_position="C1:C10",
        answer_sheet="Sheet1",
        data_position="A1:B10",
    )


@pytest.fixture
def sample_task_minimal() -> Task:
    """A minimal task without optional fields."""
    return Task(
        id="99-1",
        instruction="Calculate the sum",
        spreadsheet_path="spreadsheet/99-1",
        instruction_type="Cell-Level Manipulation",
        answer_position="D5",
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_dataset_dir(temp_dir: Path) -> Path:
    """Create a minimal dataset directory structure."""
    dataset_dir = temp_dir / "dataset"
    dataset_dir.mkdir()

    # Create dataset.json
    dataset = [
        {
            "id": "13-1",
            "instruction": "Combine data",
            "spreadsheet_path": "spreadsheet/13-1",
            "instruction_type": "Sheet-Level Manipulation",
            "answer_position": "C1:C10",
            "answer_sheet": "Sheet1",
        },
        {
            "id": "17-35",
            "instruction": "Calculate sum",
            "spreadsheet_path": "spreadsheet/17-35",
            "instruction_type": "Cell-Level Manipulation",
            "answer_position": "E5",
        },
        {
            "id": "vba-task",
            "instruction": "Run macro",
            "spreadsheet_path": "spreadsheet/vba-task",
            "instruction_type": "Cell-Level Manipulation",
            "answer_position": "A1",
        },
    ]
    with open(dataset_dir / "dataset.json", "w") as f:
        json.dump(dataset, f)

    return dataset_dir


@pytest.fixture
def sample_run_dir(temp_dir: Path) -> Path:
    """Create an empty run directory."""
    run_dir = temp_dir / "run"
    run_dir.mkdir()
    return run_dir
