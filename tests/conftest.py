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
def sample_task_v2() -> Task:
    """A sample v2 task: explicit file paths, no instruction_type."""
    return Task(
        id="01_01",
        instruction="Complete the bond accounting schedule",
        spreadsheet_path="spreadsheet/01_bond_accounting/01_01_input.xlsx",
        answer_position="'BondAccounting'!B2:N37",
        golden_response_path="spreadsheet/01_bond_accounting/01_01_golden.xlsx",
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
def sample_dataset_dir_v2(temp_dir: Path) -> Path:
    """Create a minimal v2 dataset directory structure.

    v2 datasets (Debugging, Financial_Model, Template) point spreadsheet_path
    straight at the input workbook, carry an explicit golden_response_path, and
    omit instruction_type, answer_sheet and data_position.
    """
    dataset_dir = temp_dir / "dataset-v2"
    dataset_dir.mkdir()

    dataset = [
        {
            "id": "01_01",
            "instruction": "Complete the bond accounting schedule",
            "spreadsheet_path": "spreadsheet/01_bond_accounting/01_01_input.xlsx",
            "golden_response_path": "spreadsheet/01_bond_accounting/01_01_golden.xlsx",
            "answer_position": "'BondAccounting'!B2:N37",
        },
        {
            # Debugging-style: input under input_files/, golden shared by the folder
            "id": "02_01",
            "instruction": "Audit and fix this file",
            "spreadsheet_path": "spreadsheet/02_Debugging/input_files/Unit Mismatch_input.xlsx",
            "golden_response_path": "spreadsheet/02_Debugging/02_golden.xlsx",
            "answer_position": "'Model'!B2:F33,'Summary'!A1:C10",
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
