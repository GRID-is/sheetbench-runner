"""Tests for prompt building."""

import pytest

from sheetbench_runner.entities import Task
from sheetbench_runner.prompt import build_prompt


def test_build_prompt_withholds_extra_hints():
    """answer_sheet and data_position must never leak into the prompt.

    The built-in SpreadsheetBench inference scripts surface neither field, so
    surfacing them would make our prompt more revealing than the reference.
    Sentinel values guard against a coincidental substring match.
    """
    # Arrange
    task = Task(
        id="13-1",
        instruction="Combine the data from columns A and B into column C",
        spreadsheet_path="spreadsheet/13-1",
        instruction_type="Sheet-Level Manipulation",
        answer_position="C1:C10",
        answer_sheet="SHEET_SENTINEL",
        data_position="DATAPOS_SENTINEL",
    )
    workbook_id = "wb-13-1"

    # Act
    prompt = build_prompt(task, workbook_id)

    # Assert
    assert "You are a spreadsheet expert" in prompt
    assert task.instruction in prompt
    assert workbook_id in prompt
    assert task.instruction_type in prompt
    assert task.answer_position in prompt

    assert "### answer_sheet" not in prompt
    assert "SHEET_SENTINEL" not in prompt
    assert "### data_position" not in prompt
    assert "DATAPOS_SENTINEL" not in prompt


def test_build_prompt_minimal(sample_task_minimal: Task):
    """Test prompt building with only required fields."""
    # Arrange
    workbook_id = "wb-99-1"

    # Act
    prompt = build_prompt(sample_task_minimal, workbook_id)

    # Assert
    assert "You are a spreadsheet expert" in prompt
    assert sample_task_minimal.instruction in prompt
    assert sample_task_minimal.answer_position in prompt

    # Optional fields should NOT be present
    assert "### answer_sheet" not in prompt
    assert "### data_position" not in prompt


def test_build_prompt_preserves_formatting():
    """Test that prompt maintains expected structure."""
    # Arrange
    task = Task(
        id="test",
        instruction="Test instruction with\nmultiple lines",
        spreadsheet_path="spreadsheet/test",
        instruction_type="Cell-Level Manipulation",
        answer_position="A1",
    )
    workbook_id = "wb-test"

    # Act
    prompt = build_prompt(task, workbook_id)

    # Assert - check section headers are present in order
    sections = ["### instruction", "### workbook_id", "### instruction_type", "### answer_position"]
    last_pos = -1
    for section in sections:
        pos = prompt.find(section)
        assert pos > last_pos, f"Section {section} not found in expected order"
        last_pos = pos


def test_build_prompt_empty_workbook_id_raises():
    """Test that empty workbook_id raises ValueError."""
    # Arrange
    task = Task(
        id="test",
        instruction="Test",
        spreadsheet_path="spreadsheet/test",
        instruction_type="Cell-Level Manipulation",
        answer_position="A1",
    )

    # Act & Assert
    with pytest.raises(ValueError, match="workbook_id cannot be empty"):
        build_prompt(task, "")

    with pytest.raises(ValueError, match="workbook_id cannot be empty"):
        build_prompt(task, "   ")
