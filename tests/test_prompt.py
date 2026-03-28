"""Tests for prompt building."""

import pytest

from sheetbench_runner.entities import Task
from sheetbench_runner.prompt import build_prompt


def test_build_prompt_with_all_fields(sample_task: Task):
    """Test prompt building with all optional fields present."""
    # Arrange
    workbook_id = "wb-13-1"

    # Act
    prompt = build_prompt(sample_task, workbook_id)

    # Assert
    assert "You are a spreadsheet expert" in prompt
    assert sample_task.instruction in prompt
    assert workbook_id in prompt
    assert sample_task.instruction_type in prompt
    assert sample_task.answer_position in prompt

    # Check optional fields are included
    assert "### answer_sheet" in prompt
    assert sample_task.answer_sheet in prompt
    assert "### data_position" in prompt
    assert sample_task.data_position in prompt


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
    sections = ["### instruction", "### workbook_id", "### instruction_type",
                "### answer_position"]
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
