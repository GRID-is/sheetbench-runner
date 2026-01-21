"""Tests for prompt building."""

from pathlib import Path

from sheetbench_runner.entities import Task
from sheetbench_runner.prompt import build_prompt


def test_build_prompt_with_all_fields(sample_task: Task):
    """Test prompt building with all optional fields present."""
    # Arrange
    input_path = Path("/data/spreadsheet/13-1/1_13-1_init.xlsx")
    output_path = Path("/output/13-1-output.xlsx")

    # Act
    prompt = build_prompt(sample_task, input_path, output_path)

    # Assert
    assert "You are a spreadsheet expert" in prompt
    assert sample_task.instruction in prompt
    assert str(input_path) in prompt
    assert sample_task.instruction_type in prompt
    assert sample_task.answer_position in prompt
    assert str(output_path) in prompt

    # Check optional fields are included
    assert "### answer_sheet" in prompt
    assert sample_task.answer_sheet in prompt
    assert "### data_position" in prompt
    assert sample_task.data_position in prompt


def test_build_prompt_minimal(sample_task_minimal: Task):
    """Test prompt building with only required fields."""
    # Arrange
    input_path = Path("/data/spreadsheet/99-1/1_99-1_init.xlsx")
    output_path = Path("/output/99-1-output.xlsx")

    # Act
    prompt = build_prompt(sample_task_minimal, input_path, output_path)

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
    input_path = Path("/input.xlsx")
    output_path = Path("/output.xlsx")

    # Act
    prompt = build_prompt(task, input_path, output_path)

    # Assert - check section headers are present in order
    sections = ["### instruction", "### spreadsheet_path", "### instruction_type",
                "### answer_position", "### output_path"]
    last_pos = -1
    for section in sections:
        pos = prompt.find(section)
        assert pos > last_pos, f"Section {section} not found in expected order"
        last_pos = pos
