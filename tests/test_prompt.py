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


class TestV2Prompt:
    """v2 tasks carry no instruction_type, so the prompt must omit it entirely."""

    def test_omits_instruction_type_section(self, sample_task_v2: Task):
        """A task without instruction_type gets no ### instruction_type section."""
        # Act
        prompt = build_prompt(sample_task_v2, "wb-01-01")

        # Assert
        assert "### instruction_type" not in prompt
        assert "None" not in prompt

    def test_omits_instruction_type_field_description(self, sample_task_v2: Task):
        """The field-list preamble must not describe a field the prompt omits."""
        # Act
        prompt = build_prompt(sample_task_v2, "wb-01-01")

        # Assert
        assert "- instruction_type:" not in prompt
        assert "Cell-Level Manipulation" not in prompt
        assert "Sheet-Level Manipulation" not in prompt

    def test_keeps_instruction_workbook_and_answer_position(self, sample_task_v2: Task):
        """Everything else the agent needs is still present, in order."""
        # Act
        prompt = build_prompt(sample_task_v2, "wb-01-01")

        # Assert
        assert sample_task_v2.instruction in prompt
        assert "wb-01-01" in prompt
        assert sample_task_v2.answer_position in prompt

        sections = ["### instruction", "### workbook_id", "### answer_position"]
        positions = [prompt.find(s) for s in sections]
        assert -1 not in positions
        assert positions == sorted(positions)

    def test_keeps_answer_position_scope_constraint(self, sample_task_v2: Task):
        """The 'only modify within answer_position' constraint is retained."""
        # Act
        prompt = build_prompt(sample_task_v2, "wb-01-01")

        # Assert
        assert (
            "You only need to modify or fill in values within the cell range "
            "specified by answer_position" in prompt
        )


def test_build_prompt_v1_output_is_unchanged():
    """Guard the exact v1 prompt text while the builder grows a v2 branch.

    The v1 wording deliberately mirrors the built-in SpreadsheetBench inference
    scripts (commit 3d9b34b), so any drift here would silently change what v1
    runs are measuring. Captured byte-for-byte from master.
    """
    # Arrange
    task = Task(
        id="13-1",
        instruction="INSTR",
        spreadsheet_path="spreadsheet/13-1",
        instruction_type="Sheet-Level Manipulation",
        answer_position="C1:C10",
        answer_sheet="S",
        data_position="D",
    )

    # Act
    prompt = build_prompt(task, "wb-1")

    # Assert
    assert prompt == (
        "You are a spreadsheet expert.\n"
        "\n"
        "You need to solve the given spreadsheet manipulation question, which contains "
        "the following types of information:\n"
        "- instruction: The question about spreadsheet manipulation.\n"
        "- workbook_id: The ID of the workbook that has been uploaded.\n"
        "- instruction_type: There are two values (Cell-Level Manipulation, Sheet-Level "
        "Manipulation) used to indicate whether the answer to this question applies only "
        "to specific cells or to the entire worksheet.\n"
        "- answer_position: The position need to be modified or filled. For Cell-Level "
        "Manipulation questions, this field is filled with the cell position; for "
        "Sheet-Level Manipulation, it is the maximum range of cells you need to modify. "
        "You only need to modify or fill in values within the cell range specified by "
        "answer_position.\n"
        "\n"
        "Below is the spreadsheet manipulation question you need to solve:\n"
        "### instruction\n"
        "INSTR\n"
        "\n"
        "### workbook_id\n"
        "wb-1\n"
        "\n"
        "### instruction_type\n"
        "Sheet-Level Manipulation\n"
        "\n"
        "### answer_position\n"
        "C1:C10\n"
    )


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
