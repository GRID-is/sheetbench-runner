"""Tests for prompt building.

The prompt is the user message the model sees, verbatim; the solve server
appends its own context and parses nothing out of it. v2 mirrors the
instructions block of upstream's SWE-agent config
(SpreadsheetBench-2/SWE-agent/config/spreadsheet.yaml); v1 mirrors the v1
reference inference script.
"""

from sheetbench_runner.entities import Task
from sheetbench_runner.prompt import build_prompt

V2_TASK = Task(
    id="01_01",
    instruction="Please audit and fix this file thoroughly.",
    spreadsheet_path="spreadsheet/01_Debugging/input_files/Double Counting_input.xlsx",
    answer_position="'LBO'!A2:X218,'Financials'!A2:Q29",
    golden_response_path="spreadsheet/01_Debugging/01_golden.xlsx",
)


def test_v2_prompt_is_the_upstream_instructions_block():
    """Byte-for-byte the upstream block, with the instruction substituted."""
    # Act
    prompt = build_prompt(V2_TASK)

    # Assert
    assert prompt == (
        "## Task instructions\n"
        "You need to process a spreadsheet file based on specific instructions.\n"
        "**Instruction:** Please audit and fix this file thoroughly."
    )


def test_v2_prompt_withholds_grader_fields():
    """Upstream never shows the agent answer_position, and neither do we.

    The v1 preamble, field names and workbook id must not leak into v2 either.
    """
    # Act
    prompt = build_prompt(V2_TASK)

    # Assert
    assert V2_TASK.answer_position not in prompt
    assert "'LBO'" not in prompt
    for forbidden in ("answer_position", "instruction_type", "workbook_id", "spreadsheet expert"):
        assert forbidden not in prompt


def test_v2_fixture_uses_the_v2_template(sample_task_v2: Task):
    """A task without instruction_type is a v2 task."""
    # Act
    prompt = build_prompt(sample_task_v2)

    # Assert
    assert prompt.startswith("## Task instructions\n")
    assert sample_task_v2.instruction in prompt
    assert sample_task_v2.answer_position not in prompt


def test_v1_prompt_is_unchanged_from_reference():
    """Guard the exact v1 text.

    v1 mirrors the reference inference script (commit 3d9b34b). The only
    departures are the two workbook_id lines, removed because the solve
    server no longer strips them, and the trailing newline the server used
    to trim. Drift here would silently change what v1 runs measure.
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
    prompt = build_prompt(task)

    # Assert
    assert prompt == (
        "You are a spreadsheet expert.\n"
        "\n"
        "You need to solve the given spreadsheet manipulation question, which contains "
        "the following types of information:\n"
        "- instruction: The question about spreadsheet manipulation.\n"
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
        "### instruction_type\n"
        "Sheet-Level Manipulation\n"
        "\n"
        "### answer_position\n"
        "C1:C10"
    )


def test_v1_prompt_withholds_extra_hints():
    """answer_sheet, data_position and the workbook id never reach the prompt.

    The reference script surfaces neither of the first two, so surfacing them
    would make our prompt more revealing than the reference. Sentinel values
    guard against a coincidental substring match.
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

    # Act
    prompt = build_prompt(task)

    # Assert
    assert "You are a spreadsheet expert" in prompt
    assert task.instruction in prompt
    assert task.instruction_type in prompt
    assert task.answer_position in prompt

    assert "### answer_sheet" not in prompt
    assert "SHEET_SENTINEL" not in prompt
    assert "### data_position" not in prompt
    assert "DATAPOS_SENTINEL" not in prompt
    assert "workbook_id" not in prompt


def test_v1_prompt_section_order():
    """The three v1 sections appear in the reference order."""
    # Arrange
    task = Task(
        id="test",
        instruction="Test instruction with\nmultiple lines",
        spreadsheet_path="spreadsheet/test",
        instruction_type="Cell-Level Manipulation",
        answer_position="A1",
    )

    # Act
    prompt = build_prompt(task)

    # Assert
    sections = ["### instruction", "### instruction_type", "### answer_position"]
    positions = [prompt.find(s) for s in sections]
    assert -1 not in positions
    assert positions == sorted(positions)


def test_build_prompt_minimal(sample_task_minimal: Task):
    """A v1 task with only required fields renders the v1 template."""
    # Act
    prompt = build_prompt(sample_task_minimal)

    # Assert
    assert "You are a spreadsheet expert" in prompt
    assert sample_task_minimal.instruction in prompt
    assert sample_task_minimal.answer_position in prompt
    assert "### answer_sheet" not in prompt
    assert "### data_position" not in prompt
