"""Prompt building for SpreadsheetBench tasks."""

from .entities import Task

# Mirrors the fields surfaced by the built-in SpreadsheetBench inference scripts
# (instruction, instruction_type, answer_position); workbook_id replaces the
# file-path fields. answer_sheet and data_position are intentionally withheld so
# the prompt is never more revealing than the reference benchmark.
PROMPT_TEMPLATE = """You are a spreadsheet expert.

You need to solve the given spreadsheet manipulation question, which contains the following \
types of information:
- instruction: The question about spreadsheet manipulation.
- workbook_id: The ID of the workbook that has been uploaded.
- instruction_type: There are two values (Cell-Level Manipulation, Sheet-Level \
Manipulation) used to indicate whether the answer to this question applies only to \
specific cells or to the entire worksheet.
- answer_position: The position need to be modified or filled. For Cell-Level \
Manipulation questions, this field is filled with the cell position; for Sheet-Level \
Manipulation, it is the maximum range of cells you need to modify. You only need to \
modify or fill in values within the cell range specified by answer_position.

Below is the spreadsheet manipulation question you need to solve:
### instruction
{instruction}

### workbook_id
{workbook_id}

### instruction_type
{instruction_type}

### answer_position
{answer_position}
"""


def build_prompt(task: Task, workbook_id: str) -> str:
    """
    Build the prompt for a SpreadsheetBench task.

    Args:
        task: The task to build the prompt for
        workbook_id: ID of the uploaded workbook

    Returns:
        The formatted prompt string

    Raises:
        ValueError: If workbook_id is empty
    """
    if not workbook_id or not workbook_id.strip():
        raise ValueError("workbook_id cannot be empty")

    return PROMPT_TEMPLATE.format(
        instruction=task.instruction,
        workbook_id=workbook_id,
        instruction_type=task.instruction_type,
        answer_position=task.answer_position,
    )
