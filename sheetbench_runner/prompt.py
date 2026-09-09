"""Prompt building for SpreadsheetBench tasks."""

from .entities import Task

# v1 (spreadsheetbench_verified_400) mirrors the fields the reference inference
# scripts surface (instruction, instruction_type, answer_position); workbook_id
# replaces the file-path fields. answer_sheet and data_position are intentionally
# withheld so the prompt is never more revealing than the reference benchmark.
PROMPT_TEMPLATE_V1 = """You are a spreadsheet expert.

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

# v2 (Debugging, Financial_Model, Template) posts upstream's "Important" block
# (SpreadsheetBench-2/SWE-agent/config/spreadsheet.yaml, instance_template) and
# the instruction. The solve server's system prompt already covers the persona
# and the tools, and answer_position is withheld: upstream never shows the graded
# range to the agent and the evaluator reads it from the dataset. The two ###
# sections are the ones the server's parser requires. The block goes before the
# instruction heading, as upstream orders it, and because the parser reads the
# instruction up to the next ### heading: anything after it would become part of
# the parsed instruction. The server strips workbook_id before the model sees it.
PROMPT_TEMPLATE_V2 = """## Important
- When completing spreadsheet tasks, strictly avoid altering any cells that already \
contain values unless explicitly instructed. Modify only the cells that are required \
for the task.
- You need to complete the instructions and ensure that the original formatting is \
preserved as much as possible.

### instruction
{instruction}

### workbook_id
{workbook_id}
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

    if task.instruction_type is None:
        return PROMPT_TEMPLATE_V2.format(
            instruction=task.instruction,
            workbook_id=workbook_id,
        )

    return PROMPT_TEMPLATE_V1.format(
        instruction=task.instruction,
        workbook_id=workbook_id,
        instruction_type=task.instruction_type,
        answer_position=task.answer_position,
    )
