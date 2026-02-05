"""Prompt building for /solve endpoint with workbook_id instead of paths."""

from .entities import Task

# Template using workbook_id instead of filesystem paths
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
{extra_context}"""


def build_prompt(task: Task, workbook_id: str) -> str:
    """
    Build the prompt for /solve endpoint.

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

    # Build extra context from optional fields
    extra_lines: list[str] = []
    if task.answer_sheet:
        extra_lines.append(f"\n### answer_sheet\n{task.answer_sheet}")
    if task.data_position:
        extra_lines.append(f"\n### data_position\n{task.data_position}")

    extra_context = "\n".join(extra_lines)
    if extra_context:
        extra_context += "\n"

    return PROMPT_TEMPLATE.format(
        instruction=task.instruction,
        workbook_id=workbook_id,
        instruction_type=task.instruction_type,
        answer_position=task.answer_position,
        extra_context=extra_context,
    )
