"""Prompt building for SpreadsheetBench tasks.

The prompt is the user message the model sees, verbatim. The solve server
appends its own context (workbook summary, skill suggestions) after it and
parses nothing out of it; the workbook id travels in the request body, never
in the prompt.
"""

from .entities import Task

# v1 (spreadsheetbench_verified_400) mirrors the fields the reference inference
# script surfaces: instruction, instruction_type, answer_position. answer_sheet
# and data_position are intentionally withheld so the prompt is never more
# revealing than the reference benchmark.
PROMPT_TEMPLATE_V1 = """You are a spreadsheet expert.

You need to solve the given spreadsheet manipulation question, which contains the following \
types of information:
- instruction: The question about spreadsheet manipulation.
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

### instruction_type
{instruction_type}

### answer_position
{answer_position}"""

# v2 (Debugging, Financial_Model, Template) is the instructions block of the
# upstream SWE-agent config, SpreadsheetBench-2/SWE-agent/config/spreadsheet.yaml
# at commit c2bf59d, minus its "Input and Output" section, whose role the solve
# server's "workbook has been loaded" line plays. answer_position is withheld
# because upstream never shows it to the agent; the evaluator reads it from the
# dataset.
PROMPT_TEMPLATE_V2 = """## Task instructions
You need to process a spreadsheet file based on specific instructions.
**Instruction:** {instruction}"""


def build_prompt(task: Task) -> str:
    """Build the user message for a SpreadsheetBench task.

    A task without instruction_type is a v2 task.
    """
    if task.instruction_type is None:
        return PROMPT_TEMPLATE_V2.format(instruction=task.instruction)

    return PROMPT_TEMPLATE_V1.format(
        instruction=task.instruction,
        instruction_type=task.instruction_type,
        answer_position=task.answer_position,
    )
