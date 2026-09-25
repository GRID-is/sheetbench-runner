"""
Cell-level grading findings.

The grader records what it decided about every cell it rejected, so consumers can
read the grading rather than repeat it. The records come out of the evaluation walk
that produces the saved scores; nothing here compares cells.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt

from .config import NumericToleranceMode

# The grading implementation these findings came from. Raise it whenever a change to
# the evaluation walk can move a score, so a consumer can tell one grader from another.
GRADER_VERSION = 1

Classification = Literal["regression", "modification"]
# Which comparison rejected the cell. `formula_on_error` is the value-level fallback
# evaluator_v2 applies when either side shows an Excel error.
Criterion = Literal["value", "formula", "formula_on_error", "font_color"]


def as_text(value: Any) -> str | None:
    """
    A cell as the grader's own messages render it: None for an empty cell, the formula
    text for an array formula, otherwise the value as text.
    """
    if value is None:
        return None
    text = getattr(value, "text", None)
    if isinstance(text, str):
        return text
    return str(value)


class CellState(BaseModel):
    """One side of a rejected cell."""

    model_config = ConfigDict(frozen=True)

    value: str | None
    formula: str | None
    # Only when the task grades font colors, as ARGB.
    font_color: str | None = None


class Mismatch(BaseModel):
    """One cell the grader rejected."""

    model_config = ConfigDict(frozen=True)

    sheet: str
    cell: str
    classification: Classification
    criterion: Criterion
    reason: str
    expected: CellState
    actual: CellState


class GroupCounts(BaseModel):
    """The tally for one cell group."""

    model_config = ConfigDict(frozen=True)

    correct: NonNegativeInt
    total: NonNegativeInt
    wrong: NonNegativeInt
    accuracy: float


class GradingConfiguration(BaseModel):
    """What the grader was asked to check."""

    model_config = ConfigDict(frozen=True)

    grader: Literal["evaluator_v2"] = "evaluator_v2"
    grader_version: int = GRADER_VERSION
    numeric_tolerance_mode: NumericToleranceMode
    tolerance: float
    with_font_color: bool
    with_formula: bool


class GradingDetail(BaseModel):
    """
    What one evaluation walk decided, beyond the score. Carried on EvaluationResult so the
    runner can record it without grading a second time.
    """

    model_config = ConfigDict(frozen=True)

    grader_version: int = GRADER_VERSION
    numeric_tolerance_mode: NumericToleranceMode
    tolerance: float
    with_font_color: bool
    with_formula: bool
    regression: GroupCounts
    modification: GroupCounts
    mismatches: list[Mismatch]
    notes: list[str] = []


class TaskFindings(BaseModel):
    """The grading of one task output, as the grader recorded it."""

    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1] = 1
    task_id: str
    graded_at: datetime = Field(default_factory=datetime.now)
    grading: GradingConfiguration
    passed: bool
    regression: GroupCounts
    modification: GroupCounts
    mismatches: list[Mismatch]
    # Sheet-level observations that belong to no single cell, such as a sheet the output
    # does not have.
    notes: list[str] = []


def task_findings(task_id: str, detail: GradingDetail, passed: bool) -> TaskFindings:
    """Wrap one walk's grading detail as the artifact a run directory holds."""
    return TaskFindings(
        task_id=task_id,
        grading=GradingConfiguration(
            grader_version=detail.grader_version,
            numeric_tolerance_mode=detail.numeric_tolerance_mode,
            tolerance=detail.tolerance,
            with_font_color=detail.with_font_color,
            with_formula=detail.with_formula,
        ),
        passed=passed,
        regression=detail.regression,
        modification=detail.modification,
        mismatches=detail.mismatches,
        notes=detail.notes,
    )
