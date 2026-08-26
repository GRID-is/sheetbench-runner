"""Domain entities for SheetBench Runner."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, StrictInt

from .solve_profile import SanitizedConfiguration


class TaskStatus(StrEnum):
    """Status of a task during execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"  # Transient failure (5xx, timeout) - will retry on resume
    EVALUATED = "evaluated"


class InstructionType(StrEnum):
    """Type of spreadsheet manipulation instruction."""

    CELL_LEVEL = "Cell-Level Manipulation"
    SHEET_LEVEL = "Sheet-Level Manipulation"


class Task(BaseModel):
    """
    A task from a SpreadsheetBench dataset.

    Two dataset layouts are supported:

    - v1 (``spreadsheetbench_verified_400``): ``spreadsheet_path`` is a directory
      and the input/golden filenames are derived from the task id. Carries
      ``instruction_type``, ``answer_sheet`` and ``data_position``.
    - v2 (``Debugging``, ``Financial_Model``, ``Template``): ``spreadsheet_path``
      points straight at the input workbook and ``golden_response_path`` names the
      golden file (possibly shared between tasks). The three v1-only fields are
      absent, and ``answer_position`` is always fully sheet-qualified.

    A present ``golden_response_path`` marks a task as v2.
    """

    model_config = ConfigDict(frozen=True, coerce_numbers_to_str=True)

    id: str
    instruction: str
    spreadsheet_path: str
    answer_position: str
    instruction_type: str | None = None
    answer_sheet: str | None = None
    data_position: str | None = None
    golden_response_path: str | None = None

    @property
    def input_relpath(self) -> str:
        """Path of the input workbook, relative to the dataset directory."""
        if self.golden_response_path is not None:
            # v2: spreadsheet_path is the input workbook itself
            return self.spreadsheet_path
        # v1 uses _init.xlsx naming under a per-task directory
        return f"{self.spreadsheet_path}/1_{self.id}_init.xlsx"

    @property
    def golden_relpath(self) -> str:
        """Path of the golden workbook, relative to the dataset directory."""
        if self.golden_response_path is not None:
            # v2 names the golden explicitly; several tasks may share one
            return self.golden_response_path
        # v1 golden files: spreadsheet/{task_id}/1_{task_id}_golden.xlsx
        return f"spreadsheet/{self.id}/1_{self.id}_golden.xlsx"


class SolveUsage(BaseModel):
    """Usage statistics from a solve response."""

    model_config = ConfigDict(frozen=True)

    turns: NonNegativeInt
    tool_calls: NonNegativeInt
    input_tokens: NonNegativeInt
    output_tokens: NonNegativeInt
    planning_turns: NonNegativeInt | None = None
    planning_tool_calls: NonNegativeInt | None = None


@dataclass(frozen=True)
class EvaluationResult:
    """Result of evaluating a task output against the golden file.

    regression_accuracy/modification_accuracy are set only by the v2 grader
    (None on the v1 path).
    """

    passed: bool
    message: str = ""
    regression_accuracy: float | None = None
    modification_accuracy: float | None = None


@dataclass
class TaskResult:
    """
    Result of running a task.

    Mutable during execution, becomes effectively frozen once recorded.
    """

    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    duration_seconds: float | None = None
    turns: int | None = None
    tool_calls: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    transcript_file: str | None = None
    output_file: str | None = None
    result: str | None = None  # "pass" | "fail"
    message: str = ""
    regression_accuracy: float | None = None
    modification_accuracy: float | None = None
    error: str | None = None  # For transient failures (not recorded to results.json)
    started_at: datetime | None = field(default=None, repr=False)

    def to_results_dict(self) -> dict[str, Any]:
        """Convert to the results.json format for SpreadsheetBench compatibility."""
        d: dict[str, Any] = {
            "task_id": self.task_id,
            "duration_seconds": self.duration_seconds,
            "turns": self.turns,
            "tool_calls": self.tool_calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
        }
        if self.transcript_file:
            d["transcript_file"] = self.transcript_file
        if self.output_file:
            d["output_file"] = self.output_file
        if self.result:
            d["result"] = self.result
        if self.message:
            d["message"] = self.message
        if self.regression_accuracy is not None:
            d["regression_accuracy"] = self.regression_accuracy
        if self.modification_accuracy is not None:
            d["modification_accuracy"] = self.modification_accuracy
        # Note: error field is intentionally NOT included - transient failures
        # should not be recorded so they get retried on resume
        return d


class RunMetadata(BaseModel):
    """Canonical run.json metadata."""

    model_config = ConfigDict(frozen=True)

    schema_version: Literal[2] = 2
    model: Annotated[str, Field(min_length=1)]
    git_hash: str
    solve_configuration: SanitizedConfiguration
    test_set: StrictInt | None = None
    notes: str = ""
    # Dataset directory this run was created against. Task ids overlap across
    # v2 categories, so regrading against the wrong dataset silently grades
    # against the wrong goldens; recording the binding lets tooling validate.
    dataset_path: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)
