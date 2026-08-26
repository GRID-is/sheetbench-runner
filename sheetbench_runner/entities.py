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
    """A task from the SpreadsheetBench dataset."""

    model_config = ConfigDict(frozen=True, coerce_numbers_to_str=True)

    id: str
    instruction: str
    spreadsheet_path: str
    instruction_type: str
    answer_position: str
    answer_sheet: str | None = None
    data_position: str | None = None


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
    """Result of evaluating a task output against the golden file."""

    passed: bool
    message: str = ""


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
        # Note: error field is intentionally NOT included - transient failures
        # should not be recorded so they get retried on resume
        return d


class RunMetadata(BaseModel):
    """Canonical run.json metadata."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[2] = 2
    model: Annotated[str, Field(min_length=1)]
    git_hash: str
    solve_configuration: SanitizedConfiguration
    test_set: StrictInt | None = None
    notes: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
