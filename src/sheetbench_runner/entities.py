"""Domain entities for SheetBench Runner."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


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


@dataclass(frozen=True)
class Task:
    """A task from the SpreadsheetBench dataset."""

    id: str
    instruction: str
    spreadsheet_path: str
    instruction_type: str
    answer_position: str
    answer_sheet: str | None = None
    data_position: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        """Create a Task from a dataset.json entry."""
        return cls(
            id=str(data["id"]),
            instruction=data["instruction"],
            spreadsheet_path=data["spreadsheet_path"],
            instruction_type=data["instruction_type"],
            answer_position=data["answer_position"],
            answer_sheet=data.get("answer_sheet"),
            data_position=data.get("data_position"),
        )


@dataclass(frozen=True)
class InfuserUsage:
    """Usage statistics from an infuser API response."""

    turns: int
    tool_calls: int
    input_tokens: int
    output_tokens: int
    planning_turns: int | None = None
    planning_tool_calls: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InfuserUsage":
        return cls(
            turns=data["turns"],
            tool_calls=data["tool_calls"],
            input_tokens=data["input_tokens"],
            output_tokens=data["output_tokens"],
            planning_turns=data.get("planning_turns"),
            planning_tool_calls=data.get("planning_tool_calls"),
        )


@dataclass(frozen=True)
class InfuserResponse:
    """Response from the infuser API."""

    id: str
    model: str
    usage: InfuserUsage
    output_path: str | None = None
    transcript_path: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InfuserResponse":
        return cls(
            id=data["id"],
            model=data["model"],
            usage=InfuserUsage.from_dict(data["usage"]),
            output_path=data.get("output_path"),
            transcript_path=data.get("transcript_path"),
        )


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


@dataclass(frozen=True)
class RunMetadata:
    """Metadata about a test run, stored in run.json."""

    model: str
    infuser_config: dict[str, Any]
    test_set: int | None = None
    notes: str = ""
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to run.json format."""
        return {
            "model": self.model,
            "infuser_config": self.infuser_config,
            "test_set": self.test_set,
            "notes": self.notes,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunMetadata":
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        return cls(
            model=data.get("model", "unknown"),
            infuser_config=data.get("infuser_config", {}),
            test_set=data.get("test_set"),
            notes=data.get("notes", ""),
            created_at=created_at,
        )
