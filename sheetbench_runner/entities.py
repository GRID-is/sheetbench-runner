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

    id: str
    instruction: str
    spreadsheet_path: str
    answer_position: str
    instruction_type: str | None = None
    answer_sheet: str | None = None
    data_position: str | None = None
    golden_response_path: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        """Create a Task from a dataset.json entry."""
        return cls(
            id=str(data["id"]),
            instruction=data["instruction"],
            spreadsheet_path=data["spreadsheet_path"],
            answer_position=data["answer_position"],
            instruction_type=data.get("instruction_type"),
            answer_sheet=data.get("answer_sheet"),
            data_position=data.get("data_position"),
            golden_response_path=data.get("golden_response_path"),
        )

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
    git_hash: str
    infuser_config: dict[str, Any]
    test_set: int | None = None
    notes: str = ""
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to run.json format."""
        return {
            "model": self.model,
            "git_hash": self.git_hash,
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
            git_hash=data.get("git_hash", "unknown"),
            infuser_config=data.get("infuser_config", {}),
            test_set=data.get("test_set"),
            notes=data.get("notes", ""),
            created_at=created_at,
        )
