"""Tests for the run summary."""

from sheetbench_runner.cli import cost_summary
from sheetbench_runner.runner import RunStats


def test_the_summary_prints_the_estimated_cost_and_how_many_tasks_it_covers() -> None:
    # Arrange
    stats = RunStats(completed=3, passed=2, failed=1, estimated_cost_usd=31.604, priced_tasks=2)

    # Act
    line = cost_summary(stats)

    # Assert
    assert line == (
        "Estimated cost: $31.60 (2 of 3 tasks priced; /solve calls only, "
        "solve-context probe excluded)"
    )


def test_the_summary_says_the_cost_is_unavailable_when_no_task_was_priced() -> None:
    # Act
    line = cost_summary(RunStats(completed=3, passed=3))

    # Assert
    assert line == "Estimated cost: unavailable (no task has a priced usage)"
