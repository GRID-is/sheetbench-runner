"""Tests for run directory management."""

import json
from pathlib import Path

from sheetbench_runner.entities import RunMetadata, TaskResult, TaskStatus
from sheetbench_runner.run_directory import RunDirectory


def test_create_new_run_directory(temp_dir: Path):
    """Test creating a new run directory."""
    # Arrange
    run_path = temp_dir / "new-run"
    run_dir = RunDirectory(run_path)
    metadata = RunMetadata(
        model="claude-sonnet-4-5",
        git_hash="abc123",
        infuser_config={"planning_enabled": False, "verification_enabled": True},
        test_set=1,
        notes="Test run",
    )

    # Act
    run_dir.create(metadata)

    # Assert
    assert run_path.exists()
    assert (run_path / "run.json").exists()
    assert (run_path / "results.json").exists()

    with open(run_path / "run.json") as f:
        run_data = json.load(f)
    # model and git_hash at root level for compatibility
    assert run_data["model"] == "claude-sonnet-4-5"
    assert run_data["git_hash"] == "abc123"
    # full infuser config preserved
    assert run_data["infuser_config"]["planning_enabled"] is False
    assert run_data["infuser_config"]["verification_enabled"] is True


def test_load_existing_results(temp_dir: Path):
    """Test loading existing results for resumability."""
    # Arrange
    run_path = temp_dir / "existing-run"
    run_path.mkdir()

    results = [
        {"task_id": "13-1", "result": "pass", "duration_seconds": 45.0},
        {"task_id": "17-35", "result": "fail", "duration_seconds": 30.0},
    ]
    with open(run_path / "results.json", "w") as f:
        json.dump(results, f)

    run_dir = RunDirectory(run_path)

    # Act
    run_dir.load()

    # Assert
    assert run_dir.is_completed("13-1")
    assert run_dir.is_completed("17-35")
    assert not run_dir.is_completed("99-99")
    assert run_dir.get_completed_count() == 2


def test_record_result(temp_dir: Path):
    """Test recording a task result."""
    # Arrange
    run_path = temp_dir / "record-test"
    run_path.mkdir()
    with open(run_path / "results.json", "w") as f:
        json.dump([], f)

    run_dir = RunDirectory(run_path)
    run_dir.load()

    result = TaskResult(
        task_id="13-1",
        status=TaskStatus.EVALUATED,
        duration_seconds=45.0,
        turns=5,
        tool_calls=10,
        input_tokens=1000,
        output_tokens=500,
        result="pass",
        output_file="13-1-output.xlsx",
        transcript_file="13-1-transcript.json",
    )

    # Act
    run_dir.record_result(result)

    # Assert
    assert run_dir.is_completed("13-1")

    with open(run_path / "results.json") as f:
        saved_results = json.load(f)
    assert len(saved_results) == 1
    assert saved_results[0]["task_id"] == "13-1"
    assert saved_results[0]["result"] == "pass"


def test_failed_task_not_recorded(temp_dir: Path):
    """Test that transient failures are not recorded."""
    # Arrange
    run_path = temp_dir / "failed-test"
    run_path.mkdir()
    with open(run_path / "results.json", "w") as f:
        json.dump([], f)

    run_dir = RunDirectory(run_path)
    run_dir.load()

    result = TaskResult(
        task_id="13-1",
        status=TaskStatus.FAILED,  # Transient failure
        error="Connection refused",
    )

    # Act
    run_dir.record_result(result)

    # Assert
    assert not run_dir.is_completed("13-1")

    with open(run_path / "results.json") as f:
        saved_results = json.load(f)
    assert len(saved_results) == 0


def test_results_sorted_by_task_id(temp_dir: Path):
    """Test that results are sorted by task ID."""
    # Arrange
    run_path = temp_dir / "sort-test"
    run_path.mkdir()
    with open(run_path / "results.json", "w") as f:
        json.dump([], f)

    run_dir = RunDirectory(run_path)
    run_dir.load()

    # Record in random order
    for task_id in ["99-1", "13-1", "50-5"]:
        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.EVALUATED,
            result="pass",
        )
        run_dir.record_result(result)

    # Assert
    with open(run_path / "results.json") as f:
        saved_results = json.load(f)
    task_ids = [r["task_id"] for r in saved_results]
    assert task_ids == sorted(task_ids)


def test_exists_check(temp_dir: Path):
    """Test checking if run directory exists."""
    # Arrange
    existing = temp_dir / "existing"
    existing.mkdir()

    run_dir_exists = RunDirectory(existing)
    run_dir_new = RunDirectory(temp_dir / "new")

    # Assert
    assert run_dir_exists.exists()
    assert not run_dir_new.exists()


def test_get_result_returns_result_dict(temp_dir: Path):
    """get_result() returns the result dict for a completed task."""
    # Arrange
    run_path = temp_dir / "get-result-test"
    run_path.mkdir()

    results = [
        {"task_id": "13-1", "result": "pass", "duration_seconds": 45.0},
        {"task_id": "17-35", "result": "fail", "duration_seconds": 30.0},
    ]
    with open(run_path / "results.json", "w") as f:
        json.dump(results, f)

    run_dir = RunDirectory(run_path)
    run_dir.load()

    # Act
    result = run_dir.get_result("13-1")

    # Assert
    assert result is not None
    assert result["task_id"] == "13-1"
    assert result["result"] == "pass"


def test_get_result_returns_none_for_missing(temp_dir: Path):
    """get_result() returns None for tasks not in results."""
    # Arrange
    run_path = temp_dir / "get-result-missing"
    run_path.mkdir()

    with open(run_path / "results.json", "w") as f:
        json.dump([], f)

    run_dir = RunDirectory(run_path)
    run_dir.load()

    # Act
    result = run_dir.get_result("99-99")

    # Assert
    assert result is None


def test_create_preserves_existing_results(temp_dir: Path):
    """create() must not overwrite existing results.json."""
    # Arrange - directory with results.json but no run.json
    run_path = temp_dir / "partial-run"
    run_path.mkdir()

    existing_results = [
        {"task_id": "13-1", "result": "pass", "duration_seconds": 45.0},
        {"task_id": "17-35", "result": "fail", "duration_seconds": 30.0},
    ]
    with open(run_path / "results.json", "w") as f:
        json.dump(existing_results, f)

    run_dir = RunDirectory(run_path)
    metadata = RunMetadata(model="test-model", git_hash="test123", infuser_config={})

    # Act - create run.json (results.json already exists)
    run_dir.create(metadata)

    # Assert - results.json must NOT be wiped
    with open(run_path / "results.json") as f:
        results = json.load(f)
    assert len(results) == 2
    assert results[0]["task_id"] == "13-1"
    assert results[1]["task_id"] == "17-35"
