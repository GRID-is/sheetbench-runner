"""Tests for run directory management."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from sheetbench_runner.entities import RunMetadata, TaskResult, TaskStatus
from sheetbench_runner.run_directory import (
    LegacyRunMetadata,
    RunDirectory,
    RunMetadataError,
)

SOLVE_CONFIGURATION: dict[str, Any] = {
    "models": {"default": {"transport": "anthropic", "model": "claude-sonnet-5", "options": None}},
    "modelRoles": {"default": "default"},
    "ttlSeconds": 86400,
}
RELEASED_RUN_JSON: dict[str, Any] = {
    "model": "claude-sonnet-4-5",
    "git_hash": "released-sha",
    "infuser_config": {
        "default_model": "claude-sonnet-4-5",
        "version": "released-sha",
        "status": "healthy",
    },
    "test_set": 1,
    "notes": "released run",
    "created_at": "2026-01-02T03:04:05",
}


def test_create_new_run_directory(temp_dir: Path):
    """Test creating a new run directory."""
    # Arrange
    run_path = temp_dir / "new-run"
    run_dir = RunDirectory(run_path)
    metadata = RunMetadata(
        model="claude-sonnet-5",
        git_hash="abc123",
        solve_configuration=SOLVE_CONFIGURATION,
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
    assert run_data == {
        "schema_version": 2,
        "model": "claude-sonnet-5",
        "git_hash": "abc123",
        "solve_configuration": SOLVE_CONFIGURATION,
        "test_set": 1,
        "notes": "Test run",
        "created_at": metadata.created_at.isoformat(),
    }


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
    metadata = RunMetadata(
        model="test-model", git_hash="test123", solve_configuration=SOLVE_CONFIGURATION
    )

    # Act - create run.json (results.json already exists)
    run_dir.create(metadata)

    # Assert - results.json must NOT be wiped
    with open(run_path / "results.json") as f:
        results = json.load(f)
    assert len(results) == 2
    assert results[0]["task_id"] == "13-1"
    assert results[1]["task_id"] == "17-35"


@pytest.mark.parametrize(
    "unsafe_configuration",
    [
        {
            "models": {"default": {"transport": "anthropic", "model": "m", "apiKeyEnv": "K"}},
            "modelRoles": {"default": "default"},
        },
        {
            "models": {"default": {"transport": "anthropic", "model": "m", "apiKey": "secret"}},
            "modelRoles": {"default": "default"},
        },
        {**SOLVE_CONFIGURATION, "credentials": {"K": "secret"}},
    ],
    ids=["apiKeyEnv", "apiKey", "credentials"],
)
def test_metadata_cannot_hold_api_key_material(unsafe_configuration: dict[str, Any]) -> None:
    # Act / Assert
    with pytest.raises(ValidationError):
        RunMetadata(model="m", git_hash="h", solve_configuration=unsafe_configuration)


def test_read_metadata_returns_none_without_run_json(temp_dir: Path) -> None:
    # Arrange
    run_path = temp_dir / "empty-run"
    run_path.mkdir()

    # Act / Assert
    assert RunDirectory(run_path).read_metadata() is None


def test_read_metadata_decodes_canonical_document(temp_dir: Path) -> None:
    # Arrange
    run_path = temp_dir / "canonical-run"
    run_dir = RunDirectory(run_path)
    metadata = RunMetadata(
        model="claude-sonnet-5",
        git_hash="abc123",
        solve_configuration=SOLVE_CONFIGURATION,
        test_set=2,
        notes="canonical",
    )
    run_dir.create(metadata)

    # Act
    actual = run_dir.read_metadata()

    # Assert
    assert actual == metadata


def test_read_metadata_decodes_released_legacy_document(temp_dir: Path) -> None:
    # Arrange
    run_path = temp_dir / "released-run"
    run_path.mkdir()
    (run_path / "run.json").write_text(json.dumps(RELEASED_RUN_JSON))

    # Act
    actual = RunDirectory(run_path).read_metadata()

    # Assert
    assert actual == LegacyRunMetadata(
        model="claude-sonnet-4-5",
        git_hash="released-sha",
        test_set=1,
        notes="released run",
        created_at=datetime.fromisoformat("2026-01-02T03:04:05"),
    )


@pytest.mark.parametrize(
    "document",
    [
        {**RELEASED_RUN_JSON, "solve_configuration": SOLVE_CONFIGURATION},
        {"schema_version": 1, "model": "m", "git_hash": "h", "solve_configuration": {}},
        {"schema_version": 2, "git_hash": "h", "solve_configuration": SOLVE_CONFIGURATION},
        {"schema_version": 2, "model": "m", "git_hash": "h", "solve_configuration": []},
        {key: value for key, value in RELEASED_RUN_JSON.items() if key != "model"},
        {**RELEASED_RUN_JSON, "model": None},
        {**RELEASED_RUN_JSON, "model": ""},
        {**RELEASED_RUN_JSON, "model": "unknown"},
        {**RELEASED_RUN_JSON, "model": 7},
        [],
        "not-an-object",
    ],
    ids=[
        "both-keys",
        "wrong-schema-version",
        "missing-model-canonical",
        "configuration-not-an-object",
        "missing-model-legacy",
        "null-model-legacy",
        "empty-model-legacy",
        "unknown-model-legacy",
        "wrong-type-model-legacy",
        "not-a-mapping",
        "not-json-object",
    ],
)
def test_read_metadata_fails_closed(temp_dir: Path, document: object) -> None:
    # Arrange
    run_path = temp_dir / "broken-run"
    run_path.mkdir()
    (run_path / "run.json").write_text(json.dumps(document))

    # Act / Assert
    with pytest.raises(RunMetadataError):
        RunDirectory(run_path).read_metadata()


def test_read_metadata_decodes_legacy_document_without_infuser_config(temp_dir: Path) -> None:
    """Real pre-solve run.json files never had infuser_config."""
    # Arrange
    run_path = temp_dir / "no-infuser-config-run"
    run_path.mkdir()
    document = {
        "git_hash": "abc0001",
        "model": "claude-sonnet-4-5",
        "notes": "",
        "test_set": None,
    }
    (run_path / "run.json").write_text(json.dumps(document))

    # Act
    actual = RunDirectory(run_path).read_metadata()

    # Assert
    assert isinstance(actual, LegacyRunMetadata)
    assert actual.model == "claude-sonnet-4-5"
    assert actual.git_hash == "abc0001"
    assert actual.test_set is None
    assert actual.notes == ""


def test_read_metadata_decodes_legacy_document_with_only_model(temp_dir: Path) -> None:
    # Arrange
    run_path = temp_dir / "minimal-run"
    run_path.mkdir()
    (run_path / "run.json").write_text(json.dumps({"model": "claude-sonnet-4-5"}))

    # Act
    before = datetime.now()
    actual = RunDirectory(run_path).read_metadata()
    after = datetime.now()

    # Assert
    assert isinstance(actual, LegacyRunMetadata)
    assert actual.model == "claude-sonnet-4-5"
    assert actual.git_hash == "unknown"
    assert actual.test_set is None
    assert actual.notes == ""
    assert before <= actual.created_at <= after


@pytest.mark.parametrize(
    "overrides",
    [
        {"git_hash": None},
        {"git_hash": 7},
        {"test_set": True},
        {"test_set": "1"},
        {"notes": None},
        {"notes": 7},
        {"created_at": "not-a-timestamp"},
    ],
    ids=[
        "null-git-hash",
        "wrong-type-git-hash",
        "boolean-test-set",
        "string-test-set",
        "null-notes",
        "wrong-type-notes",
        "unparseable-created-at",
    ],
)
def test_read_metadata_defaults_invalid_optional_legacy_fields(
    temp_dir: Path, overrides: dict[str, object]
) -> None:
    # Arrange
    run_path = temp_dir / "invalid-optional-fields-run"
    run_path.mkdir()
    document = {**RELEASED_RUN_JSON, **overrides}
    (run_path / "run.json").write_text(json.dumps(document))

    # Act
    before = datetime.now()
    actual = RunDirectory(run_path).read_metadata()
    after = datetime.now()

    # Assert
    assert isinstance(actual, LegacyRunMetadata)
    assert actual.model == "claude-sonnet-4-5"
    if "git_hash" in overrides:
        assert actual.git_hash == "unknown"
    if "test_set" in overrides:
        assert actual.test_set is None
    if "notes" in overrides:
        assert actual.notes == ""
    if "created_at" in overrides:
        assert before <= actual.created_at <= after


@pytest.mark.parametrize(
    "created_at",
    ["2026-01-02", "2026-01-02 03:04:05", "2026-01-02T03:04:05Z"],
    ids=["date-only", "space-separated", "zulu"],
)
def test_read_metadata_keeps_any_parseable_legacy_timestamp(
    temp_dir: Path, created_at: str
) -> None:
    # Arrange
    run_path = temp_dir / "parseable-created-at-run"
    run_path.mkdir()
    (run_path / "run.json").write_text(json.dumps({**RELEASED_RUN_JSON, "created_at": created_at}))

    # Act
    actual = RunDirectory(run_path).read_metadata()

    # Assert
    assert isinstance(actual, LegacyRunMetadata)
    assert actual.created_at == datetime.fromisoformat(created_at)


def test_read_metadata_rejects_unsafe_canonical_configuration(temp_dir: Path) -> None:
    # Arrange
    run_path = temp_dir / "unsafe-canonical-run"
    run_path.mkdir()
    document = {
        "schema_version": 2,
        "model": "m",
        "git_hash": "h",
        "solve_configuration": {
            "models": {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "apiKeyEnv": "KEY",
                }
            },
            "modelRoles": {"default": "primary"},
            "ttlSeconds": 86400,
        },
        "test_set": None,
        "notes": "",
        "created_at": "2026-01-02T03:04:05",
    }
    (run_path / "run.json").write_text(json.dumps(document))

    # Act / Assert
    with pytest.raises(RunMetadataError, match="valid canonical metadata"):
        RunDirectory(run_path).read_metadata()


@pytest.mark.parametrize(
    "extra_field",
    [
        {"context_id": "capability-marker"},
        {"apiKey": "provider-key-marker"},
        {"unexpected": "value"},
    ],
    ids=["context-capability", "api-key", "unknown"],
)
def test_read_metadata_rejects_unknown_canonical_fields(
    temp_dir: Path, extra_field: dict[str, str]
) -> None:
    # Arrange
    run_path = temp_dir / "canonical-with-extra-field"
    run_path.mkdir()
    document = RunMetadata(
        model="m",
        git_hash="h",
        solve_configuration=SOLVE_CONFIGURATION,
        created_at=datetime.fromisoformat("2026-01-02T03:04:05"),
    ).model_dump(mode="json")
    document.update(extra_field)
    (run_path / "run.json").write_text(json.dumps(document))

    # Act / Assert
    with pytest.raises(RunMetadataError, match="valid canonical metadata"):
        RunDirectory(run_path).read_metadata()


def test_migrating_released_metadata_preserves_history(temp_dir: Path) -> None:
    # Arrange
    run_path = temp_dir / "migrate-run"
    run_path.mkdir()
    (run_path / "run.json").write_text(json.dumps(RELEASED_RUN_JSON))
    run_dir = RunDirectory(run_path)
    legacy = run_dir.read_metadata()
    assert isinstance(legacy, LegacyRunMetadata)

    # Act
    migrated = run_dir.migrate_released_metadata(legacy, SOLVE_CONFIGURATION)

    # Assert
    assert json.loads((run_path / "run.json").read_text()) == {
        "schema_version": 2,
        "model": "claude-sonnet-4-5",
        "git_hash": "released-sha",
        "solve_configuration": SOLVE_CONFIGURATION,
        "test_set": 1,
        "notes": "released run",
        "created_at": "2026-01-02T03:04:05",
    }
    assert run_dir.read_metadata() == migrated
    assert not list(run_path.glob("*.tmp"))


def test_failed_metadata_write_leaves_the_original_document_intact(
    temp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    run_path = temp_dir / "atomic-run"
    run_path.mkdir()
    original = json.dumps(RELEASED_RUN_JSON)
    (run_path / "run.json").write_text(original)
    run_dir = RunDirectory(run_path)
    metadata = RunMetadata(model="m", git_hash="h", solve_configuration=SOLVE_CONFIGURATION)

    def failing_dump(*args: object, **kwargs: object) -> None:
        raise OSError("no space left on device")

    monkeypatch.setattr("sheetbench_runner.run_directory.json.dump", failing_dump)

    # Act / Assert
    with pytest.raises(OSError):
        run_dir.write_metadata(metadata)
    assert (run_path / "run.json").read_text() == original
    assert not list(run_path.glob("*.tmp"))
