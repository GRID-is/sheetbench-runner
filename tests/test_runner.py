"""Tests for once-per-run solve context orchestration."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
import respx

from sheetbench_runner.entities import SolveUsage, Task
from sheetbench_runner.run_directory import RunDirectory, RunMetadataError
from sheetbench_runner.runner import RunStats, TaskRunner, run
from sheetbench_runner.solve_client import (
    NonRetryableSolveError,
    RetryableSolveError,
    SolveContextExpiredError,
    SolveResponse,
)
from sheetbench_runner.solve_profile import SolveProfileError

PROFILE: dict[str, Any] = {
    "models": {
        "primary": {
            "transport": "openai-compatible",
            "model": "opaque-model",
            "apiKeyEnv": "OPAQUE_ENV",
        }
    },
    "modelRoles": {"default": "primary"},
}
SANITIZED_MODEL: dict[str, object] = {
    "transport": "openai-compatible",
    "model": "opaque-model",
    "options": None,
}
SANITIZED_CONFIGURATION = {
    "models": {"primary": SANITIZED_MODEL},
    "modelRoles": {"default": "primary"},
    "ttlSeconds": 86400,
}
CONTEXT_TOKEN = "Y2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2M"


def write_profile(path: Path) -> Path:
    path.write_text(json.dumps(PROFILE))
    return path


def canonical_run_json(**overrides: Any) -> str:
    document: dict[str, Any] = {
        "schema_version": 2,
        "model": "opaque-model",
        "git_hash": "old",
        "solve_configuration": SANITIZED_CONFIGURATION,
        "test_set": None,
        "notes": "",
        "created_at": "2026-01-02T03:04:05",
    }
    document.update(overrides)
    return json.dumps(document)


def released_run_json(**overrides: Any) -> str:
    """A run.json exactly as released sheetbench-runner master wrote it."""
    document: dict[str, Any] = {
        "model": "opaque-model",
        "git_hash": "released-sha",
        "infuser_config": {
            "default_model": "opaque-model",
            "version": "released-sha",
            "status": "healthy",
        },
        "test_set": 1,
        "notes": "released run",
        "created_at": "2026-01-02T03:04:05",
    }
    document.update(overrides)
    return json.dumps(document)


def context_routes() -> tuple[respx.Route, respx.Route, respx.Route]:
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json={
                "id": CONTEXT_TOKEN,
                "expiresAt": (datetime.now(UTC) + timedelta(seconds=86400)).isoformat(),
                "configuration": SANITIZED_CONFIGURATION,
            },
        )
    )
    status_route = respx.get("http://localhost:3000/status").mock(
        return_value=httpx.Response(200, json={"version": "server-sha"})
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )
    return create_route, status_route, delete_route


@respx.mock
async def test_run_creates_and_deletes_exactly_once_and_stores_only_sanitized_metadata(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    create_route, status_route, delete_route = context_routes()
    run_dir = tmp_path / "run"

    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert status_route.call_count == 1
    assert "X-Solve-Context" not in status_route.calls[0].request.headers
    run_data = json.loads((run_dir / "run.json").read_text())
    assert run_data["schema_version"] == 2
    assert run_data["model"] == "opaque-model"
    assert run_data["solve_configuration"] == SANITIZED_CONFIGURATION
    artifacts = b"".join(path.read_bytes() for path in run_dir.rglob("*") if path.is_file())
    assert b"artifact-forbidden-secret" not in artifacts
    assert CONTEXT_TOKEN.encode() not in artifacts


@respx.mock
async def test_run_cleanup_is_best_effort_and_does_not_mask_original_error(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    run_all = AsyncMock(side_effect=ValueError("original run failure"))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    create_route, _, delete_route = context_routes()
    delete_route.mock(return_value=httpx.Response(500, text="cleanup failed"))

    with pytest.raises(ValueError, match="original run failure"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1


@respx.mock
async def test_run_propagates_cleanup_failure_after_successful_work(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    create_route, _, delete_route = context_routes()
    delete_route.mock(return_value=httpx.Response(500, text="cleanup failed"))

    with pytest.raises(RetryableSolveError, match="Delete solve context"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1


@respx.mock
async def test_run_accepts_expired_context_during_final_cleanup(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    expected = RunStats(total_tasks=1, errors=1)
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=expected))
    create_route, _, delete_route = context_routes()
    delete_route.mock(return_value=httpx.Response(401, json={"error": "Invalid solve context"}))

    # Act
    actual = await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=tmp_path / "run",
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    # Assert
    assert actual is expected
    assert create_route.call_count == 1
    assert delete_route.call_count == 1


@pytest.mark.parametrize(
    "configuration",
    [
        {**SANITIZED_CONFIGURATION, "credentials": {"OPAQUE_ENV": "artifact-secret"}},
        {
            **SANITIZED_CONFIGURATION,
            "models": {
                "primary": {
                    **SANITIZED_MODEL,
                    "apiKey": "artifact-secret",
                }
            },
        },
        {
            **SANITIZED_CONFIGURATION,
            "models": {
                "primary": {
                    **SANITIZED_MODEL,
                    "model": "prefix-artifact-secret-suffix",
                }
            },
        },
    ],
    ids=["credentials", "apiKey", "changed-model"],
)
@respx.mock
async def test_untrusted_context_configuration_is_never_written_to_run_artifacts(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    configuration: dict[str, object],
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "artifact-secret")
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json={
                "id": CONTEXT_TOKEN,
                "expiresAt": (datetime.now(UTC) + timedelta(seconds=86400)).isoformat(),
                "configuration": configuration,
            },
        )
    )
    run_dir = tmp_path / "run"

    with pytest.raises(NonRetryableSolveError, match="Invalid solve context response") as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert not (run_dir / "run.json").exists()
    assert "artifact-secret" not in str(exc_info.value)


@respx.mock
async def test_run_does_not_delete_when_context_creation_fails(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(400, text="rejected")
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    with pytest.raises(NonRetryableSolveError):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 0


@respx.mock
async def test_resume_creates_a_fresh_context_for_each_invocation(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    create_route, _, delete_route = context_routes()
    run_dir = tmp_path / "run"
    profile_path = write_profile(tmp_path / "profile.json")

    for _ in range(2):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=profile_path,
            tasks=[sample_task],
        )

    assert create_route.call_count == 2
    assert delete_route.call_count == 2


@respx.mock
async def test_matching_resume_creates_context_and_skips_completed_tasks(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    create_route, _, delete_route = context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(canonical_run_json())
    (run_dir / "results.json").write_text(
        json.dumps([{"task_id": sample_task.id, "result": "pass"}])
    )

    stats = await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    assert stats.skipped == 1
    assert stats.completed == 1
    assert create_route.call_count == 1
    assert delete_route.call_count == 1


@pytest.mark.parametrize(
    "historical_metadata",
    [
        {"model": "different-model"},
        {"solve_configuration": {**SANITIZED_CONFIGURATION, "ttlSeconds": 601}},
    ],
    ids=["model", "configuration"],
)
@respx.mock
async def test_mismatched_resume_aborts_before_any_server_request(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    historical_metadata: dict[str, object],
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = canonical_run_json(**historical_metadata)
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(SolveProfileError, match="metadata") as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    # Assert
    assert not respx.calls
    run_all.assert_not_awaited()
    assert (run_dir / "run.json").read_text() == original_run_json
    assert "artifact-forbidden-secret" not in str(exc_info.value)


@respx.mock
async def test_released_run_is_migrated_to_canonical_metadata_after_context_creation(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    create_route, status_route, delete_route = context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(released_run_json())
    (run_dir / "results.json").write_text("[]")

    # Act
    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    # Assert
    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert status_route.call_count == 0
    assert json.loads((run_dir / "run.json").read_text()) == {
        "schema_version": 2,
        "model": "opaque-model",
        "git_hash": "released-sha",
        "solve_configuration": SANITIZED_CONFIGURATION,
        "test_set": 1,
        "notes": "released run",
        "created_at": "2026-01-02T03:04:05",
    }


@pytest.mark.parametrize(
    "historical_metadata",
    [{"model": "some-other-model"}],
    ids=["different"],
)
@respx.mock
async def test_released_run_with_a_different_model_fails_before_context_creation(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    historical_metadata: dict[str, object],
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json(**historical_metadata)
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(SolveProfileError, match="model") as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    # Assert
    assert not respx.calls
    run_all.assert_not_awaited()
    assert (run_dir / "run.json").read_text() == original_run_json
    assert "artifact-forbidden-secret" not in str(exc_info.value)


@respx.mock
async def test_real_legacy_run_without_infuser_config_is_migrated(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real pre-solve run.json files never had infuser_config or created_at."""
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    create_route, status_route, delete_route = context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(
        json.dumps({"model": "opaque-model", "git_hash": "abc0001", "notes": "", "test_set": None})
    )
    (run_dir / "results.json").write_text("[]")

    # Act
    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    # Assert
    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert status_route.call_count == 0
    migrated = json.loads((run_dir / "run.json").read_text())
    assert migrated["schema_version"] == 2
    assert migrated["model"] == "opaque-model"
    assert migrated["git_hash"] == "abc0001"
    assert migrated["solve_configuration"] == SANITIZED_CONFIGURATION
    assert migrated["test_set"] is None
    assert migrated["notes"] == ""


@respx.mock
async def test_legacy_run_with_secret_shaped_infuser_config_is_migrated_without_leaking_it(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    create_route, status_route, delete_route = context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json(
        infuser_config={"apiKey": "artifact-forbidden-secret", "maxRetries": 3}
    )
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    # Assert
    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert status_route.call_count == 0
    migrated_text = (run_dir / "run.json").read_text()
    assert "artifact-forbidden-secret" not in migrated_text
    assert "infuser_config" not in migrated_text
    assert json.loads(migrated_text)["model"] == "opaque-model"


@pytest.mark.parametrize(
    "historical_metadata",
    [{"model": "unknown"}, {"model": None}, {"model": 7}],
    ids=["unknown", "absent", "unknown-type"],
)
@respx.mock
async def test_malformed_released_model_fails_before_context_creation(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    historical_metadata: dict[str, object],
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json(**historical_metadata)
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(RunMetadataError, match="released metadata"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    # Assert
    assert not respx.calls
    assert (run_dir / "run.json").read_text() == original_run_json


@respx.mock
async def test_released_run_with_unknown_model_cannot_adopt_unknown_profile(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json(model="unknown")
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")
    profile = {
        **PROFILE,
        "models": {
            "primary": {
                **PROFILE["models"]["primary"],
                "model": "unknown",
            }
        },
    }
    profile_path = tmp_path / "unknown-profile.json"
    profile_path.write_text(json.dumps(profile))

    # Act / Assert
    with pytest.raises(RunMetadataError, match="released metadata"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=profile_path,
            tasks=[sample_task],
        )

    assert not respx.calls
    assert (run_dir / "run.json").read_text() == original_run_json


@respx.mock
async def test_released_run_resume_requires_a_solve_profile(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
) -> None:
    # Arrange
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json()
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(SolveProfileError, match="--solve-profile"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=None,
            tasks=[sample_task],
        )

    # Assert
    assert not respx.calls
    assert (run_dir / "run.json").read_text() == original_run_json


@respx.mock
async def test_run_json_holding_both_configuration_keys_fails_closed(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json(solve_configuration=SANITIZED_CONFIGURATION)
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(RunMetadataError):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    # Assert
    assert not respx.calls
    assert (run_dir / "run.json").read_text() == original_run_json


@pytest.mark.parametrize(
    "run_json",
    [canonical_run_json(), released_run_json()],
    ids=["canonical", "released"],
)
@respx.mock
async def test_pure_reevaluation_does_no_server_work_and_never_rewrites_run_json(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    run_json: str,
) -> None:
    # Arrange
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    stats = await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=None,
        tasks=[sample_task],
        reevaluate=True,
    )

    # Assert
    assert stats.total_tasks == 1
    assert stats.completed == 0
    assert stats.skipped == 0
    assert not respx.calls
    assert (run_dir / "run.json").read_text() == run_json


@pytest.mark.parametrize(
    ("apiKeyEnv", "environment_value"),
    [("MISSING_ENV", None), ("EMPTY_ENV", ""), ("BAD-NAME", "must-not-appear")],
)
@respx.mock
async def test_invalid_apiKeyEnv_environment_fails_before_http_or_tasks(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    apiKeyEnv: str,
    environment_value: str | None,
) -> None:
    if environment_value is None:
        monkeypatch.delenv(apiKeyEnv, raising=False)
    else:
        monkeypatch.setenv(apiKeyEnv, environment_value)
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    profile = {
        **PROFILE,
        "models": {"primary": {**PROFILE["models"]["primary"], "apiKeyEnv": apiKeyEnv}},
    }
    profile_path = tmp_path / "invalid-profile.json"
    profile_path.write_text(json.dumps(profile))

    with pytest.raises(SolveProfileError) as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            solve_server_url="http://localhost:3000",
            solve_profile_path=profile_path,
            tasks=[sample_task],
        )

    assert not respx.calls
    run_all.assert_not_awaited()
    assert "must-not-appear" not in str(exc_info.value)


async def test_context_expiry_stops_queued_tasks(
    tmp_path: Path,
    sample_task: Task,
) -> None:
    # Arrange
    run_path = tmp_path / "run"
    run_path.mkdir()
    tasks = [sample_task.model_copy(update={"id": f"task-{index}"}) for index in range(3)]
    solve_client = Mock()
    solve_client.upload_workbook = AsyncMock(
        side_effect=SolveContextExpiredError("Upload failed because solve context expired")
    )
    solve_client.solve = AsyncMock()
    dataset = Mock()
    dataset.get_input_path.return_value = tmp_path / "input.xlsx"
    runner = TaskRunner(
        solve_client=solve_client,
        evaluator=Mock(),
        dataset=dataset,
        run_dir=RunDirectory(run_path),
        concurrency=1,
    )

    # Act
    stats = await runner.run_all(tasks)

    # Assert
    assert solve_client.upload_workbook.await_count == 1
    solve_client.solve.assert_not_awaited()
    assert stats.errors == 3


async def test_missing_output_workbook_still_writes_transcript(
    tmp_path: Path,
    sample_task: Task,
) -> None:
    # Arrange
    run_path = tmp_path / "run"
    run_path.mkdir()
    input_path = tmp_path / "input.xlsx"
    input_path.write_bytes(b"input")
    transcript = {"error": "Workbook export failed", "messages": []}
    solve_client = Mock()
    solve_client.upload_workbook = AsyncMock(return_value="wb-123")
    solve_client.solve = AsyncMock(
        return_value=SolveResponse(
            id="solve-wb-123",
            model="opaque-model",
            workbook_id="wb-123",
            usage=SolveUsage(turns=1, tool_calls=0, input_tokens=2, output_tokens=3),
            output_xlsx=None,
            transcript=transcript,
        )
    )
    dataset = Mock()
    dataset.get_input_path.return_value = input_path
    evaluator = Mock()
    runner = TaskRunner(
        solve_client=solve_client,
        evaluator=evaluator,
        dataset=dataset,
        run_dir=RunDirectory(run_path),
    )

    # Act
    stats = await runner.run_all([sample_task])

    # Assert
    transcript_path = run_path / f"{sample_task.id}-transcript.json"
    assert json.loads(transcript_path.read_text()) == transcript
    evaluator.evaluate.assert_not_called()
    assert stats.errors == 1
