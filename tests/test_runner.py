"""Tests for once-per-run solve context orchestration."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
import respx

from sheetbench_runner.entities import InfuserUsage, Task
from sheetbench_runner.infuser import SolveResponse
from sheetbench_runner.infuser_base import (
    InfuserContextExpiredError,
    InfuserPermanentError,
    InfuserTransientError,
)
from sheetbench_runner.run_directory import RunDirectory
from sheetbench_runner.runner import RunStats, TaskRunner, run
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
        infuser_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert status_route.call_count == 1
    assert "X-Solve-Context" not in status_route.calls[0].request.headers
    run_data = json.loads((run_dir / "run.json").read_text())
    assert run_data["model"] == "opaque-model"
    assert run_data["infuser_config"] == SANITIZED_CONFIGURATION
    artifacts = "".join(
        path.read_text(errors="ignore") for path in run_dir.iterdir() if path.is_file()
    )
    assert "artifact-forbidden-secret" not in artifacts
    assert CONTEXT_TOKEN not in artifacts


@respx.mock
async def test_run_cleanup_is_best_effort_and_does_not_mask_original_error(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "secret")
    run_all = AsyncMock(side_effect=ValueError("original run failure"))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    create_route, _, delete_route = context_routes()
    delete_route.mock(return_value=httpx.Response(500, text=f"cleanup failed for {CONTEXT_TOKEN}"))

    with pytest.raises(ValueError, match="original run failure"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            infuser_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert CONTEXT_TOKEN not in caplog.text


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

    with pytest.raises(InfuserTransientError, match="Delete solve context"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            infuser_url="http://localhost:3000",
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
        infuser_url="http://localhost:3000",
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
    ids=["credentials", "apiKey", "embedded-resolved-secret"],
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
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )
    run_dir = tmp_path / "run"

    with pytest.raises(InfuserPermanentError, match="Invalid solve context response") as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            infuser_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert delete_route.calls[0].request.headers["X-Solve-Context"] == CONTEXT_TOKEN
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
        return_value=httpx.Response(400, text="invalid artifact-forbidden-secret")
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    with pytest.raises(InfuserPermanentError) as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            infuser_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 0
    assert "artifact-forbidden-secret" not in str(exc_info.value)


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
            infuser_url="http://localhost:3000",
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
    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "model": "opaque-model",
                "git_hash": "old",
                "infuser_config": SANITIZED_CONFIGURATION,
            }
        )
    )
    (run_dir / "results.json").write_text(
        json.dumps([{"task_id": sample_task.id, "result": "pass"}])
    )

    stats = await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        infuser_url="http://localhost:3000",
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
        {"model": "different-model", "infuser_config": SANITIZED_CONFIGURATION},
        {"model": "opaque-model", "infuser_config": {**SANITIZED_CONFIGURATION, "ttlSeconds": 601}},
    ],
)
@respx.mock
async def test_mismatched_resume_aborts_and_revokes_before_running_tasks(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    historical_metadata: dict[str, object],
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "artifact-forbidden-secret")
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    create_route, _, delete_route = context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = json.dumps({**historical_metadata, "git_hash": "old"})
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    with pytest.raises(SolveProfileError, match="configuration") as exc_info:
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            infuser_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
        )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    run_all.assert_not_awaited()
    assert (run_dir / "run.json").read_text() == original_run_json
    assert "artifact-forbidden-secret" not in str(exc_info.value)
    assert CONTEXT_TOKEN not in str(exc_info.value)


@respx.mock
async def test_pure_reevaluation_does_no_server_work(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(
        json.dumps({"model": "old", "git_hash": "old", "infuser_config": {}})
    )
    (run_dir / "results.json").write_text("[]")

    stats = await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        infuser_url="http://localhost:3000",
        solve_profile_path=None,
        tasks=[sample_task],
        reevaluate=True,
    )

    assert stats.total_tasks == 1
    assert stats.completed == 0
    assert stats.skipped == 0
    assert not respx.calls


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
            infuser_url="http://localhost:3000",
            solve_profile_path=profile_path,
            tasks=[sample_task],
        )

    assert not respx.calls
    run_all.assert_not_awaited()
    assert "must-not-appear" not in str(exc_info.value)


@pytest.mark.parametrize("sensitive_value", ["submitted-provider-key", CONTEXT_TOKEN])
async def test_task_runner_rejects_solve_artifacts_containing_sensitive_values(
    tmp_path: Path,
    sample_task: Task,
    sensitive_value: str,
) -> None:
    run_path = tmp_path / "run"
    run_path.mkdir()
    (run_path / "run.json").write_text('{"safe": true}')
    input_path = tmp_path / "input.xlsx"
    input_path.write_bytes(b"input")

    infuser = Mock()
    infuser.upload_workbook = AsyncMock(return_value="wb-123")
    infuser.solve = AsyncMock(
        return_value=SolveResponse(
            id="solve-wb-123",
            model="opaque-model",
            workbook_id="wb-123",
            usage=InfuserUsage(1, 0, 2, 3),
            output_xlsx=b"safe-output",
            transcript={"nested": [{"error": f"provider returned {sensitive_value}"}]},
        )
    )
    dataset = Mock()
    dataset.get_input_path.return_value = input_path
    runner = TaskRunner(
        infuser=infuser,
        evaluator=Mock(),
        dataset=dataset,
        run_dir=RunDirectory(run_path),
        sensitive_values=("submitted-provider-key", CONTEXT_TOKEN),
    )

    stats = await runner.run_all([sample_task])

    assert stats.errors == 1
    assert not (run_path / f"{sample_task.id}-transcript.json").exists()
    assert not (run_path / f"{sample_task.id}-output.xlsx").exists()
    artifacts = b"".join(path.read_bytes() for path in run_path.iterdir() if path.is_file())
    assert sensitive_value.encode() not in artifacts


async def test_context_expiry_stops_queued_tasks(
    tmp_path: Path,
    sample_task: Task,
) -> None:
    # Arrange
    run_path = tmp_path / "run"
    run_path.mkdir()
    tasks = [replace(sample_task, id=f"task-{index}") for index in range(3)]
    infuser = Mock()
    infuser.upload_workbook = AsyncMock(
        side_effect=InfuserContextExpiredError("Upload failed because solve context expired")
    )
    infuser.solve = AsyncMock()
    dataset = Mock()
    dataset.get_input_path.return_value = tmp_path / "input.xlsx"
    runner = TaskRunner(
        infuser=infuser,
        evaluator=Mock(),
        dataset=dataset,
        run_dir=RunDirectory(run_path),
        concurrency=1,
    )

    # Act
    stats = await runner.run_all(tasks)

    # Assert
    assert infuser.upload_workbook.await_count == 1
    infuser.solve.assert_not_awaited()
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
    infuser = Mock()
    infuser.upload_workbook = AsyncMock(return_value="wb-123")
    infuser.solve = AsyncMock(
        return_value=SolveResponse(
            id="solve-wb-123",
            model="opaque-model",
            workbook_id="wb-123",
            usage=InfuserUsage(1, 0, 2, 3),
            output_xlsx=None,
            transcript=transcript,
        )
    )
    dataset = Mock()
    dataset.get_input_path.return_value = input_path
    evaluator = Mock()
    runner = TaskRunner(
        infuser=infuser,
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
