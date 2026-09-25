"""Tests for once-per-run solve context orchestration."""

import base64
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
import respx

from sheetbench_runner import runner as runner_module
from sheetbench_runner.config import NumericToleranceMode
from sheetbench_runner.entities import EvaluationResult, SolveUsage, Task
from sheetbench_runner.evaluator import Evaluator
from sheetbench_runner.pricing import estimate_cost_usd, pricing_for
from sheetbench_runner.run_directory import RunDirectory, RunMetadataError
from sheetbench_runner.runner import RunStats, TaskRunner, run
from sheetbench_runner.solve_client import (
    RetryableSolveError,
    SolveResponse,
    SolveTimeoutError,
)
from sheetbench_runner.solve_profile import SolveConfiguration, SolveProfileError

PROFILE: dict[str, Any] = {
    "models": {
        "primary": {
            "transport": "openai-compatible",
            "apiKeyEnv": "OPAQUE_ENV",
            "request": {"model": "opaque-model"},
        }
    },
    "modelRoles": {"default": "primary"},
}
PROFILE_MODEL: dict[str, object] = {
    "transport": "openai-compatible",
    "apiKeyEnv": "OPAQUE_ENV",
    "request": {"model": "opaque-model"},
}
PROFILE_CONFIGURATION = {
    "models": {"primary": PROFILE_MODEL},
    "modelRoles": {"default": "primary"},
}
CONTEXT_TOKEN = "Y2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2M"


def write_profile(path: Path) -> Path:
    path.write_text(json.dumps(PROFILE))
    return path


def canonical_run_json(**overrides: Any) -> str:
    document: dict[str, Any] = {
        "schema_version": 3,
        "model": "opaque-model",
        "git_hash": "old",
        "solve_configuration": PROFILE_CONFIGURATION,
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
                "configuration": PROFILE_CONFIGURATION,
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
async def test_run_creates_and_deletes_exactly_once_and_stores_profile_metadata(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "key")
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
        numeric_tolerance_mode="combined",
    )

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert status_route.call_count == 1
    assert "X-Solve-Context" not in status_route.calls[0].request.headers
    run_data = json.loads((run_dir / "run.json").read_text())
    assert run_data["schema_version"] == 3
    assert run_data["model"] == "opaque-model"
    assert run_data["solve_configuration"] == PROFILE_CONFIGURATION
    assert run_data["numeric_tolerance_mode"] == "combined"


@respx.mock
async def test_resume_creates_a_fresh_context_for_each_invocation(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "key")
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
    monkeypatch.setenv("OPAQUE_ENV", "key")
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


@respx.mock
async def test_resume_rejects_a_different_numeric_tolerance_mode(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPAQUE_ENV", "key")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(canonical_run_json())
    (run_dir / "results.json").write_text("[]")

    with pytest.raises(ValueError, match="numeric tolerance mode"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=write_profile(tmp_path / "profile.json"),
            tasks=[sample_task],
            numeric_tolerance_mode="combined",
        )

    assert not respx.calls


@pytest.mark.parametrize("metadata_json", [canonical_run_json, released_run_json])
@pytest.mark.parametrize("numeric_tolerance_mode", ["relative", "combined"])
async def test_reevaluate_records_the_selected_numeric_tolerance_mode(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
    metadata_json: Callable[[], str],
    numeric_tolerance_mode: NumericToleranceMode,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(metadata_json())
    (run_dir / "output.xlsx").write_bytes(b"output")
    (run_dir / "results.json").write_text(
        json.dumps(
            [
                {
                    "task_id": sample_task.id,
                    "result": "fail",
                    "output_file": "output.xlsx",
                }
            ]
        )
    )
    monkeypatch.setattr(
        Evaluator,
        "evaluate",
        Mock(return_value=EvaluationResult(passed=True, message="")),
    )

    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=None,
        tasks=[sample_task],
        reevaluate=True,
        numeric_tolerance_mode=numeric_tolerance_mode,
    )

    run_data = json.loads((run_dir / "run.json").read_text())
    assert run_data["numeric_tolerance_mode"] == numeric_tolerance_mode


async def test_reevaluate_rejects_partial_numeric_tolerance_mode_switch(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(canonical_run_json())
    (run_dir / "output.xlsx").write_bytes(b"output")
    (run_dir / "other-output.xlsx").write_bytes(b"output")
    (run_dir / "results.json").write_text(
        json.dumps(
            [
                {
                    "task_id": sample_task.id,
                    "result": "fail",
                    "output_file": "output.xlsx",
                },
                {
                    "task_id": "other-task",
                    "result": "fail",
                    "output_file": "other-output.xlsx",
                },
            ]
        )
    )

    with pytest.raises(ValueError, match="all recorded results"):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=run_dir,
            solve_server_url="http://localhost:3000",
            solve_profile_path=None,
            tasks=[sample_task],
            reevaluate=True,
            numeric_tolerance_mode="combined",
        )

    run_data = json.loads((run_dir / "run.json").read_text())
    assert "numeric_tolerance_mode" not in run_data


@pytest.mark.parametrize(
    "historical_metadata",
    [
        {"model": "different-model"},
        {
            "solve_configuration": {
                **PROFILE_CONFIGURATION,
                "modelRoles": {"default": "primary", "review": "primary"},
            }
        },
        {
            "solve_configuration": {
                **PROFILE_CONFIGURATION,
                "models": {"primary": {**PROFILE_MODEL, "baseUrl": "https://different.example/v1"}},
            }
        },
    ],
    ids=["model", "configuration", "endpoint"],
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
    monkeypatch.setenv("OPAQUE_ENV", "key")
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = canonical_run_json(**historical_metadata)
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(SolveProfileError, match="metadata"):
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


@respx.mock
async def test_released_run_is_migrated_to_canonical_metadata_after_context_creation(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "key")
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
        "schema_version": 3,
        "model": "opaque-model",
        "git_hash": "released-sha",
        "solve_configuration": PROFILE_CONFIGURATION,
        "test_set": 1,
        "notes": "released run",
        "numeric_tolerance_mode": "relative",
        "dataset_path": None,
        "created_at": "2026-01-02T03:04:05",
    }


@respx.mock
async def test_schema_2_run_is_resumed_with_the_profile_configuration(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "key")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    create_route, status_route, delete_route = context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "model": "opaque-model",
                "git_hash": "old",
                "solve_configuration": {
                    "models": {
                        "primary": {
                            "transport": "openai-compatible",
                            "model": "opaque-model",
                            "apiKeyEnv": "OPAQUE_ENV",
                            "options": None,
                        }
                    },
                    "modelRoles": {"default": "primary"},
                },
                "test_set": None,
                "notes": "schema 2",
                "dataset_path": str(sample_dataset_dir.resolve()),
                "created_at": "2026-08-26T00:00:00",
            }
        )
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
    assert json.loads((run_dir / "run.json").read_text()) == {
        "schema_version": 3,
        "model": "opaque-model",
        "git_hash": "old",
        "solve_configuration": PROFILE_CONFIGURATION,
        "test_set": None,
        "notes": "schema 2",
        "numeric_tolerance_mode": "relative",
        "dataset_path": str(sample_dataset_dir.resolve()),
        "created_at": "2026-08-26T00:00:00",
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
    monkeypatch.setenv("OPAQUE_ENV", "key")
    run_all = AsyncMock(return_value=RunStats(total_tasks=1))
    monkeypatch.setattr(TaskRunner, "run_all", run_all)
    context_routes()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    original_run_json = released_run_json(**historical_metadata)
    (run_dir / "run.json").write_text(original_run_json)
    (run_dir / "results.json").write_text("[]")

    # Act
    with pytest.raises(SolveProfileError, match="model"):
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


@respx.mock
async def test_real_legacy_run_without_infuser_config_is_migrated(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real pre-solve run.json files never had infuser_config or created_at."""
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "key")
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
    assert migrated["schema_version"] == 3
    assert migrated["model"] == "opaque-model"
    assert migrated["git_hash"] == "abc0001"
    assert migrated["solve_configuration"] == PROFILE_CONFIGURATION
    assert migrated["test_set"] is None
    assert migrated["notes"] == ""


@pytest.mark.parametrize(
    "historical_metadata",
    [{"model": None}, {"model": 7}],
    ids=["absent", "unknown-type"],
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
    monkeypatch.setenv("OPAQUE_ENV", "key")
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
    [("MISSING_ENV", None), ("EMPTY_ENV", "")],
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

    with pytest.raises(SolveProfileError):
        await run(
            dataset_path=sample_dataset_dir,
            run_dir_path=tmp_path / "run",
            solve_server_url="http://localhost:3000",
            solve_profile_path=profile_path,
            tasks=[sample_task],
        )

    assert not respx.calls
    run_all.assert_not_awaited()


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


async def test_results_row_records_the_input_workbook_relative_to_the_dataset(
    tmp_path: Path,
    sample_task_v2: Task,
) -> None:
    # Arrange
    run_path = tmp_path / "run"
    run_path.mkdir()
    input_path = tmp_path / "input.xlsx"
    input_path.write_bytes(b"input")
    solve_client = Mock()
    solve_client.upload_workbook = AsyncMock(return_value="wb-123")
    solve_client.solve = AsyncMock(
        return_value=SolveResponse(
            id="solve-wb-123",
            model="opaque-model",
            workbook_id="wb-123",
            usage=SolveUsage(turns=1, tool_calls=0, input_tokens=2, output_tokens=3),
            output_xlsx=base64.b64encode(b"output"),
            transcript={"messages": []},
        )
    )
    dataset = Mock()
    dataset.get_input_path.return_value = input_path
    evaluator = Mock()
    evaluator.evaluate.return_value = EvaluationResult(passed=True)
    runner = TaskRunner(
        solve_client=solve_client,
        evaluator=evaluator,
        dataset=dataset,
        run_dir=RunDirectory(run_path),
    )

    # Act
    await runner.run_all([sample_task_v2])

    # Assert
    [row] = json.loads((run_path / "results.json").read_text())
    assert row["input_file"] == "spreadsheet/01_bond_accounting/01_01_input.xlsx"
    assert row["output_file"] == "01_01-output.xlsx"


def solved(workbook_id: str = "wb-123") -> SolveResponse:
    return SolveResponse(
        id=f"solve-{workbook_id}",
        model="opaque-model",
        workbook_id=workbook_id,
        usage=SolveUsage(turns=1, tool_calls=0, input_tokens=2, output_tokens=3),
        output_xlsx=base64.b64encode(b"output"),
        transcript={"messages": []},
    )


def runner_with(
    tmp_path: Path, solve_side_effect: list[object], upload_ids: list[str]
) -> tuple[TaskRunner, Path, Mock]:
    run_path = tmp_path / "run"
    run_path.mkdir()
    input_path = tmp_path / "input.xlsx"
    input_path.write_bytes(b"input")
    solve_client = Mock()
    solve_client.upload_workbook = AsyncMock(side_effect=upload_ids)
    solve_client.solve = AsyncMock(side_effect=solve_side_effect)
    dataset = Mock()
    dataset.get_input_path.return_value = input_path
    evaluator = Mock()
    evaluator.evaluate.return_value = EvaluationResult(passed=True)
    runner = TaskRunner(
        solve_client=solve_client,
        evaluator=evaluator,
        dataset=dataset,
        run_dir=RunDirectory(run_path),
    )
    return runner, run_path, solve_client


async def test_a_connection_error_is_retried_with_a_fresh_upload(
    tmp_path: Path, sample_task: Task, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(runner_module, "TRANSIENT_RETRY_WAIT_SECONDS", 0)
    runner, run_path, solve_client = runner_with(
        tmp_path,
        [RetryableSolveError("Connection error: ReadError"), solved("wb-2")],
        ["wb-1", "wb-2"],
    )

    # Act
    stats = await runner.run_all([sample_task])

    # Assert
    assert solve_client.upload_workbook.await_count == 2
    assert solve_client.solve.await_args_list[1].args[0] == "wb-2"
    [row] = json.loads((run_path / "results.json").read_text())
    assert row["result"] == "pass"
    assert stats.errors == 0


async def test_connection_errors_on_every_attempt_leave_the_task_unrecorded(
    tmp_path: Path, sample_task: Task, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(runner_module, "TRANSIENT_RETRY_WAIT_SECONDS", 0)
    runner, run_path, solve_client = runner_with(
        tmp_path,
        [RetryableSolveError("Connection error: ReadError")] * 3,
        ["wb-1", "wb-2", "wb-3"],
    )

    # Act
    stats = await runner.run_all([sample_task])

    # Assert
    assert solve_client.upload_workbook.await_count == 3
    assert not (run_path / "results.json").exists()
    assert stats.errors == 1


async def test_a_timeout_is_recorded_once_as_a_failure(tmp_path: Path, sample_task: Task) -> None:
    # Arrange
    runner, run_path, solve_client = runner_with(
        tmp_path, [SolveTimeoutError("Solve timed out: ReadTimeout")], ["wb-1"]
    )

    # Act
    stats = await runner.run_all([sample_task])

    # Assert
    assert solve_client.upload_workbook.await_count == 1
    [row] = json.loads((run_path / "results.json").read_text())
    assert row["result"] == "fail"
    assert row["message"] == "Solve timed out: ReadTimeout"
    assert "output_file" not in row
    assert stats.failed == 1


OPUS_PROFILE: dict[str, Any] = {
    "models": {
        "primary": {
            "transport": "anthropic",
            "apiKeyEnv": "OPAQUE_ENV",
            "request": {"model": "claude-opus-5-5", "max_tokens": 64000},
        }
    },
    "modelRoles": {"default": "primary"},
}
RECORDED_PRICING: dict[str, Any] = {
    "transport": "anthropic",
    "model": "claude-opus-5-5",
    "base_url": None,
    "source": "recorded when the run started",
    "rates": {
        "input": "1",
        "output": "1",
        "cache_read": "0.1",
        "cache_write_5m": "2",
        "cache_write_1h": "3",
    },
    "scope": "recorded scope",
}
PRICED_USAGE = SolveUsage(
    turns=2,
    tool_calls=1,
    input_tokens=12_500_000,
    output_tokens=100_000,
    uncached_input_tokens=1_000_000,
    cache_read_input_tokens=10_000_000,
    cache_write_input_tokens=1_500_000,
    cache_write_5m_input_tokens=1_000_000,
    cache_write_1h_input_tokens=500_000,
)


def solved_with(usage: SolveUsage) -> SolveResponse:
    return solved().model_copy(update={"usage": usage})


def priced_runner(tmp_path: Path, responses: list[object]) -> tuple[TaskRunner, Path]:
    runner, run_path, _ = runner_with(
        tmp_path, responses, [f"wb-{n}" for n in range(len(responses))]
    )
    opus = SolveConfiguration.model_validate(OPUS_PROFILE).models["primary"]
    runner._pricing = pricing_for(opus)
    return runner, run_path


async def test_a_task_with_input_token_parts_records_them_and_its_estimated_cost(
    tmp_path: Path, sample_task: Task
) -> None:
    # Arrange: $4 uncached + $2 read + $5 5m write + $4 1h write + $2 output at Opus 5.5 rates.
    runner, run_path = priced_runner(tmp_path, [solved_with(PRICED_USAGE)])

    # Act
    stats = await runner.run_all([sample_task])

    # Assert
    [row] = json.loads((run_path / "results.json").read_text())
    assert row["input_tokens"] == 12_500_000
    assert row["uncached_input_tokens"] == 1_000_000
    assert row["cache_read_input_tokens"] == 10_000_000
    assert row["cache_write_input_tokens"] == 1_500_000
    assert row["cache_write_5m_input_tokens"] == 1_000_000
    assert row["cache_write_1h_input_tokens"] == 500_000
    assert row["estimated_cost_usd"] == 17.0
    assert stats.estimated_cost_usd == 17.0
    assert stats.priced_tasks == 1


async def test_a_task_from_an_older_server_records_no_estimated_cost(
    tmp_path: Path, sample_task: Task
) -> None:
    # Arrange
    runner, run_path = priced_runner(tmp_path, [solved()])

    # Act
    stats = await runner.run_all([sample_task])

    # Assert
    [row] = json.loads((run_path / "results.json").read_text())
    assert row["input_tokens"] == 2
    assert "uncached_input_tokens" not in row
    assert "estimated_cost_usd" not in row
    assert stats.priced_tasks == 0
    assert stats.estimated_cost_usd == 0


async def test_the_run_cost_sums_the_recorded_estimates_of_resumed_tasks(
    tmp_path: Path, sample_task: Task, sample_task_minimal: Task
) -> None:
    # Arrange: one task was priced in an earlier invocation, one runs now.
    runner, run_path = priced_runner(tmp_path, [solved_with(PRICED_USAGE)])
    (run_path / "results.json").write_text(
        json.dumps(
            [{"task_id": sample_task_minimal.id, "result": "pass", "estimated_cost_usd": 1.5}]
        )
    )
    runner._run_dir.load()

    # Act
    stats = await runner.run_all([sample_task, sample_task_minimal])

    # Assert
    assert stats.estimated_cost_usd == 18.5
    assert stats.priced_tasks == 2


@respx.mock
async def test_a_new_run_records_the_rates_of_its_default_model(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "key")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    context_routes()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(OPUS_PROFILE))
    run_dir = tmp_path / "run"

    # Act
    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=profile_path,
        tasks=[sample_task],
    )

    # Assert
    pricing = json.loads((run_dir / "run.json").read_text())["pricing"]
    assert pricing["transport"] == "anthropic"
    assert pricing["model"] == "claude-opus-5-5"
    assert pricing["rates"] == {
        "input": "4",
        "output": "20",
        "cache_read": "0.20",
        "cache_write_5m": "5",
        "cache_write_1h": "8",
        "cache_write": None,
    }
    assert pricing["source"]
    assert "probe" in pricing["scope"]


@respx.mock
async def test_a_new_run_of_a_model_without_rates_records_no_pricing(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("OPAQUE_ENV", "key")
    monkeypatch.setattr(TaskRunner, "run_all", AsyncMock(return_value=RunStats(total_tasks=1)))
    context_routes()
    run_dir = tmp_path / "run"

    # Act
    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=write_profile(tmp_path / "profile.json"),
        tasks=[sample_task],
    )

    # Assert
    assert "pricing" not in json.loads((run_dir / "run.json").read_text())


@respx.mock
async def test_a_resumed_run_prices_with_the_rates_it_recorded(
    tmp_path: Path,
    sample_dataset_dir: Path,
    sample_task: Task,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: the recorded rates differ from the catalog's.
    monkeypatch.setenv("OPAQUE_ENV", "key")
    context_routes()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(OPUS_PROFILE))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(
        canonical_run_json(
            model="claude-opus-5-5",
            solve_configuration=json.loads(json.dumps(OPUS_PROFILE)),
            pricing=RECORDED_PRICING,
        )
    )
    created: list[TaskRunner] = []

    async def record_runner(self: TaskRunner, tasks: list[Task]) -> RunStats:
        created.append(self)
        return RunStats()

    monkeypatch.setattr(TaskRunner, "run_all", record_runner)

    # Act
    await run(
        dataset_path=sample_dataset_dir,
        run_dir_path=run_dir,
        solve_server_url="http://localhost:3000",
        solve_profile_path=profile_path,
        tasks=[sample_task],
    )

    # Assert
    [runner] = created
    assert runner._pricing is not None
    assert runner._pricing.source == "recorded when the run started"
    assert estimate_cost_usd(PRICED_USAGE, runner._pricing.rates) == Decimal("5.6")
