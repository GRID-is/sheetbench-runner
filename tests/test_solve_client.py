"""Tests for the solve server client."""

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from sheetbench_runner.solve_client import (
    NonRetryableSolveError,
    RetryableSolveError,
    SolveClient,
    SolveContextExpiredError,
    SolveResponse,
)
from sheetbench_runner.solve_profile import SanitizedConfiguration, SolveConfiguration

PRIMARY_MODEL: dict[str, object] = {
    "transport": "openai-compatible",
    "model": "opaque-model",
    "apiKeyEnv": "OPENAI_API_KEY",
}
SOLVE_PROFILE = SolveConfiguration.model_validate(
    {
        "models": {"primary": PRIMARY_MODEL},
        "modelRoles": {"default": "primary"},
    }
)
CONTEXT_TOKEN = "solve-context-token"
SANITIZED_PRIMARY_MODEL = {"transport": "openai-compatible", "model": "opaque-model"}
EFFECTIVE_PROFILE = {
    "models": {"primary": SANITIZED_PRIMARY_MODEL},
    "modelRoles": {"default": "primary"},
    "ttlSeconds": 7200,
}
EFFECTIVE_CONFIGURATION = SanitizedConfiguration.model_validate(EFFECTIVE_PROFILE)


def context_response(
    *,
    token: object = CONTEXT_TOKEN,
    expires_at: object | None = None,
    configuration: object = EFFECTIVE_PROFILE,
) -> dict[str, object]:
    if expires_at is None:
        expires_at = (datetime.now(UTC) + timedelta(seconds=7200)).isoformat()
    return {"id": token, "expiresAt": expires_at, "configuration": configuration}


def solve_response() -> dict[str, object]:
    return {
        "id": "test-id-123",
        "object": "solve.completion",
        "model": "opaque-model",
        "workbookId": "wb-123",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "Done"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "turns": 5,
            "tool_calls": 8,
            "input_tokens": 1000,
            "output_tokens": 500,
            "planning_turns": None,
            "planning_tool_calls": None,
        },
        "output_xlsx_base64": base64.b64encode(b"fake-xlsx-bytes").decode(),
        "transcript": {"messages": [{"role": "assistant", "content": "Done"}]},
    }


async def activate_context(client: SolveClient) -> None:
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json=context_response(),
        )
    )
    await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})


@respx.mock
async def test_context_lifecycle_uses_contract_headers_and_bodies(tmp_path: Path) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json=context_response(),
        )
    )
    workbook_id = "123e4567-e89b-42d3-a456-426614174000"
    upload_route = respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(
            200, json={"id": workbook_id, "name": "test.xlsx", "sheets": ["Sheet1"]}
        )
    )
    solve_route = respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json={**solve_response(), "workbookId": workbook_id})
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    async with SolveClient("http://localhost:3000") as client:
        created = await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        await client.upload_workbook(xlsx_file)
        await client.solve(workbook_id, "Test prompt")
        await client.delete_solve_context()

    assert created.configuration == EFFECTIVE_CONFIGURATION
    creation = create_route.calls[0].request
    assert "X-Solve-Context" not in creation.headers
    assert json.loads(creation.content) == {
        "models": {"primary": {**SANITIZED_PRIMARY_MODEL, "apiKey": "test-secret"}},
        "modelRoles": {"default": "primary"},
    }
    for route in (upload_route, solve_route, delete_route):
        assert route.calls[0].request.headers["X-Solve-Context"] == CONTEXT_TOKEN
    assert json.loads(solve_route.calls[0].request.content) == {
        "workbookId": workbook_id,
        "messages": [{"role": "user", "content": "Test prompt"}],
    }


@respx.mock
async def test_context_request_repeats_shared_keys_and_supports_different_keys() -> None:
    profile = SolveConfiguration.model_validate(
        {
            "models": {
                "primary": {
                    "transport": "anthropic",
                    "model": "model-a",
                    "apiKeyEnv": "SHARED_KEY",
                },
                "reviewer": {
                    "transport": "openai-responses",
                    "model": "model-b",
                    "apiKeyEnv": "SHARED_KEY",
                },
                "judge": {
                    "transport": "openai-compatible",
                    "model": "model-c",
                    "apiKeyEnv": "OTHER_KEY",
                    "options": {"maxOutputTokens": 123},
                },
            },
            "modelRoles": {"default": "primary", "review": "reviewer", "judge": "judge"},
            "ttlSeconds": 900,
        }
    )
    expected_configuration = {
        "models": {
            "primary": {"transport": "anthropic", "model": "model-a"},
            "reviewer": {"transport": "openai-responses", "model": "model-b"},
            "judge": {
                "transport": "openai-compatible",
                "model": "model-c",
                "options": {"maxOutputTokens": 123},
            },
        },
        "modelRoles": {"default": "primary", "review": "reviewer", "judge": "judge"},
        "ttlSeconds": 900,
    }
    route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json=context_response(
                expires_at=(datetime.now(UTC) + timedelta(seconds=900)).isoformat(),
                configuration=expected_configuration,
            ),
        )
    )

    async with SolveClient("http://localhost:3000") as client:
        created = await client.create_solve_context(
            profile,
            {"primary": "shared-secret", "reviewer": "shared-secret", "judge": "other-secret"},
        )

    assert created.configuration == SanitizedConfiguration.model_validate(expected_configuration)
    assert json.loads(route.calls[0].request.content) == {
        "models": {
            "primary": {"transport": "anthropic", "model": "model-a", "apiKey": "shared-secret"},
            "reviewer": {
                "transport": "openai-responses",
                "model": "model-b",
                "apiKey": "shared-secret",
            },
            "judge": {
                "transport": "openai-compatible",
                "model": "model-c",
                "options": {"maxOutputTokens": 123},
                "apiKey": "other-secret",
            },
        },
        "modelRoles": {"default": "primary", "review": "reviewer", "judge": "judge"},
        "ttlSeconds": 900,
    }


@respx.mock
async def test_upload_and_solve_require_a_context(tmp_path: Path) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    async with SolveClient("http://localhost:3000") as client:
        with pytest.raises(RuntimeError, match="solve context"):
            await client.upload_workbook(xlsx_file)
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")


@pytest.mark.parametrize(
    "configuration",
    [
        {**EFFECTIVE_PROFILE, "credentials": {"primary": "test-secret"}},
        {
            **EFFECTIVE_PROFILE,
            "models": {
                "primary": {
                    **SANITIZED_PRIMARY_MODEL,
                    "apiKeyEnv": "OPENAI_API_KEY",
                }
            },
        },
        {
            **EFFECTIVE_PROFILE,
            "models": {
                "primary": {
                    **SANITIZED_PRIMARY_MODEL,
                    "apiKey": "test-secret",
                }
            },
        },
        {
            **EFFECTIVE_PROFILE,
            "models": {
                "primary": {
                    **SANITIZED_PRIMARY_MODEL,
                    "model": "attacker-selected-model",
                }
            },
        },
    ],
    ids=["credentials", "apiKeyEnv", "apiKey", "changed-model"],
)
@respx.mock
async def test_invalid_context_configuration_is_never_retained(
    configuration: dict[str, object],
) -> None:
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json=context_response(configuration=configuration),
        )
    )

    async with SolveClient("http://localhost:3000") as client:
        with pytest.raises(NonRetryableSolveError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")

    assert create_route.call_count == 1


@pytest.mark.parametrize(
    "response_update",
    [
        {"id": ""},
        {"id": 123},
        {"expiresAt": "not-a-timestamp"},
        {"configuration": []},
        {"configuration": {**EFFECTIVE_PROFILE, "ttlSeconds": 7201}},
    ],
    ids=[
        "empty-id",
        "non-string-id",
        "invalid-expiry",
        "invalid-configuration-type",
        "changed-ttl",
    ],
)
@respx.mock
async def test_malformed_context_response_is_never_retained(
    response_update: dict[str, object],
) -> None:
    response_data: dict[str, object] = context_response()
    response_data.update(response_update)
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=response_data)
    )

    async with SolveClient("http://localhost:3000") as client:
        with pytest.raises(NonRetryableSolveError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")


@pytest.mark.parametrize("missing_field", ["id", "expiresAt", "configuration"])
@respx.mock
async def test_context_response_missing_required_field_is_never_retained(
    missing_field: str,
) -> None:
    response_data = context_response()
    del response_data[missing_field]
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=response_data)
    )

    async with SolveClient("http://localhost:3000") as client:
        with pytest.raises(NonRetryableSolveError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")


@respx.mock
async def test_context_response_accepts_additive_top_level_fields() -> None:
    # Arrange
    response_data = context_response()
    response_data["serverVersion"] = "next"
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=response_data)
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    # Act
    async with SolveClient("http://localhost:3000") as client:
        context = await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        await client.delete_solve_context()

    # Assert
    assert context.configuration == EFFECTIVE_CONFIGURATION
    assert delete_route.call_count == 1
    assert delete_route.calls[0].request.headers["X-Solve-Context"] == CONTEXT_TOKEN


@respx.mock
async def test_upload_and_solve_success(tmp_path: Path) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    workbook_uuid = "123e4567-e89b-42d3-a456-426614174000"
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(
            200,
            json={"id": workbook_uuid, "name": "test.xlsx", "sheets": ["Sheet1"]},
        )
    )
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json={**solve_response(), "workbookId": workbook_uuid})
    )

    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        workbook_id = await client.upload_workbook(xlsx_file)
        response = await client.solve(workbook_id, "Test prompt")

    assert workbook_id == workbook_uuid
    assert isinstance(response, SolveResponse)
    assert response.id == "test-id-123"
    assert response.model == "opaque-model"
    assert response.workbook_id == workbook_uuid
    assert response.usage.turns == 5
    assert response.output_xlsx == b"fake-xlsx-bytes"
    assert response.transcript == {"messages": [{"role": "assistant", "content": "Done"}]}


@respx.mock
async def test_upload_accepts_additive_response_fields(tmp_path: Path) -> None:
    # Arrange
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    workbook_uuid = "123e4567-e89b-42d3-a456-426614174000"
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": workbook_uuid,
                "name": "test.xlsx",
                "sheets": ["Sheet1"],
                "revisions": 1,
            },
        )
    )

    # Act
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        workbook_id = await client.upload_workbook(xlsx_file)

    # Assert
    assert workbook_id == workbook_uuid


@respx.mock
async def test_solve_without_output_workbook_preserves_transcript() -> None:
    # Arrange
    body = solve_response()
    del body["output_xlsx_base64"]
    body["transcript"] = {"error": "Workbook export failed", "messages": []}
    respx.post("http://localhost:3000/solve").mock(return_value=httpx.Response(200, json=body))

    # Act
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        response = await client.solve("wb-123", "Test prompt")

    # Assert
    assert response.output_xlsx is None
    assert response.transcript == {"error": "Workbook export failed", "messages": []}


@respx.mock
async def test_solve_accepts_additive_response_fields() -> None:
    # Arrange
    body = solve_response()
    body["warnings"] = []
    choices = body["choices"]
    assert isinstance(choices, list)
    choice = choices[0]
    assert isinstance(choice, dict)
    choice["provider_metadata"] = {"request_id": "request-123"}
    message = choice["message"]
    assert isinstance(message, dict)
    message["annotations"] = []
    usage = body["usage"]
    assert isinstance(usage, dict)
    usage["cached_input_tokens"] = 10
    respx.post("http://localhost:3000/solve").mock(return_value=httpx.Response(200, json=body))

    # Act
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        response = await client.solve("wb-123", "Test prompt")

    # Assert
    assert response.output_xlsx == b"fake-xlsx-bytes"
    assert response.usage.input_tokens == 1000


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"id": ""},
        {"id": "123e4567-e89b-42d3-a456-426614174000", "name": 3, "sheets": ["Sheet1"]},
        {"id": "123e4567-e89b-42d3-a456-426614174000", "name": "test.xlsx", "sheets": "Sheet1"},
    ],
)
@respx.mock
async def test_upload_rejects_malformed_success_response(tmp_path: Path, body: object) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(200, json=body)
    )
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(NonRetryableSolveError, match="Invalid upload response") as exc_info:
            await client.upload_workbook(xlsx_file)
    assert repr(body) not in str(exc_info.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("object", "other"),
        ("model", 4),
        ("workbookId", "attacker-workbook"),
        ("usage", {"turns": -1, "tool_calls": 1, "input_tokens": 2, "output_tokens": 3}),
        ("transcript", "secret response body"),
        ("output_xlsx_base64", "Zg"),
    ],
)
@respx.mock
async def test_solve_rejects_malformed_success_response(field: str, value: object) -> None:
    body = solve_response()
    body[field] = value
    respx.post("http://localhost:3000/solve").mock(return_value=httpx.Response(200, json=body))
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(NonRetryableSolveError, match="Invalid solve response") as exc_info:
            await client.solve("wb-123", "Test prompt")
    assert "secret response body" not in str(exc_info.value)


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [(500, RetryableSolveError), (400, NonRetryableSolveError)],
)
@respx.mock
async def test_solve_http_errors(status_code: int, error_type: type[Exception]) -> None:
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(status_code, text="server error")
    )
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(error_type, match=str(status_code)):
            await client.solve("wb-123", "Test prompt")


@respx.mock
async def test_upload_connection_error_raises_transient(tmp_path: Path) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(RetryableSolveError, match="Connection"):
            await client.upload_workbook(xlsx_file)


@respx.mock
async def test_upload_401_is_retryable_context_expiry(tmp_path: Path) -> None:
    # Arrange
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(401, json={"error": "Invalid solve context"})
    )

    # Act and assert
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(SolveContextExpiredError, match="solve context expired"):
            await client.upload_workbook(xlsx_file)


@respx.mock
async def test_solve_401_is_retryable_context_expiry() -> None:
    # Arrange
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(401, json={"error": "Invalid solve context"})
    )

    # Act and assert
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(SolveContextExpiredError, match="solve context expired"):
            await client.solve("wb-123", "Test prompt")


@respx.mock
async def test_delete_accepts_already_expired_context() -> None:
    # Arrange
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(401, json={"error": "Invalid solve context"})
    )

    # Act
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        await client.delete_solve_context()

        # Assert
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")
    assert delete_route.call_count == 1


@respx.mock
async def test_download_and_status_never_receive_context_header() -> None:
    download_route = respx.get("http://localhost:3000/workbooks/wb-123/download").mock(
        return_value=httpx.Response(200, content=b"downloaded")
    )
    status_route = respx.get("http://localhost:3000/status").mock(
        return_value=httpx.Response(200, json={"version": "abc1234"})
    )
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        assert await client.download_workbook("wb-123") == b"downloaded"
        assert (await client.get_status())["version"] == "abc1234"

    assert "X-Solve-Context" not in download_route.calls[0].request.headers
    assert "X-Solve-Context" not in status_route.calls[0].request.headers


@respx.mock
async def test_grid_api_key_never_installs_authorization_header(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRID_API_KEY", "grid-key")
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    workbook_id = "123e4567-e89b-42d3-a456-426614174000"
    status_route = respx.get("http://localhost:3000/status").mock(
        return_value=httpx.Response(200, json={"version": "abc1234"})
    )
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=context_response())
    )
    upload_route = respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(
            200, json={"id": workbook_id, "name": "test.xlsx", "sheets": ["Sheet1"]}
        )
    )
    solve_route = respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json={**solve_response(), "workbookId": workbook_id})
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )
    async with SolveClient("http://localhost:3000") as client:
        await client.get_status()
        await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        await client.upload_workbook(xlsx_file)
        await client.solve(workbook_id, "Test prompt")
        await client.delete_solve_context()

    for route in (status_route, create_route, upload_route, solve_route, delete_route):
        assert "Authorization" not in route.calls[0].request.headers


@pytest.mark.parametrize(
    "transport_failure",
    [
        httpx.RemoteProtocolError("Server disconnected without sending a response"),
        httpx.ReadError("Connection reset by peer"),
    ],
    ids=["disconnected", "reset"],
)
@respx.mock
async def test_solve_transport_failures_are_retryable(transport_failure: Exception) -> None:
    # Arrange
    respx.post("http://localhost:3000/solve").mock(side_effect=transport_failure)

    # Act and assert
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(RetryableSolveError, match="Connection error"):
            await client.solve("wb-123", "Test prompt")
