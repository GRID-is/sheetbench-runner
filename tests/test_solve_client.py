"""Tests for the solve server client."""

import base64
import json
from pathlib import Path

import httpx
import pytest
import respx

from sheetbench_runner.solve_client import (
    NonRetryableSolveError,
    RetryableSolveError,
    SolveClient,
)
from sheetbench_runner.solve_profile import SolveConfiguration

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
PAYLOAD_MODEL = {"transport": "openai-compatible", "model": "opaque-model"}


def context_response(*, token: object = CONTEXT_TOKEN) -> dict[str, object]:
    return {"id": token}


def solve_response() -> dict[str, object]:
    return {
        "id": "test-id-123",
        "model": "opaque-model",
        "workbookId": "wb-123",
        "usage": {
            "turns": 5,
            "tool_calls": 8,
            "input_tokens": 1000,
            "output_tokens": 500,
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
    await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-key"})


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
        created = await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-key"})
        await client.upload_workbook(xlsx_file)
        await client.solve(workbook_id, "Test prompt")
        await client.delete_solve_context()

    assert created == CONTEXT_TOKEN
    creation = create_route.calls[0].request
    assert "X-Solve-Context" not in creation.headers
    assert json.loads(creation.content) == {
        "models": {"primary": {**PAYLOAD_MODEL, "apiKey": "test-key"}},
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
        }
    )
    route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=context_response())
    )

    async with SolveClient("http://localhost:3000") as client:
        await client.create_solve_context(
            profile,
            {"primary": "shared-key", "reviewer": "shared-key", "judge": "other-key"},
        )

    assert json.loads(route.calls[0].request.content) == {
        "models": {
            "primary": {"transport": "anthropic", "model": "model-a", "apiKey": "shared-key"},
            "reviewer": {
                "transport": "openai-responses",
                "model": "model-b",
                "apiKey": "shared-key",
            },
            "judge": {
                "transport": "openai-compatible",
                "model": "model-c",
                "options": {"maxOutputTokens": 123},
                "apiKey": "other-key",
            },
        },
        "modelRoles": {"default": "primary", "review": "reviewer", "judge": "judge"},
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


@pytest.mark.parametrize("body", [{}, {"id": 3}], ids=["missing-id", "non-string-id"])
@respx.mock
async def test_upload_rejects_malformed_success_response(tmp_path: Path, body: object) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(200, json=body)
    )
    async with SolveClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(NonRetryableSolveError, match="Invalid upload response"):
            await client.upload_workbook(xlsx_file)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model", 4),
        ("usage", {"turns": -1, "tool_calls": 1, "input_tokens": 2, "output_tokens": 3}),
        ("transcript", "not-an-object"),
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
        with pytest.raises(NonRetryableSolveError, match="Invalid solve response"):
            await client.solve("wb-123", "Test prompt")


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


@respx.mock
async def test_a_rejected_context_is_recreated_and_the_request_retried(tmp_path: Path) -> None:
    # Arrange
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        side_effect=[
            httpx.Response(201, json=context_response()),
            httpx.Response(201, json=context_response(token="second-context")),
        ]
    )
    upload_route = respx.post("http://localhost:3000/workbooks/upload").mock(
        side_effect=[
            httpx.Response(401, json={"error": "Invalid solve context"}),
            httpx.Response(200, json={"id": "wb-123", "name": "test.xlsx", "sheets": ["Sheet1"]}),
        ]
    )

    # Act
    async with SolveClient("http://localhost:3000") as client:
        await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-key"})
        workbook_id = await client.upload_workbook(xlsx_file)

    # Assert
    assert workbook_id == "wb-123"
    assert create_route.call_count == 2
    assert upload_route.calls[1].request.headers["X-Solve-Context"] == "second-context"
