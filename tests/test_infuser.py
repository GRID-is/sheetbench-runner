"""Tests for the infuser client."""

import base64
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from sheetbench_runner.infuser import InfuserClient, SolveResponse
from sheetbench_runner.infuser_base import (
    InfuserContextExpiredError,
    InfuserPermanentError,
    InfuserTransientError,
)

PRIMARY_MODEL: dict[str, object] = {
    "transport": "openai-compatible",
    "model": "opaque-model",
    "apiKeyEnv": "OPENAI_API_KEY",
}
SOLVE_PROFILE = {
    "models": {"primary": PRIMARY_MODEL},
    "modelRoles": {"default": "primary"},
}
CONTEXT_TOKEN = base64.urlsafe_b64encode(b"c" * 32).decode().rstrip("=")
SANITIZED_PRIMARY_MODEL = {"transport": "openai-compatible", "model": "opaque-model"}
EFFECTIVE_PROFILE = {
    "models": {"primary": SANITIZED_PRIMARY_MODEL},
    "modelRoles": {"default": "primary"},
    "ttlSeconds": 7200,
}


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


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.yielded = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


async def activate_context(client: InfuserClient) -> None:
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

    async with InfuserClient("http://localhost:3000") as client:
        created = await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        await client.upload_workbook(xlsx_file)
        await client.solve(workbook_id, "Test prompt")
        await client.delete_solve_context()

    assert created.configuration == EFFECTIVE_PROFILE
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
    profile = {
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

    async with InfuserClient("http://localhost:3000") as client:
        created = await client.create_solve_context(
            profile,
            {"primary": "shared-secret", "reviewer": "shared-secret", "judge": "other-secret"},
        )

    assert created.configuration == expected_configuration
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
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(RuntimeError, match="solve context"):
            await client.upload_workbook(xlsx_file)
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")


@respx.mock
async def test_context_creation_error_never_exposes_apiKeyEnv_value() -> None:
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(400, text="invalid apiKeyEnv test-secret")
    )
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError) as exc_info:
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})

    assert "test-secret" not in str(exc_info.value)


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
                    "model": "prefix-test-secret-suffix",
                }
            },
        },
    ],
    ids=["credentials", "apiKeyEnv", "apiKey", "embedded-resolved-secret"],
)
@respx.mock
async def test_invalid_context_configuration_is_revoked_and_never_retained(
    configuration: dict[str, object],
) -> None:
    create_route = respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json=context_response(configuration=configuration),
        )
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(
            InfuserPermanentError, match="Invalid solve context response"
        ) as exc_info:
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")

    assert create_route.call_count == 1
    assert delete_route.call_count == 1
    assert delete_route.calls[0].request.headers["X-Solve-Context"] == CONTEXT_TOKEN
    assert "test-secret" not in str(exc_info.value)


@respx.mock
async def test_context_response_rejects_api_key_value_outside_configuration() -> None:
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=context_response())
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": CONTEXT_TOKEN})

    assert delete_route.call_count == 1


@pytest.mark.parametrize(
    ("response_update", "should_revoke"),
    [
        ({"id": ""}, False),
        ({"id": 123}, False),
        ({"id": "not+base64url"}, False),
        ({"id": base64.urlsafe_b64encode(b"short").decode().rstrip("=")}, False),
        ({"expiresAt": "not-a-timestamp"}, True),
        ({"expiresAt": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()}, True),
        ({"expiresAt": (datetime.now(UTC) + timedelta(days=2)).isoformat()}, True),
        ({"configuration": {**EFFECTIVE_PROFILE, "ttlSeconds": 7201}}, True),
        (
            {
                "configuration": {
                    **EFFECTIVE_PROFILE,
                    "models": {
                        "primary": {
                            **SANITIZED_PRIMARY_MODEL,
                            "model": "attacker-selected-model",
                        }
                    },
                }
            },
            True,
        ),
        ({"configuration": []}, True),
    ],
    ids=[
        "empty-id",
        "non-string-id",
        "non-base64url-id",
        "short-id",
        "invalid-expiry",
        "expired",
        "wild-expiry",
        "changed-ttl",
        "changed-model",
        "invalid-configuration-type",
    ],
)
@respx.mock
async def test_malformed_context_response_revokes_only_a_usable_id(
    response_update: dict[str, object], should_revoke: bool
) -> None:
    response_data: dict[str, object] = context_response()
    response_data.update(response_update)
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=response_data)
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})

    assert delete_route.call_count == int(should_revoke)
    if should_revoke:
        request = delete_route.calls[0].request
        assert request.url == httpx.URL("http://localhost:3000/solve-contexts/current")
        assert request.headers["X-Solve-Context"] == CONTEXT_TOKEN


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
    async with InfuserClient("http://localhost:3000") as client:
        context = await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        await client.delete_solve_context()

    # Assert
    assert context.configuration == EFFECTIVE_PROFILE
    assert delete_route.call_count == 1
    assert delete_route.calls[0].request.headers["X-Solve-Context"] == CONTEXT_TOKEN


@pytest.mark.parametrize("missing_field", ["expiresAt", "configuration"])
@respx.mock
async def test_context_response_missing_later_field_is_revoked(missing_field: str) -> None:
    response_data = context_response()
    del response_data[missing_field]
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=response_data)
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})

    assert delete_route.call_count == 1
    assert delete_route.calls[0].request.headers["X-Solve-Context"] == CONTEXT_TOKEN


@pytest.mark.parametrize(
    "invalid_id",
    [None, "not+base64url", base64.urlsafe_b64encode(b"short").decode().rstrip("=")],
    ids=["absent", "malformed", "weak"],
)
@respx.mock
async def test_context_response_without_usable_id_is_not_revoked(invalid_id: object) -> None:
    response_data = context_response()
    if invalid_id is None:
        del response_data["id"]
    else:
        response_data["id"] = invalid_id
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(201, json=response_data)
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(204)
    )

    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError, match="Invalid solve context response"):
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})

    assert delete_route.call_count == 0


@respx.mock
async def test_response_validation_error_survives_failed_revocation_without_leaking(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    respx.post("http://localhost:3000/solve-contexts").mock(
        return_value=httpx.Response(
            201,
            json=context_response(expires_at="invalid"),
        )
    )
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(500, text="cleanup leaked test-secret context-to-revoke")
    )

    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(
            InfuserPermanentError, match="Invalid solve context response"
        ) as exc_info:
            await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        with pytest.raises(RuntimeError, match="solve context"):
            await client.solve("wb-123", "Test prompt")

    assert delete_route.call_count == 1
    assert "test-secret" not in str(exc_info.value)
    assert "test-secret" not in caplog.text
    assert CONTEXT_TOKEN not in caplog.text


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

    async with InfuserClient("http://localhost:3000") as client:
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
    async with InfuserClient("http://localhost:3000") as client:
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
    async with InfuserClient("http://localhost:3000") as client:
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
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        response = await client.solve("wb-123", "Test prompt")

    # Assert
    assert response.output_xlsx == b"fake-xlsx-bytes"
    assert response.usage.input_tokens == 1000


@pytest.mark.parametrize("separator", ["\n", "\r\n", " ", "\t"])
@respx.mock
async def test_solve_accepts_whitespace_in_output_workbook_base64(separator: str) -> None:
    # Arrange
    body = solve_response()
    encoded = body["output_xlsx_base64"]
    assert isinstance(encoded, str)
    body["output_xlsx_base64"] = separator.join((encoded[:8], encoded[8:]))
    respx.post("http://localhost:3000/solve").mock(return_value=httpx.Response(200, json=body))

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        response = await client.solve("wb-123", "Test prompt")

    # Assert
    assert response.output_xlsx == b"fake-xlsx-bytes"


@pytest.mark.parametrize("separator", ["\v", "\f", "\u00a0"])
@respx.mock
async def test_solve_rejects_other_whitespace_in_output_workbook_base64(separator: str) -> None:
    # Arrange
    body = solve_response()
    encoded = body["output_xlsx_base64"]
    assert isinstance(encoded, str)
    body["output_xlsx_base64"] = separator.join((encoded[:8], encoded[8:]))
    respx.post("http://localhost:3000/solve").mock(return_value=httpx.Response(200, json=body))

    # Act and assert
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid solve response"):
            await client.solve("wb-123", "Test prompt")


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"id": ""},
        {"id": "not-a-uuid", "name": "test.xlsx", "sheets": ["Sheet1"]},
        {"id": "123e4567-e89b-42d3-a456-426614174000", "name": 3, "sheets": ["Sheet1"]},
        {"id": "123e4567-e89b-42d3-a456-426614174000", "name": "test.xlsx", "sheets": "Sheet1"},
        {"id": "123e4567-e89b-42d3-a456-426614174000", "name": "test.xlsx", "sheets": [""]},
        {
            "id": "123e4567-e89b-42d3-a456-426614174000",
            "name": "test.xlsx",
            "sheets": ["Sheet1"],
            "revision": True,
        },
    ],
)
@respx.mock
async def test_upload_rejects_malformed_success_response(tmp_path: Path, body: object) -> None:
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(200, json=body)
    )
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid upload response") as exc_info:
            await client.upload_workbook(xlsx_file)
    assert repr(body) not in str(exc_info.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", ""),
        ("object", "other"),
        ("model", 4),
        ("workbookId", "attacker-workbook"),
        ("choices", []),
        (
            "choices",
            [
                {
                    "index": True,
                    "message": {"role": "assistant", "content": "Done"},
                    "finish_reason": "stop",
                }
            ],
        ),
        (
            "choices",
            [{"index": 0, "message": {"role": "user", "content": "Done"}, "finish_reason": "stop"}],
        ),
        (
            "choices",
            [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "Done"},
                    "finish_reason": "hacked",
                }
            ],
        ),
        ("usage", {"turns": True, "tool_calls": 1, "input_tokens": 2, "output_tokens": 3}),
        ("usage", {"turns": -1, "tool_calls": 1, "input_tokens": 2, "output_tokens": 3}),
        ("transcript", "secret response body"),
        ("output_xlsx_base64", "not canonical !!!"),
        ("output_xlsx_base64", "Zg"),
    ],
)
@respx.mock
async def test_solve_rejects_malformed_success_response(field: str, value: object) -> None:
    body = solve_response()
    body[field] = value
    respx.post("http://localhost:3000/solve").mock(return_value=httpx.Response(200, json=body))
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid solve response") as exc_info:
            await client.solve("wb-123", "Test prompt")
    assert "secret response body" not in str(exc_info.value)


@respx.mock
async def test_solve_rejects_oversized_response_before_json_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sheetbench_runner.infuser.MAX_SOLVE_RESPONSE_BYTES", 100)
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, content=b"{" + b" " * 100 + b"}")
    )
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid solve response"):
            await client.solve("wb-123", "Test prompt")


@respx.mock
async def test_solve_stops_consuming_and_closes_oversized_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sheetbench_runner.infuser.MAX_SOLVE_RESPONSE_BYTES", 100)
    stream = TrackingStream([b"{" + b" " * 59, b" " * 60, b"never-consumed"])
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, headers={"Content-Length": "1"}, stream=stream)
    )

    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid solve response"):
            await client.solve("wb-123", "Test prompt")

    assert stream.yielded == 2
    assert stream.closed


@respx.mock
async def test_solve_rejects_content_length_before_consuming_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sheetbench_runner.infuser.MAX_SOLVE_RESPONSE_BYTES", 100)
    stream = TrackingStream([b"never-consumed"])
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, headers={"Content-Length": "101"}, stream=stream)
    )

    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid solve response"):
            await client.solve("wb-123", "Test prompt")

    assert stream.yielded == 0
    assert stream.closed


@respx.mock
async def test_solve_rejects_workbook_over_decoded_size_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sheetbench_runner.infuser.MAX_WORKBOOK_BYTES", 3)
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json=solve_response())
    )
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserPermanentError, match="Invalid solve response"):
            await client.solve("wb-123", "Test prompt")


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [(500, InfuserTransientError), (400, InfuserPermanentError)],
)
@respx.mock
async def test_solve_http_errors(status_code: int, error_type: type[Exception]) -> None:
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(status_code, text="server error")
    )
    async with InfuserClient("http://localhost:3000") as client:
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
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserTransientError, match="Connection"):
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
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserContextExpiredError, match="solve context expired"):
            await client.upload_workbook(xlsx_file)


@respx.mock
async def test_solve_401_is_retryable_context_expiry() -> None:
    # Arrange
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(401, json={"error": "Invalid solve context"})
    )

    # Act and assert
    async with InfuserClient("http://localhost:3000") as client:
        await activate_context(client)
        with pytest.raises(InfuserContextExpiredError, match="solve context expired"):
            await client.solve("wb-123", "Test prompt")


@respx.mock
async def test_delete_accepts_already_expired_context() -> None:
    # Arrange
    delete_route = respx.delete("http://localhost:3000/solve-contexts/current").mock(
        return_value=httpx.Response(401, json={"error": "Invalid solve context"})
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
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
    async with InfuserClient("http://localhost:3000") as client:
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
    async with InfuserClient("http://localhost:3000") as client:
        await client.get_status()
        await client.create_solve_context(SOLVE_PROFILE, {"primary": "test-secret"})
        await client.upload_workbook(xlsx_file)
        await client.solve(workbook_id, "Test prompt")
        await client.delete_solve_context()

    for route in (status_route, create_route, upload_route, solve_route, delete_route):
        assert "Authorization" not in route.calls[0].request.headers
