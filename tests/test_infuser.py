"""Tests for the infuser client."""

import httpx
import pytest
import respx

from sheetbench_runner.infuser import (
    InfuserClient,
    InfuserPermanentError,
    InfuserTransientError,
)


@pytest.fixture
def mock_infuser_response() -> dict:
    """A mock successful response from the infuser API."""
    return {
        "id": "test-id-123",
        "object": "chat.completion",
        "model": "claude-sonnet-4-5",
        "choices": [{"message": {"content": "Done"}}],
        "usage": {
            "turns": 5,
            "tool_calls": 8,
            "input_tokens": 1000,
            "output_tokens": 500,
            "planning_turns": None,
            "planning_tool_calls": None,
        },
        "output_path": "/tmp/output.xlsx",
        "transcript_path": "/tmp/transcript.json",
    }


@respx.mock
async def test_solve_success(mock_infuser_response: dict):
    """Test successful solve request."""
    # Arrange
    respx.post("http://localhost:3000/v1/chat/completions").mock(
        return_value=httpx.Response(200, json=mock_infuser_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        response = await client.solve("Test prompt")

    # Assert
    assert response.id == "test-id-123"
    assert response.model == "claude-sonnet-4-5"
    assert response.usage.turns == 5
    assert response.usage.tool_calls == 8
    assert response.usage.input_tokens == 1000
    assert response.usage.output_tokens == 500
    assert response.output_path == "/tmp/output.xlsx"
    assert response.transcript_path == "/tmp/transcript.json"


@respx.mock
async def test_solve_server_error_raises_transient():
    """Test that 5xx errors raise InfuserTransientError."""
    # Arrange
    respx.post("http://localhost:3000/v1/chat/completions").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    # Act & Assert
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserTransientError) as exc_info:
            await client.solve("Test prompt")

    assert "500" in str(exc_info.value)


@respx.mock
async def test_solve_client_error_raises_permanent():
    """Test that 4xx errors raise InfuserPermanentError."""
    # Arrange
    respx.post("http://localhost:3000/v1/chat/completions").mock(
        return_value=httpx.Response(400, text="Bad Request")
    )

    # Act & Assert
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError) as exc_info:
            await client.solve("Test prompt")

    assert "400" in str(exc_info.value)


@respx.mock
async def test_solve_connection_error_raises_transient():
    """Test that connection errors raise InfuserTransientError."""
    # Arrange
    respx.post("http://localhost:3000/v1/chat/completions").mock(
        side_effect=httpx.ConnectError("Connection refused")
    )

    # Act & Assert
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserTransientError) as exc_info:
            await client.solve("Test prompt")

    assert "Connection" in str(exc_info.value)


@respx.mock
async def test_get_status_success():
    """Test successful status request."""
    # Arrange
    status_response = {
        "default_model": "claude-sonnet-4-5",
        "version": "abc1234",
        "status": "healthy",
    }
    respx.get("http://localhost:3000/status").mock(
        return_value=httpx.Response(200, json=status_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        status = await client.get_status()

    # Assert
    assert status["default_model"] == "claude-sonnet-4-5"
    assert status["version"] == "abc1234"


@respx.mock
async def test_client_strips_trailing_slash():
    """Test that trailing slashes in URL are handled."""
    # Arrange
    respx.get("http://localhost:3000/status").mock(
        return_value=httpx.Response(200, json={"status": "ok"})
    )

    # Act
    async with InfuserClient("http://localhost:3000/") as client:
        status = await client.get_status()

    # Assert
    assert status["status"] == "ok"


async def test_client_with_custom_httpx_client():
    """Test that a custom httpx client can be injected (for testing)."""
    # Arrange
    custom_client = httpx.AsyncClient()

    # Act
    client = InfuserClient("http://localhost:3000", client=custom_client)

    # Assert
    assert client._client is custom_client
    assert client._owns_client is False

    # Cleanup
    await custom_client.aclose()


@respx.mock
async def test_api_key_sent_as_bearer_token(
    monkeypatch: pytest.MonkeyPatch, mock_infuser_response: dict,
):
    """Test that GRID_API_KEY is sent as a Bearer token when set."""
    # Arrange
    monkeypatch.setenv("GRID_API_KEY", "test-key-123")
    route = respx.post("http://localhost:3000/v1/chat/completions").mock(
        return_value=httpx.Response(200, json=mock_infuser_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        await client.solve("Test prompt")

    # Assert
    assert route.calls[0].request.headers["Authorization"] == "Bearer test-key-123"


@respx.mock
async def test_no_auth_header_without_api_key(
    monkeypatch: pytest.MonkeyPatch, mock_infuser_response: dict,
):
    """Test that no Authorization header is sent when GRID_API_KEY is unset."""
    # Arrange
    monkeypatch.delenv("GRID_API_KEY", raising=False)
    route = respx.post("http://localhost:3000/v1/chat/completions").mock(
        return_value=httpx.Response(200, json=mock_infuser_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        await client.solve("Test prompt")

    # Assert
    assert "Authorization" not in route.calls[0].request.headers
