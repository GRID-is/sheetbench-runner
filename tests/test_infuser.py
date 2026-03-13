"""Tests for the infuser client."""

import base64

import httpx
import pytest
import respx

from sheetbench_runner.infuser import (
    InfuserClient,
    SolveResponse,
)
from sheetbench_runner.infuser_base import (
    InfuserPermanentError,
    InfuserTransientError,
)


@pytest.fixture
def mock_solve_response() -> dict:
    """A mock successful response from the /solve endpoint."""
    return {
        "id": "test-id-123",
        "model": "claude-sonnet-4-5",
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


@respx.mock
async def test_upload_workbook_success(tmp_path):
    """Test successful workbook upload."""
    # Arrange
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        return_value=httpx.Response(200, json={"id": "wb-123"})
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        workbook_id = await client.upload_workbook(xlsx_file)

    # Assert
    assert workbook_id == "wb-123"


@respx.mock
async def test_solve_success(mock_solve_response: dict):
    """Test successful /solve request."""
    # Arrange
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json=mock_solve_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        response = await client.solve("wb-123", "Test prompt")

    # Assert
    assert isinstance(response, SolveResponse)
    assert response.id == "test-id-123"
    assert response.model == "claude-sonnet-4-5"
    assert response.workbook_id == "wb-123"
    assert response.usage.turns == 5
    assert response.usage.tool_calls == 8
    assert response.usage.input_tokens == 1000
    assert response.usage.output_tokens == 500
    assert response.output_xlsx == b"fake-xlsx-bytes"
    assert response.transcript == {"messages": [{"role": "assistant", "content": "Done"}]}


@respx.mock
async def test_solve_without_output_xlsx():
    """Test /solve response without output_xlsx_base64 field."""
    # Arrange
    response_data = {
        "id": "test-id",
        "model": "test-model",
        "usage": {"turns": 1, "tool_calls": 0, "input_tokens": 100, "output_tokens": 50},
    }
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json=response_data)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        response = await client.solve("wb-123", "Test prompt")

    # Assert
    assert response.output_xlsx is None
    assert response.transcript is None


@respx.mock
async def test_solve_server_error_raises_transient():
    """Test that 5xx errors raise InfuserTransientError."""
    # Arrange
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    # Act & Assert
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserTransientError) as exc_info:
            await client.solve("wb-123", "Test prompt")

    assert "500" in str(exc_info.value)


@respx.mock
async def test_solve_client_error_raises_permanent():
    """Test that 4xx errors raise InfuserPermanentError."""
    # Arrange
    respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(400, text="Bad Request")
    )

    # Act & Assert
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserPermanentError) as exc_info:
            await client.solve("wb-123", "Test prompt")

    assert "400" in str(exc_info.value)


@respx.mock
async def test_upload_connection_error_raises_transient(tmp_path):
    """Test that connection errors during upload raise InfuserTransientError."""
    # Arrange
    xlsx_file = tmp_path / "test.xlsx"
    xlsx_file.write_bytes(b"fake-xlsx-content")
    respx.post("http://localhost:3000/workbooks/upload").mock(
        side_effect=httpx.ConnectError("Connection refused")
    )

    # Act & Assert
    async with InfuserClient("http://localhost:3000") as client:
        with pytest.raises(InfuserTransientError) as exc_info:
            await client.upload_workbook(xlsx_file)

    assert "Connection" in str(exc_info.value)


@respx.mock
async def test_download_workbook_success():
    """Test successful workbook download."""
    # Arrange
    xlsx_bytes = b"downloaded-xlsx-content"
    respx.get("http://localhost:3000/workbooks/wb-123/download").mock(
        return_value=httpx.Response(200, content=xlsx_bytes)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        result = await client.download_workbook("wb-123")

    # Assert
    assert result == xlsx_bytes


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
async def test_api_key_sent_as_bearer_token(
    monkeypatch: pytest.MonkeyPatch, mock_solve_response: dict,
):
    """Test that GRID_API_KEY is sent as a Bearer token when set."""
    # Arrange
    monkeypatch.setenv("GRID_API_KEY", "test-key-123")
    route = respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json=mock_solve_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        await client.solve("wb-123", "Test prompt")

    # Assert
    assert route.calls[0].request.headers["Authorization"] == "Bearer test-key-123"


@respx.mock
async def test_no_auth_header_without_api_key(
    monkeypatch: pytest.MonkeyPatch, mock_solve_response: dict,
):
    """Test that no Authorization header is sent when GRID_API_KEY is unset."""
    # Arrange
    monkeypatch.delenv("GRID_API_KEY", raising=False)
    route = respx.post("http://localhost:3000/solve").mock(
        return_value=httpx.Response(200, json=mock_solve_response)
    )

    # Act
    async with InfuserClient("http://localhost:3000") as client:
        await client.solve("wb-123", "Test prompt")

    # Assert
    assert "Authorization" not in route.calls[0].request.headers
