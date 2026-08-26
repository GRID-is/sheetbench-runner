"""HTTP client for the solve server API with workbook upload/download."""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, AsyncIterator, Literal, Mapping, Self

import httpx
from pydantic import Base64Bytes, BaseModel, ConfigDict, Field

from .entities import SolveUsage
from .solve_profile import (
    SanitizedConfiguration,
    SolveConfiguration,
    SolveProfileError,
    sanitized_configuration,
)


class SolveError(Exception):
    """Base exception for solve server API errors."""


class RetryableSolveError(SolveError):
    """
    Transient error that should trigger a retry on resume.

    This includes 5xx errors, timeouts, and connection failures.
    """


class NonRetryableSolveError(SolveError):
    """
    Permanent error that should not be retried.

    This includes 4xx errors (bad request, validation errors).
    """


class SolveContextExpiredError(RetryableSolveError):
    """The ephemeral solve context is no longer accepted by the server."""


# Limit error message length for readability
_ERROR_TEXT_MAX_LENGTH = 200
_DEFAULT_TTL_SECONDS = 7200


@asynccontextmanager
async def handle_http_errors(
    operation: str,
    *,
    context_protected: bool = False,
) -> AsyncIterator[None]:
    """Handle HTTP errors consistently across all operations."""
    try:
        yield
    except httpx.HTTPStatusError as e:
        detail = f": {e.response.text[:_ERROR_TEXT_MAX_LENGTH]}"
        if context_protected and e.response.status_code == 401:
            raise SolveContextExpiredError(
                f"{operation} failed because solve context expired"
            ) from e
        if e.response.status_code >= 500:
            raise RetryableSolveError(f"{operation} error {e.response.status_code}{detail}") from e
        raise NonRetryableSolveError(f"{operation} error {e.response.status_code}{detail}") from e
    except (httpx.NetworkError, httpx.RemoteProtocolError, httpx.TimeoutException) as e:
        raise RetryableSolveError(f"Connection error: {e}") from e


class UploadResponse(BaseModel):
    """Body of a successful workbook upload."""

    model_config = ConfigDict(extra="ignore")

    id: Annotated[str, Field(min_length=1)]
    name: str
    sheets: list[str]


class SolveMessage(BaseModel):
    """One message of a solve choice."""

    model_config = ConfigDict(extra="ignore")

    role: str
    content: str


class SolveChoice(BaseModel):
    """One completion choice of a solve response."""

    model_config = ConfigDict(extra="ignore")

    index: int
    message: SolveMessage
    finish_reason: str


class SolveResponseBody(BaseModel):
    """Body of a successful /solve response."""

    model_config = ConfigDict(extra="ignore")

    id: str
    object: Literal["solve.completion"]
    model: str
    workbookId: str
    choices: list[SolveChoice]
    usage: SolveUsage
    output_xlsx_base64: Base64Bytes | None = None
    transcript: dict[str, Any]


class SolveContextResponseBody(BaseModel):
    """Body of a successful solve-context creation."""

    model_config = ConfigDict(extra="ignore")

    id: Annotated[str, Field(min_length=1)]
    expiresAt: datetime
    configuration: SanitizedConfiguration


@dataclass(frozen=True)
class SolveContextResponse:
    """Sanitized response from creating a solve context."""

    id: str
    expires_at: datetime
    configuration: SanitizedConfiguration


@dataclass(frozen=True)
class SolveResponse:
    """Response from /solve endpoint."""

    id: str
    model: str
    usage: SolveUsage
    workbook_id: str
    output_xlsx: bytes | None = None
    transcript: dict[str, Any] | None = None


class SolveClient:
    """
    Client for the /solve endpoint with workbook upload flow.

    Usage:
        async with SolveClient("http://localhost:3000") as client:
            workbook_id = await client.upload_workbook(input_path)
            response = await client.solve(workbook_id, prompt)
            # response.output_xlsx contains the solved workbook bytes
            # response.transcript contains the inline transcript
    """

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 3600,
        client: httpx.AsyncClient | None = None,
    ):
        """
        Initialize the client.

        Args:
            base_url: Base URL of the API (e.g., "http://localhost:3000")
            timeout_seconds: Timeout for requests (default: 1 hour)
            client: Optional httpx client for testing
        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client
        self._owns_client = client is None
        self._solve_context_id: str | None = None

    async def __aenter__(self) -> Self:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds, connect=30.0),
            )
        return self

    async def __aexit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: object
    ) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Client not initialized. Use 'async with' context manager.")
        return self._client

    async def get_status(self) -> dict[str, object]:
        """
        Get server status including model and version info.

        Returns:
            Dict with 'default_model', 'version', etc.
        """
        async with handle_http_errors("Status"):
            response = await self.client.get(f"{self.base_url}/status")
            response.raise_for_status()
            result: dict[str, object] = response.json()
            return result

    def _context_headers(self) -> dict[str, str]:
        if self._solve_context_id is None:
            raise RuntimeError("A solve context must be created before upload or solve")
        return {"X-Solve-Context": self._solve_context_id}

    async def _delete_solve_context_id(self, context_id: str) -> None:
        async with handle_http_errors("Delete solve context"):
            response = await self.client.delete(
                f"{self.base_url}/solve-contexts/current",
                headers={"X-Solve-Context": context_id},
            )
            if response.status_code == 401:
                return
            response.raise_for_status()

    async def create_solve_context(
        self,
        profile: SolveConfiguration,
        api_keys: Mapping[str, str],
    ) -> SolveContextResponse:
        """Create and retain one solve context for subsequent task requests."""
        if self._solve_context_id is not None:
            raise RuntimeError("A solve context already exists")
        if set(api_keys) != set(profile.models):
            raise SolveProfileError("API keys must exactly match configured models")
        payload_models = {
            name: {
                **model.model_dump(exclude={"apiKeyEnv"}, exclude_none=True),
                "apiKey": api_keys[name],
            }
            for name, model in profile.models.items()
        }
        payload = {**profile.model_dump(exclude_none=True), "models": payload_models}

        expected = sanitized_configuration(profile)
        if expected.ttlSeconds is None:
            expected = expected.model_copy(update={"ttlSeconds": _DEFAULT_TTL_SECONDS})

        async with handle_http_errors("Create solve context"):
            response = await self.client.post(f"{self.base_url}/solve-contexts", json=payload)
            response.raise_for_status()
        try:
            body = SolveContextResponseBody.model_validate(response.json())
        except ValueError as e:
            raise NonRetryableSolveError("Invalid solve context response") from e
        if body.configuration != expected:
            raise NonRetryableSolveError("Invalid solve context response")

        self._solve_context_id = body.id
        return SolveContextResponse(body.id, body.expiresAt, body.configuration)

    async def delete_solve_context(self) -> None:
        """Delete the retained solve context and forget its identifier."""
        headers = self._context_headers()
        try:
            await self._delete_solve_context_id(headers["X-Solve-Context"])
        finally:
            self._solve_context_id = None

    async def upload_workbook(self, filepath: Path) -> str:
        """
        Upload a workbook and return its ID.

        Args:
            filepath: Path to the xlsx file to upload

        Returns:
            The workbook ID assigned by the server

        Raises:
            RetryableSolveError: For 5xx errors, timeouts, connection failures
            NonRetryableSolveError: For 4xx errors
        """
        async with handle_http_errors("Upload", context_protected=True):
            with open(filepath, "rb") as f:
                files = {
                    "file": (
                        filepath.name,
                        f,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                }
                response = await self.client.post(
                    f"{self.base_url}/workbooks/upload",
                    files=files,
                    headers=self._context_headers(),
                )
            response.raise_for_status()
        try:
            return UploadResponse.model_validate(response.json()).id
        except ValueError as e:
            raise NonRetryableSolveError("Invalid upload response") from e

    async def solve(
        self,
        workbook_id: str,
        prompt: str,
    ) -> SolveResponse:
        """
        Solve a task using the /solve endpoint.

        Args:
            workbook_id: ID of the uploaded workbook
            prompt: The formatted prompt with workbook_id

        Returns:
            SolveResponse with inline transcript and workbook bytes

        Raises:
            RetryableSolveError: For 5xx errors, timeouts, connection failures
            NonRetryableSolveError: For 4xx errors
        """
        payload: dict[str, object] = {
            "workbookId": workbook_id,
            "messages": [{"role": "user", "content": prompt}],
        }

        async with handle_http_errors("Solve", context_protected=True):
            response = await self.client.post(
                f"{self.base_url}/solve",
                json=payload,
                headers=self._context_headers(),
            )
            response.raise_for_status()
        try:
            body = SolveResponseBody.model_validate(response.json())
        except ValueError as e:
            raise NonRetryableSolveError("Invalid solve response") from e
        if body.workbookId != workbook_id:
            raise NonRetryableSolveError("Invalid solve response")
        return SolveResponse(
            id=body.id,
            model=body.model,
            usage=body.usage,
            workbook_id=body.workbookId,
            output_xlsx=body.output_xlsx_base64,
            transcript=body.transcript,
        )

    async def download_workbook(self, workbook_id: str) -> bytes:
        """
        Download a workbook by ID.

        Args:
            workbook_id: ID of the workbook to download

        Returns:
            The xlsx file bytes

        Raises:
            RetryableSolveError: For 5xx errors, timeouts, connection failures
            NonRetryableSolveError: For 4xx errors (including 404)
        """
        async with handle_http_errors("Download"):
            response = await self.client.get(f"{self.base_url}/workbooks/{workbook_id}/download")
            response.raise_for_status()
            return response.content
