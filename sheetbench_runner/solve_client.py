"""HTTP client for the solve server API with workbook upload/download."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Self

import httpx
from pydantic import AliasChoices, Base64Bytes, BaseModel, ConfigDict, Field

from .entities import SolveUsage
from .solve_profile import SolveConfiguration


class SolveError(Exception):
    """Base exception for solve server API errors."""


class RetryableSolveError(SolveError):
    """
    Transient error that should trigger a retry.

    This includes 5xx errors and connection failures.
    """


class SolveTimeoutError(SolveError):
    """The server did not answer within the client timeout; the task is recorded as failed."""


class NonRetryableSolveError(SolveError):
    """
    Permanent error that should not be retried.

    This includes 4xx errors (bad request, validation errors).
    """


# Limit error message length for readability
_ERROR_TEXT_MAX_LENGTH = 200


@asynccontextmanager
async def handle_http_errors(operation: str) -> AsyncIterator[None]:
    """Handle HTTP errors consistently across all operations."""
    try:
        yield
    except httpx.HTTPStatusError as e:
        detail = f": {e.response.text[:_ERROR_TEXT_MAX_LENGTH]}"
        if e.response.status_code >= 500:
            raise RetryableSolveError(f"{operation} error {e.response.status_code}{detail}") from e
        raise NonRetryableSolveError(f"{operation} error {e.response.status_code}{detail}") from e
    except httpx.TimeoutException as e:
        if operation == "Solve":
            raise SolveTimeoutError(f"{operation} timed out: {type(e).__name__}") from e
        raise RetryableSolveError(f"{operation} timed out: {type(e).__name__}") from e
    except (httpx.NetworkError, httpx.RemoteProtocolError) as e:
        cause = e.__cause__ or e.__context__
        detail = f": {e}" if str(e) else ""
        caused_by = f" (caused by {type(cause).__name__}: {cause})" if cause else ""
        raise RetryableSolveError(f"Connection error: {type(e).__name__}{detail}{caused_by}") from e


class UploadResponse(BaseModel):
    """Body of a successful workbook upload."""

    model_config = ConfigDict(extra="ignore")

    id: str


class SolveResponse(BaseModel):
    """A successful /solve response."""

    model_config = ConfigDict(extra="ignore")

    id: str
    model: str
    workbook_id: str = Field(validation_alias=AliasChoices("workbookId", "workbook_id"))
    usage: SolveUsage
    output_xlsx: Base64Bytes | None = Field(
        default=None, validation_alias=AliasChoices("output_xlsx_base64", "output_xlsx")
    )
    transcript: dict[str, Any]


class SolveContextResponseBody(BaseModel):
    """Body of a successful solve-context creation."""

    model_config = ConfigDict(extra="ignore")

    id: str


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
        self._profile: SolveConfiguration | None = None
        self._api_keys: Mapping[str, str] = {}

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

    async def _context_headers(self) -> dict[str, str]:
        if self._solve_context_id is None:
            if self._profile is None:
                raise RuntimeError("A solve context must be created before upload or solve")
            await self._create_context()
        return {"X-Solve-Context": self._solve_context_id or ""}

    async def _send_with_context(
        self, send: Callable[[dict[str, str]], Awaitable[httpx.Response]]
    ) -> httpx.Response:
        response = await send(await self._context_headers())
        if response.status_code == 401:
            self._solve_context_id = None
            response = await send(await self._context_headers())
        return response

    async def create_solve_context(
        self,
        profile: SolveConfiguration,
        api_keys: Mapping[str, str],
    ) -> str:
        """Create a solve context; a context the server no longer accepts is recreated."""
        self._profile = profile
        self._api_keys = api_keys
        return await self._create_context()

    async def _create_context(self) -> str:
        assert self._profile is not None
        payload_models = {
            name: {
                **model.model_dump(exclude={"apiKeyEnv"}, exclude_none=True),
                "apiKey": self._api_keys[name],
            }
            for name, model in self._profile.models.items()
        }
        payload = {**self._profile.model_dump(exclude_none=True), "models": payload_models}

        async with handle_http_errors("Create solve context"):
            response = await self.client.post(f"{self.base_url}/solve-contexts", json=payload)
            response.raise_for_status()
        try:
            body = SolveContextResponseBody.model_validate(response.json())
        except ValueError as e:
            raise NonRetryableSolveError("Invalid solve context response") from e

        self._solve_context_id = body.id
        return body.id

    async def delete_solve_context(self) -> None:
        """Delete the retained solve context, if any."""
        if self._solve_context_id is None:
            return
        headers = {"X-Solve-Context": self._solve_context_id}
        self._solve_context_id = None
        async with handle_http_errors("Delete solve context"):
            response = await self.client.delete(
                f"{self.base_url}/solve-contexts/current", headers=headers
            )
            response.raise_for_status()

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
        files = {
            "file": (
                filepath.name,
                filepath.read_bytes(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        }
        async with handle_http_errors("Upload"):
            response = await self._send_with_context(
                lambda headers: self.client.post(
                    f"{self.base_url}/workbooks/upload", files=files, headers=headers
                )
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

        async with handle_http_errors("Solve"):
            response = await self._send_with_context(
                lambda headers: self.client.post(
                    f"{self.base_url}/solve", json=payload, headers=headers
                )
            )
            response.raise_for_status()
        try:
            return SolveResponse.model_validate(response.json())
        except ValueError as e:
            raise NonRetryableSolveError("Invalid solve response") from e

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
