"""Shared infrastructure for infuser HTTP clients."""

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Self

import httpx


class InfuserError(Exception):
    """Base exception for infuser API errors."""

    pass


class InfuserTransientError(InfuserError):
    """
    Transient error that should trigger a retry on resume.

    This includes 5xx errors, timeouts, and connection failures.
    """

    pass


class InfuserPermanentError(InfuserError):
    """
    Permanent error that should not be retried.

    This includes 4xx errors (bad request, validation errors).
    """

    pass


# Limit error message length for readability
_ERROR_TEXT_MAX_LENGTH = 200


def _to_optional_int(value: object) -> int | None:
    """Convert a value to an optional int."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (str, float)):
        return int(value)
    return None


@asynccontextmanager
async def handle_http_errors(operation: str) -> AsyncIterator[None]:
    """Handle HTTP errors consistently across all operations."""
    try:
        yield
    except httpx.HTTPStatusError as e:
        text = e.response.text[:_ERROR_TEXT_MAX_LENGTH]
        if e.response.status_code >= 500:
            raise InfuserTransientError(
                f"{operation} error {e.response.status_code}: {text}"
            ) from e
        raise InfuserPermanentError(f"{operation} error {e.response.status_code}: {text}") from e
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        raise InfuserTransientError(f"Connection error: {e}") from e


class InfuserBaseClient:
    """Base client with shared HTTP infrastructure."""

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

    async def __aenter__(self) -> Self:
        if self._client is None:
            headers: dict[str, str] = {}
            api_key = os.environ.get("GRID_API_KEY")
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            self._client = httpx.AsyncClient(
                headers=headers,
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
