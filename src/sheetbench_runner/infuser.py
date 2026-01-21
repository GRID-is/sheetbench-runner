"""HTTP client for the infuser API."""

import httpx

from .entities import InfuserResponse, InfuserUsage


def _to_optional_int(value: object) -> int | None:
    """Convert a value to an optional int."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (str, float)):
        return int(value)
    return None


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


class InfuserClient:
    """
    Client for the grid-apiary-llm-fusion infuser API.

    Usage:
        async with InfuserClient("http://localhost:3000") as client:
            response = await client.solve(prompt)
    """

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 3600,
        client: httpx.AsyncClient | None = None,
    ):
        """
        Initialize the infuser client.

        Args:
            base_url: Base URL of the infuser API (e.g., "http://localhost:3000")
            timeout_seconds: Timeout for solve requests (default: 1 hour)
            client: Optional httpx client for testing with respx
        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "InfuserClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds, connect=30.0)
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
        try:
            response = await self.client.get(f"{self.base_url}/status")
            response.raise_for_status()
            result: dict[str, object] = response.json()
            return result
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500:
                raise InfuserTransientError(f"Server error: {e.response.status_code}") from e
            raise InfuserPermanentError(f"Client error: {e.response.status_code}") from e
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise InfuserTransientError(f"Connection error: {e}") from e

    async def solve(self, prompt: str) -> InfuserResponse:
        """
        Send a prompt to the infuser and get the response.

        Args:
            prompt: The full prompt including task details

        Returns:
            InfuserResponse with output path, transcript path, and usage stats

        Raises:
            InfuserTransientError: For 5xx errors, timeouts, connection failures
            InfuserPermanentError: For 4xx errors
        """
        payload = {
            "model": "grid-api",
            "messages": [{"role": "user", "content": prompt}],
        }

        try:
            response = await self.client.post(
                f"{self.base_url}/v1/chat/completions",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return self._parse_response(data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500:
                raise InfuserTransientError(
                    f"Server error {e.response.status_code}: {e.response.text[:200]}"
                ) from e
            raise InfuserPermanentError(
                f"Client error {e.response.status_code}: {e.response.text[:200]}"
            ) from e
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise InfuserTransientError(f"Connection error: {e}") from e

    def _parse_response(self, data: dict[str, object]) -> InfuserResponse:
        """Parse the API response into an InfuserResponse object."""
        try:
            usage_raw = data.get("usage", {})
            usage_data = usage_raw if isinstance(usage_raw, dict) else {}
            usage = InfuserUsage(
                turns=int(usage_data.get("turns", 0) or 0),
                tool_calls=int(usage_data.get("tool_calls", 0) or 0),
                input_tokens=int(usage_data.get("input_tokens", 0) or 0),
                output_tokens=int(usage_data.get("output_tokens", 0) or 0),
                planning_turns=_to_optional_int(usage_data.get("planning_turns")),
                planning_tool_calls=_to_optional_int(usage_data.get("planning_tool_calls")),
            )

            id_val = data.get("id", "")
            model_val = data.get("model", "unknown")
            output_path = data.get("output_path")
            transcript_path = data.get("transcript_path")

            return InfuserResponse(
                id=str(id_val) if id_val else "",
                model=str(model_val) if model_val else "unknown",
                usage=usage,
                output_path=str(output_path) if output_path else None,
                transcript_path=str(transcript_path) if transcript_path else None,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise InfuserPermanentError(f"Invalid response format: {e}") from e
