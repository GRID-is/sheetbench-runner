"""HTTP client for the infuser API (old /v1/chat/completions endpoint)."""

from .entities import InfuserResponse, InfuserUsage
from .infuser_base import (
    InfuserBaseClient,
    InfuserError,
    InfuserPermanentError,
    InfuserTransientError,
    _to_optional_int,
    handle_http_errors,
)

# Re-export for backward compatibility
__all__ = [
    "InfuserClient",
    "InfuserError",
    "InfuserTransientError",
    "InfuserPermanentError",
]


class InfuserClient(InfuserBaseClient):
    """
    Client for the GRID Agent API.

    Uses the old /v1/chat/completions endpoint with filesystem paths.

    Usage:
        async with InfuserClient("http://localhost:3000") as client:
            response = await client.solve(prompt)
    """

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

        async with handle_http_errors("Solve"):
            response = await self.client.post(
                f"{self.base_url}/v1/chat/completions",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return self._parse_response(data)

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
