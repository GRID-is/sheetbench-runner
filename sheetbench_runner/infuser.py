"""HTTP client for the infuser /solve API with workbook upload/download."""

import base64
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .entities import InfuserUsage
from .infuser_base import (
    InfuserBaseClient,
    InfuserPermanentError,
    _to_optional_int,
    handle_http_errors,
)


@dataclass(frozen=True)
class SolveResponse:
    """Response from /solve endpoint."""

    id: str
    model: str
    usage: InfuserUsage
    workbook_id: str
    output_xlsx: bytes | None = None
    transcript: dict[str, Any] | None = None


class InfuserClient(InfuserBaseClient):
    """
    Client for the /solve endpoint with workbook upload flow.

    Usage:
        async with InfuserClient("http://localhost:3000") as client:
            workbook_id = await client.upload_workbook(input_path)
            response = await client.solve(workbook_id, prompt)
            # response.output_xlsx contains the solved workbook bytes
            # response.transcript contains the inline transcript
    """

    async def upload_workbook(self, filepath: Path) -> str:
        """
        Upload a workbook and return its ID.

        Args:
            filepath: Path to the xlsx file to upload

        Returns:
            The workbook ID assigned by the server

        Raises:
            InfuserTransientError: For 5xx errors, timeouts, connection failures
            InfuserPermanentError: For 4xx errors
        """
        async with handle_http_errors("Upload"):
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
                )
            response.raise_for_status()
            data = response.json()
            workbook_id: str = data["id"]
            return workbook_id

    async def solve(
        self,
        workbook_id: str,
        prompt: str,
        *,
        model: str | None = None,
    ) -> SolveResponse:
        """
        Solve a task using the /solve endpoint.

        Args:
            workbook_id: ID of the uploaded workbook
            prompt: The formatted prompt with workbook_id
            model: Optional model override (e.g., 'openai/gpt-4o')

        Returns:
            SolveResponse with inline transcript and workbook bytes

        Raises:
            InfuserTransientError: For 5xx errors, timeouts, connection failures
            InfuserPermanentError: For 4xx errors
        """
        payload: dict[str, object] = {
            "workbookId": workbook_id,
            "messages": [{"role": "user", "content": prompt}],
        }
        if model is not None:
            payload["model"] = model

        async with handle_http_errors("Solve"):
            response = await self.client.post(
                f"{self.base_url}/solve",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return self._parse_solve_response(data, workbook_id)

    def _parse_solve_response(self, data: dict[str, object], workbook_id: str) -> SolveResponse:
        """Parse the /solve response into a SolveResponse object."""
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

            # Decode base64 xlsx if present
            output_xlsx: bytes | None = None
            xlsx_b64 = data.get("output_xlsx_base64")
            if xlsx_b64 and isinstance(xlsx_b64, str):
                output_xlsx = base64.b64decode(xlsx_b64)

            # Get inline transcript
            transcript_raw = data.get("transcript")
            transcript: dict[str, Any] | None = None
            if isinstance(transcript_raw, dict):
                transcript = transcript_raw

            return SolveResponse(
                id=str(data.get("id", "")),
                model=str(data.get("model", "unknown")),
                usage=usage,
                workbook_id=workbook_id,
                output_xlsx=output_xlsx,
                transcript=transcript,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise InfuserPermanentError(f"Invalid response format: {e}") from e

    async def download_workbook(self, workbook_id: str) -> bytes:
        """
        Download a workbook by ID.

        Args:
            workbook_id: ID of the workbook to download

        Returns:
            The xlsx file bytes

        Raises:
            InfuserTransientError: For 5xx errors, timeouts, connection failures
            InfuserPermanentError: For 4xx errors (including 404)
        """
        async with handle_http_errors("Download"):
            response = await self.client.get(f"{self.base_url}/workbooks/{workbook_id}/download")
            response.raise_for_status()
            return response.content
