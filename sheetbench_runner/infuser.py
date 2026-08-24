"""HTTP client for the infuser /solve API with workbook upload/download."""

import base64
import binascii
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID

from .entities import InfuserUsage
from .infuser_base import (
    InfuserBaseClient,
    InfuserPermanentError,
    handle_http_errors,
)
from .solve_profile import SolveProfileError, validate_solve_configuration

logger = logging.getLogger(__name__)

# A SpreadsheetBench input is currently capped at 1 MiB by the solve server. These
# deliberately generous client-side limits permit substantial XLSX expansion while
# preventing a successful response from consuming unbounded memory.
MAX_WORKBOOK_BYTES = 32 * 1024 * 1024
MAX_WORKBOOK_BASE64_BYTES = ((MAX_WORKBOOK_BYTES + 2) // 3) * 4
MAX_SOLVE_RESPONSE_BYTES = MAX_WORKBOOK_BASE64_BYTES + 8 * 1024 * 1024
_DEFAULT_TTL_SECONDS = 7200
_CONTEXT_CLOCK_SKEW = timedelta(seconds=30)


def _nonempty_string(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError
    return value


def _nonnegative_int(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError
    return value


def _optional_nonnegative_int(value: object) -> int | None:
    return None if value is None else _nonnegative_int(value)


def _valid_context_token(value: object) -> str:
    token = _nonempty_string(value)
    if "=" in token:
        raise ValueError
    padding = "=" * (-len(token) % 4)
    try:
        decoded = base64.b64decode(token + padding, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as e:
        raise ValueError from e
    canonical = base64.urlsafe_b64encode(decoded).decode().rstrip("=")
    if canonical != token or len(decoded) < 32:
        raise ValueError
    return token


@dataclass(frozen=True)
class SolveContextResponse:
    """Sanitized response from creating a solve context."""

    id: str
    expires_at: str
    configuration: dict[str, Any]


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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._solve_context_id: str | None = None

    def _context_headers(self) -> dict[str, str]:
        if self._solve_context_id is None:
            raise RuntimeError("A solve context must be created before upload or solve")
        return {"X-Solve-Context": self._solve_context_id}

    @staticmethod
    def _contains_submitted_secret(value: object, secrets: tuple[str, ...]) -> bool:
        if isinstance(value, str):
            return any(secret in value for secret in secrets)
        if isinstance(value, dict):
            return any(
                InfuserClient._contains_submitted_secret(key, secrets)
                or InfuserClient._contains_submitted_secret(child, secrets)
                for key, child in value.items()
            )
        if isinstance(value, list):
            return any(InfuserClient._contains_submitted_secret(child, secrets) for child in value)
        return False

    async def _delete_solve_context_id(self, context_id: str) -> None:
        async with handle_http_errors("Delete solve context", include_response_body=False):
            response = await self.client.delete(
                f"{self.base_url}/solve-contexts/current",
                headers={"X-Solve-Context": context_id},
            )
            response.raise_for_status()

    async def create_solve_context(
        self,
        profile: Mapping[str, Any],
        credentials: Mapping[str, str],
    ) -> SolveContextResponse:
        """Create and retain one solve context for subsequent task requests."""
        if self._solve_context_id is not None:
            raise RuntimeError("A solve context already exists")
        payload = dict(profile)
        payload["credentials"] = {
            name: {"type": "api-key", "value": value} for name, value in credentials.items()
        }
        # The server may echo submitted credentials in validation errors. Since
        # every value in this request is sensitive, intentionally omit response
        # bodies from context-creation exceptions rather than attempting partial
        # redaction.
        request_started = datetime.now(UTC)
        try:
            async with handle_http_errors("Create solve context", include_response_body=False):
                response = await self.client.post(f"{self.base_url}/solve-contexts", json=payload)
                response.raise_for_status()
            data: object = response.json()
        except (ValueError, TypeError) as e:
            raise InfuserPermanentError("Invalid solve context response") from e
        response_received = datetime.now(UTC)

        if not isinstance(data, Mapping):
            raise InfuserPermanentError("Invalid solve context response")
        try:
            context_id = _valid_context_token(data["id"])
        except (KeyError, TypeError, ValueError) as e:
            raise InfuserPermanentError("Invalid solve context response") from e

        self._solve_context_id = context_id
        try:
            if set(data) != {"id", "expiresAt", "configuration"}:
                raise ValueError

            expires_at = _nonempty_string(data["expiresAt"])
            parsed_expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if parsed_expiry.tzinfo is None:
                raise ValueError
            parsed_expiry = parsed_expiry.astimezone(UTC)

            expected_configuration = validate_solve_configuration(profile)
            expected_configuration = {
                **expected_configuration,
                "ttlSeconds": expected_configuration.get("ttlSeconds", _DEFAULT_TTL_SECONDS),
            }
            configuration = validate_solve_configuration(data["configuration"])
            if configuration != expected_configuration:
                raise ValueError

            ttl_seconds = expected_configuration["ttlSeconds"]
            assert isinstance(ttl_seconds, int)
            now = datetime.now(UTC)
            earliest_expiry = request_started + timedelta(seconds=ttl_seconds) - _CONTEXT_CLOCK_SKEW
            latest_expiry = response_received + timedelta(seconds=ttl_seconds) + _CONTEXT_CLOCK_SKEW
            if parsed_expiry <= now or not earliest_expiry <= parsed_expiry <= latest_expiry:
                raise ValueError

            secrets = tuple(secret for secret in credentials.values() if secret)
            if self._contains_submitted_secret(configuration, secrets):
                raise SolveProfileError("configuration contains submitted credential material")
        except (KeyError, TypeError, ValueError, SolveProfileError) as e:
            primary_error = InfuserPermanentError("Invalid solve context response")
            try:
                await self._delete_solve_context_id(context_id)
            except Exception:
                logger.warning("Could not revoke invalid solve context response")
            finally:
                self._solve_context_id = None
            raise primary_error from e
        return SolveContextResponse(context_id, expires_at, configuration)

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
            InfuserTransientError: For 5xx errors, timeouts, connection failures
            InfuserPermanentError: For 4xx errors
        """
        async with handle_http_errors("Upload", include_response_body=False):
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
            data: object = response.json()
            if not isinstance(data, dict) or not {"id", "name", "sheets"} <= set(data):
                raise ValueError
            if set(data) - {"id", "name", "sheets", "revision"}:
                raise ValueError
            workbook_id = _nonempty_string(data["id"])
            if str(UUID(workbook_id)) != workbook_id:
                raise ValueError
            if _nonempty_string(data["name"]) != filepath.name:
                raise ValueError
            sheets = data["sheets"]
            if (
                not isinstance(sheets, list)
                or not sheets
                or any(not isinstance(sheet, str) or not sheet for sheet in sheets)
                or len(set(sheets)) != len(sheets)
            ):
                raise ValueError
            if "revision" in data:
                revision = data["revision"]
                if not isinstance(revision, int) or isinstance(revision, bool) or revision <= 0:
                    raise ValueError
            return workbook_id
        except (KeyError, TypeError, ValueError) as e:
            raise InfuserPermanentError("Invalid upload response") from e

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
            InfuserTransientError: For 5xx errors, timeouts, connection failures
            InfuserPermanentError: For 4xx errors
        """
        payload: dict[str, object] = {
            "workbookId": workbook_id,
            "messages": [{"role": "user", "content": prompt}],
        }

        response_body = bytearray()
        async with handle_http_errors("Solve", include_response_body=False):
            async with self.client.stream(
                "POST",
                f"{self.base_url}/solve",
                json=payload,
                headers=self._context_headers(),
            ) as response:
                response.raise_for_status()
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        if int(content_length) > MAX_SOLVE_RESPONSE_BYTES:
                            raise InfuserPermanentError("Invalid solve response")
                    except ValueError:
                        pass
                async for chunk in response.aiter_bytes():
                    if len(response_body) + len(chunk) > MAX_SOLVE_RESPONSE_BYTES:
                        raise InfuserPermanentError("Invalid solve response")
                    response_body.extend(chunk)
        try:
            data: object = json.loads(response_body)
            return self._parse_solve_response(data, workbook_id)
        except InfuserPermanentError:
            raise
        except (TypeError, ValueError) as e:
            raise InfuserPermanentError("Invalid solve response") from e

    def _parse_solve_response(self, data: object, workbook_id: str) -> SolveResponse:
        """Validate the server contract without including attacker-controlled values in errors."""
        try:
            required = {
                "id",
                "object",
                "model",
                "workbookId",
                "choices",
                "usage",
                "output_xlsx_base64",
                "transcript",
            }
            if not isinstance(data, dict) or not required <= set(data):
                raise ValueError
            if set(data) != required:
                raise ValueError

            solve_id = _nonempty_string(data["id"])
            if data["object"] != "solve.completion":
                raise ValueError
            model = _nonempty_string(data["model"])
            returned_workbook_id = _nonempty_string(data["workbookId"])
            if returned_workbook_id != workbook_id:
                raise ValueError

            choices = data["choices"]
            if not isinstance(choices, list) or len(choices) != 1:
                raise ValueError
            choice = choices[0]
            if not isinstance(choice, dict) or set(choice) != {
                "index",
                "message",
                "finish_reason",
            }:
                raise ValueError
            if _nonnegative_int(choice["index"]) != 0:
                raise ValueError
            message = choice["message"]
            if not isinstance(message, dict) or set(message) != {"role", "content"}:
                raise ValueError
            if message["role"] != "assistant" or not isinstance(message["content"], str):
                raise ValueError
            if choice["finish_reason"] not in {"stop", "error"}:
                raise ValueError

            usage_data = data["usage"]
            required_usage = {"turns", "tool_calls", "input_tokens", "output_tokens"}
            optional_usage = {"planning_turns", "planning_tool_calls"}
            if (
                not isinstance(usage_data, dict)
                or not required_usage <= set(usage_data)
                or set(usage_data) - required_usage - optional_usage
            ):
                raise ValueError
            usage = InfuserUsage(
                turns=_nonnegative_int(usage_data["turns"]),
                tool_calls=_nonnegative_int(usage_data["tool_calls"]),
                input_tokens=_nonnegative_int(usage_data["input_tokens"]),
                output_tokens=_nonnegative_int(usage_data["output_tokens"]),
                planning_turns=_optional_nonnegative_int(usage_data.get("planning_turns")),
                planning_tool_calls=_optional_nonnegative_int(
                    usage_data.get("planning_tool_calls")
                ),
            )

            xlsx_b64 = _nonempty_string(data["output_xlsx_base64"])
            if len(xlsx_b64) > MAX_WORKBOOK_BASE64_BYTES:
                raise ValueError
            output_xlsx = base64.b64decode(xlsx_b64, validate=True)
            if (
                not output_xlsx
                or len(output_xlsx) > MAX_WORKBOOK_BYTES
                or base64.b64encode(output_xlsx).decode() != xlsx_b64
            ):
                raise ValueError

            transcript = data["transcript"]
            if not isinstance(transcript, dict):
                raise ValueError

            return SolveResponse(
                id=solve_id,
                model=model,
                usage=usage,
                workbook_id=returned_workbook_id,
                output_xlsx=output_xlsx,
                transcript=transcript,
            )
        except (KeyError, TypeError, ValueError, binascii.Error) as e:
            raise InfuserPermanentError("Invalid solve response") from e

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
