"""Load and validate non-secret solve profiles."""

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

MAX_MODELS = 32
MAX_MODEL_ROLES = 32
MAX_DICTIONARY_NAME_LENGTH = 64
MAX_MODEL_ID_LENGTH = 256
MAX_OUTPUT_TOKENS = 1_000_000
MAX_API_KEY_BYTES = 4096
MAX_AGGREGATE_API_KEY_BYTES = 64 * 1024
MAX_CONTEXT_TTL_SECONDS = 86400
_PORTABLE_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TRANSPORTS = {"anthropic", "openai-responses", "openai-compatible"}


class SolveProfileError(ValueError):
    """A solve profile or its API-key environment is invalid."""


@dataclass(frozen=True)
class LoadedSolveProfile:
    """Validated configuration plus process-memory-only per-model API keys."""

    configuration: dict[str, Any]
    api_keys: dict[str, str] = field(repr=False)
    default_model: str


def _require_object(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SolveProfileError(f"Solve profile field '{field}' must be an object")
    return value


def _normalized_field(key: object) -> str:
    return str(key).lower().replace("_", "").replace("-", "")


def _check_profile_for_forbidden_fields(value: object, location: str = "profile") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = _normalized_field(key)
            if normalized in {"credential", "credentials", "apikey"}:
                raise SolveProfileError(
                    f"Solve profile must not contain unsafe field '{key}' at {location}"
                )
            _check_profile_for_forbidden_fields(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _check_profile_for_forbidden_fields(child, f"{location}[{index}]")


def _check_sanitized_configuration_fields(value: object, location: str = "configuration") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if _normalized_field(key) in {"credential", "credentials", "apikey", "apikeyenv"}:
                raise SolveProfileError(
                    f"Sanitized configuration contains forbidden field '{key}' at {location}"
                )
            _check_sanitized_configuration_fields(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _check_sanitized_configuration_fields(child, f"{location}[{index}]")


def _bounded_name(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= MAX_DICTIONARY_NAME_LENGTH


def _validate_common(value: object, *, require_api_key_env: bool) -> dict[str, Any]:
    profile = _require_object(value, "root")
    allowed_fields = {"models", "modelRoles", "ttlSeconds"}
    unknown_fields = set(profile) - allowed_fields
    if unknown_fields:
        field = sorted(unknown_fields)[0]
        raise SolveProfileError(f"Unknown or unsafe solve profile field: {field}")
    if require_api_key_env:
        _check_profile_for_forbidden_fields(profile)
    else:
        _check_sanitized_configuration_fields(profile)

    models = _require_object(profile.get("models"), "models")
    if not models:
        raise SolveProfileError("Solve profile field 'models' must not be empty")
    model_roles = _require_object(profile.get("modelRoles"), "modelRoles")
    if not model_roles:
        raise SolveProfileError("Solve profile field 'modelRoles' must not be empty")
    if len(models) > MAX_MODELS:
        raise SolveProfileError(
            f"Solve profile field 'models' must contain at most {MAX_MODELS} entries"
        )
    if len(model_roles) > MAX_MODEL_ROLES:
        raise SolveProfileError(
            f"Solve profile field 'modelRoles' must contain at most {MAX_MODEL_ROLES} entries"
        )

    ttl_seconds = profile.get("ttlSeconds")
    if ttl_seconds is not None and (
        not isinstance(ttl_seconds, int)
        or isinstance(ttl_seconds, bool)
        or ttl_seconds <= 0
        or ttl_seconds > MAX_CONTEXT_TTL_SECONDS
    ):
        raise SolveProfileError(
            "Solve profile field 'ttlSeconds' must be a positive integer no greater than "
            f"{MAX_CONTEXT_TTL_SECONDS}"
        )

    for name, model_value in models.items():
        if not _bounded_name(name):
            raise SolveProfileError(
                f"Solve profile model names must contain 1..{MAX_DICTIONARY_NAME_LENGTH} characters"
            )
        model = _require_object(model_value, f"models.{name}")
        allowed_model_fields = {"transport", "model", "options"}
        if require_api_key_env:
            allowed_model_fields.add("apiKeyEnv")
        unknown_model_fields = set(model) - allowed_model_fields
        if unknown_model_fields:
            field = sorted(unknown_model_fields)[0]
            raise SolveProfileError(f"Unknown solve profile field: models.{name}.{field}")

        if model.get("transport") not in _TRANSPORTS:
            raise SolveProfileError(f"models.{name}.transport is not supported")
        model_id = model.get("model")
        if not isinstance(model_id, str) or not 0 < len(model_id) <= MAX_MODEL_ID_LENGTH:
            raise SolveProfileError(
                f"models.{name}.model must contain 1..{MAX_MODEL_ID_LENGTH} characters"
            )

        if require_api_key_env:
            api_key_env = model.get("apiKeyEnv")
            if (
                not isinstance(api_key_env, str)
                or len(api_key_env) > MAX_DICTIONARY_NAME_LENGTH
                or _PORTABLE_ENV_NAME.fullmatch(api_key_env) is None
            ):
                raise SolveProfileError(
                    f"models.{name}.apiKeyEnv must name an environment variable using "
                    "[A-Za-z_][A-Za-z0-9_]*"
                )

        if "options" in model:
            options = _require_object(model["options"], f"models.{name}.options")
            unknown_options = set(options) - {"maxOutputTokens"}
            if unknown_options:
                field = sorted(unknown_options)[0]
                raise SolveProfileError(
                    f"Unknown solve profile field: models.{name}.options.{field}"
                )
            if "maxOutputTokens" in options:
                max_output_tokens = options["maxOutputTokens"]
                if (
                    not isinstance(max_output_tokens, int)
                    or isinstance(max_output_tokens, bool)
                    or max_output_tokens <= 0
                    or max_output_tokens > MAX_OUTPUT_TOKENS
                ):
                    raise SolveProfileError(
                        f"models.{name}.options.maxOutputTokens must be a positive integer no "
                        f"greater than {MAX_OUTPUT_TOKENS}"
                    )

    for role, model_name in model_roles.items():
        if not _bounded_name(role):
            raise SolveProfileError(
                "Solve profile modelRoles names must contain "
                f"1..{MAX_DICTIONARY_NAME_LENGTH} characters"
            )
        if not _bounded_name(model_name) or model_name not in models:
            raise SolveProfileError(
                f"modelRoles.{role} must name an existing configured model (got {model_name!r})"
            )
    if not isinstance(model_roles.get("default"), str):
        raise SolveProfileError("Solve profile modelRoles.default is required")
    return dict(profile)


def validate_solve_configuration(value: object) -> dict[str, Any]:
    """Validate and return a non-secret runner solve profile."""
    return _validate_common(value, require_api_key_env=True)


def validate_sanitized_configuration(value: object) -> dict[str, Any]:
    """Validate a server configuration that contains no key fields."""
    return _validate_common(value, require_api_key_env=False)


def sanitized_configuration(profile: object) -> dict[str, Any]:
    """Derive the exact server-visible sanitized configuration from a profile."""
    validated = validate_solve_configuration(profile)
    models = _require_object(validated["models"], "models")
    sanitized_models = {
        name: {
            key: child
            for key, child in _require_object(model, f"models.{name}").items()
            if key != "apiKeyEnv"
        }
        for name, model in models.items()
    }
    return {**validated, "models": sanitized_models}


def load_solve_profile(path: Path) -> LoadedSolveProfile:
    """Load profile JSON and resolve each model's API-key environment variable."""
    try:
        raw: object = json.loads(path.read_text())
    except FileNotFoundError as e:
        raise SolveProfileError(f"Solve profile does not exist: {path}") from e
    except (OSError, json.JSONDecodeError) as e:
        raise SolveProfileError(f"Could not load solve profile {path}: {e}") from e

    profile = validate_solve_configuration(raw)
    profile = {
        **profile,
        "ttlSeconds": profile.get("ttlSeconds", MAX_CONTEXT_TTL_SECONDS),
    }
    models = _require_object(profile["models"], "models")
    model_roles = _require_object(profile["modelRoles"], "modelRoles")
    api_keys: dict[str, str] = {}
    aggregate_bytes = 0
    for name, model_value in models.items():
        model = _require_object(model_value, f"models.{name}")
        environment_name = cast(str, model["apiKeyEnv"])
        value = os.environ.get(environment_name)
        if not value or not value.strip():
            raise SolveProfileError(
                f"Environment variable '{environment_name}' is not set or empty"
            )
        value_bytes = len(value.encode("utf-8"))
        if value_bytes > MAX_API_KEY_BYTES:
            raise SolveProfileError(
                f"API key from environment variable '{environment_name}' exceeds "
                f"{MAX_API_KEY_BYTES} UTF-8 bytes"
            )
        aggregate_bytes += value_bytes
        if aggregate_bytes > MAX_AGGREGATE_API_KEY_BYTES:
            raise SolveProfileError(
                f"Per-model API keys exceed {MAX_AGGREGATE_API_KEY_BYTES} aggregate UTF-8 bytes"
            )
        api_keys[name] = value

    default_name = cast(str, model_roles["default"])
    default = _require_object(models[default_name], f"models.{default_name}")
    default_model = cast(str, default["model"])
    return LoadedSolveProfile(dict(profile), api_keys, default_model)
