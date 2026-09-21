"""Load and validate solve profiles."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal, Mapping, Self

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class SolveProfileError(ValueError):
    """A solve profile or its API-key environment is invalid."""


class ProfileModel(BaseModel):
    """A provider request body, its transport, and the environment variable holding its API key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transport: Literal["anthropic", "openai-responses", "openai-compatible"]
    apiKeyEnv: str
    baseUrl: str | None = Field(default=None, exclude_if=lambda value: value is None)
    request: Mapping[str, Any]

    @field_validator("baseUrl", mode="before")
    @classmethod
    def _valid_base_url(cls, value: object) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 2048
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
            or any(char in value for char in "\\?#")
            or not value.lower().startswith(("http://", "https://"))
        ):
            raise ValueError(
                "baseUrl must be an absolute HTTP(S) URL without credentials, query or fragment"
            )
        authority = value.split("://", 1)[1].split("/", 1)[0]
        if not authority or "@" in authority:
            raise ValueError("baseUrl must have a host and no credentials")
        try:
            AnyHttpUrl(value)
        except ValueError as error:
            raise ValueError("baseUrl must be a valid HTTP(S) URL") from error
        return value

    @model_validator(mode="after")
    def _base_url_requires_compatible_transport(self) -> Self:
        if self.baseUrl is not None and self.transport != "openai-compatible":
            raise ValueError("baseUrl is only supported for openai-compatible")
        return self

    @field_validator("request")
    @classmethod
    def _request_names_a_model(cls, request: Mapping[str, Any]) -> Mapping[str, Any]:
        model = request.get("model")
        if not isinstance(model, str) or not model:
            raise ValueError("request.model must be a non-empty string")
        return request

    @property
    def model(self) -> str:
        model: str = self.request["model"]
        return model


class SolveConfiguration(BaseModel):
    """A solve configuration as a profile file declares it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    models: Annotated[Mapping[str, ProfileModel], Field(min_length=1)]
    modelRoles: Annotated[dict[str, str], Field(min_length=1)]

    @model_validator(mode="after")
    def _roles_name_configured_models(self) -> Self:
        unknown = sorted({name for name in self.modelRoles.values() if name not in self.models})
        if unknown:
            raise ValueError(f"modelRoles must name configured models: {unknown}")
        if "default" not in self.modelRoles:
            raise ValueError("modelRoles must define the 'default' role")
        return self


@dataclass(frozen=True)
class SolveProfile:
    """A validated profile that resolves API keys on demand."""

    configuration: SolveConfiguration
    default_model: str

    def resolve_api_keys(self) -> dict[str, str]:
        """Read each model's API key from its environment variable into process memory."""
        api_keys: dict[str, str] = {}
        for name, model in self.configuration.models.items():
            value = os.environ.get(model.apiKeyEnv)
            if not value or not value.strip():
                raise SolveProfileError(
                    f"Environment variable '{model.apiKeyEnv}' is not set or empty"
                )
            api_keys[name] = value
        return api_keys


def load_solve_profile(path: Path) -> SolveProfile:
    """Load and validate profile JSON without reading any API-key environment variable."""
    try:
        raw: object = json.loads(path.read_text())
    except FileNotFoundError as e:
        raise SolveProfileError(f"Solve profile does not exist: {path}") from e
    except (OSError, json.JSONDecodeError) as e:
        raise SolveProfileError(f"Could not load solve profile {path}: {e}") from e

    try:
        configuration = SolveConfiguration.model_validate(raw)
    except ValidationError as e:
        raise SolveProfileError(f"{path}: {e}") from e

    default_model = configuration.models[configuration.modelRoles["default"]].model
    return SolveProfile(configuration, default_model)
