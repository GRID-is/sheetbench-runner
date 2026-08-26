"""Load and validate solve profiles."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Mapping, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    ValidationError,
    model_validator,
)

DEFAULT_CONTEXT_TTL_SECONDS = 86400


class SolveProfileError(ValueError):
    """A solve profile or its API-key environment is invalid."""


class ModelOptions(BaseModel):
    """Generation options for one configured model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    maxOutputTokens: StrictInt | None = None


class SanitizedModel(BaseModel):
    """A configured model without any reference to its API key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transport: Literal["anthropic", "openai-responses", "openai-compatible"]
    model: str
    options: ModelOptions | None = None


class ProfileModel(SanitizedModel):
    """A configured model that names the environment variable holding its API key."""

    apiKeyEnv: str


class SanitizedConfiguration(BaseModel):
    """A solve configuration that carries no API-key material."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    models: Annotated[Mapping[str, SanitizedModel], Field(min_length=1)]
    modelRoles: Annotated[dict[str, str], Field(min_length=1)]
    ttlSeconds: StrictInt | None = None

    @model_validator(mode="after")
    def _roles_name_configured_models(self) -> Self:
        unknown = sorted({name for name in self.modelRoles.values() if name not in self.models})
        if unknown:
            raise ValueError(f"modelRoles must name configured models: {unknown}")
        if "default" not in self.modelRoles:
            raise ValueError("modelRoles must define the 'default' role")
        return self


class SolveConfiguration(SanitizedConfiguration):
    """A solve configuration as a profile file declares it."""

    models: Annotated[Mapping[str, ProfileModel], Field(min_length=1)]


@dataclass(frozen=True)
class SolveProfile:
    """A validated profile that resolves API keys on demand."""

    configuration: SolveConfiguration
    sanitized_configuration: SanitizedConfiguration
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


def sanitized_configuration(profile: SolveConfiguration) -> SanitizedConfiguration:
    """Derive the server-visible configuration from a profile."""
    return SanitizedConfiguration.model_validate(
        profile.model_dump(exclude={"models": {"__all__": {"apiKeyEnv"}}})
    )


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

    if configuration.ttlSeconds is None:
        configuration = configuration.model_copy(update={"ttlSeconds": DEFAULT_CONTEXT_TTL_SECONDS})
    default_model = configuration.models[configuration.modelRoles["default"]].model
    return SolveProfile(configuration, sanitized_configuration(configuration), default_model)
