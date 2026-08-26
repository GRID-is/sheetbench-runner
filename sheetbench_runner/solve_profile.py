"""Load and validate non-secret solve profiles."""

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
    StringConstraints,
    ValidationError,
    model_validator,
)

MAX_MODELS = 32
MAX_MODEL_ROLES = 32
MAX_DICTIONARY_NAME_LENGTH = 64
MAX_MODEL_ID_LENGTH = 256
MAX_OUTPUT_TOKENS = 1_000_000
MAX_CONTEXT_TTL_SECONDS = 86400

Name = Annotated[str, StringConstraints(min_length=1, max_length=MAX_DICTIONARY_NAME_LENGTH)]


class SolveProfileError(ValueError):
    """A solve profile or its API-key environment is invalid."""


class ModelOptions(BaseModel):
    """Generation options for one configured model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    maxOutputTokens: Annotated[StrictInt, Field(gt=0, le=MAX_OUTPUT_TOKENS)] | None = None


class SanitizedModel(BaseModel):
    """A configured model without any reference to its API key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transport: Literal["anthropic", "openai-responses", "openai-compatible"]
    model: Annotated[str, Field(min_length=1, max_length=MAX_MODEL_ID_LENGTH)]
    options: ModelOptions | None = None


class ProfileModel(SanitizedModel):
    """A configured model that names the environment variable holding its API key."""

    apiKeyEnv: Annotated[
        str,
        Field(max_length=MAX_DICTIONARY_NAME_LENGTH, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"),
    ]


class SanitizedConfiguration(BaseModel):
    """A solve configuration that carries no API-key material."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    models: Annotated[Mapping[Name, SanitizedModel], Field(min_length=1, max_length=MAX_MODELS)]
    modelRoles: Annotated[dict[Name, Name], Field(min_length=1, max_length=MAX_MODEL_ROLES)]
    ttlSeconds: Annotated[StrictInt, Field(gt=0, le=MAX_CONTEXT_TTL_SECONDS)] | None = None

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

    models: Annotated[Mapping[Name, ProfileModel], Field(min_length=1, max_length=MAX_MODELS)]


@dataclass(frozen=True)
class SolveProfile:
    """A validated non-secret profile that resolves API keys on demand."""

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
    """Derive the exact server-visible sanitized configuration from a profile."""
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
        configuration = configuration.model_copy(update={"ttlSeconds": MAX_CONTEXT_TTL_SECONDS})
    default_model = configuration.models[configuration.modelRoles["default"]].model
    return SolveProfile(configuration, sanitized_configuration(configuration), default_model)
