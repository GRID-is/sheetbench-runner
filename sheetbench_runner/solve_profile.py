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


class SolveProfileError(ValueError):
    """A solve profile or its API-key environment is invalid."""


class ModelOptions(BaseModel):
    """Generation options for one configured model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    maxOutputTokens: StrictInt | None = None


class ProfileModel(BaseModel):
    """A configured model and the environment variable holding its API key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transport: Literal["anthropic", "openai-responses", "openai-compatible"]
    model: str
    apiKeyEnv: str
    options: ModelOptions | None = None


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
