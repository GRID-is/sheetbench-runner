"""Tests for solve profile loading and environment resolution."""

import json
from pathlib import Path
from typing import Any

import pytest

from sheetbench_runner.solve_profile import SolveProfileError, load_solve_profile

PROFILE: dict[str, Any] = {
    "models": {
        "primary": {
            "transport": "anthropic",
            "model": "model-v9",
            "apiKeyEnv": "FIRST_KEY",
        },
        "reviewer": {
            "transport": "openai-responses",
            "model": "review-model",
            "apiKeyEnv": "SECOND_KEY",
        },
    },
    "modelRoles": {"default": "primary", "review": "reviewer"},
    "ttlSeconds": 900,
}


def write_profile(path: Path, profile: object = PROFILE) -> Path:
    path.write_text(json.dumps(profile))
    return path


def test_resolves_two_arbitrary_environment_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRST_KEY", "first-key")
    monkeypatch.setenv("SECOND_KEY", "second-key")

    loaded = load_solve_profile(write_profile(tmp_path / "profile.json"))

    assert loaded.configuration.model_dump(exclude_none=True) == PROFILE
    assert loaded.resolve_api_keys() == {
        "primary": "first-key",
        "reviewer": "second-key",
    }
    assert loaded.default_model == "model-v9"


def test_loaded_profile_defaults_to_context_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    profile = {key: value for key, value in PROFILE.items() if key != "ttlSeconds"}
    monkeypatch.setenv("FIRST_KEY", "first-key")
    monkeypatch.setenv("SECOND_KEY", "second-key")

    # Act
    loaded = load_solve_profile(write_profile(tmp_path / "profile.json", profile))

    # Assert
    assert loaded.configuration.ttlSeconds == 86400


def test_shared_environment_name_repeats_key_for_each_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = {
        **PROFILE,
        "models": {
            name: {**model, "apiKeyEnv": "SHARED_API_KEY"}
            for name, model in PROFILE["models"].items()
        },
    }
    monkeypatch.setenv("SHARED_API_KEY", "shared-key")

    loaded = load_solve_profile(write_profile(tmp_path / "profile.json", profile))

    assert loaded.resolve_api_keys() == {"primary": "shared-key", "reviewer": "shared-key"}


def test_absent_environment_variable_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRST_KEY", "key")
    monkeypatch.delenv("SECOND_KEY", raising=False)

    with pytest.raises(SolveProfileError, match="SECOND_KEY"):
        load_solve_profile(write_profile(tmp_path / "profile.json")).resolve_api_keys()


@pytest.mark.parametrize("blank_value", ["", "   "])
def test_blank_environment_value_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blank_value: str
) -> None:
    monkeypatch.setenv("FIRST_KEY", blank_value)
    monkeypatch.setenv("SECOND_KEY", "second-key")

    with pytest.raises(SolveProfileError, match="FIRST_KEY"):
        load_solve_profile(write_profile(tmp_path / "profile.json")).resolve_api_keys()


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        ({"models": {}, "modelRoles": {}}, "models"),
        ({"models": PROFILE["models"]}, "modelRoles"),
        ({**PROFILE, "credentials": {}}, "credentials"),
        ({**PROFILE, "ttlSeconds": True}, "ttlSeconds"),
        ({**PROFILE, "modelRoles": {"default": "missing"}}, "modelRoles"),
    ],
)
def test_rejects_invalid_profile_shapes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, profile: object, message: str
) -> None:
    monkeypatch.setenv("FIRST_KEY", "key")
    monkeypatch.setenv("SECOND_KEY", "key")
    with pytest.raises(SolveProfileError, match=message):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


@pytest.mark.parametrize(
    ("models", "message"),
    [
        ({"primary": {"model": "m", "apiKeyEnv": "KEY"}}, "transport"),
        (
            {"primary": {"transport": "unknown", "model": "m", "apiKeyEnv": "KEY"}},
            "transport",
        ),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "apiKeyEnv": "KEY",
                    "extra": "x",
                }
            },
            "extra",
        ),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "apiKeyEnv": "KEY",
                    "options": {"temperature": 1},
                }
            },
            "temperature",
        ),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "apiKeyEnv": "KEY",
                    "options": {"maxOutputTokens": True},
                }
            },
            "maxOutputTokens",
        ),
    ],
)
def test_rejects_invalid_model_definitions(tmp_path: Path, models: object, message: str) -> None:
    profile = {"models": models, "modelRoles": {"default": "primary"}}

    with pytest.raises(SolveProfileError, match=message):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


@pytest.mark.parametrize(
    "model_roles",
    [
        {"review": "primary"},
        {"default": "primary", "review": "missing"},
        {"default": "primary", "review": ""},
    ],
)
def test_rejects_invalid_model_roles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, model_roles: object
) -> None:
    monkeypatch.setenv("FIRST_KEY", "key")
    monkeypatch.setenv("SECOND_KEY", "key")
    profile = {**PROFILE, "modelRoles": model_roles}

    with pytest.raises(SolveProfileError, match="modelRoles"):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


def test_loading_a_profile_does_not_resolve_api_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.delenv("FIRST_KEY", raising=False)
    monkeypatch.delenv("SECOND_KEY", raising=False)

    # Act
    profile = load_solve_profile(write_profile(tmp_path / "profile.json"))

    # Assert
    assert profile.default_model == "model-v9"
    assert profile.configuration.model_dump(exclude_none=True) == PROFILE


@pytest.mark.parametrize(
    ("filename", "transport", "model", "api_key_env"),
    [
        ("anthropic-profile.json", "anthropic", "claude-sonnet-5", "ANTHROPIC_API_KEY"),
        ("openai-profile.json", "openai-responses", "gpt-5.2", "OPENAI_API_KEY"),
    ],
)
def test_standard_profile_is_valid(
    filename: str, transport: str, model: str, api_key_env: str
) -> None:
    # Arrange
    path = Path(__file__).parent.parent / "profiles" / filename
    expected_configuration = {
        "models": {"default": {"transport": transport, "model": model, "apiKeyEnv": api_key_env}},
        "modelRoles": {"default": "default"},
        "ttlSeconds": 86400,
    }

    # Act
    profile = load_solve_profile(path)

    # Assert
    assert profile.configuration.model_dump(exclude_none=True) == expected_configuration
    assert profile.default_model == model
