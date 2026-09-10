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
            "apiKeyEnv": "FIRST_KEY",
            "request": {"model": "model-v9", "max_tokens": 4096, "thinking": {"type": "adaptive"}},
        },
        "reviewer": {
            "transport": "openai-responses",
            "apiKeyEnv": "SECOND_KEY",
            "request": {"model": "review-model", "reasoning": {"effort": "low"}},
        },
    },
    "modelRoles": {"default": "primary", "review": "reviewer"},
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
        ({"primary": {"apiKeyEnv": "KEY", "request": {"model": "m"}}}, "transport"),
        (
            {"primary": {"transport": "unknown", "apiKeyEnv": "KEY", "request": {"model": "m"}}},
            "transport",
        ),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "apiKeyEnv": "KEY",
                    "request": {"model": "m"},
                    "extra": "x",
                }
            },
            "extra",
        ),
        ({"primary": {"transport": "anthropic", "model": "m", "apiKeyEnv": "KEY"}}, "model"),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "apiKeyEnv": "KEY",
                    "request": {"model": "m"},
                    "options": {"maxOutputTokens": 1},
                }
            },
            "options",
        ),
        ({"primary": {"transport": "anthropic", "apiKeyEnv": "KEY"}}, "request"),
        ({"primary": {"transport": "anthropic", "apiKeyEnv": "KEY", "request": "m"}}, "request"),
        ({"primary": {"transport": "anthropic", "apiKeyEnv": "KEY", "request": {}}}, "model"),
        (
            {"primary": {"transport": "anthropic", "apiKeyEnv": "KEY", "request": {"model": ""}}},
            "model",
        ),
        (
            {"primary": {"transport": "anthropic", "apiKeyEnv": "KEY", "request": {"model": 1}}},
            "model",
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
    ("filename", "transport", "request_body", "api_key_env"),
    [
        (
            "anthropic-profile.json",
            "anthropic",
            {
                "model": "claude-sonnet-5",
                "max_tokens": 16000,
                "thinking": {"type": "adaptive", "display": "summarized"},
            },
            "ANTHROPIC_API_KEY",
        ),
        (
            "openai-profile.json",
            "openai-responses",
            {
                "model": "gpt-5.2",
                "max_output_tokens": 16000,
                "reasoning": {"effort": "medium", "summary": "auto"},
            },
            "OPENAI_API_KEY",
        ),
    ],
)
def test_standard_profile_is_valid(
    filename: str, transport: str, request_body: dict[str, object], api_key_env: str
) -> None:
    # Arrange
    path = Path(__file__).parent.parent / "profiles" / filename
    expected_configuration = {
        "models": {
            "default": {"transport": transport, "apiKeyEnv": api_key_env, "request": request_body}
        },
        "modelRoles": {"default": "default"},
    }

    # Act
    profile = load_solve_profile(path)

    # Assert
    assert profile.configuration.model_dump(exclude_none=True) == expected_configuration
    assert profile.default_model == request_body["model"]
