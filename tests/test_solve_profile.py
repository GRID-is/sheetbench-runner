"""Tests for non-secret solve profile loading and environment resolution."""

import json
from pathlib import Path
from typing import Any

import pytest

from sheetbench_runner.solve_profile import (
    SolveProfileError,
    load_solve_profile,
    validate_solve_configuration,
)

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


def profile_with_counts(*, models: int = 1, roles: int = 1) -> dict[str, Any]:
    configured_models = {
        f"model-{index}": {
            "transport": "anthropic",
            "model": "m",
            "apiKeyEnv": f"KEY_{index}",
        }
        for index in range(models)
    }
    model_roles = {"default": "model-0"}
    model_roles.update({f"role-{index}": "model-0" for index in range(1, roles)})
    return {"models": configured_models, "modelRoles": model_roles}


def test_resolves_two_arbitrary_environment_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRST_KEY", "first-secret")
    monkeypatch.setenv("SECOND_KEY", "second-secret")

    loaded = load_solve_profile(write_profile(tmp_path / "profile.json"))

    assert loaded.configuration == PROFILE
    assert loaded.resolve_api_keys() == {
        "primary": "first-secret",
        "reviewer": "second-secret",
    }
    assert loaded.default_model == "model-v9"


def test_loaded_profile_defaults_to_maximum_context_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    profile = {key: value for key, value in PROFILE.items() if key != "ttlSeconds"}
    monkeypatch.setenv("FIRST_KEY", "first-secret")
    monkeypatch.setenv("SECOND_KEY", "second-secret")

    # Act
    loaded = load_solve_profile(write_profile(tmp_path / "profile.json", profile))

    # Assert
    assert loaded.configuration["ttlSeconds"] == 86400


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
    monkeypatch.setenv("SHARED_API_KEY", "shared-secret")

    loaded = load_solve_profile(write_profile(tmp_path / "profile.json", profile))

    assert loaded.resolve_api_keys() == {"primary": "shared-secret", "reviewer": "shared-secret"}
    assert "shared-secret" not in repr(loaded)


def test_absent_environment_variable_never_exposes_another_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRST_KEY", "must-not-appear")
    monkeypatch.delenv("SECOND_KEY", raising=False)

    with pytest.raises(SolveProfileError) as exc_info:
        load_solve_profile(write_profile(tmp_path / "profile.json")).resolve_api_keys()

    message = str(exc_info.value)
    assert "SECOND_KEY" in message
    assert "must-not-appear" not in message


@pytest.mark.parametrize("blank_value", ["", "   "])
def test_blank_environment_value_is_rejected_without_exposing_other_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blank_value: str
) -> None:
    monkeypatch.setenv("FIRST_KEY", blank_value)
    monkeypatch.setenv("SECOND_KEY", "second-secret")

    with pytest.raises(SolveProfileError, match="FIRST_KEY") as exc_info:
        load_solve_profile(write_profile(tmp_path / "profile.json")).resolve_api_keys()

    assert "second-secret" not in str(exc_info.value)


def test_api_key_utf8_byte_limit_is_enforced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRST_KEY", "é" * 2049)
    monkeypatch.setenv("SECOND_KEY", "safe")

    with pytest.raises(SolveProfileError, match="4096 UTF-8 bytes") as exc_info:
        load_solve_profile(write_profile(tmp_path / "profile.json")).resolve_api_keys()

    assert "é" not in str(exc_info.value)


def test_repeated_per_model_keys_count_toward_aggregate_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = profile_with_counts(models=17)
    for model in profile["models"].values():
        model["apiKeyEnv"] = "SHARED_API_KEY"
    monkeypatch.setenv("SHARED_API_KEY", "k" * 4096)

    with pytest.raises(SolveProfileError, match="65536 aggregate UTF-8 bytes"):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile)).resolve_api_keys()


def test_api_key_byte_caps_are_inclusive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    profile = profile_with_counts(models=16)
    for model in profile["models"].values():
        model["apiKeyEnv"] = "SHARED_API_KEY"
    monkeypatch.setenv("SHARED_API_KEY", "k" * 4096)

    loaded = load_solve_profile(write_profile(tmp_path / "profile.json", profile))

    assert len(loaded.resolve_api_keys()) == 16


@pytest.mark.parametrize(
    "apiKeyEnv",
    ["", "   ", "1KEY", "KEY-NAME", "KEY.NAME", "KEY NAME", "ÜNICODE"],
)
def test_malformed_environment_identifier_is_rejected(tmp_path: Path, apiKeyEnv: str) -> None:
    profile = {
        "models": {
            "primary": {
                "transport": "anthropic",
                "model": "m",
                "apiKeyEnv": apiKeyEnv,
            }
        },
        "modelRoles": {"default": "primary"},
    }

    with pytest.raises(SolveProfileError, match="environment variable"):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


@pytest.mark.parametrize("count", [32])
def test_accepts_server_collection_caps(count: int) -> None:
    validate_solve_configuration(profile_with_counts(models=count, roles=count))


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        (profile_with_counts(models=33), "models"),
        (profile_with_counts(roles=33), "modelRoles"),
        (profile_with_counts(models=33), "models"),
    ],
)
def test_rejects_values_above_server_collection_caps(profile: dict[str, Any], message: str) -> None:
    with pytest.raises(SolveProfileError, match=message):
        validate_solve_configuration(profile)


@pytest.mark.parametrize("length", [1, 64])
def test_accepts_server_dictionary_name_boundaries(length: int) -> None:
    name = "n" * length
    validate_solve_configuration(
        {
            "models": {name: {"transport": "anthropic", "model": "m", "apiKeyEnv": "KEY"}},
            "modelRoles": {"default": name, "r" * length: name},
        }
    )


@pytest.mark.parametrize("field", ["model", "role", "apiKeyEnv"])
def test_rejects_dictionary_names_over_server_cap(field: str) -> None:
    long_name = "n" * 65
    profile: dict[str, Any] = {
        "models": {"primary": {"transport": "anthropic", "model": "m", "apiKeyEnv": "KEY"}},
        "modelRoles": {"default": "primary"},
    }
    if field == "model":
        profile = {
            "models": {long_name: {"transport": "anthropic", "model": "m", "apiKeyEnv": "KEY"}},
            "modelRoles": {"default": long_name},
        }
    elif field == "role":
        profile["modelRoles"] = {"default": "primary", long_name: "primary"}
    else:
        profile["models"]["primary"]["apiKeyEnv"] = "K" * 65
    with pytest.raises(SolveProfileError):
        validate_solve_configuration(profile)


@pytest.mark.parametrize("length", [1, 256])
def test_accepts_server_model_id_boundaries(length: int) -> None:
    profile = profile_with_counts()
    profile["models"]["model-0"]["model"] = "m" * length
    validate_solve_configuration(profile)


def test_rejects_model_id_over_server_cap() -> None:
    profile = profile_with_counts()
    profile["models"]["model-0"]["model"] = "m" * 257
    with pytest.raises(SolveProfileError, match="model"):
        validate_solve_configuration(profile)


@pytest.mark.parametrize("value", [1, 1_000_000])
def test_accepts_server_max_output_token_boundaries(value: int) -> None:
    profile = profile_with_counts()
    profile["models"]["model-0"]["options"] = {"maxOutputTokens": value}
    validate_solve_configuration(profile)


def test_rejects_max_output_tokens_over_server_cap() -> None:
    profile = profile_with_counts()
    profile["models"]["model-0"]["options"] = {"maxOutputTokens": 1_000_001}
    with pytest.raises(SolveProfileError, match="maxOutputTokens"):
        validate_solve_configuration(profile)


def test_accepts_server_whitespace_names_and_model_id() -> None:
    validate_solve_configuration(
        {
            "models": {" ": {"transport": "anthropic", "model": " ", "apiKeyEnv": "KEY"}},
            "modelRoles": {"default": " ", " ": " "},
        }
    )


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        ({"models": {}, "modelRoles": {}}, "models"),
        ({"models": PROFILE["models"]}, "modelRoles"),
        ({**PROFILE, "credentials": {}}, "credentials"),
        ({**PROFILE, "apiKey": "not-allowed"}, "apiKey"),
        (
            {
                **PROFILE,
                "models": {"primary": {**PROFILE["models"]["primary"], "credential": "KEY"}},
            },
            "credential",
        ),
        ({**PROFILE, "ttlSeconds": 0}, "ttlSeconds"),
        ({**PROFILE, "ttlSeconds": True}, "ttlSeconds"),
        ({**PROFILE, "ttlSeconds": 86401}, "ttlSeconds"),
        ({**PROFILE, "modelRoles": {"default": "missing"}}, "missing"),
    ],
)
def test_rejects_unsafe_or_invalid_profile_shapes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, profile: object, message: str
) -> None:
    monkeypatch.setenv("FIRST_KEY", "secret")
    monkeypatch.setenv("SECOND_KEY", "secret")
    with pytest.raises(SolveProfileError, match=message):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


@pytest.mark.parametrize(
    ("models", "message"),
    [
        ({"": PROFILE["models"]["primary"]}, "1..64"),
        ({"primary": {"model": "m", "apiKeyEnv": "KEY"}}, "transport"),
        (
            {"primary": {"transport": "unknown", "model": "m", "apiKeyEnv": "KEY"}},
            "transport",
        ),
        ({"primary": {"transport": "anthropic", "model": "", "apiKeyEnv": "KEY"}}, "model"),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "apiKeyEnv": "KEY",
                    "extra": "unsafe",
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
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "apiKeyEnv": "KEY",
                    "options": {"maxOutputTokens": 0},
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
        {"": "primary"},
        {"default": "primary", "review": "missing"},
        {"default": "primary", "review": ""},
    ],
)
def test_rejects_invalid_model_roles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, model_roles: object
) -> None:
    monkeypatch.setenv("FIRST_KEY", "secret")
    monkeypatch.setenv("SECOND_KEY", "secret")
    profile = {**PROFILE, "modelRoles": model_roles}

    with pytest.raises(SolveProfileError, match="modelRoles"):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


def test_loading_a_profile_does_not_resolve_api_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.delenv("FIRST_KEY", raising=False)
    monkeypatch.delenv("SECOND_KEY", raising=False)
    expected_sanitized = {
        "models": {
            "primary": {"transport": "anthropic", "model": "model-v9"},
            "reviewer": {"transport": "openai-responses", "model": "review-model"},
        },
        "modelRoles": {"default": "primary", "review": "reviewer"},
        "ttlSeconds": 900,
    }

    # Act
    profile = load_solve_profile(write_profile(tmp_path / "profile.json"))

    # Assert
    assert profile.default_model == "model-v9"
    assert profile.sanitized_configuration == expected_sanitized


@pytest.mark.parametrize(
    ("filename", "transport", "model", "api_key_env"),
    [
        ("anthropic-profile.json", "anthropic", "claude-sonnet-5", "ANTHROPIC_API_KEY"),
        ("openai-profile.json", "openai-responses", "gpt-5.2", "OPENAI_API_KEY"),
    ],
)
def test_standard_profile_is_valid_and_non_secret(
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
    assert profile.configuration == expected_configuration
    assert profile.default_model == model
    assert profile.sanitized_configuration["models"]["default"] == {
        "transport": transport,
        "model": model,
    }
