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
            "credential": "FIRST_KEY",
        },
        "reviewer": {
            "transport": "openai-responses",
            "model": "review-model",
            "credential": "SECOND_KEY",
        },
    },
    "modelRoles": {"default": "primary", "review": "reviewer"},
    "ttlSeconds": 900,
}


def write_profile(path: Path, profile: object = PROFILE) -> Path:
    path.write_text(json.dumps(profile))
    return path


def profile_with_counts(
    *, models: int = 1, roles: int = 1, credentials: int | None = None
) -> dict[str, Any]:
    credential_count = credentials if credentials is not None else models
    configured_models = {
        f"model-{index}": {
            "transport": "anthropic",
            "model": "m",
            "credential": f"KEY_{index % credential_count}",
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
    assert loaded.credentials == {
        "FIRST_KEY": "first-secret",
        "SECOND_KEY": "second-secret",
    }
    assert loaded.default_model == "model-v9"


def test_shared_environment_name_produces_one_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = {
        **PROFILE,
        "models": {
            name: {**model, "credential": "SHARED_API_KEY"}
            for name, model in PROFILE["models"].items()
        },
    }
    monkeypatch.setenv("SHARED_API_KEY", "shared-secret")

    loaded = load_solve_profile(write_profile(tmp_path / "profile.json", profile))

    assert loaded.credentials == {"SHARED_API_KEY": "shared-secret"}


def test_absent_environment_variable_never_exposes_another_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRST_KEY", "must-not-appear")
    monkeypatch.delenv("SECOND_KEY", raising=False)

    with pytest.raises(SolveProfileError) as exc_info:
        load_solve_profile(write_profile(tmp_path / "profile.json"))

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
        load_solve_profile(write_profile(tmp_path / "profile.json"))

    assert "second-secret" not in str(exc_info.value)


@pytest.mark.parametrize(
    "credential",
    ["", "   ", "1KEY", "KEY-NAME", "KEY.NAME", "KEY NAME", "ÜNICODE"],
)
def test_malformed_environment_identifier_is_rejected(tmp_path: Path, credential: str) -> None:
    profile = {
        "models": {
            "primary": {
                "transport": "anthropic",
                "model": "m",
                "credential": credential,
            }
        },
        "modelRoles": {"default": "primary"},
    }

    with pytest.raises(SolveProfileError, match="environment variable"):
        load_solve_profile(write_profile(tmp_path / "profile.json", profile))


@pytest.mark.parametrize("count", [32])
def test_accepts_server_collection_caps(count: int) -> None:
    validate_solve_configuration(profile_with_counts(models=count, roles=count, credentials=count))


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        (profile_with_counts(models=33), "models"),
        (profile_with_counts(roles=33), "modelRoles"),
        (profile_with_counts(models=33, credentials=33), "models"),
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
            "models": {name: {"transport": "anthropic", "model": "m", "credential": "KEY"}},
            "modelRoles": {"default": name, "r" * length: name},
        }
    )


@pytest.mark.parametrize("field", ["model", "role", "credential"])
def test_rejects_dictionary_names_over_server_cap(field: str) -> None:
    long_name = "n" * 65
    profile: dict[str, Any] = {
        "models": {"primary": {"transport": "anthropic", "model": "m", "credential": "KEY"}},
        "modelRoles": {"default": "primary"},
    }
    if field == "model":
        profile = {
            "models": {long_name: {"transport": "anthropic", "model": "m", "credential": "KEY"}},
            "modelRoles": {"default": long_name},
        }
    elif field == "role":
        profile["modelRoles"] = {"default": "primary", long_name: "primary"}
    else:
        profile["models"]["primary"]["credential"] = "K" * 65
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
            "models": {" ": {"transport": "anthropic", "model": " ", "credential": "KEY"}},
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
        ({"primary": {"model": "m", "credential": "KEY"}}, "transport"),
        (
            {"primary": {"transport": "unknown", "model": "m", "credential": "KEY"}},
            "transport",
        ),
        ({"primary": {"transport": "anthropic", "model": "", "credential": "KEY"}}, "model"),
        (
            {
                "primary": {
                    "transport": "anthropic",
                    "model": "m",
                    "credential": "KEY",
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
                    "credential": "KEY",
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
                    "credential": "KEY",
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
                    "credential": "KEY",
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
