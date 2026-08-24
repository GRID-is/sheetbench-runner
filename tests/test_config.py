"""Tests for solve profile configuration and CLI options."""

from pathlib import Path

from asyncclick.testing import CliRunner

from sheetbench_runner.cli import cli
from sheetbench_runner.config import Config


def test_config_loads_solve_profile_without_secret_mapping(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[solve]
profile = "profiles/solve.json"

[infuser]
url = "http://example.test"
"""
    )

    config = Config.load(config_path)

    assert config.solve_profile == tmp_path / "profiles/solve.json"
    assert not hasattr(config, "solve_secrets")
    assert not hasattr(config, "model")


async def test_cli_help_exposes_only_solve_profile_option() -> None:
    result = await CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "--solve-profile" in result.output
    assert "--solve-secret" not in result.output
    assert "--model" not in result.output
