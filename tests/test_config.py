"""Tests for solve profile configuration and CLI options."""

from pathlib import Path

from asyncclick.testing import CliRunner

from sheetbench_runner.cli import cli
from sheetbench_runner.config import Config


def test_config_loads_solve_profile(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[solve]
url = "http://example.test"
profile = "profiles/solve.json"
"""
    )

    config = Config.load(config_path)

    assert config.solve_server_url == "http://example.test"
    assert config.solve_profile == tmp_path / "profiles/solve.json"


async def test_cli_help_exposes_solve_profile_option() -> None:
    result = await CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "--solve-profile" in result.output
