"""Configuration loading for SheetBench Runner."""

import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@dataclass(frozen=True)
class Config:
    """Configuration for SheetBench Runner."""

    solve_server_url: str = "http://localhost:3000"
    solve_profile: Path | None = None
    concurrency: int = 4
    timeout_seconds: int = 3600  # 1 hour per task

    @classmethod
    def load(cls, path: Path | None = None) -> "Config":
        """
        Load configuration from a TOML file.

        If path is None, returns default configuration.
        """
        if path is None or not path.exists():
            return cls()

        with open(path, "rb") as f:
            data = tomllib.load(f)

        runner = data.get("runner", {})
        solve = data.get("solve", {})
        profile_value = solve.get("profile")
        solve_profile = Path(profile_value) if isinstance(profile_value, str) else None
        if solve_profile is not None and not solve_profile.is_absolute():
            solve_profile = path.parent / solve_profile
        return cls(
            solve_server_url=solve.get("url", cls.solve_server_url),
            solve_profile=solve_profile,
            concurrency=runner.get("concurrency", cls.concurrency),
            timeout_seconds=runner.get("timeout_seconds", cls.timeout_seconds),
        )

    def with_overrides(
        self,
        solve_server_url: str | None = None,
        solve_profile: Path | None = None,
        concurrency: int | None = None,
        timeout_seconds: int | None = None,
    ) -> "Config":
        """Create a new Config with CLI overrides applied."""
        return Config(
            solve_server_url=solve_server_url
            if solve_server_url is not None
            else self.solve_server_url,
            solve_profile=solve_profile if solve_profile is not None else self.solve_profile,
            concurrency=concurrency if concurrency is not None else self.concurrency,
            timeout_seconds=(
                timeout_seconds if timeout_seconds is not None else self.timeout_seconds
            ),
        )
