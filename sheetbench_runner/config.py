"""Configuration loading for SheetBench Runner."""

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@dataclass(frozen=True)
class Config:
    """Configuration for SheetBench Runner."""

    infuser_url: str = "http://localhost:3000"
    model: str | None = None  # Model override (e.g., "openai/gpt-4o")
    infuser_config: dict[str, Any] = field(default_factory=dict)
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

        infuser = data.get("infuser", {})
        runner = data.get("runner", {})

        return cls(
            infuser_url=infuser.get("url", cls.infuser_url),
            model=infuser.get("model"),
            infuser_config=infuser.get("config", {}),
            concurrency=runner.get("concurrency", cls.concurrency),
            timeout_seconds=runner.get("timeout_seconds", cls.timeout_seconds),
        )

    def with_overrides(
        self,
        infuser_url: str | None = None,
        model: str | None = None,
        concurrency: int | None = None,
        timeout_seconds: int | None = None,
    ) -> "Config":
        """Create a new Config with CLI overrides applied."""
        return Config(
            infuser_url=infuser_url if infuser_url is not None else self.infuser_url,
            model=model if model is not None else self.model,
            infuser_config=self.infuser_config,
            concurrency=concurrency if concurrency is not None else self.concurrency,
            timeout_seconds=(
                timeout_seconds if timeout_seconds is not None else self.timeout_seconds
            ),
        )
