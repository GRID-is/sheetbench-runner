"""Command-line interface for SheetBench Runner."""

import logging
import sys
from pathlib import Path

import asyncclick as click

from .config import Config
from .dataset import Dataset
from .runner import run

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_task_ids_from_file(file_path: Path) -> set[str]:
    """Load task IDs from a file (one per line, # comments supported)."""
    task_ids: set[str] = set()
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                task_ids.add(line)
    return task_ids


@click.command()
@click.option(
    "--dataset",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to SpreadsheetBench dataset directory (containing dataset.json)",
)
@click.option(
    "--run-dir",
    type=click.Path(path_type=Path),
    required=True,
    help="Directory to store results (creates if missing, resumes if exists)",
)
@click.option(
    "--task-ids",
    type=str,
    default=None,
    help="Comma-separated list of specific task IDs to run",
)
@click.option(
    "--task-file",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="File with task IDs to run (one per line)",
)
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to config.toml file",
)
@click.option(
    "--infuser-url",
    type=str,
    default=None,
    help="Override infuser URL from config",
)
@click.option(
    "--concurrency",
    type=int,
    default=None,
    help="Number of parallel tasks (default: 4)",
)
@click.option(
    "--timeout",
    type=int,
    default=None,
    help="Timeout per task in seconds (default: 3600)",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
async def cli(
    dataset: Path,
    run_dir: Path,
    task_ids: str | None,
    task_file: Path | None,
    config: Path | None,
    infuser_url: str | None,
    concurrency: int | None,
    timeout: int | None,
    verbose: bool,
) -> None:
    """Parallel inference runner for SpreadsheetBench with inline evaluation."""
    setup_logging(verbose)

    # Resolve paths to absolute at CLI boundary
    dataset = dataset.resolve()
    run_dir = run_dir.resolve()

    # Load config
    cfg = Config.load(config)
    cfg = cfg.with_overrides(
        infuser_url=infuser_url,
        concurrency=concurrency,
        timeout_seconds=timeout,
    )

    # Load dataset
    ds = Dataset(dataset)

    # Determine task ID filter
    filter_ids: set[str] | None = None
    if task_ids:
        filter_ids = set(task_ids.split(","))
    elif task_file:
        filter_ids = load_task_ids_from_file(task_file)

    # Filter tasks
    tasks = ds.filter_tasks(task_ids=filter_ids)

    if not tasks:
        logger.error("No tasks to run after filtering")
        sys.exit(1)

    logger.info(f"Selected {len(tasks)} tasks")

    # Run
    stats = await run(
        dataset_path=dataset,
        run_dir_path=run_dir,
        infuser_url=cfg.infuser_url,
        infuser_config=cfg.infuser_config,
        tasks=tasks,
        concurrency=cfg.concurrency,
        timeout_seconds=cfg.timeout_seconds,
    )

    # Print summary
    print("\n" + "=" * 50)
    print("Run Complete")
    print("=" * 50)
    print(f"Total tasks:  {stats.total_tasks}")
    print(f"Skipped:      {stats.skipped} (already completed)")
    print(f"Completed:    {stats.completed}")
    if stats.passed + stats.failed > 0:
        evaluated = stats.passed + stats.failed
        print(f"  Passed:     {stats.passed} ({100*stats.passed/evaluated:.1f}%)")
        print(f"  Failed:     {stats.failed} ({100*stats.failed/evaluated:.1f}%)")
    if stats.errors:
        print(f"Errors:       {stats.errors} (will retry on resume)")
    print(f"\nResults: {run_dir}")


def main() -> None:
    """CLI entry point."""
    cli()


if __name__ == "__main__":
    main()
