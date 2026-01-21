"""Command-line interface for SheetBench Runner."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

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


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Parallel inference runner for SpreadsheetBench with inline evaluation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run test set 1 into a new folder
    sheetbench-runner --dataset ~/data/spreadsheetbench_verified_400 \\
                      --run-dir ~/runs/my-run \\
                      --test-set 1

    # Resume a run (skips already-completed tasks)
    sheetbench-runner --dataset ~/data/spreadsheetbench_verified_400 \\
                      --run-dir ~/runs/my-run

    # Run specific tasks
    sheetbench-runner --dataset ~/data/spreadsheetbench_verified_400 \\
                      --run-dir ~/runs/my-run \\
                      --task-ids 13-1,17-35,22-47

    # Run all non-VBA tasks with 8 workers
    sheetbench-runner --dataset ~/data/spreadsheetbench_verified_400 \\
                      --run-dir ~/runs/my-run \\
                      --concurrency 8
        """,
    )

    # Required arguments
    parser.add_argument(
        "--dataset",
        type=Path,
        required=True,
        help="Path to the SpreadsheetBench dataset directory (containing dataset.json)",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Directory to store results (creates if missing, resumes if exists)",
    )

    # Task filtering
    task_group = parser.add_mutually_exclusive_group()
    task_group.add_argument(
        "--test-set",
        type=int,
        choices=[1, 2],
        help="Run predefined test set 1 or 2 (50 tasks each)",
    )
    task_group.add_argument(
        "--task-ids",
        type=str,
        help="Comma-separated list of specific task IDs to run",
    )

    parser.add_argument(
        "--no-exclude",
        action="store_true",
        help="Don't exclude VBA tasks (by default, excluded_vba_tasks.txt is used)",
    )
    parser.add_argument(
        "--exclude-file",
        type=Path,
        help="Custom file with task IDs to exclude",
    )

    # Configuration
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to config.toml file",
    )
    parser.add_argument(
        "--infuser-url",
        type=str,
        help="Override infuser URL from config (default: http://localhost:3000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        help="Number of parallel tasks (default: 4)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="Timeout per task in seconds (default: 3600)",
    )

    # Misc
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    """Main async entry point."""
    # Load config
    config = Config.load(args.config)
    config = config.with_overrides(
        infuser_url=args.infuser_url,
        concurrency=args.concurrency,
        timeout_seconds=args.timeout,
    )

    # Load dataset
    if not args.dataset.exists():
        logger.error(f"Dataset not found: {args.dataset}")
        return 1

    dataset = Dataset(args.dataset)

    # Determine exclusions
    if args.no_exclude:
        exclude_ids = set()
    else:
        exclude_ids = dataset.get_excluded_task_ids(args.exclude_file)
        if exclude_ids:
            logger.info(f"Excluding {len(exclude_ids)} tasks")

    # Determine task IDs
    task_ids = None
    if args.task_ids:
        task_ids = set(args.task_ids.split(","))

    # Filter tasks
    tasks = dataset.filter_tasks(
        task_ids=task_ids,
        test_set=args.test_set,
        exclude_ids=exclude_ids,
    )

    if not tasks:
        logger.error("No tasks to run after filtering")
        return 1

    logger.info(f"Selected {len(tasks)} tasks")

    # Run
    stats = await run(
        dataset_path=args.dataset,
        run_dir_path=args.run_dir,
        infuser_url=config.infuser_url,
        infuser_config=config.infuser_config,
        tasks=tasks,
        concurrency=config.concurrency,
        timeout_seconds=config.timeout_seconds,
    )

    # Print summary
    print("\n" + "=" * 50)
    print("Run Complete")
    print("=" * 50)
    print(f"Total tasks:  {stats.total_tasks}")
    print(f"Skipped:      {stats.skipped} (already completed)")
    print(f"Completed:    {stats.completed}")
    if stats.completed > stats.skipped:
        evaluated = stats.completed - stats.skipped
        print(f"  Passed:     {stats.passed} ({100*stats.passed/evaluated:.1f}%)")
        print(f"  Failed:     {stats.failed} ({100*stats.failed/evaluated:.1f}%)")
    if stats.errors:
        print(f"Errors:       {stats.errors} (will retry on resume)")
    print(f"\nResults: {args.run_dir}")

    return 0


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    try:
        exit_code = asyncio.run(async_main(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
