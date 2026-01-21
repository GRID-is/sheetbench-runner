"""Task runner with parallel execution."""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .dataset import Dataset
from .entities import RunMetadata, Task, TaskResult, TaskStatus
from .evaluator import Evaluator
from .infuser import InfuserClient, InfuserTransientError
from .prompt import build_prompt
from .run_directory import RunDirectory

logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    """Statistics for a run."""

    total_tasks: int = 0
    completed: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0  # Transient errors (will retry)
    skipped: int = 0  # Already completed in previous run


class TaskRunner:
    """
    Runs SpreadsheetBench tasks with parallel execution.

    Handles:
    - Parallel task execution with configurable concurrency
    - Inline evaluation after each task
    - Progress tracking and reporting
    - Graceful handling of transient errors
    """

    def __init__(
        self,
        infuser: InfuserClient,
        evaluator: Evaluator,
        dataset: Dataset,
        run_dir: RunDirectory,
        concurrency: int = 4,
    ):
        """
        Initialize the task runner.

        Args:
            infuser: Client for the infuser API
            evaluator: Evaluator for comparing outputs to golden files
            dataset: The SpreadsheetBench dataset
            run_dir: Run directory for results
            concurrency: Maximum number of parallel tasks
        """
        self._infuser = infuser
        self._evaluator = evaluator
        self._dataset = dataset
        self._run_dir = run_dir
        self._semaphore = asyncio.Semaphore(concurrency)
        self._stats = RunStats()

    async def run_all(self, tasks: list[Task]) -> RunStats:
        """
        Run all tasks with parallel execution.

        Args:
            tasks: List of tasks to run

        Returns:
            RunStats with completion statistics
        """
        self._stats = RunStats(total_tasks=len(tasks))

        # Split into pending vs already-completed
        pending_tasks = [t for t in tasks if not self._run_dir.is_completed(t.id)]
        self._stats.skipped = len(tasks) - len(pending_tasks)

        if self._stats.skipped > 0:
            logger.info(f"Resuming: {self._stats.skipped} tasks already completed")

        # Run pending tasks in parallel
        if pending_tasks:
            concurrency = self._semaphore._value
            logger.info(f"Running {len(pending_tasks)} tasks with concurrency={concurrency}")
            await asyncio.gather(
                *[self._run_task_safe(task) for task in pending_tasks],
                return_exceptions=False,  # Exceptions are handled in _run_task_safe
            )
        else:
            logger.info("No tasks to run (all already completed)")

        # Accumulate stats uniformly from all tasks
        for task in tasks:
            result = self._run_dir.get_result(task.id)
            if result is None:
                self._stats.errors += 1
            elif result.get("result") == "pass":
                self._stats.passed += 1
                self._stats.completed += 1
            elif result.get("result") == "fail":
                self._stats.failed += 1
                self._stats.completed += 1

        return self._stats

    async def _run_task_safe(self, task: Task) -> TaskResult:
        """
        Run a single task with error handling.

        Catches all exceptions and returns a TaskResult with appropriate status.
        """
        try:
            return await self._run_task(task)
        except Exception as e:
            logger.exception(f"Unexpected error running task {task.id}: {e}")
            return TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                error=f"Unexpected error: {e}",
            )

    async def _run_task(self, task: Task) -> TaskResult:
        """
        Run a single task: call infuser, copy artifacts, evaluate, record.

        Uses semaphore to limit concurrency.
        """
        async with self._semaphore:
            logger.info(f"Starting task {task.id}")
            start_time = time.time()

            result = TaskResult(
                task_id=task.id,
                status=TaskStatus.RUNNING,
                started_at=datetime.now(),
            )

            try:
                # Build prompt
                input_path = self._dataset.get_input_path(task)
                output_path = self._run_dir.get_output_path(task.id)
                prompt = build_prompt(task, input_path, output_path)

                # Call infuser
                response = await self._infuser.solve(prompt)

                duration = time.time() - start_time
                logger.info(f"Task {task.id} infuser completed in {duration:.1f}s")

                # Copy artifacts
                output_file, transcript_file = self._run_dir.copy_artifacts(
                    task.id,
                    response.output_path,
                    response.transcript_path,
                )

                # Update result with infuser response
                result.duration_seconds = round(duration, 1)
                result.turns = response.usage.turns
                result.tool_calls = response.usage.tool_calls
                result.input_tokens = response.usage.input_tokens
                result.output_tokens = response.usage.output_tokens
                result.output_file = output_file
                result.transcript_file = transcript_file
                result.status = TaskStatus.COMPLETED

                # Evaluate
                if not output_file:
                    # No output file = infrastructure failure, not evaluation failure
                    # Don't record - should be retried on resume
                    result.status = TaskStatus.FAILED
                    result.error = "No output file produced"
                    logger.warning(f"Task {task.id}: No output file (will retry)")
                    return result

                # Evaluate
                eval_result = self._evaluator.evaluate(
                    task, self._run_dir.path / output_file
                )
                result.result = "pass" if eval_result.passed else "fail"
                result.message = eval_result.message
                result.status = TaskStatus.EVALUATED

                status_str = "PASS" if eval_result.passed else "FAIL"
                logger.info(f"Task {task.id}: {status_str} ({duration:.1f}s)")

                # Record result - only for actually evaluated tasks
                self._run_dir.record_result(result)
                return result

            except InfuserTransientError as e:
                duration = time.time() - start_time
                logger.warning(f"Task {task.id} transient error after {duration:.1f}s: {e}")
                result.status = TaskStatus.FAILED
                result.error = str(e)
                result.duration_seconds = round(duration, 1)
                # Don't record - should be retried on resume
                return result


async def run(
    dataset_path: Path,
    run_dir_path: Path,
    infuser_url: str,
    infuser_config: dict[str, object],
    tasks: list[Task],
    concurrency: int = 4,
    timeout_seconds: int = 3600,
) -> RunStats:
    """
    High-level function to run tasks.

    Args:
        dataset_path: Path to the SpreadsheetBench dataset
        run_dir_path: Path to the run directory
        infuser_url: URL of the infuser API
        infuser_config: Configuration metadata for run.json
        tasks: Tasks to run
        concurrency: Number of parallel tasks
        timeout_seconds: Timeout per task

    Returns:
        RunStats with completion statistics
    """
    # Set up components
    dataset = Dataset(dataset_path)
    evaluator = Evaluator(dataset_path)
    run_dir = RunDirectory(run_dir_path)

    # Create run.json if missing
    if not run_dir.run_json_path.exists():
        logger.info(f"Creating run metadata at {run_dir_path}")
        # Get model info from infuser
        async with InfuserClient(infuser_url, timeout_seconds) as infuser:
            try:
                status = await infuser.get_status()
                model_val = status.get("default_model", "unknown")
                model = str(model_val) if model_val else "unknown"
                # Merge status info into infuser_config
                config: dict[str, object] = {**infuser_config}
                if "git_hash" not in config:
                    config["git_hash"] = status.get("version", "unknown")
            except Exception as e:
                logger.warning(f"Could not get infuser status: {e}")
                model_from_config = infuser_config.get("model", "unknown")
                model = str(model_from_config) if model_from_config else "unknown"
                config = dict(infuser_config)

        metadata = RunMetadata(
            model=model,
            infuser_config=config,
        )
        run_dir.create(metadata)

    # Always load existing results (for resume)
    run_dir.load()
    if run_dir.get_completed_count() > 0:
        logger.info(f"Resuming: {run_dir.get_completed_count()} tasks already completed")

    # Run tasks
    async with InfuserClient(infuser_url, timeout_seconds) as infuser:
        runner = TaskRunner(
            infuser=infuser,
            evaluator=evaluator,
            dataset=dataset,
            run_dir=run_dir,
            concurrency=concurrency,
        )
        stats = await runner.run_all(tasks)

    return stats
