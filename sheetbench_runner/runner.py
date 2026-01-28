"""Task runner with parallel execution."""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Union

from rich.console import Console, Group
from rich.live import Live
from rich.progress import BarColumn, Progress, TaskID, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

from .dataset import Dataset
from .entities import RunMetadata, Task, TaskResult, TaskStatus
from .evaluator import Evaluator
from .infuser import InfuserClient, InfuserTransientError
from .new_infuser import NewInfuserClient
from .new_prompt import build_prompt as build_new_prompt
from .prompt import build_prompt
from .run_directory import RunDirectory

logger = logging.getLogger(__name__)
console = Console()


@dataclass
class RunStats:
    """Statistics for a run."""

    total_tasks: int = 0
    completed: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0  # Transient errors (will retry)
    skipped: int = 0  # Already completed in previous run
    running_tasks: set[str] = field(default_factory=set)

    @property
    def pass_rate(self) -> float:
        """Current pass rate as percentage."""
        evaluated = self.passed + self.failed
        return (100 * self.passed / evaluated) if evaluated > 0 else 0.0


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
        infuser: Union[InfuserClient, NewInfuserClient],
        evaluator: Evaluator,
        dataset: Dataset,
        run_dir: RunDirectory,
        concurrency: int = 4,
        use_new_endpoint: bool = False,
    ):
        """
        Initialize the task runner.

        Args:
            infuser: Client for the infuser API
            evaluator: Evaluator for comparing outputs to golden files
            dataset: The SpreadsheetBench dataset
            run_dir: Run directory for results
            concurrency: Maximum number of parallel tasks
            use_new_endpoint: Use new /solve endpoint instead of /v1/chat/completions
        """
        self._infuser = infuser
        self._evaluator = evaluator
        self._dataset = dataset
        self._run_dir = run_dir
        self._semaphore = asyncio.Semaphore(concurrency)
        self._stats = RunStats()
        self._use_new_endpoint = use_new_endpoint
        self._progress: Progress | None = None
        self._progress_task: TaskID | None = None
        self._live: Live | None = None

    def _build_display(self) -> Group:
        """Build the rich display with progress bar and running tasks."""
        # Progress bar
        progress_table = self._progress.get_renderable() if self._progress else Text("")

        # Stats line
        stats = self._stats
        evaluated = stats.passed + stats.failed
        if evaluated > 0:
            stats_text = Text()
            stats_text.append(f"Pass: {stats.passed}", style="green")
            stats_text.append(f"  Fail: {stats.failed}", style="red")
            stats_text.append(f"  Rate: {stats.pass_rate:.1f}%", style="cyan bold")
            if stats.errors > 0:
                stats_text.append(f"  Errors: {stats.errors}", style="yellow")
        else:
            stats_text = Text("Waiting for first result...", style="dim")

        # Running tasks table
        running_table = Table.grid(padding=(0, 2))
        running_table.add_column("status", width=3)
        running_table.add_column("task_id")

        running_list = sorted(stats.running_tasks)
        for task_id in running_list[:8]:  # Show max 8 running tasks
            running_table.add_row("⚙", task_id)
        if len(running_list) > 8:
            running_table.add_row("", f"... and {len(running_list) - 8} more")

        return Group(progress_table, stats_text, running_table)

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

        # Run pending tasks in parallel with live progress display
        if pending_tasks:
            concurrency = self._semaphore._value
            logger.info(f"Running {len(pending_tasks)} tasks with concurrency={concurrency}")

            # Set up progress display
            self._progress = Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                console=console,
                transient=False,
            )
            self._progress_task = self._progress.add_task(
                "Progress", total=len(pending_tasks)
            )

            with Live(self._build_display(), console=console, refresh_per_second=4) as live:
                self._live = live
                await asyncio.gather(
                    *[self._run_task_safe(task) for task in pending_tasks],
                    return_exceptions=False,  # Exceptions are handled in _run_task_safe
                )
                self._live = None
        else:
            logger.info("No tasks to run (all already completed)")

        # Accumulate stats for skipped tasks (already completed in previous runs)
        # Pending task stats are tracked inline during execution
        skipped_task_ids = {t.id for t in tasks} - {t.id for t in pending_tasks}
        for task in tasks:
            if task.id in skipped_task_ids:
                result = self._run_dir.get_result(task.id)
                if result is not None:
                    if result.get("result") == "pass":
                        self._stats.passed += 1
                    elif result.get("result") == "fail":
                        self._stats.failed += 1

        # Count completed = passed + failed
        self._stats.completed = self._stats.passed + self._stats.failed

        # Count errors = tasks that should have results but don't
        for task in pending_tasks:
            if self._run_dir.get_result(task.id) is None:
                self._stats.errors += 1

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

    def _update_display(self) -> None:
        """Update the live display."""
        if self._live:
            self._live.update(self._build_display())

    def _task_completed(self, task_id: str, passed: bool | None) -> None:
        """Update stats and display when a task completes."""
        self._stats.running_tasks.discard(task_id)
        if passed is True:
            self._stats.passed += 1
        elif passed is False:
            self._stats.failed += 1
        # Update progress bar
        if self._progress and self._progress_task is not None:
            self._progress.advance(self._progress_task)
        self._update_display()

    async def _run_task(self, task: Task) -> TaskResult:
        """
        Run a single task: call infuser, copy artifacts, evaluate, record.

        Uses semaphore to limit concurrency.
        """
        async with self._semaphore:
            self._stats.running_tasks.add(task.id)
            self._update_display()
            logger.debug(f"Starting task {task.id}")
            start_time = time.time()

            result = TaskResult(
                task_id=task.id,
                status=TaskStatus.RUNNING,
                started_at=datetime.now(),
            )

            try:
                if self._use_new_endpoint:
                    # New flow: upload workbook, call /solve, get inline response
                    output_file, transcript_file = await self._run_task_new_endpoint(
                        task, result
                    )
                else:
                    # Old flow: send paths in prompt, copy files from returned paths
                    output_file, transcript_file = await self._run_task_old_endpoint(
                        task, result
                    )

                duration = time.time() - start_time
                result.duration_seconds = round(duration, 1)
                result.output_file = output_file
                result.transcript_file = transcript_file
                result.status = TaskStatus.COMPLETED

                # Evaluate
                if output_file is None:
                    # No output file = infrastructure failure, not evaluation failure
                    # Don't record - should be retried on resume
                    result.status = TaskStatus.FAILED
                    result.error = "No output file produced"
                    logger.warning(f"Task {task.id}: No output file (will retry)")
                    self._task_completed(task.id, passed=None)
                    return result

                # Evaluate
                eval_result = self._evaluator.evaluate(
                    task, self._run_dir.path / output_file
                )
                result.result = "pass" if eval_result.passed else "fail"
                result.message = eval_result.message
                result.status = TaskStatus.EVALUATED

                status_str = "PASS" if eval_result.passed else "FAIL"
                logger.debug(f"Task {task.id}: {status_str} ({duration:.1f}s)")

                # Record result - only for actually evaluated tasks
                self._run_dir.record_result(result)
                self._task_completed(task.id, passed=eval_result.passed)
                return result

            except InfuserTransientError as e:
                duration = time.time() - start_time
                logger.warning(f"Task {task.id} transient error after {duration:.1f}s: {e}")
                result.status = TaskStatus.FAILED
                result.error = str(e)
                result.duration_seconds = round(duration, 1)
                # Don't record - should be retried on resume
                self._task_completed(task.id, passed=None)
                return result

    async def _run_task_old_endpoint(
        self, task: Task, result: TaskResult
    ) -> tuple[str | None, str | None]:
        """Run task using old /v1/chat/completions endpoint with filesystem paths."""
        assert isinstance(self._infuser, InfuserClient)

        # Build prompt with filesystem paths
        input_path = self._dataset.get_input_path(task)
        output_path = self._run_dir.get_output_path(task.id)
        prompt = build_prompt(task, input_path, output_path)

        # Call infuser
        response = await self._infuser.solve(prompt)

        logger.info(f"Task {task.id} infuser completed")

        # Copy artifacts from server filesystem paths
        output_file, transcript_file = self._run_dir.copy_artifacts(
            task.id,
            response.output_path,
            response.transcript_path,
        )

        # Update result with usage stats
        result.turns = response.usage.turns
        result.tool_calls = response.usage.tool_calls
        result.input_tokens = response.usage.input_tokens
        result.output_tokens = response.usage.output_tokens

        return output_file, transcript_file

    async def _run_task_new_endpoint(
        self, task: Task, result: TaskResult
    ) -> tuple[str | None, str | None]:
        """Run task using new /solve endpoint with workbook upload."""
        assert isinstance(self._infuser, NewInfuserClient)

        # Upload workbook
        input_path = self._dataset.get_input_path(task)
        workbook_id = await self._infuser.upload_workbook(input_path)
        logger.info(f"Task {task.id} uploaded workbook as {workbook_id}")

        # Build prompt with workbook_id
        prompt = build_new_prompt(task, workbook_id)

        # Call /solve
        response = await self._infuser.solve(workbook_id, prompt)

        logger.info(f"Task {task.id} solve completed")

        # Write artifacts directly from response
        output_file: str | None = None
        transcript_file: str | None = None

        if response.output_xlsx:
            output_file = f"{task.id}-output.xlsx"
            output_path = self._run_dir.path / output_file
            output_path.write_bytes(response.output_xlsx)

        if response.transcript:
            transcript_file = f"{task.id}-transcript.json"
            transcript_path = self._run_dir.path / transcript_file
            transcript_path.write_text(json.dumps(response.transcript, indent=2))

        # Update result with usage stats
        result.turns = response.usage.turns
        result.tool_calls = response.usage.tool_calls
        result.input_tokens = response.usage.input_tokens
        result.output_tokens = response.usage.output_tokens

        return output_file, transcript_file


async def run(
    dataset_path: Path,
    run_dir_path: Path,
    infuser_url: str,
    infuser_config: dict[str, object],
    tasks: list[Task],
    concurrency: int = 4,
    timeout_seconds: int = 3600,
    reevaluate: bool = False,
    use_new_endpoint: bool = False,
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
        reevaluate: Re-evaluate tasks with existing output files
        use_new_endpoint: Use new /solve endpoint instead of /v1/chat/completions

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
        # Get status from infuser - all fields go into infuser_config
        async with InfuserClient(infuser_url, timeout_seconds) as infuser:
            try:
                status = await infuser.get_status()
                model_val = status.get("default_model", "unknown")
                model = str(model_val) if model_val else "unknown"
                git_hash_val = status.get("version", "unknown")
                git_hash = str(git_hash_val) if git_hash_val else "unknown"
                # Merge all status fields into infuser_config
                config: dict[str, object] = {**status, **infuser_config}
            except Exception as e:
                logger.warning(f"Could not get infuser status: {e}")
                model_from_config = infuser_config.get("model", "unknown")
                model = str(model_from_config) if model_from_config else "unknown"
                git_hash = str(infuser_config.get("git_hash", "unknown"))
                config = dict(infuser_config)

        metadata = RunMetadata(
            model=model,
            git_hash=git_hash,
            infuser_config=config,
            notes=run_dir_path.name,
        )
        run_dir.create(metadata)

    # Always load existing results (for resume)
    run_dir.load()
    if run_dir.get_completed_count() > 0:
        logger.info(f"Resuming: {run_dir.get_completed_count()} tasks already completed")

    # Re-evaluate existing results if requested
    if reevaluate:
        reevaluated = 0
        changed = 0
        for task in tasks:
            existing = run_dir.get_result(task.id)
            if not existing:
                continue
            output_file = existing.get("output_file")
            if not output_file:
                continue
            output_path = run_dir.path / output_file
            if not output_path.exists():
                continue

            # Re-evaluate
            eval_result = evaluator.evaluate(task, output_path)
            new_result = "pass" if eval_result.passed else "fail"
            old_result = existing.get("result")

            if new_result != old_result:
                changed += 1
                logger.info(
                    f"Task {task.id}: {old_result} -> {new_result} "
                    f"({eval_result.message or 'OK'})"
                )

            # Update the result in place
            existing["result"] = new_result
            existing["message"] = eval_result.message
            reevaluated += 1

        if reevaluated > 0:
            run_dir._save_results()
            logger.info(f"Re-evaluated {reevaluated} tasks, {changed} changed")

    # Run tasks
    # Note: if/else needed for proper type inference of infuser client
    if use_new_endpoint:
        async with NewInfuserClient(infuser_url, timeout_seconds) as infuser:
            runner = TaskRunner(
                infuser=infuser,
                evaluator=evaluator,
                dataset=dataset,
                run_dir=run_dir,
                concurrency=concurrency,
                use_new_endpoint=True,
            )
            stats = await runner.run_all(tasks)
    else:
        async with InfuserClient(infuser_url, timeout_seconds) as infuser:
            runner = TaskRunner(
                infuser=infuser,
                evaluator=evaluator,
                dataset=dataset,
                run_dir=run_dir,
                concurrency=concurrency,
                use_new_endpoint=False,
            )
            stats = await runner.run_all(tasks)

    return stats
