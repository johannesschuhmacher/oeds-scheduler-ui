"""Runtime execution helpers for planned scheduler jobs."""

from __future__ import annotations

import subprocess
import os
import shlex
import sys
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Sequence

from oeds_scheduler_ui.factory import CrawlerFactory
from oeds_scheduler_ui.interfaces import run_crawler_instance
from oeds_scheduler_ui.planner import SchedulerJobPlan


@dataclass(frozen=True)
class PostRunCommandResult:
    """Result of one scheduler-owned post-run command."""

    command: str
    returncode: int | None
    success: bool
    error: str | None = None


@dataclass(frozen=True)
class CrawlerRunResult:
    """Result of executing one planned crawler job."""

    job_id: str
    display_name: str
    crawler_name: str
    job_name: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    crawler_success: bool
    post_run_success: bool
    success: bool
    crawler_result: Any = None
    error: str | None = None
    post_run_results: tuple[PostRunCommandResult, ...] = ()


@dataclass(frozen=True)
class ScheduledCrawlerJob:
    """Runtime queue item created from a dry scheduler plan."""

    plan: SchedulerJobPlan
    lock_keys: frozenset[str]

    @property
    def job_id(self) -> str:
        return self.plan.job_id

    @property
    def display_name(self) -> str:
        return self.plan.display_name


@dataclass(frozen=True)
class QueuedCrawlerJob:
    """Queued runtime job with scheduling timestamps."""

    job: ScheduledCrawlerJob
    scheduled_for: datetime
    enqueued_at: datetime


PostRunExecutor = Callable[[str, SchedulerJobPlan], PostRunCommandResult]


class CrawlerJobRunner:
    """Construct and run crawler instances through the shared factory."""

    def __init__(
        self,
        factory: CrawlerFactory,
        post_run_executor: PostRunExecutor | None = None,
    ) -> None:
        self._factory = factory
        self._post_run_executor = post_run_executor or execute_post_command

    def run(self, plan: SchedulerJobPlan) -> CrawlerRunResult:
        started_at = datetime.now()
        crawler_success = False
        crawler_result: Any = None
        error: str | None = None
        post_run_results: tuple[PostRunCommandResult, ...] = ()

        try:
            crawler = self._factory.construct(plan.crawler_name, plan.crawler_config)
            crawler_result = run_crawler_instance(crawler)
            crawler_success = True
        except Exception as exc:  # pragma: no cover - exact crawler failures vary.
            error = f"{type(exc).__name__}: {exc}"

        if crawler_success and plan.run_post_scripts and plan.post_run_scripts:
            post_run_results = tuple(
                self._execute_post_run_command(command, plan)
                for command in plan.post_run_scripts
            )

        post_run_success = all(result.success for result in post_run_results)
        finished_at = datetime.now()
        duration_seconds = (finished_at - started_at).total_seconds()
        return CrawlerRunResult(
            job_id=plan.job_id,
            display_name=plan.display_name,
            crawler_name=plan.crawler_name,
            job_name=plan.job_name,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            crawler_success=crawler_success,
            post_run_success=post_run_success,
            success=crawler_success and post_run_success,
            crawler_result=crawler_result,
            error=error,
            post_run_results=post_run_results,
        )

    def _execute_post_run_command(
        self,
        command: str,
        plan: SchedulerJobPlan,
    ) -> PostRunCommandResult:
        try:
            return self._post_run_executor(command, plan)
        except Exception as exc:  # pragma: no cover - executor is injectable.
            return PostRunCommandResult(
                command=command,
                returncode=None,
                success=False,
                error=f"{type(exc).__name__}: {exc}",
            )


class CrawlerJobQueue:
    """Queue planned jobs while preventing duplicate and conflicting runs."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending: deque[QueuedCrawlerJob] = deque()
        self._pending_job_ids: set[str] = set()
        self._active_job_ids: set[str] = set()
        self._active_job_locks: dict[str, frozenset[str]] = {}
        self._active_lock_keys: set[str] = set()

    def enqueue(
        self,
        job: ScheduledCrawlerJob,
        scheduled_for: datetime | None = None,
    ) -> bool:
        with self._lock:
            if job.job_id in self._pending_job_ids or job.job_id in self._active_job_ids:
                return False

            self._pending.append(
                QueuedCrawlerJob(
                    job=job,
                    scheduled_for=scheduled_for or datetime.now(),
                    enqueued_at=datetime.now(),
                )
            )
            self._pending_job_ids.add(job.job_id)
            return True

    def pop_ready_jobs(self) -> tuple[QueuedCrawlerJob, ...]:
        ready: list[QueuedCrawlerJob] = []
        blocked: deque[QueuedCrawlerJob] = deque()

        with self._lock:
            while self._pending:
                queued_job = self._pending.popleft()
                job = queued_job.job

                if job.lock_keys & self._active_lock_keys:
                    blocked.append(queued_job)
                    continue

                self._pending_job_ids.remove(job.job_id)
                self._active_job_ids.add(job.job_id)
                self._active_job_locks[job.job_id] = job.lock_keys
                self._active_lock_keys.update(job.lock_keys)
                ready.append(queued_job)

            self._pending = blocked

        return tuple(ready)

    def mark_finished(self, job: ScheduledCrawlerJob) -> None:
        with self._lock:
            self._active_job_ids.discard(job.job_id)
            lock_keys = self._active_job_locks.pop(job.job_id, frozenset())
            for lock_key in lock_keys:
                self._active_lock_keys.discard(lock_key)


def execute_python_script(
    command: str,
    plan: SchedulerJobPlan,
) -> PostRunCommandResult:
    """Run a legacy post-run Python script."""

    completed = subprocess.run([sys.executable, command], check=False)
    return PostRunCommandResult(
        command=command,
        returncode=completed.returncode,
        success=completed.returncode == 0,
    )


def execute_post_command(
    command: str,
    plan: SchedulerJobPlan,
) -> PostRunCommandResult:
    """Run a post-run command.

    Legacy ``*.py`` script paths are executed with the current Python
    interpreter. Stable commands such as ``oeds-post gapfill entsoe-fms`` are
    executed as process argv.
    """

    argv = shlex.split(command)
    if not argv:
        return PostRunCommandResult(
            command=command,
            returncode=None,
            success=False,
            error="empty post-run command",
        )

    executable = argv[0]
    if executable == "oeds-post":
        result = _try_run_oeds_post(argv[1:])
        if result is not None:
            return result

    if executable.endswith(".py"):
        process_argv = [sys.executable, *argv]
    else:
        process_argv = argv

    completed = subprocess.run(
        process_argv,
        check=False,
        env=_subprocess_env_with_workspace_pythonpath(),
    )
    return PostRunCommandResult(
        command=command,
        returncode=completed.returncode,
        success=completed.returncode == 0,
    )


def _try_run_oeds_post(args: Sequence[str]) -> PostRunCommandResult | None:
    try:
        from oeds_post_scripts.runner import run_post_command
    except ImportError:
        return None

    result = run_post_command(args)
    return PostRunCommandResult(
        command=result.command,
        returncode=result.returncode,
        success=result.success,
    )


def _subprocess_env_with_workspace_pythonpath() -> dict[str, str]:
    env = os.environ.copy()
    workspace = str(Path.cwd())
    existing_pythonpath = env.get("PYTHONPATH")
    if existing_pythonpath:
        env["PYTHONPATH"] = os.pathsep.join([workspace, existing_pythonpath])
    else:
        env["PYTHONPATH"] = workspace
    return env


def scheduled_job_from_plan(
    plan: SchedulerJobPlan,
    lock_keys: Sequence[str] | None = None,
) -> ScheduledCrawlerJob:
    """Create a runtime queue job from a dry scheduler plan."""

    effective_lock_keys = (
        frozenset(lock_keys) if lock_keys is not None else lock_keys_from_plan(plan)
    )
    return ScheduledCrawlerJob(plan=plan, lock_keys=effective_lock_keys)


def lock_keys_from_plan(plan: SchedulerJobPlan) -> frozenset[str]:
    """Return conservative lock keys without importing crawler modules."""

    configured_lock_keys = plan.raw_config.get("lock_keys")
    if isinstance(configured_lock_keys, str) and configured_lock_keys:
        return frozenset({configured_lock_keys})
    if isinstance(configured_lock_keys, Sequence) and not isinstance(
        configured_lock_keys,
        str,
    ):
        lock_keys = tuple(str(lock_key) for lock_key in configured_lock_keys)
        if lock_keys:
            return frozenset(lock_keys)

    if plan.crawler_name == "entsoe_fms":
        target_data_items = plan.raw_config.get("target_data_items")
        if isinstance(target_data_items, Sequence) and not isinstance(
            target_data_items,
            str,
        ):
            data_item_keys = tuple(str(item) for item in target_data_items)
            if data_item_keys:
                return frozenset(
                    f"{plan.crawler_name}:{data_item}" for data_item in data_item_keys
                )

    return frozenset({f"crawler:{plan.crawler_name}"})


def run_ready_jobs(
    queue: CrawlerJobQueue,
    runner: CrawlerJobRunner,
) -> tuple[CrawlerRunResult, ...]:
    """Synchronously run currently unblocked queued jobs."""

    results: list[CrawlerRunResult] = []
    while True:
        ready_jobs = queue.pop_ready_jobs()
        if not ready_jobs:
            break
        for queued_job in ready_jobs:
            try:
                results.append(runner.run(queued_job.job.plan))
            finally:
                queue.mark_finished(queued_job.job)
    return tuple(results)
