"""Small scheduler service boundary for planned crawler jobs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from oeds_scheduler_ui.planner import SchedulerJobPlan
from oeds_scheduler_ui.runtime import (
    CrawlerJobQueue,
    CrawlerJobRunner,
    CrawlerRunResult,
    ScheduledCrawlerJob,
    lock_keys_from_plan,
    run_ready_jobs,
)


class ScheduleAdapter(Protocol):
    """Schedule abstraction used by the runtime loop."""

    def next_after(self, ref_time: datetime) -> datetime:
        """Return the next runtime after ``ref_time``."""


ScheduleFactory = Callable[[str], ScheduleAdapter]


@dataclass(frozen=True)
class SchedulerServiceIssue:
    """Non-fatal service setup issue."""

    job_id: str
    reason: str


@dataclass
class ScheduledPlan:
    """A scheduler plan with parsed schedule state."""

    plan: SchedulerJobPlan
    schedule: ScheduleAdapter
    lock_keys: frozenset[str]
    next_run_time: datetime

    @property
    def job_id(self) -> str:
        return self.plan.job_id

    @property
    def display_name(self) -> str:
        return self.plan.display_name

    def advance_after(self, ref_time: datetime) -> None:
        self.next_run_time = self.schedule.next_after(ref_time)

    def to_runtime_job(self) -> ScheduledCrawlerJob:
        return ScheduledCrawlerJob(plan=self.plan, lock_keys=self.lock_keys)


class CronConverterSchedule:
    """`cron-converter` backed schedule adapter."""

    def __init__(self, expression: str) -> None:
        try:
            from cron_converter import Cron
        except ImportError as exc:  # pragma: no cover - dependency is optional here.
            raise RuntimeError(
                "cron-converter is required to parse scheduler cron expressions"
            ) from exc

        self._cron = Cron(expression)

    def next_after(self, ref_time: datetime) -> datetime:
        return self._cron.schedule(ref_time).next()


def cron_schedule_factory(expression: str) -> CronConverterSchedule:
    """Build the default cron schedule adapter."""

    return CronConverterSchedule(expression)


class SchedulerService:
    """Enqueue due planned jobs and dispatch unblocked runtime jobs."""

    def __init__(
        self,
        plans: Sequence[SchedulerJobPlan],
        runner: CrawlerJobRunner,
        schedule_factory: ScheduleFactory = cron_schedule_factory,
        queue: CrawlerJobQueue | None = None,
        reference_time: datetime | None = None,
    ) -> None:
        self._runner = runner
        self._schedule_factory = schedule_factory
        self._queue = queue or CrawlerJobQueue()
        self._jobs: list[ScheduledPlan] = []
        self._issues: list[SchedulerServiceIssue] = []

        self.reload_plans(plans, reference_time=reference_time)

    @property
    def jobs(self) -> tuple[ScheduledPlan, ...]:
        return tuple(sorted(self._jobs, key=lambda job: (job.next_run_time, job.job_id)))

    @property
    def issues(self) -> tuple[SchedulerServiceIssue, ...]:
        return tuple(self._issues)

    @property
    def next_run_time(self) -> datetime | None:
        if not self._jobs:
            return None
        return min(job.next_run_time for job in self._jobs)

    def reload_plans(
        self,
        plans: Sequence[SchedulerJobPlan],
        reference_time: datetime | None = None,
    ) -> None:
        ref_time = reference_time or datetime.now()
        self._jobs.clear()
        self._issues.clear()

        for plan in plans:
            if not plan.schedule:
                self._issues.append(
                    SchedulerServiceIssue(plan.job_id, "job has no schedule")
                )
                continue
            try:
                schedule = self._schedule_factory(plan.schedule)
                next_run_time = schedule.next_after(ref_time)
            except Exception as exc:
                self._issues.append(
                    SchedulerServiceIssue(
                        plan.job_id,
                        f"invalid schedule {plan.schedule!r}: {exc}",
                    )
                )
                continue

            self._jobs.append(
                ScheduledPlan(
                    plan=plan,
                    schedule=schedule,
                    lock_keys=lock_keys_from_plan(plan),
                    next_run_time=next_run_time,
                )
            )

    def enqueue_due_jobs(self, now: datetime | None = None) -> int:
        ref_time = now or datetime.now()
        enqueued = 0
        for job in self._jobs:
            if job.next_run_time <= ref_time:
                scheduled_for = job.next_run_time
                if self._queue.enqueue(job.to_runtime_job(), scheduled_for):
                    enqueued += 1
                job.advance_after(ref_time)
        return enqueued

    def dispatch_ready_jobs(self) -> tuple[CrawlerRunResult, ...]:
        return run_ready_jobs(self._queue, self._runner)

    def tick(self, now: datetime | None = None) -> tuple[CrawlerRunResult, ...]:
        self.enqueue_due_jobs(now)
        return self.dispatch_ready_jobs()
