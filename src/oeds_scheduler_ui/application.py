"""Production-oriented scheduler application assembly."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from oeds_scheduler_ui.config import file_signature, load_scheduler_config
from oeds_scheduler_ui.distribution import load_inventory, registries_from_inventory
from oeds_scheduler_ui.factory import CrawlerFactory
from oeds_scheduler_ui.interfaces import merge_crawler_registries
from oeds_scheduler_ui.planner import (
    SchedulerPlanIssue,
    SchedulerPlanResult,
    build_scheduler_job_plans,
)
from oeds_scheduler_ui.runtime import (
    CrawlerJobRunner,
    CrawlerRunResult,
    PostRunExecutor,
)
from oeds_scheduler_ui.service import (
    ScheduleFactory,
    SchedulerService,
    SchedulerServiceIssue,
    cron_schedule_factory,
)


ConfigLoader = Callable[[Path], Mapping[str, Any]]


@dataclass(frozen=True)
class SchedulerApplicationSnapshot:
    """Observable state after building or reloading the scheduler app."""

    config_path: Path
    inventory_path: Path
    workspace_root: Path
    crawler_count: int
    planned_job_count: int
    skipped: tuple[SchedulerPlanIssue, ...]
    plan_errors: tuple[SchedulerPlanIssue, ...]
    service_issues: tuple[SchedulerServiceIssue, ...]

    @property
    def ready(self) -> bool:
        return not self.plan_errors and not self.service_issues


class SchedulerApplication:
    """Assemble registry, planning, service, and reload handling."""

    def __init__(
        self,
        config_path: str | Path,
        inventory_path: str | Path,
        workspace_root: str | Path,
        *,
        schedule_factory: ScheduleFactory = cron_schedule_factory,
        post_run_executor: PostRunExecutor | None = None,
        config_loader: ConfigLoader = load_scheduler_config,
        reference_time: datetime | None = None,
    ) -> None:
        self.config_path = Path(config_path).resolve()
        self.inventory_path = Path(inventory_path).resolve()
        self.workspace_root = Path(workspace_root).resolve()
        self._schedule_factory = schedule_factory
        self._post_run_executor = post_run_executor
        self._config_loader = config_loader
        self._factory: CrawlerFactory | None = None
        self._plan_result: SchedulerPlanResult | None = None
        self._service: SchedulerService | None = None
        self._snapshot: SchedulerApplicationSnapshot | None = None
        self._config_signature: tuple[int, int] | None = None

        self.reload(reference_time=reference_time)

    @property
    def factory(self) -> CrawlerFactory:
        if self._factory is None:
            raise RuntimeError("scheduler application has not been loaded")
        return self._factory

    @property
    def plan_result(self) -> SchedulerPlanResult:
        if self._plan_result is None:
            raise RuntimeError("scheduler application has not been loaded")
        return self._plan_result

    @property
    def service(self) -> SchedulerService:
        if self._service is None:
            raise RuntimeError("scheduler application has not been loaded")
        return self._service

    @property
    def snapshot(self) -> SchedulerApplicationSnapshot:
        if self._snapshot is None:
            raise RuntimeError("scheduler application has not been loaded")
        return self._snapshot

    def reload(
        self,
        reference_time: datetime | None = None,
    ) -> SchedulerApplicationSnapshot:
        inventory = load_inventory(self.inventory_path)
        registries = registries_from_inventory(
            inventory,
            workspace_root=self.workspace_root,
        )
        merged_registry = merge_crawler_registries(registries)
        factory = CrawlerFactory(merged_registry)
        raw_config = self._config_loader(self.config_path)
        plan_result = build_scheduler_job_plans(raw_config, factory)
        runner = CrawlerJobRunner(
            factory,
            post_run_executor=self._post_run_executor,
        )
        service = SchedulerService(
            plan_result.plans,
            runner,
            schedule_factory=self._schedule_factory,
            reference_time=reference_time,
        )

        snapshot = SchedulerApplicationSnapshot(
            config_path=self.config_path,
            inventory_path=self.inventory_path,
            workspace_root=self.workspace_root,
            crawler_count=len(merged_registry),
            planned_job_count=len(plan_result.plans),
            skipped=plan_result.skipped,
            plan_errors=plan_result.errors,
            service_issues=service.issues,
        )
        self._factory = factory
        self._plan_result = plan_result
        self._service = service
        self._snapshot = snapshot
        self._config_signature = file_signature(self.config_path)
        return snapshot

    def reload_if_changed(
        self,
        reference_time: datetime | None = None,
    ) -> bool:
        current_signature = file_signature(self.config_path)
        if current_signature == self._config_signature:
            return False
        self.reload(reference_time=reference_time)
        return True

    def tick(
        self,
        now: datetime | None = None,
        reload_if_changed: bool = True,
    ) -> tuple[CrawlerRunResult, ...]:
        if reload_if_changed:
            self.reload_if_changed(reference_time=now)
        return self.service.tick(now)


def format_application_summary(snapshot: SchedulerApplicationSnapshot) -> str:
    """Create a compact CLI-friendly application status summary."""

    return "\n".join(
        [
            f"crawlers: {snapshot.crawler_count}",
            f"planned jobs: {snapshot.planned_job_count}",
            f"skipped jobs: {len(snapshot.skipped)}",
            f"plan errors: {len(snapshot.plan_errors)}",
            f"service issues: {len(snapshot.service_issues)}",
        ]
    )
