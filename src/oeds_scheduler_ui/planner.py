"""Scheduler job planning without importing crawler implementations."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from oeds_scheduler_ui.factory import ConstructorPlan, CrawlerFactory
from oeds_scheduler_ui.interfaces import CrawlerSpec, OedsCrawlerConfig, normalize_crawler_config


@dataclass(frozen=True)
class CrawlerJobConfig:
    """Effective config for one scheduled crawler job."""

    crawler_name: str
    job_name: str
    config: Mapping[str, Any]
    schedule: str | None

    @property
    def job_id(self) -> str:
        return f"{self.crawler_name}:{self.job_name}"

    @property
    def display_name(self) -> str:
        if self.job_name == "default":
            return self.crawler_name
        return self.job_id

    @property
    def enabled(self) -> bool:
        if "enable" in self.config:
            return self.config["enable"] is True
        return self.config.get("enabled") is True

    @property
    def run_post_scripts(self) -> bool:
        return self.config.get("run_post_scripts", True) is not False


@dataclass(frozen=True)
class SchedulerJobPlan:
    """Dry run plan for one scheduler job."""

    crawler_name: str
    job_name: str
    job_id: str
    display_name: str
    source_name: str
    schedule: str | None
    enabled: bool
    run_post_scripts: bool
    post_run_scripts: Sequence[str]
    raw_config: Mapping[str, Any]
    crawler_config: OedsCrawlerConfig
    constructor_plan: ConstructorPlan


@dataclass(frozen=True)
class SchedulerPlanIssue:
    """Non-fatal scheduler planning issue."""

    crawler_name: str
    job_name: str | None
    reason: str


@dataclass(frozen=True)
class SchedulerPlanResult:
    """Planned jobs plus skipped entries and configuration errors."""

    plans: tuple[SchedulerJobPlan, ...]
    skipped: tuple[SchedulerPlanIssue, ...]
    errors: tuple[SchedulerPlanIssue, ...]


def merge_job_config(
    base_config: Mapping[str, Any],
    override_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Recursively merge scheduler config mappings."""

    merged_config = deepcopy(dict(base_config))
    for key, value in override_config.items():
        current_value = merged_config.get(key)
        if isinstance(value, Mapping) and isinstance(current_value, Mapping):
            merged_config[key] = merge_job_config(current_value, value)
        else:
            merged_config[key] = deepcopy(value)
    return merged_config


def apply_default_config(raw_config: Mapping[str, Any]) -> dict[str, Any]:
    """Apply the top-level ``default`` section to each crawler config."""

    default_config = raw_config.get("default", {})
    if default_config is None:
        default_config = {}
    if not isinstance(default_config, Mapping):
        raise ValueError("default config must be a mapping")

    merged_config: dict[str, Any] = {"default": dict(default_config)}
    for crawler_name, crawler_config in raw_config.items():
        if crawler_name == "default":
            continue
        if not isinstance(crawler_config, Mapping):
            continue

        crawler_base = {
            key: value for key, value in crawler_config.items() if key != "jobs"
        }
        merged_crawler = merge_job_config(default_config, crawler_base)
        if "schema_name" not in merged_crawler:
            merged_crawler["schema_name"] = crawler_name
        if "jobs" in crawler_config:
            merged_crawler["jobs"] = deepcopy(crawler_config["jobs"])
        merged_config[str(crawler_name)] = merged_crawler

    return merged_config


def expand_crawler_job_configs(
    crawler_name: str,
    crawler_config: Mapping[str, Any],
) -> tuple[CrawlerJobConfig, ...]:
    """Expand a crawler config into default or named job configs."""

    jobs = crawler_config.get("jobs")
    if isinstance(jobs, Mapping) and jobs:
        base_config = {
            key: deepcopy(value)
            for key, value in crawler_config.items()
            if key != "jobs"
        }
        job_configs: list[CrawlerJobConfig] = []
        for job_name, job_config in jobs.items():
            if not isinstance(job_config, Mapping):
                continue

            effective_config = merge_job_config(base_config, job_config)
            effective_config["_scheduler_job_name"] = str(job_name)
            effective_config["_scheduler_job_id"] = f"{crawler_name}:{job_name}"
            job_configs.append(
                CrawlerJobConfig(
                    crawler_name=crawler_name,
                    job_name=str(job_name),
                    config=effective_config,
                    schedule=_schedule_from_config(effective_config),
                )
            )
        return tuple(job_configs)

    effective_config = deepcopy(dict(crawler_config))
    effective_config.pop("jobs", None)
    effective_config["_scheduler_job_name"] = "default"
    effective_config["_scheduler_job_id"] = f"{crawler_name}:default"
    return (
        CrawlerJobConfig(
            crawler_name=crawler_name,
            job_name="default",
            config=effective_config,
            schedule=_schedule_from_config(effective_config),
        ),
    )


def build_scheduler_job_plans(
    raw_config: Mapping[str, Any],
    factory: CrawlerFactory,
) -> SchedulerPlanResult:
    """Build dry scheduler plans from config and a merged crawler registry."""

    config = apply_default_config(raw_config)
    known_crawlers = set(factory.list_crawlers())
    plans: list[SchedulerJobPlan] = []
    skipped: list[SchedulerPlanIssue] = []
    errors: list[SchedulerPlanIssue] = []

    for crawler_name, crawler_config in config.items():
        if crawler_name == "default":
            continue
        if not isinstance(crawler_config, Mapping):
            errors.append(
                SchedulerPlanIssue(crawler_name, None, "crawler config is not a mapping")
            )
            continue
        if crawler_name not in known_crawlers:
            skipped.append(
                SchedulerPlanIssue(crawler_name, None, "crawler is not in registry")
            )
            continue

        audit = factory.audit(crawler_name)
        if not audit.run_methods:
            errors.append(
                SchedulerPlanIssue(
                    crawler_name,
                    None,
                    "crawler has no supported run method",
                )
            )
            continue

        for job_config in expand_crawler_job_configs(crawler_name, crawler_config):
            if not job_config.enabled:
                skipped.append(
                    SchedulerPlanIssue(
                        crawler_name,
                        job_config.job_name,
                        "job is disabled",
                    )
                )
                continue

            try:
                normalized_config = normalize_crawler_config(job_config.config)
                constructor_plan = factory.constructor_plan(
                    crawler_name,
                    normalized_config,
                )
            except Exception as exc:
                errors.append(
                    SchedulerPlanIssue(crawler_name, job_config.job_name, str(exc))
                )
                continue

            target = factory.get_target(crawler_name)
            source_name = (
                target.source_name if isinstance(target, CrawlerSpec) else "loaded-class"
            )
            plans.append(
                SchedulerJobPlan(
                    crawler_name=crawler_name,
                    job_name=job_config.job_name,
                    job_id=job_config.job_id,
                    display_name=job_config.display_name,
                    source_name=source_name,
                    schedule=job_config.schedule,
                    enabled=job_config.enabled,
                    run_post_scripts=job_config.run_post_scripts,
                    post_run_scripts=_post_run_scripts(job_config.config),
                    raw_config=dict(job_config.config),
                    crawler_config=normalized_config,
                    constructor_plan=constructor_plan,
                )
            )

    return SchedulerPlanResult(
        plans=tuple(plans),
        skipped=tuple(skipped),
        errors=tuple(errors),
    )


def _schedule_from_config(config: Mapping[str, Any]) -> str | None:
    schedule = config.get("schedule")
    if schedule is None:
        return None
    return str(schedule)


def _post_run_scripts(config: Mapping[str, Any]) -> tuple[str, ...]:
    scripts = config.get("post_run_scripts")
    if scripts is None:
        return ()
    if isinstance(scripts, str):
        return (scripts,)
    if not isinstance(scripts, Sequence):
        return ()
    return tuple(str(script) for script in scripts)
