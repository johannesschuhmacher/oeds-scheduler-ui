from __future__ import annotations

import errno
import os
import pkgutil
import re
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import Any

import yaml
from crawler.common.local_env import apply_email_env_overrides
from cron_converter import Cron
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

CONFIG_FILENAME = "CRAWLER_CONFIG.yml"
EXCLUDED_CRAWLER_MODULES = {"__init__", "common", "data"}
GAPFILL_POSTRUN_SCRIPT = "scripts/gapfill_timeseries.py"
WINDOWS_TIMEZONE_ABBREVIATIONS = {
    "W. Europe Standard Time": "CET",
    "W. Europe Daylight Time": "CEST",
}


@dataclass(frozen=True)
class ValidationIssue:
    level: str
    message: str
    scope: str = "global"
    location: str | None = None

    @property
    def badge_label(self) -> str:
        return "Error" if self.level == "error" else "Warning"


@dataclass(frozen=True)
class CronPreview:
    schedule: str | None
    summary: str
    next_runs: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class CrawlerJobPreview:
    name: str
    enabled: bool | None
    schedule: str | None
    preview: CronPreview


@dataclass(frozen=True)
class ValidationResult:
    data: dict[str, Any] | None
    issues: list[ValidationIssue]

    @property
    def has_errors(self) -> bool:
        return any(issue.level == "error" for issue in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level == "warning")


@dataclass(frozen=True)
class CrawlerCard:
    name: str
    description: str
    configured: bool
    module_present: bool
    enabled: bool | None
    schema_name: str | None
    schedule: str | None
    schedule_source: str | None
    preview: CronPreview
    job_previews: list[CrawlerJobPreview]
    issues: list[ValidationIssue]
    state_label: str
    state_variant: str


@dataclass(frozen=True)
class DashboardState:
    cards: list[CrawlerCard]
    global_issues: list[ValidationIssue]
    validation: ValidationResult
    available_count: int
    configured_count: int
    enabled_count: int
    config_path: Path
    config_hash: str
    config_mtime: str
    time_label: str
    server_time: str
    schedule_samples: list[str]


@dataclass(frozen=True)
class CrawlerOverview:
    card: CrawlerCard | None
    crawler_name: str
    raw_config: dict[str, Any]
    effective_config: dict[str, Any]
    default_config: dict[str, Any]
    available_crawlers: list[str]
    validation: ValidationResult
    issues: list[ValidationIssue]


def get_repo_root() -> Path:
    configured_root = os.getenv("OEDS_ADMIN_REPO_ROOT") or os.getenv("OEDS_REPO_ROOT")
    if configured_root:
        return Path(configured_root).expanduser().resolve()

    current_root = Path.cwd().resolve()
    if (current_root / CONFIG_FILENAME).is_file() and (current_root / "crawler").is_dir():
        return current_root

    app_root = Path("/app")
    if (app_root / CONFIG_FILENAME).is_file() and (app_root / "crawler").is_dir():
        return app_root

    return Path(__file__).resolve().parent.parent


def get_config_path(repo_root: Path | None = None) -> Path:
    root = repo_root or get_repo_root()
    return root / CONFIG_FILENAME


def read_config_text(repo_root: Path | None = None) -> str:
    return get_config_path(repo_root).read_text(encoding="utf-8")


def compute_content_hash(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def get_local_time_label() -> str:
    local_now = datetime.now().astimezone()
    tz_label = local_now.tzname() or ""

    if tz_label in WINDOWS_TIMEZONE_ABBREVIATIONS:
        return WINDOWS_TIMEZONE_ABBREVIATIONS[tz_label]

    if re.fullmatch(r"[A-Z]{2,6}", tz_label):
        return tz_label

    offset = local_now.strftime("%z")
    if len(offset) == 5:
        return f"UTC{offset[:3]}:{offset[3:]}"

    return "Local time"


def format_local_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M")


def get_file_mtime_display(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def discover_crawler_modules(repo_root: Path | None = None) -> list[str]:
    root = repo_root or get_repo_root()
    crawler_dir = root / "crawler"
    modules: set[str] = set()

    for file_path in crawler_dir.glob("*.py"):
        module_name = file_path.stem
        if module_name.startswith("__") or module_name in EXCLUDED_CRAWLER_MODULES:
            continue
        modules.add(module_name)

    try:
        import crawler as crawler_package
    except ImportError:
        pass
    else:
        modules.update(
            module.name
            for module in pkgutil.iter_modules(crawler_package.__path__)
            if not module.ispkg and module.name not in EXCLUDED_CRAWLER_MODULES
        )

    return sorted(modules)


def merge_with_defaults(default_config: dict[str, Any], crawler_config: dict[str, Any]) -> dict[str, Any]:
    merged = dict(default_config)
    for key, value in crawler_config.items():
        if key not in merged:
            merged[key] = value
        elif isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_with_defaults(merged[key], value)
        else:
            merged[key] = value
    return merged


def parse_yaml_text(yaml_text: str) -> ValidationResult:
    issues: list[ValidationIssue] = []

    try:
        parsed = yaml.safe_load(yaml_text)
    except yaml.MarkedYAMLError as exc:
        location = None
        if exc.problem_mark is not None:
            location = f"Line {exc.problem_mark.line + 1}, column {exc.problem_mark.column + 1}"
        issues.append(
            ValidationIssue(
                level="error",
                message=str(exc.problem or exc).strip(),
                location=location,
            )
        )
        return ValidationResult(data=None, issues=issues)
    except yaml.YAMLError as exc:
        issues.append(ValidationIssue(level="error", message=str(exc).strip()))
        return ValidationResult(data=None, issues=issues)

    if parsed is None:
        issues.append(ValidationIssue(level="error", message="The YAML document is empty."))
        return ValidationResult(data=None, issues=issues)

    if not isinstance(parsed, dict):
        issues.append(ValidationIssue(level="error", message="The top-level YAML document must be a mapping."))
        return ValidationResult(data=None, issues=issues)

    return ValidationResult(data=parsed, issues=issues)


def validate_config_text(yaml_text: str, available_crawlers: list[str] | None = None) -> ValidationResult:
    parsed_result = parse_yaml_text(yaml_text)
    if parsed_result.has_errors or parsed_result.data is None:
        return parsed_result

    issues = list(parsed_result.issues)
    available = set(available_crawlers or discover_crawler_modules())
    config = parsed_result.data

    default_config = config.get("default")
    if default_config is None:
        issues.append(ValidationIssue(level="error", message="Missing required top-level 'default' section."))
        default_config = {}
    elif not isinstance(default_config, dict):
        issues.append(
            ValidationIssue(
                level="error",
                message="The 'default' section must be a mapping of shared crawler settings.",
                scope="default",
            )
        )
        default_config = {}

    for key, value in config.items():
        if not isinstance(key, str):
            issues.append(ValidationIssue(level="error", message=f"Top-level key {key!r} must be a string."))
            continue

        if key == "default":
            continue

        if not isinstance(value, dict):
            issues.append(
                ValidationIssue(
                    level="error",
                    message="Crawler configuration must be a mapping.",
                    scope=key,
                )
            )
            continue

        if key not in available:
            issues.append(
                ValidationIssue(
                    level="warning",
                    message="No matching crawler module was found in crawler/.",
                    scope=key,
                )
            )

        merged = merge_with_defaults(default_config, value)
        _validate_merged_crawler_config(key, value, merged, issues)

    return ValidationResult(data=config, issues=issues)


def _validate_merged_crawler_config(
    crawler_name: str,
    crawler_config: dict[str, Any],
    merged_config: dict[str, Any],
    issues: list[ValidationIssue],
) -> None:
    jobs_value = crawler_config.get("jobs")
    schedule_value = merged_config.get("schedule")
    if schedule_value is None:
        issues.append(
            ValidationIssue(
                level="error",
                message="Missing effective 'schedule'. Add it either in the crawler section or in 'default'.",
                scope=crawler_name,
            )
        )
    elif not isinstance(schedule_value, str):
        issues.append(
            ValidationIssue(
                level="error",
                message="The effective 'schedule' must be a CRON string.",
                scope=crawler_name,
            )
        )
    else:
        preview = build_cron_preview(schedule_value)
        if preview.error:
            issues.append(
                ValidationIssue(
                    level="error",
                    message=f"Invalid CRON schedule: {preview.error}",
                    scope=crawler_name,
            )
        )

    if jobs_value is not None:
        _validate_crawler_jobs(crawler_name, merged_config, jobs_value, issues)

    enable_value = merged_config.get("enable")
    if not isinstance(enable_value, bool):
        issues.append(
            ValidationIssue(
                level="error",
                message="The effective 'enable' value must be true or false.",
                scope=crawler_name,
            )
        )

    schema_name = merged_config.get("schema_name")
    if not isinstance(schema_name, str) or not schema_name.strip():
        issues.append(
            ValidationIssue(
                level="error",
                message="Missing effective 'schema_name'.",
                scope=crawler_name,
            )
        )

    database_uri = merged_config.get("database_uri")
    if not isinstance(database_uri, str) or not database_uri.strip():
        issues.append(
            ValidationIssue(
                level="error",
                message="Missing effective 'database_uri'.",
                scope=crawler_name,
            )
        )

    post_run_scripts = merged_config.get("post_run_scripts")
    if post_run_scripts is not None and not isinstance(post_run_scripts, list):
        issues.append(
            ValidationIssue(
                level="error",
                message="The effective 'post_run_scripts' value must be a list.",
                scope=crawler_name,
            )
        )

    if "description" in crawler_config and not isinstance(crawler_config["description"], str):
        issues.append(
            ValidationIssue(
                level="warning",
                message="The crawler description should be a string.",
                scope=crawler_name,
            )
        )


def _validate_crawler_jobs(
    crawler_name: str,
    merged_config: dict[str, Any],
    jobs_value: Any,
    issues: list[ValidationIssue],
) -> None:
    if not isinstance(jobs_value, dict):
        issues.append(
            ValidationIssue(
                level="error",
                message="The 'jobs' value must be a mapping of named scheduler jobs.",
                scope=crawler_name,
            )
        )
        return

    if not jobs_value:
        issues.append(
            ValidationIssue(
                level="warning",
                message="The 'jobs' mapping is empty; the crawler will fall back to its legacy schedule.",
                scope=crawler_name,
            )
        )
        return

    base_config = {key: value for key, value in merged_config.items() if key != "jobs"}
    for job_name, job_config in jobs_value.items():
        job_scope = f"{crawler_name}:{job_name}"
        if not isinstance(job_name, str):
            issues.append(
                ValidationIssue(
                    level="error",
                    message=f"Job key {job_name!r} must be a string.",
                    scope=crawler_name,
                )
            )
            continue

        if not isinstance(job_config, dict):
            issues.append(
                ValidationIssue(
                    level="error",
                    message="Crawler job configuration must be a mapping.",
                    scope=job_scope,
                )
            )
            continue

        effective_job_config = merge_with_defaults(base_config, job_config)
        job_schedule = effective_job_config.get("schedule")
        if not isinstance(job_schedule, str) or not job_schedule.strip():
            issues.append(
                ValidationIssue(
                    level="error",
                    message="Crawler job must have an effective CRON 'schedule'.",
                    scope=job_scope,
                )
            )
        else:
            preview = build_cron_preview(job_schedule)
            if preview.error:
                issues.append(
                    ValidationIssue(
                        level="error",
                        message=f"Invalid job CRON schedule: {preview.error}",
                        scope=job_scope,
                    )
                )

        job_enable = effective_job_config.get("enable")
        if not isinstance(job_enable, bool):
            issues.append(
                ValidationIssue(
                    level="error",
                    message="Crawler job must have an effective boolean 'enable' value.",
                    scope=job_scope,
                )
            )

        if crawler_name == "entsoe_fms":
            window_months = effective_job_config.get("fms_package_window_months")
            if window_months is not None:
                try:
                    parsed_window_months = int(window_months)
                except (TypeError, ValueError):
                    parsed_window_months = 0
                if parsed_window_months < 1:
                    issues.append(
                        ValidationIssue(
                            level="error",
                            message="'fms_package_window_months' must be an integer greater than or equal to 1.",
                            scope=job_scope,
                        )
                    )

            write_mode = effective_job_config.get("fms_package_write_mode")
            if write_mode is not None and write_mode != "full_upsert":
                issues.append(
                    ValidationIssue(
                        level="warning",
                        message="Unknown ENTSO-E FMS package write mode; supported value is 'full_upsert'.",
                        scope=job_scope,
                    )
                )


def build_cron_preview(schedule: str | None, count: int = 3) -> CronPreview:
    if schedule is None:
        return CronPreview(schedule=None, summary="No schedule configured", error="No effective schedule found.")

    if not isinstance(schedule, str) or not schedule.strip():
        return CronPreview(schedule=schedule, summary="Invalid schedule", error="Schedule must be a non-empty string.")

    clean_schedule = schedule.strip()

    try:
        iterator = Cron(clean_schedule).schedule(datetime.now())
        runs = [format_local_timestamp(iterator.next()) for _ in range(count)]
    except Exception as exc:  # cron_converter exposes plain Exceptions for invalid parts
        return CronPreview(
            schedule=clean_schedule,
            summary="Invalid schedule",
            error=str(exc).strip() or "Could not parse CRON expression.",
        )

    return CronPreview(
        schedule=clean_schedule,
        summary=describe_cron_expression(clean_schedule),
        next_runs=runs,
    )


def describe_cron_expression(schedule: str) -> str:
    parts = schedule.split()
    if len(parts) != 5:
        return "Custom CRON schedule"

    minute, hour, day_of_month, month, day_of_week = parts

    if schedule == "* * * * *":
        return "Every minute"

    minute_step = _parse_step(minute)
    hour_step = _parse_step(hour)

    if minute_step and hour == "*" and day_of_month == "*" and month == "*" and day_of_week == "*":
        return f"Every {minute_step} minutes"

    if hour_step and minute.isdigit() and day_of_month == "*" and month == "*" and day_of_week == "*":
        return f"Every {hour_step} hours at minute {int(minute):02d}"

    if hour == "*" and minute.isdigit() and day_of_month == "*" and month == "*" and day_of_week == "*":
        return f"Every hour at minute {int(minute):02d}"

    if _is_list_token(hour) and minute.isdigit() and day_of_month == "*" and month == "*" and day_of_week == "*":
        times = ", ".join(f"{int(token):02d}:{int(minute):02d}" for token in hour.split(","))
        return f"Every day at {times}"

    if hour.isdigit() and minute.isdigit() and day_of_month == "*" and month == "*" and day_of_week == "*":
        return f"Every day at {int(hour):02d}:{int(minute):02d}"

    if hour.isdigit() and minute.isdigit() and day_of_month == "*" and month == "*" and day_of_week != "*":
        return f"{_describe_day_token(day_of_week)} at {int(hour):02d}:{int(minute):02d}"

    return "Custom CRON schedule"


def _parse_step(token: str) -> int | None:
    match = re.fullmatch(r"\*/(\d+)", token)
    if not match:
        return None
    return int(match.group(1))


def _is_list_token(token: str) -> bool:
    parts = token.split(",")
    return len(parts) > 1 and all(part.isdigit() for part in parts)


def _describe_day_token(token: str) -> str:
    day_map = {
        "0": "Every Sunday",
        "1": "Every Monday",
        "2": "Every Tuesday",
        "3": "Every Wednesday",
        "4": "Every Thursday",
        "5": "Every Friday",
        "6": "Every Saturday",
        "7": "Every Sunday",
    }
    if token in day_map:
        return day_map[token]

    if _is_list_token(token):
        labels = [day_map.get(part, part) for part in token.split(",")]
        stripped = [label.replace("Every ", "") for label in labels]
        return "Every " + ", ".join(stripped)

    return "Custom weekdays"


def build_crawler_job_previews(
    default_config: dict[str, Any],
    crawler_config: dict[str, Any],
    merged_config: dict[str, Any],
) -> list[CrawlerJobPreview]:
    jobs_value = crawler_config.get("jobs")
    if not isinstance(jobs_value, dict) or not jobs_value:
        return []

    base_config = {key: value for key, value in merged_config.items() if key != "jobs"}
    job_previews: list[CrawlerJobPreview] = []
    for job_name, job_config in jobs_value.items():
        if not isinstance(job_name, str) or not isinstance(job_config, dict):
            continue

        effective_job_config = merge_with_defaults(base_config, job_config)
        schedule = effective_job_config.get("schedule")
        preview = build_cron_preview(schedule if isinstance(schedule, str) else None)
        enabled = effective_job_config.get("enable") if isinstance(effective_job_config.get("enable"), bool) else None
        job_previews.append(
            CrawlerJobPreview(
                name=job_name,
                enabled=enabled,
                schedule=schedule if isinstance(schedule, str) else None,
                preview=preview,
            )
        )

    return job_previews


def build_multi_job_preview(job_previews: list[CrawlerJobPreview]) -> CronPreview:
    enabled_jobs = [job for job in job_previews if job.enabled is True]
    if not enabled_jobs:
        return CronPreview(schedule=None, summary="No enabled jobs", next_runs=[])

    next_runs = []
    for job in enabled_jobs:
        if job.preview.next_runs:
            next_runs.append(f"{job.name}: {job.preview.next_runs[0]}")
        elif job.preview.error:
            return CronPreview(schedule=None, summary="Invalid job schedule", error=f"{job.name}: {job.preview.error}")

    next_runs.sort()
    return CronPreview(
        schedule=None,
        summary=f"{len(enabled_jobs)} enabled scheduler job(s)",
        next_runs=next_runs,
    )


def build_dashboard_state(repo_root: Path | None = None) -> DashboardState:
    root = repo_root or get_repo_root()
    config_path = get_config_path(root)
    yaml_text = read_config_text(root)
    available_crawlers = discover_crawler_modules(root)
    validation = validate_config_text(yaml_text, available_crawlers=available_crawlers)
    runtime_config = apply_email_env_overrides(validation.data, root) if validation.data is not None else None

    cards: list[CrawlerCard] = []
    global_issues = [issue for issue in validation.issues if issue.scope == "global"]

    configured_crawlers: list[str] = []
    enabled_count = 0
    schedule_samples: set[str] = {
        "0 4 * * *",
        "0 * * * *",
        "15 */3 * * *",
        "0 4,16 * * *",
    }

    if validation.data is not None:
        runtime_data = runtime_config or validation.data
        default_config = runtime_data.get("default", {})
        configured_crawlers = sorted(name for name in validation.data.keys() if name != "default")

        all_names = sorted(set(available_crawlers).union(configured_crawlers))
        for crawler_name in all_names:
            configured = crawler_name in validation.data
            module_present = crawler_name in available_crawlers
            crawler_config = validation.data.get(crawler_name, {}) if configured else {}
            merged = merge_with_defaults(default_config, crawler_config) if configured else {}
            description = (
                crawler_config.get("description")
                if isinstance(crawler_config, dict) and isinstance(crawler_config.get("description"), str)
                else "Crawler module available without dedicated description."
            )

            job_previews = build_crawler_job_previews(default_config, crawler_config, merged) if configured else []
            schedule = merged.get("schedule") if configured and not job_previews else None
            if isinstance(schedule, str):
                schedule_samples.add(schedule)
            for job_preview in job_previews:
                if isinstance(job_preview.schedule, str):
                    schedule_samples.add(job_preview.schedule)

            preview = build_multi_job_preview(job_previews) if job_previews else build_cron_preview(schedule if isinstance(schedule, str) else None)
            enabled = merged.get("enable") if configured and isinstance(merged.get("enable"), bool) else None
            if enabled:
                enabled_count += 1

            schedule_source = None
            if configured and isinstance(crawler_config, dict):
                if job_previews:
                    schedule_source = "jobs"
                elif "schedule" in crawler_config:
                    schedule_source = "crawler"
                elif isinstance(default_config, dict) and "schedule" in default_config:
                    schedule_source = "default"

            card_issues = [
                issue
                for issue in validation.issues
                if issue.scope == crawler_name or issue.scope.startswith(f"{crawler_name}:")
            ]
            state_label, state_variant = _determine_card_state(
                configured=configured,
                module_present=module_present,
                enabled=enabled,
                issue_count=len(card_issues),
            )

            cards.append(
                CrawlerCard(
                    name=crawler_name,
                    description=description,
                    configured=configured,
                    module_present=module_present,
                    enabled=enabled,
                    schema_name=merged.get("schema_name") if configured else None,
                    schedule=schedule if isinstance(schedule, str) else None,
                    schedule_source=schedule_source,
                    preview=preview,
                    job_previews=job_previews,
                    issues=card_issues,
                    state_label=state_label,
                    state_variant=state_variant,
                )
            )

    cards.sort(
        key=lambda card: (
            0 if card.configured else 1,
            0 if card.enabled else 1,
            card.name,
        )
    )

    return DashboardState(
        cards=cards,
        global_issues=global_issues,
        validation=validation,
        available_count=len(available_crawlers),
        configured_count=len(configured_crawlers),
        enabled_count=enabled_count,
        config_path=config_path,
        config_hash=compute_content_hash(yaml_text),
        config_mtime=get_file_mtime_display(config_path),
        time_label=get_local_time_label(),
        server_time=datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        schedule_samples=sorted(schedule_samples),
    )


def get_effective_config_map(
    repo_root: Path | None = None,
) -> tuple[ValidationResult, dict[str, Any], dict[str, dict[str, Any]], list[str]]:
    root = repo_root or get_repo_root()
    yaml_text = read_config_text(root)
    available_crawlers = discover_crawler_modules(root)
    validation = validate_config_text(yaml_text, available_crawlers=available_crawlers)
    runtime_config = apply_email_env_overrides(validation.data, root) if validation.data is not None else None

    default_config: dict[str, Any] = {}
    effective_config_map: dict[str, dict[str, Any]] = {}

    if validation.data is not None:
        runtime_data = runtime_config or validation.data
        default_candidate = runtime_data.get("default", {})
        if isinstance(default_candidate, dict):
            default_config = default_candidate

        for crawler_name, crawler_config in validation.data.items():
            if crawler_name == "default" or not isinstance(crawler_config, dict):
                continue
            effective_config_map[crawler_name] = merge_with_defaults(default_config, crawler_config)

    return validation, default_config, effective_config_map, available_crawlers


def get_crawler_overview(crawler_name: str, repo_root: Path | None = None) -> CrawlerOverview:
    overviews = get_all_crawler_overviews(repo_root)
    return overviews.get(
        crawler_name,
        CrawlerOverview(
            card=None,
            crawler_name=crawler_name,
            raw_config={},
            effective_config={},
            default_config={},
            available_crawlers=[],
            validation=ValidationResult(data=None, issues=[]),
            issues=[],
        ),
    )


def get_all_crawler_overviews(repo_root: Path | None = None) -> dict[str, CrawlerOverview]:
    root = repo_root or get_repo_root()
    dashboard_state = build_dashboard_state(root)
    validation, default_config, effective_config_map, available_crawlers = get_effective_config_map(root)
    raw_configs: dict[str, dict[str, Any]] = {}

    if validation.data is not None:
        for candidate_name, candidate_config in validation.data.items():
            if candidate_name == "default" or not isinstance(candidate_config, dict):
                continue
            raw_configs[candidate_name] = candidate_config

    card_map = {card.name: card for card in dashboard_state.cards}
    all_names = sorted(set(card_map).union(raw_configs).union(effective_config_map))
    issues_by_scope: dict[str, list[ValidationIssue]] = {}
    for issue in validation.issues:
        if issue.scope == "global":
            continue
        crawler_scope = issue.scope.split(":", 1)[0]
        issues_by_scope.setdefault(crawler_scope, []).append(issue)

    overview_map: dict[str, CrawlerOverview] = {}
    for crawler_name in all_names:
        overview_map[crawler_name] = CrawlerOverview(
            card=card_map.get(crawler_name),
            crawler_name=crawler_name,
            raw_config=raw_configs.get(crawler_name, {}),
            effective_config=effective_config_map.get(crawler_name, {}),
            default_config=default_config,
            available_crawlers=available_crawlers,
            validation=validation,
            issues=issues_by_scope.get(crawler_name, []),
        )

    return overview_map


def update_crawler_schedule_config_text(
    crawler_name: str,
    *,
    enabled: bool,
    schedule: str,
    job_name: str | None = None,
    repo_root: Path | None = None,
) -> tuple[str, bool]:
    root = repo_root or get_repo_root()
    yaml_text = read_config_text(root)
    yaml_rt = _create_roundtrip_yaml()
    config_data = yaml_rt.load(yaml_text)

    if config_data is None:
        raise ValueError("CRAWLER_CONFIG.yml is empty.")

    if not isinstance(config_data, dict):
        raise ValueError("The top-level YAML document must be a mapping.")

    created_section = False
    crawler_config = config_data.get(crawler_name)
    normalized_job_name = str(job_name).strip() if job_name else None
    if crawler_config is None:
        if normalized_job_name:
            raise ValueError(f"Crawler section '{crawler_name}' must exist before a scheduler job can be edited.")
        crawler_config = CommentedMap()
        config_data[crawler_name] = crawler_config
        crawler_config["enable"] = enabled
        crawler_config["schema_name"] = DoubleQuotedScalarString(crawler_name)
        crawler_config["schedule"] = DoubleQuotedScalarString(schedule)
        created_section = True
    elif not isinstance(crawler_config, dict):
        raise ValueError(f"Crawler section '{crawler_name}' must be a mapping before it can be edited.")
    elif normalized_job_name:
        jobs_config = crawler_config.get("jobs")
        if not isinstance(jobs_config, dict) or not jobs_config:
            raise ValueError(f"Crawler '{crawler_name}' does not define named scheduler jobs.")

        job_config = jobs_config.get(normalized_job_name)
        if job_config is None:
            raise ValueError(f"Crawler '{crawler_name}' has no scheduler job named '{normalized_job_name}'.")
        if not isinstance(job_config, dict):
            raise ValueError(
                f"Scheduler job '{crawler_name}:{normalized_job_name}' must be a mapping before it can be edited."
            )

        job_config["enable"] = enabled
        job_config["schedule"] = DoubleQuotedScalarString(schedule)
    elif isinstance(crawler_config.get("jobs"), dict):
        raise ValueError(
            f"Crawler '{crawler_name}' uses multiple scheduler jobs. Choose a scheduler job before saving."
        )
    else:
        crawler_config["enable"] = enabled
        crawler_config["schedule"] = DoubleQuotedScalarString(schedule)

    buffer = StringIO()
    yaml_rt.dump(config_data, buffer)
    return buffer.getvalue(), created_section


def update_gapfill_config_text(
    crawler_name: str,
    *,
    enabled: bool,
    script_enabled: bool,
    selected_tables: list[str],
    target_schema: str,
    method: str,
    candidate_periods: list[str],
    donor_context_periods: int,
    donor_search_radius: str,
    refinement_periods: int,
    max_gap_periods: int,
    lookback: str,
    fail_on_table_error: bool,
    table_methods: dict[str, str] | None = None,
    repo_root: Path | None = None,
) -> str:
    root = repo_root or get_repo_root()
    yaml_text = read_config_text(root)
    yaml_rt = _create_roundtrip_yaml()
    config_data = yaml_rt.load(yaml_text)

    if config_data is None:
        raise ValueError("CRAWLER_CONFIG.yml is empty.")

    if not isinstance(config_data, dict):
        raise ValueError("The top-level YAML document must be a mapping.")

    crawler_config = config_data.get(crawler_name)
    if crawler_config is None:
        crawler_config = CommentedMap()
        config_data[crawler_name] = crawler_config
        crawler_config["enable"] = False
        crawler_config["schema_name"] = DoubleQuotedScalarString(crawler_name)
    elif not isinstance(crawler_config, dict):
        raise ValueError(f"Crawler section '{crawler_name}' must be a mapping before it can be edited.")

    post_run_scripts = crawler_config.get("post_run_scripts")
    if post_run_scripts is None:
        post_run_list: list[str] = []
    elif isinstance(post_run_scripts, list):
        post_run_list = [str(item) for item in post_run_scripts]
    else:
        raise ValueError("The crawler 'post_run_scripts' value must be a list before gapfill can be edited.")

    post_run_list = [script for script in post_run_list if script != GAPFILL_POSTRUN_SCRIPT]
    if script_enabled:
        post_run_list.insert(0, GAPFILL_POSTRUN_SCRIPT)
    crawler_config["post_run_scripts"] = [DoubleQuotedScalarString(script) for script in post_run_list]

    gapfill_config = crawler_config.get("gapfill")
    if gapfill_config is None:
        gapfill_config = CommentedMap()
        crawler_config["gapfill"] = gapfill_config
    elif not isinstance(gapfill_config, dict):
        raise ValueError("The crawler 'gapfill' value must be a mapping before it can be edited.")

    gapfill_config["enable"] = enabled
    gapfill_config["target_schema"] = DoubleQuotedScalarString(target_schema)
    gapfill_config["method"] = DoubleQuotedScalarString(method)
    gapfill_config["candidate_periods"] = [DoubleQuotedScalarString(value) for value in candidate_periods]
    gapfill_config["donor_context_periods"] = donor_context_periods
    gapfill_config["donor_search_radius"] = DoubleQuotedScalarString(donor_search_radius)
    gapfill_config["refinement_periods"] = refinement_periods
    gapfill_config["max_gap_periods"] = max_gap_periods
    gapfill_config["lookback"] = DoubleQuotedScalarString(lookback)
    gapfill_config["fail_on_table_error"] = fail_on_table_error
    gapfill_config["tables"] = [DoubleQuotedScalarString(table_name) for table_name in selected_tables]
    method_by_table = table_methods or {}
    table_method_config = CommentedMap()
    for table_name in selected_tables:
        table_method_config[DoubleQuotedScalarString(table_name)] = DoubleQuotedScalarString(
            method_by_table.get(table_name, method)
        )
    gapfill_config["table_methods"] = table_method_config

    buffer = StringIO()
    yaml_rt.dump(config_data, buffer)
    return buffer.getvalue()


def _determine_card_state(
    *,
    configured: bool,
    module_present: bool,
    enabled: bool | None,
    issue_count: int,
) -> tuple[str, str]:
    if issue_count:
        return "Needs attention", "warning"
    if configured and enabled:
        return "Enabled", "enabled"
    if configured and enabled is False:
        return "Disabled", "disabled"
    if configured and not module_present:
        return "Config only", "warning"
    if module_present and not configured:
        return "Module only", "muted"
    return "Unknown", "muted"


def _create_roundtrip_yaml() -> YAML:
    yaml_rt = YAML()
    yaml_rt.preserve_quotes = True
    yaml_rt.indent(mapping=2, sequence=4, offset=2)
    return yaml_rt


def write_config_text_atomic(
    yaml_text: str,
    expected_hash: str | None = None,
    repo_root: Path | None = None,
) -> tuple[bool, str]:
    root = repo_root or get_repo_root()
    config_path = get_config_path(root)
    current_text = read_config_text(root)
    current_hash = compute_content_hash(current_text)

    if expected_hash and current_hash != expected_hash:
        return False, current_hash

    newline = ""
    if yaml_text and not yaml_text.endswith(("\n", "\r")):
        newline = "\r\n" if "\r\n" in yaml_text else "\n"

    payload = yaml_text + newline
    temp_path = config_path.with_suffix(config_path.suffix + ".tmp")

    try:
        with open(temp_path, "w", encoding="utf-8", newline="") as handle:
            handle.write(payload)
        try:
            os.replace(temp_path, config_path)
        except OSError as exc:
            # Docker bind-mounts of single files can reject atomic rename-over-write
            # with EBUSY/EXDEV even though direct writes to the mounted target work.
            if exc.errno not in {errno.EBUSY, errno.EXDEV, errno.EPERM}:
                raise
            with open(config_path, "w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    return True, compute_content_hash(payload)
