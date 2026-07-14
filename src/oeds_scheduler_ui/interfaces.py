"""Initial interface layer between scheduler runtime and OEDS crawlers."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class OedsCrawlerConfig:
    """Minimal config shape that shared OEDS crawlers should understand."""

    schema_name: str
    db_uri: str
    source: Mapping[str, Any] = field(default_factory=dict)
    options: Mapping[str, Any] = field(default_factory=dict)

    def as_legacy_dict(self) -> dict[str, Any]:
        """Return a dict compatible with current OEDS-style crawler config."""

        return {
            "schema_name": self.schema_name,
            "db_uri": self.db_uri,
            "database_uri": self.db_uri,
            **dict(self.source),
            **dict(self.options),
        }


@dataclass(frozen=True)
class SchedulerJob:
    """Scheduler-owned job metadata kept outside shared crawler code."""

    name: str
    crawler_name: str
    schema_name: str
    schedule: str | None
    enabled: bool
    post_run_commands: Sequence[str] = ()
    crawler_config: OedsCrawlerConfig | None = None


@dataclass(frozen=True)
class CrawlerRegistry:
    """Named crawler classes from one source, such as core or crawler-pack."""

    source_name: str
    crawlers: Mapping[str, Any]


@dataclass(frozen=True)
class CrawlerSpec:
    """Lazy crawler target reference.

    Keeping crawler imports lazy lets the scheduler list and prioritize crawlers
    without importing every optional crawler dependency at process startup.
    """

    source_name: str
    module: str
    attribute: str
    source_path: Path | None = None

    @classmethod
    def parse(
        cls,
        source_name: str,
        target: str,
        source_path: str | Path | None = None,
    ) -> "CrawlerSpec":
        if ":" not in target:
            raise ValueError(f"Crawler target must use 'module:attribute': {target}")
        module, attribute = target.split(":", 1)
        if not module or not attribute:
            raise ValueError(f"Crawler target must use 'module:attribute': {target}")
        return cls(
            source_name=source_name,
            module=module,
            attribute=attribute,
            source_path=Path(source_path).resolve() if source_path else None,
        )


def merge_crawler_registries(
    registries: Sequence[CrawlerRegistry],
) -> dict[str, Any]:
    """Merge registries in priority order.

    Earlier registries win. This lets an enhanced crawler pack preserve KIT
    behavior while upstream OEDS catches up.
    """

    merged: dict[str, Any] = {}
    for registry in registries:
        for name, crawler_class in registry.crawlers.items():
            if name not in merged:
                merged[name] = crawler_class
    return merged


def registry_from_spec_strings(
    source_name: str,
    crawler_specs: Mapping[str, str],
    source_path: str | Path | None = None,
) -> CrawlerRegistry:
    """Build a lazy registry from ``name -> module:attribute`` strings."""

    return CrawlerRegistry(
        source_name=source_name,
        crawlers={
            name: CrawlerSpec.parse(source_name, target, source_path)
            for name, target in crawler_specs.items()
        },
    )


@contextmanager
def _temporary_sys_path(path: Path | None):
    if path is None:
        yield
        return

    resolved = str(path)
    if resolved in sys.path:
        yield
        return

    sys.path.insert(0, resolved)
    try:
        yield
    finally:
        try:
            sys.path.remove(resolved)
        except ValueError:
            pass


def load_crawler_target(crawler_target: Any) -> type:
    """Resolve a crawler class from a lazy spec or return an existing class."""

    if isinstance(crawler_target, type):
        return crawler_target
    if not isinstance(crawler_target, CrawlerSpec):
        raise TypeError(f"Unsupported crawler target: {crawler_target!r}")

    with _temporary_sys_path(crawler_target.source_path):
        module = importlib.import_module(crawler_target.module)
    crawler_class = getattr(module, crawler_target.attribute)
    if not isinstance(crawler_class, type):
        raise TypeError(
            f"{crawler_target.module}:{crawler_target.attribute} is not a class"
        )
    return crawler_class


def normalize_crawler_config(raw_config: Mapping[str, Any]) -> OedsCrawlerConfig:
    """Convert scheduler YAML config into the shared crawler config shape."""

    schema_name = raw_config.get("schema_name")
    if not isinstance(schema_name, str) or not schema_name:
        raise ValueError("schema_name is required")

    db_uri = raw_config.get("db_uri") or raw_config.get("database_uri")
    if not isinstance(db_uri, str) or not db_uri:
        raise ValueError("db_uri or database_uri is required")

    scheduler_keys = {
        "enable",
        "enabled",
        "jobs",
        "post_run_commands",
        "post_run_scripts",
        "run_post_scripts",
        "schedule",
        "schema_name",
        "db_uri",
        "database_uri",
    }

    source = raw_config.get("source", {})
    if source is None:
        source = {}
    if not isinstance(source, Mapping):
        raise ValueError("source must be a mapping when provided")

    options = {
        key: value
        for key, value in raw_config.items()
        if key not in scheduler_keys and key != "source"
    }

    return OedsCrawlerConfig(
        schema_name=schema_name,
        db_uri=db_uri,
        source=dict(source),
        options=options,
    )


def run_crawler_instance(crawler: Any) -> Any:
    """Run old and new OEDS crawler shapes through one scheduler path."""

    if hasattr(crawler, "run"):
        return crawler.run()
    if hasattr(crawler, "crawl_temporal"):
        return crawler.crawl_temporal()
    if hasattr(crawler, "crawl_structural"):
        return crawler.crawl_structural()
    raise TypeError(f"{type(crawler).__name__} has no supported run method")
