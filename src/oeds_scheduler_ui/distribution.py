"""Load crawler registry configuration from a distribution inventory."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from oeds_scheduler_ui.discovery import discover_crawler_specs
from oeds_scheduler_ui.interfaces import CrawlerRegistry, CrawlerSpec


def load_inventory(path: str | Path) -> dict[str, Any]:
    """Load a JSON distribution inventory."""

    inventory_path = Path(path)
    return json.loads(inventory_path.read_text(encoding="utf-8"))


def registries_from_inventory(
    inventory: Mapping[str, Any],
    *,
    workspace_root: str | Path,
) -> list[CrawlerRegistry]:
    """Build prioritized lazy crawler registries from inventory data."""

    root = Path(workspace_root).resolve()
    priority = list(inventory.get("registry_priority", []))
    grouped: dict[str, dict[str, CrawlerSpec]] = {
        source_name: {} for source_name in priority
    }

    registry_sources = inventory.get("registry_sources", [])
    if registry_sources:
        if not isinstance(registry_sources, list):
            raise ValueError("inventory['registry_sources'] must be a list")

        for raw_source in registry_sources:
            if not isinstance(raw_source, Mapping):
                raise ValueError("registry source entries must be mappings")
            source_name = raw_source.get("source_name")
            source_path = raw_source.get("source_path")
            crawler_package_path = raw_source.get("crawler_package_path")
            module_prefix = raw_source.get("module_prefix")
            if not all(
                isinstance(value, str) and value
                for value in (
                    source_name,
                    source_path,
                    crawler_package_path,
                    module_prefix,
                )
            ):
                raise ValueError(f"incomplete registry source entry: {raw_source}")

            discovery = discover_crawler_specs(
                source_name=source_name,
                source_path=(root / source_path).resolve(),
                crawler_package_path=crawler_package_path,
                module_prefix=module_prefix,
            )
            grouped.setdefault(source_name, {}).update(discovery.crawlers)

    pilot = inventory.get("pilot", {})
    if not isinstance(pilot, Mapping):
        raise ValueError("inventory['pilot'] must be a mapping")

    for crawler_name, raw_entry in pilot.items():
        if not isinstance(raw_entry, Mapping):
            raise ValueError(f"inventory pilot entry must be a mapping: {crawler_name}")

        preferred_source = raw_entry.get("preferred_source")
        module = raw_entry.get("module")
        attribute = raw_entry.get("attribute")
        source_path = raw_entry.get("source_path")
        if not all(isinstance(value, str) and value for value in (preferred_source, module, attribute)):
            raise ValueError(f"incomplete crawler inventory entry: {crawler_name}")

        resolved_source_path = (
            (root / source_path).resolve()
            if isinstance(source_path, str) and source_path
            else None
        )
        grouped.setdefault(preferred_source, {})[crawler_name] = CrawlerSpec(
            source_name=preferred_source,
            module=module,
            attribute=attribute,
            source_path=resolved_source_path,
        )

    ordered_sources = [source for source in priority if grouped.get(source)]
    ordered_sources.extend(
        source for source in grouped if source not in priority and grouped.get(source)
    )

    return [
        CrawlerRegistry(source_name=source_name, crawlers=grouped[source_name])
        for source_name in ordered_sources
    ]
