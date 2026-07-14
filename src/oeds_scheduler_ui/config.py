"""Scheduler configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def load_scheduler_config(path: str | Path) -> dict[str, Any]:
    """Load a scheduler YAML config file."""

    config_path = Path(path)
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency is optional here.
        raise RuntimeError("pyyaml is required to load scheduler YAML config") from exc

    with config_path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file)

    if loaded is None:
        return {}
    if not isinstance(loaded, Mapping):
        raise ValueError(f"scheduler config must be a mapping: {config_path}")
    return dict(loaded)


def file_signature(path: str | Path) -> tuple[int, int] | None:
    """Return a cheap change signature for a file."""

    config_path = Path(path)
    try:
        stat = config_path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_mtime_ns, stat.st_size)
