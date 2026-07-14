"""Static crawler discovery from source trees."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from oeds_scheduler_ui.interfaces import CrawlerSpec


CRAWLER_BASE_NAMES = {"BaseCrawler", "ContinuousCrawler", "DownloadOnceCrawler"}
CRAWLER_RUN_METHODS = {"run", "crawl_temporal", "crawl_structural"}


@dataclass(frozen=True)
class DiscoveryIssue:
    """Non-fatal issue found while discovering crawler source files."""

    module_name: str
    path: Path
    reason: str


@dataclass(frozen=True)
class DiscoveryResult:
    """Discovered crawler specs plus non-fatal skipped-file issues."""

    source_name: str
    crawlers: dict[str, CrawlerSpec]
    issues: tuple[DiscoveryIssue, ...]


def _base_name(base: ast.expr) -> str:
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        return base.attr
    return ast.unparse(base)


def _class_methods(class_node: ast.ClassDef) -> set[str]:
    return {
        node.name for node in class_node.body if isinstance(node, ast.FunctionDef)
    }


def _is_crawler_candidate(class_node: ast.ClassDef) -> bool:
    bases = {_base_name(base) for base in class_node.bases}
    if bases & CRAWLER_BASE_NAMES:
        return True

    methods = _class_methods(class_node)
    if methods & CRAWLER_RUN_METHODS and class_node.name.endswith(
        ("Crawler", "Downloader")
    ):
        return True

    return False


def _candidate_score(class_node: ast.ClassDef) -> tuple[int, int, str]:
    bases = {_base_name(base) for base in class_node.bases}
    methods = _class_methods(class_node)
    base_score = 1 if bases & CRAWLER_BASE_NAMES else 0
    method_score = len(methods & CRAWLER_RUN_METHODS)
    return (base_score, method_score, class_node.name)


def _find_crawler_class(path: Path) -> str | None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    candidates = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and _is_crawler_candidate(node)
    ]
    if not candidates:
        return None

    return max(candidates, key=_candidate_score).name


def discover_crawler_specs(
    *,
    source_name: str,
    source_path: str | Path,
    crawler_package_path: str | Path,
    module_prefix: str,
) -> DiscoveryResult:
    """Discover class-based crawler modules below a source tree."""

    root = Path(source_path).resolve()
    crawler_dir = root / crawler_package_path
    crawlers: dict[str, CrawlerSpec] = {}
    issues: list[DiscoveryIssue] = []

    if not crawler_dir.is_dir():
        return DiscoveryResult(
            source_name=source_name,
            crawlers={},
            issues=(
                DiscoveryIssue(
                    module_name="",
                    path=crawler_dir,
                    reason="crawler package directory does not exist",
                ),
            ),
        )

    for module_file in sorted(crawler_dir.glob("*.py")):
        if module_file.name.startswith("__"):
            continue

        registry_name = module_file.stem
        module_name = f"{module_prefix}.{module_file.stem}"
        try:
            class_name = _find_crawler_class(module_file)
        except (OSError, SyntaxError) as exc:
            issues.append(
                DiscoveryIssue(
                    module_name=module_name,
                    path=module_file,
                    reason=str(exc),
                )
            )
            continue

        if class_name is None:
            issues.append(
                DiscoveryIssue(
                    module_name=module_name,
                    path=module_file,
                    reason="no crawler class found",
                )
            )
            continue

        crawlers[registry_name] = CrawlerSpec(
            source_name=source_name,
            module=module_name,
            attribute=class_name,
            source_path=root,
        )

    return DiscoveryResult(
        source_name=source_name,
        crawlers=crawlers,
        issues=tuple(issues),
    )
