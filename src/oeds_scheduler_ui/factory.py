"""Crawler factory and compatibility audit helpers."""

from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from oeds_scheduler_ui.interfaces import (
    CrawlerSpec,
    OedsCrawlerConfig,
    load_crawler_target,
)


CONSTRUCTOR_CRAWLER_NAME_CONFIG = "crawler_name_config"
CONSTRUCTOR_SCHEMA_NAME_CONFIG = "schema_name_config"
CONSTRUCTOR_SCHEMA_NAME_ONLY = "schema_name_only"
CONSTRUCTOR_UNKNOWN = "unknown"


@dataclass(frozen=True)
class ConstructorAudit:
    """Static constructor compatibility result for one crawler target."""

    crawler_name: str
    target: CrawlerSpec
    module_file: Path | None
    class_name: str | None
    bases: tuple[str, ...]
    methods: tuple[str, ...]
    init_parameters: tuple[str, ...]
    constructor_style: str
    import_required_for_details: bool = False
    error: str | None = None

    @property
    def has_supported_constructor(self) -> bool:
        return self.constructor_style in {
            CONSTRUCTOR_CRAWLER_NAME_CONFIG,
            CONSTRUCTOR_SCHEMA_NAME_CONFIG,
            CONSTRUCTOR_SCHEMA_NAME_ONLY,
        }

    @property
    def run_methods(self) -> tuple[str, ...]:
        detected = [
            method
            for method in ("run", "crawl_temporal", "crawl_structural")
            if method in self.methods
        ]
        if "ContinuousCrawler" in self.bases and "crawl_temporal" not in detected:
            detected.append("crawl_temporal")
        if "DownloadOnceCrawler" in self.bases and "crawl_structural" not in detected:
            detected.append("crawl_structural")
        return tuple(detected)


@dataclass(frozen=True)
class ConstructorPlan:
    """Dry constructor plan for one crawler."""

    crawler_name: str
    constructor_style: str
    args: tuple[Any, ...]
    kwargs: Mapping[str, Any]


def module_file_from_spec(spec: CrawlerSpec) -> Path | None:
    """Resolve a Python source file for a lazy crawler spec when possible."""

    if spec.source_path is None:
        return None

    module_parts = spec.module.split(".")
    module_path = spec.source_path.joinpath(*module_parts)
    module_file = module_path.with_suffix(".py")
    if module_file.is_file():
        return module_file

    package_file = module_path / "__init__.py"
    if package_file.is_file():
        return package_file

    return None


def _base_name(base: ast.expr) -> str:
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        return base.attr
    return ast.unparse(base)


def _function_parameters(function: ast.FunctionDef) -> tuple[str, ...]:
    return tuple(arg.arg for arg in function.args.args if arg.arg != "self")


def _callable_parameters(function: Any) -> tuple[str, ...]:
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return ()

    parameters: list[str] = []
    for parameter in signature.parameters.values():
        if parameter.name == "self":
            continue
        if parameter.kind in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }:
            parameters.append(parameter.name)
    return tuple(parameters)


def _find_class(tree: ast.AST, class_name: str) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    return None


def _infer_constructor_style(
    spec: CrawlerSpec,
    bases: tuple[str, ...],
    init_parameters: tuple[str, ...],
) -> str:
    if init_parameters[:2] == ("crawler_name", "config"):
        return CONSTRUCTOR_CRAWLER_NAME_CONFIG
    if init_parameters[:2] == ("schema_name", "config"):
        return CONSTRUCTOR_SCHEMA_NAME_CONFIG
    if init_parameters == ("schema_name",):
        return CONSTRUCTOR_SCHEMA_NAME_ONLY

    if init_parameters:
        return CONSTRUCTOR_UNKNOWN

    if spec.module.startswith("oeds.") or any(
        base in {"ContinuousCrawler", "DownloadOnceCrawler"} for base in bases
    ):
        return CONSTRUCTOR_SCHEMA_NAME_CONFIG

    if spec.module.startswith("crawler."):
        return CONSTRUCTOR_CRAWLER_NAME_CONFIG

    return CONSTRUCTOR_UNKNOWN


def audit_crawler_spec(crawler_name: str, target: CrawlerSpec) -> ConstructorAudit:
    """Statically inspect a crawler target without importing crawler dependencies."""

    module_file = module_file_from_spec(target)
    if module_file is None:
        return ConstructorAudit(
            crawler_name=crawler_name,
            target=target,
            module_file=None,
            class_name=None,
            bases=(),
            methods=(),
            init_parameters=(),
            constructor_style=CONSTRUCTOR_UNKNOWN,
            import_required_for_details=True,
            error="module source file could not be resolved",
        )

    try:
        tree = ast.parse(module_file.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as exc:
        return ConstructorAudit(
            crawler_name=crawler_name,
            target=target,
            module_file=module_file,
            class_name=None,
            bases=(),
            methods=(),
            init_parameters=(),
            constructor_style=CONSTRUCTOR_UNKNOWN,
            error=str(exc),
        )

    class_node = _find_class(tree, target.attribute)
    if class_node is None:
        return ConstructorAudit(
            crawler_name=crawler_name,
            target=target,
            module_file=module_file,
            class_name=None,
            bases=(),
            methods=(),
            init_parameters=(),
            constructor_style=CONSTRUCTOR_UNKNOWN,
            error=f"class {target.attribute!r} was not found",
        )

    bases = tuple(_base_name(base) for base in class_node.bases)
    methods = tuple(
        node.name for node in class_node.body if isinstance(node, ast.FunctionDef)
    )
    init_node = next(
        (
            node
            for node in class_node.body
            if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        ),
        None,
    )
    init_parameters = _function_parameters(init_node) if init_node else ()
    constructor_style = _infer_constructor_style(target, bases, init_parameters)

    return ConstructorAudit(
        crawler_name=crawler_name,
        target=target,
        module_file=module_file,
        class_name=class_node.name,
        bases=bases,
        methods=methods,
        init_parameters=init_parameters,
        constructor_style=constructor_style,
    )


class CrawlerFactory:
    """Resolve, audit, and construct crawler instances from merged registries."""

    def __init__(self, crawlers: Mapping[str, Any]):
        self._crawlers = dict(crawlers)

    def list_crawlers(self) -> tuple[str, ...]:
        return tuple(sorted(self._crawlers))

    def get_target(self, crawler_name: str) -> Any:
        try:
            return self._crawlers[crawler_name]
        except KeyError as exc:
            raise KeyError(f"unknown crawler: {crawler_name}") from exc

    def audit(self, crawler_name: str) -> ConstructorAudit:
        target = self.get_target(crawler_name)
        if isinstance(target, CrawlerSpec):
            return audit_crawler_spec(crawler_name, target)

        bases = tuple(base.__name__ for base in target.__bases__)
        methods = tuple(name for name in dir(target) if not name.startswith("_"))
        init_parameters = _callable_parameters(target.__init__)
        constructor_style = _infer_constructor_style(
            CrawlerSpec(
                source_name="loaded-class",
                module=target.__module__,
                attribute=target.__name__,
            ),
            bases,
            init_parameters,
        )
        return ConstructorAudit(
            crawler_name=crawler_name,
            target=CrawlerSpec(
                source_name="loaded-class",
                module=target.__module__,
                attribute=target.__name__,
            ),
            module_file=None,
            class_name=target.__name__,
            bases=bases,
            methods=methods,
            init_parameters=init_parameters,
            constructor_style=constructor_style,
            import_required_for_details=True,
        )

    def constructor_plan(
        self,
        crawler_name: str,
        config: OedsCrawlerConfig,
    ) -> ConstructorPlan:
        audit = self.audit(crawler_name)
        if not audit.has_supported_constructor:
            raise ValueError(
                f"{crawler_name} has unsupported constructor: "
                f"{audit.constructor_style}"
            )

        legacy_config = config.as_legacy_dict()
        if audit.constructor_style == CONSTRUCTOR_CRAWLER_NAME_CONFIG:
            args = (crawler_name, legacy_config)
        elif audit.constructor_style == CONSTRUCTOR_SCHEMA_NAME_CONFIG:
            args = (config.schema_name, legacy_config)
        else:
            args = (config.schema_name,)

        return ConstructorPlan(
            crawler_name=crawler_name,
            constructor_style=audit.constructor_style,
            args=args,
            kwargs={},
        )

    def construct(self, crawler_name: str, config: OedsCrawlerConfig) -> Any:
        """Instantiate a crawler. This may open DB connections in crawler code."""

        plan = self.constructor_plan(crawler_name, config)
        crawler_class = load_crawler_target(self.get_target(crawler_name))
        return crawler_class(*plan.args, **dict(plan.kwargs))
