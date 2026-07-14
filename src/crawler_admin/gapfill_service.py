from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd
from crawler.common.runtime_env import resolve_database_uri
from oeds_gapfill.config import BUILTIN_GAPFILL_TABLES_BY_JOB, DEFAULT_POSTRUN_TABLES
from oeds_gapfill.core import GAPFILL_METHODS
from oeds_gapfill.selftest import (
    HoldoutTestResult,
    SelfTestResult,
    list_holdout_datasets,
    list_self_test_cases,
    run_holdout_test,
    run_self_tests,
)
from sqlalchemy import bindparam, create_engine, text

GAPFILL_POSTRUN_SCRIPT = "scripts/gapfill_timeseries.py"
GAPFILL_DASHBOARD_NAME = "OEDS Gapfilling Quality"


@dataclass(frozen=True)
class GapfillSelfTestCatalogItem:
    name: str
    description: str
    fault_type: str
    method: str
    expected_filled: int
    source_rows: int
    value_columns: str
    period_label: str
    candidate_periods_label: str


@dataclass(frozen=True)
class GapfillHoldoutDatasetItem:
    name: str
    label: str
    description: str
    method: str
    row_count: int
    max_gap_length: int
    recommended_gap_start: int
    recommended_gap_length: int
    period_label: str
    candidate_periods_label: str


@dataclass(frozen=True)
class GapfillErrorMetric:
    label: str
    value: str


@dataclass(frozen=True)
class GapfillChartMarker:
    cx: str
    cy: str
    label: str


@dataclass(frozen=True)
class GapfillChart:
    test_name: str
    truth_segments: list[str]
    source_segments: list[str]
    gapfilled_segments: list[str]
    filled_markers: list[GapfillChartMarker]
    start_label: str
    end_label: str
    y_min_label: str
    y_max_label: str
    filled_count: int

    @property
    def has_data(self) -> bool:
        return bool(
            self.truth_segments or self.source_segments or self.gapfilled_segments
        )


@dataclass(frozen=True)
class GapfillSelfTestView:
    run_id: str
    checked_at: str
    selected_names: list[str]
    results: list[SelfTestResult]
    charts: list[GapfillChart]
    passed_count: int
    failed_count: int
    series_rows: int

    @property
    def all_passed(self) -> bool:
        return self.failed_count == 0 and bool(self.results)


@dataclass(frozen=True)
class GapfillHoldoutView:
    run_id: str
    checked_at: str
    result: HoldoutTestResult
    charts: list[GapfillChart]
    error_metrics: list[GapfillErrorMetric]
    series_rows: int

    @property
    def passed(self) -> bool:
        return self.result.status == "passed"


@dataclass(frozen=True)
class GapfillRuntimeTableItem:
    table_name: str
    value_columns_label: str
    groupby_columns_label: str
    selected: bool
    method: str
    database_status: str
    database_status_label: str


@dataclass(frozen=True)
class GapfillRuntimeView:
    crawler_name: str
    supported: bool
    source_schema: str
    target_schema: str
    script_enabled: bool
    gapfill_enabled: bool
    method: str
    candidate_periods_label: str
    donor_context_periods: int
    donor_search_radius_label: str
    refinement_periods: int
    max_gap_periods: int
    lookback_label: str
    fail_on_table_error: bool
    metadata_source_label: str
    database_status_note: str
    tables: list[GapfillRuntimeTableItem]
    post_run_scripts: list[str]
    dashboard_name: str | None

    @property
    def selected_table_count(self) -> int:
        return sum(1 for item in self.tables if item.selected)

    @property
    def has_gapfill_script(self) -> bool:
        return self.script_enabled

    @property
    def writes_to_separate_schema(self) -> bool:
        return self.target_schema != self.source_schema


def build_gapfill_selftest_catalog() -> list[GapfillSelfTestCatalogItem]:
    catalog = []
    for test_case in list_self_test_cases():
        catalog.append(
            GapfillSelfTestCatalogItem(
                name=test_case.name,
                description=test_case.description,
                fault_type=test_case.fault_type,
                method=test_case.method,
                expected_filled=test_case.expected_filled,
                source_rows=test_case.source_rows,
                value_columns=", ".join(test_case.config.value_columns),
                period_label=_format_timedelta(test_case.config.period),
                candidate_periods_label=_format_candidate_periods(
                    test_case.config.candidate_periods
                ),
            )
        )
    return catalog


def build_gapfill_holdout_catalog() -> list[GapfillHoldoutDatasetItem]:
    catalog = []
    for dataset in list_holdout_datasets():
        catalog.append(
            GapfillHoldoutDatasetItem(
                name=dataset.name,
                label=dataset.label,
                description=dataset.description,
                method=dataset.method,
                row_count=dataset.row_count,
                max_gap_length=dataset.max_gap_length,
                recommended_gap_start=dataset.recommended_gap_start,
                recommended_gap_length=dataset.recommended_gap_length,
                period_label=_format_timedelta(dataset.config.period),
                candidate_periods_label=_format_candidate_periods(
                    dataset.config.candidate_periods
                ),
            )
        )
    return catalog


def build_gapfill_selftest_view(
    selected_names: Sequence[str] | None = None,
) -> GapfillSelfTestView:
    run_id, results, series = run_self_tests(selected_names)
    charts = _build_charts(series)
    passed_count = sum(1 for result in results if result.status == "passed")
    failed_count = sum(1 for result in results if result.status != "passed")
    return GapfillSelfTestView(
        run_id=run_id,
        checked_at=_extract_checked_at(series),
        selected_names=[result.test_name for result in results],
        results=results,
        charts=charts,
        passed_count=passed_count,
        failed_count=failed_count,
        series_rows=len(series),
    )


def build_gapfill_holdout_view(
    dataset_name: str,
    gap_length_periods: int,
    *,
    gap_start_index: int | None = None,
    fault_type: str = "value_gap",
    method: str | None = None,
) -> GapfillHoldoutView:
    run_id, result, series = run_holdout_test(
        dataset_name,
        gap_length_periods,
        gap_start_index=gap_start_index,
        fault_type=fault_type,
        method=method,
    )
    return GapfillHoldoutView(
        run_id=run_id,
        checked_at=_extract_checked_at(series),
        result=result,
        charts=_build_charts(series),
        error_metrics=_build_error_metrics(result),
        series_rows=len(series),
    )


def gapfill_method_options() -> list[str]:
    return list(GAPFILL_METHODS)


def build_gapfill_runtime_view(
    crawler_name: str,
    raw_config: dict[str, object] | None,
    effective_config: dict[str, object] | None,
    *,
    check_database_tables: bool = False,
) -> GapfillRuntimeView:
    raw = raw_config if isinstance(raw_config, dict) else {}
    effective = effective_config if isinstance(effective_config, dict) else {}
    raw_gapfill = raw.get("gapfill") if isinstance(raw.get("gapfill"), dict) else {}
    effective_gapfill = (
        effective.get("gapfill") if isinstance(effective.get("gapfill"), dict) else {}
    )
    source_schema = str(effective.get("schema_name") or crawler_name)
    target_schema = str(
        effective_gapfill.get("target_schema") or f"{source_schema}_gapfilled"
    )
    scripts = (
        list(effective.get("post_run_scripts") or [])
        if isinstance(effective.get("post_run_scripts"), list)
        else []
    )
    supported_tables = BUILTIN_GAPFILL_TABLES_BY_JOB.get(crawler_name, ())
    default_method = str(effective_gapfill.get("method") or "donor_refined")
    table_methods = _extract_table_methods(effective_gapfill)
    table_names = [table.table_name for table in supported_tables]
    if check_database_tables:
        database_presence, database_status_note = _load_source_table_presence(
            effective,
            source_schema,
            table_names,
        )
    else:
        database_presence = {table_name: None for table_name in table_names}
        database_status_note = (
            "Database table status is not checked while opening Settings. "
            "Use Check DB tables to verify the current source schema."
        )

    if isinstance(raw_gapfill, dict) and "tables" in raw_gapfill:
        selected_names = {str(name) for name in (raw_gapfill.get("tables") or [])}
    elif isinstance(effective_gapfill, dict) and "tables" in effective_gapfill:
        selected_names = {str(name) for name in (effective_gapfill.get("tables") or [])}
    else:
        selected_names = set(
            DEFAULT_POSTRUN_TABLES if crawler_name == "entsoe_fms" else []
        )

    tables = [
        GapfillRuntimeTableItem(
            table_name=table.table_name,
            value_columns_label=", ".join(table.value_columns),
            groupby_columns_label=", ".join(table.groupby_columns),
            selected=table.table_name in selected_names,
            method=table_methods.get(table.table_name, default_method),
            database_status=_database_status(table.table_name, database_presence),
            database_status_label=_database_status_label(
                _database_status(table.table_name, database_presence)
            ),
        )
        for table in supported_tables
    ]

    return GapfillRuntimeView(
        crawler_name=crawler_name,
        supported=bool(supported_tables),
        source_schema=source_schema,
        target_schema=target_schema,
        script_enabled=GAPFILL_POSTRUN_SCRIPT in scripts,
        gapfill_enabled=bool(
            effective_gapfill.get("enable", False) if effective_gapfill else False
        ),
        method=default_method,
        candidate_periods_label=_format_period_config(
            effective_gapfill.get("candidate_periods"), fallback="24h, 7d"
        ),
        donor_context_periods=int(effective_gapfill.get("donor_context_periods", 6)),
        donor_search_radius_label=str(
            effective_gapfill.get("donor_search_radius") or "28d"
        ),
        refinement_periods=int(effective_gapfill.get("refinement_periods", 3)),
        max_gap_periods=int(effective_gapfill.get("max_gap_periods", 24)),
        lookback_label=str(effective_gapfill.get("lookback") or "7d"),
        fail_on_table_error=bool(effective_gapfill.get("fail_on_table_error", True)),
        metadata_source_label=(
            "Built-in ENTSO-E FMS table metadata"
            if crawler_name == "entsoe_fms"
            else "Built-in table metadata"
        ),
        database_status_note=database_status_note,
        tables=tables,
        post_run_scripts=scripts,
        dashboard_name=GAPFILL_DASHBOARD_NAME if supported_tables else None,
    )


def _extract_table_methods(gapfill_config: object) -> dict[str, str]:
    if not isinstance(gapfill_config, dict):
        return {}
    raw_methods = gapfill_config.get("table_methods")
    if not isinstance(raw_methods, dict):
        return {}
    return {
        str(table_name): str(method)
        for table_name, method in raw_methods.items()
        if str(table_name).strip() and str(method).strip()
    }


def _load_source_table_presence(
    effective_config: dict[str, object],
    source_schema: str,
    table_names: list[str],
) -> tuple[dict[str, bool | None], str]:
    if not table_names:
        return {}, "No built-in gapfill tables are configured for this crawler."

    database_uri = str(effective_config.get("database_uri") or "").strip()
    if not database_uri:
        return (
            {table_name: None for table_name in table_names},
            "Database table status unavailable because no database URI is configured.",
        )

    try:
        resolved_uri = resolve_database_uri(database_uri)
        engine = create_engine(
            resolved_uri,
            connect_args=_connect_args_for_database_uri(resolved_uri),
        )
        statement = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name IN :table_names
            """
        ).bindparams(bindparam("table_names", expanding=True))
        with engine.connect() as connection:
            present = {
                str(row[0])
                for row in connection.execute(
                    statement,
                    {"schema": source_schema, "table_names": table_names},
                )
            }
    except Exception:
        return (
            {table_name: None for table_name in table_names},
            "Database table status could not be checked. The list below is still the supported gapfill metadata list.",
        )

    return (
        {table_name: table_name in present for table_name in table_names},
        "Database table status was checked against the configured source schema.",
    )


def _connect_args_for_database_uri(database_uri: str) -> dict[str, int]:
    if database_uri.startswith(("postgresql://", "postgresql+psycopg2://")):
        return {"connect_timeout": 1}
    return {}


def _database_status(
    table_name: str,
    database_presence: dict[str, bool | None],
) -> str:
    present = database_presence.get(table_name)
    if present is True:
        return "present"
    if present is False:
        return "missing"
    return "unknown"


def _database_status_label(status: str) -> str:
    if status == "present":
        return "DB table present"
    if status == "missing":
        return "Not in source DB"
    return "DB not checked"


def _build_error_metrics(result: HoldoutTestResult) -> list[GapfillErrorMetric]:
    return [
        GapfillErrorMetric("MAE", _format_optional_number(result.mean_absolute_error)),
        GapfillErrorMetric(
            "RMSE", _format_optional_number(result.root_mean_squared_error)
        ),
        GapfillErrorMetric(
            "Max abs.", _format_optional_number(result.max_absolute_error)
        ),
        GapfillErrorMetric(
            "MAPE",
            _format_optional_number(result.mean_absolute_percentage_error, suffix="%"),
        ),
        GapfillErrorMetric(
            "Compared", f"{result.compared_points} / {result.expected_points}"
        ),
        GapfillErrorMetric("Filled", str(result.actual_filled)),
    ]


def _build_charts(series: pd.DataFrame) -> list[GapfillChart]:
    if series.empty:
        return []

    charts = []
    ordered_names = list(dict.fromkeys(series["test_name"].astype(str).tolist()))
    for test_name in ordered_names:
        test_frame = series[series["test_name"] == test_name].copy()
        test_frame["time"] = pd.to_datetime(test_frame["time"], utc=True)
        finite_values = test_frame["value"].dropna().astype(float)
        if finite_values.empty:
            continue

        start_time = test_frame["time"].min()
        end_time = test_frame["time"].max()
        y_min = float(finite_values.min())
        y_max = float(finite_values.max())
        y_span = y_max - y_min
        if math.isclose(y_span, 0.0):
            y_span = 1.0

        truth_frame = test_frame[test_frame["series_name"] == "truth"]
        source_frame = test_frame[test_frame["series_name"] == "source"]
        gapfilled_frame = test_frame[test_frame["series_name"] == "gapfilled"]
        filled_frame = gapfilled_frame[gapfilled_frame["was_filled"].astype(bool)]

        charts.append(
            GapfillChart(
                test_name=test_name,
                truth_segments=_build_segments(
                    truth_frame, start_time, end_time, y_min, y_span
                ),
                source_segments=_build_segments(
                    source_frame, start_time, end_time, y_min, y_span
                ),
                gapfilled_segments=_build_segments(
                    gapfilled_frame, start_time, end_time, y_min, y_span
                ),
                filled_markers=_build_markers(
                    filled_frame, start_time, end_time, y_min, y_span
                ),
                start_label=_format_timestamp(start_time),
                end_label=_format_timestamp(end_time),
                y_min_label=_format_number(y_min),
                y_max_label=_format_number(y_max),
                filled_count=len(filled_frame),
            )
        )

    return charts


def _build_segments(
    frame: pd.DataFrame,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    y_min: float,
    y_span: float,
) -> list[str]:
    segments: list[str] = []
    current_points: list[str] = []

    for _, row in frame.sort_values("time").iterrows():
        value = row["value"]
        if pd.isna(value):
            if len(current_points) > 1:
                segments.append(" ".join(current_points))
            current_points = []
            continue
        current_points.append(
            _point_string(
                pd.Timestamp(row["time"]),
                float(value),
                start_time,
                end_time,
                y_min,
                y_span,
            )
        )

    if len(current_points) > 1:
        segments.append(" ".join(current_points))

    return segments


def _build_markers(
    frame: pd.DataFrame,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    y_min: float,
    y_span: float,
) -> list[GapfillChartMarker]:
    markers = []
    for _, row in frame.sort_values("time").iterrows():
        value = row["value"]
        if pd.isna(value):
            continue
        timestamp = pd.Timestamp(row["time"])
        x, y = _point(timestamp, float(value), start_time, end_time, y_min, y_span)
        markers.append(
            GapfillChartMarker(
                cx=f"{x:.2f}",
                cy=f"{y:.2f}",
                label=f"{_format_timestamp(timestamp)}: {_format_number(float(value))}",
            )
        )
    return markers


def _point_string(
    timestamp: pd.Timestamp,
    value: float,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    y_min: float,
    y_span: float,
) -> str:
    x, y = _point(timestamp, value, start_time, end_time, y_min, y_span)
    return f"{x:.2f},{y:.2f}"


def _point(
    timestamp: pd.Timestamp,
    value: float,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    y_min: float,
    y_span: float,
) -> tuple[float, float]:
    total_seconds = max((end_time - start_time).total_seconds(), 1.0)
    elapsed_seconds = max((timestamp - start_time).total_seconds(), 0.0)
    x = min(max(elapsed_seconds / total_seconds * 100.0, 0.0), 100.0)
    y = 38.0 - ((value - y_min) / y_span * 34.0)
    return x, min(max(y, 4.0), 38.0)


def _extract_checked_at(series: pd.DataFrame) -> str:
    if series.empty or "checked_at" not in series:
        return ""
    value = pd.to_datetime(series["checked_at"].iloc[0], utc=True)
    return _format_timestamp(value)


def _format_candidate_periods(periods: tuple[pd.Timedelta, ...] | None) -> str:
    if not periods:
        return "-"
    return ", ".join(_format_timedelta(period) for period in periods)


def _format_period_config(value: object, *, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, str | int | float):
        return str(value)
    if isinstance(value, tuple | list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(parts) if parts else fallback
    return str(value)


def _format_timedelta(value: pd.Timedelta) -> str:
    seconds = int(value.total_seconds())
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}min"
    return f"{seconds}s"


def _format_timestamp(value: pd.Timestamp) -> str:
    return value.strftime("%Y-%m-%d %H:%M UTC")


def _format_optional_number(value: float | None, *, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{_format_number(value)}{suffix}"


def _format_number(value: float) -> str:
    if math.isclose(value, round(value), abs_tol=0.0001):
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")
