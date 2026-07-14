from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime
from importlib import import_module
from inspect import isclass
from pathlib import Path
from typing import Any

from oeds_gapfill.config import BUILTIN_GAPFILL_TABLES_BY_JOB

from crawler_admin.config_service import (
    CrawlerOverview,
    get_crawler_overview,
    get_repo_root,
)

FMS_ACTIVE_DATA_ITEMS = (
    (
        "AggregatedFillingRateOfWaterReservoirsAndHydroStoragePlants_16.1.D_r3",
        "AggregatedFillingRateOfWaterReservoirsAndHydroStoragePlants",
    ),
    ("ActualTotalLoad_6.1.A_r3", "ActualTotalLoad"),
    ("DayAheadTotalLoadForecast_6.1.B_r3", "DayAheadTotalLoadForecast"),
    (
        "ActualGenerationOutputPerGenerationUnit_16.1.A_r3",
        "ActualGenerationOutputPerGenerationUnit",
    ),
    ("AggregatedGenerationPerType_16.1.B_C_r3", "AggregatedGenerationPerType"),
    ("CommercialSchedulesNetPositions_12.1.F_r3", "CommercialSchedulesNetPositions"),
    ("DayAheadAggregatedGeneration_14.1.C_r3", "DayAheadAggregatedGeneration"),
    (
        "GenerationForecastsForWindAndSolar_14.1.D_r3",
        "GenerationForecastsForWindAndSolar",
    ),
    ("EnergyPrices_12.1.D_r3", "EnergyPrices"),
    ("ExpansionAndDismantlingProjects_9.1_r3", "ExpansionAndDismantlingProjects"),
    ("ForecastedTransferCapacities_11.1_r3", "ForecastedTransferCapacities"),
    (
        "InstalledGenerationCapacityPerProductionUnit_14.1.B_r3",
        "InstalledGenerationCapacityPerProductionUnit",
    ),
    (
        "InstalledGenerationCapacityAggregated_14.1.A_r3",
        "InstalledGenerationCapacityAggregated",
    ),
    ("PhysicalFlows_12.1.G_r3", "PhysicalFlows"),
    ("ProductionAndGenerationUnits_r3", "ProductionAndGenerationUnits"),
    ("TotalCapacityAlreadyAllocated_12.1.C_r3", "TotalCapacityAlreadyAllocated"),
    ("TotalCapacityNominated_12.1.B_r3", "TotalCapacityNominated"),
    ("TotalLoadForecast_6.1.C_D_E_r3", "TotalLoadForecast"),
    ("TransmissionAssets_r3", "TransmissionAssets"),
    (
        "UnavailabilityInTheTransmissionGrid_10.1.A_B_r3",
        "UnavailabilityInTheTransmissionGrid",
    ),
    ("UnavailabilityOfConsumptionUnits_7.1.A_B_r3", "UnavailabilityOfConsumptionUnits"),
    ("UnavailabilityOfOffshoreGrid_10.1.C_r3", "UnavailabilityOfOffshoreGrid"),
    (
        "UnavailabilityOfProductionAndGenerationUnits_15.1.A_B_C_D_r3",
        "UnavailabilityOfProductionAndGenerationUnits",
    ),
    ("UseOfTransferCapacity_12.1.A_r3", "UseOfTransferCapacity"),
    ("YearAheadForecastMargin_8.1_r3", "YearAheadForecastMargin"),
)

FMS_SINGLE_FILE_DATA_ITEMS = {
    "InstalledGenerationCapacityAggregated_14.1.A_r3",
    "InstalledGenerationCapacityPerProductionUnit_14.1.B_r3",
    "ProductionAndGenerationUnits_r3",
    "TransmissionAssets_r3",
    "YearAheadForecastMargin_8.1_r3",
}

FMS_ANNUAL_FILE_DATA_ITEMS = {
    "AggregatedFillingRateOfWaterReservoirsAndHydroStoragePlants_16.1.D_r3",
    "ExpansionAndDismantlingProjects_9.1_r3",
}


@dataclass(frozen=True)
class ActionOption:
    value: str
    label: str


@dataclass(frozen=True)
class ActionField:
    name: str
    label: str
    input_type: str
    required: bool = False
    help_text: str | None = None
    placeholder: str | None = None
    options: list[ActionOption] = field(default_factory=list)
    default_value: Any = None
    min_value: int | None = None
    max_value: int | None = None


@dataclass(frozen=True)
class ActionDefinition:
    action_id: str
    label: str
    description: str
    button_label: str
    fields: list[ActionField] = field(default_factory=list)
    note: str | None = None


@dataclass(frozen=True)
class RunRecord:
    run_id: int
    crawler_name: str
    action_id: str
    action_label: str
    trigger_source: str
    status: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    duration_seconds: float | None
    log_path: str
    summary: str | None
    error_message: str | None
    config_overrides: dict[str, Any]
    action_payload: dict[str, Any]

    @property
    def is_active(self) -> bool:
        return self.status in {"queued", "running"}


@dataclass(frozen=True)
class ActionRuntimeBenchmark:
    action_id: str
    action_label: str
    sample_count: int
    success_count: int
    failure_count: int
    success_rate: float
    latest_seconds: float | None
    latest_finished_at: str | None


@dataclass(frozen=True)
class PreparedAction:
    crawler_name: str
    action_id: str
    action_label: str
    description: str
    effective_config: dict[str, Any]
    config_overrides: dict[str, Any]
    action_payload: dict[str, Any]
    run_post_scripts: bool
    executor: Callable[[Any], str | None]
    requires_crawler: bool = True


class ActionValidationError(Exception):
    def __init__(self, errors: list[str]):
        super().__init__("Action validation failed.")
        self.errors = errors


class LockError(Exception):
    pass


class _LogWriter(io.TextIOBase):
    def __init__(self, handle: io.TextIOBase):
        self.handle = handle

    def write(self, data: str) -> int:
        if self.handle.closed:
            return 0
        self.handle.write(data)
        self.handle.flush()
        return len(data)

    def flush(self) -> None:
        if self.handle.closed:
            return
        self.handle.flush()


class CrawlerRunService:
    def __init__(self, repo_root: Path | None = None):
        self.repo_root = repo_root or get_repo_root()
        self.state_dir = self._resolve_state_dir()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self.state_dir / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.state_dir / "runs.sqlite3"
        self._guard = threading.Lock()
        self._active_runs: dict[str, int] = {}
        self._active_threads: dict[int, threading.Thread] = {}
        self._init_db()
        self._mark_interrupted_runs()

    def _resolve_state_dir(self) -> Path:
        explicit_dir = os.getenv("OEDS_ADMIN_STATE_DIR")
        if explicit_dir:
            return Path(explicit_dir)

        if os.name == "nt":
            local_app_data = os.getenv("LOCALAPPDATA")
            if local_app_data:
                return Path(local_app_data) / "OEDS" / "crawler-admin"

        return Path.home() / ".oeds-crawler-admin"

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS crawler_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    crawler_name TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    action_label TEXT NOT NULL,
                    trigger_source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    duration_seconds REAL,
                    log_path TEXT NOT NULL,
                    summary TEXT,
                    error_message TEXT,
                    config_overrides_json TEXT NOT NULL,
                    action_payload_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_crawler_runs_name_created
                ON crawler_runs (crawler_name, created_at DESC)
                """
            )
            connection.commit()

    def _mark_interrupted_runs(self) -> None:
        timestamp = self._utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE crawler_runs
                SET status = 'interrupted',
                    finished_at = COALESCE(finished_at, ?),
                    summary = COALESCE(summary, 'Admin service restarted while the run was active.'),
                    error_message = COALESCE(error_message, 'Admin service restarted while the run was active.')
                WHERE status IN ('queued', 'running')
                """,
                (timestamp,),
            )
            connection.commit()

    def list_runs(
        self, crawler_name: str | None = None, limit: int = 20
    ) -> list[RunRecord]:
        query = """
            SELECT *
            FROM crawler_runs
        """
        parameters: list[Any] = []

        if crawler_name:
            query += " WHERE crawler_name = ?"
            parameters.append(crawler_name)

        query += " ORDER BY run_id DESC LIMIT ?"
        parameters.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()

        return [self._row_to_record(row) for row in rows]

    def get_run(self, run_id: int) -> RunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM crawler_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()

        return self._row_to_record(row) if row else None

    def get_latest_runs_map(self) -> dict[str, RunRecord]:
        records = self.list_runs(limit=200)
        latest_by_crawler: dict[str, RunRecord] = {}
        for record in records:
            latest_by_crawler.setdefault(record.crawler_name, record)
        return latest_by_crawler

    def get_runtime_benchmarks(
        self,
        crawler_name: str,
        limit: int = 100,
    ) -> list[ActionRuntimeBenchmark]:
        records = [
            record
            for record in self.list_runs(crawler_name, limit=limit)
            if record.duration_seconds is not None and not record.is_active
        ]
        grouped_records: dict[str, list[RunRecord]] = {}
        for record in records:
            grouped_records.setdefault(record.action_id, []).append(record)

        benchmarks: list[ActionRuntimeBenchmark] = []
        for action_id, action_records in grouped_records.items():
            success_records = [
                record for record in action_records if record.status == "succeeded"
            ]
            latest_record = action_records[0]
            sample_count = len(action_records)
            success_count = len(success_records)
            failure_count = sample_count - success_count
            success_rate = (
                round(success_count / sample_count * 100.0, 1) if sample_count else 0.0
            )

            benchmarks.append(
                ActionRuntimeBenchmark(
                    action_id=action_id,
                    action_label=latest_record.action_label,
                    sample_count=sample_count,
                    success_count=success_count,
                    failure_count=failure_count,
                    success_rate=success_rate,
                    latest_seconds=latest_record.duration_seconds,
                    latest_finished_at=latest_record.finished_at,
                )
            )

        return benchmarks

    def get_active_run(self, crawler_name: str) -> RunRecord | None:
        with self._guard:
            run_id = self._active_runs.get(crawler_name)
        if run_id is None:
            return None
        return self.get_run(run_id)

    def is_locked(self, crawler_name: str) -> bool:
        with self._guard:
            return crawler_name in self._active_runs

    def start_action(
        self,
        crawler_name: str,
        action_id: str,
        action_payload: dict[str, Any],
        *,
        trigger_source: str = "manual",
    ) -> int:
        prepared = self.prepare_action(crawler_name, action_id, action_payload)

        with self._guard:
            if crawler_name in self._active_runs:
                raise LockError(f"Crawler '{crawler_name}' already has an active run.")

            run_id = self._create_run_record(prepared, trigger_source=trigger_source)
            self._active_runs[crawler_name] = run_id

            thread = threading.Thread(
                target=self._execute_run,
                args=(run_id, prepared),
                daemon=True,
                name=f"crawler-run-{crawler_name}-{run_id}",
            )
            self._active_threads[run_id] = thread
            thread.start()

        return run_id

    def prepare_action(
        self,
        crawler_name: str,
        action_id: str,
        action_payload: dict[str, Any],
    ) -> PreparedAction:
        overview = get_crawler_overview(crawler_name, self.repo_root)
        if overview.card is None:
            raise ActionValidationError([f"Unknown crawler '{crawler_name}'."])

        if not overview.raw_config:
            raise ActionValidationError(
                [
                    f"Crawler '{crawler_name}' has no section in CRAWLER_CONFIG.yml and cannot be started from the admin UI."
                ]
            )

        definitions = {
            definition.action_id: definition
            for definition in self.get_action_definitions(overview)
        }
        definition = definitions.get(action_id)
        if definition is None:
            raise ActionValidationError(
                [f"Unknown action '{action_id}' for crawler '{crawler_name}'."]
            )

        effective_config = dict(overview.effective_config)

        if action_id == "run_now":
            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description="One-off manual run with the current YAML configuration. Scheduler settings stay unchanged.",
                effective_config=effective_config,
                config_overrides={},
                action_payload={},
                run_post_scripts=True,
                executor=lambda crawler: self._run_default(crawler),
            )

        if action_id == "weather_window":
            forecast_hours = self._parse_int(
                action_payload.get("forecast_hours"),
                field_label="Forecast hours",
                minimum=1,
                maximum=336,
            )
            past_hours = self._parse_int(
                action_payload.get("past_hours"),
                field_label="Past hours",
                minimum=0,
                maximum=336,
            )
            location_ids = self._split_csv_values(action_payload.get("location_ids"))
            config_overrides: dict[str, Any] = {
                "forecast_hours": forecast_hours,
                "past_hours": past_hours,
            }
            if location_ids:
                locations = self._filter_weather_locations(overview, location_ids)
                config_overrides["locations"] = locations

            effective_config.update(config_overrides)
            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description=f"Weather run with forecast_hours={forecast_hours}, past_hours={past_hours}.",
                effective_config=effective_config,
                config_overrides=config_overrides,
                action_payload={"location_ids": location_ids},
                run_post_scripts=True,
                executor=lambda crawler: self._run_default(crawler),
            )

        if action_id == "eurostat_range":
            dataset_id = self._require_text(
                action_payload.get("dataset_id"), "Dataset id"
            )
            start_year = self._parse_int(
                action_payload.get("start_year"),
                field_label="Start year",
                minimum=1990,
                maximum=2100,
            )
            end_year = self._parse_int(
                action_payload.get("end_year"),
                field_label="End year",
                minimum=1990,
                maximum=2100,
            )
            if end_year < start_year:
                raise ActionValidationError(
                    ["End year must not be earlier than start year."]
                )

            config_overrides = {
                "dataset_id": dataset_id,
                "start_year": start_year,
                "end_year": end_year,
            }
            effective_config.update(config_overrides)
            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description=f"Eurostat run for dataset {dataset_id} and years {start_year}-{end_year}.",
                effective_config=effective_config,
                config_overrides=config_overrides,
                action_payload={},
                run_post_scripts=True,
                executor=lambda crawler: self._run_default(crawler),
            )

        if action_id == "entsoe_targeted":
            target_items = self._ensure_list(action_payload.get("target_data_items"))
            invalid_items = sorted(
                set(target_items) - {item for item, _ in FMS_ACTIVE_DATA_ITEMS}
            )
            if invalid_items:
                raise ActionValidationError(
                    [f"Unknown FMS data item(s): {', '.join(invalid_items)}."]
                )

            config_overrides: dict[str, Any] = {}
            if target_items:
                config_overrides["target_data_items"] = target_items
                effective_config["target_data_items"] = list(target_items)

            selection_label = (
                f"{len(target_items)} selected item(s)"
                if target_items
                else "all active items"
            )
            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description=f"ENTSO-E targeted run for {selection_label}.",
                effective_config=effective_config,
                config_overrides=config_overrides,
                action_payload={"target_data_items": target_items},
                run_post_scripts=True,
                executor=lambda crawler: self._run_default(crawler),
            )

        if action_id == "entsoe_backfill":
            data_item = self._require_text(action_payload.get("data_item"), "Data item")
            valid_data_items = {item for item, _ in FMS_ACTIVE_DATA_ITEMS}
            if data_item not in valid_data_items:
                raise ActionValidationError([f"Unknown FMS data item '{data_item}'."])

            cadence = self._get_entsoe_backfill_cadence(data_item)
            start_value = str(action_payload.get("start", "")).strip()
            end_value = str(action_payload.get("end", "")).strip()

            if cadence != "single":
                if not start_value or not end_value:
                    raise ActionValidationError(
                        [
                            "Backfill start and end are required for monthly and annual files."
                        ]
                    )
                self._validate_entsoe_backfill_range(data_item, start_value, end_value)
            else:
                start_value = ""
                end_value = ""

            target_index = next(
                index
                for index, item in enumerate(name for name, _ in FMS_ACTIVE_DATA_ITEMS)
                if item == data_item
            )
            payload = {
                "data_item": data_item,
                "start": start_value,
                "end": end_value,
                "target_files_dir_index": target_index,
                "cadence": cadence,
            }
            description = f"ENTSO-E backfill for {data_item}"
            if cadence != "single":
                description += f" ({start_value} to {end_value})"

            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description=description + ".",
                effective_config=effective_config,
                config_overrides={},
                action_payload=payload,
                run_post_scripts=False,
                executor=lambda crawler: self._run_entsoe_backfill(crawler, payload),
            )

        if action_id == "gapfill_backfill":
            supported_tables = BUILTIN_GAPFILL_TABLES_BY_JOB.get(crawler_name, ())
            if not supported_tables:
                raise ActionValidationError(
                    [
                        f"Crawler '{crawler_name}' does not have built-in gapfill table metadata."
                    ]
                )

            start_value = self._require_text(
                action_payload.get("gapfill_start"), "Gapfill start"
            )
            end_value = str(action_payload.get("gapfill_end") or "").strip()
            start_timestamp = self._parse_timestamp(start_value, "Gapfill start")
            end_timestamp = (
                self._parse_timestamp(end_value, "Gapfill end") if end_value else None
            )
            if end_timestamp is not None and end_timestamp < start_timestamp:
                raise ActionValidationError(
                    ["Gapfill end must not be earlier than gapfill start."]
                )

            selected_tables = self._ensure_list(action_payload.get("gapfill_tables"))
            valid_tables = {table.table_name for table in supported_tables}
            invalid_tables = sorted(set(selected_tables) - valid_tables)
            if invalid_tables:
                raise ActionValidationError(
                    [f"Unknown gapfill table(s): {', '.join(invalid_tables)}."]
                )

            payload = {
                "job": crawler_name,
                "start": start_value,
                "end": end_value,
                "tables": selected_tables,
            }
            description = f"Gapfill backfill for {crawler_name} from {start_value}"
            if end_value:
                description += f" to {end_value}"

            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description=description + ".",
                effective_config=effective_config,
                config_overrides={},
                action_payload=payload,
                run_post_scripts=False,
                executor=lambda _crawler: self._run_gapfill_backfill(payload),
                requires_crawler=False,
            )

        if action_id == "price_forecast":
            if crawler_name != "entsoe_api":
                raise ActionValidationError(
                    ["Price forecast can only be started from the entsoe_api crawler."]
                )

            run_mode = str(action_payload.get("run_mode") or "forecast").strip().lower()
            if run_mode not in {"forecast", "self_test"}:
                raise ActionValidationError(["Price forecast run mode is invalid."])

            target_date = str(action_payload.get("target_date") or "").strip()
            if target_date:
                self._parse_date(target_date, "Target date")
            train_days = self._parse_int(
                action_payload.get("train_days"),
                field_label="Train days",
                minimum=14,
                maximum=365,
            )
            backtest_days = self._parse_int(
                action_payload.get("backtest_days"),
                field_label="Backtest days",
                minimum=0,
                maximum=60,
            )
            model_backend = (
                str(action_payload.get("model_backend") or "auto").strip().lower()
            )
            if model_backend not in {"auto", "upstream", "ridge"}:
                raise ActionValidationError(
                    ["Model backend must be auto, upstream, or ridge."]
                )

            payload = {
                "run_mode": run_mode,
                "target_date": target_date,
                "train_days": train_days,
                "backtest_days": backtest_days,
                "model_backend": model_backend,
            }
            description = (
                "Price forecast self-test"
                if run_mode == "self_test"
                else "Price forecast run"
            )
            if target_date:
                description += f" for {target_date}"

            return PreparedAction(
                crawler_name=crawler_name,
                action_id=action_id,
                action_label=definition.label,
                description=description + ".",
                effective_config=effective_config,
                config_overrides={},
                action_payload=payload,
                run_post_scripts=False,
                executor=lambda _crawler: self._run_price_forecast(payload),
                requires_crawler=False,
            )

        raise ActionValidationError(
            [f"No runtime handler registered for action '{action_id}'."]
        )

    def get_action_definitions(
        self, overview: CrawlerOverview
    ) -> list[ActionDefinition]:
        effective_config = overview.effective_config
        actions = [
            ActionDefinition(
                action_id="run_now",
                label="Run once",
                description="Execute the crawler immediately as a one-off manual run. The YAML schedule is not changed.",
                button_label="Start one-off run",
            )
        ]

        if overview.crawler_name == "weather_forecast":
            actions.append(
                ActionDefinition(
                    action_id="weather_window",
                    label="Weather window override",
                    description="Run the weather crawler with temporary forecast and history windows.",
                    button_label="Start weather run",
                    fields=[
                        ActionField(
                            name="forecast_hours",
                            label="Forecast hours",
                            input_type="number",
                            required=True,
                            min_value=1,
                            max_value=336,
                            default_value=effective_config.get("forecast_hours", 120),
                        ),
                        ActionField(
                            name="past_hours",
                            label="Past hours",
                            input_type="number",
                            required=True,
                            min_value=0,
                            max_value=336,
                            default_value=effective_config.get("past_hours", 24),
                        ),
                        ActionField(
                            name="location_ids",
                            label="Location ids (optional)",
                            input_type="text",
                            placeholder="berlin,hamburg",
                            help_text="Optional comma-separated subset of weather locations. Leave empty to use the configured set.",
                            default_value="",
                        ),
                    ],
                )
            )

        if overview.crawler_name == "eurostat_crawler":
            actions.append(
                ActionDefinition(
                    action_id="eurostat_range",
                    label="Eurostat range run",
                    description="Run Eurostat with a temporary dataset id and year range.",
                    button_label="Start Eurostat run",
                    fields=[
                        ActionField(
                            name="dataset_id",
                            label="Dataset id",
                            input_type="text",
                            required=True,
                            default_value=effective_config.get(
                                "dataset_id", "nrg_inf_epcrw"
                            ),
                        ),
                        ActionField(
                            name="start_year",
                            label="Start year",
                            input_type="number",
                            required=True,
                            min_value=1990,
                            max_value=2100,
                            default_value=effective_config.get("start_year", 2019),
                        ),
                        ActionField(
                            name="end_year",
                            label="End year",
                            input_type="number",
                            required=True,
                            min_value=1990,
                            max_value=2100,
                            default_value=effective_config.get(
                                "end_year", datetime.now().year
                            ),
                        ),
                    ],
                )
            )

        if overview.crawler_name == "entsoe_fms":
            target_defaults = effective_config.get("target_data_items")
            if not isinstance(target_defaults, list):
                target_defaults = []

            actions.append(
                ActionDefinition(
                    action_id="entsoe_targeted",
                    label="ENTSO-E targeted run",
                    description="Run ENTSO-E FMS only for a selected subset of active data items.",
                    button_label="Start targeted run",
                    fields=[
                        ActionField(
                            name="target_data_items",
                            label="Target data items",
                            input_type="multiselect",
                            options=[
                                ActionOption(
                                    value=item_name, label=f"{table_name} ({item_name})"
                                )
                                for item_name, table_name in FMS_ACTIVE_DATA_ITEMS
                            ],
                            help_text="Leave the selection empty to process the full active item list.",
                            default_value=list(target_defaults),
                        )
                    ],
                )
            )

            actions.append(
                ActionDefinition(
                    action_id="entsoe_backfill",
                    label="ENTSO-E backfill",
                    description="Trigger the historical backfill helper for one FMS data item.",
                    button_label="Start backfill",
                    note="Monthly files use YYYY_MM. Annual files use YYYY. Single-file items ignore start and end.",
                    fields=[
                        ActionField(
                            name="data_item",
                            label="Data item",
                            input_type="select",
                            required=True,
                            options=[
                                ActionOption(
                                    value=item_name,
                                    label=f"{table_name} [{self._get_entsoe_backfill_cadence(item_name)}]",
                                )
                                for item_name, table_name in FMS_ACTIVE_DATA_ITEMS
                            ],
                        ),
                        ActionField(
                            name="start",
                            label="Start",
                            input_type="text",
                            placeholder="2024_01 or 2024",
                            help_text="Ignored for single-file items.",
                        ),
                        ActionField(
                            name="end",
                            label="End",
                            input_type="text",
                            placeholder="2024_06 or 2025",
                            help_text="Ignored for single-file items.",
                        ),
                    ],
                )
            )

        if overview.crawler_name == "entsoe_api":
            actions.append(
                ActionDefinition(
                    action_id="price_forecast",
                    label="Price forecast",
                    description="Run the day-ahead price forecast post-run workflow on demand.",
                    button_label="Start price forecast",
                    note="The normal scheduled path still runs scripts/run_price_forecast.py through post_run_scripts.",
                    fields=[
                        ActionField(
                            name="run_mode",
                            label="Run mode",
                            input_type="select",
                            required=True,
                            options=[
                                ActionOption(
                                    value="forecast", label="Database forecast"
                                ),
                                ActionOption(value="self_test", label="Self-test"),
                            ],
                            default_value="forecast",
                        ),
                        ActionField(
                            name="target_date",
                            label="Target date",
                            input_type="text",
                            placeholder="2026-05-23",
                            help_text="Leave empty to forecast tomorrow in Europe/Berlin.",
                            default_value="",
                        ),
                        ActionField(
                            name="train_days",
                            label="Train days",
                            input_type="number",
                            required=True,
                            min_value=14,
                            max_value=365,
                            default_value=self._env_int_default(
                                "OEDS_PRICE_FORECAST_TRAIN_DAYS", 56
                            ),
                        ),
                        ActionField(
                            name="backtest_days",
                            label="Backtest days",
                            input_type="number",
                            required=True,
                            min_value=0,
                            max_value=60,
                            default_value=self._env_int_default(
                                "OEDS_PRICE_FORECAST_BACKTEST_DAYS", 2
                            ),
                        ),
                        ActionField(
                            name="model_backend",
                            label="Model backend",
                            input_type="select",
                            required=True,
                            options=[
                                ActionOption(value="auto", label="auto"),
                                ActionOption(value="upstream", label="upstream"),
                                ActionOption(value="ridge", label="ridge"),
                            ],
                            default_value=os.getenv(
                                "OEDS_PRICE_FORECAST_BACKEND", "auto"
                            ),
                        ),
                    ],
                )
            )

        supported_gapfill_tables = BUILTIN_GAPFILL_TABLES_BY_JOB.get(
            overview.crawler_name, ()
        )
        if supported_gapfill_tables:
            gapfill_config = effective_config.get("gapfill")
            configured_tables = []
            if isinstance(gapfill_config, dict) and isinstance(
                gapfill_config.get("tables"), list
            ):
                configured_tables = [
                    str(table_name)
                    for table_name in gapfill_config.get("tables", [])
                    if str(table_name).strip()
                ]
            actions.append(
                ActionDefinition(
                    action_id="gapfill_backfill",
                    label="Gapfill backfill",
                    description="Run the configured gapfill process for an explicit source time window.",
                    button_label="Start gapfill backfill",
                    note="Uses the current gapfill settings and writes to the configured target schema. It does not run the crawler first.",
                    fields=[
                        ActionField(
                            name="gapfill_start",
                            label="Start timestamp",
                            input_type="text",
                            required=True,
                            placeholder="2024-01-01 or 2024-01-01T00:00:00Z",
                            help_text="First source timestamp to process. Date-only values are treated as UTC midnight.",
                        ),
                        ActionField(
                            name="gapfill_end",
                            label="End timestamp (optional)",
                            input_type="text",
                            placeholder="2024-12-31 or 2024-12-31T23:00:00Z",
                            help_text="Leave empty to process through the latest available source timestamp.",
                        ),
                        ActionField(
                            name="gapfill_tables",
                            label="Tables (optional)",
                            input_type="multiselect",
                            options=[
                                ActionOption(
                                    value=table.table_name, label=table.table_name
                                )
                                for table in supported_gapfill_tables
                            ],
                            help_text="Leave empty to use the tables selected in the current gapfill configuration.",
                            default_value=configured_tables,
                        ),
                    ],
                )
            )

        return actions

    def get_run_log_tail(self, run_id: int, lines: int = 200) -> dict[str, Any]:
        record = self.get_run(run_id)
        if record is None:
            return {
                "text": "",
                "line_count": 0,
                "status": "missing",
                "summary": None,
                "error_message": "Run not found.",
            }

        log_path = Path(record.log_path)
        if not log_path.exists():
            return {
                "text": "",
                "line_count": 0,
                "status": record.status,
                "summary": record.summary,
                "error_message": record.error_message,
            }

        content = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        tail_lines = content[-lines:]
        return {
            "text": "\n".join(tail_lines),
            "line_count": len(tail_lines),
            "status": record.status,
            "summary": record.summary,
            "error_message": record.error_message,
        }

    def _create_run_record(
        self, prepared: PreparedAction, *, trigger_source: str
    ) -> int:
        created_at = self._utc_now()
        placeholder_name = f"run-pending-{prepared.crawler_name}-{created_at.replace(':', '').replace('-', '')}.log"
        placeholder_path = str((self.logs_dir / placeholder_name).resolve())

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO crawler_runs (
                    crawler_name,
                    action_id,
                    action_label,
                    trigger_source,
                    status,
                    created_at,
                    log_path,
                    config_overrides_json,
                    action_payload_json
                )
                VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)
                """,
                (
                    prepared.crawler_name,
                    prepared.action_id,
                    prepared.action_label,
                    trigger_source,
                    created_at,
                    placeholder_path,
                    json.dumps(prepared.config_overrides, ensure_ascii=True),
                    json.dumps(prepared.action_payload, ensure_ascii=True),
                ),
            )
            run_id = int(cursor.lastrowid)
            final_log_path = str(
                (self.logs_dir / f"{prepared.crawler_name}-{run_id}.log").resolve()
            )
            connection.execute(
                "UPDATE crawler_runs SET log_path = ? WHERE run_id = ?",
                (final_log_path, run_id),
            )
            connection.commit()

        return run_id

    def _execute_run(self, run_id: int, prepared: PreparedAction) -> None:
        started_at = self._utc_now()
        log_path = Path(self.get_run(run_id).log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with self._connect() as connection:
            connection.execute(
                "UPDATE crawler_runs SET status = 'running', started_at = ? WHERE run_id = ?",
                (started_at, run_id),
            )
            connection.commit()

        root_logger = logging.getLogger()
        root_handler = logging.StreamHandler(
            log_path.open("a", encoding="utf-8", newline="")
        )
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        root_handler.setFormatter(formatter)
        previous_level = root_logger.level
        if previous_level == logging.NOTSET or previous_level > logging.INFO:
            root_logger.setLevel(logging.INFO)
        root_logger.addHandler(root_handler)

        duration_seconds: float | None = None
        summary = prepared.description
        error_message: str | None = None
        status = "succeeded"

        try:
            with log_path.open("a", encoding="utf-8", newline="") as handle:
                writer = _LogWriter(handle)
                with redirect_stdout(writer), redirect_stderr(writer):
                    print(f"[RUN] {prepared.action_label}")
                    print(f"[CRAWLER] {prepared.crawler_name}")
                    print(f"[STARTED] {started_at}")
                    if prepared.config_overrides:
                        print(
                            "[OVERRIDES] "
                            + json.dumps(
                                prepared.config_overrides, ensure_ascii=True, indent=2
                            )
                        )
                    if prepared.action_payload:
                        print(
                            "[PAYLOAD] "
                            + json.dumps(
                                prepared.action_payload, ensure_ascii=True, indent=2
                            )
                        )

                    crawler = (
                        self._build_crawler_instance(
                            prepared.crawler_name, prepared.effective_config
                        )
                        if prepared.requires_crawler
                        else None
                    )
                    result_summary = prepared.executor(crawler)
                    if prepared.run_post_scripts:
                        self._run_post_scripts(prepared.effective_config, handle)
                    if result_summary:
                        summary = result_summary
                    print("[RESULT] Run completed.")
        except Exception as exc:
            status = "failed"
            error_message = str(exc).strip() or exc.__class__.__name__
            summary = f"{prepared.action_label} failed."
            with log_path.open("a", encoding="utf-8", newline="") as handle:
                handle.write("\n[TRACEBACK]\n")
                handle.write(traceback.format_exc())
                handle.flush()
        finally:
            finished_at = self._utc_now()
            duration_seconds = self._duration_seconds(started_at, finished_at)
            root_logger.removeHandler(root_handler)
            root_handler.close()
            if previous_level == logging.NOTSET or previous_level > logging.INFO:
                root_logger.setLevel(previous_level)

            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE crawler_runs
                    SET status = ?,
                        finished_at = ?,
                        duration_seconds = ?,
                        summary = ?,
                        error_message = ?
                    WHERE run_id = ?
                    """,
                    (
                        status,
                        finished_at,
                        duration_seconds,
                        summary,
                        error_message,
                        run_id,
                    ),
                )
                connection.commit()

            with self._guard:
                self._active_runs.pop(prepared.crawler_name, None)
                self._active_threads.pop(run_id, None)

    def _run_default(self, crawler: Any) -> str | None:
        crawler.run()
        return "Run completed successfully."

    def _run_entsoe_backfill(self, crawler: Any, payload: dict[str, Any]) -> str | None:
        local_dir = str((self.repo_root / "crawler" / "data").resolve())
        crawler.backwards_update(
            start=payload["start"],
            end=payload["end"],
            local_dir=local_dir,
            TARGET_FILES_DIR_index=int(payload["target_files_dir_index"]),
        )

        if payload["cadence"] == "single":
            return f"Backfill completed for {payload['data_item']}."
        return f"Backfill completed for {payload['data_item']} from {payload['start']} to {payload['end']}."

    def _run_gapfill_backfill(self, payload: dict[str, Any]) -> str | None:
        command = [
            sys.executable,
            "scripts/gapfill_timeseries.py",
            "--job",
            str(payload["job"]),
            "--start",
            str(payload["start"]),
        ]
        if payload.get("end"):
            command.extend(["--end", str(payload["end"])])
        if payload.get("tables"):
            command.extend(
                ["--tables", ",".join(str(table) for table in payload["tables"])]
            )

        completed = subprocess.run(
            command,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.stdout:
            print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n")
        if completed.stderr:
            print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n")
        if completed.returncode != 0:
            raise RuntimeError(
                f"Gapfill backfill exited with code {completed.returncode}."
            )

        if payload.get("end"):
            return f"Gapfill backfill completed for {payload['job']} from {payload['start']} to {payload['end']}."
        return (
            f"Gapfill backfill completed for {payload['job']} from {payload['start']}."
        )

    def _run_price_forecast(self, payload: dict[str, Any]) -> str | None:
        command = [
            sys.executable,
            "scripts/run_price_forecast.py",
            "--train-days",
            str(payload["train_days"]),
            "--backtest-days",
            str(payload["backtest_days"]),
            "--model-backend",
            str(payload["model_backend"]),
        ]
        if payload.get("target_date"):
            command.extend(["--target-date", str(payload["target_date"])])
        if payload.get("run_mode") == "self_test":
            command.append("--self-test")

        completed = subprocess.run(
            command,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.stdout:
            print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n")
        if completed.stderr:
            print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n")
        if completed.returncode != 0:
            raise RuntimeError(
                f"Price forecast exited with code {completed.returncode}."
            )

        if payload.get("run_mode") == "self_test":
            return "Price forecast self-test completed."
        if payload.get("target_date"):
            return f"Price forecast completed for {payload['target_date']}."
        return "Price forecast completed."

    def _run_post_scripts(
        self, effective_config: dict[str, Any], handle: io.TextIOBase
    ) -> None:
        scripts = effective_config.get("post_run_scripts")
        if not isinstance(scripts, list) or not scripts:
            return

        print(f"[POST] Executing {len(scripts)} post-run script(s).")
        for script in scripts:
            print(f"[POST] python {script}")
            completed = subprocess.run(
                [sys.executable, script],
                cwd=self.repo_root,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"Post-run script '{script}' exited with code {completed.returncode}."
                )

    def _build_crawler_instance(
        self, crawler_name: str, effective_config: dict[str, Any]
    ) -> Any:
        from crawler_core.base import BaseCrawler

        crawler_module = import_module(f"crawler.{crawler_name}")

        for module_element_name in dir(crawler_module):
            crawler_class = getattr(crawler_module, module_element_name)
            if (
                isclass(crawler_class)
                and issubclass(crawler_class, BaseCrawler)
                and crawler_class is not BaseCrawler
            ):
                return crawler_class(crawler_name, effective_config)

        raise RuntimeError(
            f"No BaseCrawler subclass found in crawler module '{crawler_name}'."
        )

    def _row_to_record(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=int(row["run_id"]),
            crawler_name=str(row["crawler_name"]),
            action_id=str(row["action_id"]),
            action_label=str(row["action_label"]),
            trigger_source=str(row["trigger_source"]),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            duration_seconds=float(row["duration_seconds"])
            if row["duration_seconds"] is not None
            else None,
            log_path=str(row["log_path"]),
            summary=row["summary"],
            error_message=row["error_message"],
            config_overrides=json.loads(row["config_overrides_json"] or "{}"),
            action_payload=json.loads(row["action_payload_json"] or "{}"),
        )

    def _parse_int(
        self,
        value: Any,
        *,
        field_label: str,
        minimum: int | None = None,
        maximum: int | None = None,
    ) -> int:
        raw_value = str(value or "").strip()
        if not raw_value:
            raise ActionValidationError([f"{field_label} is required."])

        try:
            parsed = int(raw_value)
        except ValueError as exc:
            raise ActionValidationError([f"{field_label} must be an integer."]) from exc

        if minimum is not None and parsed < minimum:
            raise ActionValidationError([f"{field_label} must be at least {minimum}."])
        if maximum is not None and parsed > maximum:
            raise ActionValidationError([f"{field_label} must be at most {maximum}."])

        return parsed

    def _require_text(self, value: Any, field_label: str) -> str:
        text_value = str(value or "").strip()
        if not text_value:
            raise ActionValidationError([f"{field_label} is required."])
        return text_value

    def _parse_timestamp(self, value: str, field_label: str) -> datetime:
        text_value = value.strip()
        try:
            return datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(
                tzinfo=None
            )
        except ValueError as exc:
            raise ActionValidationError(
                [f"{field_label} must be a valid ISO date or timestamp."]
            ) from exc

    def _parse_date(self, value: str, field_label: str) -> datetime:
        text_value = value.strip()
        try:
            return datetime.fromisoformat(text_value)
        except ValueError as exc:
            raise ActionValidationError(
                [f"{field_label} must be a valid ISO date."]
            ) from exc

    def _env_int_default(self, name: str, fallback: int) -> int:
        try:
            return int(os.getenv(name, str(fallback)))
        except ValueError:
            return fallback

    def _ensure_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        text_value = str(value).strip()
        return [text_value] if text_value else []

    def _split_csv_values(self, value: Any) -> list[str]:
        text_value = str(value or "").strip()
        if not text_value:
            return []

        parts = []
        for chunk in text_value.replace("\n", ",").split(","):
            candidate = chunk.strip()
            if candidate:
                parts.append(candidate)
        return parts

    def _filter_weather_locations(
        self, overview: CrawlerOverview, location_ids: list[str]
    ) -> list[dict[str, Any]]:
        try:
            from crawler.weather_forecast import WeatherForecastCrawler
        except Exception as exc:
            raise ActionValidationError(
                [
                    f"Weather location filtering is unavailable because the weather module could not be imported: {exc}."
                ]
            ) from exc

        available_locations = overview.effective_config.get("locations")
        if not isinstance(available_locations, list) or not available_locations:
            available_locations = list(WeatherForecastCrawler.DEFAULT_LOCATIONS)

        selected_locations = []
        missing = []
        for location_id in location_ids:
            match = next(
                (
                    location
                    for location in available_locations
                    if isinstance(location, dict)
                    and str(location.get("location_id")) == location_id
                ),
                None,
            )
            if match is None:
                missing.append(location_id)
            else:
                selected_locations.append(dict(match))

        if missing:
            raise ActionValidationError(
                [f"Unknown weather location id(s): {', '.join(missing)}."]
            )

        if not selected_locations:
            raise ActionValidationError(
                ["At least one weather location must be selected."]
            )

        return selected_locations

    def _get_entsoe_backfill_cadence(self, data_item: str) -> str:
        if data_item in FMS_SINGLE_FILE_DATA_ITEMS:
            return "single"
        if data_item in FMS_ANNUAL_FILE_DATA_ITEMS:
            return "annual"
        return "monthly"

    def _validate_entsoe_backfill_range(
        self, data_item: str, start_value: str, end_value: str
    ) -> None:
        from datetime import datetime as dt

        cadence = self._get_entsoe_backfill_cadence(data_item)
        format_string = "%Y" if cadence == "annual" else "%Y_%m"

        try:
            start_dt = dt.strptime(start_value, format_string)
            end_dt = dt.strptime(end_value, format_string)
        except ValueError as exc:
            expected = "YYYY" if cadence == "annual" else "YYYY_MM"
            raise ActionValidationError(
                [f"{data_item} expects {expected} values for start and end."]
            ) from exc

        if end_dt < start_dt:
            raise ActionValidationError(
                ["Backfill end must not be earlier than start."]
            )

    def _duration_seconds(self, started_at: str, finished_at: str) -> float:
        start_dt = datetime.fromisoformat(started_at)
        finish_dt = datetime.fromisoformat(finished_at)
        return round((finish_dt - start_dt).total_seconds(), 3)

    def _utc_now(self) -> str:
        return datetime.utcnow().isoformat(timespec="seconds")
