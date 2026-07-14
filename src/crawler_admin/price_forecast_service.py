from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from crawler.common.runtime_env import resolve_database_uri
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

PRICE_FORECAST_SCRIPT = "scripts/run_price_forecast.py"
PRICE_FORECAST_SCHEMA = "price_forecast"
PRICE_FORECAST_DASHBOARD_NAME = "Day-ahead Price Forecast"


@dataclass(frozen=True)
class PriceForecastRunItem:
    run_id: str
    created_at: str
    target_date: str
    market_area: str
    variant: str
    status: str
    model_name: str
    model_version: str
    message: str | None


@dataclass(frozen=True)
class PriceForecastRuntimeView:
    script_enabled: bool
    schema_name: str
    dashboard_name: str
    runs: list[PriceForecastRunItem]
    database_status_note: str
    available: bool

    @property
    def latest_run(self) -> PriceForecastRunItem | None:
        return self.runs[0] if self.runs else None


def build_price_forecast_runtime_view(
    effective_config: dict[str, Any],
    *,
    limit: int = 5,
) -> PriceForecastRuntimeView:
    scripts = (
        list(effective_config.get("post_run_scripts") or [])
        if isinstance(effective_config.get("post_run_scripts"), list)
        else []
    )
    database_uri = str(effective_config.get("database_uri") or "").strip()

    if not database_uri:
        return PriceForecastRuntimeView(
            script_enabled=PRICE_FORECAST_SCRIPT in scripts,
            schema_name=PRICE_FORECAST_SCHEMA,
            dashboard_name=PRICE_FORECAST_DASHBOARD_NAME,
            runs=[],
            database_status_note="No database URI is configured for this crawler.",
            available=False,
        )

    engine = create_engine(resolve_database_uri(database_uri))
    try:
        with engine.connect() as conn:
            table_exists = bool(
                conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = :schema_name
                              AND table_name = 'forecast_runs'
                        )
                        """
                    ),
                    {"schema_name": PRICE_FORECAST_SCHEMA},
                ).scalar()
            )
            if not table_exists:
                return PriceForecastRuntimeView(
                    script_enabled=PRICE_FORECAST_SCRIPT in scripts,
                    schema_name=PRICE_FORECAST_SCHEMA,
                    dashboard_name=PRICE_FORECAST_DASHBOARD_NAME,
                    runs=[],
                    database_status_note="price_forecast.forecast_runs does not exist yet.",
                    available=False,
                )

            rows = conn.execute(
                text(
                    """
                    SELECT
                        run_id,
                        created_at,
                        target_date,
                        market_area,
                        variant,
                        status,
                        model_name,
                        model_version,
                        message
                    FROM price_forecast.forecast_runs
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).mappings().all()
    except SQLAlchemyError as exc:
        return PriceForecastRuntimeView(
            script_enabled=PRICE_FORECAST_SCRIPT in scripts,
            schema_name=PRICE_FORECAST_SCHEMA,
            dashboard_name=PRICE_FORECAST_DASHBOARD_NAME,
            runs=[],
            database_status_note=f"Price forecast status could not be read: {_short_sql_error(exc)}",
            available=False,
        )
    finally:
        engine.dispose()

    return PriceForecastRuntimeView(
        script_enabled=PRICE_FORECAST_SCRIPT in scripts,
        schema_name=PRICE_FORECAST_SCHEMA,
        dashboard_name=PRICE_FORECAST_DASHBOARD_NAME,
        runs=[
            PriceForecastRunItem(
                run_id=str(row["run_id"]),
                created_at=str(row["created_at"]),
                target_date=str(row["target_date"]),
                market_area=str(row["market_area"]),
                variant=str(row["variant"]),
                status=str(row["status"]),
                model_name=str(row["model_name"]),
                model_version=str(row["model_version"]),
                message=row["message"],
            )
            for row in rows
        ],
        database_status_note="Latest price forecast runs from price_forecast.forecast_runs.",
        available=True,
    )


def _short_sql_error(exc: SQLAlchemyError) -> str:
    raw = str(getattr(exc, "orig", None) or exc).strip()
    return raw.splitlines()[0] if raw else exc.__class__.__name__
