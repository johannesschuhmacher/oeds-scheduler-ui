from __future__ import annotations

import re
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import yaml
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from crawler_admin.config_service import (
    build_cron_preview,
    build_dashboard_state,
    compute_content_hash,
    get_all_crawler_overviews,
    get_config_path,
    get_crawler_overview,
    get_file_mtime_display,
    get_local_time_label,
    read_config_text,
    update_crawler_schedule_config_text,
    update_gapfill_config_text,
    validate_config_text,
    write_config_text_atomic,
)
from crawler_admin.gapfill_service import (
    GapfillHoldoutView,
    GapfillSelfTestView,
    build_gapfill_holdout_catalog,
    build_gapfill_holdout_view,
    build_gapfill_runtime_view,
    build_gapfill_selftest_catalog,
    build_gapfill_selftest_view,
    gapfill_method_options,
)
from crawler_admin.price_forecast_service import build_price_forecast_runtime_view
from crawler_admin.runtime_service import (
    ActionValidationError,
    CrawlerRunService,
    LockError,
    RunRecord,
)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
run_service = CrawlerRunService()
latest_gapfill_selftest_view: GapfillSelfTestView | None = None
latest_gapfill_holdout_view: GapfillHoldoutView | None = None
SCHEDULER_WEEKDAY_ORDER = ["1", "2", "3", "4", "5", "6", "0"]
SCHEDULER_WEEKDAY_OPTIONS = [
    {"value": "1", "label": "Monday"},
    {"value": "2", "label": "Tuesday"},
    {"value": "3", "label": "Wednesday"},
    {"value": "4", "label": "Thursday"},
    {"value": "5", "label": "Friday"},
    {"value": "6", "label": "Saturday"},
    {"value": "0", "label": "Sunday"},
]
SCHEDULER_HOUR_OPTIONS = [{"value": str(hour), "label": f"{hour:02d}"} for hour in range(24)]
SCHEDULER_MINUTE_OPTIONS = [{"value": str(minute), "label": f"{minute:02d}"} for minute in range(60)]
SCHEDULER_DEFAULT_FORM = {
    "scheduler_mode": "daily",
    "hourly_interval_hours": "1",
    "hourly_minute": "0",
    "daily_hour": "4",
    "daily_minute": "0",
    "weekly_hour": "4",
    "weekly_minute": "0",
    "weekly_days": ["1", "2", "3", "4", "5"],
    "advanced_schedule": "",
}

app = FastAPI(title="OEDS Crawler Control", version="0.5.0")
app.mount("/admin/static", StaticFiles(directory=str(BASE_DIR / "static")), name="admin_static")


@app.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/admin", status_code=307)


@app.get("/admin", response_class=HTMLResponse)
def dashboard(
    request: Request,
    started: int | None = Query(default=None),
    crawler: str | None = Query(default=None),
    scheduler_saved: int | None = Query(default=None),
    created: int | None = Query(default=None),
    edit: str | None = Query(default=None),
    job: str | None = Query(default=None),
    confirm_run: str | None = Query(default=None),
    inspect: str | None = Query(default=None),
    email_tested: int | None = Query(default=None),
) -> HTMLResponse:
    message_kind = None
    message_text = None
    if scheduler_saved and crawler:
        message_kind = "success"
        schedule_target = f"{crawler}:{job}" if job else crawler
        message_text = (
            f"Created YAML section and saved scheduler settings for {schedule_target}."
            if created
            else f"Saved scheduler settings for {schedule_target}."
        )
    elif started and crawler:
        message_kind = "success"
        message_text = f"Started one-off manual run for {crawler}. Scheduler unchanged."
    elif email_tested and crawler:
        message_kind = "success"
        message_text = f"Sent test email for {crawler}."

    return _render_dashboard(
        request=request,
        message_kind=message_kind,
        message_text=message_text,
        selected_scheduler_for=edit,
        selected_scheduler_job_for=job,
        selected_run_confirmation_for=confirm_run,
        selected_inspect_for=inspect,
    )


@app.get("/admin/gapfill", response_class=HTMLResponse)
def gapfill_dashboard(
    request: Request,
    self_tested: int | None = Query(default=None),
    holdout_tested: int | None = Query(default=None),
) -> HTMLResponse:
    message_kind = None
    message_text = None
    if holdout_tested and latest_gapfill_holdout_view:
        message_kind = "success" if latest_gapfill_holdout_view.passed else "error"
        message_text = (
            "Gapfill holdout test completed."
            if latest_gapfill_holdout_view.passed
            else "Gapfill holdout test completed with missing comparison points."
        )
    elif self_tested and latest_gapfill_selftest_view:
        message_kind = "success" if latest_gapfill_selftest_view.all_passed else "error"
        message_text = (
            "Gapfill self-tests completed."
            if latest_gapfill_selftest_view.all_passed
            else "Gapfill self-tests completed with failures."
        )

    return _render_gapfill_dashboard(
        request=request,
        latest_selftest_view=latest_gapfill_selftest_view,
        latest_holdout_view=latest_gapfill_holdout_view,
        message_kind=message_kind,
        message_text=message_text,
    )


@app.post("/admin/gapfill/self-tests", response_class=HTMLResponse)
async def run_gapfill_self_tests(request: Request) -> HTMLResponse:
    global latest_gapfill_selftest_view

    form = await request.form()
    selected_names = [str(value) for value in form.getlist("test_names")]

    try:
        view = build_gapfill_selftest_view(selected_names)
    except ValueError as exc:
        return _render_gapfill_dashboard(
            request=request,
            latest_selftest_view=latest_gapfill_selftest_view,
            latest_holdout_view=latest_gapfill_holdout_view,
            message_kind="error",
            message_text=str(exc),
            selected_names=selected_names,
        )

    latest_gapfill_selftest_view = view

    return RedirectResponse(url="/admin/gapfill?self_tested=1", status_code=303)


@app.post("/admin/gapfill/holdout-tests", response_class=HTMLResponse)
async def run_gapfill_holdout_test(request: Request) -> HTMLResponse:
    global latest_gapfill_holdout_view

    form = await request.form()
    dataset_name = str(form.get("dataset_name") or "").strip()
    fault_type = str(form.get("fault_type") or "value_gap").strip()
    method = str(form.get("method") or "").strip() or None
    submitted_holdout = {
        "dataset_name": dataset_name,
        "fault_type": fault_type,
        "method": method or "",
        "gap_length_periods": str(form.get("gap_length_periods") or ""),
        "gap_start_index": str(form.get("gap_start_index") or ""),
    }

    try:
        gap_length_periods = _parse_optional_int(form.get("gap_length_periods"))
        gap_start_index = _parse_optional_int(form.get("gap_start_index"))
        if gap_length_periods is None:
            raise ValueError("gap_length_periods is required.")
        view = build_gapfill_holdout_view(
            dataset_name,
            gap_length_periods,
            gap_start_index=gap_start_index,
            fault_type=fault_type,
            method=method,
        )
    except ValueError as exc:
        return _render_gapfill_dashboard(
            request=request,
            latest_selftest_view=latest_gapfill_selftest_view,
            latest_holdout_view=latest_gapfill_holdout_view,
            message_kind="error",
            message_text=str(exc),
            submitted_holdout=submitted_holdout,
        )

    latest_gapfill_holdout_view = view

    return RedirectResponse(url="/admin/gapfill?holdout_tested=1", status_code=303)


@app.post("/admin/crawlers/{crawler_name}/scheduler", response_class=HTMLResponse)
async def save_crawler_scheduler(
    request: Request,
    crawler_name: str,
) -> HTMLResponse:
    form = await request.form()
    overview = get_crawler_overview(crawler_name)
    if overview.card is None:
        raise HTTPException(status_code=404, detail=f"Crawler '{crawler_name}' not found.")

    enable = str(form.get("enable") or "")
    job_name = str(form.get("job_name") or "").strip() or None
    base_hash = str(form.get("base_hash") or "")
    redirect_to = str(form.get("redirect_to") or "/admin")
    enable_value = _parse_enable_form_value(enable)
    schedule_form = _extract_scheduler_form_values(form)
    schedule_value, schedule_form, form_errors = _resolve_scheduler_schedule(schedule_form)
    schedule_form["enable"] = enable.strip().lower()
    form_values = {crawler_name: schedule_form}

    if enable_value is None:
        form_errors.append("Enable must be either true or false.")
    elif schedule_value:
        preview = build_cron_preview(schedule_value)
        if preview.error:
            form_errors.append(f"Invalid CRON schedule: {preview.error}")

    if form_errors:
        return _render_dashboard(
            request=request,
            message_kind="error",
            message_text="Scheduler settings were not saved.",
            scheduler_form_overrides=form_values,
            scheduler_errors={crawler_name: form_errors},
            selected_scheduler_for=crawler_name,
            selected_scheduler_job_for=job_name,
        )

    try:
        updated_yaml_text, created_section = update_crawler_schedule_config_text(
            crawler_name,
            enabled=bool(enable_value),
            schedule=schedule_value,
            job_name=job_name,
        )
    except ValueError as exc:
        return _render_dashboard(
            request=request,
            message_kind="error",
            message_text="Scheduler settings were not saved.",
            scheduler_form_overrides=form_values,
            scheduler_errors={crawler_name: [str(exc)]},
            selected_scheduler_for=crawler_name,
            selected_scheduler_job_for=job_name,
        )

    saved, _ = write_config_text_atomic(updated_yaml_text, expected_hash=base_hash)
    if not saved:
        return _render_dashboard(
            request=request,
            message_kind="error",
            message_text=(
                "CRAWLER_CONFIG.yml changed on disk while you were editing. "
                "Reload the dashboard and apply the change again."
            ),
            scheduler_form_overrides=form_values,
            scheduler_errors={crawler_name: ["The dashboard was stale and could not save the change."]},
            selected_scheduler_for=crawler_name,
            selected_scheduler_job_for=job_name,
        )

    success_params = {
        "scheduler_saved": "1",
        "crawler": crawler_name,
        "created": "1" if created_section else "0",
    }
    if job_name:
        success_params["job"] = job_name

    return RedirectResponse(
        url=_append_query_string(redirect_to, success_params),
        status_code=303,
    )


@app.get("/admin/crawlers/{crawler_name}", response_class=HTMLResponse)
def crawler_detail(
    request: Request,
    crawler_name: str,
    run: int | None = Query(default=None),
    started: int | None = Query(default=None),
    email_tested: int | None = Query(default=None),
    gapfill_saved: int | None = Query(default=None),
    check_gapfill_db: int | None = Query(default=None),
) -> HTMLResponse:
    message_kind = None
    message_text = None
    if started:
        message_kind = "success"
        message_text = f"Started one-off manual run for {crawler_name}. Scheduler unchanged."
    elif email_tested:
        message_kind = "success"
        message_text = f"Sent test email for {crawler_name}."
    elif gapfill_saved:
        message_kind = "success"
        message_text = f"Saved gapfill settings for {crawler_name}."

    return _render_crawler_detail(
        request=request,
        crawler_name=crawler_name,
        selected_run_id=run,
        message_kind=message_kind,
        message_text=message_text,
        check_gapfill_db=bool(check_gapfill_db),
    )


@app.post("/admin/crawlers/{crawler_name}/gapfill", response_class=HTMLResponse)
async def save_crawler_gapfill(request: Request, crawler_name: str) -> HTMLResponse:
    form = await request.form()
    overview = get_crawler_overview(crawler_name)
    if overview.card is None:
        raise HTTPException(status_code=404, detail=f"Crawler '{crawler_name}' not found.")

    redirect_to = str(form.get("redirect_to") or f"/admin/crawlers/{crawler_name}")
    base_hash = str(form.get("base_hash") or "")
    gapfill_values = _extract_gapfill_form_values(form, overview)
    gapfill_errors = _validate_gapfill_form_values(gapfill_values, overview)

    if gapfill_errors:
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text="Gapfill settings were not saved.",
            gapfill_errors=gapfill_errors,
            gapfill_values=gapfill_values,
        )

    try:
        updated_yaml_text = update_gapfill_config_text(
            crawler_name,
            enabled=gapfill_values["gapfill_enabled"] == "true",
            script_enabled=gapfill_values["script_enabled"] == "true",
            selected_tables=list(gapfill_values["selected_tables"]),
            table_methods=dict(gapfill_values["table_methods"]),
            target_schema=gapfill_values["target_schema"],
            method=gapfill_values["method"],
            candidate_periods=_split_gapfill_periods(gapfill_values["candidate_periods"]),
            donor_context_periods=int(gapfill_values["donor_context_periods"]),
            donor_search_radius=gapfill_values["donor_search_radius"],
            refinement_periods=int(gapfill_values["refinement_periods"]),
            max_gap_periods=int(gapfill_values["max_gap_periods"]),
            lookback=gapfill_values["lookback"],
            fail_on_table_error=gapfill_values["fail_on_table_error"] == "true",
        )
    except ValueError as exc:
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text="Gapfill settings were not saved.",
            gapfill_errors=[str(exc)],
            gapfill_values=gapfill_values,
        )

    saved, _ = write_config_text_atomic(updated_yaml_text, expected_hash=base_hash)
    if not saved:
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text=(
                "CRAWLER_CONFIG.yml changed on disk while you were editing. "
                "Reload the crawler page and apply the change again."
            ),
            gapfill_errors=["The crawler page was stale and could not save the gapfill change."],
            gapfill_values=gapfill_values,
        )

    return RedirectResponse(url=_append_query_string(redirect_to, {"gapfill_saved": "1"}), status_code=303)


@app.post("/admin/crawlers/{crawler_name}/actions/{action_id}", response_class=HTMLResponse)
async def crawler_action(request: Request, crawler_name: str, action_id: str) -> HTMLResponse:
    form = await request.form()
    payload = _form_to_payload(form)
    redirect_to = str(form.get("redirect_to") or f"/admin/crawlers/{crawler_name}")
    response_context = str(form.get("response_context") or "").strip().lower()

    try:
        run_id = run_service.start_action(crawler_name, action_id, payload)
    except ActionValidationError as exc:
        if response_context == "dashboard-run-confirm":
            return _render_dashboard(
                request=request,
                message_kind="error",
                message_text="The crawler run could not be started.",
                run_confirmation_errors={crawler_name: exc.errors},
                selected_run_confirmation_for=crawler_name,
            )
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text="The action could not be started.",
            form_errors=exc.errors,
            submitted_action_id=action_id,
            submitted_values=payload,
        )
    except LockError as exc:
        if response_context == "dashboard-run-confirm":
            return _render_dashboard(
                request=request,
                message_kind="error",
                message_text="The crawler run could not be started.",
                run_confirmation_errors={crawler_name: [str(exc)]},
                selected_run_confirmation_for=crawler_name,
            )
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text=str(exc),
            submitted_action_id=action_id,
            submitted_values=payload,
        )

    return RedirectResponse(
        url=_append_query_string(redirect_to, {"started": "1", "crawler": crawler_name, "run": str(run_id)}),
        status_code=303,
    )


@app.post("/admin/crawlers/{crawler_name}/email/test", response_class=HTMLResponse)
async def crawler_email_test(request: Request, crawler_name: str) -> HTMLResponse:
    form = await request.form()
    redirect_to = str(form.get("redirect_to") or f"/admin/crawlers/{crawler_name}")
    overview = get_crawler_overview(crawler_name)
    if overview.card is None:
        raise HTTPException(status_code=404, detail=f"Crawler '{crawler_name}' not found.")

    email_state = _build_email_alert_state(overview)
    if not email_state["is_ready"]:
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text="The test email could not be sent.",
            email_test_errors=[str(email_state["summary"])],
        )

    try:
        _send_test_email(crawler_name, overview.effective_config)
    except Exception as exc:
        return _render_crawler_detail(
            request=request,
            crawler_name=crawler_name,
            message_kind="error",
            message_text="The test email could not be sent.",
            email_test_errors=[str(exc).strip() or exc.__class__.__name__],
        )

    return RedirectResponse(
        url=_append_query_string(
            redirect_to,
            {"email_tested": "1"},
        ),
        status_code=303,
    )


@app.get("/admin/editor", response_class=HTMLResponse)
def editor(request: Request, saved: int | None = Query(default=None)) -> HTMLResponse:
    yaml_text = read_config_text()
    validation = validate_config_text(yaml_text)
    return templates.TemplateResponse(
        request,
        "editor.html",
        {
            "request": request,
            "page_name": "editor",
            "yaml_text": yaml_text,
            "base_hash": compute_content_hash(yaml_text),
            "config_path": get_config_path(),
            "config_mtime": get_file_mtime_display(get_config_path()),
            "time_label": get_local_time_label(),
            "validation": validation,
            "message_kind": "success" if saved else None,
            "message_text": "CRAWLER_CONFIG.yml was saved successfully." if saved else None,
            "schedule_samples": build_dashboard_state().schedule_samples,
        },
    )


@app.post("/admin/editor", response_class=HTMLResponse)
async def editor_submit(
    request: Request,
    yaml_content: str = Form(...),
    base_hash: str = Form(""),
    action: str = Form("validate"),
) -> HTMLResponse:
    validation = validate_config_text(yaml_content)
    message_kind = None
    message_text = None
    next_hash = base_hash or compute_content_hash(read_config_text())

    if action == "save":
        if validation.has_errors:
            message_kind = "error"
            message_text = "The configuration was not saved because validation failed."
        else:
            saved, current_hash = write_config_text_atomic(yaml_content, expected_hash=base_hash)
            if saved:
                return RedirectResponse(url="/admin/editor?saved=1", status_code=303)

            message_kind = "error"
            message_text = (
                "The file changed on disk while you were editing. "
                "Reload the editor, review the current file, and save again."
            )
            next_hash = current_hash if not base_hash else base_hash

    elif action == "validate":
        if validation.has_errors:
            message_kind = "error"
            message_text = "Validation found blocking errors."
        elif validation.warning_count:
            message_kind = "warning"
            message_text = "Validation passed with warnings."
        else:
            message_kind = "success"
            message_text = "Validation passed."

    return templates.TemplateResponse(
        request,
        "editor.html",
        {
            "request": request,
            "page_name": "editor",
            "yaml_text": yaml_content,
            "base_hash": next_hash,
            "config_path": get_config_path(),
            "config_mtime": get_file_mtime_display(get_config_path()),
            "time_label": get_local_time_label(),
            "validation": validation,
            "message_kind": message_kind,
            "message_text": message_text,
            "schedule_samples": build_dashboard_state().schedule_samples,
        },
    )


@app.get("/admin/api/cron-preview", response_class=JSONResponse)
def cron_preview(schedule: str = Query(..., min_length=1)) -> JSONResponse:
    preview = build_cron_preview(schedule)
    return JSONResponse(
        {
            "schedule": preview.schedule,
            "summary": preview.summary,
            "next_runs": preview.next_runs,
            "error": preview.error,
            "time_label": get_local_time_label(),
        }
    )


@app.get("/admin/api/runs/{run_id}", response_class=JSONResponse)
def run_status(run_id: int) -> JSONResponse:
    record = run_service.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    return JSONResponse(_run_record_payload(record))


@app.get("/admin/api/runs/{run_id}/log", response_class=JSONResponse)
def run_log(run_id: int, lines: int = Query(default=200, ge=20, le=1000)) -> JSONResponse:
    record = run_service.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Run not found.")

    payload = run_service.get_run_log_tail(run_id, lines=lines)
    payload.update({"run_id": run_id})
    return JSONResponse(payload)


@app.get("/admin/healthz", response_class=JSONResponse)
def healthcheck() -> JSONResponse:
    return JSONResponse({"status": "ok"})


def _render_gapfill_dashboard(
    *,
    request: Request,
    latest_selftest_view: GapfillSelfTestView | None,
    latest_holdout_view: GapfillHoldoutView | None,
    message_kind: str | None = None,
    message_text: str | None = None,
    selected_names: list[str] | None = None,
    submitted_holdout: dict[str, str] | None = None,
) -> HTMLResponse:
    catalog = build_gapfill_selftest_catalog()
    holdout_catalog = build_gapfill_holdout_catalog()
    if selected_names is None:
        selected_names = latest_selftest_view.selected_names if latest_selftest_view else [item.name for item in catalog]
    if submitted_holdout is None:
        submitted_holdout = _build_holdout_form_defaults(latest_holdout_view, holdout_catalog)

    return templates.TemplateResponse(
        request,
        "gapfill.html",
        {
            "request": request,
            "page_name": "gapfill",
            "catalog": catalog,
            "holdout_catalog": holdout_catalog,
            "latest_view": latest_selftest_view,
            "latest_holdout_view": latest_holdout_view,
            "selected_names": set(selected_names),
            "holdout_values": submitted_holdout,
            "gapfill_methods": gapfill_method_options(),
            "message_kind": message_kind,
            "message_text": message_text,
        },
    )


def _build_holdout_form_defaults(
    latest_holdout_view: GapfillHoldoutView | None,
    holdout_catalog: list[Any],
) -> dict[str, str]:
    if latest_holdout_view is not None:
        result = latest_holdout_view.result
        return {
            "dataset_name": result.dataset_name,
            "fault_type": result.fault_type,
            "method": result.method,
            "gap_length_periods": str(result.gap_length_periods),
            "gap_start_index": str(result.gap_start_index),
        }

    default_dataset = holdout_catalog[0] if holdout_catalog else None
    return {
        "dataset_name": default_dataset.name if default_dataset else "",
        "fault_type": "value_gap",
        "method": default_dataset.method if default_dataset else "donor_refined",
        "gap_length_periods": str(default_dataset.recommended_gap_length if default_dataset else 6),
        "gap_start_index": str(default_dataset.recommended_gap_start if default_dataset else ""),
    }


def _extract_gapfill_form_values(form: Any, overview: Any) -> dict[str, Any]:
    defaults = _build_gapfill_form_defaults(overview)
    selected_tables = [str(value) for value in form.getlist("gapfill_tables") if str(value).strip()]
    table_methods = {
        table_name: str(
            form.get(f"gapfill_table_method__{table_name}")
            or defaults["table_methods"].get(table_name)
            or defaults["method"]
        ).strip()
        for table_name in defaults["table_methods"]
    }
    return {
        "script_enabled": "true" if form.get("script_enabled") else "false",
        "gapfill_enabled": "true" if form.get("gapfill_enabled") else "false",
        "target_schema": str(form.get("target_schema") or defaults["target_schema"]).strip(),
        "method": str(form.get("method") or defaults["method"]).strip(),
        "candidate_periods": str(form.get("candidate_periods") or defaults["candidate_periods"]).strip(),
        "donor_context_periods": str(form.get("donor_context_periods") or defaults["donor_context_periods"]).strip(),
        "donor_search_radius": str(form.get("donor_search_radius") or defaults["donor_search_radius"]).strip(),
        "refinement_periods": str(form.get("refinement_periods") or defaults["refinement_periods"]).strip(),
        "max_gap_periods": str(form.get("max_gap_periods") or defaults["max_gap_periods"]).strip(),
        "lookback": str(form.get("lookback") or defaults["lookback"]).strip(),
        "fail_on_table_error": "true" if form.get("fail_on_table_error") else "false",
        "selected_tables": selected_tables,
        "table_methods": table_methods,
    }


def _build_gapfill_form_defaults(overview: Any) -> dict[str, Any]:
    gapfill_view = build_gapfill_runtime_view(
        overview.crawler_name,
        overview.raw_config,
        overview.effective_config,
    )
    return _build_gapfill_form_defaults_from_view(gapfill_view)


def _build_gapfill_form_defaults_from_view(gapfill_view: Any) -> dict[str, Any]:
    return {
        "script_enabled": "true" if gapfill_view.script_enabled else "false",
        "gapfill_enabled": "true" if gapfill_view.gapfill_enabled else "false",
        "target_schema": gapfill_view.target_schema,
        "method": gapfill_view.method,
        "candidate_periods": gapfill_view.candidate_periods_label if gapfill_view.candidate_periods_label != "-" else "",
        "donor_context_periods": str(gapfill_view.donor_context_periods),
        "donor_search_radius": gapfill_view.donor_search_radius_label,
        "refinement_periods": str(gapfill_view.refinement_periods),
        "max_gap_periods": str(gapfill_view.max_gap_periods),
        "lookback": gapfill_view.lookback_label,
        "fail_on_table_error": "true" if gapfill_view.fail_on_table_error else "false",
        "selected_tables": [item.table_name for item in gapfill_view.tables if item.selected],
        "table_methods": {item.table_name: item.method for item in gapfill_view.tables},
    }


def _validate_gapfill_form_values(gapfill_values: dict[str, Any], overview: Any) -> list[str]:
    errors: list[str] = []
    gapfill_view = build_gapfill_runtime_view(
        overview.crawler_name,
        overview.raw_config,
        overview.effective_config,
    )

    if not gapfill_view.supported:
        return ["Gapfill controls are only available for crawlers with built-in table metadata."]

    target_schema = str(gapfill_values["target_schema"]).strip()
    if not target_schema:
        errors.append("Target schema is required.")
    elif not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_schema):
        errors.append("Target schema must be a valid PostgreSQL schema identifier.")

    method = str(gapfill_values["method"]).strip()
    if method not in gapfill_method_options():
        errors.append(f"Gapfill method must be one of: {', '.join(gapfill_method_options())}.")

    try:
        donor_context_periods = int(gapfill_values["donor_context_periods"])
        if donor_context_periods < 1:
            errors.append("Donor context periods must be at least 1.")
    except (TypeError, ValueError):
        errors.append("Donor context periods must be an integer.")

    try:
        refinement_periods = int(gapfill_values["refinement_periods"])
        if refinement_periods < 0:
            errors.append("Refinement periods must be 0 or greater.")
    except (TypeError, ValueError):
        errors.append("Refinement periods must be an integer.")

    try:
        max_gap_periods = int(gapfill_values["max_gap_periods"])
        if max_gap_periods < 1:
            errors.append("Max gap periods must be at least 1.")
    except (TypeError, ValueError):
        errors.append("Max gap periods must be an integer.")

    for label, raw_value in (
        ("Candidate periods", gapfill_values["candidate_periods"]),
        ("Donor search radius", gapfill_values["donor_search_radius"]),
        ("Lookback", gapfill_values["lookback"]),
    ):
        values = _split_gapfill_periods(raw_value) if label == "Candidate periods" else [str(raw_value).strip()]
        for value in values:
            if not value:
                continue
            try:
                pd.Timedelta(value)
            except (TypeError, ValueError):
                errors.append(f"{label} contains an invalid duration: {value}")

    known_tables = {item.table_name for item in gapfill_view.tables}
    invalid_tables = sorted(set(gapfill_values["selected_tables"]) - known_tables)
    if invalid_tables:
        errors.append(f"Unknown gapfill tables: {', '.join(invalid_tables)}.")

    table_methods = gapfill_values.get("table_methods")
    if not isinstance(table_methods, dict):
        errors.append("Table methods could not be read from the submitted form.")
    else:
        invalid_method_tables = [
            table_name
            for table_name in gapfill_values["selected_tables"]
            if table_methods.get(table_name) not in gapfill_method_options()
        ]
        if invalid_method_tables:
            errors.append(
                "Each selected table needs a valid gapfill method. Invalid tables: "
                + ", ".join(invalid_method_tables)
                + "."
            )

    return errors


def _split_gapfill_periods(raw_value: object) -> list[str]:
    return [item.strip() for item in str(raw_value or "").split(",") if item.strip()]


def _render_crawler_detail(
    *,
    request: Request,
    crawler_name: str,
    selected_run_id: int | None = None,
    message_kind: str | None = None,
    message_text: str | None = None,
    form_errors: list[str] | None = None,
    email_test_errors: list[str] | None = None,
    gapfill_errors: list[str] | None = None,
    gapfill_values: dict[str, Any] | None = None,
    check_gapfill_db: bool = False,
    submitted_action_id: str | None = None,
    submitted_values: dict[str, Any] | None = None,
) -> HTMLResponse:
    overview = get_crawler_overview(crawler_name)
    if overview.card is None:
        raise HTTPException(status_code=404, detail=f"Crawler '{crawler_name}' not found.")

    actions = run_service.get_action_definitions(overview)
    history = run_service.list_runs(crawler_name, limit=25)
    active_run = run_service.get_active_run(crawler_name)
    selected_run = _select_run(history, active_run, selected_run_id)
    selected_log = run_service.get_run_log_tail(selected_run.run_id) if selected_run else None
    latest_run = history[0] if history else None
    runtime_benchmarks = run_service.get_runtime_benchmarks(crawler_name)
    action_values = _build_action_values(actions, submitted_action_id, submitted_values)
    email_state = _build_email_alert_state(overview)
    gapfill_view = build_gapfill_runtime_view(
        crawler_name,
        overview.raw_config,
        overview.effective_config,
        check_database_tables=check_gapfill_db,
    )
    gapfill_form_values = gapfill_values or _build_gapfill_form_defaults_from_view(
        gapfill_view
    )
    price_forecast_view = (
        build_price_forecast_runtime_view(overview.effective_config)
        if crawler_name == "entsoe_api"
        else None
    )
    config_hash = compute_content_hash(read_config_text())

    return templates.TemplateResponse(
        request,
        "crawler_detail.html",
        {
            "request": request,
            "page_name": "dashboard",
            "overview": overview,
            "actions": actions,
            "action_values": action_values,
            "history": history,
            "active_run": active_run,
            "latest_run": latest_run,
            "runtime_benchmarks": runtime_benchmarks,
            "selected_run": selected_run,
            "selected_log": selected_log,
            "is_locked": run_service.is_locked(crawler_name),
            "email_state": email_state,
            "email_test_errors": email_test_errors or [],
            "effective_config_yaml": _format_yaml_preview(overview.effective_config if overview else {}),
            "raw_config_yaml": _format_yaml_preview(overview.raw_config if overview else {}),
            "post_run_script_count": len(overview.effective_config.get("post_run_scripts", []))
            if overview and isinstance(overview.effective_config.get("post_run_scripts"), list)
            else 0,
            "message_kind": message_kind,
            "message_text": message_text,
            "form_errors": form_errors or [],
            "gapfill_view": gapfill_view,
            "price_forecast_view": price_forecast_view,
            "check_gapfill_db": check_gapfill_db,
            "gapfill_errors": gapfill_errors or [],
            "gapfill_values": gapfill_form_values,
            "gapfill_methods": gapfill_method_options(),
            "base_hash": config_hash,
            "submitted_action_id": submitted_action_id,
            "time_label": get_local_time_label(),
        },
    )


def _render_dashboard(
    *,
    request: Request,
    message_kind: str | None = None,
    message_text: str | None = None,
    scheduler_form_overrides: dict[str, dict[str, Any]] | None = None,
    scheduler_errors: dict[str, list[str]] | None = None,
    selected_scheduler_for: str | None = None,
    selected_scheduler_job_for: str | None = None,
    run_confirmation_errors: dict[str, list[str]] | None = None,
    selected_run_confirmation_for: str | None = None,
    email_test_errors: dict[str, list[str]] | None = None,
    selected_inspect_for: str | None = None,
) -> HTMLResponse:
    state = build_dashboard_state()
    overview_map = get_all_crawler_overviews()
    latest_runs = run_service.get_latest_runs_map()
    scheduler_form_overrides = scheduler_form_overrides or {}
    scheduler_errors = scheduler_errors or {}
    run_confirmation_errors = run_confirmation_errors or {}
    email_test_errors = email_test_errors or {}
    crawler_rows = []

    for card in state.cards:
        overview = overview_map.get(card.name)
        active_run = run_service.get_active_run(card.name)
        latest_run = active_run or latest_runs.get(card.name)
        email_state = _build_email_alert_state(overview)
        scheduler_job_name = (
            _select_scheduler_job_name(card, selected_scheduler_job_for)
            if selected_scheduler_for == card.name
            else _select_scheduler_job_name(card, None)
        )
        scheduler_job = _find_scheduler_job(card, scheduler_job_name)
        scheduler_form = _build_scheduler_form_state(
            card=card,
            overview=overview,
            override_values=scheduler_form_overrides.get(card.name),
            job_name=scheduler_job_name,
        )
        crawler_rows.append(
            {
                "card": card,
                "overview": overview,
                "active_run": active_run,
                "latest_run": latest_run,
                "is_locked": run_service.is_locked(card.name),
                "can_start": bool(overview and overview.raw_config),
                "scheduler_form": scheduler_form,
                "scheduler_preview": build_cron_preview(scheduler_form["schedule"] or None),
                "scheduler_job_name": scheduler_job_name,
                "scheduler_job": scheduler_job,
                "scheduler_current_preview": scheduler_job.preview if scheduler_job else card.preview,
                "scheduler_current_schedule": scheduler_job.schedule if scheduler_job else card.schedule,
                "scheduler_errors": scheduler_errors.get(card.name, []),
                "run_confirmation_errors": run_confirmation_errors.get(card.name, []),
                "email_test_errors": email_test_errors.get(card.name, []),
                "effective_config_yaml": _format_yaml_preview(overview.effective_config if overview else {}),
                "raw_config_yaml": _format_yaml_preview(overview.raw_config if overview else {}),
                "post_run_script_count": len(overview.effective_config.get("post_run_scripts", []))
                if overview and isinstance(overview.effective_config.get("post_run_scripts"), list)
                else 0,
                "email_state": email_state,
            }
        )

    selected_scheduler_row = None
    if selected_scheduler_for:
        selected_scheduler_row = next((row for row in crawler_rows if row["card"].name == selected_scheduler_for), None)

    selected_run_confirmation_row = None
    if selected_run_confirmation_for:
        selected_run_confirmation_row = next(
            (row for row in crawler_rows if row["card"].name == selected_run_confirmation_for),
            None,
        )

    selected_inspect_row = None
    if selected_inspect_for:
        selected_inspect_row = next((row for row in crawler_rows if row["card"].name == selected_inspect_for), None)

    selected_card_name = None
    if selected_scheduler_row:
        selected_card_name = selected_scheduler_row["card"].name
    elif selected_run_confirmation_row:
        selected_card_name = selected_run_confirmation_row["card"].name
    elif selected_inspect_row:
        selected_card_name = selected_inspect_row["card"].name

    for row in crawler_rows:
        row["is_selected"] = bool(selected_card_name and row["card"].name == selected_card_name)
        row["is_expanded"] = bool(selected_inspect_row and row["card"].name == selected_inspect_row["card"].name)

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "page_name": "dashboard",
            "state": state,
            "crawler_rows": crawler_rows,
            "scheduler_target": selected_scheduler_row,
            "run_confirm_target": selected_run_confirmation_row,
            "inspect_target": selected_inspect_row,
            "message_kind": message_kind,
            "message_text": message_text,
            "scheduler_weekday_options": SCHEDULER_WEEKDAY_OPTIONS,
            "scheduler_hour_options": SCHEDULER_HOUR_OPTIONS,
            "scheduler_minute_options": SCHEDULER_MINUTE_OPTIONS,
        },
    )


def _build_action_values(
    actions: list[Any],
    submitted_action_id: str | None,
    submitted_values: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    action_values: dict[str, dict[str, Any]] = {}
    submitted_values = submitted_values or {}

    for action in actions:
        field_values: dict[str, Any] = {}
        for field in action.fields:
            value = field.default_value
            if submitted_action_id == action.action_id and field.name in submitted_values:
                value = submitted_values[field.name]
            field_values[field.name] = value
        action_values[action.action_id] = field_values

    return action_values


def _format_yaml_preview(value: Any) -> str:
    if value in (None, "", {}, []):
        return "# No settings available"

    rendered = yaml.safe_dump(
        value,
        sort_keys=False,
        allow_unicode=False,
        default_flow_style=False,
    )
    return rendered.rstrip()


def _build_email_alert_state(overview: Any) -> dict[str, Any]:
    email_config = overview.effective_config.get("email") if overview else None
    state = {
        "filter_state": "missing",
        "is_configured": False,
        "is_ready": False,
        "summary": "No email alert configuration.",
        "mailhost": "",
        "fromaddr": "",
        "recipient_count": 0,
        "subject": "",
        "auth_mode": "none",
        "errors": [],
    }

    if not isinstance(email_config, dict):
        return state

    state["is_configured"] = True
    mailhost = str(email_config.get("mailhost") or "").strip()
    fromaddr = str(email_config.get("fromaddr") or "").strip()
    subject = str(email_config.get("subject") or "").strip()
    toaddrs_raw = email_config.get("toaddrs")
    toaddrs = [str(item).strip() for item in toaddrs_raw if str(item).strip()] if isinstance(toaddrs_raw, list) else []
    username = str(email_config.get("username") or "").strip()
    password = str(email_config.get("password") or "").strip()

    errors: list[str] = []
    if not mailhost:
        errors.append("mailhost missing")
    if not fromaddr:
        errors.append("fromaddr missing")
    if not toaddrs:
        errors.append("toaddrs missing")
    if not subject:
        errors.append("subject missing")

    auth_mode = "authenticated" if (username or password) else "relay"
    state.update(
        {
            "mailhost": mailhost,
            "fromaddr": fromaddr,
            "recipient_count": len(toaddrs),
            "subject": subject,
            "auth_mode": auth_mode,
            "errors": errors,
        }
    )

    if errors:
        state["filter_state"] = "incomplete"
        state["summary"] = "Email alerts incomplete: " + ", ".join(errors) + "."
        return state

    state["filter_state"] = "ready"
    state["is_ready"] = True
    state["summary"] = (
        f"Critical errors send mail via {mailhost} to {len(toaddrs)} recipient(s)."
    )
    return state


def _parse_mailhost(mailhost: str) -> tuple[str, int]:
    candidate = mailhost.strip()
    if ":" in candidate:
        host, port_text = candidate.rsplit(":", 1)
        if port_text.isdigit():
            return host, int(port_text)
    return candidate, 25


def _resolve_email_subject(config: dict[str, Any], crawler_name: str) -> str:
    email_config = config.get("email", {})
    subject_template = str(email_config.get("subject") or "OEDS crawler test email")
    return subject_template.replace(":crawler_name", crawler_name)


def _send_test_email(crawler_name: str, effective_config: dict[str, Any]) -> None:
    email_config = effective_config.get("email")
    if not isinstance(email_config, dict):
        raise RuntimeError("No email configuration found.")

    mailhost, port = _parse_mailhost(str(email_config.get("mailhost") or ""))
    if not mailhost:
        raise RuntimeError("SMTP mailhost is empty.")

    recipients = [str(item).strip() for item in email_config.get("toaddrs", []) if str(item).strip()]
    if not recipients:
        raise RuntimeError("No email recipients configured.")

    message = EmailMessage()
    message["Subject"] = "[TEST] " + _resolve_email_subject(effective_config, crawler_name)
    message["From"] = str(email_config.get("fromaddr") or "")
    message["To"] = ", ".join(recipients)
    message.set_content(
        "\n".join(
            [
                "This is a test message from OEDS Crawler Control.",
                f"Crawler: {crawler_name}",
                "Trigger: manual dashboard test",
            ]
        )
    )

    username = str(email_config.get("username") or "").strip()
    password = str(email_config.get("password") or "").strip()

    with smtplib.SMTP(mailhost, port, timeout=15) as server:
        if username or password:
            server.login(username, password)
        server.send_message(message)


def _select_scheduler_job_name(card: Any, requested_job_name: str | None) -> str | None:
    job_previews = list(getattr(card, "job_previews", []) or [])
    if not job_previews:
        return None

    requested = str(requested_job_name).strip() if requested_job_name else ""
    if requested and any(job.name == requested for job in job_previews):
        return requested

    enabled_job = next((job for job in job_previews if job.enabled is True), None)
    return (enabled_job or job_previews[0]).name


def _find_scheduler_job(card: Any, job_name: str | None) -> Any | None:
    if not job_name:
        return None
    return next((job for job in getattr(card, "job_previews", []) or [] if job.name == job_name), None)


def _build_scheduler_form_state(
    card: Any,
    overview: Any,
    override_values: dict[str, Any] | None = None,
    job_name: str | None = None,
) -> dict[str, Any]:
    default_enable = False
    default_schedule = ""

    if overview is not None:
        default_candidate = overview.default_config
        if isinstance(default_candidate.get("enable"), bool):
            default_enable = default_candidate["enable"]
        if isinstance(default_candidate.get("schedule"), str):
            default_schedule = default_candidate["schedule"]

    selected_job = _find_scheduler_job(card, job_name)
    if selected_job is not None:
        selected_enabled = selected_job.enabled if selected_job.enabled is not None else default_enable
        enable_value = "true" if selected_enabled else "false"
        schedule_value = selected_job.schedule or default_schedule or "0 4 * * *"
    else:
        enable_value = "true" if (card.enabled if card.enabled is not None else default_enable) else "false"
        schedule_value = card.schedule or default_schedule or "0 4 * * *"

    form_state = _parse_schedule_into_form_state(schedule_value)
    form_state["enable"] = enable_value

    if override_values:
        for key, value in override_values.items():
            if key == "weekly_days":
                form_state[key] = [str(item) for item in value]
            else:
                form_state[key] = str(value)
        if "enable" in override_values:
            form_state["enable"] = str(override_values["enable"]).strip().lower()
        if "schedule" in override_values:
            form_state["schedule"] = str(override_values["schedule"]).strip()
        if "advanced_schedule" not in form_state or not form_state["advanced_schedule"]:
            form_state["advanced_schedule"] = form_state["schedule"]

    return form_state


def _extract_scheduler_form_values(form: Any) -> dict[str, Any]:
    return {
        "scheduler_mode": str(form.get("scheduler_mode") or SCHEDULER_DEFAULT_FORM["scheduler_mode"]).strip().lower(),
        "hourly_interval_hours": str(form.get("hourly_interval_hours") or SCHEDULER_DEFAULT_FORM["hourly_interval_hours"]).strip(),
        "hourly_minute": str(form.get("hourly_minute") or SCHEDULER_DEFAULT_FORM["hourly_minute"]).strip(),
        "daily_hour": str(form.get("daily_hour") or SCHEDULER_DEFAULT_FORM["daily_hour"]).strip(),
        "daily_minute": str(form.get("daily_minute") or SCHEDULER_DEFAULT_FORM["daily_minute"]).strip(),
        "weekly_hour": str(form.get("weekly_hour") or SCHEDULER_DEFAULT_FORM["weekly_hour"]).strip(),
        "weekly_minute": str(form.get("weekly_minute") or SCHEDULER_DEFAULT_FORM["weekly_minute"]).strip(),
        "weekly_days": _normalize_weekday_values(form.getlist("weekly_days")),
        "advanced_schedule": str(form.get("advanced_schedule") or "").strip(),
        "schedule": str(form.get("schedule") or "").strip(),
    }


def _resolve_scheduler_schedule(form_values: dict[str, Any]) -> tuple[str, dict[str, Any], list[str]]:
    normalized: dict[str, Any] = {
        key: (list(value) if isinstance(value, list) else value)
        for key, value in SCHEDULER_DEFAULT_FORM.items()
    }
    normalized.update(form_values)
    errors: list[str] = []
    scheduler_mode = str(normalized.get("scheduler_mode", "daily")).strip().lower()
    valid_weekdays = {option["value"] for option in SCHEDULER_WEEKDAY_OPTIONS}

    if scheduler_mode not in {"hourly", "daily", "weekly", "advanced"}:
        errors.append("Unknown scheduler mode.")
        scheduler_mode = "daily"
        normalized["scheduler_mode"] = scheduler_mode

    if scheduler_mode == "advanced":
        schedule = str(normalized.get("advanced_schedule", "")).strip()
        if not schedule:
            errors.append("Schedule is required.")
        normalized["schedule"] = schedule
        normalized["advanced_schedule"] = schedule
        return schedule, normalized, errors

    schedule = ""
    if scheduler_mode == "hourly":
        minute_value, minute_error = _parse_bounded_integer(normalized.get("hourly_minute", "0"), 0, 59, "Minute")
        if minute_error:
            errors.append(minute_error)
        interval_value, interval_error = _parse_bounded_integer(
            normalized.get("hourly_interval_hours", "1"),
            1,
            23,
            "Hour interval",
        )
        if interval_error:
            errors.append(interval_error)

        if not errors and minute_value is not None and interval_value is not None:
            schedule = f"{minute_value} * * * *" if interval_value == 1 else f"{minute_value} */{interval_value} * * *"

    elif scheduler_mode == "daily":
        hour_value, hour_error = _parse_bounded_integer(normalized.get("daily_hour", "0"), 0, 23, "Hour")
        if hour_error:
            errors.append(hour_error)
        minute_value, minute_error = _parse_bounded_integer(normalized.get("daily_minute", "0"), 0, 59, "Minute")
        if minute_error:
            errors.append(minute_error)

        if not errors and hour_value is not None and minute_value is not None:
            schedule = f"{minute_value} {hour_value} * * *"

    elif scheduler_mode == "weekly":
        hour_value, hour_error = _parse_bounded_integer(normalized.get("weekly_hour", "0"), 0, 23, "Hour")
        if hour_error:
            errors.append(hour_error)
        minute_value, minute_error = _parse_bounded_integer(normalized.get("weekly_minute", "0"), 0, 59, "Minute")
        if minute_error:
            errors.append(minute_error)
        weekday_values = _normalize_weekday_values(normalized.get("weekly_days", []))
        if not weekday_values:
            errors.append("Select at least one weekday.")
        elif any(value not in valid_weekdays for value in weekday_values):
            errors.append("Weekday selection is invalid.")

        if not errors and hour_value is not None and minute_value is not None:
            schedule = f"{minute_value} {hour_value} * * {','.join(weekday_values)}"
        normalized["weekly_days"] = weekday_values

    normalized["schedule"] = schedule
    normalized["advanced_schedule"] = str(normalized.get("advanced_schedule") or schedule)
    return schedule, normalized, errors


def _parse_schedule_into_form_state(schedule: str) -> dict[str, Any]:
    form_state: dict[str, Any] = {
        key: (list(value) if isinstance(value, list) else value)
        for key, value in SCHEDULER_DEFAULT_FORM.items()
    }
    form_state["schedule"] = schedule
    form_state["advanced_schedule"] = schedule
    clean_schedule = schedule.strip()

    match = re.fullmatch(r"(\d+)\s+\*\s+\*\s+\*\s+\*", clean_schedule)
    if match:
        form_state["scheduler_mode"] = "hourly"
        form_state["hourly_interval_hours"] = "1"
        form_state["hourly_minute"] = match.group(1)
        return form_state

    match = re.fullmatch(r"(\d+)\s+\*/(\d+)\s+\*\s+\*\s+\*", clean_schedule)
    if match:
        form_state["scheduler_mode"] = "hourly"
        form_state["hourly_minute"] = match.group(1)
        form_state["hourly_interval_hours"] = match.group(2)
        return form_state

    match = re.fullmatch(r"(\d+)\s+(\d+)\s+\*\s+\*\s+\*", clean_schedule)
    if match:
        form_state["scheduler_mode"] = "daily"
        form_state["daily_minute"] = match.group(1)
        form_state["daily_hour"] = match.group(2)
        return form_state

    match = re.fullmatch(r"(\d+)\s+(\d+)\s+\*\s+\*\s+([\d,]+)", clean_schedule)
    if match:
        weekday_values = _normalize_weekday_values(match.group(3).split(","))
        if weekday_values:
            form_state["scheduler_mode"] = "weekly"
            form_state["weekly_minute"] = match.group(1)
            form_state["weekly_hour"] = match.group(2)
            form_state["weekly_days"] = weekday_values
            return form_state

    form_state["scheduler_mode"] = "advanced"
    return form_state


def _normalize_weekday_values(values: Any) -> list[str]:
    if not isinstance(values, list):
        values = [values] if values else []

    normalized: set[str] = set()
    for value in values:
        token = str(value).strip()
        if token == "7":
            token = "0"
        if token in SCHEDULER_WEEKDAY_ORDER:
            normalized.add(token)

    return [value for value in SCHEDULER_WEEKDAY_ORDER if value in normalized]


def _parse_bounded_integer(value: str, minimum: int, maximum: int, label: str) -> tuple[int | None, str | None]:
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return None, f"{label} must be a number."

    if parsed < minimum or parsed > maximum:
        return None, f"{label} must be between {minimum} and {maximum}."

    return parsed, None


def _select_run(history: list[RunRecord], active_run: RunRecord | None, selected_run_id: int | None) -> RunRecord | None:
    if selected_run_id is not None:
        selected = next((record for record in history if record.run_id == selected_run_id), None)
        if selected is not None:
            return selected

    if active_run is not None:
        return active_run

    if history:
        return history[0]

    return None


def _run_record_payload(record: RunRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "crawler_name": record.crawler_name,
        "action_id": record.action_id,
        "action_label": record.action_label,
        "trigger_source": record.trigger_source,
        "status": record.status,
        "created_at": record.created_at,
        "started_at": record.started_at,
        "finished_at": record.finished_at,
        "duration_seconds": record.duration_seconds,
        "summary": record.summary,
        "error_message": record.error_message,
        "log_path": record.log_path,
        "config_overrides": record.config_overrides,
        "action_payload": record.action_payload,
    }


def _form_to_payload(form: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    keys = list(form.keys())
    for key in keys:
        if key == "redirect_to":
            continue
        values = form.getlist(key)
        if len(values) == 1:
            payload[key] = values[0]
        else:
            payload[key] = values
    return payload


def _parse_optional_int(value: Any) -> int | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    try:
        return int(text_value)
    except ValueError as exc:
        raise ValueError(f"Expected an integer value, got '{text_value}'.") from exc


def _parse_enable_form_value(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def _append_query_string(url: str, params: dict[str, str]) -> str:
    split = urlsplit(url)
    query_items = parse_qsl(split.query, keep_blank_values=True)
    for key, value in params.items():
        query_items.append((key, value))
    new_query = urlencode(query_items)
    return urlunsplit((split.scheme, split.netloc, split.path, new_query, split.fragment))
