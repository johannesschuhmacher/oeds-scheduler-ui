# oeds-scheduler-ui

Scheduler and crawler admin UI module for the modular OEDS stack.

This repository is part of the modular OEDS stack. The shared crawler and
database core remains in
[open-energy-data-server](https://github.com/open-energy-data-server/open-energy-data-server),
while crawler extensions, post-processing, and installation live in
[oeds-crawler-pack](https://github.com/johannesschuhmacher/oeds-crawler-pack),
[oeds-post-scripts](https://github.com/johannesschuhmacher/oeds-post-scripts),
and [oeds-deployment](https://github.com/johannesschuhmacher/oeds-deployment).

## Responsibility

This module should run and operate OEDS crawlers. It owns scheduler/runtime
coordination and the operator-facing admin UI. It should not own crawler
implementation files.

## Current State

The module contains a registry-based scheduler core:

- `interfaces.py`: lazy crawler specs, config normalization, generic run helper
- `factory.py`: static constructor audit and crawler construction
- `planner.py`: dry scheduler job plans from config + registry
- `runtime.py`: job runner, post-run hook boundary, queue locking
- `service.py`: schedule adapter and scheduler service tick
- `application.py`: config + inventory assembly and reload boundary
- `config.py`: YAML config loading and file change signature
- `daemon.py`: stoppable loop around the scheduler application

The current KIT admin UI has also been extracted into this repo:

- `crawler_admin/`: FastAPI app, templates, static files, config editor, manual
  run controls, gapfill/forecast views, and run history service
- `crawler_admin_server.py`: CLI-compatible server launcher
- `oeds-crawler-admin`: package entry point for the server launcher

The dependency direction is:

```text
scheduler-ui -> crawler registry -> crawler implementation
```

The scheduler does not need to import `crawler.<name>` directly during planning.
Crawler import/construction happens only when a planned job is dispatched.

## Current CLIs

The package entry point now loads the modular application:

```powershell
oeds-scheduler --config CRAWLER_CONFIG.yml --inventory modular_repos/docs/crawler-inventory.json --workspace-root modular_repos
```

`--once` runs one scheduler tick. `--daemon` runs the persistent scheduler loop.

Run the admin UI:

```powershell
oeds-crawler-admin
```

Environment variables preserved from KIT:

| Variable | Default | Purpose |
| --- | --- | --- |
| `OEDS_ADMIN_HOST` | `127.0.0.1` | bind host |
| `OEDS_ADMIN_PORT` | `3010` | bind port |
| `OEDS_ADMIN_RELOAD` | unset | enable Uvicorn reload for local development |
| `OEDS_ADMIN_STATE_DIR` | platform user data path | run history and logs |

The admin UI still expects the assembled deployment workspace to provide the
current crawler package, `CRAWLER_CONFIG.yml`, `crawler/.env`, and post-script
modules. That keeps crawler implementation ownership outside this repo while
preserving the current UI behavior.

## Local Development

Run the repository-level verifier:

```powershell
python .\modular_repos\tools\verify_modules.py
```

This checks:

- registry priority and constructor compatibility
- job expansion for `default` and named `jobs`
- post-run metadata preservation
- queue locking
- scheduler service ticks and daemon wait calculation
- application reload on config changes

The repository includes a starter GitHub Actions workflow for compile checks
and standalone scheduler-core tests. Admin UI, gapfill, database, crawler, and
Docker coverage is handled by the deployment-level full function test.

## Reproducibility Against KIT

The scheduler implementation is not a byte-for-byte copy of
`crawler_scheduler.py`; it is a modular rewrite. Reproducibility is therefore
checked by contract tests:

- same config merge behavior for `default` and `jobs`
- same `enable`, `schedule`, `post_run_scripts`, and `run_post_scripts`
  semantics
- same crawler constructor support for KIT and upstream OEDS crawler styles
- no crawler import during planning

The extracted admin UI is currently checked byte-for-byte against the current
KIT source by:

```powershell
python .\modular_repos\tools\verify_split_parity.py
```

## Future Work

- deeper admin-service integration with the registry-based scheduler runtime
- threaded production dispatch if needed
- packaging decision for the shared `crawler_core`/`crawler.common` facade
