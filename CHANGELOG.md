# Changelog

## 0.0.0-local

- Initial local split repository for registry-based scheduler runtime.
- Added scheduler CLI, daemon wrapper, planning, queueing, and runtime contracts.
- Extracted the current KIT crawler admin UI, templates, static assets, and
  server launcher into this module.
- Fixed legacy post-run subprocess imports by prepending the workspace CWD to
  `PYTHONPATH`.
- Fixed Admin UI repository root resolution for installed/containerized runtime
  workspaces.
- Added starter GitHub Actions CI for compile and standalone scheduler-core
  test checks.
