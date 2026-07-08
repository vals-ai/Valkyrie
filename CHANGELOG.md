# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-07-08]

### Changed
- Split the 1949-line tracker `utils.py` into a `utils/` package (`resources`, `reporting`, `task_execution`, `run_control`, `run_orchestration`, `harness_config`) behind a re-exporting `__init__.py` shim, so existing `from tracker.utils import X` imports keep working (#508)
- Update the `writing-tests` skill with new conventions: handling unused arguments (`ruff ARG`) in tests, tighter module docstrings, collapsing duplicate tests into parametrized cases, gating only credential-dependent integration tests, and avoiding integration coverage that duplicates a smoke/dispatch workflow (#509)

## [2026-07-07]

### Added
- Add per-task retry/attempt history: task and benchmark task list APIs now return the number of attempts and the history of prior evaluation/error results for retried tasks (#477)
- Add CLI commands to configure named sandbox providers (`valkyrie config provider set/default/remove`) and select one per run with `valkyrie run start --provider <name>` (#499)
- Add a benchmark service catalog endpoint that lists hosted and custom benchmark services visible to the calling tenant, proxied through the tracker to the benchmark catalog service (#502)
- Add a `writing-tests` agent skill documenting testing conventions and rubrics (layout, docstrings, naming, fixtures, determinism, typing) for the repo (#497)

### Changed
- Route sandbox provider secret resolution through named providers configured via CBS/CLI config instead of a single flat `SANDBOX_PROVIDER_SECRET_NAME`, with the legacy key still accepted as a fallback (#499)
- Replace the per-task `error_message` column with a queryable `ErrorResult` history table, and add `created_at` to evaluation results so retry history can be sorted and surfaced (#477)
- Speed up benchmark service listing by lazily running service health checks (#502)
- Bump `create-benchmark-service` dependency to v0.8.2 (#501)
