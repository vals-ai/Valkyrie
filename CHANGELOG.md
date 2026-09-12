# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-12]

### Added
- Add `--lambda` option to `run retry`/`run resume` to override the post-run Lambda function for a run, persisted on the accepted dispatch (#778)

### Fixed
- Isolate Sentry task context per tracked task and clear stale sandbox identity (tags/context) before each sandbox-recovery attempt, preventing concurrent tasks or retries from leaking sandbox correlation data (#772)

### Changed
- Upgrade Tracker's `create-benchmark-service` dependency to v0.38.0, adding OpenTelemetry and Sentry FastAPI extras to its dependency graph (#786)
