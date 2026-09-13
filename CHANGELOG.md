# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-13]

### Added
- Add `valkyrie queue status` CLI command and `client.scheduler.overview()` SDK method to inspect sandbox queue priorities, positions, wait times, and active tasks, with independently paginated waiting/active lists (#773)

### Changed
- Correlate task execution telemetry by recording sandbox ID/name/state on the creation span, carrying the executor dispatch ID and attempt start time through span/log/error context, and exposing OTel transaction-root identity fields as Sentry tags; upgrade the CBS dependency to v0.39.1 (#789)
- Publish Sentry release and deployment metadata for core and executor deployments in dev and production so deployed errors link back to their source commits (#790)
