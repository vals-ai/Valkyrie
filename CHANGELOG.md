# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-09]

### Added
- Add PostgreSQL-backed sandbox scheduler primitives: per-run priority (P0–P4) and hashed provider-capacity-pool tracking, advisory-lock-based admission and task claiming, and recovery of abandoned builds (#612)
- Queue sandbox creation through the new scheduler when `SANDBOX_QUEUE_ENABLED` is set and the selected provider supports managed admission, ordering waiting runs by priority, enqueue time, and task ID while unmanaged providers keep direct execution; add `--priority` to `valkyrie run start`, `priority` to the SDK's `runs.start`, and an organization-scoped `GET /scheduler/overview` endpoint (#558)

### Fixed
- Correct the post-run lambda docs: a failing lambda no longer marks the run `ERROR` — the run stays `FINISHED` and the failure is only recorded in tracker logs, not surfaced on the run status (#765)
