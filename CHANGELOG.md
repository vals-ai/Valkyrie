# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-22]

### Changed

- Automatically deploy the dev environment on every push to `dev`, reusing the existing protected-environment and AWS-target validation path while still allowing manual plan/deploy/scope dispatches (#604)
- Improve run-level error messages when every task in a run fails: group similar task errors with `difflib`, store one representative message per distinct error group (with frequency counts), and surface the stored error through tracker, SDK, CLI text, and JSON/JSONL fetch responses (#593)

### Fixed

- Fix `valk run start --connect` exiting early when a run's task rows haven't been discovered yet, by returning a valid zero-count task breakdown instead of raising, so the connected CLI and tracker SSE stream stay open until discovery progresses or the run reaches a terminal state (#599)
