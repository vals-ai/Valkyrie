# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-16]

### Added
- Add SDK V2 with hosted workflow parity: typed APIs for inspecting benchmarks, task statuses, artifacts, launch metadata, and result-existence checks; run analysis and output-archive streaming; agent listing and download URLs; benchmark-service discovery and dataset task-ID lookup (#550)

### Changed
- Allow stopping specific tasks within a run via `client.runs.stop(..., task_ids=...)` instead of only the whole run (#550)
- Bump valkyrie-sdk package version to 0.2.0 (#550)

### Fixed
- Log and translate transport-level HTTPX errors raised while streaming SDK results, instead of letting them surface as raw exceptions (#550)
