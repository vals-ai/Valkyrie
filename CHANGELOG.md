# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-01]

### Added
- Add `--benchmark-url` override to the CLI and SDK retry/resume commands, letting callers point a run's retries and resumes at a different benchmark service; the tracker validates the URL and persists it on the run (#626)
- Forward benchmark task volume mounts into sandbox creation and tag sandboxes with a `run-id` label so the sandbox provider can resolve per-run volume subpaths (#633)

### Changed
- Bump the `create-benchmark-service` dependency to v0.23.0 for safe Modal/Daytona volume mount support (#633)

### Fixed
- Fix the prod deployment gate by loading the trusted maintenance classifier from the repository's default branch and verifying the fetched base/candidate commits before use, instead of checking out from PR/merge-group refs that don't exist on prod (#632)
