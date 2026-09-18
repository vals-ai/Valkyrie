# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-18]

### Added
- Expose optional aggregate GPU capacity and allowed GPU types on the scheduler overview API and Python SDK, alongside existing CPU/memory/disk capacity (#803)
- Persist each run's resolved AWS region, S3 bucket, log group, and log retention (`environment`/`properties` fields on start requests) so retries, resumes, and artifact/log reads keep using the original settings even after deployment defaults change (#797)

### Changed
- Compose storage, secrets, logging, and sandbox access behind a shared runtime services interface and inject it into agent storage, log routes, and benchmark execution, replacing ad hoc per-route AWS runtime resolution (#797, #798)

### Fixed
- Speed up the benchmarks filter-options endpoint (runs-list dropdown values) by ~7x, replacing a full per-org table load and Python-side dedup with a single SQL `DISTINCT` query (#804)
