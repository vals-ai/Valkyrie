# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [2026-06-27]

### Added

- Tracker API `GET /benchmarks/{id}` now returns `cloudwatch_url` and `s3_bucket_url` for the run, derived from harness headers; both fields are `null` when harness headers are absent (#489)
- Tracker API `GET /benchmarks/{id}/tasks` accepts `sort` (`task_id` | `started_at` | `duration` | `status`) and `sort_dir` (`asc` | `desc`) query parameters; `sort=status desc` surfaces errors first using an attention-priority ordering (#489)

### Fixed

- Benchmark progress now counts `STOPPED` tasks as finished in `create_benchmark_table_row()`, matching the behavior of `build_benchmark_table_rows()` (#494)
- Tracker CLI harness config accepts the legacy `DAYTONA_SECRET_NAME` key as a fallback for `SANDBOX_PROVIDER_SECRET_NAME`, preventing config validation failures on existing deployments (#494)
