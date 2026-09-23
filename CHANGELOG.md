# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-23]

### Added
- Let an authorized ValSmith organization start a managed run in its own application-provisioned S3 bucket (`managed_s3_bucket`), with CloudWatch logs derived from and saved locations reused across execution, retry, and recovery; adds `ValkyrieRunAcceptedError` to the SDK so callers can distinguish an accepted run with an unacknowledged dispatch from a genuine storage rejection (#813)

### Changed
- Reduce task-list database work by adding a composite index on `Task(benchmark, org_id, started_at)`, evaluating the correlated latest-error lookup only for `ERROR` tasks, and using an exact `COUNT(*)` so PostgreSQL can use an index-only scan (#829)
- Stop issuing a duplicate `EvaluationResult` query on `/retrieve-results` by reusing the already-loaded result rows, with a deterministic `created_at`/`id` tie-break and a supporting history index (#828)

### Fixed
- Stop ExecutorHost from killing a healthy running executor when a periodic dispatch-authority check hits a transient PostgreSQL connection failure; the executor now stays alive until an unexpired, heartbeat-renewed lease actually expires or authority is confirmed lost (#827)
- Standardize Tracker RDS allocated storage at 100 GiB across BENCH, PROD, DEV, and RELEASE_TEST (up from 20 GiB), after BENCH's free storage was trending toward exhaustion (#831)
- Keep at least two Tracker ECS tasks running in every stage (desired count and scaling min/max raised from 1 to 2), after BENCH was repeatedly left with zero healthy ALB targets while its single task was replaced (#830)
