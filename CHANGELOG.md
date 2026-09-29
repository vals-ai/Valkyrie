# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-29]

### Changed
- Route Tracker, ExecutorHost, release control, and release-test driver database connections through a new RDS Proxy (TLS required, 8-hour idle client timeout) in every stage; the Tracker engine now uses `NullPool` and the `DATABASE_POOL_SIZE`/`DATABASE_MAX_OVERFLOW` settings are removed (#854)
- Replace the per-dispatch heartbeat loop and 5-second authority polling in ExecutorHost with a single host-level lease keeper that renews all dispatch leases in one batched statement, so lease renewal no longer reads `benchmark` and cannot be blocked by table locks (#854)
- Expire noncurrent object versions after 30 days in the shared artifact bucket for Dev and Production; current objects, the one-day incomplete-upload cleanup, and the legacy Bench bucket are unchanged (#865)
- Update `create-benchmark-service` from v0.41.0 to v0.41.1 in Tracker and executor-artifact, with root and service lockfiles aligned (#861)

### Fixed
- Stop database connection counts from growing with the number of runs, which exhausted connection slots on the dev database when many runs started at once; transient database errors during claims, finish, terminalize, and task monitoring are now treated as unknown ownership and retried instead of failing the run (#854)
- Fix live status streams (`GET /fetch-benchmark?connect=true`) holding a database connection idle in transaction for the life of the stream; the request session is now closed before streaming and each poll closes its session before yielding events (#856)
- Allow `STOPPED` tasks to be retried while their run is still in progress, so `valk run retry` no longer requires stopping the whole run after a per-task stop (#860)
