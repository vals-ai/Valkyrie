# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-08-20]

### Added

- Tracker now records structured failure provenance (producer, operation, error type, cause code, retry-scheduled flag, and failed attempt number) alongside each error message, so task/run failures show where and how they happened instead of requiring message-text parsing; intermediate retry failures are retained for diagnostics but excluded from the current error shown to users (#688)
- Recover opted-in tasks after sandbox loss (e.g. Modal's 24-hour sandbox lifetime expiring mid-run on 120-hour KSP ladders): the sandbox is recreated with the same run id, provider, environment, and durable volume mounts, preserving the original deadline, setup retry bounds, and lazy evaluation resume; forwards `AWS_SESSION_TOKEN` for temporary credentials (#683)

### Fixed

- Resuming a terminal benchmark run with no remaining tasks (re-running only the final score) no longer 500s with a `benchmark_finished_requires_timestamp` check-constraint violation; the benchmark lifecycle is now properly reset to `IN_PROGRESS` before the empty resume proceeds (#715)
- Restored a single Alembic migration head after the failure-provenance and managed-AWS migrations both branched from the same parent, which had been breaking tracker startup during deployment (#711)
- Closed a race where a retrying dispatch could take over authority while a superseded dispatch was still publishing terminal side effects (final view upload, completion Lambda callback, Slack notification), which could produce stale output or duplicate one-shot notifications; the dispatch-authority lock is now held across all three side effects, and the completion callback is bounded to one 60-second attempt and offloaded to a worker thread so it can't block the executor event loop (#706)
