# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-06]

### Added
- Add an hourly production cleanup schedule for orphaned sandboxes: a provider-generic policy engine (list/refresh/delete over normalized candidate metadata) with a Daytona adapter, deleting sandboxes strictly older than 48 hours unless opted out via a `clean-up: false` label; ships disabled and dry-run by default behind `SANDBOX_CLEANUP_ENABLED`/`SANDBOX_CLEANUP_DRY_RUN`, with a dedicated Lambda, EventBridge schedule, and dead-letter queue (#524)

### Fixed
- Fix deployment maintenance classification incorrectly stopping active runs for unrelated WorkerStack changes: maintenance is now required only when synthesized base/head CDK templates show a real ExecutorHost task-definition/service rollout or an unsafe database migration, instead of flagging any change under executor-related source paths (#648)
- Fix CI required-check deadlock where path-filtered required workflows (tracker unit/integration tests, executor build, SDK package/compatibility) never reported a status on PRs that didn't touch their paths; these now always trigger and use an inline guard to skip heavy work and report success when irrelevant, with fail-closed behavior if the diff can't be computed (#649)
