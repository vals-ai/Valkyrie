# Changelog

All notable changes to this project are documented in this file, generated daily from merged pull requests.

---

## [2026-08-12]

### Fixed
- Stop forced sandbox teardown from deleting sandboxes out from under a still-running executor: force stop now gives a live dispatch a bounded 15s window to release its own sandboxes before reaping stragglers, and the orphan reaper now emits the same `sandbox.delete` audit record (`initiated_by="orphan_cleanup"`) as every other deletion path (#664)
- Preserve a task's own success/failure outcome when sandbox teardown deletion fails at task completion, instead of letting the teardown's `ProviderSandboxError` override it; the failure is still audited via the existing `sandbox.delete` record (#668)

### Changed
- Bump `create-benchmark-service` to v0.27.6, pulling in lifecycle fixes where durable PTY completion now wins over later health failures, status publication is atomic, and cleanup reaches DELETE without unnecessary start/refresh/autostop preparation (#668)
