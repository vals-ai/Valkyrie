# Changelog

All notable changes to this project are documented in this file, generated daily from merged pull requests.

---

## [2026-08-11]

### Added
- Emit structured `sandbox.delete` audit events recording sandbox id, benchmark/task context, initiator, and outcome for every deletion path (create-cancelled, force-stop, task-teardown), closing a gap where sandbox deletions left no traceable record (#662)

### Changed
- Bump `create-benchmark-service` to v0.27.4, adding bounded timeouts and retries around Daytona sandbox operations so a stalled toolbox call now fails with a clear, logged error instead of hanging task setup indefinitely (#665)

### Fixed
- Cache successful Descope access-key exchanges for up to 10 seconds per process to stop hitting Descope rate limits (429s) during authentication bursts (#653)
- Close the benchmark-service client after `/start-benchmark` preflight checks so failed health/task-id validation no longer leaks HTTP connection pools and cached sandbox providers (#655)
- Fix a live integration test assertion that broke after the `create-benchmark-service` bump reworded the sandbox start-failure message (#666)

### Security
- Restrict automatic forwarding of the tracker's Descope API key to benchmark services hosted at the Vals-owned origin; custom benchmark services no longer receive the tracker's credential (#663)
