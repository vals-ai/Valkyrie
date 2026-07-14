# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-14]

### Added
- Add `valkyrie run errors <run-id>` command to inspect stored run and current task error messages inline, with `--format json` for machine-readable output, without needing to download a results file first (#536)
- Publish `valkyrie-sdk` as an independently versioned package with verified PyPI/TestPyPI publishing via GitHub OIDC, while preserving the existing `valkyrie.sdk` API (#535)
- Add task-scoped run stops: `valkyrie run stop` now accepts `--task-ids` / `--task-ids-file` to stop only selected tasks, with race-safe state transitions and sandbox cleanup scoped to those tasks (#523)

### Fixed
- Fix empty task error messages by falling back to the exception class name when the exception string is empty, so failures are no longer logged as blank errors (#541)
- Fix a Daytona PTY reconnect bug that leaked websocket connections and caused "Daytona PTY session no longer exists" failures by bumping `create-benchmark-service` to v0.13.1 (#542)

### Removed
- Remove the tracker's redundant direct Daytona SDK dependency now that it's provided transitively via `create-benchmark-service` (#546)
