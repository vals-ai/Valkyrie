# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-17]

### Added
- Add `--count`/`-n` flag to `valkyrie run start` for launching up to 10 independent runs from a single command, with sequential fail-fast starts, per-run confirmation output, and a combined `run status` command for tracking all started runs (#561)

### Changed
- Reorganize tracker unit tests by behavior (agent, API, AWS, database, logging, middleware, observability, utility), add database-backed coverage for the run and task detail APIs (org scoping, retry history, task sorting, literal wildcard search), and replace wall-clock waits with deterministic synchronization (#565)
