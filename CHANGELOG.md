# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-15]

### Added
- Add `valkyrie run artifacts` CLI command and `client.artifacts` SDK resource (`list`, `download_url`, `download`) to list and download run artifacts through organization-scoped Tracker routes with short-lived, server-issued download URLs (#785)
- Add `client.runs.download_outputs()` SDK method to download and extract run outputs into a new local directory, unpacking `agent_output.tar.gz` task archives while guarding against unsafe paths, symlinks, duplicate entries, and oversized transfers (#784)
- Add opt-in `include_capacity` to the Scheduler overview (`valkyrie queue status` / `client.scheduler.overview()`) exposing per-target, per-sandbox-class CPU/memory/disk capacity domains (#768)

### Changed
- `valkyrie run output` now requires a new, non-existing output directory; existing destinations are refused (#785)
- CBS now admits targeted snapshot sources by measuring the exact target, class, and resources instead of Valkyrie eagerly rejecting them before admission (#768)
- Default new root traces to 10% sampling in the Logfire/OTel configuration while preserving parent sampling decisions and existing `LOGFIRE_TRACE_SAMPLE_RATE`/`OTEL_TRACES_SAMPLER_ARG` overrides (#793)

### Fixed
- Recover expired executor dispatches by persisting claim deadlines, heartbeats, and lease expiry, and reconciling stale `RUNNING` dispatches so a dead executor host no longer leaves runs stuck (#726)
