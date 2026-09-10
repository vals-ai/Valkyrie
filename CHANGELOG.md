# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-10]

### Fixed
- Retry task execution on a fresh sandbox when the benchmark service reports a broken Daytona sandbox (missing `dockerd` in a corrupted rootfs), instead of committing a terminal error; other service-reported errors still fail terminally (#770)

### Changed
- Pin `create-benchmark-service` to released tag `v0.35.0` (later `v0.37.1`) across Tracker, the executor artifact, and all lockfiles, replacing the temporary git-revision pin (#766, #770)
