# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-02]

### Changed
- Raised the tracker's output artifact size limits from 50 MiB to 100 MiB, both per-artifact and in total, after a benchmark run produced a 64–68 MB `trajectory.json` that was rejected under the old limit (#638, #639)

### Fixed
- Reverted a same-day change that had made missing/oversized/failed output artifact uploads nonfatal; a required output artifact failure once again marks the task as `ERROR` (and the benchmark as `ERROR` before final scoring) instead of silently skipping the artifact and evaluating with incomplete results (#639)
