# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-19]

### Added
- Add live concurrency updates for in-progress runs via `valkyrie run update <run-id> --concurrency <N>` and `PATCH /benchmarks/{benchmark_id}/concurrency`; increases admit more waiting tasks on the next monitor refresh, decreases preserve in-flight work and pause new admissions until usage drops below the new limit (#590)
- Add `--count` flag for Valkyrie runs (#561)
- Add SDK V2 for the Valkyrie SDK package (#550)
- Add CLI environment selection (#471)
- Add focused run error inspection (#536)

### Changed
- Byte-cap the retained agent output tail at 64KB (previously capped by chunk count, letting a single large chunk balloon worker memory) and stream agent output archive uploads to S3 via multipart upload instead of buffering the whole archive, fixing prod worker OOM during long agent runs (#591)
- Retry transient agent dependency-setup failures up to four times (with 0s/10s/60s backoff) in the current sandbox before falling back to one fresh sandbox, so infrastructure flakiness no longer immediately fails the task (#563)
- Use standard S3 retry configuration (#556)
- Rotate matching benchmark-service auth to use a hosted API key (#554)
- Security pass across the tracker service (#547)
- Improve tracker test coverage, structure, and run finalization logic (#564)
- Deploy the Valkyrie dev environment in its own AWS account, using a `ValkDev` stack name prefix (#570, #577)
- Improve CLI test coverage and integration checks (#574)
- Self-seed the tracker integration agent artifact (#511)

### Fixed
- Preserve WebSocket close details from benchmark-service instead of discarding them (#555)

### Removed
- Remove redundant Daytona dependency from tracker (#546)
