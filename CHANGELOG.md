# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-25]

### Changed
- Bump `create-benchmark-service` to v0.40.2 in the tracker and executor_artifact, adding paced Modal sandbox creation (5 creates/s) and retry-with-backoff for `RESOURCE_EXHAUSTED` responses instead of surfacing a `SandboxError` (#846)
