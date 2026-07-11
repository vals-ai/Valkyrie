# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-07-11]

### Added
- Add an async Python SDK (`valkyrie.sdk.ValkyrieClient`) for programmatic run management, with typed YAML configuration and methods to start, fetch, list, stream, retrieve results for, stop, resume, and retry runs, plus typed SDK errors and SSE stream parsing (#518)

### Changed
- Forward the tracker-resolved sandbox provider config through the eval-only resume path so `BenchmarkServiceClient.resume_evaluation` can rebuild a sandbox from persisted state without adding provider secrets to each benchmark-service deployment (#531)
- Pin tracker to `create-benchmark-service` v0.13.0 (#531)
