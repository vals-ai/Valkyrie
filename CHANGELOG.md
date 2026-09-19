# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-19]

### Fixed
- Size PostgreSQL connection pools per deployment stage: dev now caps Tracker and executor processes at 5 connections plus 2 overflow (down from 60), while bench and production keep 50 + 10; invalid (unbounded or negative) pool settings are now rejected at startup (#818)
- Bump `valkyrie-sdk` package version to 0.2.23 to clear the SDK version-check gate blocking production promotion (#812)

### Changed
- Convert Tracker's AWS runtime calls (Secrets Manager reads, Lambda invocation, CloudWatch log group setup) to native async operations instead of thread-offloaded sync calls, and consolidate secret access behind a single async `SecretStore` interface (#816)
- Rework executor artifact download to stream to a temp file via `asyncio.to_thread` with cleanup on failure, and compute artifact digests by chunked hashing instead of `hashlib.file_digest` (#816)

### Removed
- Remove the filesystem-based local executor artifact reader and release initializer (`tracker.local.*` modules) along with its local-development documentation, ahead of a replacement local adapter (#816)
