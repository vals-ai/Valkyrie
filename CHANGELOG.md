# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-21]

### Fixed
- Preserve dispatch enqueue when a start/retry benchmark request is cancelled after admission commits, preventing runs from being stranded until recovery marks the dispatch failed; adds real PostgreSQL row-lock coverage for both HTTP handlers (#825)

### Changed
- Grant the dev Tracker task role `s3:PutObject`/`s3:AbortMultipartUpload` on the `agents/*` bucket prefix so `agent push` can upload bundles through dev Tracker, while leaving bench/prod/release-test permissions unchanged (#824)
