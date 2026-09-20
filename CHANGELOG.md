# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-20]

### Fixed
- Grant the dev Tracker role `s3:PutObject`/`s3:AbortMultipartUpload` on its bucket's `agents/*` prefix so `agent push` can upload bundles in dev, while leaving bench/prod/release-test roles unchanged (#824)
- Make `valkyrie run logs` resolve the tracker URL via `config_location()`/`tracker_service_url()` instead of defaulting, so it targets the correct environment's SDK client (#823)
