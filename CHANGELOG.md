# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-25]

### Changed
- Refactored the tracker service's AWS integration into pluggable runtime abstractions — `ObjectStore` for object storage, a secrets store, CloudWatch log-location resolution, and executor artifact distribution — replacing direct S3/Secrets Manager/CloudWatch calls throughout `main.py` and related modules (#736, #656, #657, #658, #659)
- Raised the cached Descope M2M access-key exchange TTL from 10 seconds to 1 hour, cutting access-key exchange calls to Descope by roughly 360x while still expiring cache entries no later than the session token's own expiry (#735)
- Bumped the `create-benchmark-service` dependency to v0.31.2 and relocked `uv.lock` across the tracker service, executor_artifact service, and the root workspace (#737, #738, #739)
