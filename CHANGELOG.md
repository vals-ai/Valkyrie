# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-23]

### Changed
- Upgrade create-benchmark-service to v0.17.3 to pick up provider-level retries for transient Daytona failures, retrying only the failed API call instead of the whole task (#611)
